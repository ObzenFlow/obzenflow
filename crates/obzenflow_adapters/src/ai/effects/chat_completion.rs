// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::error_mapping::ai_client_error_to_effect_error;
use async_trait::async_trait;
use obzenflow_core::ai::{
    params_hash_for_chat, prompt_hash_for_chat, schema_hash_for_response_format,
    CanonicalizationComponent, ChatClient, ChatCompletionReply, ChatRequest, ChatTarget, LlmHashes,
    LlmObservability, ResolvedTokenEstimator, CHAT_CLIENT_PORT,
};
use obzenflow_core::BoundedBindingEvidence;
use obzenflow_runtime::effects::{
    Effect, EffectBindingEvidence, EffectBindingUse, EffectContext, EffectError, EffectPortSlot,
    EffectPortSlotSet, EffectSafety, Named, NamedEffect, RecordedReply,
};

const MAX_CANONICALIZATION_DETAIL_BYTES: usize = 512;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ChatCompletionBuildError {
    #[error("chat request {component:?} canonicalization failed: {detail}")]
    RequestCanonicalization {
        component: CanonicalizationComponent,
        detail: String,
    },
    #[error("chat request target does not match the selected effect binding")]
    BindingTargetMismatch,
}

#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
pub enum ChatBindingEvidenceBuildError {
    #[error("chat binding target and token estimator select different models")]
    EstimatorModelMismatch,
    #[error("chat binding evidence could not be canonicalised")]
    CanonicalizationFailed,
    #[error("chat binding evidence exceeds the framework byte bound")]
    EvidenceTooLarge,
}

/// Credential-free binding evidence plus the resolved estimator used by chat effects.
#[derive(Clone)]
pub struct ChatBindingEvidence {
    target: ChatTarget,
    estimator: ResolvedTokenEstimator,
    canonical: BoundedBindingEvidence,
}

impl ChatBindingEvidence {
    pub fn new(
        target: ChatTarget,
        estimator: ResolvedTokenEstimator,
    ) -> Result<Self, ChatBindingEvidenceBuildError> {
        if target.model != estimator.info().model {
            return Err(ChatBindingEvidenceBuildError::EstimatorModelMismatch);
        }
        let canonical = serde_json::to_vec(&(&target, estimator.info()))
            .map_err(|_| ChatBindingEvidenceBuildError::CanonicalizationFailed)?;
        let canonical = BoundedBindingEvidence::try_new(canonical)
            .map_err(|_| ChatBindingEvidenceBuildError::EvidenceTooLarge)?;
        Ok(Self {
            target,
            estimator,
            canonical,
        })
    }

    pub fn target(&self) -> &ChatTarget {
        &self.target
    }

    pub fn estimator(&self) -> &ResolvedTokenEstimator {
        &self.estimator
    }
}

impl PartialEq for ChatBindingEvidence {
    fn eq(&self, other: &Self) -> bool {
        self.canonical == other.canonical
    }
}

impl Eq for ChatBindingEvidence {}

impl std::fmt::Debug for ChatBindingEvidence {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChatBindingEvidence")
            .field("evidence", &"<not disclosed>")
            .finish()
    }
}

impl EffectBindingEvidence for ChatBindingEvidence {
    const SCHEMA_VERSION: u32 = 1;

    fn canonical_bytes(&self) -> BoundedBindingEvidence {
        self.canonical.clone()
    }
}

pub const CHAT_CLIENT: EffectPortSlot<dyn ChatClient> = EffectPortSlot::new(CHAT_CLIENT_PORT);

/// Public replay-safe chat-completion effect.
#[derive(Clone)]
pub struct ChatCompletion {
    label: String,
    request: ChatRequest,
    binding: EffectBindingUse<Self>,
    hashes: LlmHashes,
}

impl std::fmt::Debug for ChatCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatCompletion")
            .field("label", &self.label)
            .field("binding", &self.binding)
            .field("hashes", &self.hashes)
            .finish_non_exhaustive()
    }
}

impl ChatCompletion {
    pub fn new(
        label: impl Into<String>,
        request: ChatRequest,
        binding: EffectBindingUse<Self>,
    ) -> Result<Self, ChatCompletionBuildError> {
        if !binding
            .evidence()
            .target()
            .logically_matches(&request.target())
        {
            return Err(ChatCompletionBuildError::BindingTargetMismatch);
        }
        let prompt_hash = prompt_hash_for_chat(&request.messages)
            .map_err(|error| canonicalization_error(CanonicalizationComponent::Prompt, error))?;
        let schema_hash = schema_hash_for_response_format(request.response_format.as_ref())
            .map_err(|error| {
                canonicalization_error(CanonicalizationComponent::ResponseSchema, error)
            })?;
        let params_hash = params_hash_for_chat(&request).map_err(|error| {
            canonicalization_error(CanonicalizationComponent::Parameters, error)
        })?;
        let mut hashes = LlmHashes::new(prompt_hash, params_hash);
        hashes.schema_hash = schema_hash;
        Ok(Self {
            label: label.into(),
            request,
            binding,
            hashes,
        })
    }

    pub fn request(&self) -> &ChatRequest {
        &self.request
    }

    pub fn hashes(&self) -> &LlmHashes {
        &self.hashes
    }
}

#[async_trait]
impl Effect for ChatCompletion {
    const EFFECT_TYPE: &'static str = "obzenflow.ai.chat_completion";
    const SCHEMA_VERSION: u32 = 3;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;
    type BindingMode = Named<ChatBindingEvidence>;

    type Outcome = ChatCompletionReply;
    type OutcomeSemantics = RecordedReply;

    fn label(&self) -> &str {
        &self.label
    }

    fn canonical_input(&self) -> serde_json::Value {
        // Construction has already canonicalised every request component.
        // ChatRequest contains only serialisable DTO fields.
        serde_json::json!({
            "binding_target": self.binding.evidence().target(),
            "request": &self.request,
        })
    }

    fn validate_port_bindings(&self, ctx: &EffectContext) -> Result<(), EffectError> {
        let client = ctx.port(CHAT_CLIENT)?;
        let expected = self.binding.evidence().target();
        let observed = client.target();
        if expected != observed {
            return Err(EffectError::target_invariant_violation(CHAT_CLIENT));
        }
        Ok(())
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<ChatCompletionReply, EffectError> {
        let client = ctx.port(CHAT_CLIENT)?;
        let estimated_input_tokens = self
            .binding
            .evidence()
            .estimator()
            .estimator()
            .estimate_chat_request(&self.request);
        let response = client
            .chat(self.request.clone())
            .await
            .map_err(|error| ai_client_error_to_effect_error(error, CHAT_CLIENT, "chat_client"))?;

        let mut observability = LlmObservability::new(
            self.request.provider.clone(),
            self.request.model.clone(),
            self.hashes.clone(),
        );
        observability.usage = response.usage.clone();
        observability.estimated_input_tokens = Some(estimated_input_tokens);
        observability.estimated_input_resolution =
            Some(self.binding.evidence().estimator().info().clone());

        Ok(ChatCompletionReply {
            response,
            observability,
        })
    }
}

impl NamedEffect for ChatCompletion {
    type BindingEvidence = ChatBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(CHAT_CLIENT)
    }
}

fn canonicalization_error(
    component: CanonicalizationComponent,
    error: impl std::fmt::Display,
) -> ChatCompletionBuildError {
    let mut detail = error.to_string();
    if detail.len() > MAX_CANONICALIZATION_DETAIL_BYTES {
        let mut boundary = MAX_CANONICALIZATION_DETAIL_BYTES;
        while !detail.is_char_boundary(boundary) {
            boundary -= 1;
        }
        detail.truncate(boundary);
    }
    ChatCompletionBuildError::RequestCanonicalization { component, detail }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonicalization_detail_is_bounded_at_a_utf8_boundary() {
        let diagnostic = format!("{}é", "x".repeat(MAX_CANONICALIZATION_DETAIL_BYTES - 1));

        let ChatCompletionBuildError::RequestCanonicalization { detail, .. } =
            canonicalization_error(CanonicalizationComponent::Prompt, diagnostic)
        else {
            panic!("expected canonicalization diagnostic")
        };

        assert_eq!(detail, "x".repeat(MAX_CANONICALIZATION_DETAIL_BYTES - 1));
        assert!(detail.len() <= MAX_CANONICALIZATION_DETAIL_BYTES);
    }
}
