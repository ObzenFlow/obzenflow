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
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectPortRequirement, EffectSafety, RecordedReply,
};

const MAX_CANONICALIZATION_DETAIL_BYTES: usize = 512;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ChatCompletionBuildError {
    #[error("chat request {component:?} canonicalization failed: {detail}")]
    RequestCanonicalization {
        component: CanonicalizationComponent,
        detail: String,
    },
}

/// Public replay-safe chat-completion effect.
#[derive(Clone)]
pub struct ChatCompletion {
    label: String,
    request: ChatRequest,
    binding_target: ChatTarget,
    hashes: LlmHashes,
    estimator: ResolvedTokenEstimator,
}

impl std::fmt::Debug for ChatCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatCompletion")
            .field("label", &self.label)
            .field("target", &self.binding_target)
            .field("hashes", &self.hashes)
            .field("estimator", &self.estimator)
            .finish_non_exhaustive()
    }
}

impl ChatCompletion {
    pub fn new(
        label: impl Into<String>,
        request: ChatRequest,
        binding_target: ChatTarget,
        estimator: ResolvedTokenEstimator,
    ) -> Result<Self, ChatCompletionBuildError> {
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
            binding_target,
            hashes,
            estimator,
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

    type Outcome = ChatCompletionReply;
    type OutcomeSemantics = RecordedReply;

    fn label(&self) -> &str {
        &self.label
    }

    fn canonical_input(&self) -> serde_json::Value {
        // Construction has already canonicalised every request component.
        // ChatRequest contains only serialisable DTO fields.
        serde_json::json!({
            "binding_target": &self.binding_target,
            "request": &self.request,
        })
    }

    fn required_ports() -> Vec<EffectPortRequirement> {
        vec![EffectPortRequirement::of::<dyn ChatClient>(
            CHAT_CLIENT_PORT,
        )]
    }

    fn validate_port_bindings(&self, ctx: &EffectContext) -> Result<(), EffectError> {
        let client = ctx.port::<dyn ChatClient>(CHAT_CLIENT_PORT)?;
        let expected = &self.binding_target;
        let observed = client.target();
        if expected != observed {
            return Err(EffectError::EffectPortBindingMismatch {
                port: CHAT_CLIENT_PORT.to_string(),
                expected: expected.to_string(),
                observed: observed.to_string(),
            });
        }
        Ok(())
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<ChatCompletionReply, EffectError> {
        let client = ctx.port::<dyn ChatClient>(CHAT_CLIENT_PORT)?;
        let estimated_input_tokens = self
            .estimator
            .estimator()
            .estimate_chat_request(&self.request);
        let response = client.chat(self.request.clone()).await.map_err(|error| {
            ai_client_error_to_effect_error(error, CHAT_CLIENT_PORT, "chat_client")
        })?;

        let mut observability = LlmObservability::new(
            self.request.provider.clone(),
            self.request.model.clone(),
            self.hashes.clone(),
        );
        observability.usage = response.usage.clone();
        observability.estimated_input_tokens = Some(estimated_input_tokens);
        observability.estimated_input_resolution = Some(self.estimator.info().clone());

        Ok(ChatCompletionReply {
            response,
            observability,
        })
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
            canonicalization_error(CanonicalizationComponent::Prompt, diagnostic);

        assert_eq!(detail, "x".repeat(MAX_CANONICALIZATION_DETAIL_BYTES - 1));
        assert!(detail.len() <= MAX_CANONICALIZATION_DETAIL_BYTES);
    }
}
