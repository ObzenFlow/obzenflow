// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::error_mapping::{ai_client_error_to_effect_error, provider_error_to_resolution};
use crate::ai::unreachable::UnreachableChatClient;
use async_trait::async_trait;
use obzenflow_core::ai::{
    params_hash_for_chat, prompt_hash_for_chat, schema_hash_for_response_format, AiEndpoint,
    AiModelSpec, CanonicalizationComponent, ChatClient, ChatCompletionOutcome, ChatCompletionReply,
    ChatModelDescription, ChatModelProvider, ChatRequest, ChatRequestSpec, ChatTarget, LlmHashes,
    LlmObservability, ResolvedTokenEstimator, CHAT_CLIENT_PORT,
};
use obzenflow_core::BoundedBindingEvidence;
use obzenflow_runtime::ai::{AiBinding, AiBindingError, AiEffect, AiModelSource, AiRequestError};
use obzenflow_runtime::effects::{
    BoundEffect, Configured, Effect, EffectBindingEvidence, EffectBindingUse, EffectContext,
    EffectError, EffectName, EffectPortMetadataContext, EffectPortResolverWithMetadata,
    EffectPortSlot, EffectPortSlotSet, EffectRegistrationBuilder, EffectSafety,
    LogicalEffectBindingName, RecordedReply, ResolvedEffectPort,
};
use std::sync::Arc;

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
    pub(crate) fn new(
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

pub const CHAT_CLIENT: EffectPortSlot<dyn ChatClient, ChatTarget> =
    EffectPortSlot::new(CHAT_CLIENT_PORT);

/// Replay-safe chat completion, performed only by an `ai!` stage.
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
    pub(crate) fn new(
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
    type BindingMode = Configured<ChatBindingEvidence>;

    type Outcome = ChatCompletionOutcome;
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

    fn validate_port_metadata(&self, ctx: &EffectPortMetadataContext) -> Result<(), EffectError> {
        let expected = self.binding.evidence().target();
        let observed = ctx.metadata(CHAT_CLIENT)?;
        if expected != observed.as_ref() {
            return Err(EffectError::target_invariant_violation(CHAT_CLIENT));
        }
        Ok(())
    }

    async fn execute(&self, ctx: &mut EffectContext) -> Result<ChatCompletionOutcome, EffectError> {
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

        Ok(ChatCompletionOutcome {
            response,
            observability,
        })
    }
}

impl BoundEffect for ChatCompletion {
    type BindingEvidence = ChatBindingEvidence;

    fn binding_use(&self) -> &EffectBindingUse<Self> {
        &self.binding
    }

    fn required_slots() -> EffectPortSlotSet {
        EffectPortSlotSet::single(CHAT_CLIENT)
    }
}

impl EffectName for ChatCompletion {
    type Effect = Self;
    type Name = Self;
}

impl AiEffect for ChatCompletion {
    type Request = ChatRequestSpec;
    type Reply = ChatCompletionReply;
    type Provider = dyn ChatModelProvider;

    const SECTION: &'static str = "ai.models";
    const FAMILY: &'static str = "chat";

    fn effect(
        label: &'static str,
        request: &ChatRequestSpec,
        binding: EffectBindingUse<Self>,
    ) -> Result<Self, AiRequestError> {
        let bound = request.bind_target(binding.evidence().target());
        Self::new(label, bound, binding).map_err(|error| AiRequestError(error.to_string()))
    }

    fn reply(request: ChatRequestSpec, outcome: ChatCompletionOutcome) -> ChatCompletionReply {
        ChatCompletionReply::new(request, outcome)
    }

    fn bind(
        source: AiModelSource<dyn ChatModelProvider>,
    ) -> Result<AiBinding<Self>, AiBindingError> {
        let (stage, endpoint_name, description, resolver): (
            String,
            String,
            _,
            EffectPortResolverWithMetadata<_, _>,
        ) = match source {
            AiModelSource::Configured {
                stage,
                endpoint,
                model,
                provider,
            } => {
                let description = provider.describe(&endpoint, &model)?;
                let name = endpoint.name.clone();
                let resolver = connecting(provider, endpoint, description.clone());
                (stage, name, description, resolver)
            }
            AiModelSource::Recorded {
                stage,
                spec,
                provider,
            } => {
                let description = provider.describe_recorded(&spec)?;
                let resolver = unreachable(description.target.clone(), spec.endpoint.clone());
                (stage, spec.endpoint, description, resolver)
            }
            AiModelSource::Kept {
                stage,
                spec,
                endpoint,
                provider,
            } => {
                let description = provider.describe_recorded(&spec)?;
                let serving = endpoint.filter(|endpoint| {
                    provider
                        .describe(endpoint, &spec.model)
                        .is_ok_and(|current| {
                            current.target.binding_fingerprint
                                == description.target.binding_fingerprint
                        })
                });
                let resolver = match serving {
                    Some(endpoint) => connecting(provider, endpoint, description.clone()),
                    None => unreachable(description.target.clone(), spec.endpoint.clone()),
                };
                (stage, spec.endpoint, description, resolver)
            }
        };
        let spec = AiModelSpec {
            effect_type: Self::EFFECT_TYPE.to_string(),
            endpoint: endpoint_name,
            provider: description.target.provider.clone(),
            model: description.target.model.clone(),
            binding_fingerprint: description
                .target
                .binding_fingerprint
                .as_ref()
                .map(|fingerprint| fingerprint.as_str().to_string())
                .unwrap_or_default(),
            estimator: Some(description.estimator.info().clone()),
        };
        let evidence = ChatBindingEvidence::new(description.target, description.estimator)
            .map_err(|error| AiBindingError::Evidence(error.to_string()))?;
        let logical_name = LogicalEffectBindingName::new(stage)
            .map_err(|error| AiBindingError::Evidence(error.to_string()))?;
        let binding = EffectRegistrationBuilder::<Self>::new(logical_name, evidence)
            .bind_deferred_with_metadata(CHAT_CLIENT, resolver)?
            .finish()?;
        Ok(AiBinding { binding, spec })
    }
}

fn connecting(
    provider: Arc<dyn ChatModelProvider>,
    endpoint: AiEndpoint,
    description: ChatModelDescription,
) -> EffectPortResolverWithMetadata<dyn ChatClient, ChatTarget> {
    Arc::new(move || {
        let client = provider
            .connect(&endpoint, &description)
            .map_err(provider_error_to_resolution)?;
        let metadata = Arc::new(client.target().clone());
        Ok(ResolvedEffectPort::new(client, metadata))
    })
}

fn unreachable(
    target: ChatTarget,
    endpoint: String,
) -> EffectPortResolverWithMetadata<dyn ChatClient, ChatTarget> {
    Arc::new(move || {
        let client: Arc<dyn ChatClient> =
            Arc::new(UnreachableChatClient::new(target.clone(), endpoint.clone()));
        Ok(ResolvedEffectPort::new(client, Arc::new(target.clone())))
    })
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
