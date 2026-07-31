// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::ai::{
    params_hash_for_chat, prompt_hash_for_chat, schema_hash_for_response_format, AiClientError,
    CanonicalizationComponent, ChatClient, ChatCompletionReply, ChatRequest, ChatTarget, LlmHashes,
    LlmObservability, ResolvedTokenEstimator, CHAT_CLIENT_PORT,
};
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectOutcomePayload, EffectPortRequirement, EffectRecord,
    EffectSafety, RecordedReply,
};

const LEGACY_CHAT_COMPLETION_EVENT_TYPE: &str = "ai.chat_completion.completed";
const LEGACY_CHAT_COMPLETION_EVENT_TYPE_V1: &str = "ai.chat_completion.completed.v1";
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
    const SCHEMA_VERSION: u32 = 2;
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
        let response = client
            .chat(self.request.clone())
            .await
            .map_err(map_client_error)?;

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

    fn decode_legacy_recorded_reply(
        records: &[&EffectRecord],
    ) -> Result<Option<Self::Outcome>, EffectError> {
        let [record] = records else {
            return Ok(None);
        };
        if record.descriptor.effect_type.as_str() != Self::EFFECT_TYPE {
            return Ok(None);
        }
        let EffectOutcomePayload::SucceededFact {
            event_type,
            output,
            outcome_fact_ordinal,
            outcome_fact_count,
        } = &record.outcome
        else {
            return Ok(None);
        };
        if !matches!(
            event_type.as_str(),
            LEGACY_CHAT_COMPLETION_EVENT_TYPE | LEGACY_CHAT_COMPLETION_EVENT_TYPE_V1
        ) {
            return Ok(None);
        }
        if outcome_fact_ordinal.get() != 0 || outcome_fact_count.get() != 1 {
            return Err(EffectError::EffectProvenanceMismatch(
                "legacy ChatCompletion reply must be one complete ordinal-0 outcome row"
                    .to_string(),
            ));
        }
        serde_json::from_value(output.clone())
            .map(Some)
            .map_err(|error| EffectError::Serialization(error.to_string()))
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

fn map_client_error(error: AiClientError) -> EffectError {
    match error {
        AiClientError::TargetMismatch { requested, bound } => {
            EffectError::EffectPortBindingInvariantViolation {
                port: CHAT_CLIENT_PORT.to_string(),
                expected: bound.to_string(),
                observed: requested.to_string(),
            }
        }
        AiClientError::Timeout { message } => dependency("timeout", message),
        AiClientError::Remote { message } => dependency("remote", message),
        AiClientError::RateLimited { message, .. } => dependency("rate_limited", message),
        AiClientError::Auth { message } => dependency("authentication", message),
        AiClientError::InvalidRequest { message } => dependency("invalid_request", message),
        AiClientError::Unsupported { message } => dependency("unsupported", message),
        AiClientError::Other { message } => dependency("other", message),
    }
}

fn dependency(code: &'static str, message: String) -> EffectError {
    EffectError::DependencyFailed {
        failure_source: EffectFailureSource::new("chat_client"),
        code: EffectFailureCode::new(code),
        message,
        retry: RetryDisposition::NotRetryable,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::{
        EffectCursor, EffectDescriptor, EffectDescriptorHash, EffectRecord, OutcomeFactOrdinal,
    };
    use obzenflow_runtime::effects::OutcomeFactCount;

    fn legacy_record(
        effect_type: &str,
        event_type: &str,
        ordinal: u32,
        count: u32,
    ) -> EffectRecord {
        let reply = ChatCompletionReply {
            response: obzenflow_core::ai::ChatResponse {
                text: "archived reply".to_string(),
                tool_calls: Vec::new(),
                usage: None,
                raw: None,
            },
            observability: LlmObservability::new(
                obzenflow_core::ai::AiProvider::new("fixture"),
                "model",
                LlmHashes::new("prompt".to_string(), "params".to_string()),
            ),
        };
        EffectRecord {
            cursor: EffectCursor::new("flow", "stage", 1_u64, 0_u32),
            descriptor_hash: EffectDescriptorHash::new("descriptor"),
            descriptor: EffectDescriptor::new(effect_type, "chat", 2_u32, "1", "input"),
            outcome: EffectOutcomePayload::SucceededFact {
                event_type: obzenflow_core::EventType::from(event_type),
                output: serde_json::to_value(reply).expect("reply serialises"),
                outcome_fact_ordinal: OutcomeFactOrdinal::new(ordinal),
                outcome_fact_count: OutcomeFactCount::new(count),
            },
            origin: None,
        }
    }

    #[test]
    fn canonicalization_detail_is_bounded_at_a_utf8_boundary() {
        let diagnostic = format!("{}é", "x".repeat(MAX_CANONICALIZATION_DETAIL_BYTES - 1));

        let ChatCompletionBuildError::RequestCanonicalization { detail, .. } =
            canonicalization_error(CanonicalizationComponent::Prompt, diagnostic);

        assert_eq!(detail, "x".repeat(MAX_CANONICALIZATION_DETAIL_BYTES - 1));
        assert!(detail.len() <= MAX_CANONICALIZATION_DETAIL_BYTES);
    }

    #[test]
    fn legacy_decoder_is_exactly_scoped_to_historical_chat_completion_rows() {
        for event_type in [
            LEGACY_CHAT_COMPLETION_EVENT_TYPE,
            LEGACY_CHAT_COMPLETION_EVENT_TYPE_V1,
        ] {
            let record = legacy_record(ChatCompletion::EFFECT_TYPE, event_type, 0, 1);
            let decoded = <ChatCompletion as Effect>::decode_legacy_recorded_reply(&[&record])
                .expect("legacy row decodes")
                .expect("legacy row is recognised");
            assert_eq!(decoded.response.text, "archived reply");
        }

        let wrong_effect = legacy_record("test.not_chat", LEGACY_CHAT_COMPLETION_EVENT_TYPE, 0, 1);
        assert!(
            <ChatCompletion as Effect>::decode_legacy_recorded_reply(&[&wrong_effect])
                .expect("unrelated effect is ignored")
                .is_none()
        );
        let wrong_fact = legacy_record(ChatCompletion::EFFECT_TYPE, "test.not_legacy_chat", 0, 1);
        assert!(
            <ChatCompletion as Effect>::decode_legacy_recorded_reply(&[&wrong_fact])
                .expect("unrelated fact is ignored")
                .is_none()
        );
        let malformed = legacy_record(
            ChatCompletion::EFFECT_TYPE,
            LEGACY_CHAT_COMPLETION_EVENT_TYPE,
            1,
            2,
        );
        assert!(matches!(
            <ChatCompletion as Effect>::decode_legacy_recorded_reply(&[&malformed]),
            Err(EffectError::EffectProvenanceMismatch(_))
        ));
    }
}
