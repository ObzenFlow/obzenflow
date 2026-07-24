// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::ai::{
    params_hash_for_chat, prompt_hash_for_chat, schema_hash_for_response_format, AiClientError,
    CanonicalizationComponent, ChatClient, ChatCompletionCompleted, ChatRequest, LlmHashes,
    LlmObservability, ResolvedTokenEstimator,
};
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectPortRequirement, EffectSafety,
};

const CHAT_PORT: &str = "chat";
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
    hashes: LlmHashes,
    estimator: ResolvedTokenEstimator,
}

impl std::fmt::Debug for ChatCompletion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChatCompletion")
            .field("label", &self.label)
            .field("target", &self.request.target())
            .field("hashes", &self.hashes)
            .field("estimator", &self.estimator)
            .finish_non_exhaustive()
    }
}

impl ChatCompletion {
    pub fn new(
        label: impl Into<String>,
        request: ChatRequest,
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
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;

    type Outcome = ChatCompletionCompleted;

    fn label(&self) -> &str {
        &self.label
    }

    fn canonical_input(&self) -> serde_json::Value {
        // Construction has already canonicalised every request component.
        // ChatRequest contains only serialisable DTO fields.
        serde_json::to_value(&self.request)
            .expect("a constructed ChatCompletion request must serialize")
    }

    fn required_ports() -> Vec<EffectPortRequirement> {
        vec![EffectPortRequirement::of::<dyn ChatClient>(CHAT_PORT)]
    }

    fn validate_port_bindings(&self, ctx: &EffectContext) -> Result<(), EffectError> {
        let client = ctx.port::<dyn ChatClient>(CHAT_PORT)?;
        let expected = self.request.target();
        let observed = client.target();
        if &expected != observed {
            return Err(EffectError::EffectPortBindingMismatch {
                port: CHAT_PORT.to_string(),
                expected: expected.to_string(),
                observed: observed.to_string(),
            });
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut EffectContext,
    ) -> Result<ChatCompletionCompleted, EffectError> {
        let client = ctx.port::<dyn ChatClient>(CHAT_PORT)?;
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

        Ok(ChatCompletionCompleted {
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

fn map_client_error(error: AiClientError) -> EffectError {
    match error {
        AiClientError::TargetMismatch { requested, bound } => {
            EffectError::EffectPortBindingInvariantViolation {
                port: CHAT_PORT.to_string(),
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

    #[test]
    fn canonicalization_detail_is_bounded_at_a_utf8_boundary() {
        let diagnostic = format!("{}é", "x".repeat(MAX_CANONICALIZATION_DETAIL_BYTES - 1));

        let ChatCompletionBuildError::RequestCanonicalization { detail, .. } =
            canonicalization_error(CanonicalizationComponent::Prompt, diagnostic);

        assert_eq!(detail, "x".repeat(MAX_CANONICALIZATION_DETAIL_BYTES - 1));
        assert!(detail.len() <= MAX_CANONICALIZATION_DETAIL_BYTES);
    }
}
