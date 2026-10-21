// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::transforms::ChatTransformSettings;
use super::{ChatCompletion, ChatTransform, EmbeddingGeneration, EmbeddingTransform};
use obzenflow_core::ai::{
    ChatParams, ChatResponse, ChatResponseFormat, EmbeddingDimensions, EmbeddingParams,
    EmbeddingResponse, ToolDefinition,
};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::EffectBinding;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde_json::Value;
use std::sync::Arc;

/// Deterministic typed builder for one standalone chat effect per input.
#[derive(Clone)]
pub struct ChatTransformBuilder {
    logic_version: Option<String>,
    system: Option<String>,
    params: ChatParams,
    tools: Vec<ToolDefinition>,
    response_format: Option<ChatResponseFormat>,
}

// `Default` is intentionally absent; `new` is the sole empty-builder entry point.
#[allow(clippy::new_without_default)]
impl ChatTransformBuilder {
    pub fn new() -> Self {
        Self {
            logic_version: None,
            system: None,
            params: ChatParams::default(),
            tools: Vec::new(),
            response_format: None,
        }
    }

    pub fn logic_version(mut self, version: impl Into<String>) -> Self {
        self.logic_version = Some(version.into());
        self
    }

    pub fn system(mut self, text: impl Into<String>) -> Self {
        self.system = Some(text.into());
        self
    }

    pub fn temperature(mut self, temperature: f32) -> Self {
        self.params.temperature = Some(temperature);
        self
    }

    pub fn max_tokens(mut self, max_tokens: u32) -> Self {
        self.params.max_tokens = Some(max_tokens);
        self
    }

    pub fn top_p(mut self, top_p: f32) -> Self {
        self.params.top_p = Some(top_p);
        self
    }

    pub fn seed(mut self, seed: u64) -> Self {
        self.params.seed = Some(seed);
        self
    }

    pub fn response_format(mut self, response_format: ChatResponseFormat) -> Self {
        self.response_format = Some(response_format);
        self
    }

    pub fn tools(mut self, tools: Vec<ToolDefinition>) -> Self {
        self.tools = tools;
        self
    }

    pub fn extra_param(mut self, key: impl Into<String>, value: Value) -> Self {
        self.params.extras.insert(key.into(), value);
        self
    }

    /// Build the transform that calls `chat` once per input.
    pub fn build_typed<In, Out>(
        self,
        chat: EffectBinding<ChatCompletion>,
        input_to_prompt: impl Fn(&In) -> Result<String, HandlerError> + Send + Sync + 'static,
        response_to_output: impl Fn(In, ChatResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<ChatTransform<In, Out>, HandlerError>
    where
        In: TypedPayload + Send + Sync + 'static,
        Out: TypedPayload + Send + Sync + 'static,
    {
        let logic_version = required_logic_version(
            self.logic_version,
            "ChatTransformBuilder::build_typed: missing required logic_version",
        )?;
        Ok(ChatTransform::from_parts(
            chat,
            ChatTransformSettings {
                system: self.system,
                params: self.params,
                tools: self.tools,
                response_format: self.response_format,
            },
            Arc::new(input_to_prompt),
            Arc::new(response_to_output),
            logic_version,
        ))
    }
}

/// Deterministic typed builder for one standalone embedding effect per input.
#[derive(Clone)]
pub struct EmbeddingTransformBuilder {
    logic_version: Option<String>,
    dimensions: Option<EmbeddingDimensions>,
}

// Keep this aligned with `ChatTransformBuilder`: the negative public-surface
// contract requires construction to remain explicit through `new`.
#[allow(clippy::new_without_default)]
impl EmbeddingTransformBuilder {
    pub fn new() -> Self {
        Self {
            logic_version: None,
            dimensions: None,
        }
    }

    pub fn logic_version(mut self, version: impl Into<String>) -> Self {
        self.logic_version = Some(version.into());
        self
    }

    pub fn dimensions(mut self, dimensions: EmbeddingDimensions) -> Self {
        self.dimensions = Some(dimensions);
        self
    }

    /// Build the transform that calls `embedding` once per input.
    pub fn build_typed<In, Out>(
        self,
        embedding: EffectBinding<EmbeddingGeneration>,
        input_to_inputs: impl Fn(&In) -> Result<Vec<String>, HandlerError> + Send + Sync + 'static,
        response_to_output: impl Fn(In, EmbeddingResponse) -> Result<Out, HandlerError>
            + Send
            + Sync
            + 'static,
    ) -> Result<EmbeddingTransform<In, Out>, HandlerError>
    where
        In: TypedPayload + Send + Sync + 'static,
        Out: TypedPayload + Send + Sync + 'static,
    {
        let logic_version = required_logic_version(
            self.logic_version,
            "EmbeddingTransformBuilder::build_typed: missing required logic_version",
        )?;
        Ok(EmbeddingTransform::from_parts(
            embedding,
            EmbeddingParams {
                dimensions: self.dimensions,
            },
            Arc::new(input_to_inputs),
            Arc::new(response_to_output),
            logic_version,
        ))
    }
}

fn required_logic_version(
    version: Option<String>,
    missing_message: &'static str,
) -> Result<String, HandlerError> {
    version
        .filter(|version| !version.trim().is_empty())
        .ok_or_else(|| HandlerError::Validation(missing_message.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::{ChatBindingEvidence, EmbeddingBindingEvidence, CHAT_CLIENT, EMBEDDING_CLIENT};
    use obzenflow_core::ai::{
        embedding_binding_fingerprint, AiProvider, ChatTarget, EmbeddingTarget,
        HeuristicTokenEstimator, ResolvedTokenEstimator, TokenEstimatorFallbackReason,
        TokenEstimatorResolutionInfo,
    };
    use obzenflow_runtime::effects::{
        EffectPortResolutionError, EffectRegistrationBuilder, LogicalEffectBindingName,
    };

    fn chat_binding() -> EffectBinding<ChatCompletion> {
        let estimator = ResolvedTokenEstimator::new(
            Arc::new(HeuristicTokenEstimator::default()),
            TokenEstimatorResolutionInfo::heuristic(
                "test-model",
                TokenEstimatorFallbackReason::ExplicitHeuristic,
                None,
            ),
        );
        EffectRegistrationBuilder::<ChatCompletion>::new(
            LogicalEffectBindingName::new("chat").unwrap(),
            ChatBindingEvidence::new(ChatTarget::new("ollama", "test-model"), estimator).unwrap(),
        )
        .bind_deferred_with_metadata(
            CHAT_CLIENT,
            Arc::new(|| Err(EffectPortResolutionError::ClientConstructionFailed)),
        )
        .unwrap()
        .finish()
        .unwrap()
    }

    fn embedding_binding() -> EffectBinding<EmbeddingGeneration> {
        let provider = AiProvider::new("ollama");
        let target = EmbeddingTarget::new(
            provider.clone(),
            "nomic-embed-text",
            embedding_binding_fingerprint(&provider, "nomic-embed-text", "http://localhost:11434"),
        );
        EffectRegistrationBuilder::<EmbeddingGeneration>::new(
            LogicalEffectBindingName::new("embedding").unwrap(),
            EmbeddingBindingEvidence::new(target).unwrap(),
        )
        .bind_deferred_with_metadata(
            EMBEDDING_CLIENT,
            Arc::new(|| Err(EffectPortResolutionError::ClientConstructionFailed)),
        )
        .unwrap()
        .finish()
        .unwrap()
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Input;
    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "test.standalone_builder.input";
    }

    #[derive(Debug, serde::Serialize, serde::Deserialize)]
    struct Output;
    impl TypedPayload for Output {
        const EVENT_TYPE: &'static str = "test.standalone_builder.output";
    }

    #[test]
    fn chat_logic_version_is_required_with_the_locked_diagnostic() {
        let error = ChatTransformBuilder::new()
            .build_typed::<Input, Output>(
                chat_binding(),
                |_| Ok("prompt".into()),
                |_, _| Ok(Output),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            HandlerError::Validation(message)
                if message == "ChatTransformBuilder::build_typed: missing required logic_version"
        ));
    }

    #[test]
    fn embedding_logic_version_is_required_with_the_locked_diagnostic() {
        let error = EmbeddingTransformBuilder::new()
            .build_typed::<Input, Output>(
                embedding_binding(),
                |_| Ok(vec!["input".into()]),
                |_, _| Ok(Output),
            )
            .unwrap_err();
        assert!(matches!(
            error,
            HandlerError::Validation(message)
                if message == "EmbeddingTransformBuilder::build_typed: missing required logic_version"
        ));
    }
}
