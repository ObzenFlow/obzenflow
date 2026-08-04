// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::error_mapping::ai_client_error_to_effect_error;
use async_trait::async_trait;
use obzenflow_core::ai::{
    params_hash_for_embedding, prompt_hash_for_embedding_inputs, CanonicalizationComponent,
    EmbeddingClient, EmbeddingDimensions, EmbeddingGenerationReply, EmbeddingRequest,
    EmbeddingResponse, EmbeddingTarget, LlmHashes, LlmObservability, EMBEDDING_CLIENT_PORT,
};
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_runtime::effects::{
    Effect, EffectContext, EffectError, EffectPortRequirement, EffectSafety, RecordedReply,
};

const MAX_CANONICALIZATION_DETAIL_BYTES: usize = 512;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EmbeddingGenerationBuildError {
    #[error("embedding request requires at least one input")]
    EmptyInputs,
    #[error("embedding request {component:?} canonicalization failed: {detail}")]
    RequestCanonicalization {
        component: CanonicalizationComponent,
        detail: String,
    },
}

/// Replay-safe embedding generation through the sealed embedding port.
#[derive(Clone)]
pub struct EmbeddingGeneration {
    label: String,
    request: EmbeddingRequest,
    binding_target: EmbeddingTarget,
    hashes: LlmHashes,
}

impl std::fmt::Debug for EmbeddingGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddingGeneration")
            .field("label", &self.label)
            .field("target", &self.binding_target)
            .field("hashes", &self.hashes)
            .finish_non_exhaustive()
    }
}

impl EmbeddingGeneration {
    pub fn new(
        label: impl Into<String>,
        request: EmbeddingRequest,
        binding_target: EmbeddingTarget,
    ) -> Result<Self, EmbeddingGenerationBuildError> {
        if request.inputs.is_empty() {
            return Err(EmbeddingGenerationBuildError::EmptyInputs);
        }
        let prompt_hash = prompt_hash_for_embedding_inputs(&request.inputs)
            .map_err(|error| canonicalization_error(CanonicalizationComponent::Prompt, error))?;
        let params_hash = params_hash_for_embedding(&request).map_err(|error| {
            canonicalization_error(CanonicalizationComponent::Parameters, error)
        })?;

        Ok(Self {
            label: label.into(),
            request,
            binding_target,
            hashes: LlmHashes::new(prompt_hash, params_hash),
        })
    }

    pub fn request(&self) -> &EmbeddingRequest {
        &self.request
    }

    pub fn hashes(&self) -> &LlmHashes {
        &self.hashes
    }
}

#[async_trait]
impl Effect for EmbeddingGeneration {
    const EFFECT_TYPE: &'static str = "obzenflow.ai.embedding_generation";
    const SCHEMA_VERSION: u32 = 1;
    const SAFETY: EffectSafety = EffectSafety::NonIdempotentAtLeastOnce;

    type Outcome = EmbeddingGenerationReply;
    type OutcomeSemantics = RecordedReply;

    fn label(&self) -> &str {
        &self.label
    }

    fn canonical_input(&self) -> serde_json::Value {
        serde_json::json!({
            "binding_target": &self.binding_target,
            "request": &self.request,
        })
    }

    fn required_ports() -> Vec<EffectPortRequirement> {
        vec![EffectPortRequirement::of::<dyn EmbeddingClient>(
            EMBEDDING_CLIENT_PORT,
        )]
    }

    fn validate_port_bindings(&self, ctx: &EffectContext) -> Result<(), EffectError> {
        let client = ctx.port::<dyn EmbeddingClient>(EMBEDDING_CLIENT_PORT)?;
        let observed = client.target();
        if &self.binding_target != observed {
            return Err(EffectError::EffectPortBindingMismatch {
                port: EMBEDDING_CLIENT_PORT.to_string(),
                expected: self.binding_target.to_string(),
                observed: observed.to_string(),
            });
        }
        Ok(())
    }

    async fn execute(
        &self,
        ctx: &mut EffectContext,
    ) -> Result<EmbeddingGenerationReply, EffectError> {
        let client = ctx.port::<dyn EmbeddingClient>(EMBEDDING_CLIENT_PORT)?;
        let response = client.embed(self.request.clone()).await.map_err(|error| {
            ai_client_error_to_effect_error(error, EMBEDDING_CLIENT_PORT, "embedding_client")
        })?;
        let response = validate_response(&self.request, response)?;

        let mut observability = LlmObservability::new(
            self.request.provider.clone(),
            self.request.model.clone(),
            self.hashes.clone(),
        );
        observability.usage = response.usage.clone();

        Ok(EmbeddingGenerationReply {
            response,
            observability,
        })
    }
}

fn validate_response(
    request: &EmbeddingRequest,
    mut response: EmbeddingResponse,
) -> Result<EmbeddingResponse, EffectError> {
    if response.vectors.len() != request.inputs.len() {
        return Err(invalid_response(format!(
            "embedding response cardinality mismatch: expected {}, observed {}",
            request.inputs.len(),
            response.vectors.len()
        )));
    }

    let observed_width = response.vectors.first().map(Vec::len).unwrap_or_default();
    let observed_width = u32::try_from(observed_width)
        .ok()
        .and_then(|width| EmbeddingDimensions::try_from(width).ok())
        .ok_or_else(|| invalid_response("embedding response vectors must have non-zero width"))?;

    if response
        .vectors
        .iter()
        .any(|vector| vector.len() != observed_width.get() as usize)
    {
        return Err(invalid_response(
            "embedding response vectors must have one common width",
        ));
    }
    if response.vector_dim != observed_width {
        return Err(invalid_response(format!(
            "embedding response vector_dim mismatch: declared {}, observed {}",
            response.vector_dim, observed_width
        )));
    }
    if let Some(requested) = request.params.dimensions {
        if requested != observed_width {
            return Err(invalid_response(format!(
                "embedding response width mismatch: requested {}, observed {}",
                requested, observed_width
            )));
        }
    }

    response.vector_dim = observed_width;
    Ok(response)
}

fn invalid_response(message: impl Into<String>) -> EffectError {
    EffectError::BoundaryRejected {
        rejected_by: EffectFailureSource::new("embedding_client"),
        code: EffectFailureCode::new("invalid_response"),
        message: message.into(),
        retry: RetryDisposition::NotRetryable,
    }
}

fn canonicalization_error(
    component: CanonicalizationComponent,
    error: impl std::fmt::Display,
) -> EmbeddingGenerationBuildError {
    let mut detail = error.to_string();
    if detail.len() > MAX_CANONICALIZATION_DETAIL_BYTES {
        let mut boundary = MAX_CANONICALIZATION_DETAIL_BYTES;
        while !detail.is_char_boundary(boundary) {
            boundary -= 1;
        }
        detail.truncate(boundary);
    }
    EmbeddingGenerationBuildError::RequestCanonicalization { component, detail }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{
        embedding_binding_fingerprint, AiProvider, EmbeddingParams, EmbeddingTarget,
    };

    fn target() -> EmbeddingTarget {
        let provider = AiProvider::new("ollama");
        EmbeddingTarget::new(
            provider.clone(),
            "nomic-embed-text",
            embedding_binding_fingerprint(&provider, "nomic-embed-text", "http://localhost:11434"),
        )
    }

    fn request(dimensions: Option<EmbeddingDimensions>) -> EmbeddingRequest {
        EmbeddingRequest {
            provider: AiProvider::new("ollama"),
            model: "nomic-embed-text".to_string(),
            inputs: vec!["one".to_string(), "two".to_string()],
            params: EmbeddingParams { dimensions },
        }
    }

    #[test]
    fn validates_cardinality_and_common_requested_width() {
        let dimensions = EmbeddingDimensions::try_from(2).unwrap();
        let response = EmbeddingResponse {
            vectors: vec![vec![1.0, 2.0], vec![3.0, 4.0]],
            vector_dim: dimensions,
            usage: None,
        };
        assert_eq!(
            validate_response(&request(Some(dimensions)), response)
                .unwrap()
                .vector_dim,
            dimensions
        );

        let mixed = EmbeddingResponse {
            vectors: vec![vec![1.0, 2.0], vec![3.0]],
            vector_dim: dimensions,
            usage: None,
        };
        assert!(matches!(
            validate_response(&request(Some(dimensions)), mixed),
            Err(EffectError::BoundaryRejected { .. })
        ));
    }

    #[test]
    fn canonical_input_contains_target_and_request() {
        let effect =
            EmbeddingGeneration::new("standalone.embedding_generation", request(None), target())
                .unwrap();
        let input = effect.canonical_input();
        assert!(input.get("binding_target").is_some());
        assert!(input.get("request").is_some());
    }

    #[test]
    fn durable_effect_contract_matches_the_locked_release_surface() {
        let declaration =
            obzenflow_runtime::effects::EffectDeclaration::at_least_once::<EmbeddingGeneration>();

        assert_eq!(
            EmbeddingGeneration::EFFECT_TYPE,
            "obzenflow.ai.embedding_generation"
        );
        assert_eq!(EmbeddingGeneration::SCHEMA_VERSION, 1);
        assert_eq!(
            EmbeddingGeneration::SAFETY,
            obzenflow_runtime::effects::EffectSafety::NonIdempotentAtLeastOnce
        );
        assert_eq!(
            declaration.outcome_kind,
            obzenflow_runtime::effects::EffectOutcomeKind::RecordedReply
        );
        assert!(declaration.public_outcome_fact_types.is_empty());
        assert_eq!(declaration.required_ports.len(), 1);
        assert_eq!(declaration.required_ports[0].name, EMBEDDING_CLIENT_PORT);
    }
}
