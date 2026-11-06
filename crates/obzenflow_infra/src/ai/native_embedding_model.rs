// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::endpoint_identity::{
    bound_embedding_target, default_ollama_base_url, default_openai_base_url,
};
use crate::ai::provider_errors::{unclaimed_status, unreadable_body, ModelCalls};
use crate::http_client::ReqwestHttpClient;
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiProvider, AiProviderError, EmbeddingDimensions, EmbeddingModel, EmbeddingRequest,
    EmbeddingResponse, EmbeddingTarget, Usage, UsageSource,
};
use obzenflow_core::http_client::{Bytes, HttpClient, HttpRequest};
use obzenflow_runtime::runtime_config::AI_EMBEDDINGS_SECTION;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

#[derive(Clone)]
enum NativeEmbeddingBackend {
    Ollama { endpoint: Url },
    OpenAi { endpoint: Url, api_key: String },
}

/// Native provider-wire implementation of the framework embedding port.
///
/// This model speaks the provider protocols directly, so requests own their
/// dimensions, and each call is one send through the framework's client.
#[derive(Clone)]
pub struct NativeEmbeddingModel {
    target: EmbeddingTarget,
    backend: NativeEmbeddingBackend,
    client: Arc<dyn HttpClient>,
    calls: ModelCalls,
}

impl NativeEmbeddingModel {
    /// An Ollama-backed model whose calls each carry `request_timeout`.
    pub fn ollama(
        model: impl Into<String>,
        base_url: Option<Url>,
        request_timeout: Duration,
    ) -> Self {
        let endpoint = base_url.unwrap_or_else(default_ollama_base_url);
        Self::through(
            bound_embedding_target("ollama", model.into(), &endpoint),
            NativeEmbeddingBackend::Ollama { endpoint },
            Arc::new(ReqwestHttpClient::new()),
            request_timeout,
        )
    }

    /// An OpenAI-backed model at the default OpenAI base URL.
    pub fn openai(
        model: impl Into<String>,
        api_key: impl Into<String>,
        request_timeout: Duration,
    ) -> Self {
        Self::openai_at(
            "openai",
            model.into(),
            api_key.into(),
            default_openai_base_url(),
            request_timeout,
        )
    }

    /// An OpenAI-compatible model at `base_url`.
    pub fn openai_compatible(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: Url,
        request_timeout: Duration,
    ) -> Self {
        Self::openai_at(
            "openai_compatible",
            model.into(),
            api_key.into(),
            base_url,
            request_timeout,
        )
    }

    pub fn provider(&self) -> &AiProvider {
        &self.target.provider
    }

    pub fn model(&self) -> &str {
        &self.target.model
    }

    fn openai_at(
        provider: &'static str,
        model: String,
        api_key: String,
        endpoint: Url,
        request_timeout: Duration,
    ) -> Self {
        Self::through(
            bound_embedding_target(provider, model, &endpoint),
            NativeEmbeddingBackend::OpenAi { endpoint, api_key },
            Arc::new(ReqwestHttpClient::new()),
            request_timeout,
        )
    }

    fn through(
        target: EmbeddingTarget,
        backend: NativeEmbeddingBackend,
        client: Arc<dyn HttpClient>,
        request_timeout: Duration,
    ) -> Self {
        Self {
            target,
            backend,
            client,
            calls: ModelCalls {
                source: "embedding_model",
                section: AI_EMBEDDINGS_SECTION,
                request_timeout,
            },
        }
    }

    async fn send(&self, request: HttpRequest) -> Result<Bytes, AiProviderError> {
        let response = self
            .client
            .send(request.with_timeout(self.calls.request_timeout))
            .await
            .map_err(|error| self.calls.unsent(error))?;
        match response.status {
            200 => Ok(response.body),
            _ => Err(unclaimed_status(&response)),
        }
    }

    async fn embed_ollama(
        &self,
        endpoint: &Url,
        request: &EmbeddingRequest,
    ) -> Result<EmbeddingResponse, AiProviderError> {
        let url = provider_url(endpoint, "api/embed")?;
        let mut body = json!({
            "model": request.model,
            "input": request.inputs,
        });
        if let Some(dimensions) = request.params.dimensions {
            body["dimensions"] = json!(dimensions.get());
        }

        let bytes = self.send(HttpRequest::post(url).with_json(&body)).await?;
        let response: OllamaEmbeddingResponse =
            serde_json::from_slice(&bytes).map_err(|error| {
                unreadable_body(format!("invalid Ollama embedding response: {error}"))
            })?;

        normalise_vectors(
            &request.inputs,
            request.params.dimensions,
            response.embeddings,
            response.prompt_eval_count.map(|input_tokens| Usage {
                source: UsageSource::Provider,
                input_tokens,
                output_tokens: 0,
                total_tokens: input_tokens,
            }),
        )
    }

    async fn embed_openai(
        &self,
        endpoint: &Url,
        api_key: &str,
        request: &EmbeddingRequest,
    ) -> Result<EmbeddingResponse, AiProviderError> {
        let url = provider_url(endpoint, "embeddings")?;
        let mut body = json!({
            "model": request.model,
            "input": request.inputs,
            "encoding_format": "float",
        });
        if let Some(dimensions) = request.params.dimensions {
            body["dimensions"] = json!(dimensions.get());
        }

        let bytes = self
            .send(
                HttpRequest::post(url)
                    .with_bearer_token(api_key)
                    .with_json(&body),
            )
            .await?;
        let response: OpenAiEmbeddingResponse =
            serde_json::from_slice(&bytes).map_err(|error| {
                unreadable_body(format!("invalid OpenAI embedding response: {error}"))
            })?;

        if response.data.len() != request.inputs.len() {
            return Err(invalid_provider_response(format!(
                "embedding response cardinality mismatch: expected {}, observed {}",
                request.inputs.len(),
                response.data.len()
            )));
        }
        for (position, item) in response.data.iter().enumerate() {
            if item.index != position {
                return Err(invalid_provider_response(format!(
                    "embedding response order mismatch at position {position}: observed index {}",
                    item.index
                )));
            }
        }

        let vectors = response
            .data
            .into_iter()
            .map(|item| item.embedding)
            .collect();
        let usage = response.usage.map(|usage| Usage {
            source: UsageSource::Provider,
            input_tokens: usage.prompt_tokens,
            output_tokens: 0,
            total_tokens: usage.total_tokens,
        });
        normalise_vectors(&request.inputs, request.params.dimensions, vectors, usage)
    }
}

#[async_trait]
impl EmbeddingModel for NativeEmbeddingModel {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, AiProviderError> {
        validate_request_target(&request, &self.target)?;
        if request.inputs.is_empty() {
            return Err(AiProviderError::InvalidRequest {
                message: "embedding request requires at least one input".to_string(),
            });
        }

        match &self.backend {
            NativeEmbeddingBackend::Ollama { endpoint } => {
                self.embed_ollama(endpoint, &request).await
            }
            NativeEmbeddingBackend::OpenAi { endpoint, api_key } => {
                self.embed_openai(endpoint, api_key, &request).await
            }
        }
    }
}

fn normalise_vectors(
    inputs: &[String],
    requested_dimensions: Option<EmbeddingDimensions>,
    vectors: Vec<Vec<f32>>,
    usage: Option<Usage>,
) -> Result<EmbeddingResponse, AiProviderError> {
    if vectors.len() != inputs.len() {
        return Err(invalid_provider_response(format!(
            "embedding response cardinality mismatch: expected {}, observed {}",
            inputs.len(),
            vectors.len()
        )));
    }
    let width = vectors.first().map(Vec::len).unwrap_or_default();
    let width = u32::try_from(width)
        .ok()
        .and_then(|width| EmbeddingDimensions::try_from(width).ok())
        .ok_or_else(|| {
            invalid_provider_response("embedding response vectors must have non-zero width")
        })?;
    if vectors
        .iter()
        .any(|vector| vector.len() != width.get() as usize)
    {
        return Err(invalid_provider_response(
            "embedding response vectors must have one common width",
        ));
    }
    if let Some(requested) = requested_dimensions {
        if requested != width {
            return Err(invalid_provider_response(format!(
                "embedding response width mismatch: requested {requested}, observed {width}"
            )));
        }
    }

    Ok(EmbeddingResponse {
        vectors,
        vector_dim: width,
        usage,
    })
}

fn validate_request_target(
    request: &EmbeddingRequest,
    target: &EmbeddingTarget,
) -> Result<(), AiProviderError> {
    if !request.logically_targets(target) {
        return Err(AiProviderError::InvalidRequest {
            message: format!(
                "embedding request target '{}/{}' does not match bound target '{}'",
                request.provider, request.model, target
            ),
        });
    }
    Ok(())
}

fn provider_url(endpoint: &Url, route: &str) -> Result<Url, AiProviderError> {
    let mut base = endpoint.clone();
    let mut path = base.path().trim_end_matches('/').to_string();
    path.push('/');
    base.set_path(&path);
    base.join(route)
        .map_err(|error| AiProviderError::InvalidRequest {
            message: format!("invalid embedding endpoint: {error}"),
        })
}

fn invalid_provider_response(message: impl Into<String>) -> AiProviderError {
    AiProviderError::Remote {
        message: message.into(),
    }
}

#[derive(Debug, Deserialize)]
struct OllamaEmbeddingResponse {
    embeddings: Vec<Vec<f32>>,
    #[serde(default)]
    prompt_eval_count: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingResponse {
    data: Vec<OpenAiEmbeddingData>,
    #[serde(default)]
    usage: Option<OpenAiEmbeddingUsage>,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingData {
    embedding: Vec<f32>,
    index: usize,
}

#[derive(Debug, Deserialize)]
struct OpenAiEmbeddingUsage {
    prompt_tokens: u64,
    total_tokens: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{AiProvider, EmbeddingParams};

    #[test]
    fn validates_provider_and_model_match() {
        let target =
            bound_embedding_target("ollama", "nomic-embed-text", &default_ollama_base_url());
        let request = EmbeddingRequest {
            provider: AiProvider::new("ollama"),
            model: "nomic-embed-text".to_string(),
            inputs: vec!["hello".to_string()],
            params: EmbeddingParams::default(),
        };
        validate_request_target(&request, &target).unwrap();
    }

    #[test]
    fn rejects_zero_mixed_and_requested_width_mismatch() {
        let requested = EmbeddingDimensions::try_from(2).unwrap();
        assert!(normalise_vectors(&["a".into()], None, vec![vec![]], None).is_err());
        assert!(normalise_vectors(
            &["a".into(), "b".into()],
            None,
            vec![vec![1.0], vec![2.0, 3.0]],
            None
        )
        .is_err());
        assert!(normalise_vectors(&["a".into()], Some(requested), vec![vec![1.0]], None).is_err());
    }
}
