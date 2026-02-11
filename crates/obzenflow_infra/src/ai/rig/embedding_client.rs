// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::error_mapping::map_rig_error;
use super::preflight::{preflight_ollama, preflight_openai_models};
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiClientError, AiProvider, EmbeddingClient, EmbeddingRequest, EmbeddingResponse,
};
use rig_rs::client::{EmbeddingsClient, Nothing};
use rig_rs::embeddings::EmbeddingModel;
use rig_rs::providers::{ollama, openai};
use std::sync::Arc;
use url::Url;

const DEFAULT_OLLAMA_BASE_URL: &str = "http://localhost:11434/";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1/";

#[derive(Clone)]
enum RigEmbeddingBackend {
    Ollama {
        client: Arc<ollama::Client>,
        dimensions: Option<usize>,
    },
    OpenAi {
        client: Arc<openai::Client>,
        dimensions: Option<usize>,
    },
}

/// Rig-backed implementation of `EmbeddingClient`.
///
/// Embedding vector dimensionality is configured at client construction time.
/// Per-request `EmbeddingParams.dimensions` is ignored (FLOWIP-086r D13).
#[derive(Clone)]
pub struct RigEmbeddingClient {
    provider: AiProvider,
    model: String,
    backend: RigEmbeddingBackend,
}

impl RigEmbeddingClient {
    pub fn ollama(
        model: impl Into<String>,
        base_url: Option<Url>,
        dimensions: Option<usize>,
    ) -> Result<Self, AiClientError> {
        let client = match base_url {
            None => ollama::Client::new(Nothing).map_err(|err| AiClientError::InvalidRequest {
                message: err.to_string(),
            })?,
            Some(url) => ollama::Client::builder()
                .api_key(Nothing)
                // rig-core's Provider::build_uri appends a trailing `/` to `base_url`.
                // `url::Url::as_str()` includes `/` for the root path, which would otherwise
                // produce `//api/...` and can trigger redirects/method changes on some servers.
                .base_url(url.as_str().trim_end_matches('/'))
                .build()
                .map_err(|err| AiClientError::InvalidRequest {
                    message: err.to_string(),
                })?,
        };

        Ok(Self {
            provider: AiProvider::new("ollama"),
            model: model.into(),
            backend: RigEmbeddingBackend::Ollama {
                client: Arc::new(client),
                dimensions,
            },
        })
    }

    /// Create an Ollama-backed embedding client and fail fast if the provider/model is not available.
    ///
    /// This performs a lightweight preflight call to the Ollama server (no inference)
    /// and verifies the requested model exists in `/api/tags`.
    pub async fn ollama_checked(
        model: impl Into<String>,
        base_url: Option<Url>,
        dimensions: Option<usize>,
    ) -> Result<Self, AiClientError> {
        let model = model.into();
        let base_url = base_url.unwrap_or_else(|| {
            Url::parse(DEFAULT_OLLAMA_BASE_URL).expect("default ollama base url parses")
        });

        preflight_ollama(&base_url, Some(model.as_str())).await?;
        Self::ollama(model, Some(base_url), dimensions)
    }

    pub fn openai_compatible(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: Url,
        dimensions: Option<usize>,
    ) -> Result<Self, AiClientError> {
        let client = openai::Client::builder()
            .api_key(api_key.into())
            // See note in `ollama()` about trailing slashes.
            .base_url(base_url.as_str().trim_end_matches('/'))
            .build()
            .map_err(|err| AiClientError::InvalidRequest {
                message: err.to_string(),
            })?;

        Ok(Self {
            provider: AiProvider::new("openai"),
            model: model.into(),
            backend: RigEmbeddingBackend::OpenAi {
                client: Arc::new(client),
                dimensions,
            },
        })
    }

    /// Create an OpenAI-compatible-backed embedding client and fail fast if the endpoint is not reachable.
    ///
    /// This preflights `GET /models` against the supplied base URL (no inference).
    pub async fn openai_compatible_checked(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: Url,
        dimensions: Option<usize>,
    ) -> Result<Self, AiClientError> {
        let model = model.into();
        let api_key = api_key.into();

        preflight_openai_models(&base_url, api_key.as_str(), Some(model.as_str())).await?;
        Self::openai_compatible(model, api_key, base_url, dimensions)
    }

    pub fn openai(
        model: impl Into<String>,
        api_key: impl Into<String>,
        dimensions: Option<usize>,
    ) -> Result<Self, AiClientError> {
        let client =
            openai::Client::new(api_key.into()).map_err(|err| AiClientError::InvalidRequest {
                message: err.to_string(),
            })?;

        Ok(Self {
            provider: AiProvider::new("openai"),
            model: model.into(),
            backend: RigEmbeddingBackend::OpenAi {
                client: Arc::new(client),
                dimensions,
            },
        })
    }

    /// Create an OpenAI-backed embedding client and fail fast if the endpoint is not reachable.
    ///
    /// This preflights `GET /models` against the default OpenAI base URL (no inference).
    pub async fn openai_checked(
        model: impl Into<String>,
        api_key: impl Into<String>,
        dimensions: Option<usize>,
    ) -> Result<Self, AiClientError> {
        let model = model.into();
        let api_key = api_key.into();
        let base_url = Url::parse(DEFAULT_OPENAI_BASE_URL).expect("default openai base url parses");

        preflight_openai_models(&base_url, api_key.as_str(), Some(model.as_str())).await?;
        Self::openai(model, api_key, dimensions)
    }

    pub fn provider(&self) -> &AiProvider {
        &self.provider
    }

    pub fn model(&self) -> &str {
        &self.model
    }
}

#[async_trait]
impl EmbeddingClient for RigEmbeddingClient {
    async fn embed(&self, req: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        validate_request_target(&req, &self.provider, &self.model)?;

        if req.inputs.is_empty() {
            return Err(AiClientError::InvalidRequest {
                message: "embedding request requires at least one input".to_string(),
            });
        }

        if !req.params.extras.is_empty() {
            tracing::warn!(
                extras_len = req.params.extras.len(),
                "embedding params extras are ignored by rig"
            );
        }

        let (embeddings, vector_dim) = match &self.backend {
            RigEmbeddingBackend::Ollama { client, dimensions } => {
                let model = match dimensions {
                    Some(ndims) => client.embedding_model_with_ndims(self.model.as_str(), *ndims),
                    None => client.embedding_model(self.model.as_str()),
                };
                let ndims = model.ndims();
                let embeddings = model
                    .embed_texts(req.inputs.clone())
                    .await
                    .map_err(map_rig_error)?;
                (embeddings, ndims)
            }
            RigEmbeddingBackend::OpenAi { client, dimensions } => {
                let model = match dimensions {
                    Some(ndims) => client.embedding_model_with_ndims(self.model.as_str(), *ndims),
                    None => client.embedding_model(self.model.as_str()),
                };
                let ndims = model.ndims();
                let embeddings = model
                    .embed_texts(req.inputs.clone())
                    .await
                    .map_err(map_rig_error)?;
                (embeddings, ndims)
            }
        };

        let vectors = embeddings
            .into_iter()
            .map(|embedding| embedding.vec.into_iter().map(|v| v as f32).collect())
            .collect::<Vec<Vec<f32>>>();

        Ok(EmbeddingResponse {
            vectors,
            vector_dim,
            usage: None,
            raw: None,
        })
    }
}

fn validate_request_target(
    req: &EmbeddingRequest,
    provider: &AiProvider,
    model: &str,
) -> Result<(), AiClientError> {
    if req.provider != *provider {
        return Err(AiClientError::InvalidRequest {
            message: format!(
                "request provider '{}' does not match RigEmbeddingClient provider '{}'",
                req.provider.as_str(),
                provider.as_str()
            ),
        });
    }
    if req.model != model {
        return Err(AiClientError::InvalidRequest {
            message: format!(
                "request model '{}' does not match RigEmbeddingClient model '{}'",
                req.model, model
            ),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::EmbeddingParams;

    #[test]
    fn validates_provider_and_model_match() {
        let req = EmbeddingRequest {
            provider: AiProvider::new("openai"),
            model: "text-embedding-3-small".to_string(),
            inputs: vec!["hello".to_string()],
            params: EmbeddingParams::default(),
        };

        validate_request_target(&req, &AiProvider::new("openai"), "text-embedding-3-small")
            .expect("should accept matching provider+model");
    }

    #[test]
    fn rejects_mismatched_provider() {
        let req = EmbeddingRequest {
            provider: AiProvider::new("ollama"),
            model: "nomic-embed-text".to_string(),
            inputs: vec!["hello".to_string()],
            params: EmbeddingParams::default(),
        };

        let err = validate_request_target(&req, &AiProvider::new("openai"), "nomic-embed-text")
            .expect_err("should reject mismatched provider");

        assert!(matches!(err, AiClientError::InvalidRequest { .. }));
    }
}
