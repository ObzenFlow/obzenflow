// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::endpoint_identity::{
    bound_embedding_target, default_ollama_base_url, default_openai_base_url,
};
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiClientError, AiProvider, EmbeddingClient, EmbeddingDimensions, EmbeddingRequest,
    EmbeddingResponse, EmbeddingTarget, Usage, UsageSource,
};
use reqwest::StatusCode;
use serde::Deserialize;
use serde_json::json;
use url::Url;

#[derive(Clone)]
enum NativeEmbeddingBackend {
    Ollama {
        client: reqwest::Client,
        endpoint: Url,
    },
    OpenAi {
        client: reqwest::Client,
        endpoint: Url,
        api_key: String,
    },
}

/// Native provider-wire implementation of the framework embedding port.
///
/// Rig 0.34 does not put explicit Ollama dimensions on the wire, so this
/// transport deliberately speaks the provider protocols directly. Requests
/// own dimensions and no adapter-local retry is used.
#[derive(Clone)]
pub struct NativeEmbeddingClient {
    target: EmbeddingTarget,
    backend: NativeEmbeddingBackend,
}

impl NativeEmbeddingClient {
    pub fn ollama(model: impl Into<String>, base_url: Option<Url>) -> Result<Self, AiClientError> {
        let model = model.into();
        let endpoint = base_url.unwrap_or_else(default_ollama_base_url);
        Ok(Self {
            target: bound_embedding_target("ollama", model, &endpoint),
            backend: NativeEmbeddingBackend::Ollama {
                client: build_http_client()?,
                endpoint,
            },
        })
    }

    pub fn openai(
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, AiClientError> {
        Self::openai_at(
            "openai",
            model.into(),
            api_key.into(),
            default_openai_base_url(),
        )
    }

    pub fn openai_compatible(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: Url,
    ) -> Result<Self, AiClientError> {
        Self::openai_at("openai_compatible", model.into(), api_key.into(), base_url)
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
    ) -> Result<Self, AiClientError> {
        Ok(Self {
            target: bound_embedding_target(provider, model, &endpoint),
            backend: NativeEmbeddingBackend::OpenAi {
                client: build_http_client()?,
                endpoint,
                api_key,
            },
        })
    }
}

#[async_trait]
impl EmbeddingClient for NativeEmbeddingClient {
    fn target(&self) -> &EmbeddingTarget {
        &self.target
    }

    async fn embed(&self, request: EmbeddingRequest) -> Result<EmbeddingResponse, AiClientError> {
        validate_request_target(&request, &self.target)?;
        if request.inputs.is_empty() {
            return Err(AiClientError::InvalidRequest {
                message: "embedding request requires at least one input".to_string(),
            });
        }

        match &self.backend {
            NativeEmbeddingBackend::Ollama { client, endpoint } => {
                embed_ollama(client, endpoint, &request).await
            }
            NativeEmbeddingBackend::OpenAi {
                client,
                endpoint,
                api_key,
            } => embed_openai(client, endpoint, api_key, &request).await,
        }
    }
}

fn build_http_client() -> Result<reqwest::Client, AiClientError> {
    reqwest::Client::builder()
        .build()
        .map_err(|error| AiClientError::Other {
            message: format!("embedding HTTP client construction failed: {error}"),
        })
}

async fn embed_ollama(
    client: &reqwest::Client,
    endpoint: &Url,
    request: &EmbeddingRequest,
) -> Result<EmbeddingResponse, AiClientError> {
    let url = provider_url(endpoint, "api/embed")?;
    let mut body = json!({
        "model": request.model,
        "input": request.inputs,
    });
    if let Some(dimensions) = request.params.dimensions {
        body["dimensions"] = json!(dimensions.get());
    }

    let response = client
        .post(url)
        .header("content-type", "application/json")
        .body(
            serde_json::to_vec(&body).map_err(|error| AiClientError::InvalidRequest {
                message: error.to_string(),
            })?,
        )
        .send()
        .await
        .map_err(map_reqwest_error)?;
    let bytes = checked_body(response).await?;
    let response: OllamaEmbeddingResponse =
        serde_json::from_slice(&bytes).map_err(|error| AiClientError::Remote {
            message: format!("invalid Ollama embedding response: {error}"),
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
    client: &reqwest::Client,
    endpoint: &Url,
    api_key: &str,
    request: &EmbeddingRequest,
) -> Result<EmbeddingResponse, AiClientError> {
    let url = provider_url(endpoint, "embeddings")?;
    let mut body = json!({
        "model": request.model,
        "input": request.inputs,
        "encoding_format": "float",
    });
    if let Some(dimensions) = request.params.dimensions {
        body["dimensions"] = json!(dimensions.get());
    }

    let response = client
        .post(url)
        .bearer_auth(api_key)
        .header("content-type", "application/json")
        .body(
            serde_json::to_vec(&body).map_err(|error| AiClientError::InvalidRequest {
                message: error.to_string(),
            })?,
        )
        .send()
        .await
        .map_err(map_reqwest_error)?;
    let bytes = checked_body(response).await?;
    let response: OpenAiEmbeddingResponse =
        serde_json::from_slice(&bytes).map_err(|error| AiClientError::Remote {
            message: format!("invalid OpenAI embedding response: {error}"),
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

fn normalise_vectors(
    inputs: &[String],
    requested_dimensions: Option<EmbeddingDimensions>,
    vectors: Vec<Vec<f32>>,
    usage: Option<Usage>,
) -> Result<EmbeddingResponse, AiClientError> {
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
) -> Result<(), AiClientError> {
    if !request.logically_targets(target) {
        return Err(AiClientError::InvalidRequest {
            message: format!(
                "embedding request target '{}/{}' does not match bound target '{}'",
                request.provider, request.model, target
            ),
        });
    }
    Ok(())
}

fn provider_url(endpoint: &Url, route: &str) -> Result<Url, AiClientError> {
    let mut base = endpoint.clone();
    let mut path = base.path().trim_end_matches('/').to_string();
    path.push('/');
    base.set_path(&path);
    base.join(route)
        .map_err(|error| AiClientError::InvalidRequest {
            message: format!("invalid embedding endpoint: {error}"),
        })
}

async fn checked_body(response: reqwest::Response) -> Result<Vec<u8>, AiClientError> {
    let status = response.status();
    let bytes = response.bytes().await.map_err(map_reqwest_error)?;
    if status.is_success() {
        return Ok(bytes.to_vec());
    }

    let mut message = String::from_utf8_lossy(&bytes).into_owned();
    message.truncate(message.floor_char_boundary(2_048));
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => Err(AiClientError::Auth { message }),
        StatusCode::TOO_MANY_REQUESTS => Err(AiClientError::RateLimited {
            message,
            retry_after: None,
        }),
        status if status.is_client_error() => Err(AiClientError::InvalidRequest { message }),
        _ => Err(AiClientError::Remote { message }),
    }
}

fn map_reqwest_error(error: reqwest::Error) -> AiClientError {
    if error.is_timeout() {
        AiClientError::Timeout {
            message: error.to_string(),
        }
    } else {
        AiClientError::Remote {
            message: error.to_string(),
        }
    }
}

fn invalid_provider_response(message: impl Into<String>) -> AiClientError {
    AiClientError::Remote {
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
