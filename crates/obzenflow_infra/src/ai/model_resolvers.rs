// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::endpoint_identity::{
    bound_chat_target, bound_embedding_target, default_ollama_base_url, default_openai_base_url,
    endpoint_has_credentials,
};
use super::resolve_estimator_for_model;
use crate::ai::rig::RigChatModel;
use crate::ai::NativeEmbeddingModel;
use obzenflow_core::ai::{
    AiEndpoint, AiModelSpec, AiProvider, AiResolutionError, ChatBindingFingerprint, ChatModel,
    ChatModelDescription, ChatModelResolver, ChatTarget, EmbeddingBindingFingerprint,
    EmbeddingModel, EmbeddingModelDescription, EmbeddingModelResolver, EmbeddingTarget,
    ResolvedTokenEstimator,
};
use obzenflow_core::http_client::Url;
use std::sync::Arc;
use std::time::Duration;

const OLLAMA: &str = "ollama";
const OPENAI: &str = "openai";
const OPENAI_COMPATIBLE: &str = "openai_compatible";

/// Chat models on `ollama`, `openai` and `openai_compatible` endpoints,
/// served by rig.
#[derive(Debug, Clone, Copy, Default)]
pub struct RigChatModelResolver;

/// Embedding models on `ollama`, `openai` and `openai_compatible` endpoints,
/// served over each provider's own wire protocol.
#[derive(Debug, Clone, Copy, Default)]
pub struct NativeEmbeddingModelResolver;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Wire {
    Ollama,
    OpenAi,
    OpenAiCompatible,
}

fn wire(provider: &AiProvider) -> Option<Wire> {
    match provider.as_str() {
        OLLAMA => Some(Wire::Ollama),
        OPENAI => Some(Wire::OpenAi),
        OPENAI_COMPATIBLE => Some(Wire::OpenAiCompatible),
        _ => None,
    }
}

fn served_wire(endpoint: &AiEndpoint, family: &'static str) -> Result<Wire, AiResolutionError> {
    wire(&endpoint.provider).ok_or_else(|| AiResolutionError::UnsupportedProvider {
        provider: endpoint.provider.to_string(),
        family,
    })
}

fn endpoint_url(endpoint: &AiEndpoint, wire: Wire) -> Result<Url, AiResolutionError> {
    let url = match (wire, &endpoint.base_url) {
        (Wire::Ollama, base_url) => base_url.clone().unwrap_or_else(default_ollama_base_url),
        (Wire::OpenAi, _) => default_openai_base_url(),
        (Wire::OpenAiCompatible, Some(base_url)) => base_url.clone(),
        (Wire::OpenAiCompatible, None) => {
            return Err(AiResolutionError::MissingBaseUrl {
                endpoint: endpoint.name.clone(),
                provider: endpoint.provider.to_string(),
            })
        }
    };
    if endpoint_has_credentials(&url) {
        return Err(AiResolutionError::CredentialedBaseUrl {
            endpoint: endpoint.name.clone(),
        });
    }
    Ok(url)
}

fn credential(endpoint: &AiEndpoint) -> Result<String, AiResolutionError> {
    match &endpoint.api_key_env {
        None => Ok(String::new()),
        Some(reference) => reference
            .resolve()
            .map(|secret| secret.expose().to_string())
            .map_err(|_| AiResolutionError::CredentialUnavailable {
                endpoint: endpoint.name.clone(),
                env: reference.env_name().to_string(),
            }),
    }
}

fn recorded_error(spec: &AiModelSpec, detail: &str) -> AiResolutionError {
    AiResolutionError::Recorded {
        spec: spec.to_string(),
        detail: detail.to_string(),
    }
}

impl ChatModelResolver for RigChatModelResolver {
    fn serves(&self, provider: &AiProvider) -> bool {
        wire(provider).is_some()
    }

    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<ChatModelDescription, AiResolutionError> {
        let url = endpoint_url(endpoint, served_wire(endpoint, "chat")?)?;
        Ok(ChatModelDescription {
            target: bound_chat_target(endpoint.provider.clone(), model, &url),
            estimator: resolve_estimator_for_model(model),
        })
    }

    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<ChatModelDescription, AiResolutionError> {
        if spec.binding_fingerprint.is_empty() {
            return Err(recorded_error(spec, "no binding fingerprint was recorded"));
        }
        let info = spec
            .estimator
            .clone()
            .ok_or_else(|| recorded_error(spec, "no token estimator was recorded"))?;
        Ok(ChatModelDescription {
            target: ChatTarget::with_binding_fingerprint(
                spec.provider.clone(),
                spec.model.clone(),
                ChatBindingFingerprint::new(spec.binding_fingerprint.clone()),
            ),
            estimator: ResolvedTokenEstimator::new(
                resolve_estimator_for_model(&spec.model).estimator(),
                info,
            ),
        })
    }

    fn connect(
        &self,
        endpoint: &AiEndpoint,
        description: &ChatModelDescription,
        request_timeout: Duration,
    ) -> Result<Arc<dyn ChatModel>, AiResolutionError> {
        let wire = served_wire(endpoint, "chat")?;
        let url = endpoint_url(endpoint, wire)?;
        let model = description.target.model.clone();
        let chat = match wire {
            Wire::Ollama => RigChatModel::ollama(model, endpoint.base_url.clone(), request_timeout),
            Wire::OpenAi => RigChatModel::openai(model, credential(endpoint)?, request_timeout),
            Wire::OpenAiCompatible => {
                RigChatModel::openai_compatible(model, credential(endpoint)?, url, request_timeout)
            }
        };
        Ok(Arc::new(chat))
    }
}

impl EmbeddingModelResolver for NativeEmbeddingModelResolver {
    fn serves(&self, provider: &AiProvider) -> bool {
        wire(provider).is_some()
    }

    fn describe(
        &self,
        endpoint: &AiEndpoint,
        model: &str,
    ) -> Result<EmbeddingModelDescription, AiResolutionError> {
        let url = endpoint_url(endpoint, served_wire(endpoint, "embeddings")?)?;
        Ok(EmbeddingModelDescription {
            target: bound_embedding_target(endpoint.provider.clone(), model, &url),
        })
    }

    fn describe_recorded(
        &self,
        spec: &AiModelSpec,
    ) -> Result<EmbeddingModelDescription, AiResolutionError> {
        if spec.binding_fingerprint.is_empty() {
            return Err(recorded_error(spec, "no binding fingerprint was recorded"));
        }
        Ok(EmbeddingModelDescription {
            target: EmbeddingTarget::new(
                spec.provider.clone(),
                spec.model.clone(),
                EmbeddingBindingFingerprint::new(spec.binding_fingerprint.clone()),
            ),
        })
    }

    fn connect(
        &self,
        endpoint: &AiEndpoint,
        description: &EmbeddingModelDescription,
        request_timeout: Duration,
    ) -> Result<Arc<dyn EmbeddingModel>, AiResolutionError> {
        let wire = served_wire(endpoint, "embeddings")?;
        let url = endpoint_url(endpoint, wire)?;
        let model = description.target.model.clone();
        let embedding = match wire {
            Wire::Ollama => {
                NativeEmbeddingModel::ollama(model, endpoint.base_url.clone(), request_timeout)
            }
            Wire::OpenAi => {
                NativeEmbeddingModel::openai(model, credential(endpoint)?, request_timeout)
            }
            Wire::OpenAiCompatible => NativeEmbeddingModel::openai_compatible(
                model,
                credential(endpoint)?,
                url,
                request_timeout,
            ),
        };
        Ok(Arc::new(embedding))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::config::SecretRef;

    fn endpoint(
        name: &str,
        provider: &str,
        base_url: Option<&str>,
        key: Option<&str>,
    ) -> AiEndpoint {
        AiEndpoint {
            name: name.to_string(),
            provider: AiProvider::from(provider),
            base_url: base_url.map(|url| Url::parse(url).unwrap()),
            api_key_env: key.map(|env| SecretRef::new(env).unwrap()),
        }
    }

    #[test]
    fn a_compatible_endpoint_needs_its_own_url() {
        let error = RigChatModelResolver
            .describe(&endpoint("groq", OPENAI_COMPATIBLE, None, None), "m")
            .unwrap_err();
        assert_eq!(
            error,
            AiResolutionError::MissingBaseUrl {
                endpoint: "groq".to_string(),
                provider: OPENAI_COMPATIBLE.to_string(),
            }
        );
    }

    #[test]
    fn endpoints_on_different_urls_bind_different_targets() {
        let left = RigChatModelResolver
            .describe(
                &endpoint("a", OPENAI_COMPATIBLE, Some("http://127.0.0.1:1/v1"), None),
                "m",
            )
            .unwrap();
        let right = RigChatModelResolver
            .describe(
                &endpoint("b", OPENAI_COMPATIBLE, Some("http://127.0.0.1:2/v1"), None),
                "m",
            )
            .unwrap();
        assert_ne!(left.target, right.target);
        let encoded = serde_json::to_string(&left.target).unwrap();
        assert!(!encoded.contains("127.0.0.1"));
    }

    #[test]
    fn a_compatible_endpoint_without_a_credential_reads_no_environment() {
        assert_eq!(
            credential(&endpoint(
                "local",
                OPENAI_COMPATIBLE,
                Some("http://127.0.0.1:1/v1"),
                None
            ))
            .unwrap(),
            ""
        );
    }

    #[test]
    fn a_missing_credential_names_its_endpoint_and_variable() {
        let error = credential(&endpoint(
            "groq",
            OPENAI_COMPATIBLE,
            Some("http://127.0.0.1:1/v1"),
            Some("OBZENFLOW_TEST_UNSET_GROQ_KEY"),
        ))
        .unwrap_err();
        assert_eq!(
            error.to_string(),
            "endpoint `groq` cannot read its credential from OBZENFLOW_TEST_UNSET_GROQ_KEY"
        );
    }

    #[test]
    fn a_recorded_spec_rebuilds_the_described_target() {
        let described = RigChatModelResolver
            .describe(&endpoint(OLLAMA, OLLAMA, None, None), "llama3.1:8b")
            .unwrap();
        let spec = AiModelSpec {
            effect_type: "obzenflow.ai.chat_completion".to_string(),
            endpoint: OLLAMA.to_string(),
            provider: described.target.provider.clone(),
            model: described.target.model.clone(),
            binding_fingerprint: described
                .target
                .binding_fingerprint
                .as_ref()
                .unwrap()
                .as_str()
                .to_string(),
            estimator: Some(described.estimator.info().clone()),
        };
        let recorded = RigChatModelResolver.describe_recorded(&spec).unwrap();
        assert_eq!(recorded.target, described.target);
        assert_eq!(recorded.estimator.info(), described.estimator.info());
    }

    #[test]
    fn unknown_providers_are_not_served() {
        assert!(!RigChatModelResolver.serves(&AiProvider::from("jev")));
        assert!(!NativeEmbeddingModelResolver.serves(&AiProvider::from("jev")));
        assert!(RigChatModelResolver.serves(&AiProvider::from(OLLAMA)));
    }
}
