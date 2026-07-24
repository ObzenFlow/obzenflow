// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deferred, single-target chat-effect binding.

use super::resolve_estimator_for_model;
use crate::ai::rig::RigChatClient;
use obzenflow_core::ai::{AiProvider, ChatClient, ChatTarget, ResolvedTokenEstimator};
use obzenflow_core::config::SecretRef;
use obzenflow_core::http_client::Url;
use obzenflow_runtime::effects::{
    EffectPortResolutionError, EffectPortResolveFuture, EffectPortResolver,
};
use obzenflow_runtime::runtime_config::AiModelsConfig;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ChatEffectBindingError {
    #[error(
        "unsupported ai.models.provider='{provider}' (expected 'ollama', 'openai', or 'openai_compatible')"
    )]
    UnsupportedProvider { provider: String },
    #[error("ai.models.model is required for the single-target ChatEffectBinding")]
    MissingModel,
    #[error("ai.models.base_url is required when ai.models.provider=openai_compatible")]
    MissingBaseUrl,
    #[error("invalid ai.models.base_url: {message}")]
    InvalidBaseUrl { message: String },
}

#[derive(Debug, Clone)]
enum DeferredProvider {
    Ollama { base_url: Option<Url> },
    OpenAi { api_key: SecretRef },
    OpenAiCompatible { api_key: SecretRef, base_url: Url },
}

/// Credential-free build authority for one immutable chat provider/model.
///
/// Construction validates only local configuration. `into_resolver()` reads a
/// secret and constructs the unchecked Rig client on the first executable
/// effect continuation; it performs no provider or model preflight.
#[derive(Debug, Clone)]
pub struct ChatEffectBinding {
    target: ChatTarget,
    estimator: ResolvedTokenEstimator,
    provider: DeferredProvider,
}

impl ChatEffectBinding {
    pub fn from_config(config: &AiModelsConfig) -> Result<Self, ChatEffectBindingError> {
        let provider = config.provider.value.trim().to_ascii_lowercase();
        let model = config
            .model
            .as_ref()
            .map(|resolved| resolved.value.trim())
            .filter(|model| !model.is_empty())
            .ok_or(ChatEffectBindingError::MissingModel)?
            .to_string();
        let base_url = config
            .base_url
            .as_ref()
            .map(|resolved| parse_url(&resolved.value))
            .transpose()?;

        let deferred = match provider.as_str() {
            "ollama" => DeferredProvider::Ollama { base_url },
            "openai" => DeferredProvider::OpenAi {
                api_key: config.api_key_env.value.clone(),
            },
            "openai_compatible" => DeferredProvider::OpenAiCompatible {
                api_key: config.api_key_env.value.clone(),
                base_url: base_url.ok_or(ChatEffectBindingError::MissingBaseUrl)?,
            },
            _ => {
                return Err(ChatEffectBindingError::UnsupportedProvider {
                    provider: provider.clone(),
                })
            }
        };
        let target = ChatTarget::new(AiProvider::new(provider), model.clone());
        let estimator = resolve_estimator_for_model(&model);
        Ok(Self {
            target,
            estimator,
            provider: deferred,
        })
    }

    pub fn target(&self) -> &ChatTarget {
        &self.target
    }

    pub fn resolved_estimator(&self) -> &ResolvedTokenEstimator {
        &self.estimator
    }

    pub fn into_resolver(self) -> EffectPortResolver<dyn ChatClient> {
        let binding = Arc::new(self);
        Arc::new(move || {
            let binding = Arc::clone(&binding);
            Box::pin(async move { binding.resolve_client() })
                as EffectPortResolveFuture<dyn ChatClient>
        })
    }

    fn resolve_client(&self) -> Result<Arc<dyn ChatClient>, EffectPortResolutionError> {
        let result =
            catch_unwind(AssertUnwindSafe(|| match &self.provider {
                DeferredProvider::Ollama { base_url } => {
                    RigChatClient::ollama(self.target.model.clone(), base_url.clone())
                }
                DeferredProvider::OpenAi { api_key } => {
                    let secret = api_key.resolve().map_err(|error| {
                        obzenflow_core::ai::AiClientError::Auth {
                            message: error.to_string(),
                        }
                    })?;
                    RigChatClient::openai(self.target.model.clone(), secret.expose())
                }
                DeferredProvider::OpenAiCompatible { api_key, base_url } => {
                    let secret = api_key.resolve().map_err(|error| {
                        obzenflow_core::ai::AiClientError::Auth {
                            message: error.to_string(),
                        }
                    })?;
                    RigChatClient::openai_compatible(
                        self.target.model.clone(),
                        secret.expose(),
                        base_url.clone(),
                    )
                }
            }));

        match result {
            Ok(Ok(client)) => Ok(Arc::new(client)),
            Ok(Err(error)) => Err(EffectPortResolutionError::failed(error.to_string())),
            Err(_) => Err(EffectPortResolutionError::failed(
                "Rig chat client construction panicked",
            )),
        }
    }
}

fn parse_url(raw: &str) -> Result<Url, ChatEffectBindingError> {
    Url::parse(raw).map_err(|error| ChatEffectBindingError::InvalidBaseUrl {
        message: error.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::config::{
        ConfigScope, ConfigSource, ConfigSubject, ConfigValueMeta, SecretRef,
    };
    use obzenflow_runtime::runtime_config::Resolved;

    fn resolved<T>(key_path: &str, value: T) -> Resolved<T> {
        Resolved {
            value,
            meta: ConfigValueMeta {
                key_path: key_path.to_string(),
                source: ConfigSource::File,
                scope: ConfigScope::Global,
                subject: ConfigSubject::Unqualified,
            },
        }
    }

    fn config(provider: &str, model: Option<&str>, base_url: Option<&str>) -> AiModelsConfig {
        AiModelsConfig {
            provider: resolved("ai.models.provider", provider.to_string()),
            model: model.map(|model| resolved("ai.models.model", model.to_string())),
            base_url: base_url.map(|base_url| resolved("ai.models.base_url", base_url.to_string())),
            api_key_env: resolved(
                "ai.models.api_key_env",
                SecretRef::new("FLOWIP_128G_TEST_API_KEY"),
            ),
        }
    }

    #[test]
    fn generated_chat_binding_requires_an_explicit_model() {
        let error = ChatEffectBinding::from_config(&config("ollama", None, None))
            .expect_err("the generated single-target binding has no provider-model default");
        assert_eq!(error, ChatEffectBindingError::MissingModel);
        assert_eq!(
            error.to_string(),
            "ai.models.model is required for the single-target ChatEffectBinding"
        );
    }

    #[test]
    fn non_ollama_binding_target_and_estimator_share_one_model() {
        let binding = ChatEffectBinding::from_config(&config(
            "openai_compatible",
            Some("fixture-model"),
            Some("http://127.0.0.1:12345/v1"),
        ))
        .expect("local non-secret binding construction succeeds");

        assert_eq!(
            binding.target(),
            &ChatTarget::new("openai_compatible", "fixture-model")
        );
        assert_eq!(
            binding.resolved_estimator().info().model,
            binding.target().model
        );
    }
}
