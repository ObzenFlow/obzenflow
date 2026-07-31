// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deferred, single-target chat-effect binding.

use super::endpoint_identity::{
    bound_chat_target, default_ollama_base_url, default_openai_base_url,
};
use super::resolve_estimator_for_model;
use crate::ai::rig::RigChatClient;
use obzenflow_core::ai::{
    AiProvider, ChatBindingContract, ChatBindingContractError, ChatClient, ChatTarget,
    CHAT_CLIENT_PORT,
};
use obzenflow_core::config::SecretRef;
use obzenflow_core::http_client::Url;
use obzenflow_runtime::effects::{
    EffectPortRegistrationError, EffectPortRegistry, EffectPortResolutionError,
    EffectPortResolveFuture, EffectPortResolver,
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
    #[error(transparent)]
    InvalidContract(#[from] ChatBindingContractError),
}

#[derive(Debug, Clone)]
enum DeferredProvider {
    Ollama { base_url: Option<Url> },
    OpenAi { api_key: SecretRef },
    OpenAiCompatible { api_key: SecretRef, base_url: Url },
}

/// One immutable configuration decision waiting to be split into
/// credential-free contract evidence and opaque live registration authority.
pub struct ChatEffectBinding {
    contract: ChatBindingContract,
    provider: DeferredProvider,
}

impl std::fmt::Debug for ChatEffectBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChatEffectBinding")
            .field("contract", &self.contract)
            .field("registration", &"<opaque>")
            .finish()
    }
}

/// Opaque, consuming authority to install the single live chat resolver.
pub struct ChatEffectRegistration {
    target: ChatTarget,
    provider: DeferredProvider,
}

impl std::fmt::Debug for ChatEffectRegistration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChatEffectRegistration")
            .field("authority", &"<opaque>")
            .finish()
    }
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

        let (deferred, endpoint) = match provider.as_str() {
            "ollama" => {
                let endpoint = base_url.clone().unwrap_or_else(default_ollama_base_url);
                (DeferredProvider::Ollama { base_url }, endpoint)
            }
            "openai" => (
                DeferredProvider::OpenAi {
                    api_key: config.api_key_env.value.clone(),
                },
                default_openai_base_url(),
            ),
            "openai_compatible" => {
                let endpoint = base_url.ok_or(ChatEffectBindingError::MissingBaseUrl)?;
                (
                    DeferredProvider::OpenAiCompatible {
                        api_key: config.api_key_env.value.clone(),
                        base_url: endpoint.clone(),
                    },
                    endpoint,
                )
            }
            _ => {
                return Err(ChatEffectBindingError::UnsupportedProvider {
                    provider: provider.clone(),
                })
            }
        };
        let target = bound_chat_target(AiProvider::new(provider), model.clone(), &endpoint);
        let estimator = resolve_estimator_for_model(&model);
        Ok(Self {
            contract: ChatBindingContract::from_resolved(target, estimator)?,
            provider: deferred,
        })
    }

    pub fn into_parts(self) -> (ChatBindingContract, ChatEffectRegistration) {
        let target = self.contract.target().clone();
        (
            self.contract,
            ChatEffectRegistration {
                target,
                provider: self.provider,
            },
        )
    }
}

impl ChatEffectRegistration {
    pub fn install_into(
        self,
        registry: EffectPortRegistry,
    ) -> Result<EffectPortRegistry, EffectPortRegistrationError> {
        registry.with_deferred::<dyn ChatClient>(CHAT_CLIENT_PORT, self.into_resolver())
    }

    fn into_resolver(self) -> EffectPortResolver<dyn ChatClient> {
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
        let (contract, _registration) = binding.into_parts();

        assert!(contract
            .target()
            .logically_matches(&ChatTarget::new("openai_compatible", "fixture-model")));
        assert!(contract.target().binding_fingerprint.is_some());
        assert_eq!(contract.estimator().info().model, contract.target().model);
    }

    #[test]
    fn compatible_endpoint_is_part_of_the_non_secret_binding_identity() {
        let left = ChatEffectBinding::from_config(&config(
            "openai_compatible",
            Some("fixture-model"),
            Some("http://127.0.0.1:12345/v1"),
        ))
        .unwrap();
        let equivalent = ChatEffectBinding::from_config(&config(
            "openai_compatible",
            Some("fixture-model"),
            Some("http://127.0.0.1:12345/v1/"),
        ))
        .unwrap();
        let right = ChatEffectBinding::from_config(&config(
            "openai_compatible",
            Some("fixture-model"),
            Some("http://127.0.0.1:54321/v1"),
        ))
        .unwrap();

        let (left, _) = left.into_parts();
        let (equivalent, _) = equivalent.into_parts();
        let (right, _) = right.into_parts();
        assert_eq!(left.target(), equivalent.target());
        assert_ne!(left.target(), right.target());

        let encoded = serde_json::to_string(left.target()).unwrap();
        assert!(!encoded.contains("127.0.0.1"));
        assert!(!encoded.contains("12345"));
    }

    #[test]
    fn clones_share_one_contract_family_but_equal_constructions_do_not() {
        let (left, _) =
            ChatEffectBinding::from_config(&config("ollama", Some("fixture-model"), None))
                .unwrap()
                .into_parts();
        let alias = left.clone();
        let (equal_but_separate, _) =
            ChatEffectBinding::from_config(&config("ollama", Some("fixture-model"), None))
                .unwrap()
                .into_parts();

        assert!(left.shares_construction_origin(&alias));
        assert!(!left.shares_construction_origin(&equal_but_separate));
        assert_eq!(left.target(), equal_but_separate.target());
    }

    #[test]
    fn registration_installs_only_at_the_sealed_chat_coordinate() {
        let (_, registration) =
            ChatEffectBinding::from_config(&config("ollama", Some("fixture-model"), None))
                .unwrap()
                .into_parts();

        let registry = registration
            .install_into(EffectPortRegistry::new())
            .expect("first sealed registration succeeds");
        let requirement = obzenflow_runtime::effects::EffectPortRequirement::of::<dyn ChatClient>(
            CHAT_CLIENT_PORT,
        );
        assert!(registry.contains_requirement(&requirement));
    }
}
