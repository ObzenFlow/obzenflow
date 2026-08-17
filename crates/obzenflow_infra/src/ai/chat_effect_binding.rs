// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deferred, single-target chat-effect binding.

use super::endpoint_identity::{
    bound_chat_target, default_ollama_base_url, default_openai_base_url, endpoint_has_credentials,
};
use super::resolve_estimator_for_model;
use crate::ai::rig::RigChatClient;
use obzenflow_adapters::ai::{
    ChatBindingEvidence, ChatBindingEvidenceBuildError, ChatCompletion, CHAT_CLIENT,
};
use obzenflow_core::ai::{AiProvider, ChatClient, ChatTarget};
use obzenflow_core::config::SecretRef;
use obzenflow_core::http_client::Url;
use obzenflow_runtime::effects::{
    EffectBinding, EffectBindingBuildError, EffectPortRegistrationError, EffectPortRegistry,
    EffectPortResolutionError, EffectPortResolver, EffectPortResolverWithMetadata,
    EffectRegistration, EffectRegistrationBuilder, LogicalEffectBindingName, ResolvedEffectPort,
};
use obzenflow_runtime::runtime_config::AiModelsConfig;
use std::sync::Arc;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum ChatEffectBindingError {
    #[error(
        "unsupported ai.models.provider (expected 'ollama', 'openai', or 'openai_compatible')"
    )]
    UnsupportedProvider,
    #[error("ai.models.model is required for the single-target ChatEffectBinding")]
    MissingModel,
    #[error("ai.models.base_url is required when ai.models.provider=openai_compatible")]
    MissingBaseUrl,
    #[error("invalid ai.models.base_url")]
    InvalidBaseUrl,
    #[error("AI effect binding endpoints must not contain URL credentials")]
    CredentialedBaseUrl,
    #[error(transparent)]
    InvalidEvidence(#[from] ChatBindingEvidenceBuildError),
    #[error(transparent)]
    InvalidRegistration(#[from] EffectBindingBuildError),
    #[error(transparent)]
    Installation(#[from] EffectPortRegistrationError),
}

#[derive(Clone)]
enum DeferredProvider {
    Ollama { base_url: Option<Url> },
    OpenAi { api_key: SecretRef },
    OpenAiCompatible { api_key: SecretRef, base_url: Url },
}

impl std::fmt::Debug for DeferredProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeferredProvider(<not disclosed>)")
    }
}

#[derive(Clone)]
enum DeferredChatAuthority {
    Provider(DeferredProvider),
    Resolver(EffectPortResolver<dyn ChatClient>),
}

impl std::fmt::Debug for DeferredChatAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("DeferredChatAuthority(<not disclosed>)")
    }
}

/// One immutable configuration decision waiting to be split into
/// credential-free contract evidence and opaque live registration authority.
pub struct ChatEffectBinding {
    evidence: ChatBindingEvidence,
    authority: DeferredChatAuthority,
}

impl std::fmt::Debug for ChatEffectBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ChatEffectBinding")
            .field("evidence", &"<not disclosed>")
            .field("registration", &"<opaque>")
            .finish()
    }
}

impl ChatEffectBinding {
    pub fn ollama(
        model: impl Into<String>,
        base_url: Option<Url>,
    ) -> Result<Self, ChatEffectBindingError> {
        let model = required_model(model.into())?;
        let endpoint = base_url.clone().unwrap_or_else(default_ollama_base_url);
        validate_endpoint(&endpoint)?;
        Self::new_bound(
            "ollama",
            model,
            endpoint,
            DeferredProvider::Ollama { base_url },
        )
    }

    pub fn openai(
        model: impl Into<String>,
        api_key: SecretRef,
    ) -> Result<Self, ChatEffectBindingError> {
        Self::new_bound(
            "openai",
            required_model(model.into())?,
            default_openai_base_url(),
            DeferredProvider::OpenAi { api_key },
        )
    }

    pub fn openai_compatible(
        model: impl Into<String>,
        api_key: SecretRef,
        base_url: Url,
    ) -> Result<Self, ChatEffectBindingError> {
        validate_endpoint(&base_url)?;
        Self::new_bound(
            "openai_compatible",
            required_model(model.into())?,
            base_url.clone(),
            DeferredProvider::OpenAiCompatible { api_key, base_url },
        )
    }

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

        if let Some(base_url) = &base_url {
            validate_endpoint(base_url)?;
        }

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
            _ => return Err(ChatEffectBindingError::UnsupportedProvider),
        };
        Self::new_bound(provider, model, endpoint, deferred)
    }

    /// Build an application-selected deferred chat binding.
    ///
    /// The facade derives the declaration evidence and snapshots the resolved
    /// client's observed target. Callers do not construct registration slots or
    /// metadata projections.
    pub fn from_resolver(
        target: ChatTarget,
        resolver: EffectPortResolver<dyn ChatClient>,
    ) -> Result<Self, ChatEffectBindingError> {
        let estimator = resolve_estimator_for_model(&target.model);
        Ok(Self {
            evidence: ChatBindingEvidence::new(target, estimator)?,
            authority: DeferredChatAuthority::Resolver(resolver),
        })
    }

    /// Install this facade-owned registration and return its lexical binding.
    pub fn install_into(
        self,
        effect_ports: &mut EffectPortRegistry,
    ) -> Result<EffectBinding<ChatCompletion>, ChatEffectBindingError> {
        let (binding, registration) = self.into_parts()?;
        effect_ports.install(registration)?;
        Ok(binding)
    }

    fn into_parts(
        self,
    ) -> Result<
        (
            EffectBinding<ChatCompletion>,
            EffectRegistration<ChatCompletion>,
        ),
        ChatEffectBindingError,
    > {
        let target = self.evidence.target().clone();
        let authority = Arc::new(self.authority);
        let resolver: EffectPortResolverWithMetadata<dyn ChatClient, ChatTarget> =
            Arc::new(move || {
                let client = resolve_client(&target, &authority)?;
                // Snapshot the resolved client's observed target. Reusing the
                // requested target here would make pre-boundary validation tautological.
                let metadata = Arc::new(client.target().clone());
                Ok(ResolvedEffectPort::new(client, metadata))
            });
        EffectRegistrationBuilder::<ChatCompletion>::new(
            LogicalEffectBindingName::new("chat")
                .expect("framework chat binding name is a valid public identifier"),
            self.evidence,
        )
        .bind_deferred_with_metadata(CHAT_CLIENT, resolver)?
        .finish()
        .map_err(Into::into)
    }

    fn new_bound(
        provider: impl Into<AiProvider>,
        model: String,
        endpoint: Url,
        deferred: DeferredProvider,
    ) -> Result<Self, ChatEffectBindingError> {
        let target = bound_chat_target(provider, model.clone(), &endpoint);
        let estimator = resolve_estimator_for_model(&model);
        Ok(Self {
            evidence: ChatBindingEvidence::new(target, estimator)?,
            authority: DeferredChatAuthority::Provider(deferred),
        })
    }
}

fn resolve_client(
    target: &ChatTarget,
    authority: &DeferredChatAuthority,
) -> Result<Arc<dyn ChatClient>, EffectPortResolutionError> {
    let provider = match authority {
        DeferredChatAuthority::Provider(provider) => provider,
        DeferredChatAuthority::Resolver(resolver) => return resolver(),
    };
    let client = match provider {
        DeferredProvider::Ollama { base_url } => {
            RigChatClient::ollama(target.model.clone(), base_url.clone())
        }
        DeferredProvider::OpenAi { api_key } => {
            let secret = api_key
                .resolve()
                .map_err(|_| EffectPortResolutionError::CredentialUnavailable)?;
            RigChatClient::openai(target.model.clone(), secret.expose())
        }
        DeferredProvider::OpenAiCompatible { api_key, base_url } => {
            let secret = api_key
                .resolve()
                .map_err(|_| EffectPortResolutionError::CredentialUnavailable)?;
            RigChatClient::openai_compatible(
                target.model.clone(),
                secret.expose(),
                base_url.clone(),
            )
        }
    }
    .map_err(|_| EffectPortResolutionError::ClientConstructionFailed)?;
    Ok(Arc::new(client))
}

fn parse_url(raw: &str) -> Result<Url, ChatEffectBindingError> {
    Url::parse(raw).map_err(|_| ChatEffectBindingError::InvalidBaseUrl)
}

fn required_model(model: String) -> Result<String, ChatEffectBindingError> {
    let model = model.trim();
    if model.is_empty() {
        Err(ChatEffectBindingError::MissingModel)
    } else {
        Ok(model.to_string())
    }
}

fn validate_endpoint(endpoint: &Url) -> Result<(), ChatEffectBindingError> {
    if endpoint_has_credentials(endpoint) {
        Err(ChatEffectBindingError::CredentialedBaseUrl)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::config::{
        ConfigScope, ConfigSource, ConfigSubject, ConfigValueMeta, SecretRef,
    };
    use obzenflow_runtime::effects::EffectPortRegistry;
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

    fn install(binding: ChatEffectBinding) -> EffectBinding<ChatCompletion> {
        let mut effect_ports = EffectPortRegistry::new();
        binding
            .install_into(&mut effect_ports)
            .expect("facade installation succeeds")
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
        let contract = install(binding);

        assert!(contract
            .evidence()
            .target()
            .logically_matches(&ChatTarget::new("openai_compatible", "fixture-model")));
        assert!(contract.evidence().target().binding_fingerprint.is_some());
        assert_eq!(
            contract.evidence().estimator().info().model,
            contract.evidence().target().model
        );
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

        let left = install(left);
        let equivalent = install(equivalent);
        let right = install(right);
        assert_eq!(left.evidence().target(), equivalent.evidence().target());
        assert_ne!(left.evidence().target(), right.evidence().target());

        let encoded = serde_json::to_string(left.evidence().target()).unwrap();
        assert!(!encoded.contains("127.0.0.1"));
        assert!(!encoded.contains("12345"));
    }

    #[test]
    fn clones_share_one_contract_family_but_equal_constructions_do_not() {
        let left = install(
            ChatEffectBinding::from_config(&config("ollama", Some("fixture-model"), None)).unwrap(),
        );
        let alias = left.clone();
        let equal_but_separate = install(
            ChatEffectBinding::from_config(&config("ollama", Some("fixture-model"), None)).unwrap(),
        );

        assert!(left.shares_construction_family(&alias));
        assert!(!left.shares_construction_family(&equal_but_separate));
        assert_eq!(
            left.evidence().target(),
            equal_but_separate.evidence().target()
        );
    }

    #[test]
    fn registration_installs_only_at_the_sealed_chat_coordinate() {
        let mut registry = EffectPortRegistry::new();
        ChatEffectBinding::from_config(&config("ollama", Some("fixture-model"), None))
            .unwrap()
            .install_into(&mut registry)
            .expect("first sealed registration succeeds");
    }

    #[test]
    fn programmatic_constructors_defer_secrets_and_reject_url_credentials() {
        ChatEffectBinding::openai(
            "fixture-model",
            SecretRef::new("FLOWIP_128B_MISSING_OPENAI_KEY"),
        )
        .expect("constructing a binding does not resolve its secret");

        let endpoint = Url::parse("https://user:password@example.com/v1").unwrap();
        let error = ChatEffectBinding::openai_compatible(
            "fixture-model",
            SecretRef::new("FLOWIP_128B_MISSING_COMPAT_KEY"),
            endpoint,
        )
        .unwrap_err();
        assert_eq!(error, ChatEffectBindingError::CredentialedBaseUrl);
    }
}
