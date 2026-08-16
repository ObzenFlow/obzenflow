// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Deferred, single-target embedding-effect binding.

use super::endpoint_identity::{
    bound_embedding_target, default_ollama_base_url, default_openai_base_url,
    endpoint_has_credentials,
};
use crate::ai::NativeEmbeddingClient;
use obzenflow_adapters::ai::{
    EmbeddingBindingEvidence, EmbeddingBindingEvidenceBuildError, EmbeddingGeneration,
    EMBEDDING_CLIENT,
};
use obzenflow_core::ai::{AiProvider, EmbeddingClient, EmbeddingTarget};
use obzenflow_core::config::SecretRef;
use obzenflow_core::http_client::Url;
use obzenflow_runtime::effects::{
    EffectBinding, EffectBindingBuildError, EffectPortResolutionError, EffectPortResolver,
    EffectRegistration, EffectRegistrationBuilder, LogicalEffectBindingName,
};
use obzenflow_runtime::runtime_config::AiModelsConfig;
use std::sync::Arc;

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum EmbeddingEffectBindingError {
    #[error(
        "unsupported ai.models.provider (expected 'ollama', 'openai', or 'openai_compatible')"
    )]
    UnsupportedProvider,
    #[error("ai.models.model is required for the single-target EmbeddingEffectBinding")]
    MissingModel,
    #[error("ai.models.base_url is required when ai.models.provider=openai_compatible")]
    MissingBaseUrl,
    #[error("invalid ai.models.base_url")]
    InvalidBaseUrl,
    #[error("AI effect binding endpoints must not contain URL credentials")]
    CredentialedBaseUrl,
    #[error(transparent)]
    InvalidEvidence(#[from] EmbeddingBindingEvidenceBuildError),
    #[error(transparent)]
    InvalidRegistration(#[from] EffectBindingBuildError),
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

/// One immutable embedding decision split later into contract evidence and
/// opaque live registration authority.
pub struct EmbeddingEffectBinding {
    evidence: EmbeddingBindingEvidence,
    provider: DeferredProvider,
}

impl std::fmt::Debug for EmbeddingEffectBinding {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EmbeddingEffectBinding")
            .field("evidence", &"<not disclosed>")
            .field("registration", &"<opaque>")
            .finish()
    }
}

impl EmbeddingEffectBinding {
    pub fn ollama(
        model: impl Into<String>,
        base_url: Option<Url>,
    ) -> Result<Self, EmbeddingEffectBindingError> {
        let model = required_model(model.into())?;
        let endpoint = base_url.clone().unwrap_or_else(default_ollama_base_url);
        validate_endpoint(&endpoint)?;
        Self::new_bound(
            "ollama",
            model,
            &endpoint,
            DeferredProvider::Ollama { base_url },
        )
    }

    pub fn openai(
        model: impl Into<String>,
        api_key: SecretRef,
    ) -> Result<Self, EmbeddingEffectBindingError> {
        let model = required_model(model.into())?;
        Self::new_bound(
            "openai",
            model,
            &default_openai_base_url(),
            DeferredProvider::OpenAi { api_key },
        )
    }

    pub fn openai_compatible(
        model: impl Into<String>,
        api_key: SecretRef,
        base_url: Url,
    ) -> Result<Self, EmbeddingEffectBindingError> {
        validate_endpoint(&base_url)?;
        Self::new_bound(
            "openai_compatible",
            required_model(model.into())?,
            &base_url,
            DeferredProvider::OpenAiCompatible {
                api_key,
                base_url: base_url.clone(),
            },
        )
    }

    pub fn from_config(config: &AiModelsConfig) -> Result<Self, EmbeddingEffectBindingError> {
        let provider = config.provider.value.trim().to_ascii_lowercase();
        let model = config
            .model
            .as_ref()
            .map(|resolved| resolved.value.clone())
            .ok_or(EmbeddingEffectBindingError::MissingModel)?;
        let base_url = config
            .base_url
            .as_ref()
            .map(|resolved| parse_url(&resolved.value))
            .transpose()?;

        if let Some(base_url) = &base_url {
            validate_endpoint(base_url)?;
        }

        match provider.as_str() {
            "ollama" => Self::ollama(model, base_url),
            "openai" => Self::openai(model, config.api_key_env.value.clone()),
            "openai_compatible" => Self::openai_compatible(
                model,
                config.api_key_env.value.clone(),
                base_url.ok_or(EmbeddingEffectBindingError::MissingBaseUrl)?,
            ),
            _ => Err(EmbeddingEffectBindingError::UnsupportedProvider),
        }
    }

    pub fn into_parts(
        self,
    ) -> Result<
        (
            EffectBinding<EmbeddingGeneration>,
            EffectRegistration<EmbeddingGeneration>,
        ),
        EmbeddingEffectBindingError,
    > {
        let target = self.evidence.target().clone();
        let provider = Arc::new(self.provider);
        let resolver: EffectPortResolver<dyn EmbeddingClient> =
            Arc::new(move || resolve_client(&target, &provider));
        EffectRegistrationBuilder::<EmbeddingGeneration>::new(
            LogicalEffectBindingName::new("embedding")
                .expect("framework embedding binding name is a valid public identifier"),
            self.evidence,
        )
        .bind_deferred(EMBEDDING_CLIENT, resolver)?
        .finish()
        .map_err(Into::into)
    }

    fn new_bound(
        provider: impl Into<AiProvider>,
        model: String,
        endpoint: &Url,
        deferred: DeferredProvider,
    ) -> Result<Self, EmbeddingEffectBindingError> {
        Ok(Self {
            evidence: EmbeddingBindingEvidence::new(bound_embedding_target(
                provider, model, endpoint,
            ))?,
            provider: deferred,
        })
    }
}

fn resolve_client(
    target: &EmbeddingTarget,
    provider: &DeferredProvider,
) -> Result<Arc<dyn EmbeddingClient>, EffectPortResolutionError> {
    let client = match provider {
        DeferredProvider::Ollama { base_url } => {
            NativeEmbeddingClient::ollama(target.model.clone(), base_url.clone())
        }
        DeferredProvider::OpenAi { api_key } => {
            let secret = api_key
                .resolve()
                .map_err(|_| EffectPortResolutionError::CredentialUnavailable)?;
            NativeEmbeddingClient::openai(target.model.clone(), secret.expose())
        }
        DeferredProvider::OpenAiCompatible { api_key, base_url } => {
            let secret = api_key
                .resolve()
                .map_err(|_| EffectPortResolutionError::CredentialUnavailable)?;
            NativeEmbeddingClient::openai_compatible(
                target.model.clone(),
                secret.expose(),
                base_url.clone(),
            )
        }
    }
    .map_err(|_| EffectPortResolutionError::ClientConstructionFailed)?;
    Ok(Arc::new(client))
}

fn required_model(model: String) -> Result<String, EmbeddingEffectBindingError> {
    let model = model.trim();
    if model.is_empty() {
        Err(EmbeddingEffectBindingError::MissingModel)
    } else {
        Ok(model.to_string())
    }
}

fn parse_url(raw: &str) -> Result<Url, EmbeddingEffectBindingError> {
    Url::parse(raw).map_err(|_| EmbeddingEffectBindingError::InvalidBaseUrl)
}

fn validate_endpoint(endpoint: &Url) -> Result<(), EmbeddingEffectBindingError> {
    if endpoint_has_credentials(endpoint) {
        Err(EmbeddingEffectBindingError::CredentialedBaseUrl)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::config::SecretRef;
    use obzenflow_runtime::effects::EffectPortRegistry;

    #[test]
    fn ollama_is_a_native_fingerprinted_binding() {
        let (contract, registration) = EmbeddingEffectBinding::ollama("nomic-embed-text", None)
            .unwrap()
            .into_parts()
            .unwrap();
        assert_eq!(contract.evidence().target().provider.as_str(), "ollama");
        assert_eq!(contract.evidence().target().model, "nomic-embed-text");
        let mut registry = EffectPortRegistry::new();
        registry
            .install(registration)
            .expect("sealed registration succeeds");
    }

    #[test]
    fn credentialed_endpoint_is_rejected_before_it_can_be_hashed() {
        let endpoint = Url::parse("https://user:password@example.com/v1").unwrap();
        let error = EmbeddingEffectBinding::ollama("model", Some(endpoint)).unwrap_err();
        assert_eq!(error, EmbeddingEffectBindingError::CredentialedBaseUrl);
    }

    #[test]
    fn hosted_construction_and_registration_defer_and_hide_the_secret() {
        let secret_name = "FLOWIP_128B_MISSING_EMBEDDING_KEY";
        let (contract, registration) =
            EmbeddingEffectBinding::openai("text-embedding", SecretRef::new(secret_name))
                .expect("binding construction does not resolve its secret")
                .into_parts()
                .unwrap();

        let encoded_target = serde_json::to_string(contract.evidence().target()).unwrap();
        assert!(!encoded_target.contains(secret_name));
        let mut registry = EffectPortRegistry::new();
        registry
            .install(registration)
            .expect("deferred registration does not resolve its secret");

        let endpoint = Url::parse("https://user:password@example.com/v1").unwrap();
        let error = EmbeddingEffectBinding::openai_compatible(
            "text-embedding",
            SecretRef::new(secret_name),
            endpoint,
        )
        .unwrap_err();
        assert_eq!(error, EmbeddingEffectBindingError::CredentialedBaseUrl);
    }
}
