// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! AI model configuration: named endpoints and each AI stage's model.

use super::error::ConfigResolveError;
use super::model::{Resolved, ResolvedRuntimeConfig};
use super::resolve::{resolve_at, ResolutionPoint};
use super::schema::knob;
use super::ConfigValue;
use obzenflow_core::ai::{AiEndpoint, AiProvider};
use obzenflow_core::config::SecretRef;
use obzenflow_core::event::EffectType;
use obzenflow_core::http_client::Url;
use obzenflow_core::StageKey;
use std::collections::BTreeMap;

/// The chat family's configuration section.
pub const AI_MODELS_SECTION: &str = "ai.models";
/// The embedding family's configuration section.
pub const AI_EMBEDDINGS_SECTION: &str = "ai.embeddings";
/// The open-key limit of a keyed `ai!` stage.
pub const MAX_OPEN_KEYS_KEY: &str = "runtime.ai.max_open_keys";
/// The open-key limit when configuration sets none.
pub const DEFAULT_MAX_OPEN_KEYS: u64 = 1_000;
/// Endpoint names that need no table and that no table may reuse.
pub const BUILTIN_AI_ENDPOINTS: &[&str] = &["ollama", "openai"];

const OPENAI_DEFAULT_CREDENTIAL: &str = "OPENAI_API_KEY";

/// One `[ai.endpoints.<name>]` table: a provider, its URL and the name of
/// its credential, always read together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiEndpointTable {
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub api_key_env: Option<String>,
}

/// Every endpoint table a configuration defines.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AiEndpointTables {
    tables: BTreeMap<String, AiEndpoint>,
}

/// Why AI configuration cannot bind a stage.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AiConfigError {
    #[error("[ai.endpoints.{name}] has no provider; an endpoint states its provider with its URL and credential")]
    EndpointWithoutProvider { name: String },
    #[error("[ai.endpoints.{name}] reuses a built-in endpoint name; choose another name")]
    ReservedEndpointName { name: String },
    #[error("[ai.endpoints.{name}] is not a valid endpoint name; use letters, digits, '_', '.' or '-'")]
    InvalidEndpointName { name: String },
    #[error("[ai.endpoints.{name}] base_url is not a URL: {detail}")]
    InvalidBaseUrl { name: String, detail: String },
    #[error("[ai.endpoints.{name}] base_url must not carry credentials; name the credential with api_key_env")]
    CredentialedBaseUrl { name: String },
    #[error("[ai.endpoints.{name}] sets base_url, but provider `openai` has a fixed URL; use provider `openai_compatible` for another URL")]
    FixedUrlProvider { name: String },
    #[error("[ai.endpoints.{name}] api_key_env must name an environment variable")]
    InvalidCredentialName { name: String },
    #[error("config: stage `{stage}` names endpoint `{endpoint}`, which is not defined; define [ai.endpoints.{endpoint}] or use a built-in endpoint (ollama, openai)")]
    UnknownEndpoint { stage: String, endpoint: String },
    #[error("stage `{stage}`: no model configured for {effect_type}; set {section}.model, or model under [{section}.stages.{stage}]")]
    MissingModel {
        stage: String,
        effect_type: String,
        section: String,
    },
    #[error(transparent)]
    Resolve(#[from] ConfigResolveError),
}

fn builtin(name: &str) -> Option<AiEndpoint> {
    match name {
        "ollama" => Some(AiEndpoint {
            name: name.to_string(),
            provider: AiProvider::new("ollama"),
            base_url: None,
            api_key_env: None,
        }),
        "openai" => Some(AiEndpoint {
            name: name.to_string(),
            provider: AiProvider::new("openai"),
            base_url: None,
            api_key_env: SecretRef::new(OPENAI_DEFAULT_CREDENTIAL).ok(),
        }),
        _ => None,
    }
}

fn valid_endpoint_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 64
        && bytes[0].is_ascii_alphanumeric()
        && bytes[1..]
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

impl AiEndpointTables {
    /// Admit one endpoint table, refusing reserved names, a missing
    /// provider and a URL that carries credentials.
    pub fn admit(&mut self, name: &str, table: AiEndpointTable) -> Result<(), AiConfigError> {
        let name_owned = name.to_string();
        if BUILTIN_AI_ENDPOINTS.contains(&name) {
            return Err(AiConfigError::ReservedEndpointName { name: name_owned });
        }
        if !valid_endpoint_name(name) {
            return Err(AiConfigError::InvalidEndpointName { name: name_owned });
        }
        let provider = table
            .provider
            .filter(|provider| !provider.trim().is_empty())
            .ok_or_else(|| AiConfigError::EndpointWithoutProvider {
                name: name_owned.clone(),
            })?;
        let provider = AiProvider::new(provider);
        let base_url = table
            .base_url
            .map(|raw| {
                Url::parse(&raw).map_err(|error| AiConfigError::InvalidBaseUrl {
                    name: name_owned.clone(),
                    detail: error.to_string(),
                })
            })
            .transpose()?;
        if let Some(url) = &base_url {
            if !url.username().is_empty() || url.password().is_some() {
                return Err(AiConfigError::CredentialedBaseUrl { name: name_owned });
            }
            if provider.as_str() == "openai" {
                return Err(AiConfigError::FixedUrlProvider { name: name_owned });
            }
        }
        let api_key_env = match table.api_key_env {
            Some(env) => Some(SecretRef::new(env).map_err(|_| {
                AiConfigError::InvalidCredentialName {
                    name: name_owned.clone(),
                }
            })?),
            None if provider.as_str() == "openai" => SecretRef::new(OPENAI_DEFAULT_CREDENTIAL).ok(),
            None => None,
        };
        self.tables.insert(
            name_owned.clone(),
            AiEndpoint {
                name: name_owned,
                provider,
                base_url,
                api_key_env,
            },
        );
        Ok(())
    }

    /// The endpoint a stage names, built in or defined by a table.
    pub fn resolve(&self, stage: &str, name: &str) -> Result<AiEndpoint, AiConfigError> {
        builtin(name)
            .or_else(|| self.tables.get(name).cloned())
            .ok_or_else(|| AiConfigError::UnknownEndpoint {
                stage: stage.to_string(),
                endpoint: name.to_string(),
            })
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }
}

/// One AI stage's endpoint and model for one effect, with the configuration
/// that chose each.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedAiModel {
    pub endpoint: AiEndpoint,
    pub model: String,
    pub endpoint_choice: Resolved<ConfigValue>,
    pub model_choice: Resolved<ConfigValue>,
}

impl ResolvedRuntimeConfig {
    /// Resolve one AI stage's endpoint and model for one effect through the
    /// stage, flow and global ladder of the effect family's section.
    pub fn resolve_ai_model(
        &self,
        section: &str,
        stage: &str,
        effect_type: &str,
    ) -> Result<ResolvedAiModel, AiConfigError> {
        let point = ResolutionPoint::Effect {
            stage: StageKey::from(stage),
            effect_type: EffectType::from(effect_type),
        };
        let resolve = |key: String| -> Result<Option<Resolved<ConfigValue>>, AiConfigError> {
            let spec = knob(&key).ok_or(ConfigResolveError::UnknownKnob { key_path: key })?;
            Ok(resolve_at(spec, &point, self.candidates())?)
        };
        let model_choice = resolve(format!("{section}.model"))?
            .filter(|resolved| {
                resolved
                    .value
                    .as_text()
                    .is_some_and(|model| !model.trim().is_empty())
            })
            .ok_or_else(|| AiConfigError::MissingModel {
                stage: stage.to_string(),
                effect_type: effect_type.to_string(),
                section: section.to_string(),
            })?;
        let endpoint_choice = resolve(format!("{section}.endpoint"))?.ok_or_else(|| {
            AiConfigError::Resolve(ConfigResolveError::UnknownKnob {
                key_path: format!("{section}.endpoint"),
            })
        })?;
        let endpoint_name = endpoint_choice.value.as_text().unwrap_or_default().trim();
        let endpoint = self.ai_endpoints().resolve(stage, endpoint_name)?;
        let model = model_choice
            .value
            .as_text()
            .unwrap_or_default()
            .trim()
            .to_string();
        Ok(ResolvedAiModel {
            endpoint,
            model,
            endpoint_choice,
            model_choice,
        })
    }

    /// The open-key limit at a keyed AI stage's point.
    pub fn resolve_max_open_keys(&self, stage: &str) -> Result<u64, ConfigResolveError> {
        Ok(self
            .resolve_stage_value(MAX_OPEN_KEYS_KEY, stage)?
            .and_then(|resolved| resolved.value.as_u64())
            .unwrap_or(DEFAULT_MAX_OPEN_KEYS))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::{CandidateSet, ScopedCandidate};
    use obzenflow_core::config::{ConfigAddress, ConfigScope, ConfigSource};

    fn admit(set: &mut CandidateSet, key: &str, address: ConfigAddress, value: &str) {
        set.admit(ScopedCandidate {
            key_path: key.to_string(),
            address,
            source: ConfigSource::File,
            value: ConfigValue::Text(value.to_string()),
        })
        .unwrap();
    }

    fn config(set: CandidateSet, tables: AiEndpointTables) -> ResolvedRuntimeConfig {
        ResolvedRuntimeConfig::new(set).with_ai_endpoints(tables)
    }

    #[test]
    fn exact_effect_over_stage_over_flow_over_global() {
        let mut set = CandidateSet::default();
        let chat = "obzenflow.ai.chat_completion";
        admit(
            &mut set,
            "ai.models.model",
            ConfigAddress::unqualified(ConfigScope::Global),
            "global",
        );
        admit(
            &mut set,
            "ai.models.model",
            ConfigAddress::unqualified(ConfigScope::Flow),
            "flow",
        );
        admit(
            &mut set,
            "ai.models.model",
            ConfigAddress::unqualified(ConfigScope::stage("digest")),
            "stage",
        );
        admit(
            &mut set,
            "ai.models.model",
            ConfigAddress::effect(StageKey::from("digest"), EffectType::from(chat)),
            "exact",
        );
        let config = config(set, AiEndpointTables::default());
        let model = |stage: &str| {
            config
                .resolve_ai_model(AI_MODELS_SECTION, stage, chat)
                .unwrap()
                .model
        };
        assert_eq!(model("digest"), "exact");
        assert_eq!(model("other"), "flow");
    }

    #[test]
    fn a_stage_without_a_model_names_itself_and_its_section() {
        let config = config(CandidateSet::default(), AiEndpointTables::default());
        let error = config
            .resolve_ai_model(AI_MODELS_SECTION, "digest", "obzenflow.ai.chat_completion")
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "stage `digest`: no model configured for obzenflow.ai.chat_completion; set ai.models.model, or model under [ai.models.stages.digest]"
        );
    }

    #[test]
    fn built_in_and_named_endpoints_resolve_whole() {
        let mut tables = AiEndpointTables::default();
        tables
            .admit(
                "groq",
                AiEndpointTable {
                    provider: Some("openai_compatible".to_string()),
                    base_url: Some("https://api.groq.com/openai/v1".to_string()),
                    api_key_env: Some("GROQ_API_KEY".to_string()),
                },
            )
            .unwrap();
        let groq = tables.resolve("digest", "groq").unwrap();
        assert_eq!(groq.provider.as_str(), "openai_compatible");
        assert_eq!(
            groq.api_key_env.as_ref().map(SecretRef::env_name),
            Some("GROQ_API_KEY")
        );

        let ollama = tables.resolve("digest", "ollama").unwrap();
        assert_eq!(ollama.provider.as_str(), "ollama");
        assert!(ollama.api_key_env.is_none());

        let openai = tables.resolve("digest", "openai").unwrap();
        assert_eq!(
            openai.api_key_env.as_ref().map(SecretRef::env_name),
            Some("OPENAI_API_KEY")
        );
    }

    #[test]
    fn a_compatible_endpoint_never_inherits_the_openai_credential() {
        let mut tables = AiEndpointTables::default();
        tables
            .admit(
                "local",
                AiEndpointTable {
                    provider: Some("openai_compatible".to_string()),
                    base_url: Some("http://127.0.0.1:8080/v1".to_string()),
                    api_key_env: None,
                },
            )
            .unwrap();
        assert!(tables
            .resolve("digest", "local")
            .unwrap()
            .api_key_env
            .is_none());
    }

    #[test]
    fn endpoint_tables_refuse_reserved_names_and_missing_providers() {
        let mut tables = AiEndpointTables::default();
        assert_eq!(
            tables
                .admit(
                    "ollama",
                    AiEndpointTable {
                        provider: Some("ollama".to_string()),
                        base_url: None,
                        api_key_env: None,
                    },
                )
                .unwrap_err()
                .to_string(),
            "[ai.endpoints.ollama] reuses a built-in endpoint name; choose another name"
        );
        assert_eq!(
            tables
                .admit(
                    "groq",
                    AiEndpointTable {
                        provider: None,
                        base_url: Some("https://api.groq.com/openai/v1".to_string()),
                        api_key_env: Some("GROQ_API_KEY".to_string()),
                    },
                )
                .unwrap_err()
                .to_string(),
            "[ai.endpoints.groq] has no provider; an endpoint states its provider with its URL and credential"
        );
        assert_eq!(
            tables.resolve("digest", "grok").unwrap_err().to_string(),
            "config: stage `digest` names endpoint `grok`, which is not defined; define [ai.endpoints.grok] or use a built-in endpoint (ollama, openai)"
        );
    }
}
