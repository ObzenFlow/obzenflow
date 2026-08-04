// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Configuration-only model profile facade.
//!
//! Provider invocation moved to effect bindings in FLOWIP-128b. This type
//! remains for profile and estimator inspection, but deliberately has no
//! transform-builder or chat terminal.

use super::resolve_chat_model_profile;
use anyhow::anyhow;
use obzenflow_core::ai::{ChatModelProfile, ResolvedTokenEstimator, TokenCount, TokenEstimator};
use obzenflow_core::http_client::Url;
use std::sync::Arc;

const ENV_OPENAI_API_KEY: &str = "OPENAI_API_KEY";
const ENV_OPENAI_BASE_URL: &str = "OPENAI_BASE_URL";
const ENV_OLLAMA_BASE_URL: &str = "OLLAMA_BASE_URL";

const DEFAULT_PROVIDER: &str = "ollama";
const DEFAULT_MODEL_OLLAMA: &str = "llama3.1:8b";
const DEFAULT_MODEL_OPENAI: &str = "gpt-4.1-mini";
const DEFAULT_MODEL_OPENAI_COMPATIBLE: &str = "llama3.1:8b";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProviderKind {
    Ollama,
    OpenAi,
    OpenAiCompatible,
}

impl ProviderKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Ollama => "ollama",
            Self::OpenAi => "openai",
            Self::OpenAiCompatible => "openai_compatible",
        }
    }

    fn default_model(self) -> &'static str {
        match self {
            Self::Ollama => DEFAULT_MODEL_OLLAMA,
            Self::OpenAi => DEFAULT_MODEL_OPENAI,
            Self::OpenAiCompatible => DEFAULT_MODEL_OPENAI_COMPATIBLE,
        }
    }
}

#[derive(Debug, Clone)]
enum ProviderConfig {
    Ollama { base_url: Option<String> },
    OpenAi,
    OpenAiCompatible { base_url: String },
}

#[derive(Debug, Clone)]
pub struct ModelConfig {
    profile: ChatModelProfile,
    provider: ProviderConfig,
}

impl ModelConfig {
    pub fn ollama(model: impl Into<String>) -> Self {
        let profile = resolve_chat_model_profile(&model.into());
        Self {
            profile,
            provider: ProviderConfig::Ollama { base_url: None },
        }
    }

    pub fn openai(model: impl Into<String>, _api_key: impl Into<String>) -> Self {
        let profile = resolve_chat_model_profile(&model.into());
        Self {
            profile,
            provider: ProviderConfig::OpenAi,
        }
    }

    pub fn openai_compatible(
        model: impl Into<String>,
        _api_key: impl Into<String>,
        base_url: impl Into<String>,
    ) -> Self {
        let profile = resolve_chat_model_profile(&model.into());
        Self {
            profile,
            provider: ProviderConfig::OpenAiCompatible {
                base_url: base_url.into(),
            },
        }
    }

    pub fn from_config(
        config: &obzenflow_runtime::runtime_config::AiModelsConfig,
    ) -> anyhow::Result<Self> {
        let provider_raw = config.provider.value.as_str();
        let provider_kind = parse_provider(provider_raw).ok_or_else(|| {
            anyhow!("unsupported ai.models.provider='{provider_raw}' (expected 'ollama', 'openai', or 'openai_compatible')")
        })?;
        let model = config
            .model
            .as_ref()
            .map(|resolved| resolved.value.clone())
            .unwrap_or_else(|| provider_kind.default_model().to_string());
        let profile = resolve_chat_model_profile(&model);
        let base_url = config.base_url.as_ref().map(|value| value.value.clone());

        let provider = match provider_kind {
            ProviderKind::Ollama => {
                validate_optional_url(base_url.as_deref(), "ai.models.base_url")?;
                ProviderConfig::Ollama { base_url }
            }
            ProviderKind::OpenAi => {
                resolve_config_secret(config, provider_kind)?;
                ProviderConfig::OpenAi
            }
            ProviderKind::OpenAiCompatible => {
                resolve_config_secret(config, provider_kind)?;
                let base_url = base_url.ok_or_else(|| {
                    anyhow!(
                        "ai.models.base_url is required when ai.models.provider=openai_compatible"
                    )
                })?;
                validate_optional_url(Some(&base_url), "ai.models.base_url")?;
                ProviderConfig::OpenAiCompatible { base_url }
            }
        };
        Ok(Self { profile, provider })
    }

    pub fn from_env_with_prefix(prefix: &str) -> anyhow::Result<Self> {
        let provider_name = prefixed_env_name(prefix, "PROVIDER");
        let model_name = prefixed_env_name(prefix, "MODEL");
        let provider_raw = env_value(&provider_name).unwrap_or_else(|| DEFAULT_PROVIDER.into());
        let provider_kind = parse_provider(&provider_raw).ok_or_else(|| {
            anyhow!("unsupported {provider_name}='{provider_raw}' (expected 'ollama', 'openai', or 'openai_compatible')")
        })?;
        let model =
            env_value(&model_name).unwrap_or_else(|| provider_kind.default_model().to_string());
        let profile = resolve_chat_model_profile(&model);

        let provider = match provider_kind {
            ProviderKind::Ollama => {
                let base_url = env_value(ENV_OLLAMA_BASE_URL);
                validate_optional_url(base_url.as_deref(), ENV_OLLAMA_BASE_URL)?;
                ProviderConfig::Ollama { base_url }
            }
            ProviderKind::OpenAi => {
                require_env(ENV_OPENAI_API_KEY, &provider_name, provider_kind)?;
                ProviderConfig::OpenAi
            }
            ProviderKind::OpenAiCompatible => {
                require_env(ENV_OPENAI_API_KEY, &provider_name, provider_kind)?;
                let base_url = env_value(ENV_OPENAI_BASE_URL).ok_or_else(|| {
                    anyhow!(
                        "{ENV_OPENAI_BASE_URL} is required when {provider_name}={}",
                        provider_kind.as_str()
                    )
                })?;
                validate_optional_url(Some(&base_url), ENV_OPENAI_BASE_URL)?;
                ProviderConfig::OpenAiCompatible { base_url }
            }
        };
        Ok(Self { profile, provider })
    }

    pub fn provider_label(&self) -> &str {
        match self.provider {
            ProviderConfig::Ollama { .. } => "ollama",
            ProviderConfig::OpenAi => "openai",
            ProviderConfig::OpenAiCompatible { .. } => "openai_compatible",
        }
    }

    pub fn model_label(&self) -> &str {
        &self.profile.model
    }

    pub fn context_window(&self) -> Option<TokenCount> {
        self.profile.context_window
    }

    pub fn estimator(&self) -> Arc<dyn TokenEstimator> {
        self.profile.estimator.estimator()
    }

    pub fn resolved_estimator(&self) -> &ResolvedTokenEstimator {
        &self.profile.estimator
    }

    fn base_url_for_display(&self) -> Option<&str> {
        match &self.provider {
            ProviderConfig::Ollama { base_url } => base_url.as_deref(),
            ProviderConfig::OpenAi => None,
            ProviderConfig::OpenAiCompatible { base_url } => Some(base_url),
        }
    }
}

impl std::fmt::Display for ModelConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut lines = vec![
            format!("provider: {}", self.provider_label()),
            format!("model: {}", self.model_label()),
        ];
        if let Some(base_url) = self.base_url_for_display() {
            lines.push(format!("base_url: {base_url}"));
        }
        lines.push(format!(
            "token_estimator: {:?}",
            self.resolved_estimator().source()
        ));
        lines.push(match self.context_window() {
            Some(context_window) => format!("context_window: {context_window}"),
            None => "context_window: unknown".to_string(),
        });
        formatter.write_str(&lines.join("\n"))
    }
}

fn resolve_config_secret(
    config: &obzenflow_runtime::runtime_config::AiModelsConfig,
    provider: ProviderKind,
) -> anyhow::Result<()> {
    config
        .api_key_env
        .value
        .resolve()
        .map(|_| ())
        .map_err(|error| {
            anyhow!(
                "ai.models.api_key_env: {error} (required when ai.models.provider={})",
                provider.as_str()
            )
        })
}

fn require_env(name: &str, provider_name: &str, provider: ProviderKind) -> anyhow::Result<()> {
    env_value(name).map(|_| ()).ok_or_else(|| {
        anyhow!(
            "{name} is required when {provider_name}={}",
            provider.as_str()
        )
    })
}

fn validate_optional_url(value: Option<&str>, name: &str) -> anyhow::Result<()> {
    if let Some(value) = value {
        Url::parse(value).map_err(|error| anyhow!("invalid {name}: {error}"))?;
    }
    Ok(())
}

fn parse_provider(value: &str) -> Option<ProviderKind> {
    match value.trim().to_ascii_lowercase().as_str() {
        "ollama" => Some(ProviderKind::Ollama),
        "openai" => Some(ProviderKind::OpenAi),
        "openai_compatible" => Some(ProviderKind::OpenAiCompatible),
        _ => None,
    }
}

fn prefixed_env_name(prefix: &str, suffix: &str) -> String {
    format!("{prefix}{suffix}")
}

fn env_value(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}
