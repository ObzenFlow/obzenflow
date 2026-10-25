// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    AiProvider, ChatTarget, EmbeddingTarget, ResolvedTokenEstimator, TokenEstimatorResolutionInfo,
};
use crate::config::SecretRef;
use crate::http_client::Url;
use serde::{Deserialize, Serialize};

/// One AI endpoint with its provider, URL and credential, written whole
/// under `[ai.endpoints.<name>]` or built in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AiEndpoint {
    pub name: String,
    pub provider: AiProvider,
    pub base_url: Option<Url>,
    pub api_key_env: Option<SecretRef>,
}

/// Credential-free description of the model an AI stage used for one effect.
/// Reconstruction reads it from the run manifest, never from current configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AiModelSpec {
    pub effect_type: String,
    pub endpoint: String,
    pub provider: AiProvider,
    pub model: String,
    pub binding_fingerprint: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub estimator: Option<TokenEstimatorResolutionInfo>,
}

impl std::fmt::Display for AiModelSpec {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}/{}", self.endpoint, self.model)
    }
}

/// A chat model provider's view of one configured model before any call,
/// with the bound target and the estimator that sizes its requests.
#[derive(Debug, Clone)]
pub struct ChatModelDescription {
    pub target: ChatTarget,
    pub estimator: ResolvedTokenEstimator,
}

/// What an embedding model provider knows about one configured model
/// before any call.
#[derive(Debug, Clone)]
pub struct EmbeddingModelDescription {
    pub target: EmbeddingTarget,
}

/// Why a model provider cannot describe or connect a model.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AiModelProviderError {
    #[error("provider `{provider}` cannot serve {family}")]
    UnsupportedProvider {
        provider: String,
        family: &'static str,
    },
    #[error("endpoint `{endpoint}` needs base_url for provider `{provider}`")]
    MissingBaseUrl { endpoint: String, provider: String },
    #[error("endpoint `{endpoint}` must not carry credentials in base_url")]
    CredentialedBaseUrl { endpoint: String },
    #[error("endpoint `{endpoint}` cannot read its credential from {env}")]
    CredentialUnavailable { endpoint: String, env: String },
    #[error("endpoint `{endpoint}` could not build a client: {detail}")]
    Client { endpoint: String, detail: String },
    #[error("recorded model {spec} cannot be rebuilt: {detail}")]
    Recorded { spec: String, detail: String },
}
