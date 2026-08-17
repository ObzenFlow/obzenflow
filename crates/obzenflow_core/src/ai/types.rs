// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;
use std::num::NonZeroU32;
/// The sealed runtime coordinate for ObzenFlow's concrete chat capability.
///
/// User-facing AI syntax selects a typed effect binding lexically. It does not
/// select or manufacture a runtime registry coordinate.
pub const CHAT_CLIENT_PORT: &str = "chat";

/// The sealed runtime coordinate for ObzenFlow's concrete embedding capability.
pub const EMBEDDING_CLIENT_PORT: &str = "embedding";

/// Provider identifier for AI requests.
///
/// Canonical names are lower-case identifiers (for example: `ollama`, `openai`).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct AiProvider(String);

impl AiProvider {
    pub fn new(provider: impl Into<String>) -> Self {
        Self(provider.into().trim().to_ascii_lowercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AiProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for AiProvider {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for AiProvider {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// Non-reversible identity of the physical endpoint bound to a chat target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct ChatBindingFingerprint(String);

impl ChatBindingFingerprint {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ChatBindingFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Credential-free identity of the immutable chat binding used by a flow.
///
/// The optional fingerprint is a non-reversible digest of provider, model,
/// and normalised endpoint identity. Legacy and provider-agnostic clients may
/// remain logical-only; infrastructure-created effect bindings always carry
/// it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ChatTarget {
    pub provider: AiProvider,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub binding_fingerprint: Option<ChatBindingFingerprint>,
}

/// Component of a chat request that failed deterministic canonicalisation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalizationComponent {
    Prompt,
    Parameters,
    ResponseSchema,
}

impl ChatTarget {
    pub fn new(provider: impl Into<AiProvider>, model: impl Into<String>) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            binding_fingerprint: None,
        }
    }

    pub fn with_binding_fingerprint(
        provider: impl Into<AiProvider>,
        model: impl Into<String>,
        binding_fingerprint: ChatBindingFingerprint,
    ) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            binding_fingerprint: Some(binding_fingerprint),
        }
    }

    /// Whether two targets identify the same provider and model, ignoring
    /// endpoint binding. Requests carry this logical pair; effect descriptors
    /// and resolved clients additionally agree on the fingerprint.
    pub fn logically_matches(&self, other: &Self) -> bool {
        self.provider == other.provider && self.model == other.model
    }
}

impl fmt::Display for ChatTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.provider, self.model)?;
        if let Some(fingerprint) = &self.binding_fingerprint {
            write!(f, "#{fingerprint}")?;
        }
        Ok(())
    }
}

/// Provider-agnostic chat role string.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct ChatRole(String);

impl ChatRole {
    pub fn new(role: impl Into<String>) -> Self {
        Self(role.into().trim().to_ascii_lowercase())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn system() -> Self {
        Self::new("system")
    }

    pub fn user() -> Self {
        Self::new("user")
    }

    pub fn assistant() -> Self {
        Self::new("assistant")
    }

    pub fn tool() -> Self {
        Self::new("tool")
    }
}

impl fmt::Display for ChatRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for ChatRole {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

impl From<&str> for ChatRole {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

/// A user-role prompt: the per-request instruction sent to the LLM.
///
/// This is distinct from arbitrary strings so prompt functions can be traceable
/// through type signatures.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct UserPrompt(String);

impl UserPrompt {
    /// Escape hatch for callers that want to construct a prompt directly from a string.
    pub fn raw(prompt: impl Into<String>) -> Self {
        Self(prompt.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for UserPrompt {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for UserPrompt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for UserPrompt {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for UserPrompt {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<UserPrompt> for String {
    fn from(value: UserPrompt) -> Self {
        value.0
    }
}

/// A system-role prompt: the static behavioural instruction for the LLM.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SystemPrompt(String);

impl SystemPrompt {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for SystemPrompt {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SystemPrompt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<String> for SystemPrompt {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for SystemPrompt {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl From<SystemPrompt> for String {
    fn from(value: SystemPrompt) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChatMessage {
    pub role: ChatRole,
    pub content: String,
}

impl ChatMessage {
    pub fn system(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::system(),
            content: content.into(),
        }
    }

    pub fn user(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::user(),
            content: content.into(),
        }
    }

    pub fn assistant(content: impl Into<String>) -> Self {
        Self {
            role: ChatRole::assistant(),
            content: content.into(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct ChatParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extras: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ChatResponseFormat {
    #[default]
    Text,
    JsonObject,
    JsonSchema {
        schema: Value,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolDefinition {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parameters_schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ToolCall {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatRequest {
    pub provider: AiProvider,
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub params: ChatParams,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ChatResponseFormat>,
}

impl ChatRequest {
    /// Project the existing provider and model fields as the immutable target.
    ///
    /// This does not add anything to the serialised request shape.
    pub fn target(&self) -> ChatTarget {
        ChatTarget {
            provider: self.provider.clone(),
            model: self.model.clone(),
            binding_fingerprint: None,
        }
    }

    pub fn resolved_response_format(&self) -> ChatResponseFormat {
        self.response_format.clone().unwrap_or_default()
    }
}

/// Target-free request material prepared by an AI role.
///
/// Provider, model, and endpoint identity belong to the selected
/// the selected typed chat binding, not to role logic. Binding a spec produces
/// the canonical `ChatRequest` wire shape.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatRequestSpec {
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub params: ChatParams,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tools: Vec<ToolDefinition>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub response_format: Option<ChatResponseFormat>,
}

impl ChatRequestSpec {
    pub fn bind_target(&self, target: &ChatTarget) -> ChatRequest {
        ChatRequest {
            provider: target.provider.clone(),
            model: target.model.clone(),
            messages: self.messages.clone(),
            params: self.params.clone(),
            tools: self.tools.clone(),
            response_format: self.response_format.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum UsageSource {
    Provider,
    Estimate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Usage {
    pub source: UsageSource,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub total_tokens: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatResponse {
    pub text: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<Value>,
}

/// Durable framework-owned reply of the replay-safe chat-completion effect.
///
/// This is deliberately not a `TypedPayload`: persistence makes the value
/// replay evidence, not a public stage fact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChatCompletionReply {
    pub response: ChatResponse,
    pub observability: super::LlmObservability,
}

/// Requested or observed embedding vector width.
///
/// A zero-width vector has no valid embedding meaning, so the invalid state is
/// excluded from request, response, and durable effect evidence alike.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[serde(transparent)]
pub struct EmbeddingDimensions(NonZeroU32);

impl EmbeddingDimensions {
    pub const fn get(self) -> u32 {
        self.0.get()
    }
}

impl TryFrom<u32> for EmbeddingDimensions {
    type Error = EmbeddingDimensionsError;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        NonZeroU32::new(value)
            .map(Self)
            .ok_or(EmbeddingDimensionsError)
    }
}

impl From<NonZeroU32> for EmbeddingDimensions {
    fn from(value: NonZeroU32) -> Self {
        Self(value)
    }
}

impl From<EmbeddingDimensions> for NonZeroU32 {
    fn from(value: EmbeddingDimensions) -> Self {
        value.0
    }
}

impl fmt::Display for EmbeddingDimensions {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error, PartialEq, Eq)]
#[error("embedding dimensions must be non-zero")]
pub struct EmbeddingDimensionsError;

/// Non-reversible identity of the physical endpoint bound to an embedding target.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(transparent)]
pub struct EmbeddingBindingFingerprint(String);

impl EmbeddingBindingFingerprint {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for EmbeddingBindingFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Credential-free identity of one immutable embedding binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct EmbeddingTarget {
    pub provider: AiProvider,
    pub model: String,
    pub binding_fingerprint: EmbeddingBindingFingerprint,
}

impl EmbeddingTarget {
    pub fn new(
        provider: impl Into<AiProvider>,
        model: impl Into<String>,
        binding_fingerprint: EmbeddingBindingFingerprint,
    ) -> Self {
        Self {
            provider: provider.into(),
            model: model.into(),
            binding_fingerprint,
        }
    }

    pub fn logically_matches(&self, other: &Self) -> bool {
        self.provider == other.provider && self.model == other.model
    }
}

impl fmt::Display for EmbeddingTarget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}/{}#{}",
            self.provider, self.model, self.binding_fingerprint
        )
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<EmbeddingDimensions>,
}

/// Target-free embedding input prepared by a standalone handler.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingRequestSpec {
    pub inputs: Vec<String>,
    #[serde(default)]
    pub params: EmbeddingParams,
}

impl EmbeddingRequestSpec {
    pub fn bind_target(&self, target: &EmbeddingTarget) -> EmbeddingRequest {
        EmbeddingRequest {
            provider: target.provider.clone(),
            model: target.model.clone(),
            inputs: self.inputs.clone(),
            params: self.params.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingRequest {
    pub provider: AiProvider,
    pub model: String,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub params: EmbeddingParams,
}

impl EmbeddingRequest {
    pub fn logically_targets(&self, target: &EmbeddingTarget) -> bool {
        self.provider == target.provider && self.model == target.model
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingResponse {
    pub vectors: Vec<Vec<f32>>,
    pub vector_dim: EmbeddingDimensions,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

/// Durable framework-owned reply of the replay-safe embedding effect.
///
/// This is replay evidence, not a public stage fact.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingGenerationReply {
    pub response: EmbeddingResponse,
    pub observability: super::LlmObservability,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ai_provider_normalizes_to_lowercase() {
        let provider = AiProvider::new(" OpenAI ");
        assert_eq!(provider.as_str(), "openai");
    }

    #[test]
    fn chat_role_normalizes_to_lowercase() {
        let role = ChatRole::new(" USER ");
        assert_eq!(role.as_str(), "user");
    }

    #[test]
    fn chat_request_defaults_to_text_response_format() {
        let req = ChatRequest {
            provider: AiProvider::new("ollama"),
            model: "llama3.1:8b".to_string(),
            messages: vec![],
            params: ChatParams::default(),
            tools: vec![],
            response_format: None,
        };

        assert_eq!(req.resolved_response_format(), ChatResponseFormat::Text);
        assert_eq!(req.target(), ChatTarget::new("ollama", "llama3.1:8b"));
    }

    #[test]
    fn target_free_request_binding_preserves_the_existing_request_shape() {
        let spec = ChatRequestSpec {
            messages: vec![ChatMessage::user("hello")],
            params: ChatParams::default(),
            tools: vec![],
            response_format: Some(ChatResponseFormat::JsonObject),
        };
        let target = ChatTarget::with_binding_fingerprint(
            "openai",
            "gpt-test",
            ChatBindingFingerprint::new("sha256:test"),
        );

        let bound = spec.bind_target(&target);

        assert_eq!(bound.provider, target.provider);
        assert_eq!(bound.model, target.model);
        assert_eq!(bound.messages, spec.messages);
        assert_eq!(bound.response_format, spec.response_format);
        assert_eq!(
            serde_json::to_value(&bound).unwrap(),
            serde_json::json!({
                "provider": "openai",
                "model": "gpt-test",
                "messages": [{"role": "user", "content": "hello"}],
                "params": {},
                "response_format": {"kind": "json_object"}
            })
        );
    }

    #[test]
    fn embedding_dimensions_exclude_zero_and_round_trip_as_a_number() {
        assert_eq!(
            EmbeddingDimensions::try_from(0),
            Err(EmbeddingDimensionsError)
        );
        let dimensions = EmbeddingDimensions::try_from(768).unwrap();
        assert_eq!(dimensions.get(), 768);
        assert_eq!(
            serde_json::to_value(dimensions).unwrap(),
            serde_json::json!(768)
        );
        assert_eq!(
            serde_json::from_value::<EmbeddingDimensions>(serde_json::json!(768)).unwrap(),
            dimensions
        );
    }

    #[test]
    fn embedding_request_binding_preserves_order_duplicates_and_exact_text() {
        let fingerprint = EmbeddingBindingFingerprint::new("sha256:v1:fixture");
        let target = EmbeddingTarget::new("ollama", "nomic-embed-text", fingerprint);
        let spec = EmbeddingRequestSpec {
            inputs: vec![
                " same ".to_string(),
                "same".to_string(),
                " same ".to_string(),
            ],
            params: EmbeddingParams {
                dimensions: Some(EmbeddingDimensions::try_from(3).unwrap()),
            },
        };

        let request = spec.bind_target(&target);
        assert_eq!(request.inputs, spec.inputs);
        assert_eq!(request.params, spec.params);
        assert_eq!(request.provider, target.provider);
        assert_eq!(request.model, target.model);
    }
}
