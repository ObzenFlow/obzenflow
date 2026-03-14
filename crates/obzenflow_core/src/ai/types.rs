// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt;

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
    pub fn resolved_response_format(&self) -> ChatResponseFormat {
        self.response_format.clone().unwrap_or_default()
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

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<usize>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extras: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingRequest {
    pub provider: AiProvider,
    pub model: String,
    pub inputs: Vec<String>,
    #[serde(default)]
    pub params: EmbeddingParams,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct EmbeddingResponse {
    pub vectors: Vec<Vec<f32>>,
    pub vector_dim: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw: Option<Value>,
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
    }
}
