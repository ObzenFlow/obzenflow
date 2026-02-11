// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::{
    ChatMessage, ChatParams, ChatRequest, ChatResponseFormat, EmbeddingParams, EmbeddingRequest,
    ToolDefinition,
};
use ring::digest::{digest, SHA256};
use serde::Serialize;
use serde_json::Value;

pub const LLM_HASH_VERSION_SHA256_V1: &str = "sha256:v1";

#[derive(Debug, thiserror::Error)]
pub enum AiHashError {
    #[error("failed to serialize canonical value: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[derive(Debug, Serialize)]
struct CanonicalChatMessage<'a> {
    role: &'a str,
    content: &'a str,
}

#[derive(Debug, Serialize)]
struct CanonicalChatPrompt<'a> {
    kind: &'static str,
    messages: Vec<CanonicalChatMessage<'a>>,
}

#[derive(Debug, Serialize)]
struct CanonicalEmbeddingPrompt<'a> {
    kind: &'static str,
    inputs: &'a [String],
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum CanonicalResponseFormat<'a> {
    Text,
    JsonObject,
    JsonSchema { schema_hash: &'a str },
}

#[derive(Debug, Serialize)]
struct CanonicalChatParams<'a> {
    kind: &'static str,
    provider: &'a str,
    model: &'a str,
    params: &'a ChatParams,
    tools: &'a [ToolDefinition],
    response_format: CanonicalResponseFormat<'a>,
}

#[derive(Debug, Serialize)]
struct CanonicalEmbeddingParams<'a> {
    kind: &'static str,
    provider: &'a str,
    model: &'a str,
    params: &'a EmbeddingParams,
}

pub fn prompt_hash_for_chat(messages: &[ChatMessage]) -> Result<String, AiHashError> {
    let canonical = CanonicalChatPrompt {
        kind: "chat",
        messages: messages
            .iter()
            .map(|m| CanonicalChatMessage {
                role: m.role.as_str(),
                content: m.content.as_str(),
            })
            .collect(),
    };

    hash_canonical_json(&canonical)
}

pub fn prompt_hash_for_embedding_inputs(inputs: &[String]) -> Result<String, AiHashError> {
    let canonical = CanonicalEmbeddingPrompt {
        kind: "embedding",
        inputs,
    };
    hash_canonical_json(&canonical)
}

pub fn schema_hash_from_json(schema: &Value) -> Result<String, AiHashError> {
    hash_canonical_json(schema)
}

pub fn schema_hash_from_text(schema: &str) -> String {
    sha256_hex(schema.as_bytes())
}

pub fn schema_hash_for_response_format(
    response_format: Option<&ChatResponseFormat>,
) -> Result<Option<String>, AiHashError> {
    match response_format {
        Some(ChatResponseFormat::JsonSchema { schema }) => schema_hash_from_json(schema).map(Some),
        _ => Ok(None),
    }
}

pub fn params_hash_for_chat(request: &ChatRequest) -> Result<String, AiHashError> {
    let schema_hash = schema_hash_for_response_format(request.response_format.as_ref())?;
    let response_format =
        canonical_response_format(request.response_format.as_ref(), schema_hash.as_deref());

    let canonical = CanonicalChatParams {
        kind: "chat",
        provider: request.provider.as_str(),
        model: request.model.as_str(),
        params: &request.params,
        tools: &request.tools,
        response_format,
    };

    hash_canonical_json(&canonical)
}

pub fn params_hash_for_embedding(request: &EmbeddingRequest) -> Result<String, AiHashError> {
    let canonical = CanonicalEmbeddingParams {
        kind: "embedding",
        provider: request.provider.as_str(),
        model: request.model.as_str(),
        params: &request.params,
    };

    hash_canonical_json(&canonical)
}

fn canonical_response_format<'a>(
    response_format: Option<&'a ChatResponseFormat>,
    schema_hash: Option<&'a str>,
) -> CanonicalResponseFormat<'a> {
    match response_format {
        None | Some(ChatResponseFormat::Text) => CanonicalResponseFormat::Text,
        Some(ChatResponseFormat::JsonObject) => CanonicalResponseFormat::JsonObject,
        Some(ChatResponseFormat::JsonSchema { .. }) => CanonicalResponseFormat::JsonSchema {
            // `schema_hash` is precomputed from the request schema. This should
            // always exist for JsonSchema; use empty string only as defensive fallback.
            schema_hash: schema_hash.unwrap_or(""),
        },
    }
}

fn hash_canonical_json<T: Serialize>(value: &T) -> Result<String, AiHashError> {
    let json_value = serde_json::to_value(value)?;
    let canonical = canonicalize_json_value(json_value);
    let bytes = serde_json::to_vec(&canonical)?;
    Ok(sha256_hex(&bytes))
}

fn canonicalize_json_value(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut entries: Vec<(String, Value)> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));

            let mut out = serde_json::Map::new();
            for (k, v) in entries {
                out.insert(k, canonicalize_json_value(v));
            }
            Value::Object(out)
        }
        Value::Array(values) => Value::Array(
            values
                .into_iter()
                .map(canonicalize_json_value)
                .collect::<Vec<_>>(),
        ),
        other => other,
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let hash = digest(&SHA256, bytes);
    hash.as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::{AiProvider, ChatRole};
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn prompt_hash_changes_when_prompt_changes() {
        let messages_a = vec![ChatMessage {
            role: ChatRole::user(),
            content: "hello".to_string(),
        }];
        let messages_b = vec![ChatMessage {
            role: ChatRole::user(),
            content: "hello there".to_string(),
        }];

        let hash_a = prompt_hash_for_chat(&messages_a).expect("hash should compute");
        let hash_b = prompt_hash_for_chat(&messages_b).expect("hash should compute");

        assert_ne!(hash_a, hash_b);
        assert_eq!(hash_a.len(), 64);
        assert_eq!(hash_b.len(), 64);
    }

    #[test]
    fn params_hash_excludes_prompt_content() {
        let req_a = ChatRequest {
            provider: AiProvider::new("ollama"),
            model: "llama3.1:8b".to_string(),
            messages: vec![ChatMessage {
                role: ChatRole::user(),
                content: "prompt A".to_string(),
            }],
            params: ChatParams {
                temperature: Some(0.2),
                max_tokens: Some(256),
                top_p: Some(0.9),
                seed: Some(42),
                extras: BTreeMap::new(),
            },
            tools: vec![],
            response_format: None,
        };

        let mut req_b = req_a.clone();
        req_b.messages = vec![ChatMessage {
            role: ChatRole::user(),
            content: "prompt B".to_string(),
        }];

        let hash_a = params_hash_for_chat(&req_a).expect("hash should compute");
        let hash_b = params_hash_for_chat(&req_b).expect("hash should compute");
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn schema_hash_for_json_is_canonical() {
        let schema_a = json!({
            "type": "object",
            "properties": {
                "a": { "type": "string" },
                "b": { "type": "number" }
            },
            "required": ["a", "b"]
        });

        let schema_b = json!({
            "required": ["a", "b"],
            "properties": {
                "b": { "type": "number" },
                "a": { "type": "string" }
            },
            "type": "object"
        });

        let hash_a = schema_hash_from_json(&schema_a).expect("hash should compute");
        let hash_b = schema_hash_from_json(&schema_b).expect("hash should compute");

        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn params_hash_changes_with_schema_hash() {
        let mut req = ChatRequest {
            provider: AiProvider::new("openai"),
            model: "gpt-4.1-mini".to_string(),
            messages: vec![ChatMessage {
                role: ChatRole::user(),
                content: "summarize".to_string(),
            }],
            params: ChatParams::default(),
            tools: vec![],
            response_format: Some(ChatResponseFormat::JsonSchema {
                schema: json!({
                    "type": "object",
                    "properties": {
                        "summary": { "type": "string" }
                    },
                    "required": ["summary"]
                }),
            }),
        };

        let hash_a = params_hash_for_chat(&req).expect("hash should compute");

        req.response_format = Some(ChatResponseFormat::JsonSchema {
            schema: json!({
                "type": "object",
                "properties": {
                    "headline": { "type": "string" }
                },
                "required": ["headline"]
            }),
        });

        let hash_b = params_hash_for_chat(&req).expect("hash should compute");
        assert_ne!(hash_a, hash_b);
    }
}
