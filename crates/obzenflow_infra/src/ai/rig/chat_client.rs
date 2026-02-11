// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::error_mapping::map_rig_error;
use super::preflight::{preflight_ollama, preflight_openai_models};
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiClientError, AiProvider, ChatClient, ChatMessage, ChatParams, ChatRequest, ChatResponse,
    ChatResponseFormat, ToolCall, ToolDefinition, Usage, UsageSource,
};
use rig_rs::client::{CompletionClient, Nothing};
use rig_rs::completion::CompletionModel;
use rig_rs::completion::{
    CompletionRequest, CompletionResponse, ToolDefinition as RigToolDefinition,
};
use rig_rs::message::{AssistantContent, Message, Text, UserContent};
use rig_rs::providers::{ollama, openai};
use rig_rs::OneOrMany;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use url::Url;

const TOOL_ROLE_UNSUPPORTED_MESSAGE: &str =
    "tool-role messages require tool execution (not supported in P0)";

const DEFAULT_OLLAMA_BASE_URL: &str = "http://localhost:11434/";
const DEFAULT_OPENAI_BASE_URL: &str = "https://api.openai.com/v1/";

#[derive(Clone)]
enum RigChatBackend {
    Ollama { client: Arc<ollama::Client> },
    OpenAi { client: Arc<openai::Client> },
}

/// Rig-backed implementation of `ChatClient`.
///
/// This client is configured for exactly one provider+model pair. Incoming requests
/// must match the configured `provider` and `model` (to keep hashes/observability honest).
#[derive(Clone)]
pub struct RigChatClient {
    provider: AiProvider,
    model: String,
    backend: RigChatBackend,
}

impl RigChatClient {
    /// Create an Ollama-backed client.
    pub fn ollama(model: impl Into<String>, base_url: Option<Url>) -> Result<Self, AiClientError> {
        let client = match base_url {
            None => ollama::Client::new(Nothing).map_err(|err| AiClientError::InvalidRequest {
                message: err.to_string(),
            })?,
            Some(url) => ollama::Client::builder()
                .api_key(Nothing)
                // rig-core's Provider::build_uri appends a trailing `/` to `base_url`.
                // `url::Url::as_str()` includes `/` for the root path, which would otherwise
                // produce `//api/...` and can trigger redirects/method changes on some servers.
                .base_url(url.as_str().trim_end_matches('/'))
                .build()
                .map_err(|err| AiClientError::InvalidRequest {
                    message: err.to_string(),
                })?,
        };

        Ok(Self {
            provider: AiProvider::new("ollama"),
            model: model.into(),
            backend: RigChatBackend::Ollama {
                client: Arc::new(client),
            },
        })
    }

    /// Create an Ollama-backed client and fail fast if the provider/model is not available.
    ///
    /// This performs a lightweight preflight call to the Ollama server (no inference)
    /// and verifies the requested model exists in `/api/tags`.
    pub async fn ollama_checked(
        model: impl Into<String>,
        base_url: Option<Url>,
    ) -> Result<Self, AiClientError> {
        let model = model.into();
        let base_url = base_url.unwrap_or_else(|| {
            Url::parse(DEFAULT_OLLAMA_BASE_URL).expect("default ollama base url parses")
        });

        preflight_ollama(&base_url, Some(model.as_str())).await?;
        Self::ollama(model, Some(base_url))
    }

    /// Create an OpenAI-compatible-backed client.
    ///
    /// This covers OpenAI-hosted APIs as well as OpenAI-compatible servers (vLLM, LM Studio, etc.)
    /// by supplying a base URL.
    pub fn openai_compatible(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: Url,
    ) -> Result<Self, AiClientError> {
        let client = openai::Client::builder()
            .api_key(api_key.into())
            // See note in `ollama()` about trailing slashes.
            .base_url(base_url.as_str().trim_end_matches('/'))
            .build()
            .map_err(|err| AiClientError::InvalidRequest {
                message: err.to_string(),
            })?;

        Ok(Self {
            provider: AiProvider::new("openai"),
            model: model.into(),
            backend: RigChatBackend::OpenAi {
                client: Arc::new(client),
            },
        })
    }

    /// Create an OpenAI-compatible-backed client and fail fast if the endpoint is not reachable.
    ///
    /// This preflights `GET /models` against the supplied base URL (no inference).
    pub async fn openai_compatible_checked(
        model: impl Into<String>,
        api_key: impl Into<String>,
        base_url: Url,
    ) -> Result<Self, AiClientError> {
        let model = model.into();
        let api_key = api_key.into();

        preflight_openai_models(&base_url, api_key.as_str(), Some(model.as_str())).await?;
        Self::openai_compatible(model, api_key, base_url)
    }

    /// Create an OpenAI-backed client that targets the default OpenAI base URL.
    pub fn openai(
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, AiClientError> {
        let client =
            openai::Client::new(api_key.into()).map_err(|err| AiClientError::InvalidRequest {
                message: err.to_string(),
            })?;

        Ok(Self {
            provider: AiProvider::new("openai"),
            model: model.into(),
            backend: RigChatBackend::OpenAi {
                client: Arc::new(client),
            },
        })
    }

    /// Create an OpenAI-backed client and fail fast if the endpoint is not reachable.
    ///
    /// This preflights `GET /models` against the default OpenAI base URL (no inference).
    pub async fn openai_checked(
        model: impl Into<String>,
        api_key: impl Into<String>,
    ) -> Result<Self, AiClientError> {
        let model = model.into();
        let api_key = api_key.into();
        let base_url = Url::parse(DEFAULT_OPENAI_BASE_URL).expect("default openai base url parses");

        preflight_openai_models(&base_url, api_key.as_str(), Some(model.as_str())).await?;
        Self::openai(model, api_key)
    }

    pub fn provider(&self) -> &AiProvider {
        &self.provider
    }

    pub fn model(&self) -> &str {
        &self.model
    }
}

#[async_trait]
impl ChatClient for RigChatClient {
    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, AiClientError> {
        validate_request_target(&req, &self.provider, &self.model)?;

        let (preamble, chat_history) = preamble_and_history(&req.messages)?;

        let additional_params = build_additional_params(&req.params, req.response_format.as_ref());
        let tools = req
            .tools
            .iter()
            .map(map_tool_definition)
            .collect::<Vec<_>>();

        let completion_req = CompletionRequest {
            preamble,
            chat_history: OneOrMany::many(chat_history)
                .expect("preamble_and_history validates non-empty chat history"),
            documents: vec![],
            tools,
            temperature: req.params.temperature.map(|t| t as f64),
            max_tokens: req.params.max_tokens.map(|n| n as u64),
            tool_choice: None,
            additional_params,
        };

        let response = match &self.backend {
            RigChatBackend::Ollama { client } => {
                let model = client.completion_model(self.model.as_str());
                let resp = model
                    .completion(completion_req.clone())
                    .await
                    .map_err(map_rig_error)?;
                map_chat_response(resp)
            }
            RigChatBackend::OpenAi { client } => {
                let model = client.completion_model(self.model.as_str());
                let resp = model
                    .completion(completion_req)
                    .await
                    .map_err(map_rig_error)?;
                map_chat_response(resp)
            }
        };

        Ok(response)
    }
}

fn validate_request_target(
    req: &ChatRequest,
    provider: &AiProvider,
    model: &str,
) -> Result<(), AiClientError> {
    if req.provider != *provider {
        return Err(AiClientError::InvalidRequest {
            message: format!(
                "request provider '{}' does not match RigChatClient provider '{}'",
                req.provider.as_str(),
                provider.as_str()
            ),
        });
    }
    if req.model != model {
        return Err(AiClientError::InvalidRequest {
            message: format!(
                "request model '{}' does not match RigChatClient model '{}'",
                req.model, model
            ),
        });
    }

    Ok(())
}

fn preamble_and_history(
    messages: &[ChatMessage],
) -> Result<(Option<String>, Vec<Message>), AiClientError> {
    let mut preamble_parts = Vec::new();
    let mut history = Vec::new();

    for message in messages {
        match message.role.as_str() {
            "system" => preamble_parts.push(message.content.clone()),
            "user" => history.push(Message::User {
                content: OneOrMany::one(UserContent::Text(Text {
                    text: message.content.clone(),
                })),
            }),
            "assistant" => history.push(Message::Assistant {
                id: None,
                content: OneOrMany::one(AssistantContent::Text(Text {
                    text: message.content.clone(),
                })),
            }),
            "tool" => {
                return Err(AiClientError::InvalidRequest {
                    message: TOOL_ROLE_UNSUPPORTED_MESSAGE.to_string(),
                })
            }
            other => {
                return Err(AiClientError::InvalidRequest {
                    message: format!("unsupported chat role: '{other}'"),
                })
            }
        }
    }

    if history.is_empty() {
        return Err(AiClientError::InvalidRequest {
            message: "chat request requires at least one non-system message".to_string(),
        });
    }

    let preamble = if preamble_parts.is_empty() {
        None
    } else {
        Some(preamble_parts.join("\n"))
    };

    Ok((preamble, history))
}

fn build_additional_params(
    params: &ChatParams,
    response_format: Option<&ChatResponseFormat>,
) -> Option<Value> {
    let mut map = Map::new();

    for (key, value) in &params.extras {
        map.insert(key.clone(), value.clone());
    }

    // Typed fields win over extras.
    if params.temperature.is_some() {
        map.remove("temperature");
    }
    if params.max_tokens.is_some() {
        map.remove("max_tokens");
    }

    if let Some(value) = params.top_p {
        map.insert("top_p".to_string(), json!(value));
    }
    if let Some(value) = params.seed {
        map.insert("seed".to_string(), json!(value));
    }

    if let Some(format) = response_format {
        match format {
            ChatResponseFormat::Text => {
                map.remove("response_format");
            }
            ChatResponseFormat::JsonObject => {
                map.insert(
                    "response_format".to_string(),
                    json!({ "type": "json_object" }),
                );
            }
            ChatResponseFormat::JsonSchema { schema } => {
                map.insert(
                    "response_format".to_string(),
                    json!({
                        "type": "json_schema",
                        "json_schema": schema,
                    }),
                );
            }
        }
    }

    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

fn map_tool_definition(tool: &ToolDefinition) -> RigToolDefinition {
    let parameters = tool.parameters_schema.clone().unwrap_or_else(|| {
        json!({
            "type": "object",
            "properties": {},
        })
    });

    RigToolDefinition {
        name: tool.name.clone(),
        description: tool.description.clone().unwrap_or_default(),
        parameters,
    }
}

fn map_chat_response<T>(resp: CompletionResponse<T>) -> ChatResponse
where
    T: serde::Serialize,
{
    let mut text_parts = Vec::new();
    let mut tool_calls = Vec::new();

    for content in resp.choice.into_iter() {
        match content {
            AssistantContent::Text(text) => text_parts.push(text.text),
            AssistantContent::ToolCall(call) => {
                tool_calls.push(ToolCall {
                    id: Some(call.id),
                    name: call.function.name,
                    arguments: call.function.arguments,
                });
            }
            _ => {}
        }
    }

    let usage = map_usage(resp.usage);

    ChatResponse {
        text: text_parts.join(""),
        tool_calls,
        usage,
        raw: serde_json::to_value(resp.raw_response).ok(),
    }
}

fn map_usage(usage: rig_rs::completion::Usage) -> Option<Usage> {
    if usage.input_tokens == 0 && usage.output_tokens == 0 && usage.total_tokens == 0 {
        return None;
    }

    Some(Usage {
        source: UsageSource::Provider,
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        total_tokens: usage.total_tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{ChatRole, ToolCall};
    use std::collections::BTreeMap;

    #[test]
    fn tool_role_messages_are_rejected_with_locked_error_message() {
        let messages = vec![ChatMessage {
            role: ChatRole::tool(),
            content: "tool output".to_string(),
        }];

        let err = preamble_and_history(&messages).expect_err("tool role should be rejected");

        match err {
            AiClientError::InvalidRequest { message } => {
                assert_eq!(message, TOOL_ROLE_UNSUPPORTED_MESSAGE);
            }
            other => panic!("expected InvalidRequest, got {other:?}"),
        }
    }

    #[test]
    fn response_format_is_encoded_as_openai_shaped_json() {
        let params = ChatParams::default();

        let value = build_additional_params(&params, Some(&ChatResponseFormat::JsonObject))
            .expect("should set additional_params for json_object");

        assert_eq!(
            value,
            json!({
                "response_format": { "type": "json_object" }
            })
        );
    }

    #[test]
    fn json_schema_response_format_is_encoded_with_schema_value() {
        let params = ChatParams::default();

        let schema = json!({
            "type": "object",
            "properties": { "x": { "type": "number" } },
            "required": ["x"]
        });

        let value = build_additional_params(
            &params,
            Some(&ChatResponseFormat::JsonSchema {
                schema: schema.clone(),
            }),
        )
        .expect("should set additional_params for json_schema");

        assert_eq!(
            value,
            json!({
                "response_format": {
                    "type": "json_schema",
                    "json_schema": schema,
                }
            })
        );
    }

    #[test]
    fn explicit_text_response_format_removes_extras_response_format() {
        let mut extras = BTreeMap::new();
        extras.insert(
            "response_format".to_string(),
            json!({ "type": "json_object" }),
        );

        let params = ChatParams {
            extras,
            ..ChatParams::default()
        };

        let value = build_additional_params(&params, Some(&ChatResponseFormat::Text));
        assert_eq!(value, None);
    }

    #[test]
    fn typed_params_override_extras_values() {
        let mut extras = BTreeMap::new();
        extras.insert("top_p".to_string(), json!(0.01));
        extras.insert("seed".to_string(), json!(123));
        extras.insert("temperature".to_string(), json!(0.9));

        let params = ChatParams {
            temperature: Some(0.2),
            max_tokens: None,
            top_p: Some(0.8),
            seed: Some(42),
            extras,
        };

        let additional = build_additional_params(&params, None)
            .expect("should emit additional_params when extras present");

        let Value::Object(map) = additional else {
            panic!("expected JSON object additional_params");
        };

        assert_eq!(map.len(), 2);
        assert_eq!(map.get("seed"), Some(&json!(42)));

        let top_p = map
            .get("top_p")
            .and_then(|v| v.as_f64())
            .expect("top_p should be a number");
        assert!(
            (top_p - 0.8).abs() < 1e-6,
            "top_p should be close to 0.8, got {top_p}"
        );
    }

    #[test]
    fn system_messages_become_preamble_and_history_keeps_order() {
        let messages = vec![
            ChatMessage {
                role: ChatRole::system(),
                content: "a".to_string(),
            },
            ChatMessage {
                role: ChatRole::user(),
                content: "b".to_string(),
            },
            ChatMessage {
                role: ChatRole::system(),
                content: "c".to_string(),
            },
            ChatMessage {
                role: ChatRole::assistant(),
                content: "d".to_string(),
            },
        ];

        let (preamble, history) = preamble_and_history(&messages).expect("should map");
        assert_eq!(preamble, Some("a\nc".to_string()));
        assert_eq!(history.len(), 2);
    }

    #[test]
    fn map_chat_response_accumulates_text_and_tool_calls() {
        let resp: CompletionResponse<Value> = CompletionResponse {
            choice: OneOrMany::many(vec![
                AssistantContent::Text(Text {
                    text: "hello ".to_string(),
                }),
                AssistantContent::ToolCall(rig_rs::message::ToolCall {
                    id: "t1".to_string(),
                    call_id: None,
                    function: rig_rs::message::ToolFunction {
                        name: "do_it".to_string(),
                        arguments: json!({"x": 1}),
                    },
                    signature: None,
                    additional_params: None,
                }),
                AssistantContent::Text(Text {
                    text: "world".to_string(),
                }),
            ])
            .expect("non-empty choice"),
            usage: rig_rs::completion::Usage {
                input_tokens: 1,
                output_tokens: 2,
                total_tokens: 3,
                cached_input_tokens: 0,
            },
            raw_response: json!({"ok": true}),
        };

        let out = map_chat_response(resp);
        assert_eq!(out.text, "hello world".to_string());
        assert_eq!(out.tool_calls.len(), 1);
        assert_eq!(
            out.tool_calls[0],
            ToolCall {
                id: Some("t1".to_string()),
                name: "do_it".to_string(),
                arguments: json!({"x": 1}),
            }
        );
        assert!(out.usage.is_some());
    }
}
