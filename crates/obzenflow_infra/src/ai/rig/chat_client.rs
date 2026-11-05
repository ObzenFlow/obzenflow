// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::error_mapping::map_rig_error;
use super::preflight::{preflight_ollama, preflight_openai_models};
use super::transport::ChatTransport;
use crate::ai::endpoint_identity::{
    bound_chat_target, default_ollama_base_url, default_openai_base_url,
};
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiClientError, AiProvider, ChatClient, ChatMessage, ChatParams, ChatRequest, ChatResponse,
    ChatResponseFormat, ChatTarget, ToolCall, ToolDefinition, Usage, UsageSource,
};
use rig_rs::completion::{
    CompletionRequest, CompletionResponse, ToolDefinition as RigToolDefinition,
};
use rig_rs::message::{AssistantContent, Message, ToolCall as RigToolCall, ToolName};
use rig_rs::providers::ollama::{self, OllamaConfig};
use rig_rs::providers::openai::responses_api::wire::Responses;
use rig_rs::providers::openai::OpenAIConfig;
use rig_rs::schemars::Schema;
use rig_rs::Model;
use serde_json::{json, Map, Value};
use std::sync::Arc;
use url::Url;

const TOOL_ROLE_UNSUPPORTED_MESSAGE: &str =
    "tool-role messages require tool execution (not supported in P0)";

const RESPONSES_ADDITIONAL_FIELDS: [&str; 13] = [
    "background",
    "include",
    "metadata",
    "parallel_tool_calls",
    "previous_response_id",
    "reasoning",
    "service_tier",
    "store",
    "text",
    "tools",
    "top_p",
    "truncation",
    "user",
];

#[derive(Clone)]
enum RigChatBackend {
    Ollama { model: Arc<Model<ollama::Chat>> },
    OpenAi { model: Arc<Model<Responses>> },
}

/// Rig-backed implementation of `ChatClient`.
///
/// This client is configured for exactly one provider+model pair. Incoming requests
/// must match the configured `provider` and `model` (to keep hashes/observability honest).
#[derive(Clone)]
pub struct RigChatClient {
    target: ChatTarget,
    backend: RigChatBackend,
}

impl RigChatClient {
    /// Create an Ollama-backed client.
    pub fn ollama(model: impl Into<String>, base_url: Option<Url>) -> Result<Self, AiClientError> {
        let model = model.into();
        let endpoint = base_url.unwrap_or_else(default_ollama_base_url);
        let client = OllamaConfig::new()
            .with_base_url(endpoint.as_str())
            .connect(ChatTransport::new());

        Ok(Self {
            backend: RigChatBackend::Ollama {
                model: Arc::new(client.native_completion(model.as_str())),
            },
            target: bound_chat_target("ollama", model, &endpoint),
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
        let base_url = base_url.unwrap_or_else(default_ollama_base_url);

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
        let model = model.into();

        Ok(Self {
            backend: RigChatBackend::OpenAi {
                model: Arc::new(responses_model(model.as_str(), api_key.into(), &base_url)),
            },
            target: bound_chat_target("openai_compatible", model, &base_url),
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
        let model = model.into();
        let endpoint = default_openai_base_url();

        Ok(Self {
            backend: RigChatBackend::OpenAi {
                model: Arc::new(responses_model(model.as_str(), api_key.into(), &endpoint)),
            },
            target: bound_chat_target("openai", model, &endpoint),
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
        let base_url = default_openai_base_url();

        preflight_openai_models(&base_url, api_key.as_str(), Some(model.as_str())).await?;
        Self::openai(model, api_key)
    }

    pub fn provider(&self) -> &AiProvider {
        &self.target.provider
    }

    pub fn model(&self) -> &str {
        &self.target.model
    }
}

#[async_trait]
impl ChatClient for RigChatClient {
    fn target(&self) -> &ChatTarget {
        &self.target
    }

    async fn chat(&self, req: ChatRequest) -> Result<ChatResponse, AiClientError> {
        validate_request_target(&req, &self.target)?;

        let format = req.response_format.as_ref();
        let additional_params = build_additional_params(&req.params, format);
        match &self.backend {
            RigChatBackend::Ollama { model } => {
                let request =
                    completion_request(&req, ollama_additional_params(additional_params, format))?;
                let response = model.call(request).await.map_err(map_rig_error)?;
                map_chat_response(response, ollama_call_id)
            }
            RigChatBackend::OpenAi { model } => {
                let request = completion_request(
                    &req,
                    responses_additional_params(additional_params, format),
                )?;
                let response = model.call(request).await.map_err(map_rig_error)?;
                map_chat_response(response, responses_call_id)
            }
        }
    }
}

fn responses_model(model: &str, api_key: String, base_url: &Url) -> Model<Responses> {
    let mut responses = OpenAIConfig::new(api_key)
        .with_base_url(base_url.as_str().trim_end_matches('/'))
        .with_system_instructions_as_messages()
        .connect(ChatTransport::new())
        .responses(model);
    responses.wire = responses.wire.with_strict_tools();
    responses
}

fn completion_request(
    req: &ChatRequest,
    additional_params: Option<Value>,
) -> Result<CompletionRequest, AiClientError> {
    let (preamble, chat_history) = preamble_and_history(&req.messages)?;
    let tools = req
        .tools
        .iter()
        .map(map_tool_definition)
        .collect::<Result<Vec<_>, _>>()?;

    let mut request = CompletionRequest::from(chat_history).tools(tools);
    if let Some(preamble) = preamble {
        request = request.preamble(preamble);
    }
    request.temperature = req.params.temperature.map(|t| t as f64);
    request.max_tokens = req.params.max_tokens.map(|n| n as u64);
    request.additional_params = additional_params;
    if let Some(ChatResponseFormat::JsonSchema { schema }) = &req.response_format {
        let schema =
            Schema::try_from(schema.clone()).map_err(|err| AiClientError::InvalidRequest {
                message: format!("invalid JSON schema for the response format: {err}"),
            })?;
        request.output_schema = Some(schema);
    }
    Ok(request)
}

fn ollama_additional_params(
    additional_params: Option<Value>,
    format: Option<&ChatResponseFormat>,
) -> Option<Value> {
    let mut map = match additional_params {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    map.entry("think").or_insert(Value::Bool(false));
    match format {
        Some(ChatResponseFormat::JsonObject) => {
            map.insert("format".to_string(), json!("json"));
        }
        Some(ChatResponseFormat::Text | ChatResponseFormat::JsonSchema { .. }) => {
            map.remove("format");
        }
        None => {}
    }
    Some(Value::Object(map))
}

fn responses_additional_params(
    additional_params: Option<Value>,
    format: Option<&ChatResponseFormat>,
) -> Option<Value> {
    let mut map = match additional_params {
        Some(Value::Object(map)) => map,
        _ => Map::new(),
    };
    map.retain(|key, _| RESPONSES_ADDITIONAL_FIELDS.contains(&key.as_str()));
    match format {
        Some(ChatResponseFormat::JsonObject) => {
            map.insert(
                "text".to_string(),
                json!({ "format": { "type": "json_object" } }),
            );
        }
        Some(ChatResponseFormat::Text | ChatResponseFormat::JsonSchema { .. }) => {
            map.remove("text");
        }
        None => {}
    }
    (!map.is_empty()).then_some(Value::Object(map))
}

fn validate_request_target(req: &ChatRequest, bound: &ChatTarget) -> Result<(), AiClientError> {
    let requested = req.target();
    if !requested.logically_matches(bound) {
        return Err(AiClientError::target_mismatch(requested, bound.clone()));
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
            "user" => history.push(Message::user(message.content.clone())),
            "assistant" => history.push(Message::assistant(message.content.clone())),
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

    if response_format.is_some() {
        map.remove("response_format");
    }

    if map.is_empty() {
        None
    } else {
        Some(Value::Object(map))
    }
}

fn map_tool_definition(tool: &ToolDefinition) -> Result<RigToolDefinition, AiClientError> {
    let name = ToolName::new(tool.name.as_str()).map_err(|err| AiClientError::InvalidRequest {
        message: err.to_string(),
    })?;
    let parameters = tool.parameters_schema.clone().unwrap_or_else(|| {
        json!({
            "type": "object",
            "properties": {},
        })
    });

    Ok(RigToolDefinition::new(
        name,
        tool.description.clone().unwrap_or_default(),
        parameters,
    ))
}

fn ollama_call_id(call: &RigToolCall) -> Result<String, AiClientError> {
    Ok(call.function.name.as_str().to_string())
}

fn responses_call_id(call: &RigToolCall) -> Result<String, AiClientError> {
    if call.id.is_local() {
        return Err(AiClientError::Other {
            message: format!(
                "the provider's tool call `{}` has no call_id",
                call.function.name.as_str()
            ),
        });
    }
    Ok(call.id.wire().into_owned())
}

fn map_tool_call(
    call: RigToolCall,
    call_id: fn(&RigToolCall) -> Result<String, AiClientError>,
) -> Result<ToolCall, AiClientError> {
    let id = call_id(&call)?;
    let name = call.function.name.as_str().to_string();
    if let Some(arguments) = call.function.invalid_arguments {
        return Err(AiClientError::Other {
            message: format!(
                "the provider's tool call `{name}` has invalid arguments: {arguments}"
            ),
        });
    }
    Ok(ToolCall {
        id: Some(id),
        name,
        arguments: Value::Object(call.function.arguments),
    })
}

fn map_chat_response(
    resp: CompletionResponse,
    call_id: fn(&RigToolCall) -> Result<String, AiClientError>,
) -> Result<ChatResponse, AiClientError> {
    let mut text_parts = Vec::new();
    let mut tool_calls = Vec::new();

    for content in resp.choice {
        match content {
            AssistantContent::Text(text) => text_parts.push(text.text),
            AssistantContent::ToolCall(call) => tool_calls.push(map_tool_call(call, call_id)?),
            _ => {}
        }
    }

    if text_parts.is_empty() && tool_calls.is_empty() {
        let reported = resp
            .error
            .map_or_else(String::new, |error| format!(": {error}"));
        return Err(AiClientError::Other {
            message: format!("the provider's reply has no text and no tool call{reported}"),
        });
    }

    Ok(ChatResponse {
        text: text_parts.join(""),
        tool_calls,
        usage: map_usage(resp.usage),
        raw: Some(resp.raw),
    })
}

fn map_usage(usage: rig_rs::completion::Usage) -> Option<Usage> {
    let input_tokens = usage.input_tokens.unwrap_or(0);
    let output_tokens = usage.output_tokens.unwrap_or(0);
    let total_tokens = usage.total_tokens.unwrap_or(input_tokens + output_tokens);
    if input_tokens == 0 && output_tokens == 0 && total_tokens == 0 {
        return None;
    }

    Some(Usage {
        source: UsageSource::Provider,
        input_tokens,
        output_tokens,
        total_tokens,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::{ChatRole, ToolCall};
    use rig_rs::message::{CallId, Origin, ToolFunction};
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
        let mut request = Vec::new();
        let mut buffer = [0_u8; 4096];
        let mut expected_len = None;

        loop {
            let read = stream
                .read(&mut buffer)
                .await
                .expect("controlled HTTP request should be readable");
            assert_ne!(read, 0, "controlled HTTP request ended before its body");
            request.extend_from_slice(&buffer[..read]);

            if expected_len.is_none() {
                let Some(header_end) = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|index| index + 4)
                else {
                    continue;
                };
                let headers = std::str::from_utf8(&request[..header_end])
                    .expect("controlled HTTP headers should be UTF-8");
                let content_length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().expect("valid content length"))
                    })
                    .unwrap_or(0);
                expected_len = Some(header_end + content_length);
            }

            if request.len() >= expected_len.expect("header length was established") {
                return request;
            }
        }
    }

    fn request_line_and_body(request: &[u8]) -> (String, Vec<u8>) {
        let header_end = request
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
            .map(|index| index + 4)
            .expect("controlled request should contain complete headers");
        let request_line = std::str::from_utf8(&request[..header_end])
            .expect("controlled request headers should be UTF-8")
            .lines()
            .next()
            .expect("controlled request should contain a request line")
            .to_string();
        (request_line, request[header_end..].to_vec())
    }

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

    #[tokio::test]
    async fn one_chat_invocation_can_follow_a_body_preserving_redirect() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("controlled HTTP listener should bind");
        let address = listener
            .local_addr()
            .expect("controlled HTTP listener should have an address");

        let server = tokio::spawn(async move {
            let (mut first, _) = listener
                .accept()
                .await
                .expect("initial completion request should arrive");
            let first_request = read_http_request(&mut first).await;
            first
                .write_all(
                    b"HTTP/1.1 307 Temporary Redirect\r\n\
                      Location: /v1/redirected\r\n\
                      Content-Length: 0\r\n\
                      Connection: close\r\n\r\n",
                )
                .await
                .expect("redirect response should be writable");
            first
                .shutdown()
                .await
                .expect("redirect connection should close");

            let (mut second, _) = listener
                .accept()
                .await
                .expect("redirected completion request should arrive");
            let second_request = read_http_request(&mut second).await;
            let body = json!({
                "id": "resp-flowip-128g",
                "object": "response",
                "created_at": 1,
                "status": "completed",
                "error": null,
                "incomplete_details": null,
                "instructions": null,
                "max_output_tokens": null,
                "model": "fixture-model",
                "output": [{
                    "type": "message",
                    "id": "msg-flowip-128g",
                    "role": "assistant",
                    "status": "completed",
                    "content": [{
                        "type": "output_text",
                        "text": "redirected response"
                    }]
                }],
                "tools": [],
                "usage": {
                    "input_tokens": 1,
                    "input_tokens_details": null,
                    "output_tokens": 2,
                    "output_tokens_details": {
                        "reasoning_tokens": 0
                    },
                    "total_tokens": 3
                }
            })
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            second
                .write_all(response.as_bytes())
                .await
                .expect("completion response should be writable");
            second
                .shutdown()
                .await
                .expect("completion connection should close");

            [first_request, second_request]
        });

        let base_url = Url::parse(&format!("http://{address}/v1"))
            .expect("controlled OpenAI-compatible URL should parse");
        let client = RigChatClient::openai_compatible("fixture-model", "fixture-key", base_url)
            .expect("Rig client should build without preflight");
        let response = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.chat(ChatRequest {
                provider: AiProvider::new("openai_compatible"),
                model: "fixture-model".to_string(),
                messages: vec![ChatMessage::user("prove the redirect boundary")],
                params: ChatParams::default(),
                tools: Vec::new(),
                response_format: None,
            }),
        )
        .await
        .expect("one caller-visible chat invocation should complete")
        .expect("redirected completion should decode");
        assert_eq!(response.text, "redirected response");

        let [first_request, second_request] =
            tokio::time::timeout(std::time::Duration::from_secs(5), server)
                .await
                .expect("both controlled HTTP requests should arrive")
                .expect("controlled HTTP server should not panic");
        let (first_line, first_body) = request_line_and_body(&first_request);
        let (second_line, second_body) = request_line_and_body(&second_request);
        assert_eq!(first_line, "POST /v1/responses HTTP/1.1");
        assert_eq!(second_line, "POST /v1/redirected HTTP/1.1");
        assert!(!first_body.is_empty());
        assert_eq!(
            second_body, first_body,
            "307 must preserve the completion request body across the second HTTP send"
        );
    }

    #[test]
    fn map_chat_response_accumulates_text_and_tool_calls() {
        let resp = CompletionResponse::new(
            vec![
                AssistantContent::text("hello "),
                AssistantContent::ToolCall(RigToolCall::new(
                    CallId::from_wire("t1"),
                    ToolFunction::new(ToolName::new("do_it").unwrap(), json!({"x": 1})),
                )),
                AssistantContent::text("world"),
            ],
            rig_rs::completion::Usage::default()
                .input_tokens(1)
                .output_tokens(2)
                .total_tokens(3),
            Origin::new("fixture", "fixture", "fixture"),
            json!({"ok": true}),
        );

        let out = map_chat_response(resp, responses_call_id).expect("reply maps");
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
