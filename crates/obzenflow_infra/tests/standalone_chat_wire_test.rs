// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "ai-rig")]

use obzenflow_core::ai::{
    AiClientError, AiProvider, ChatClient, ChatMessage, ChatParams, ChatRequest, ChatResponse,
    ChatResponseFormat, ToolDefinition,
};
use obzenflow_core::http_client::Url;
use obzenflow_infra::ai::rig::RigChatClient;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const MODEL: &str = "fixture-model";

#[derive(Clone, Copy, Debug)]
enum Route {
    Ollama,
    Responses,
}

const ROUTES: [Route; 2] = [Route::Ollama, Route::Responses];

impl Route {
    fn provider(self) -> &'static str {
        match self {
            Self::Ollama => "ollama",
            Self::Responses => "openai_compatible",
        }
    }

    fn client(self, address: std::net::SocketAddr) -> RigChatClient {
        match self {
            Self::Ollama => {
                let url = Url::parse(&format!("http://{address}/")).unwrap();
                RigChatClient::ollama(MODEL, Some(url)).unwrap()
            }
            Self::Responses => {
                let url = Url::parse(&format!("http://{address}/v1")).unwrap();
                RigChatClient::openai_compatible(MODEL, "fixture-key", url).unwrap()
            }
        }
    }

    fn reply_without_content(self) -> Value {
        match self {
            Self::Ollama => ollama_reply(json!({"role": "assistant", "content": ""})),
            Self::Responses => responses_reply(json!([])),
        }
    }

    fn reply_without_counts(self) -> Value {
        match self {
            Self::Ollama => ollama_reply(json!({"role": "assistant", "content": "hello"})),
            Self::Responses => responses_reply(json!([{
                "type": "message",
                "id": "msg_1",
                "role": "assistant",
                "status": "completed",
                "content": [{"type": "output_text", "text": "hello"}]
            }])),
        }
    }
}

#[derive(Debug)]
struct Sent {
    path: String,
    body: Value,
}

async fn read_request(stream: &mut TcpStream) -> Sent {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0, "client closed before request headers");
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(position) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut chunk).await.unwrap();
        assert!(read > 0, "client closed before request body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    Sent {
        path: headers
            .lines()
            .next()
            .unwrap()
            .split_whitespace()
            .nth(1)
            .unwrap()
            .to_string(),
        body: serde_json::from_slice(&bytes[header_end..]).unwrap(),
    }
}

async fn exchange_raw(
    route: Route,
    status: u16,
    reply: String,
    request: ChatRequest,
) -> (Result<ChatResponse, AiClientError>, Sent) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let connections = Arc::new(Mutex::new(Vec::new()));
    let recorded = connections.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let sent = read_request(&mut stream).await;
            recorded.lock().unwrap().push(sent);
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 {status} Fixture\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{reply}",
                        reply.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
        }
    });
    let result = route.client(address).chat(request).await;
    server.abort();
    let mut connections = connections.lock().unwrap();
    assert_eq!(connections.len(), 1, "one connection per chat call");
    (result, connections.pop().unwrap())
}

async fn exchange(
    route: Route,
    status: u16,
    reply: Value,
    request: ChatRequest,
) -> (Result<ChatResponse, AiClientError>, Sent) {
    exchange_raw(route, status, reply.to_string(), request).await
}

fn chat_request(route: Route) -> ChatRequest {
    ChatRequest {
        provider: AiProvider::new(route.provider()),
        model: MODEL.to_string(),
        messages: vec![ChatMessage::user("ping")],
        params: ChatParams::default(),
        tools: Vec::new(),
        response_format: None,
    }
}

fn ollama_reply(message: Value) -> Value {
    json!({
        "model": MODEL,
        "created_at": "2026-01-01T00:00:00Z",
        "message": message,
        "done": true
    })
}

fn responses_reply(output: Value) -> Value {
    json!({
        "id": "resp_1",
        "object": "response",
        "created_at": 1,
        "status": "completed",
        "error": null,
        "incomplete_details": null,
        "instructions": null,
        "max_output_tokens": null,
        "model": MODEL,
        "output": output,
        "tools": []
    })
}

fn assert_other(result: &Result<ChatResponse, AiClientError>) {
    assert!(
        matches!(result, Err(AiClientError::Other { .. })),
        "{result:?}"
    );
}

#[tokio::test]
async fn ollama_chat_sends_think_false() {
    let route = Route::Ollama;
    let (result, sent) = exchange(
        route,
        200,
        route.reply_without_counts(),
        chat_request(route),
    )
    .await;

    assert_eq!(result.unwrap().text, "hello");
    assert_eq!(sent.path, "/api/chat");
    assert_eq!(sent.body["think"], json!(false));
    assert_eq!(sent.body["stream"], json!(false));
}

#[tokio::test]
async fn ollama_caller_think_overrides_default() {
    let route = Route::Ollama;
    let mut request = chat_request(route);
    request
        .params
        .extras
        .insert("think".to_string(), json!(true));

    let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

    assert_eq!(sent.body["think"], json!(true));
}

#[tokio::test]
async fn responses_system_prompt_sends_input_item() {
    let route = Route::Responses;
    let mut request = chat_request(route);
    request.messages = vec![ChatMessage::system("be brief"), ChatMessage::user("ping")];

    let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

    assert_eq!(sent.path, "/v1/responses");
    assert!(sent.body.get("instructions").is_none());
    assert_eq!(sent.body["input"][0]["role"], json!("system"));
    assert_eq!(
        sent.body["input"][0]["content"][0]["text"],
        json!("be brief")
    );
    assert_eq!(sent.body["input"][1]["role"], json!("user"));
}

#[tokio::test]
async fn responses_tools_send_strict() {
    let route = Route::Responses;
    let mut request = chat_request(route);
    request.tools = vec![ToolDefinition {
        name: "do_it".to_string(),
        description: Some("does it".to_string()),
        parameters_schema: Some(json!({
            "type": "object",
            "properties": {"x": {"type": "number"}}
        })),
    }];

    let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

    assert_eq!(sent.body["tools"][0]["name"], json!("do_it"));
    assert_eq!(sent.body["tools"][0]["strict"], json!(true));
    assert_eq!(
        sent.body["tools"][0]["parameters"]["additionalProperties"],
        json!(false)
    );
}

#[tokio::test]
async fn responses_extras_forward_accepted_fields() {
    let route = Route::Responses;
    let accepted = json!({
        "background": false,
        "metadata": {"run": "fixture"},
        "parallel_tool_calls": true,
        "previous_response_id": "resp_0",
        "reasoning": {"effort": "low"},
        "service_tier": "flex",
        "store": true,
        "text": {"format": {"type": "text"}},
        "top_p": 0.5,
        "truncation": "auto",
        "user": "fixture-user"
    });
    let mut request = chat_request(route);
    for (key, value) in accepted.as_object().unwrap() {
        request.params.extras.insert(key.clone(), value.clone());
    }
    request
        .params
        .extras
        .insert("include".to_string(), json!(["file_search_call.results"]));
    request.params.extras.insert(
        "tools".to_string(),
        json!([{
            "type": "function",
            "name": "extra_tool",
            "description": "from extras",
            "parameters": {"type": "object", "properties": {}}
        }]),
    );
    request
        .params
        .extras
        .insert("frequency_penalty".to_string(), json!(0.5));
    request.params.seed = Some(7);

    let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

    for (key, value) in accepted.as_object().unwrap() {
        assert_eq!(&sent.body[key], value, "{key}");
    }
    assert!(sent.body["include"]
        .as_array()
        .unwrap()
        .contains(&json!("file_search_call.results")));
    assert_eq!(sent.body["tools"][0]["name"], json!("extra_tool"));
    for key in ["frequency_penalty", "seed"] {
        assert!(sent.body.get(key).is_none(), "{key}");
    }
}

fn story_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "stories": {"type": "array", "items": {"type": "integer", "enum": [11, 12]}}
        },
        "required": ["stories"]
    })
}

#[tokio::test]
async fn ollama_schema_sends_format() {
    let route = Route::Ollama;
    let mut request = chat_request(route);
    request.response_format = Some(ChatResponseFormat::JsonSchema {
        schema: story_schema(),
    });

    let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

    assert_eq!(sent.body["format"], story_schema());
    assert!(sent.body["options"].get("response_format").is_none());
}

#[tokio::test]
async fn responses_schema_sends_text_format() {
    let route = Route::Responses;
    let mut request = chat_request(route);
    request.response_format = Some(ChatResponseFormat::JsonSchema {
        schema: story_schema(),
    });

    let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

    let format = &sent.body["text"]["format"];
    assert_eq!(format["type"], json!("json_schema"));
    assert_eq!(format["strict"], json!(true));
    assert_eq!(format["schema"]["properties"], story_schema()["properties"]);
}

#[tokio::test]
async fn json_object_sends_provider_format() {
    for (route, field, expected) in [
        (Route::Ollama, "format", json!("json")),
        (
            Route::Responses,
            "text",
            json!({"format": {"type": "json_object"}}),
        ),
    ] {
        let mut request = chat_request(route);
        request.response_format = Some(ChatResponseFormat::JsonObject);

        let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

        assert_eq!(sent.body[field], expected, "{route:?}");
    }
}

#[tokio::test]
async fn invalid_schema_fails_invalid_request() {
    for route in ROUTES {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let mut request = chat_request(route);
        request.response_format = Some(ChatResponseFormat::JsonSchema {
            schema: json!("not an object"),
        });

        let result = route.client(address).chat(request).await;

        assert!(
            matches!(result, Err(AiClientError::InvalidRequest { .. })),
            "{route:?}: {result:?}"
        );
    }
}

#[tokio::test]
async fn reply_without_content_fails_other() {
    for route in ROUTES {
        let (result, _) = exchange(
            route,
            200,
            route.reply_without_content(),
            chat_request(route),
        )
        .await;

        assert_other(&result);
    }
}

#[tokio::test]
async fn ollama_missing_counts_default_zero() {
    let route = Route::Ollama;
    let mut reply = route.reply_without_counts();
    reply["eval_count"] = json!(5);

    let (result, _) = exchange(route, 200, reply, chat_request(route)).await;

    let usage = result.unwrap().usage.unwrap();
    assert_eq!(usage.input_tokens, 0);
    assert_eq!(usage.output_tokens, 5);
    assert_eq!(usage.total_tokens, 5);
}

#[tokio::test]
async fn reply_without_counts_omits_usage() {
    for route in ROUTES {
        let (result, _) = exchange(
            route,
            200,
            route.reply_without_counts(),
            chat_request(route),
        )
        .await;

        assert_eq!(result.unwrap().usage, None, "{route:?}");
    }
}

#[tokio::test]
async fn responses_unparseable_arguments_fail_other() {
    let route = Route::Responses;
    let reply = responses_reply(json!([{
        "type": "function_call",
        "id": "fc_1",
        "call_id": "call_1",
        "name": "do_it",
        "arguments": "{not json",
        "status": "completed"
    }]));

    let (result, _) = exchange(route, 200, reply, chat_request(route)).await;

    assert_other(&result);
}

#[tokio::test]
async fn responses_missing_call_id_fails_other() {
    let route = Route::Responses;
    let reply = responses_reply(json!([{
        "type": "function_call",
        "id": "fc_1",
        "name": "do_it",
        "arguments": "{}",
        "status": "completed"
    }]));

    let (result, _) = exchange(route, 200, reply, chat_request(route)).await;

    assert_other(&result);
}

#[tokio::test]
async fn empty_reply_body_fails_other() {
    for route in ROUTES {
        let (result, _) = exchange_raw(route, 200, String::new(), chat_request(route)).await;

        assert_other(&result);
    }
}

#[tokio::test]
async fn ollama_tool_call_takes_function_name() {
    let route = Route::Ollama;
    let reply = ollama_reply(json!({
        "role": "assistant",
        "content": "",
        "tool_calls": [{"function": {"name": "do_it", "arguments": {"x": 1}}}]
    }));

    let (result, _) = exchange(route, 200, reply, chat_request(route)).await;

    let response = result.unwrap();
    assert_eq!(response.tool_calls.len(), 1);
    assert_eq!(response.tool_calls[0].id.as_deref(), Some("do_it"));
    assert_eq!(response.tool_calls[0].arguments, json!({"x": 1}));
}

#[tokio::test]
async fn provider_status_maps_failure_class() {
    type ClassCheck = fn(&AiClientError) -> bool;
    let cases: [(u16, ClassCheck); 6] = [
        (401, |error| matches!(error, AiClientError::Auth { .. })),
        (403, |error| matches!(error, AiClientError::Auth { .. })),
        (429, |error| {
            matches!(error, AiClientError::RateLimited { .. })
        }),
        (404, |error| {
            matches!(error, AiClientError::InvalidRequest { .. })
        }),
        (500, |error| matches!(error, AiClientError::Remote { .. })),
        (503, |error| matches!(error, AiClientError::Remote { .. })),
    ];
    for route in ROUTES {
        for (status, class) in cases {
            let (result, _) = exchange(
                route,
                status,
                json!({"error": {"message": "fixture"}}),
                chat_request(route),
            )
            .await;

            let error = result.unwrap_err();
            assert!(class(&error), "{route:?} {status}: {error:?}");
        }
    }
}

#[tokio::test]
async fn refused_connection_fails_remote() {
    for route in ROUTES {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);

        let result = route.client(address).chat(chat_request(route)).await;

        assert!(
            matches!(result, Err(AiClientError::Remote { .. })),
            "{route:?}: {result:?}"
        );
    }
}

#[tokio::test]
async fn https_endpoint_opens_tls_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut first_byte = [0_u8; 1];
        stream.read_exact(&mut first_byte).await.unwrap();
        first_byte[0]
    });
    let url = Url::parse(&format!("https://{address}/v1")).unwrap();
    let client = RigChatClient::openai_compatible(MODEL, "fixture-key", url).unwrap();

    let result = client.chat(chat_request(Route::Responses)).await;

    let first_byte = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("the client connects")
        .unwrap();
    assert_eq!(first_byte, 0x16, "a TLS handshake record");
    assert!(matches!(result, Err(AiClientError::Remote { .. })));
}

#[tokio::test]
async fn ollama_max_tokens_sends_num_predict() {
    let route = Route::Ollama;
    let mut request = chat_request(route);
    request.params.max_tokens = Some(64);

    let (_, sent) = exchange(route, 200, route.reply_without_counts(), request).await;

    assert_eq!(sent.body["options"]["num_predict"], json!(64));
    assert!(sent.body.get("max_tokens").is_none());
}

#[tokio::test]
async fn responses_reasoning_only_reply_fails() {
    let route = Route::Responses;
    let reply = responses_reply(json!([{
        "type": "reasoning",
        "id": "rs_1",
        "summary": [{"type": "summary_text", "text": "thinking"}]
    }]));

    let (result, _) = exchange(route, 200, reply, chat_request(route)).await;

    assert_other(&result);
}
