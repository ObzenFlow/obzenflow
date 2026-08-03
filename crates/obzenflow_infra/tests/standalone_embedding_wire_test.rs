// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-128b native Ollama/OpenAI embedding wire contract.

#![cfg(feature = "ai-rig")]

use obzenflow_core::ai::{
    AiProvider, EmbeddingClient, EmbeddingDimensions, EmbeddingParams, EmbeddingRequest,
};
use obzenflow_core::http_client::Url;
use obzenflow_infra::ai::NativeEmbeddingClient;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[derive(Debug, Clone)]
struct CapturedRequest {
    path: String,
    authorization: Option<String>,
    body: Value,
}

async fn fixture_server(
    request_count: usize,
    responder: impl Fn(&CapturedRequest) -> Value + Send + Sync + 'static,
) -> (
    Url,
    Arc<Mutex<Vec<CapturedRequest>>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let captured = Arc::new(Mutex::new(Vec::new()));
    let captured_for_task = captured.clone();
    let responder = Arc::new(responder);
    let task = tokio::spawn(async move {
        for _ in 0..request_count {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let header_end = loop {
                let mut chunk = [0_u8; 4096];
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
                .unwrap_or_default();
            while bytes.len() < header_end + content_length {
                let mut chunk = [0_u8; 4096];
                let read = stream.read(&mut chunk).await.unwrap();
                assert!(read > 0, "client closed before request body");
                bytes.extend_from_slice(&chunk[..read]);
            }
            let request_line = headers.lines().next().unwrap();
            let path = request_line.split_whitespace().nth(1).unwrap().to_string();
            let authorization = headers.lines().find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("authorization")
                    .then(|| value.trim().to_string())
            });
            let body = serde_json::from_slice(
                &bytes[header_end..header_end.saturating_add(content_length)],
            )
            .unwrap();
            let request = CapturedRequest {
                path,
                authorization,
                body,
            };
            let response = serde_json::to_vec(&responder(&request)).unwrap();
            captured_for_task.lock().unwrap().push(request);
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                        response.len()
                    )
                    .as_bytes(),
                )
                .await
                .unwrap();
            stream.write_all(&response).await.unwrap();
        }
    });
    (
        Url::parse(&format!("http://{address}/")).unwrap(),
        captured,
        task,
    )
}

fn request(
    provider: &str,
    model: &str,
    dimensions: Option<EmbeddingDimensions>,
) -> EmbeddingRequest {
    EmbeddingRequest {
        provider: AiProvider::new(provider),
        model: model.to_string(),
        inputs: vec!["first".to_string(), "second".to_string()],
        params: EmbeddingParams { dimensions },
    }
}

fn vectors(width: usize) -> Vec<Vec<f32>> {
    vec![vec![0.25; width], vec![0.5; width]]
}

#[tokio::test]
async fn ollama_and_openai_send_default_and_explicit_dimensions_natively() {
    let (ollama_url, ollama_requests, ollama_task) = fixture_server(2, |request| {
        let width = request.body["dimensions"].as_u64().unwrap_or(2) as usize;
        json!({
            "model": "fixture-embedding",
            "embeddings": vectors(width),
            "prompt_eval_count": 9,
            "raw_provider_field": "must not survive normalisation"
        })
    })
    .await;
    let ollama = NativeEmbeddingClient::ollama("fixture-embedding", Some(ollama_url)).unwrap();
    let default = ollama
        .embed(request("ollama", "fixture-embedding", None))
        .await
        .unwrap();
    let explicit_dimensions = EmbeddingDimensions::try_from(3).unwrap();
    let explicit = ollama
        .embed(request(
            "ollama",
            "fixture-embedding",
            Some(explicit_dimensions),
        ))
        .await
        .unwrap();
    ollama_task.await.unwrap();

    assert_eq!(default.vector_dim.get(), 2);
    assert_eq!(explicit.vector_dim, explicit_dimensions);
    assert!(serde_json::to_value(&explicit)
        .unwrap()
        .get("raw")
        .is_none());
    {
        let ollama_requests = ollama_requests.lock().unwrap();
        assert_eq!(ollama_requests.len(), 2);
        assert!(ollama_requests
            .iter()
            .all(|request| request.path == "/api/embed"));
        assert_eq!(ollama_requests[0].body["model"], "fixture-embedding");
        assert_eq!(ollama_requests[0].body["input"], json!(["first", "second"]));
        assert!(ollama_requests[0].body.get("dimensions").is_none());
        assert_eq!(ollama_requests[1].body["dimensions"], 3);
    }

    let (openai_root, openai_requests, openai_task) = fixture_server(2, |request| {
        let width = request.body["dimensions"].as_u64().unwrap_or(2) as usize;
        json!({
            "object": "list",
            "data": [
                {"object": "embedding", "embedding": vec![0.25; width], "index": 0},
                {"object": "embedding", "embedding": vec![0.5; width], "index": 1}
            ],
            "model": "fixture-embedding",
            "usage": {"prompt_tokens": 9, "total_tokens": 9}
        })
    })
    .await;
    let openai_url = openai_root.join("v1/").unwrap();
    let openai =
        NativeEmbeddingClient::openai_compatible("fixture-embedding", "fixture-secret", openai_url)
            .unwrap();
    let default = openai
        .embed(request("openai_compatible", "fixture-embedding", None))
        .await
        .unwrap();
    let explicit = openai
        .embed(request(
            "openai_compatible",
            "fixture-embedding",
            Some(explicit_dimensions),
        ))
        .await
        .unwrap();
    openai_task.await.unwrap();

    assert_eq!(default.vector_dim.get(), 2);
    assert_eq!(explicit.vector_dim, explicit_dimensions);
    let openai_requests = openai_requests.lock().unwrap();
    assert_eq!(openai_requests.len(), 2);
    assert!(openai_requests
        .iter()
        .all(|request| request.path == "/v1/embeddings"));
    assert!(openai_requests
        .iter()
        .all(|request| request.authorization.as_deref() == Some("Bearer fixture-secret")));
    assert!(openai_requests[0].body.get("dimensions").is_none());
    assert_eq!(openai_requests[1].body["dimensions"], 3);
}

#[tokio::test]
async fn native_adapters_reject_bad_cardinality_order_and_width_without_retrying() {
    let cases = [
        json!({"embeddings": [[1.0, 2.0]]}),
        json!({"embeddings": [[], []]}),
        json!({"embeddings": [[1.0], [2.0, 3.0]]}),
        json!({"embeddings": [[1.0], [2.0]]}),
    ];
    for (index, response) in cases.into_iter().enumerate() {
        let (url, captured, task) = fixture_server(1, move |_| response.clone()).await;
        let client = NativeEmbeddingClient::ollama("fixture", Some(url)).unwrap();
        let requested = (index == 3).then(|| EmbeddingDimensions::try_from(2).unwrap());
        let result = client.embed(request("ollama", "fixture", requested)).await;
        assert!(result.is_err());
        task.await.unwrap();
        assert_eq!(captured.lock().unwrap().len(), 1, "no adapter-local retry");
    }

    let (root, captured, task) = fixture_server(1, |_| {
        json!({
            "data": [
                {"embedding": [1.0, 2.0], "index": 1},
                {"embedding": [3.0, 4.0], "index": 0}
            ]
        })
    })
    .await;
    let client =
        NativeEmbeddingClient::openai_compatible("fixture", "secret", root.join("v1/").unwrap())
            .unwrap();
    let result = client
        .embed(request("openai_compatible", "fixture", None))
        .await;
    assert!(result.is_err());
    task.await.unwrap();
    assert_eq!(captured.lock().unwrap().len(), 1, "no adapter-local retry");
}
