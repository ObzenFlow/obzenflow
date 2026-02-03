// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use obzenflow::sources::Url;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

pub struct MockHnServer {
    addr: SocketAddr,
    _task: tokio::task::JoinHandle<()>,
}

impl MockHnServer {
    pub fn base_url(&self) -> Url {
        format!("http://{}/", self.addr)
            .parse()
            .expect("mock base url parses")
    }
}

pub async fn spawn_mock_hn_server() -> Result<MockHnServer> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;

    let items: HashMap<u64, String> = HashMap::from([
        (
            100_u64,
            r#"{"id":100,"type":"story","by":"alice","time":123,"title":"Hello HN","url":"https://example.com/hello","score":10,"descendants":2}"#
                .to_string(),
        ),
        (
            101_u64,
            r#"{"id":101,"type":"story","by":"bob","time":124,"title":"Unicode: café 漢字","url":"https://example.com/unicode","score":42,"descendants":7}"#
                .to_string(),
        ),
        // Simulate a deleted item (HN returns `null`).
        (102_u64, "null".to_string()),
    ]);

    let topstories = "[100,101,102]";
    let items = Arc::new(items);

    let task = tokio::spawn({
        let items = items.clone();
        async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };

                let items = items.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(socket, &items, topstories).await {
                        tracing::debug!(?err, "mock HN server connection failed");
                    }
                });
            }
        }
    });

    Ok(MockHnServer { addr, _task: task })
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    items: &HashMap<u64, String>,
    topstories: &str,
) -> Result<()> {
    let request = read_http_head(&mut socket).await?;
    let path = request_path(&request);

    let (status, body) = if path.starts_with("/v0/topstories.json") {
        (200_u16, topstories.to_string())
    } else if let Some(id) = extract_item_id(path) {
        match items.get(&id) {
            Some(body) => (200_u16, body.clone()),
            None => (404_u16, r#"{"message":"not_found"}"#.to_string()),
        }
    } else {
        (404_u16, r#"{"message":"not_found"}"#.to_string())
    };

    let response = http_response(status, &body);
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
    Ok(())
}

async fn read_http_head(socket: &mut tokio::net::TcpStream) -> Result<String> {
    let mut buf = Vec::with_capacity(4096);
    let mut tmp = [0_u8; 1024];

    loop {
        match socket.read(&mut tmp).await {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&tmp[..n]);
                if buf.windows(4).any(|w| w == b"\r\n\r\n") {
                    break;
                }
                if buf.len() > 64 * 1024 {
                    break;
                }
            }
            Err(err) => return Err(err.into()),
        }
    }

    Ok(String::from_utf8_lossy(&buf).to_string())
}

fn http_response(status: u16, body: &str) -> String {
    format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

fn request_path(req: &str) -> &str {
    req.lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/")
}

fn extract_item_id(path: &str) -> Option<u64> {
    let path = path.split('?').next().unwrap_or(path);
    let suffix = path.strip_prefix("/v0/item/")?;
    let id_str = suffix.strip_suffix(".json")?;
    id_str.parse().ok()
}
