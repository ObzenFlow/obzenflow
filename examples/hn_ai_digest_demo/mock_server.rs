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

    let (items, topstories) = build_mock_hn_payloads();
    let items = Arc::new(items);
    let topstories: Arc<str> = Arc::from(topstories);

    let task = tokio::spawn({
        let items = items.clone();
        let topstories = topstories.clone();
        async move {
            loop {
                let (socket, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };

                let items = items.clone();
                let topstories = topstories.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_connection(socket, &items, topstories.as_ref()).await {
                        tracing::debug!(?err, "mock HN server connection failed");
                    }
                });
            }
        }
    });

    Ok(MockHnServer { addr, _task: task })
}

fn build_mock_hn_payloads() -> (HashMap<u64, String>, String) {
    const MOCK_TOPSTORIES: usize = 64;
    const DELETED_EVERY_NTH: usize = 11;
    const TOPICS: [&str; 10] = [
        "Rust",
        "AI",
        "Databases",
        "Security",
        "Browsers",
        "Linux",
        "Compilers",
        "Startups",
        "Robotics",
        "Distributed Systems",
    ];
    const VERBS: [&str; 8] = [
        "tooling update",
        "benchmark",
        "postmortem",
        "deep dive",
        "launch",
        "migration guide",
        "research note",
        "case study",
    ];

    let mut items = HashMap::with_capacity(MOCK_TOPSTORIES);
    let mut topstories = Vec::with_capacity(MOCK_TOPSTORIES);

    for offset in 0..MOCK_TOPSTORIES {
        let id = 100_u64 + offset as u64;
        topstories.push(id.to_string());

        if (offset + 1) % DELETED_EVERY_NTH == 0 {
            items.insert(id, "null".to_string());
            continue;
        }

        let topic = TOPICS[offset % TOPICS.len()];
        let verb = VERBS[offset % VERBS.len()];
        let time = 1_774_600_000_u64 + offset as u64;
        let score = 180_u64.saturating_sub((offset as u64) * 2);
        let descendants = (offset % 29) as u64;
        let author = format!("author{}", (offset % 17) + 1);
        let title = format!("{topic}: {verb} #{id}");
        let url = format!("https://example.com/hn/{id}");

        items.insert(
            id,
            format!(
                r#"{{"id":{id},"type":"story","by":"{author}","time":{time},"title":"{title}","url":"{url}","score":{score},"descendants":{descendants}}}"#
            ),
        );
    }

    (items, format!("[{}]", topstories.join(",")))
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

#[cfg(test)]
mod tests {
    use super::build_mock_hn_payloads;

    #[test]
    fn mock_fixture_exposes_realistic_story_volume() {
        let (items, topstories) = build_mock_hn_payloads();

        assert_eq!(items.len(), 64);

        let listed_ids = topstories.trim_matches(['[', ']']).split(',').count();
        assert_eq!(listed_ids, 64);

        let valid_in_first_sixty = (100_u64..160_u64)
            .filter(|id| items.get(id).is_some_and(|body| body != "null"))
            .count();

        assert_eq!(valid_in_first_sixty, 55);
    }
}
