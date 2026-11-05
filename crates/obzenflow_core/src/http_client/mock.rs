// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::http_client::{Body, HttpClient, HttpClientError, HttpRequest, HttpResponse};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Mutex;

/// A client for tests: each send takes the next queued response or error.
/// It ignores a request's deadline.
#[derive(Debug, Default)]
pub struct MockHttpClient {
    outcomes: Mutex<VecDeque<Result<HttpResponse, HttpClientError>>>,
}

impl MockHttpClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn enqueue(&self, response: HttpResponse) {
        self.enqueue_response(response);
    }

    pub fn enqueue_response(&self, response: HttpResponse) {
        self.outcomes.lock().unwrap().push_back(Ok(response));
    }

    pub fn enqueue_error(&self, error: HttpClientError) {
        self.outcomes.lock().unwrap().push_back(Err(error));
    }
}

#[async_trait]
impl HttpClient for MockHttpClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        if let Body::Unencodable(message) = request.body {
            return Err(HttpClientError::Unencodable(message));
        }
        self.outcomes
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| {
                Err(HttpClientError::Transport(
                    "MockHttpClient: no outcomes queued".into(),
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;

    #[tokio::test]
    async fn unencodable_request_skips_queue() {
        let client = MockHttpClient::new();
        client.enqueue(HttpResponse::new(200, HeaderMap::new(), "ok"));
        let url: url::Url = "https://api.example.test/v1".parse().unwrap();

        let refused = client
            .send(HttpRequest::post(url.clone()).with_idempotency_key("key\n"))
            .await;
        assert!(matches!(refused, Err(HttpClientError::Unencodable(_))));

        let answered = client.send(HttpRequest::post(url)).await.unwrap();
        assert_eq!(answered.status, 200);
    }
}
