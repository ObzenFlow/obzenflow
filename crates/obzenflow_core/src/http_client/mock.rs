// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::http_client::{Body, HttpClient, HttpClientError, HttpRequest, HttpResponse};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Mutex;

/// A client for tests: each send records the request and takes the next
/// queued response or error. It ignores a request's deadline.
#[derive(Debug, Default)]
pub struct MockHttpClient {
    outcomes: Mutex<VecDeque<Result<HttpResponse, HttpClientError>>>,
    sent: Mutex<Vec<HttpRequest>>,
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

    /// The requests sent so far, in order. A request that could not be
    /// encoded was never sent and is absent.
    pub fn sent(&self) -> Vec<HttpRequest> {
        self.sent.lock().unwrap().clone()
    }
}

#[async_trait]
impl HttpClient for MockHttpClient {
    async fn send(&self, request: HttpRequest) -> Result<HttpResponse, HttpClientError> {
        if let Body::Unencodable(message) = &request.body {
            return Err(HttpClientError::Unencodable(message.clone()));
        }
        self.sent.lock().unwrap().push(request);
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

    #[tokio::test]
    async fn unencodable_request_records_nothing() {
        let client = MockHttpClient::new();
        let url: url::Url = "https://api.example.test/v1".parse().unwrap();

        let refused = client
            .send(HttpRequest::post(url).with_bearer_token("token\n"))
            .await;

        assert!(matches!(refused, Err(HttpClientError::Unencodable(_))));
        assert!(client.sent().is_empty());
    }

    #[tokio::test]
    async fn sent_lists_requests_in_order() {
        let client = MockHttpClient::new();
        client.enqueue(HttpResponse::new(200, HeaderMap::new(), "first"));
        client.enqueue(HttpResponse::new(204, HeaderMap::new(), ""));
        let first: url::Url = "https://api.example.test/first".parse().unwrap();
        let second: url::Url = "https://api.example.test/second".parse().unwrap();

        client.send(HttpRequest::get(first)).await.unwrap();
        client.send(HttpRequest::post(second)).await.unwrap();

        let sent = client.sent();
        assert_eq!(sent.len(), 2);
        assert_eq!(sent[0].url.path(), "/first");
        assert_eq!(sent[1].url.path(), "/second");
    }
}
