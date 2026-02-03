// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::http_client::{HttpClient, HttpClientError, HttpResponse, RequestSpec};
use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::Mutex;

/// Test utility: queue-based mock HTTP client.
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
    async fn execute(&self, _request: RequestSpec) -> Result<HttpResponse, HttpClientError> {
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
