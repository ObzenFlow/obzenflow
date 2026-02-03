// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::shared::join_path;
use super::IngestionState;
use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Request, Response, WebError};
use serde::Serialize;

pub struct IngestionHealthEndpoint {
    state: IngestionState,
    path: String,
}

impl IngestionHealthEndpoint {
    pub fn new(state: IngestionState) -> Self {
        let path = join_path(&state.config.base_path, "health");
        Self { state, path }
    }
}

#[derive(Debug, Serialize)]
struct IngestionHealthResponse {
    status: &'static str,
    pipeline_ready: bool,
    channel_depth: usize,
    channel_capacity: usize,
}

#[async_trait]
impl HttpEndpoint for IngestionHealthEndpoint {
    fn path(&self) -> &str {
        &self.path
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, _request: Request) -> Result<Response, WebError> {
        let pipeline_ready = self.state.is_ready();
        let status = if pipeline_ready { "ready" } else { "not_ready" };
        let body = IngestionHealthResponse {
            status,
            pipeline_ready,
            channel_depth: self.state.channel_depth(),
            channel_capacity: self.state.buffer_capacity,
        };

        let response = if pipeline_ready {
            Response::ok()
        } else {
            Response::new(503).with_header("Retry-After".to_string(), "1".to_string())
        };

        let response = response
            .with_json(&body)
            .map_err(|e| WebError::RequestHandlingFailed {
                message: e.to_string(),
                source: None,
            })?;
        Ok(response)
    }
}
