// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::shared::join_path;
use super::IngestionState;
use async_trait::async_trait;
use obzenflow_core::web::{
    EndpointError, HttpEndpoint, HttpMethod, ManagedResponse, Request, Response,
};
use serde::Serialize;

pub struct IngestionHealthEndpoint<T> {
    state: IngestionState<T>,
    path: String,
}

impl<T> IngestionHealthEndpoint<T> {
    pub fn new(state: IngestionState<T>) -> Self {
        let path = join_path(&state.config.base_path, "health");
        Self { state, path }
    }
}

#[derive(Debug, Serialize)]
struct IngestionHealthResponse {
    status: &'static str,
    pipeline_ready: bool,
    /// The two readiness conjuncts (FLOWIP-120n F12): the pipeline-running
    /// watcher and the hosted source's resume-live flag.
    pipeline_running: bool,
    resume_live: bool,
    channel_depth: usize,
    channel_capacity: usize,
    /// FLOWIP-084l: request identities the source currently deduplicates.
    idempotency_entries: u64,
    idempotency_window_secs: u64,
}

#[async_trait]
impl<T: Send + Sync + 'static> HttpEndpoint for IngestionHealthEndpoint<T> {
    fn path(&self) -> &str {
        &self.path
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
        let pipeline_ready = self.state.is_ready();
        let status = if pipeline_ready { "ready" } else { "not_ready" };
        let slot = self.state.ingress_slot();
        let body = IngestionHealthResponse {
            status,
            pipeline_ready,
            pipeline_running: self.state.pipeline_running(),
            resume_live: self.state.resume_live(),
            channel_depth: self.state.channel_depth(),
            channel_capacity: self.state.channel_capacity(),
            idempotency_entries: slot.idempotency_entries(),
            idempotency_window_secs: slot.timing().idempotency_window().as_secs(),
        };

        let response = if pipeline_ready {
            Response::ok()
        } else {
            Response::new(503).with_header("Retry-After".to_string(), "1".to_string())
        };

        let response = response
            .with_json(&body)
            .map_err(|e| EndpointError::with_source("Serializing endpoint response", e))?;
        Ok(response.into())
    }
}
