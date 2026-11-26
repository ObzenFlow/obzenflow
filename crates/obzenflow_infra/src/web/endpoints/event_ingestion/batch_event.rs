// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::contract::{refusal_response, IngressErrorCode};
use super::http::{invalid_body, precheck, process, Precheck};
use super::shared::join_path;
use super::IngestionState;
use async_trait::async_trait;
use obzenflow_core::ingress::BatchSubmission;
use obzenflow_core::web::{EndpointError, HttpEndpoint, HttpMethod, ManagedResponse, Request};
use tokio::time::Instant;

pub struct BatchEventEndpoint {
    state: IngestionState,
    path: String,
}

impl BatchEventEndpoint {
    pub fn new(state: IngestionState) -> Self {
        let path = join_path(&state.config.base_path, "batch");
        Self { state, path }
    }
}

#[async_trait]
impl HttpEndpoint for BatchEventEndpoint {
    fn path(&self) -> &str {
        &self.path
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Post]
    }

    async fn handle(&self, request: Request) -> Result<ManagedResponse, EndpointError> {
        // One deadline, from arrival, covers the whole batch (FLOWIP-084l B1).
        let arrived = Instant::now();
        let attempt_seq = self.state.next_attempt_seq();
        let request_id = match precheck(&self.state, &request, attempt_seq, true).await? {
            Precheck::Proceed(request_id) => request_id,
            Precheck::Answered(response) => return Ok(response),
        };
        let batch: BatchSubmission = match serde_json::from_slice(&request.body) {
            Ok(batch) => batch,
            Err(error) => return invalid_body(&error),
        };
        let maximum = self.state.config.max_batch_size;
        if batch.events.len() > maximum {
            return refusal_response(
                413,
                IngressErrorCode::BatchTooLarge,
                format!(
                    "batch size {} exceeds maximum {maximum}",
                    batch.events.len()
                ),
                None,
            );
        }
        process(
            &self.state,
            arrived,
            attempt_seq,
            request_id,
            batch.events,
            true,
        )
        .await
    }
}
