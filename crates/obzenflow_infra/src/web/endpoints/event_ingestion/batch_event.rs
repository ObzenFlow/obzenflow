// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::shared::join_path;
use super::IngestionState;
use super::{authorize_request, validate_submission};
use async_trait::async_trait;
use obzenflow_core::event::ingestion::{
    BatchSubmission, IngestionRejectionReason, SubmissionResponse,
};
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Request, Response, WebError};
use serde_json::json;
use tokio::sync::mpsc::error::TrySendError;

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

fn best_effort_batch_len(body: &[u8]) -> Option<usize> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("events").and_then(|e| e.as_array()).map(|a| a.len()))
}

#[async_trait]
impl HttpEndpoint for BatchEventEndpoint {
    fn path(&self) -> &str {
        &self.path
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Post]
    }

    async fn handle(&self, request: Request) -> Result<Response, WebError> {
        self.state.telemetry.observe_request();

        if request.body.len() > self.state.config.max_body_size {
            let response = Response::new(413)
                .with_json(&json!({"error": "payload too large"}))
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            self.state
                .telemetry
                .observe_rejected(IngestionRejectionReason::PayloadTooLarge, 1);
            return Ok(response);
        }

        if let Some(ref auth) = self.state.config.auth {
            if let Err(e) = authorize_request(auth, &request) {
                let response = Response::new(401)
                    .with_json(&json!({"error": e.to_string()}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                let rejected = best_effort_batch_len(&request.body).unwrap_or(1);
                self.state
                    .telemetry
                    .observe_rejected(IngestionRejectionReason::Auth, rejected);
                return Ok(response);
            }
        }

        if !self.state.is_ready() {
            let response = Response::new(503)
                .with_header("Retry-After".to_string(), "1".to_string())
                .with_json(&json!({"error": "not ready"}))
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            let rejected = best_effort_batch_len(&request.body).unwrap_or(1);
            self.state
                .telemetry
                .observe_rejected(IngestionRejectionReason::NotReady, rejected);
            return Ok(response);
        }

        let submission: BatchSubmission = match serde_json::from_slice(&request.body) {
            Ok(s) => s,
            Err(e) => {
                let response = Response::new(400)
                    .with_json(&json!({"error": format!("invalid request body: {e}")}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                self.state
                    .telemetry
                    .observe_rejected(IngestionRejectionReason::InvalidJson, 1);
                return Ok(response);
            }
        };

        if submission.events.len() > self.state.config.max_batch_size {
            let response = Response::new(413)
                .with_json(&json!({
                    "error": format!(
                        "batch size {} exceeds maximum {}",
                        submission.events.len(),
                        self.state.config.max_batch_size
                    )
                }))
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            self.state.telemetry.observe_rejected(
                IngestionRejectionReason::PayloadTooLarge,
                submission.events.len(),
            );
            return Ok(response);
        }

        let mut accepted_events = Vec::new();
        let mut rejected = 0usize;
        let mut errors = Vec::new();

        if let Some(ref validation) = self.state.config.validation {
            for event in submission.events {
                match validate_submission(&event, validation) {
                    Ok(()) => accepted_events.push(event),
                    Err(e) => {
                        rejected += 1;
                        errors.push(format!("{}: {}", event.event_type, e.to_message()));
                    }
                }
            }
        } else {
            accepted_events = submission.events;
        }

        let accepted = accepted_events.len();
        if accepted == 0 {
            let response = Response::ok()
                .with_json(&SubmissionResponse {
                    accepted,
                    rejected,
                    errors,
                })
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            self.state
                .telemetry
                .observe_rejected(IngestionRejectionReason::Validation, rejected);
            return Ok(response);
        }

        let mut permits = Vec::with_capacity(accepted);
        for _ in 0..accepted {
            match self.state.tx.try_reserve() {
                Ok(permit) => permits.push(permit),
                Err(TrySendError::Full(_)) => {
                    let response = Response::new(503)
                        .with_header("Retry-After".to_string(), "1".to_string())
                        .with_json(&json!({"error": "buffer full"}))
                        .map_err(|e| WebError::RequestHandlingFailed {
                            message: e.to_string(),
                            source: None,
                        })?;
                    self.state
                        .telemetry
                        .observe_rejected(IngestionRejectionReason::Validation, rejected);
                    self.state
                        .telemetry
                        .observe_rejected(IngestionRejectionReason::BufferFull, accepted);
                    return Ok(response);
                }
                Err(TrySendError::Closed(_)) => {
                    let response = Response::internal_error()
                        .with_json(&json!({"error": "ingestion channel closed"}))
                        .map_err(|e| WebError::RequestHandlingFailed {
                            message: e.to_string(),
                            source: None,
                        })?;
                    self.state
                        .telemetry
                        .observe_rejected(IngestionRejectionReason::Validation, rejected);
                    self.state
                        .telemetry
                        .observe_rejected(IngestionRejectionReason::ChannelClosed, accepted);
                    return Ok(response);
                }
            }
        }

        for (permit, event) in permits.into_iter().zip(accepted_events) {
            permit.send(event);
        }

        let response = Response::ok()
            .with_json(&SubmissionResponse {
                accepted,
                rejected,
                errors,
            })
            .map_err(|e| WebError::RequestHandlingFailed {
                message: e.to_string(),
                source: None,
            })?;
        self.state.telemetry.observe_accepted(accepted);
        self.state
            .telemetry
            .observe_rejected(IngestionRejectionReason::Validation, rejected);
        Ok(response)
    }
}
