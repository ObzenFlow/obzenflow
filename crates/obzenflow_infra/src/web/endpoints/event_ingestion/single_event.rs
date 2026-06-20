// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::shared::{join_path, unix_now_nanos};
use super::IngestionState;
use super::{authorize_request, validate_submission};
use async_trait::async_trait;
use obzenflow_core::ingress::{EventSubmission, SubmissionIngressContext, SubmissionResponse};
use obzenflow_core::ingress::{
    IngressAdmissionDecision, IngressAdmissionOutcome, IngressAttemptContext, IngressRefusalReason,
};
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc::error::TrySendError;

pub struct SingleEventEndpoint {
    state: IngestionState,
    path: String,
}

impl SingleEventEndpoint {
    pub fn new(state: IngestionState) -> Self {
        let path = join_path(&state.config.base_path, "events");
        Self { state, path }
    }
}

#[async_trait]
impl HttpEndpoint for SingleEventEndpoint {
    fn path(&self) -> &str {
        &self.path
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Post]
    }

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        // FLOWIP-115d: one attempt sequence per submission, allocated up front so
        // a refusal fact and the accepted row (if admitted) share it. Request and
        // accepted totals are projected from journal facts (refusal facts here,
        // accepted source rows downstream) and the bucketed HTTP-surface metrics,
        // not from in-memory ingestion counters.
        let attempt = IngressAttemptContext {
            attempt_seq: self.state.next_attempt_seq(),
            request_count: 1,
            event_count: 1,
            batch_count: 0,
        };

        // Protocol 4xx rejects (payload-too-large, auth, invalid-json) are not
        // journalled; they always return a 4xx the HTTP-surface metrics record.
        if request.body.len() > self.state.config.max_body_size {
            let response = Response::new(413)
                .with_json(&json!({"error": "payload too large"}))
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            return Ok(response.into());
        }

        if let Some(ref auth) = self.state.config.auth {
            if let Err(e) = authorize_request(auth, &request) {
                let response = Response::new(401)
                    .with_json(&json!({"error": e.to_string()}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                return Ok(response.into());
            }
        }

        // Not-ready is a hosted-edge shed: journal it as a refusal fact.
        if !self.state.is_ready() {
            if let Some(response) = self
                .state
                .record_refusal_or_unavailable(
                    IngressRefusalReason::NotReady,
                    &attempt,
                    503,
                    Some(Duration::from_secs(1)),
                )
                .await?
            {
                return Ok(response);
            }
            let response = Response::new(503)
                .with_header("Retry-After".to_string(), "1".to_string())
                .with_json(&json!({"error": "not ready"}))
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            return Ok(response.into());
        }

        let mut submission: EventSubmission = match serde_json::from_slice(&request.body) {
            Ok(s) => s,
            Err(e) => {
                let response = Response::new(400)
                    .with_json(&json!({"error": format!("invalid request body: {e}")}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                return Ok(response.into());
            }
        };

        // Validation rejection is journalled per event: a partially accepted batch
        // returns 200, so validation refusals are otherwise invisible to the
        // bucketed HTTP-surface 4xx metric.
        if let Some(ref validation) = self.state.config.validation {
            if let Err(e) = validate_submission(&submission, validation) {
                if let Some(response) = self
                    .state
                    .record_refusal_or_unavailable(
                        IngressRefusalReason::Validation,
                        &attempt,
                        400,
                        None,
                    )
                    .await?
                {
                    return Ok(response);
                }
                let response = Response::new(400)
                    .with_json(&json!({"error": e.to_message()}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                return Ok(response.into());
            }
        }

        match self.state.tx.try_reserve() {
            Ok(permit) => {
                // FLOWIP-115d: the edge reserved capacity (so buffer-full/503
                // already had precedence); now run fail-fast ingress admission.
                // Token exhaustion is RateLimited/429; the limiter never waits
                // while holding the listener request.
                if let Some(boundary) = self.state.ingress_boundary() {
                    match boundary.on_ingress(&attempt) {
                        IngressAdmissionDecision::Accept => {
                            boundary.observe(&attempt, IngressAdmissionOutcome::AcceptedForEnqueue);
                        }
                        IngressAdmissionDecision::Reject { retry_after } => {
                            boundary.observe(&attempt, IngressAdmissionOutcome::RejectedBy);
                            drop(permit);
                            if let Some(response) = self
                                .state
                                .record_refusal_or_unavailable(
                                    IngressRefusalReason::RateLimited,
                                    &attempt,
                                    429,
                                    retry_after,
                                )
                                .await?
                            {
                                return Ok(response);
                            }
                            let retry_secs = retry_after.map(|d| d.as_secs().max(1)).unwrap_or(1);
                            let response = Response::new(429)
                                .with_header("Retry-After".to_string(), retry_secs.to_string())
                                .with_json(&json!({"error": "rate limited"}))
                                .map_err(|e| WebError::RequestHandlingFailed {
                                    message: e.to_string(),
                                    source: None,
                                })?;
                            return Ok(response.into());
                        }
                        IngressAdmissionDecision::Shed {
                            reason,
                            retry_after,
                        } => {
                            boundary.observe(&attempt, IngressAdmissionOutcome::ShedBy);
                            drop(permit);
                            if let Some(refusal) = IngressRefusalReason::from_edge_shed(reason) {
                                if let Some(response) = self
                                    .state
                                    .record_refusal_or_unavailable(
                                        refusal,
                                        &attempt,
                                        503,
                                        retry_after,
                                    )
                                    .await?
                                {
                                    return Ok(response);
                                }
                            }
                            let retry_secs = retry_after.map(|d| d.as_secs().max(1)).unwrap_or(1);
                            let response = Response::new(503)
                                .with_header("Retry-After".to_string(), retry_secs.to_string())
                                .with_json(&json!({"error": "overloaded"}))
                                .map_err(|e| WebError::RequestHandlingFailed {
                                    message: e.to_string(),
                                    source: None,
                                })?;
                            return Ok(response.into());
                        }
                    }
                }
                submission.ingress_handoff = Some(SubmissionIngressContext {
                    accepted_at_ns: unix_now_nanos(),
                    ingress_key: self.state.config.ingress_key.clone(),
                    batch_index: None,
                    attempt_seq: attempt.attempt_seq,
                });
                permit.send(submission);
                let response = Response::ok()
                    .with_json(&SubmissionResponse {
                        accepted: 1,
                        rejected: 0,
                        errors: Vec::new(),
                    })
                    .map_err(|e| WebError::RequestHandlingFailed {
                        message: e.to_string(),
                        source: None,
                    })?;
                Ok(response.into())
            }
            Err(TrySendError::Full(_)) => {
                if let Some(response) = self
                    .state
                    .record_refusal_or_unavailable(
                        IngressRefusalReason::BufferFull,
                        &attempt,
                        503,
                        Some(Duration::from_secs(1)),
                    )
                    .await?
                {
                    return Ok(response);
                }
                let response = Response::new(503)
                    .with_header("Retry-After".to_string(), "1".to_string())
                    .with_json(&json!({"error": "buffer full"}))
                    .map_err(|e| WebError::RequestHandlingFailed {
                        message: e.to_string(),
                        source: None,
                    })?;
                Ok(response.into())
            }
            Err(TrySendError::Closed(_)) => {
                if let Some(response) = self
                    .state
                    .record_refusal_or_unavailable(
                        IngressRefusalReason::ChannelClosed,
                        &attempt,
                        500,
                        None,
                    )
                    .await?
                {
                    return Ok(response);
                }
                let response = Response::internal_error()
                    .with_json(&json!({"error": "ingestion channel closed"}))
                    .map_err(|e| WebError::RequestHandlingFailed {
                        message: e.to_string(),
                        source: None,
                    })?;
                Ok(response.into())
            }
        }
    }
}
