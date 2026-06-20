// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::shared::{join_path, unix_now_nanos};
use super::IngestionState;
use super::{authorize_request, validate_submission};
use async_trait::async_trait;
use obzenflow_core::ingress::{BatchSubmission, SubmissionIngressContext, SubmissionResponse};
use obzenflow_core::ingress::{
    IngressAdmissionDecision, IngressAdmissionOutcome, IngressAttemptContext, IngressRefusalReason,
};
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use serde_json::json;
use std::time::Duration;
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

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        // FLOWIP-115d: one attempt sequence for the whole batch, allocated up
        // front. Accepted rows share it (ordered by batch_index); refusal facts
        // carry it as the cross-journal merge key.
        let attempt_seq = self.state.next_attempt_seq();

        // Protocol 4xx rejects (payload-too-large, auth, invalid-json, oversize
        // batch) are not journalled; they always return a 4xx the HTTP-surface
        // metrics record.
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

        // Not-ready sheds the whole batch before validation: journal it.
        if !self.state.is_ready() {
            let count = best_effort_batch_len(&request.body).unwrap_or(1) as u64;
            let attempt = IngressAttemptContext {
                attempt_seq,
                request_count: 1,
                event_count: count,
                batch_count: 1,
            };
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

        let submission: BatchSubmission = match serde_json::from_slice(&request.body) {
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
            return Ok(response.into());
        }

        let mut accepted_events = Vec::new();
        let mut rejected = 0usize;
        let mut errors = Vec::new();

        if let Some(ref validation) = self.state.config.validation {
            for (batch_index, event) in submission.events.into_iter().enumerate() {
                match validate_submission(&event, validation) {
                    Ok(()) => accepted_events.push((batch_index, event)),
                    Err(e) => {
                        rejected += 1;
                        errors.push(format!("{}: {}", event.event_type, e.to_message()));
                    }
                }
            }
        } else {
            accepted_events = submission.events.into_iter().enumerate().collect();
        }

        // FLOWIP-115d: per-event validation rejections are journalled once for the
        // whole batch. A partially accepted batch returns 200, so these are
        // otherwise invisible to the bucketed HTTP-surface 4xx metric.
        if rejected > 0 {
            let attempt = IngressAttemptContext {
                attempt_seq,
                request_count: 1,
                event_count: rejected as u64,
                batch_count: 1,
            };
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
            return Ok(response.into());
        }

        // The accepted subset's terminal fact (buffer-full, channel-closed,
        // rate-limited, or edge-shed) carries the accepted-event count and shares
        // the batch attempt sequence.
        let subset = IngressAttemptContext {
            attempt_seq,
            request_count: 1,
            event_count: accepted as u64,
            batch_count: 1,
        };

        let mut permits = Vec::with_capacity(accepted);
        for _ in 0..accepted {
            match self.state.tx.try_reserve() {
                Ok(permit) => permits.push(permit),
                Err(TrySendError::Full(_)) => {
                    if let Some(response) = self
                        .state
                        .record_refusal_or_unavailable(
                            IngressRefusalReason::BufferFull,
                            &subset,
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
                    return Ok(response.into());
                }
                Err(TrySendError::Closed(_)) => {
                    if let Some(response) = self
                        .state
                        .record_refusal_or_unavailable(
                            IngressRefusalReason::ChannelClosed,
                            &subset,
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
                    return Ok(response.into());
                }
            }
        }

        // FLOWIP-115d: one fail-fast ingress admission for the whole
        // validation-accepted subset, charging one token per accepted event so
        // `/batch` cannot bypass the `/events` admission scope. The edge already
        // reserved one permit per accepted event (buffer-full/503 had
        // precedence); on refusal, drop all permits and refuse the subset
        // atomically with `accepted: 0`.
        if let Some(boundary) = self.state.ingress_boundary() {
            match boundary.on_ingress(&subset) {
                IngressAdmissionDecision::Accept => {
                    boundary.observe(&subset, IngressAdmissionOutcome::AcceptedForEnqueue);
                }
                IngressAdmissionDecision::Reject { retry_after } => {
                    boundary.observe(&subset, IngressAdmissionOutcome::RejectedBy);
                    drop(permits);
                    if let Some(response) = self
                        .state
                        .record_refusal_or_unavailable(
                            IngressRefusalReason::RateLimited,
                            &subset,
                            429,
                            retry_after,
                        )
                        .await?
                    {
                        return Ok(response);
                    }
                    let mut errors = errors;
                    errors.push(format!(
                        "rate limited: {accepted} validation-accepted events not admitted"
                    ));
                    let retry_secs = retry_after.map(|d| d.as_secs().max(1)).unwrap_or(1);
                    let response = Response::new(429)
                        .with_header("Retry-After".to_string(), retry_secs.to_string())
                        .with_json(&SubmissionResponse {
                            accepted: 0,
                            rejected: rejected + accepted,
                            errors,
                        })
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
                    boundary.observe(&subset, IngressAdmissionOutcome::ShedBy);
                    drop(permits);
                    if let Some(refusal) = IngressRefusalReason::from_edge_shed(reason) {
                        if let Some(response) = self
                            .state
                            .record_refusal_or_unavailable(refusal, &subset, 503, retry_after)
                            .await?
                        {
                            return Ok(response);
                        }
                    }
                    let mut errors = errors;
                    errors.push(format!("overloaded: {accepted} events not admitted"));
                    let retry_secs = retry_after.map(|d| d.as_secs().max(1)).unwrap_or(1);
                    let response = Response::new(503)
                        .with_header("Retry-After".to_string(), retry_secs.to_string())
                        .with_json(&SubmissionResponse {
                            accepted: 0,
                            rejected: rejected + accepted,
                            errors,
                        })
                        .map_err(|e| WebError::RequestHandlingFailed {
                            message: e.to_string(),
                            source: None,
                        })?;
                    return Ok(response.into());
                }
            }
        }

        let accepted_at_ns = unix_now_nanos();
        let ingress_key = self.state.config.ingress_key.clone();
        for (permit, (batch_index, mut event)) in permits.into_iter().zip(accepted_events) {
            event.ingress_handoff = Some(SubmissionIngressContext {
                accepted_at_ns,
                ingress_key: ingress_key.clone(),
                batch_index: Some(batch_index),
                attempt_seq,
            });
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
        Ok(response.into())
    }
}
