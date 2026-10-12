// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084l: the hosted HTTP admission path shared by `/events` and `/batch`.

use super::auth::{authorize_request, header_value};
use super::contract::{
    refusal_response, IngressErrorCode, SubmissionMemberOutcome, SubmissionMemberResult,
    SubmissionResponse, INVALID_IDEMPOTENCY_KEY,
};
use super::settlement::EvidenceState;
use super::shared::{
    admit_submission, AdmittedRequest, EdgeRefusal, IngestionState, MemberRefusal,
};
use obzenflow_core::ingress::{
    EventSubmission, IngressAttemptContext, IngressAttemptSeq, IngressRequestId,
    InvalidIngressRequestId,
};
use obzenflow_core::web::{EndpointError, ManagedResponse, Request};
use obzenflow_runtime::stages::IngressDecoder;
use tokio::time::Instant;

const IDEMPOTENCY_KEY: &str = "Idempotency-Key";

/// The outcome of the checks every request makes before its body is admitted.
pub(super) enum Precheck {
    Proceed(Option<IngressRequestId>),
    Answered(ManagedResponse),
}

/// An RFC 8941 string or a bare token whose content is a valid request ID.
pub(super) fn parse_idempotency_key(
    request: &Request,
) -> Result<Option<IngressRequestId>, InvalidIngressRequestId> {
    let Some(raw) = header_value(request, IDEMPOTENCY_KEY) else {
        return Ok(None);
    };
    let raw = raw.trim();
    let content = match raw.strip_prefix('"') {
        Some(quoted) => quoted.strip_suffix('"').ok_or(InvalidIngressRequestId)?,
        None => raw,
    };
    IngressRequestId::parse(content).map(Some)
}

pub(super) async fn precheck<T>(
    state: &IngestionState<T>,
    request: &Request,
    attempt_seq: IngressAttemptSeq,
    batch: bool,
) -> Result<Precheck, EndpointError> {
    let max = state.config.max_body_size;
    if request.body.len() > max {
        return answered(refusal_response(
            413,
            IngressErrorCode::PayloadTooLarge,
            format!("request body exceeds {max} bytes"),
            None,
        ));
    }
    if let Some(auth) = state.config.auth.as_ref() {
        if let Err(error) = authorize_request(auth, request) {
            return answered(refusal_response(
                401,
                IngressErrorCode::Unauthorized,
                error.to_string(),
                None,
            ));
        }
    }
    let Ok(request_id) = parse_idempotency_key(request) else {
        return answered(refusal_response(
            400,
            IngressErrorCode::InvalidIdempotencyKey,
            INVALID_IDEMPOTENCY_KEY,
            None,
        ));
    };
    if !state.is_ready() {
        let attempt = IngressAttemptContext {
            attempt_seq,
            request_count: 1,
            event_count: if batch {
                best_effort_batch_len(&request.body).unwrap_or(1)
            } else {
                1
            },
            batch_count: u64::from(batch),
        };
        let refused = state.refuse_not_ready(&attempt, request_id.as_ref()).await;
        return answered(edge_refusal_response(refused));
    }
    Ok(Precheck::Proceed(request_id))
}

/// A batch's event count for a refusal recorded before full parsing.
fn best_effort_batch_len(body: &[u8]) -> Option<u64> {
    #[derive(serde::Deserialize)]
    struct Count {
        events: Vec<serde::de::IgnoredAny>,
    }
    serde_json::from_slice::<Count>(body)
        .ok()
        .map(|count| count.events.len() as u64)
}

fn answered(response: Result<ManagedResponse, EndpointError>) -> Result<Precheck, EndpointError> {
    response.map(Precheck::Answered)
}

pub(super) fn invalid_body(error: &serde_json::Error) -> Result<ManagedResponse, EndpointError> {
    refusal_response(
        400,
        IngressErrorCode::InvalidBody,
        format!("invalid request body: {error}"),
        None,
    )
}

/// Admit, queue and settle one request's submissions, then answer per member.
pub(super) async fn process<D: IngressDecoder>(
    state: &IngestionState<D::Output>,
    decoder: &D,
    arrived: Instant,
    attempt_seq: IngressAttemptSeq,
    request_id: Option<IngressRequestId>,
    submissions: Vec<EventSubmission>,
    batch: bool,
) -> Result<ManagedResponse, EndpointError> {
    let mut refused: Vec<(usize, MemberRefusal)> = Vec::new();
    let mut members = Vec::new();
    for (position, submission) in submissions.iter().enumerate() {
        match admit_submission(
            decoder,
            state.config.validation.as_ref(),
            submission,
            request_id.as_ref(),
        ) {
            Ok(admitted) => members.push((position, batch.then_some(position), admitted)),
            Err(member) => refused.push((position, member)),
        }
    }

    if !refused.is_empty()
        && state
            .record_validation_refusals(attempt_seq, batch, request_id.as_ref(), &refused)
            .await
            .is_err()
    {
        return edge_refusal_response(EdgeRefusal::EvidenceUnavailable);
    }

    let mut results: Vec<SubmissionMemberResult> = refused
        .into_iter()
        .map(|(position, member)| SubmissionMemberResult {
            index: position,
            outcome: SubmissionMemberOutcome::Rejected {
                code: IngressErrorCode::ValidationFailed,
                error: member.message,
            },
        })
        .collect();

    if !members.is_empty() {
        let attempt = IngressAttemptContext {
            attempt_seq,
            request_count: 1,
            event_count: members.len() as u64,
            batch_count: u64::from(batch),
        };
        let admitted = AdmittedRequest {
            attempt,
            request_id: request_id.clone(),
            members,
        };
        let settlement = match state.enqueue(admitted).await {
            Ok(settlement) => settlement,
            Err(edge) => return edge_refusal_response(edge),
        };
        let snapshot = settlement.wait_until(arrived + state.ack_deadline()).await;
        if snapshot.evidence == EvidenceState::Unavailable {
            return edge_refusal_response(EdgeRefusal::EvidenceUnavailable);
        }
        results.extend(snapshot.members.iter().map(|(position, settlement)| {
            SubmissionMemberResult {
                index: *position,
                outcome: snapshot.outcome(*settlement, request_id.as_ref()),
            }
        }));
    }
    results.sort_by_key(|result| result.index);
    SubmissionResponse::new(results).into_managed()
}

pub(super) fn edge_refusal_response(edge: EdgeRefusal) -> Result<ManagedResponse, EndpointError> {
    refusal_response(
        edge.status(),
        edge.code(),
        edge.message(),
        Some(edge.retry_after()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::web::HttpMethod;

    fn with_key(value: &str) -> Request {
        Request::new(HttpMethod::Post, "/events".to_string())
            .with_header("idempotency-key".to_string(), value.to_string())
    }

    #[test]
    fn idempotency_keys_accept_quoted_and_bare_forms() {
        let quoted = parse_idempotency_key(&with_key("\"tx-0001\"")).unwrap();
        let bare = parse_idempotency_key(&with_key("tx-0001")).unwrap();
        assert_eq!(quoted, bare);
        assert_eq!(quoted.unwrap().as_str(), "tx-0001");
    }

    #[test]
    fn malformed_idempotency_keys_are_refused() {
        for value in ["\"tx-0001", "\"\"", "tx 1", "\"tx/1\"", &"x".repeat(256)] {
            assert!(parse_idempotency_key(&with_key(value)).is_err(), "{value}");
        }
    }
}
