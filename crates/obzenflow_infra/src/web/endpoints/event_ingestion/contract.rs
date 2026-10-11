// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084l: the durable admission contract's codes, outcomes and HTTP shapes.

use obzenflow_core::event::{SourceDiagnostic, SourceDiagnosticReason};
use obzenflow_core::ingress::{IngressRequestId, OUTCOME_UNKNOWN_RETRY_AFTER};
use obzenflow_core::web::{EndpointError, ManagedResponse, Response};
use obzenflow_core::EventId;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Closed set of ingress refusal and outcome codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IngressErrorCode {
    InvalidBody,
    InvalidIdempotencyKey,
    Unauthorized,
    PayloadTooLarge,
    BatchTooLarge,
    RateLimited,
    NotReady,
    BufferFull,
    Overloaded,
    ListenerUnavailable,
    ShuttingDown,
    ValidationFailed,
    IdempotencyKeyConflict,
    OutcomeUnknown,
}

impl IngressErrorCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidBody => "invalid_body",
            Self::InvalidIdempotencyKey => "invalid_idempotency_key",
            Self::Unauthorized => "unauthorized",
            Self::PayloadTooLarge => "payload_too_large",
            Self::BatchTooLarge => "batch_too_large",
            Self::RateLimited => "rate_limited",
            Self::NotReady => "not_ready",
            Self::BufferFull => "buffer_full",
            Self::Overloaded => "overloaded",
            Self::ListenerUnavailable => "listener_unavailable",
            Self::ShuttingDown => "shutting_down",
            Self::ValidationFailed => "validation_failed",
            Self::IdempotencyKeyConflict => "idempotency_key_conflict",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }

    /// Whether the same request may succeed when sent again.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::RateLimited
                | Self::NotReady
                | Self::BufferFull
                | Self::Overloaded
                | Self::ListenerUnavailable
                | Self::ShuttingDown
                | Self::OutcomeUnknown
        )
    }
}

/// The result of one in-process submission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressOutcome {
    /// The value's row committed.
    Committed { event_id: EventId },
    /// An equal request already committed; `event_id` names its row.
    Duplicate { event_id: EventId },
    /// Not admitted. `retry_after` is set when the same request may succeed
    /// later; a final refusal carries `None`.
    Refused {
        code: IngressErrorCode,
        retry_after: Option<Duration>,
    },
    /// The commit outcome is unknown; resend the identical request.
    Unknown { retry_after: Duration },
}

/// Per-member result of a processed request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum SubmissionMemberOutcome {
    Committed {
        event_id: EventId,
    },
    Duplicate {
        event_id: EventId,
    },
    Rejected {
        code: IngressErrorCode,
        error: String,
    },
    Unknown {
        code: IngressErrorCode,
        error: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionMemberResult {
    pub index: usize,
    #[serde(flatten)]
    pub outcome: SubmissionMemberOutcome,
}

/// Result shape shared by single and batch requests once members are processed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubmissionResponse {
    pub committed: usize,
    pub duplicates: usize,
    pub rejected: usize,
    pub unknown: usize,
    pub results: Vec<SubmissionMemberResult>,
}

impl SubmissionResponse {
    pub fn new(results: Vec<SubmissionMemberResult>) -> Self {
        let count = |pick: fn(&SubmissionMemberOutcome) -> bool| {
            results
                .iter()
                .filter(|result| pick(&result.outcome))
                .count()
        };
        Self {
            committed: count(|outcome| {
                matches!(outcome, SubmissionMemberOutcome::Committed { .. })
            }),
            duplicates: count(|outcome| {
                matches!(outcome, SubmissionMemberOutcome::Duplicate { .. })
            }),
            rejected: count(|outcome| matches!(outcome, SubmissionMemberOutcome::Rejected { .. })),
            unknown: count(|outcome| matches!(outcome, SubmissionMemberOutcome::Unknown { .. })),
            results,
        }
    }

    /// Unknown wins, then conflict, then nothing admitted, otherwise accepted.
    pub fn status(&self) -> u16 {
        let conflict = self.results.iter().any(|result| {
            matches!(
                result.outcome,
                SubmissionMemberOutcome::Rejected {
                    code: IngressErrorCode::IdempotencyKeyConflict,
                    ..
                }
            )
        });
        if self.unknown > 0 {
            503
        } else if conflict {
            422
        } else if self.committed + self.duplicates == 0 {
            400
        } else {
            202
        }
    }

    pub(crate) fn into_managed(self) -> Result<ManagedResponse, EndpointError> {
        let status = self.status();
        let mut response = Response::new(status);
        if status == 503 {
            response = response.with_header(
                "Retry-After".to_string(),
                OUTCOME_UNKNOWN_RETRY_AFTER.as_secs().to_string(),
            );
        }
        Ok(response
            .with_json(&self)
            .map_err(|error| EndpointError::with_source("Serialising endpoint response", error))?
            .into())
    }
}

/// Request-level refusal envelope, used before any member is processed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngressErrorBody {
    pub error: String,
    pub code: IngressErrorCode,
    pub retryable: bool,
}

pub(crate) fn refusal_response(
    status: u16,
    code: IngressErrorCode,
    error: impl Into<String>,
    retry_after: Option<Duration>,
) -> Result<ManagedResponse, EndpointError> {
    let mut response = Response::new(status);
    if let Some(delay) = retry_after {
        response = response.with_header(
            "Retry-After".to_string(),
            retry_after_secs(delay).to_string(),
        );
    }
    Ok(response
        .with_json(&IngressErrorBody {
            error: error.into(),
            code,
            retryable: code.is_retryable(),
        })
        .map_err(|error| EndpointError::with_source("Serialising endpoint response", error))?
        .into())
}

/// Whole seconds, at least one, as `Retry-After` carries.
pub(crate) fn retry_after_secs(delay: Duration) -> u64 {
    delay.as_secs().max(1)
}

pub(crate) const INVALID_IDEMPOTENCY_KEY: &str = "Idempotency-Key must be 1 to 255 characters of A-Z, a-z, 0-9, '.', '_', ':' or '-', quoted or bare";
pub(crate) const NOT_READY: &str = "ingress is not ready; retry once the pipeline is live";
pub(crate) const BUFFER_FULL: &str = "ingress buffer is full";
pub(crate) const OVERLOADED: &str = "ingress is overloaded";
pub(crate) const LISTENER_UNAVAILABLE: &str = "ingress listener unavailable";
pub(crate) const SHUTTING_DOWN: &str =
    "ingress is shutting down; retry with the same Idempotency-Key";
pub(crate) const RATE_LIMITED: &str = "rate limited";

pub(crate) fn unknown_member(keyed: bool) -> SubmissionMemberOutcome {
    SubmissionMemberOutcome::Unknown {
        code: IngressErrorCode::OutcomeUnknown,
        error: if keyed {
            "commit outcome unknown; retry with the same Idempotency-Key".to_string()
        } else {
            "commit outcome unknown; without an Idempotency-Key a retry may be admitted twice"
                .to_string()
        },
    }
}

pub(crate) fn conflict_member(id: &IngressRequestId) -> SubmissionMemberOutcome {
    SubmissionMemberOutcome::Rejected {
        code: IngressErrorCode::IdempotencyKeyConflict,
        error: format!("Idempotency-Key \"{id}\" was already used with different content"),
    }
}

/// The decoder's report about the sender's own payload, quoted back to that
/// sender only; it is never journalled.
pub(crate) fn diagnostic_message(diagnostic: &SourceDiagnostic) -> String {
    let reason = diagnostic.reason();
    match diagnostic.location().field_path_text() {
        None => reason.phrase().to_string(),
        Some(path)
            if matches!(
                reason,
                SourceDiagnosticReason::MissingField | SourceDiagnosticReason::UnknownField
            ) =>
        {
            format!("{} `{path}`", reason.phrase())
        }
        Some(path) => format!("{} at `{path}`", reason.phrase()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn committed(index: usize) -> SubmissionMemberResult {
        SubmissionMemberResult {
            index,
            outcome: SubmissionMemberOutcome::Committed {
                event_id: EventId::new(),
            },
        }
    }

    fn rejected(index: usize, code: IngressErrorCode) -> SubmissionMemberResult {
        SubmissionMemberResult {
            index,
            outcome: SubmissionMemberOutcome::Rejected {
                code,
                error: "x".into(),
            },
        }
    }

    #[test]
    fn status_follows_the_recorded_precedence() {
        let unknown = SubmissionMemberResult {
            index: 1,
            outcome: unknown_member(true),
        };
        assert_eq!(
            SubmissionResponse::new(vec![committed(0), unknown.clone()]).status(),
            503
        );
        assert_eq!(
            SubmissionResponse::new(vec![
                committed(0),
                rejected(1, IngressErrorCode::IdempotencyKeyConflict)
            ])
            .status(),
            422
        );
        assert_eq!(
            SubmissionResponse::new(vec![rejected(0, IngressErrorCode::ValidationFailed)]).status(),
            400
        );
        assert_eq!(
            SubmissionResponse::new(vec![
                committed(0),
                rejected(1, IngressErrorCode::ValidationFailed)
            ])
            .status(),
            202
        );
    }

    #[test]
    fn member_results_flatten_into_the_documented_shape() {
        let event_id = EventId::new();
        let response = SubmissionResponse::new(vec![
            SubmissionMemberResult {
                index: 0,
                outcome: SubmissionMemberOutcome::Duplicate { event_id },
            },
            rejected(1, IngressErrorCode::ValidationFailed),
        ]);
        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "committed": 0, "duplicates": 1, "rejected": 1, "unknown": 0,
                "results": [
                    {"index": 0, "outcome": "duplicate", "event_id": event_id},
                    {"index": 1, "outcome": "rejected", "code": "validation_failed", "error": "x"}
                ]
            })
        );
    }

    #[test]
    fn decoder_reports_name_the_sender_field() {
        let missing = SourceDiagnostic::new(SourceDiagnosticReason::MissingField)
            .within_field("amount_cents");
        assert_eq!(diagnostic_message(&missing), "missing field `amount_cents`");
        let unrepresentable = SourceDiagnostic::new(SourceDiagnosticReason::UnrepresentableValue)
            .within_field("tags");
        assert_eq!(
            diagnostic_message(&unrepresentable),
            "the decoded value cannot be recorded faithfully at `tags`"
        );
        assert_eq!(
            diagnostic_message(&SourceDiagnostic::new(
                SourceDiagnosticReason::InvalidRecord
            )),
            "invalid record"
        );
    }

    #[test]
    fn only_transient_codes_are_retryable() {
        assert!(IngressErrorCode::BufferFull.is_retryable());
        assert!(IngressErrorCode::OutcomeUnknown.is_retryable());
        assert!(!IngressErrorCode::ValidationFailed.is_retryable());
        assert!(!IngressErrorCode::IdempotencyKeyConflict.is_retryable());
        assert_eq!(
            serde_json::to_value(IngressErrorCode::IdempotencyKeyConflict).unwrap(),
            json!("idempotency_key_conflict")
        );
    }
}
