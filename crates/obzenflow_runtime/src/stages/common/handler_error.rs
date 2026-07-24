// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-level error type for stage logic
//!
//! This represents failures that occur inside handlers (sources, transforms,
//! joins, stateful handlers, sinks, observers). Pipeline coordination and
//! FSM control continue to use `StageError`; we provide `From<HandlerError>`
//! so supervisors can map handler failures into stage-level errors when
//! needed.

use crate::stages::common::stage_handle::StageError;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::EventId;
use std::fmt;
use std::time::Duration;

const MAX_FATAL_DETAIL_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageFatal {
    pub code: StageFatalCode,
    pub reason: StageFatalReason,
    pub detail: String,
    pub primary_cause_event_id: Option<EventId>,
}

impl StageFatal {
    pub fn new(code: StageFatalCode, reason: StageFatalReason, detail: impl Into<String>) -> Self {
        let mut detail = detail.into();
        if detail.len() > MAX_FATAL_DETAIL_BYTES {
            let mut boundary = MAX_FATAL_DETAIL_BYTES;
            while !detail.is_char_boundary(boundary) {
                boundary -= 1;
            }
            detail.truncate(boundary);
        }
        Self {
            code,
            reason,
            detail,
            primary_cause_event_id: None,
        }
    }

    pub fn secondary_to(mut self, primary_cause_event_id: EventId) -> Self {
        self.primary_cause_event_id = Some(primary_cause_event_id);
        self
    }
}

/// Error type for handler-level failures.
#[derive(Debug, Clone)]
pub enum HandlerError {
    /// Trusted runtime/protocol invariant failure. Supervisors must record it
    /// on the error journal and fail the stage, never convert it into a normal
    /// error-marked domain input.
    Fatal(StageFatal),
    /// Timeout talking to a remote dependency.
    Timeout(String),
    /// Remote/transport failures (HTTP 5xx, connection refused, etc.).
    Remote(String),
    /// Request was rate limited and may be retried after an optional delay.
    RateLimited {
        message: String,
        retry_after: Option<Duration>,
    },
    /// Permanent failure where retry is not expected to help (auth, bad credentials, etc.).
    PermanentFailure(String),
    /// Unable to deserialize/parse the input payload into the expected type.
    Deserialization(String),
    /// Business validation or rule violation.
    Validation(String),
    /// Broader domain logic failure.
    Domain(String),
    /// Typed AI chunk-planning failure consumed by the sealed map-reduce
    /// planning adapter.
    #[doc(hidden)]
    AiMapReducePlanning(obzenflow_core::ai::AiMapReducePlanningFailure),
    /// A trusted framework or handler contract was contradicted at runtime.
    ///
    /// A call site enforcing a trusted runtime contract may promote this to a
    /// stage-fatal FSM error. The effectful stateful supervisor does so for
    /// `OneFactStageOutput` contradictions and rejection of already-committed
    /// facts by the state fold.
    ContractViolation(String),
    /// Generic or unclassified error.
    Other(String),
}

impl HandlerError {
    /// Convenience constructor for generic errors.
    pub fn other(msg: impl Into<String>) -> Self {
        HandlerError::Other(msg.into())
    }

    /// Map this handler error to a structured ErrorKind.
    pub fn kind(&self) -> ErrorKind {
        match self {
            HandlerError::Fatal(_) => ErrorKind::Unknown,
            HandlerError::Timeout(_) => ErrorKind::Timeout,
            HandlerError::Remote(_) => ErrorKind::Remote,
            HandlerError::RateLimited { .. } => ErrorKind::RateLimited,
            HandlerError::PermanentFailure(_) => ErrorKind::PermanentFailure,
            HandlerError::Deserialization(_) => ErrorKind::Deserialization,
            HandlerError::Validation(_) => ErrorKind::Validation,
            HandlerError::Domain(_) => ErrorKind::Domain,
            HandlerError::AiMapReducePlanning(_) => ErrorKind::Validation,
            // The effectful stateful supervisor intercepts its durable-state
            // contract violations before per-record error routing. `Unknown`
            // is retained for exhaustive callers that still inspect `kind()`.
            HandlerError::ContractViolation(_) => ErrorKind::Unknown,
            HandlerError::Other(_) => ErrorKind::Unknown,
        }
    }

    /// Whether the failure contradicts a trusted contract, allowing the
    /// enforcing call site to promote it above ordinary per-record routing.
    pub fn is_contract_violation(&self) -> bool {
        matches!(self, Self::ContractViolation(_))
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::Fatal(_))
    }

    pub fn as_fatal(&self) -> Option<&StageFatal> {
        match self {
            Self::Fatal(fatal) => Some(fatal),
            _ => None,
        }
    }
}

impl fmt::Display for HandlerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HandlerError::Fatal(fatal) => {
                write!(
                    f,
                    "Fatal {:?}/{:?}: {}",
                    fatal.code, fatal.reason, fatal.detail
                )
            }
            HandlerError::Timeout(msg) => write!(f, "Timeout: {msg}"),
            HandlerError::Remote(msg) => write!(f, "Remote error: {msg}"),
            HandlerError::RateLimited {
                message,
                retry_after,
            } => match retry_after {
                Some(wait) => write!(
                    f,
                    "Rate limited: {message} (retry_after_ms={})",
                    wait.as_millis()
                ),
                None => write!(f, "Rate limited: {message}"),
            },
            HandlerError::PermanentFailure(msg) => write!(f, "Permanent failure: {msg}"),
            HandlerError::Deserialization(msg) => {
                write!(f, "Deserialization error: {msg}")
            }
            HandlerError::Validation(msg) => write!(f, "Validation error: {msg}"),
            HandlerError::Domain(msg) => write!(f, "Domain error: {msg}"),
            HandlerError::AiMapReducePlanning(cause) => {
                write!(f, "AI map-reduce planning failure: {cause:?}")
            }
            HandlerError::ContractViolation(msg) => {
                write!(f, "Contract violation: {msg}")
            }
            HandlerError::Other(msg) => write!(f, "Handler error: {msg}"),
        }
    }
}

impl std::error::Error for HandlerError {}

impl From<crate::effects::EffectError> for HandlerError {
    fn from(error: crate::effects::EffectError) -> Self {
        Self::Other(error.to_string())
    }
}

impl From<HandlerError> for StageError {
    fn from(err: HandlerError) -> Self {
        StageError::handler_failure(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contract_violation_is_distinct_from_record_errors() {
        let error = HandlerError::ContractViolation(
            "one_fact_stage_output: singleton reconstruction failed".to_string(),
        );

        assert!(error.is_contract_violation());
        assert_eq!(error.kind(), ErrorKind::Unknown);
        assert_eq!(
            error.to_string(),
            "Contract violation: one_fact_stage_output: singleton reconstruction failed"
        );
    }
}
