// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::EffectError;
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_core::ai::{AiProviderFailureKind, AiUnavailable, AiUnavailableCause};
use obzenflow_core::event::{StageFatalCode, StageFatalReason};

fn fatal(code: StageFatalCode, reason: StageFatalReason, detail: impl Into<String>) -> HandlerError {
    HandlerError::Fatal(StageFatal::new(code, reason, detail))
}

pub(crate) fn integrity_failure(error: EffectError) -> HandlerError {
    match error {
        error @ EffectError::BindingAuthority { .. } => HandlerError::from(error),
        EffectError::Journal(message) => fatal(
            StageFatalCode::Journal,
            StageFatalReason::JournalFailure,
            message,
        ),
        error @ EffectError::RecordingEnded { .. } => fatal(
            StageFatalCode::Replay,
            StageFatalReason::RecordingEnded,
            error.to_string(),
        ),
        error @ EffectError::DescriptorMismatch { .. } => fatal(
            StageFatalCode::Replay,
            StageFatalReason::DescriptorMismatch,
            error.to_string(),
        ),
        error @ EffectError::MissingRecordedEffect { .. } => fatal(
            StageFatalCode::Replay,
            StageFatalReason::MissingRecordedEffect,
            error.to_string(),
        ),
        error @ (EffectError::DuplicateRecordedEffect { .. }
        | EffectError::IncompleteOutcomeGroup { .. }) => fatal(
            StageFatalCode::Replay,
            StageFatalReason::ReplayDivergence,
            error.to_string(),
        ),
        EffectError::Serialization(message)
        | EffectError::EffectProvenanceMismatch(message)
        | EffectError::ReplayArchive(message) => fatal(
            StageFatalCode::Replay,
            StageFatalReason::ReplayDivergence,
            message,
        ),
        other => fatal(
            StageFatalCode::Coordination,
            StageFatalReason::CoordinationFailure,
            other.to_string(),
        ),
    }
}

pub(crate) fn unavailable_or_integrity(error: EffectError) -> Result<AiUnavailable, HandlerError> {
    match error {
        EffectError::BoundaryRejected {
            rejected_by,
            code,
            message,
            ..
        } => Ok(AiUnavailable::new(
            AiUnavailableCause::BoundaryRejected {
                source: rejected_by.to_string(),
                code: code.to_string(),
            },
            message,
        )),
        EffectError::RecoveryAbandoned {
            last_started_attempt,
            failure_source,
            code,
            message,
            ..
        } => Ok(AiUnavailable::new(
            AiUnavailableCause::RecoveryAbandoned {
                last_started_attempt: last_started_attempt.get(),
                source: failure_source.to_string(),
                code: code.to_string(),
            },
            message,
        )),
        EffectError::DependencyFailed { code, message, .. } => Ok(AiUnavailable::new(
            AiUnavailableCause::Provider {
                failure: AiProviderFailureKind::from_code(code.as_str()),
            },
            message,
        )),
        EffectError::RecordedFailure {
            error_type,
            error_message,
            cause,
            detail: None,
            ..
        } if error_type.as_str() == "boundary_rejected" => {
            let cause = cause.ok_or_else(|| {
                fatal(
                    StageFatalCode::Replay,
                    StageFatalReason::ReplayDivergence,
                    "recorded boundary rejection is missing its structured cause",
                )
            })?;
            Ok(AiUnavailable::new(
                AiUnavailableCause::BoundaryRejected {
                    source: cause.source.to_string(),
                    code: cause.code.to_string(),
                },
                error_message,
            ))
        }
        EffectError::RecordedFailure {
            error_type,
            error_message,
            cause,
            detail: None,
            ..
        } if error_type.as_str() != "recovery_abandoned" => {
            let code = cause
                .as_ref()
                .map(|cause| cause.code.as_str())
                .unwrap_or_else(|| error_type.as_str());
            Ok(AiUnavailable::new(
                AiUnavailableCause::Provider {
                    failure: AiProviderFailureKind::from_code(code),
                },
                error_message,
            ))
        }
        other => Err(integrity_failure(other)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::{
        EffectCursor, EffectFailureCause, EffectFailureCode, EffectFailureKind,
        EffectFailureSource, RetryDisposition,
    };

    #[test]
    fn live_and_replayed_provider_failures_reach_the_role_as_one_value() {
        let live = unavailable_or_integrity(EffectError::DependencyFailed {
            failure_source: EffectFailureSource::new("chat_client"),
            code: EffectFailureCode::new("remote"),
            message: "provider unavailable".to_string(),
            retry: RetryDisposition::NotRetryable,
        })
        .unwrap();
        let replayed = unavailable_or_integrity(EffectError::RecordedFailure {
            error_type: EffectFailureKind::new("remote"),
            error_message: "provider unavailable".to_string(),
            retry: RetryDisposition::NotRetryable,
            cause: Some(EffectFailureCause {
                source: EffectFailureSource::new("chat_client"),
                code: EffectFailureCode::new("remote"),
            }),
            detail: None,
        })
        .unwrap();
        assert_eq!(live, replayed);
        assert_eq!(
            live.cause,
            AiUnavailableCause::Provider {
                failure: AiProviderFailureKind::Remote
            }
        );
    }

    #[test]
    fn live_and_replayed_boundary_rejections_keep_their_cause() {
        let live = unavailable_or_integrity(EffectError::BoundaryRejected {
            rejected_by: EffectFailureSource::new("circuit_breaker"),
            code: EffectFailureCode::new("open"),
            message: "breaker open".to_string(),
            retry: RetryDisposition::NotRetryable,
        })
        .unwrap();
        let replayed = unavailable_or_integrity(EffectError::RecordedFailure {
            error_type: EffectFailureKind::new("boundary_rejected"),
            error_message: "breaker open".to_string(),
            retry: RetryDisposition::NotRetryable,
            cause: Some(EffectFailureCause {
                source: EffectFailureSource::new("circuit_breaker"),
                code: EffectFailureCode::new("open"),
            }),
            detail: None,
        })
        .unwrap();
        assert_eq!(live, replayed);
    }

    #[test]
    fn integrity_failures_never_reach_a_role() {
        let error = unavailable_or_integrity(EffectError::RecordingEnded {
            cursor: EffectCursor::new("flow", "stage", 1_u64, 0_u32),
        })
        .expect_err("the end of a recording is an integrity failure");
        let fatal = error.as_fatal().expect("stage-fatal").clone();
        assert_eq!(fatal.code, StageFatalCode::Replay);
        assert_eq!(fatal.reason, StageFatalReason::RecordingEnded);

        let error = unavailable_or_integrity(EffectError::Journal("disk full".to_string()))
            .expect_err("a journal fault is an integrity failure");
        assert!(error.is_fatal());
    }
}
