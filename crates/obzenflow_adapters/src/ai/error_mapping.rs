// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::AiClientError;
use obzenflow_core::event::{
    EffectFailureCode, EffectFailureSource, RetryDisposition, StageFatalCode, StageFatalReason,
};
use obzenflow_runtime::effects::EffectError;
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};

/// Map an AI port failure into durable effect failure evidence.
pub(crate) fn ai_client_error_to_effect_error(
    error: AiClientError,
    port: &'static str,
    failure_source: &'static str,
) -> EffectError {
    match error {
        AiClientError::TargetMismatch { requested, bound } => {
            EffectError::EffectPortBindingInvariantViolation {
                port: port.to_string(),
                expected: bound.to_string(),
                observed: requested.to_string(),
            }
        }
        AiClientError::Timeout { message } => dependency(failure_source, "timeout", message),
        AiClientError::Remote { message } => dependency(failure_source, "remote", message),
        AiClientError::RateLimited { message, .. } => {
            dependency(failure_source, "rate_limited", message)
        }
        AiClientError::Auth { message } => dependency(failure_source, "authentication", message),
        AiClientError::InvalidRequest { message } => {
            dependency(failure_source, "invalid_request", message)
        }
        AiClientError::Unsupported { message } => {
            dependency(failure_source, "unsupported", message)
        }
        AiClientError::Other { message } => dependency(failure_source, "other", message),
    }
}

fn dependency(source: &'static str, code: &'static str, message: String) -> EffectError {
    EffectError::DependencyFailed {
        failure_source: EffectFailureSource::new(source),
        code: EffectFailureCode::new(code),
        message,
        retry: RetryDisposition::NotRetryable,
    }
}

fn fatal(
    code: StageFatalCode,
    reason: StageFatalReason,
    detail: impl Into<String>,
) -> HandlerError {
    HandlerError::Fatal(StageFatal::new(code, reason, detail))
}

/// Shared public projection from effect-runtime failures to handler failures.
///
/// Generated and standalone AI handlers intentionally use this exact mapping
/// so live and replayed provider failures cannot acquire surface-specific
/// classifications.
pub fn effect_error_to_handler_error(error: EffectError) -> HandlerError {
    match error {
        EffectError::MissingEffectPort { name, .. } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortRegistrationMissing,
            format!("required effect port '{name}' is not registered"),
        ),
        EffectError::EffectPortResolutionFailed { name, message, .. } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortResolutionFailed,
            format!("effect port '{name}' failed to resolve: {message}"),
        ),
        EffectError::EffectPortBindingMismatch {
            port,
            expected,
            observed,
        } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortBindingMismatch,
            format!(
                "effect port '{port}' binding mismatch: expected {expected}, observed {observed}"
            ),
        ),
        EffectError::EffectPortBindingInvariantViolation {
            port,
            expected,
            observed,
        } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortTargetInvariantViolation,
            format!(
                "effect port '{port}' target invariant failed: expected {expected}, observed {observed}"
            ),
        ),
        error @ (EffectError::MissingRecordedEffect { .. }
        | EffectError::EffectInDoubt { .. }
        | EffectError::DuplicateRecordedEffect { .. }
        | EffectError::DescriptorMismatch { .. }
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
        EffectError::Journal(message) => fatal(
            StageFatalCode::Journal,
            StageFatalReason::JournalFailure,
            message,
        ),
        EffectError::DependencyFailed { message, .. }
        | EffectError::BoundaryRejected { message, .. }
        | EffectError::RecoveryAbandoned { message, .. }
        | EffectError::RecordedFailure {
            error_message: message,
            ..
        } => HandlerError::Remote(message),
        other => HandlerError::Other(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::{
        EffectFailureCode, EffectFailureKind, EffectFailureSource, RetryDisposition,
    };
    #[test]
    fn live_and_replayed_provider_failures_have_the_same_handler_classification() {
        let live = effect_error_to_handler_error(EffectError::DependencyFailed {
            failure_source: EffectFailureSource::new("chat_client"),
            code: EffectFailureCode::new("remote"),
            message: "provider unavailable".to_string(),
            retry: RetryDisposition::NotRetryable,
        });
        let replay = effect_error_to_handler_error(EffectError::RecordedFailure {
            error_type: EffectFailureKind::new("remote"),
            error_message: "provider unavailable".to_string(),
            retry: RetryDisposition::NotRetryable,
            cause: None,
            detail: None,
        });

        assert!(matches!(live, HandlerError::Remote(message) if message == "provider unavailable"));
        assert!(
            matches!(replay, HandlerError::Remote(message) if message == "provider unavailable")
        );
    }
}
