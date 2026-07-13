// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::super::retry::{BoundaryRetryOwner, RetryWaitError};
use super::super::disposition::abort_reason_from_cause;
use super::attempt::AdmissionExit;
use super::invocation::RetryInvocationState;
use crate::middleware::MiddlewareAbortCause;
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    EffectFailureCause, EffectFailureCode, EffectFailureSource, RetryDisposition,
};
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundaryOutcome, EffectBoundaryReport, EffectError,
};

pub(super) enum InterruptionPhase {
    Admission,
    BeforeExecutorStart,
}

pub(super) fn interruption_cause(
    error: RetryWaitError,
    phase: InterruptionPhase,
) -> MiddlewareAbortCause {
    let (code, message) = match (error, phase) {
        (RetryWaitError::Deadline, InterruptionPhase::Admission) => (
            "retry_total_wall_time",
            "retry total-wall deadline expired during effect policy admission",
        ),
        (RetryWaitError::Drain, InterruptionPhase::Admission) => (
            "drain_requested",
            "graceful drain requested during effect policy admission",
        ),
        (RetryWaitError::Abort, InterruptionPhase::Admission) => (
            "force_aborted",
            "force abort requested during effect policy admission",
        ),
        (RetryWaitError::Deadline, InterruptionPhase::BeforeExecutorStart) => (
            "retry_total_wall_time",
            "retry total-wall deadline expired before effect executor start",
        ),
        (RetryWaitError::Drain, InterruptionPhase::BeforeExecutorStart) => (
            "drain_requested",
            "graceful drain requested before effect executor start",
        ),
        (RetryWaitError::Abort, InterruptionPhase::BeforeExecutorStart) => (
            "force_aborted",
            "force abort requested before effect executor start",
        ),
    };
    MiddlewareAbortCause {
        source: EffectFailureSource::new("effect_boundary"),
        code: EffectFailureCode::new(code),
        message: message.to_string(),
        retry: RetryDisposition::NotRetryable,
        event: None,
    }
}

pub(super) fn aborted(cause: MiddlewareAbortCause) -> EffectBoundaryOutcome {
    EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause))
}

pub(super) fn unrepresentable_budget(owner: &BoundaryRetryOwner) -> EffectBoundaryReport {
    EffectBoundaryReport {
        outcome: EffectBoundaryOutcome::Aborted(EffectAbortReason {
            cause: EffectFailureCause {
                source: EffectFailureSource::new("effect_boundary"),
                code: EffectFailureCode::new("retry_deadline_unrepresentable"),
            },
            message: format!(
                "circuit_breaker retry rejected for effect '{}': max_total_wall_time cannot be represented from the current monotonic instant; choose a smaller limit",
                owner.protected_unit_label
            ),
            retry: RetryDisposition::NotRetryable,
        }),
        control_events: Vec::new(),
    }
}

pub(super) fn deadline_outcome_unknown_error() -> EffectError {
    EffectError::BoundaryRejected {
        rejected_by: EffectFailureSource::new("effect_boundary"),
        code: EffectFailureCode::new("deadline_outcome_unknown"),
        message: "effect deadline expired while the external outcome remained in doubt".to_string(),
        retry: RetryDisposition::NotRetryable,
    }
}

pub(super) fn deadline_outcome_unknown() -> EffectBoundaryOutcome {
    EffectBoundaryOutcome::Aborted(EffectAbortReason {
        cause: EffectFailureCause {
            source: EffectFailureSource::new("effect_boundary"),
            code: EffectFailureCode::new("deadline_outcome_unknown"),
        },
        message: "effect deadline expired while the external outcome remained in doubt".to_string(),
        retry: RetryDisposition::NotRetryable,
    })
}

pub(super) fn force_aborted_during_execution() -> EffectBoundaryOutcome {
    EffectBoundaryOutcome::Aborted(EffectAbortReason {
        cause: EffectFailureCause {
            source: EffectFailureSource::new("effect_boundary"),
            code: EffectFailureCode::new("force_aborted"),
        },
        message: "force abort requested during effect execution".to_string(),
        retry: RetryDisposition::NotRetryable,
    })
}

pub(super) fn finish_admission_exit(
    mut invocation: RetryInvocationState,
    exit: AdmissionExit,
) -> EffectBoundaryReport {
    match exit {
        AdmissionExit::Synthesized {
            results,
            source,
            control_events,
        } => {
            invocation.extend_events(control_events);
            invocation.exhaust_previous_if_recovering(RetryExhaustionCause::PolicyRejected);
            invocation.report(EffectBoundaryOutcome::Skipped {
                results,
                source: Some(source),
            })
        }
        AdmissionExit::Rejected {
            cause,
            control_events,
        } => {
            invocation.extend_events(control_events);
            invocation.exhaust_previous_if_recovering(RetryExhaustionCause::PolicyRejected);
            invocation.report(aborted(cause))
        }
        AdmissionExit::Interrupted {
            wait_error,
            cause,
            control_events,
        } => {
            invocation.extend_events(control_events);
            if matches!(wait_error, RetryWaitError::Deadline) {
                invocation.exhaust(
                    invocation.attempt.get().saturating_sub(1),
                    RetryExhaustionCause::TotalWallTime,
                    None,
                );
            } else if matches!(wait_error, RetryWaitError::Drain) {
                invocation.exhaust_previous_if_recovering(RetryExhaustionCause::DrainRequested);
            }
            let outcome = aborted(cause);
            if matches!(wait_error, RetryWaitError::Abort) {
                invocation.silent_report(outcome)
            } else {
                invocation.report(outcome)
            }
        }
    }
}

pub(super) fn finish_active_drain(
    mut invocation: RetryInvocationState,
    cause: MiddlewareAbortCause,
    control_events: Vec<obzenflow_core::ChainEvent>,
) -> EffectBoundaryReport {
    invocation.extend_events(control_events);
    invocation.exhaust_previous_if_recovering(RetryExhaustionCause::DrainRequested);
    invocation.report(aborted(cause))
}

pub(super) fn finish_active_deadline(mut invocation: RetryInvocationState) -> EffectBoundaryReport {
    invocation.exhaust_current(
        RetryExhaustionCause::TotalWallTime,
        Some(ErrorKind::Timeout),
    );
    invocation.report(deadline_outcome_unknown())
}
