// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::super::retry::{await_before_execution, AttemptDisposition, RetryWaitError};
use super::super::{
    run_once, run_retryable_attempt, sink_attempt_disposition, PerSinkDeliveryPolicyBoundary,
    RetryableSinkAttempt,
};
use super::SinkRetryInvocation;
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryBoundaryReport, SinkDeliveryExecutor,
};

pub(super) async fn run(
    boundary: &PerSinkDeliveryPolicyBoundary,
    execute: &mut dyn SinkDeliveryExecutor,
    mut stop: BoundaryStopReceiver,
) -> SinkDeliveryBoundaryReport {
    let Some(owner) = boundary.retry_owner() else {
        return run_once(boundary.policies(), execute).await;
    };
    let parent_event_id = execute.parent_event_id();
    let mut invocation = match SinkRetryInvocation::new(owner, parent_event_id) {
        Ok(invocation) => invocation,
        Err(report) => return report,
    };

    loop {
        let physical_attempt =
            run_retryable_attempt(boundary.policies(), execute, invocation.budget(), &mut stop)
                .await;
        let settled = match physical_attempt {
            RetryableSinkAttempt::Settled(settled) => settled,
            RetryableSinkAttempt::Rejected(rejected) => {
                return invocation.finish_rejected(rejected);
            }
            RetryableSinkAttempt::Aborted(_held_guards) => {
                return invocation.finish_active_abort();
            }
        };

        invocation.append_attempt_events(settled.control_events);
        if settled.deadline_outcome_unknown {
            return invocation.finish_deadline_outcome_unknown();
        }

        match sink_attempt_disposition(&settled.outcome) {
            AttemptDisposition::Completed => {
                return invocation.finish_completed(settled.outcome);
            }
            AttemptDisposition::TerminalFailure { kind } => {
                return invocation.finish_terminal(settled.outcome, kind);
            }
            AttemptDisposition::RetryableFailure { kind, retry_after } => {
                let retry_barrier = if settled.drain_requested {
                    Some(RetryExhaustionCause::DrainRequested)
                } else if !settled.recovery_allowed {
                    Some(RetryExhaustionCause::PolicyRejected)
                } else if invocation.next_attempt().is_none() {
                    Some(RetryExhaustionCause::MaxAttempts)
                } else {
                    None
                };
                if let Some(cause) = retry_barrier {
                    return invocation.finish_retry_exhausted(settled.outcome, cause, kind);
                }

                let delay = match invocation.delay_after(retry_after) {
                    Ok(delay) => delay,
                    Err(cause) => {
                        return invocation.finish_retry_exhausted(settled.outcome, cause, kind);
                    }
                };
                let next_attempt = invocation
                    .next_attempt()
                    .expect("retry slot was validated before backoff");
                invocation.record_attempt_failed(kind.clone(), delay);

                match await_before_execution(
                    tokio::time::sleep(delay),
                    invocation.budget().deadline(),
                    &mut stop,
                )
                .await
                {
                    Ok(()) => invocation.advance(next_attempt),
                    Err(wait_error @ (RetryWaitError::Deadline | RetryWaitError::Drain)) => {
                        return invocation.finish_backoff_interrupted(
                            settled.outcome,
                            wait_error,
                            kind,
                        );
                    }
                    Err(RetryWaitError::Abort) => {
                        return invocation.finish_backoff_interrupted(
                            settled.outcome,
                            RetryWaitError::Abort,
                            kind,
                        );
                    }
                }
            }
            AttemptDisposition::NotExecuted => unreachable!(),
        }
    }
}
