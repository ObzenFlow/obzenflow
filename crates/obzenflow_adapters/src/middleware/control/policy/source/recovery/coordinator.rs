// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Logical source-poll recovery coordination.

use super::continuation::{continue_after_retryable_failure, RetryStep};
use super::invocation::SourceRetryInvocation;
use super::termination::finish_rejection;
use crate::middleware::control::policy::retry::AttemptDisposition;
use crate::middleware::control::policy::source::boundary::PerSourcePolicyBoundary;
use crate::middleware::control::policy::source::disposition::source_attempt_disposition;
use crate::middleware::control::policy::source::execution::{
    execute_physical_attempt, RetryableSourceAttempt,
};
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::source::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollExecutor,
};
use std::num::NonZeroU32;

pub(in crate::middleware::control::policy::source) fn around_retryable_poll<'a>(
    boundary: &'a PerSourcePolicyBoundary,
    execute: &'a mut dyn SourcePollExecutor,
    mut stop: BoundaryStopReceiver,
) -> SourceBoundaryFuture<'a> {
    Box::pin(async move {
        let Some(owner) = boundary.retry_owner() else {
            return boundary.around_poll(execute.attempt(NonZeroU32::MIN)).await;
        };
        let mut invocation = match SourceRetryInvocation::new(owner) {
            Ok(invocation) => invocation,
            Err(reason) => {
                return SourceBoundaryReport {
                    outcome: SourceBoundaryOutcome::Rejected { reason },
                    control_events: Vec::new(),
                };
            }
        };
        let mut attempt_number = NonZeroU32::MIN;

        loop {
            let physical_attempt = execute_physical_attempt(
                boundary.policies(),
                boundary.writer_id(),
                execute,
                attempt_number,
                invocation.budget(),
                &mut stop,
            )
            .await;
            let settled = match physical_attempt {
                RetryableSourceAttempt::Settled(settled) => settled,
                RetryableSourceAttempt::Rejected(rejection) => {
                    return finish_rejection(&mut invocation, attempt_number, rejection);
                }
            };
            invocation.extend_events(settled.control_events);

            match source_attempt_disposition(&settled.poll) {
                AttemptDisposition::Completed => {
                    if invocation.recovery_started() {
                        invocation.record_succeeded(attempt_number.get());
                    }
                    return invocation.finish_polled(settled.poll);
                }
                AttemptDisposition::TerminalFailure { kind } => {
                    if invocation.recovery_started() {
                        invocation.record_exhausted(
                            attempt_number.get(),
                            RetryExhaustionCause::TerminalFailure,
                            Some(kind),
                        );
                    }
                    return invocation.finish_polled(settled.poll);
                }
                AttemptDisposition::RetryableFailure { kind, retry_after } => {
                    match continue_after_retryable_failure(
                        &mut invocation,
                        attempt_number,
                        settled.poll,
                        kind,
                        retry_after,
                        settled.barriers,
                        &mut stop,
                    )
                    .await
                    {
                        RetryStep::Continue(next_attempt) => attempt_number = next_attempt,
                        RetryStep::Finish(report) => return report,
                    }
                }
                AttemptDisposition::NotExecuted => unreachable!(),
            }
        }
    })
}
