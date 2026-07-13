// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Decides whether a retryable physical failure may continue recovery.

use super::invocation::SourceRetryInvocation;
use crate::middleware::control::policy::retry::{await_before_execution, RetryWaitError};
use crate::middleware::control::policy::source::execution::AttemptBarriers;
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::source::{SourceBoundaryReport, SourcePollReport};
use std::num::NonZeroU32;
use std::time::Duration;

pub(super) enum RetryStep {
    Continue(NonZeroU32),
    Finish(SourceBoundaryReport),
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn continue_after_retryable_failure(
    invocation: &mut SourceRetryInvocation,
    attempt: NonZeroU32,
    poll: SourcePollReport,
    kind: ErrorKind,
    retry_after: Option<Duration>,
    barriers: AttemptBarriers,
    stop: &mut BoundaryStopReceiver,
) -> RetryStep {
    if let Some(cause) = exhaustion_barrier(invocation, attempt, &barriers) {
        invocation.record_exhausted(attempt.get(), cause, Some(kind));
        return RetryStep::Finish(invocation.finish_polled(poll));
    }

    let delay = match invocation.budget().delay_after(attempt, retry_after) {
        Ok(delay) => delay,
        Err(cause) => {
            invocation.record_exhausted(attempt.get(), cause, Some(kind));
            return RetryStep::Finish(invocation.finish_polled(poll));
        }
    };
    let next_attempt = invocation
        .budget()
        .next_attempt(attempt)
        .expect("retry slot was validated before backoff");
    invocation.record_attempt_failed(attempt, kind.clone(), delay);
    let deadline = invocation.budget().deadline();

    match await_before_execution(tokio::time::sleep(delay), deadline, stop).await {
        Ok(()) => RetryStep::Continue(next_attempt),
        Err(wait_error @ (RetryWaitError::Deadline | RetryWaitError::Drain)) => {
            let cause = match wait_error {
                RetryWaitError::Deadline => RetryExhaustionCause::TotalWallTime,
                RetryWaitError::Drain => RetryExhaustionCause::DrainRequested,
                RetryWaitError::Abort => unreachable!(),
            };
            invocation.record_exhausted(attempt.get(), cause, Some(kind));
            RetryStep::Finish(invocation.finish_polled(poll))
        }
        Err(RetryWaitError::Abort) => RetryStep::Finish(SourceRetryInvocation::abort_polled(poll)),
    }
}

fn exhaustion_barrier(
    invocation: &SourceRetryInvocation,
    attempt: NonZeroU32,
    barriers: &AttemptBarriers,
) -> Option<RetryExhaustionCause> {
    if barriers.active_deadline || barriers.settlement_deadline {
        Some(RetryExhaustionCause::TotalWallTime)
    } else if barriers.drain_requested {
        Some(RetryExhaustionCause::DrainRequested)
    } else if !barriers.recovery_allowed {
        Some(RetryExhaustionCause::PolicyRejected)
    } else if invocation.budget().next_attempt(attempt).is_none() {
        Some(RetryExhaustionCause::MaxAttempts)
    } else {
        None
    }
}
