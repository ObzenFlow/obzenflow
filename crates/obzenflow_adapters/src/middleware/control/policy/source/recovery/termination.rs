// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Maps pre-execution rejection into invocation evidence and a boundary report.

use super::invocation::SourceRetryInvocation;
use crate::middleware::control::policy::retry::RetryWaitError;
use crate::middleware::control::policy::source::execution::{RejectedSourceAttempt, RejectionKind};
use obzenflow_core::event::payloads::observability_payload::RetryExhaustionCause;
use obzenflow_runtime::stages::source::SourceBoundaryReport;
use std::num::NonZeroU32;

pub(super) fn finish_rejection(
    invocation: &mut SourceRetryInvocation,
    attempt: NonZeroU32,
    rejection: RejectedSourceAttempt,
) -> SourceBoundaryReport {
    invocation.extend_events(rejection.control_events);

    match rejection.kind {
        RejectionKind::Policy => {
            if attempt.get() > 1 || invocation.recovery_started() {
                invocation.record_exhausted(
                    attempt.get().saturating_sub(1),
                    RetryExhaustionCause::PolicyRejected,
                    None,
                );
            }
            invocation.finish_rejected(rejection.reason)
        }
        RejectionKind::Interrupted(wait_error) => {
            if matches!(wait_error, RetryWaitError::Deadline)
                || (matches!(wait_error, RetryWaitError::Drain) && invocation.recovery_started())
            {
                let cause = match wait_error {
                    RetryWaitError::Deadline => RetryExhaustionCause::TotalWallTime,
                    RetryWaitError::Drain => RetryExhaustionCause::DrainRequested,
                    RetryWaitError::Abort => unreachable!(),
                };
                invocation.record_exhausted(attempt.get().saturating_sub(1), cause, None);
            }
            if matches!(wait_error, RetryWaitError::Abort) {
                SourceRetryInvocation::abort_rejected(rejection.reason)
            } else {
                invocation.finish_rejected(rejection.reason)
            }
        }
        RejectionKind::Abort => SourceRetryInvocation::abort_rejected(rejection.reason),
    }
}
