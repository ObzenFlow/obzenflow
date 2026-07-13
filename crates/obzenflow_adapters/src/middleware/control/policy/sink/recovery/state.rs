// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::super::retry::{
    attempt_failed_event, exhausted_event, succeeded_event, RetryBudget, RetryWaitError,
};
use super::super::RejectedSinkAttempt;
use crate::middleware::BoundaryRetryOwner;
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryInvocation, RetryLifecycleContext, RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::EventId;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptOutcome, SinkDeliveryBoundaryOutcome, SinkDeliveryBoundaryReport,
    SinkDeliveryRejection,
};
use std::num::NonZeroU32;
use std::time::Duration;

/// Invocation-wide retry bookkeeping. Attempt contexts and guards deliberately
/// live in `attempt`, while this state survives from one physical call to the next.
pub(super) struct SinkRetryInvocation {
    owner: BoundaryRetryOwner,
    lifecycle: RetryLifecycleContext,
    budget: RetryBudget,
    parent_event_id: EventId,
    attempt: NonZeroU32,
    recovery_started: bool,
    control_events: Vec<ChainEvent>,
}

impl SinkRetryInvocation {
    pub(super) fn new(
        owner: BoundaryRetryOwner,
        parent_event_id: EventId,
    ) -> Result<Self, SinkDeliveryBoundaryReport> {
        let Some(budget) = RetryBudget::new(owner.policy.clone()) else {
            return Err(SinkDeliveryBoundaryReport {
                outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                    policy: "circuit_breaker".to_string(),
                    reason: format!(
                        "circuit_breaker retry rejected for sink-delivery '{}': max_total_wall_time cannot be represented from the current monotonic instant; choose a smaller limit",
                        owner.protected_unit_label
                    ),
                }),
                control_events: Vec::new(),
            });
        };
        let lifecycle = RetryLifecycleContext {
            stage_id: owner.stage_id,
            attachment_id: owner.attachment_id.as_ulid(),
            protected_unit: RetryProtectedUnit::SinkDelivery {
                configured_target_id: owner.sink_configured_target_id,
            },
            invocation: RetryInvocation::SinkDelivery { parent_event_id },
        };
        Ok(Self {
            owner,
            lifecycle,
            budget,
            parent_event_id,
            attempt: NonZeroU32::MIN,
            recovery_started: false,
            control_events: Vec::new(),
        })
    }

    pub(super) fn budget(&self) -> &RetryBudget {
        &self.budget
    }

    pub(super) fn append_attempt_events(&mut self, events: Vec<ChainEvent>) {
        self.control_events.extend(events);
    }

    pub(super) fn next_attempt(&self) -> Option<NonZeroU32> {
        self.budget.next_attempt(self.attempt)
    }

    pub(super) fn delay_after(
        &self,
        retry_after: Option<Duration>,
    ) -> Result<Duration, RetryExhaustionCause> {
        self.budget.delay_after(self.attempt, retry_after)
    }

    pub(super) fn record_attempt_failed(&mut self, kind: ErrorKind, delay: Duration) {
        self.control_events.push(attempt_failed_event(
            &self.owner,
            &self.lifecycle,
            self.attempt,
            kind,
            delay,
            &self.budget,
            Some(self.parent_event_id),
        ));
        self.recovery_started = true;
    }

    pub(super) fn advance(&mut self, next_attempt: NonZeroU32) {
        self.attempt = next_attempt;
    }

    pub(super) fn finish_rejected(
        mut self,
        rejected: RejectedSinkAttempt,
    ) -> SinkDeliveryBoundaryReport {
        self.control_events.extend(rejected.control_events);
        match rejected.wait_error {
            None if self.recovery_started => self.push_exhausted(
                self.attempt.get().saturating_sub(1),
                RetryExhaustionCause::PolicyRejected,
                None,
            ),
            Some(RetryWaitError::Deadline) => self.push_exhausted(
                self.attempt.get().saturating_sub(1),
                RetryExhaustionCause::TotalWallTime,
                None,
            ),
            Some(RetryWaitError::Drain) if self.recovery_started => self.push_exhausted(
                self.attempt.get().saturating_sub(1),
                RetryExhaustionCause::DrainRequested,
                None,
            ),
            None | Some(RetryWaitError::Drain | RetryWaitError::Abort) => {}
        }
        SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Rejected(rejected.rejection),
            control_events: if matches!(rejected.wait_error, Some(RetryWaitError::Abort)) {
                Vec::new()
            } else {
                self.control_events
            },
        }
    }

    pub(super) fn finish_active_abort(self) -> SinkDeliveryBoundaryReport {
        SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                policy: "retry_coordinator".to_string(),
                reason: "force abort requested during sink delivery".to_string(),
            }),
            control_events: Vec::new(),
        }
    }

    pub(super) fn finish_deadline_outcome_unknown(mut self) -> SinkDeliveryBoundaryReport {
        self.push_exhausted(
            self.attempt.get(),
            RetryExhaustionCause::TotalWallTime,
            Some(ErrorKind::Timeout),
        );
        SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::DeadlineOutcomeUnknown {
                message: "sink deadline expired while the external outcome remained in doubt"
                    .to_string(),
            },
            control_events: self.control_events,
        }
    }

    pub(super) fn finish_completed(
        mut self,
        outcome: SinkDeliveryAttemptOutcome,
    ) -> SinkDeliveryBoundaryReport {
        if self.recovery_started {
            self.control_events.push(succeeded_event(
                &self.owner,
                &self.lifecycle,
                self.attempt.get(),
                &self.budget,
                Some(self.parent_event_id),
            ));
        }
        self.finish_attempted(outcome)
    }

    pub(super) fn finish_terminal(
        mut self,
        outcome: SinkDeliveryAttemptOutcome,
        kind: ErrorKind,
    ) -> SinkDeliveryBoundaryReport {
        if self.recovery_started {
            self.push_exhausted(
                self.attempt.get(),
                RetryExhaustionCause::TerminalFailure,
                Some(kind),
            );
        }
        self.finish_attempted(outcome)
    }

    pub(super) fn finish_retry_exhausted(
        mut self,
        outcome: SinkDeliveryAttemptOutcome,
        cause: RetryExhaustionCause,
        kind: ErrorKind,
    ) -> SinkDeliveryBoundaryReport {
        self.push_exhausted(self.attempt.get(), cause, Some(kind));
        self.finish_attempted(outcome)
    }

    pub(super) fn finish_backoff_interrupted(
        mut self,
        outcome: SinkDeliveryAttemptOutcome,
        wait_error: RetryWaitError,
        kind: ErrorKind,
    ) -> SinkDeliveryBoundaryReport {
        match wait_error {
            RetryWaitError::Deadline => self.push_exhausted(
                self.attempt.get(),
                RetryExhaustionCause::TotalWallTime,
                Some(kind),
            ),
            RetryWaitError::Drain => self.push_exhausted(
                self.attempt.get(),
                RetryExhaustionCause::DrainRequested,
                Some(kind),
            ),
            RetryWaitError::Abort => {
                return SinkDeliveryBoundaryReport {
                    outcome: SinkDeliveryBoundaryOutcome::Attempted(outcome),
                    control_events: Vec::new(),
                };
            }
        }
        self.finish_attempted(outcome)
    }

    fn push_exhausted(
        &mut self,
        total_attempts: u32,
        cause: RetryExhaustionCause,
        kind: Option<ErrorKind>,
    ) {
        self.control_events.push(exhausted_event(
            &self.owner,
            &self.lifecycle,
            total_attempts,
            cause,
            kind,
            &self.budget,
            Some(self.parent_event_id),
        ));
    }

    fn finish_attempted(self, outcome: SinkDeliveryAttemptOutcome) -> SinkDeliveryBoundaryReport {
        SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Attempted(outcome),
            control_events: self.control_events,
        }
    }
}
