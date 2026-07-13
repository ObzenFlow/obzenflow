// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Invocation-wide retry state and lifecycle evidence.

use crate::middleware::control::policy::retry::{
    attempt_failed_event, exhausted_event, succeeded_event, RetryBudget,
};
use crate::middleware::BoundaryRetryOwner;
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryInvocation, RetryLifecycleContext, RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{ChainEvent, EventId};
use obzenflow_runtime::stages::source::{
    SourceBoundaryOutcome, SourceBoundaryReport, SourcePollReport,
};
use std::num::NonZeroU32;
use std::time::Duration;

pub(super) struct SourceRetryInvocation {
    owner: BoundaryRetryOwner,
    budget: RetryBudget,
    lifecycle: RetryLifecycleContext,
    poll_id: EventId,
    events: Vec<ChainEvent>,
    recovery_started: bool,
}

impl SourceRetryInvocation {
    pub(super) fn new(owner: BoundaryRetryOwner) -> Result<Self, String> {
        let Some(budget) = RetryBudget::new(owner.policy.clone()) else {
            return Err(format!(
                "circuit_breaker retry rejected for source-poll '{}': max_total_wall_time cannot be represented from the current monotonic instant; choose a smaller limit",
                owner.protected_unit_label
            ));
        };
        let poll_id = EventId::new();
        let lifecycle = RetryLifecycleContext {
            stage_id: owner.stage_id,
            attachment_id: owner.attachment_id.as_ulid(),
            protected_unit: RetryProtectedUnit::SourcePoll,
            invocation: RetryInvocation::SourcePoll { poll_id },
        };

        Ok(Self {
            owner,
            budget,
            lifecycle,
            poll_id,
            events: Vec::new(),
            recovery_started: false,
        })
    }

    pub(super) fn budget(&self) -> &RetryBudget {
        &self.budget
    }

    pub(super) fn recovery_started(&self) -> bool {
        self.recovery_started
    }

    pub(super) fn extend_events(&mut self, events: Vec<ChainEvent>) {
        self.events.extend(events);
    }

    pub(super) fn record_attempt_failed(
        &mut self,
        attempt: NonZeroU32,
        kind: ErrorKind,
        delay: Duration,
    ) {
        self.events.push(attempt_failed_event(
            &self.owner,
            &self.lifecycle,
            attempt,
            kind,
            delay,
            &self.budget,
            Some(self.poll_id),
        ));
        self.recovery_started = true;
    }

    pub(super) fn record_succeeded(&mut self, attempts: u32) {
        self.events.push(succeeded_event(
            &self.owner,
            &self.lifecycle,
            attempts,
            &self.budget,
            Some(self.poll_id),
        ));
    }

    pub(super) fn record_exhausted(
        &mut self,
        attempts: u32,
        cause: RetryExhaustionCause,
        last_error_kind: Option<ErrorKind>,
    ) {
        self.events.push(exhausted_event(
            &self.owner,
            &self.lifecycle,
            attempts,
            cause,
            last_error_kind,
            &self.budget,
            Some(self.poll_id),
        ));
    }

    pub(super) fn finish_polled(&mut self, poll: SourcePollReport) -> SourceBoundaryReport {
        SourceBoundaryReport {
            outcome: SourceBoundaryOutcome::Polled(poll),
            control_events: std::mem::take(&mut self.events),
        }
    }

    pub(super) fn finish_rejected(&mut self, reason: impl Into<String>) -> SourceBoundaryReport {
        SourceBoundaryReport {
            outcome: SourceBoundaryOutcome::Rejected {
                reason: reason.into(),
            },
            control_events: std::mem::take(&mut self.events),
        }
    }

    pub(super) fn abort_polled(poll: SourcePollReport) -> SourceBoundaryReport {
        SourceBoundaryReport {
            outcome: SourceBoundaryOutcome::Polled(poll),
            control_events: Vec::new(),
        }
    }

    pub(super) fn abort_rejected(reason: impl Into<String>) -> SourceBoundaryReport {
        SourceBoundaryReport {
            outcome: SourceBoundaryOutcome::Rejected {
                reason: reason.into(),
            },
            control_events: Vec::new(),
        }
    }
}
