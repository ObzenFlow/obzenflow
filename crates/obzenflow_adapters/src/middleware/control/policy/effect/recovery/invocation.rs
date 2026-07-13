// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::super::retry::{
    attempt_failed_event, exhausted_event, succeeded_event, BoundaryRetryOwner, RetryBudget,
};
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryInvocation, RetryLifecycleContext, RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::ChainEvent;
use obzenflow_runtime::effects::{EffectBoundaryOutcome, EffectBoundaryReport, EffectIdentity};
use std::num::NonZeroU32;
use std::time::Duration;

/// State whose lifetime spans every physical attempt in one logical effect
/// invocation. Per-attempt policy contexts and guards deliberately live in
/// `attempt.rs` instead.
pub(super) struct RetryInvocationState {
    pub(super) owner: BoundaryRetryOwner,
    pub(super) lifecycle: RetryLifecycleContext,
    pub(super) budget: RetryBudget,
    pub(super) parent_id: obzenflow_core::EventId,
    pub(super) attempt: NonZeroU32,
    pub(super) recovery_started: bool,
    events: Vec<ChainEvent>,
}

impl RetryInvocationState {
    pub(super) fn new(
        owner: BoundaryRetryOwner,
        identity: &EffectIdentity,
        parent_id: obzenflow_core::EventId,
    ) -> Option<Self> {
        let budget = RetryBudget::new(owner.policy.clone())?;
        let lifecycle = RetryLifecycleContext {
            stage_id: owner.stage_id,
            attachment_id: owner.attachment_id.as_ulid(),
            protected_unit: RetryProtectedUnit::Effect {
                effect_type: identity.effect_type.into(),
            },
            invocation: RetryInvocation::Effect {
                cursor: identity.cursor.clone(),
            },
        };
        Some(Self {
            owner,
            lifecycle,
            budget,
            parent_id,
            attempt: NonZeroU32::MIN,
            recovery_started: false,
            events: Vec::new(),
        })
    }

    pub(super) fn extend_events(&mut self, events: Vec<ChainEvent>) {
        self.events.extend(events);
    }

    pub(super) fn exhaust(
        &mut self,
        attempt_count: u32,
        cause: RetryExhaustionCause,
        kind: Option<ErrorKind>,
    ) {
        self.events.push(exhausted_event(
            &self.owner,
            &self.lifecycle,
            attempt_count,
            cause,
            kind,
            &self.budget,
            Some(self.parent_id),
        ));
    }

    pub(super) fn exhaust_current(&mut self, cause: RetryExhaustionCause, kind: Option<ErrorKind>) {
        self.exhaust(self.attempt.get(), cause, kind);
    }

    pub(super) fn exhaust_previous_if_recovering(&mut self, cause: RetryExhaustionCause) {
        if self.recovery_started {
            self.exhaust(self.attempt.get().saturating_sub(1), cause, None);
        }
    }

    pub(super) fn record_attempt_failed(&mut self, kind: ErrorKind, delay: Duration) {
        self.events.push(attempt_failed_event(
            &self.owner,
            &self.lifecycle,
            self.attempt,
            kind,
            delay,
            &self.budget,
            Some(self.parent_id),
        ));
        self.recovery_started = true;
    }

    pub(super) fn record_success_if_recovering(&mut self) {
        if self.recovery_started {
            self.events.push(succeeded_event(
                &self.owner,
                &self.lifecycle,
                self.attempt.get(),
                &self.budget,
                Some(self.parent_id),
            ));
        }
    }

    pub(super) fn advance(&mut self) {
        self.attempt = self
            .budget
            .next_attempt(self.attempt)
            .expect("retry slot was validated before backoff");
    }

    pub(super) fn report(self, outcome: EffectBoundaryOutcome) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome,
            control_events: self.events,
        }
    }

    pub(super) fn silent_report(self, outcome: EffectBoundaryOutcome) -> EffectBoundaryReport {
        EffectBoundaryReport {
            outcome,
            control_events: Vec::new(),
        }
    }
}
