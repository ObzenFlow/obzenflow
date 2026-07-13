// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-owned source policy boundary (FLOWIP-115a).
//!
//! The runtime sees only `SourceBoundary`. This module owns the
//! middleware policy onion hidden behind that seam.

use super::retry::{
    attempt_failed_event, await_active_execution, await_before_execution, await_settlement,
    exhausted_event, succeeded_event, AttemptDisposition, RetryBudget, RetryWaitError,
};
use crate::middleware::{BoundaryRetryOwner, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryInvocation, RetryLifecycleContext, RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, EventId, MiddlewareExecutionScope, WriterId};
use obzenflow_runtime::prelude::SourceError;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::source::{
    SourceBoundary, SourceBoundaryFuture, SourceBoundaryOutcome, SourceBoundaryReport,
    SourcePollCompletion, SourcePollExecution, SourcePollExecutor, SourcePollReport,
};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// RAII guard returned by source-policy admission for reserved resources.
pub trait SourceAdmissionGuard: Send + Sync {}

impl<T: Send + Sync> SourceAdmissionGuard for T {}

/// Admission decision from one source policy.
pub enum SourceAdmission {
    /// Admit the poll, optionally holding a guard across the protected attempt.
    Admit(Option<Box<dyn SourceAdmissionGuard>>),
    /// Reject before polling. 115a policies do not return this, but the seam is
    /// reserved for future fail-fast policies.
    Reject { reason: String },
}

/// Post-poll source-policy decision. 115a supports proceeding only, while the
/// enum leaves the trait extensible for future post-poll policy decisions.
pub enum SourceAfterPoll {
    Proceed,
}

/// Source-batch facts exposed to control policies after a successful poll.
///
/// Policies need the control-relevant shape of the batch, not raw payload or
/// lineage access. The boundary computes these facts once from the delivered
/// batch before invoking policy hooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceBatchFacts {
    pub event_count: usize,
    pub has_error_marked: bool,
}

impl SourceBatchFacts {
    pub fn from_events(events: &[ChainEvent]) -> Self {
        Self {
            event_count: events.len(),
            has_error_marked: events.iter().any(|event| {
                matches!(event.processing_info.status, ProcessingStatus::Error { .. })
            }),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.event_count == 0
    }
}

/// Raw source-poll outcome shown independently to each policy.
pub enum SourcePollOutcome<'a> {
    Delivered {
        batch: SourceBatchFacts,
        poll_duration: Duration,
    },
    Empty {
        poll_duration: Duration,
    },
    Eof {
        poll_duration: Duration,
    },
    Failed {
        error: &'a SourceError,
        poll_duration: Duration,
    },
    RejectedBy {
        policy: &'static str,
        reason: &'a str,
    },
    /// Admission was reserved but no physical source poll began.
    NotExecuted {
        reason: &'a str,
    },
}

/// Source-shaped policy context. It owns the observability outbox returned by
/// the boundary report and never crosses into the runtime supervisor.
pub struct SourcePolicyCtx {
    writer_id: WriterId,
    synthetic_event: Option<ChainEvent>,
    middleware_ctx: MiddlewareContext,
    retry_physical_call: bool,
}

impl SourcePolicyCtx {
    pub fn new(writer_id: WriterId) -> Self {
        Self {
            writer_id,
            synthetic_event: None,
            middleware_ctx: MiddlewareContext::with_scope(
                MiddlewareExecutionScope::LiveSourceBoundary,
            ),
            retry_physical_call: false,
        }
    }

    pub(crate) fn new_retry_physical_call(writer_id: WriterId) -> Self {
        let mut ctx = Self::new(writer_id);
        ctx.retry_physical_call = true;
        ctx
    }

    pub(crate) fn retry_physical_call(&self) -> bool {
        self.retry_physical_call
    }

    pub fn writer_id(&self) -> WriterId {
        self.writer_id
    }

    pub fn synthetic_event_clone(&mut self) -> ChainEvent {
        self.synthetic_event().clone()
    }

    pub fn write_control_event(&mut self, event: ChainEvent) {
        self.middleware_ctx.write_control_event(event);
    }

    pub fn take_control_events(&mut self) -> Vec<ChainEvent> {
        self.middleware_ctx.take_control_events()
    }

    pub(crate) fn middleware_context_mut(&mut self) -> &mut MiddlewareContext {
        &mut self.middleware_ctx
    }

    pub(crate) fn middleware_context(&self) -> &MiddlewareContext {
        &self.middleware_ctx
    }

    fn synthetic_event(&mut self) -> &ChainEvent {
        if self.synthetic_event.is_none() {
            self.synthetic_event = Some(ChainEventFactory::data_event(
                self.writer_id,
                "system.source.next",
                serde_json::json!({
                    "source_type": "boundary",
                    "timestamp_ms": SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                }),
            ));
        }
        self.synthetic_event
            .as_ref()
            .expect("synthetic event must be initialized")
    }
}

/// A source resilience policy behind the adapter-owned boundary.
#[async_trait]
pub trait SourcePolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission;

    /// Commit any execution-based reservations immediately before the source
    /// poll executor starts. Default policies reserve no such resource.
    fn commit_execution(&self, _ctx: &mut SourcePolicyCtx) {}

    async fn after_poll(
        &self,
        _batch: SourceBatchFacts,
        _ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        SourceAfterPoll::Proceed
    }

    fn observe(&self, outcome: &SourcePollOutcome<'_>, ctx: &mut SourcePolicyCtx);

    #[doc(hidden)]
    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        None
    }

    #[doc(hidden)]
    fn recovery_allowed_after_settlement(&self, _ctx: &SourcePolicyCtx) -> bool {
        true
    }
}

/// Source boundary backed by a declared-order policy chain.
pub struct PerSourcePolicyBoundary {
    policies: Arc<Vec<Arc<dyn SourcePolicy>>>,
    writer_id: WriterId,
}

impl PerSourcePolicyBoundary {
    pub fn new(policies: Vec<Arc<dyn SourcePolicy>>, writer_id: WriterId) -> Self {
        Self {
            policies: Arc::new(policies),
            writer_id,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.policies.is_empty()
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        let mut owners = self
            .policies
            .iter()
            .filter_map(|policy| policy.retry_owner());
        let owner = owners.next()?;
        debug_assert!(
            owners.next().is_none(),
            "binder must reject multiple retry owners"
        );
        Some(owner)
    }
}

type SourceAdmitGuard = Option<Box<dyn SourceAdmissionGuard>>;

impl SourceBoundary for PerSourcePolicyBoundary {
    fn graceful_drain_settles_active_attempt(&self) -> bool {
        self.retry_owner().is_some()
    }

    fn around_poll<'a>(&'a self, execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a> {
        Box::pin(async move {
            if self.policies.is_empty() {
                return SourceBoundaryReport {
                    outcome: SourceBoundaryOutcome::Polled(execute.await),
                    control_events: Vec::new(),
                };
            }

            let mut ctx = SourcePolicyCtx::new(self.writer_id);
            let mut admitted: Vec<(&Arc<dyn SourcePolicy>, SourceAdmitGuard)> = Vec::new();

            for policy in self.policies.iter() {
                match policy.admit(&mut ctx).await {
                    SourceAdmission::Admit(guard) => admitted.push((policy, guard)),
                    SourceAdmission::Reject { reason } => {
                        let outcome = SourcePollOutcome::RejectedBy {
                            policy: policy.label(),
                            reason: &reason,
                        };
                        for (prior, _) in admitted.iter().rev() {
                            prior.observe(&outcome, &mut ctx);
                        }
                        return SourceBoundaryReport {
                            outcome: SourceBoundaryOutcome::Rejected { reason },
                            control_events: ctx.take_control_events(),
                        };
                    }
                }
            }

            for (policy, _) in &admitted {
                policy.commit_execution(&mut ctx);
            }
            let poll = execute.await;
            let batch = match &poll.result {
                Ok(SourcePollCompletion::Batch(batch)) if !batch.is_empty() => {
                    Some(SourceBatchFacts::from_events(batch))
                }
                _ => None,
            };
            let outcome = source_poll_outcome(&poll);
            for (policy, _) in admitted.iter().rev() {
                if let Some(batch) = batch {
                    match policy.after_poll(batch, &mut ctx).await {
                        SourceAfterPoll::Proceed => {}
                    }
                }
                policy.observe(&outcome, &mut ctx);
            }

            SourceBoundaryReport {
                outcome: SourceBoundaryOutcome::Polled(poll),
                control_events: ctx.take_control_events(),
            }
        })
    }

    fn around_retryable_poll<'a>(
        &'a self,
        execute: &'a mut dyn SourcePollExecutor,
        mut stop: BoundaryStopReceiver,
    ) -> SourceBoundaryFuture<'a> {
        Box::pin(async move {
            let Some(owner) = self.retry_owner() else {
                return self.around_poll(execute.attempt(NonZeroU32::MIN)).await;
            };
            let Some(budget) = RetryBudget::new(owner.policy.clone()) else {
                return SourceBoundaryReport {
                    outcome: SourceBoundaryOutcome::Rejected {
                        reason: format!(
                            "circuit_breaker retry rejected for source-poll '{}': max_total_wall_time cannot be represented from the current monotonic instant; choose a smaller limit",
                            owner.protected_unit_label
                        ),
                    },
                    control_events: Vec::new(),
                };
            };

            let poll_id = EventId::new();
            let lifecycle = RetryLifecycleContext {
                stage_id: owner.stage_id,
                attachment_id: owner.attachment_id.as_ulid(),
                protected_unit: RetryProtectedUnit::SourcePoll,
                invocation: RetryInvocation::SourcePoll { poll_id },
            };
            let mut attempt = NonZeroU32::MIN;
            let mut recovery_started = false;
            let mut invocation_events = Vec::new();

            loop {
                let mut ctx = SourcePolicyCtx::new_retry_physical_call(self.writer_id);
                let mut admitted: Vec<(&Arc<dyn SourcePolicy>, SourceAdmitGuard)> = Vec::new();

                for policy in self.policies.iter() {
                    let admission = await_before_execution(
                        policy.admit(&mut ctx),
                        budget.deadline(),
                        &mut stop,
                    )
                    .await;
                    match admission {
                        Ok(SourceAdmission::Admit(guard)) => admitted.push((policy, guard)),
                        Ok(SourceAdmission::Reject { reason }) => {
                            let outcome = SourcePollOutcome::RejectedBy {
                                policy: policy.label(),
                                reason: &reason,
                            };
                            for (prior, _) in admitted.iter().rev() {
                                prior.observe(&outcome, &mut ctx);
                            }
                            invocation_events.extend(ctx.take_control_events());
                            if attempt.get() > 1 || recovery_started {
                                invocation_events.push(exhausted_event(
                                    &owner,
                                    &lifecycle,
                                    attempt.get().saturating_sub(1),
                                    RetryExhaustionCause::PolicyRejected,
                                    None,
                                    &budget,
                                    Some(poll_id),
                                ));
                            }
                            return SourceBoundaryReport {
                                outcome: SourceBoundaryOutcome::Rejected { reason },
                                control_events: invocation_events,
                            };
                        }
                        Err(wait_error) => {
                            let reason = match wait_error {
                                RetryWaitError::Deadline => {
                                    "retry total-wall deadline expired during source policy admission"
                                }
                                RetryWaitError::Drain => {
                                    "graceful drain requested during source policy admission"
                                }
                                RetryWaitError::Abort => {
                                    "force abort requested during source policy admission"
                                }
                            };
                            let outcome = SourcePollOutcome::NotExecuted { reason };
                            for (prior, _) in admitted.iter().rev() {
                                prior.observe(&outcome, &mut ctx);
                            }
                            invocation_events.extend(ctx.take_control_events());
                            if matches!(wait_error, RetryWaitError::Deadline)
                                || (matches!(wait_error, RetryWaitError::Drain) && recovery_started)
                            {
                                invocation_events.push(exhausted_event(
                                    &owner,
                                    &lifecycle,
                                    attempt.get().saturating_sub(1),
                                    match wait_error {
                                        RetryWaitError::Deadline => {
                                            RetryExhaustionCause::TotalWallTime
                                        }
                                        RetryWaitError::Drain => {
                                            RetryExhaustionCause::DrainRequested
                                        }
                                        RetryWaitError::Abort => unreachable!(),
                                    },
                                    None,
                                    &budget,
                                    Some(poll_id),
                                ));
                            }
                            return SourceBoundaryReport {
                                outcome: SourceBoundaryOutcome::Rejected {
                                    reason: reason.to_string(),
                                },
                                control_events: if matches!(wait_error, RetryWaitError::Abort) {
                                    Vec::new()
                                } else {
                                    invocation_events
                                },
                            };
                        }
                    }
                }

                let pre_execution_barrier = if !budget.can_start_execution() {
                    Some(RetryWaitError::Deadline)
                } else {
                    match stop.intent() {
                        obzenflow_runtime::stages::common::BoundaryStopIntent::Running => None,
                        obzenflow_runtime::stages::common::BoundaryStopIntent::Drain => {
                            Some(RetryWaitError::Drain)
                        }
                        obzenflow_runtime::stages::common::BoundaryStopIntent::Abort => {
                            Some(RetryWaitError::Abort)
                        }
                    }
                };
                if let Some(barrier) = pre_execution_barrier {
                    let reason = match barrier {
                        RetryWaitError::Deadline => {
                            "retry total-wall deadline expired before source executor start"
                        }
                        RetryWaitError::Drain => {
                            "graceful drain requested before source executor start"
                        }
                        RetryWaitError::Abort => {
                            "force abort requested before source executor start"
                        }
                    };
                    let outcome = SourcePollOutcome::NotExecuted { reason };
                    for (prior, _) in admitted.iter().rev() {
                        prior.observe(&outcome, &mut ctx);
                    }
                    invocation_events.extend(ctx.take_control_events());
                    if matches!(barrier, RetryWaitError::Deadline)
                        || (matches!(barrier, RetryWaitError::Drain) && recovery_started)
                    {
                        invocation_events.push(exhausted_event(
                            &owner,
                            &lifecycle,
                            attempt.get().saturating_sub(1),
                            if matches!(barrier, RetryWaitError::Deadline) {
                                RetryExhaustionCause::TotalWallTime
                            } else {
                                RetryExhaustionCause::DrainRequested
                            },
                            None,
                            &budget,
                            Some(poll_id),
                        ));
                    }
                    return SourceBoundaryReport {
                        outcome: SourceBoundaryOutcome::Rejected {
                            reason: reason.to_string(),
                        },
                        control_events: if matches!(barrier, RetryWaitError::Abort) {
                            Vec::new()
                        } else {
                            invocation_events
                        },
                    };
                }

                let active = await_active_execution(
                    || {
                        // This closure is the physical-call commit point. The
                        // final deadline/stop barrier has passed; reservations
                        // are charged immediately before the executor future is
                        // constructed with its one-based ordinal.
                        for (policy, _) in &admitted {
                            policy.commit_execution(&mut ctx);
                        }
                        execute.attempt(attempt)
                    },
                    budget.deadline(),
                    &mut stop,
                )
                .await;
                let (poll, mut drain_requested, active_deadline) = match active {
                    Ok((poll, drain_requested)) => (poll, drain_requested, false),
                    Err(RetryWaitError::Deadline) => (
                        SourcePollReport {
                            result: Err(SourceError::Timeout("source_poll_cancelled".to_string())),
                            poll_duration: Duration::ZERO,
                        },
                        false,
                        true,
                    ),
                    Err(RetryWaitError::Drain) => {
                        let reason = "graceful drain requested before source executor start";
                        let outcome = SourcePollOutcome::NotExecuted { reason };
                        for (prior, _) in admitted.iter().rev() {
                            prior.observe(&outcome, &mut ctx);
                        }
                        invocation_events.extend(ctx.take_control_events());
                        if recovery_started {
                            invocation_events.push(exhausted_event(
                                &owner,
                                &lifecycle,
                                attempt.get().saturating_sub(1),
                                RetryExhaustionCause::DrainRequested,
                                None,
                                &budget,
                                Some(poll_id),
                            ));
                        }
                        return SourceBoundaryReport {
                            outcome: SourceBoundaryOutcome::Rejected {
                                reason: reason.to_string(),
                            },
                            control_events: invocation_events,
                        };
                    }
                    Err(RetryWaitError::Abort) => {
                        return SourceBoundaryReport {
                            outcome: SourceBoundaryOutcome::Rejected {
                                reason: "force abort requested during source poll".to_string(),
                            },
                            control_events: Vec::new(),
                        };
                    }
                };

                let mut settlement_deadline = false;
                let batch = match &poll.result {
                    Ok(SourcePollCompletion::Batch(batch)) if !batch.is_empty() => {
                        Some(SourceBatchFacts::from_events(batch))
                    }
                    _ => None,
                };
                let outcome = source_poll_outcome(&poll);
                for (policy, _) in admitted.iter().rev() {
                    if let Some(batch) = batch {
                        if !settlement_deadline {
                            match await_settlement(
                                policy.after_poll(batch, &mut ctx),
                                budget.deadline(),
                                &mut stop,
                            )
                            .await
                            {
                                Ok((SourceAfterPoll::Proceed, settlement_drain)) => {
                                    drain_requested |= settlement_drain;
                                }
                                Err(RetryWaitError::Deadline) => settlement_deadline = true,
                                Err(RetryWaitError::Drain) => unreachable!(
                                    "post-call settlement remembers drain without cancelling"
                                ),
                                Err(RetryWaitError::Abort) => {
                                    return SourceBoundaryReport {
                                        outcome: SourceBoundaryOutcome::Rejected {
                                            reason: "force abort requested during source policy settlement"
                                                .to_string(),
                                        },
                                        control_events: Vec::new(),
                                    };
                                }
                            }
                        }
                    }
                    policy.observe(&outcome, &mut ctx);
                }
                invocation_events.extend(ctx.take_control_events());
                let recovery_allowed = self
                    .policies
                    .iter()
                    .all(|policy| policy.recovery_allowed_after_settlement(&ctx));
                drop(admitted);
                drop(ctx);

                let disposition = source_attempt_disposition(&poll);
                match disposition {
                    AttemptDisposition::Completed => {
                        if recovery_started {
                            invocation_events.push(succeeded_event(
                                &owner,
                                &lifecycle,
                                attempt.get(),
                                &budget,
                                Some(poll_id),
                            ));
                        }
                        return SourceBoundaryReport {
                            outcome: SourceBoundaryOutcome::Polled(poll),
                            control_events: invocation_events,
                        };
                    }
                    AttemptDisposition::TerminalFailure { kind } => {
                        if recovery_started {
                            invocation_events.push(exhausted_event(
                                &owner,
                                &lifecycle,
                                attempt.get(),
                                RetryExhaustionCause::TerminalFailure,
                                Some(kind),
                                &budget,
                                Some(poll_id),
                            ));
                        }
                        return SourceBoundaryReport {
                            outcome: SourceBoundaryOutcome::Polled(poll),
                            control_events: invocation_events,
                        };
                    }
                    AttemptDisposition::RetryableFailure { kind, retry_after } => {
                        let barrier = if active_deadline || settlement_deadline {
                            Some(RetryExhaustionCause::TotalWallTime)
                        } else if drain_requested {
                            Some(RetryExhaustionCause::DrainRequested)
                        } else if !recovery_allowed {
                            Some(RetryExhaustionCause::PolicyRejected)
                        } else if budget.next_attempt(attempt).is_none() {
                            Some(RetryExhaustionCause::MaxAttempts)
                        } else {
                            None
                        };
                        if let Some(cause) = barrier {
                            invocation_events.push(exhausted_event(
                                &owner,
                                &lifecycle,
                                attempt.get(),
                                cause,
                                Some(kind),
                                &budget,
                                Some(poll_id),
                            ));
                            return SourceBoundaryReport {
                                outcome: SourceBoundaryOutcome::Polled(poll),
                                control_events: invocation_events,
                            };
                        }

                        let delay = match budget.delay_after(attempt, retry_after) {
                            Ok(delay) => delay,
                            Err(cause) => {
                                invocation_events.push(exhausted_event(
                                    &owner,
                                    &lifecycle,
                                    attempt.get(),
                                    cause,
                                    Some(kind),
                                    &budget,
                                    Some(poll_id),
                                ));
                                return SourceBoundaryReport {
                                    outcome: SourceBoundaryOutcome::Polled(poll),
                                    control_events: invocation_events,
                                };
                            }
                        };
                        invocation_events.push(attempt_failed_event(
                            &owner,
                            &lifecycle,
                            attempt,
                            kind.clone(),
                            delay,
                            &budget,
                            Some(poll_id),
                        ));
                        recovery_started = true;
                        match await_before_execution(
                            tokio::time::sleep(delay),
                            budget.deadline(),
                            &mut stop,
                        )
                        .await
                        {
                            Ok(()) => {
                                attempt = budget
                                    .next_attempt(attempt)
                                    .expect("retry slot was validated before backoff");
                            }
                            Err(
                                wait_error @ (RetryWaitError::Deadline | RetryWaitError::Drain),
                            ) => {
                                let cause = match wait_error {
                                    RetryWaitError::Deadline => RetryExhaustionCause::TotalWallTime,
                                    RetryWaitError::Drain => RetryExhaustionCause::DrainRequested,
                                    RetryWaitError::Abort => unreachable!(
                                        "force abort is handled by the following match arm"
                                    ),
                                };
                                invocation_events.push(exhausted_event(
                                    &owner,
                                    &lifecycle,
                                    attempt.get(),
                                    cause,
                                    Some(kind),
                                    &budget,
                                    Some(poll_id),
                                ));
                                return SourceBoundaryReport {
                                    outcome: SourceBoundaryOutcome::Polled(poll),
                                    control_events: invocation_events,
                                };
                            }
                            Err(RetryWaitError::Abort) => {
                                return SourceBoundaryReport {
                                    outcome: SourceBoundaryOutcome::Polled(poll),
                                    control_events: Vec::new(),
                                };
                            }
                        }
                    }
                    AttemptDisposition::NotExecuted => unreachable!(),
                }
            }
        })
    }
}

fn source_poll_outcome(report: &SourcePollReport) -> SourcePollOutcome<'_> {
    match &report.result {
        Ok(SourcePollCompletion::Batch(batch)) if batch.is_empty() => SourcePollOutcome::Empty {
            poll_duration: report.poll_duration,
        },
        Ok(SourcePollCompletion::Batch(batch)) => SourcePollOutcome::Delivered {
            batch: SourceBatchFacts::from_events(batch),
            poll_duration: report.poll_duration,
        },
        Ok(SourcePollCompletion::Eof) => SourcePollOutcome::Eof {
            poll_duration: report.poll_duration,
        },
        Err(err) => SourcePollOutcome::Failed {
            error: err,
            poll_duration: report.poll_duration,
        },
    }
}

fn source_attempt_disposition(report: &SourcePollReport) -> AttemptDisposition {
    use obzenflow_core::event::status::processing_status::ErrorKind;
    match &report.result {
        Ok(_) => AttemptDisposition::Completed,
        Err(SourceError::Timeout(_)) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::Timeout,
            retry_after: None,
        },
        Err(SourceError::Transport(_)) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::Remote,
            retry_after: None,
        },
        Err(SourceError::RateLimited { retry_after, .. }) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::RateLimited,
            retry_after: *retry_after,
        },
        Err(SourceError::Deserialization(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Deserialization,
        },
        Err(SourceError::PermanentFailure(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::PermanentFailure,
        },
        Err(SourceError::Other(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Unknown,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::circuit_breaker::{
        CircuitBreakerMiddleware, CircuitBreakerSourcePolicy,
    };
    use crate::middleware::{
        BoundaryRetryPolicy, MiddlewareAttachmentId, MiddlewareAttachmentRequest,
        MiddlewareDeclaration, MiddlewareDeclarationIndex, MiddlewareOrigin, MiddlewareSurface,
        MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId, SourcePollSurface, SourcePollUnitId,
    };
    use obzenflow_core::event::payloads::observability_payload::{
        MiddlewareLifecycle, ObservabilityPayload, RetryEvent,
    };
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{ChainEventContent, ChainEventFactory};
    use obzenflow_core::{MiddlewareContextKey, StageId, Ulid};
    use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Notify;

    struct RecordingPolicy {
        label: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SourcePolicy for RecordingPolicy {
        fn label(&self) -> &'static str {
            self.label
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.log
                .lock()
                .unwrap()
                .push(format!("admit:{}", self.label));
            SourceAdmission::Admit(None)
        }

        async fn after_poll(
            &self,
            _batch: SourceBatchFacts,
            _ctx: &mut SourcePolicyCtx,
        ) -> SourceAfterPoll {
            self.log
                .lock()
                .unwrap()
                .push(format!("after:{}", self.label));
            SourceAfterPoll::Proceed
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            self.log
                .lock()
                .unwrap()
                .push(format!("observe:{}", self.label));
        }
    }

    fn test_event() -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.source",
            json!({ "value": 1 }),
        )
    }

    fn source_retry_owner(
        max_attempts: u32,
        delay: Duration,
        max_single_delay: Duration,
    ) -> BoundaryRetryOwner {
        let stage_id = StageId::from_ulid(Ulid::from(0x115_0001_u128));
        let declaration = MiddlewareDeclaration::control(
            "test.source_retry_owner",
            vec![MiddlewareSurfaceKind::SourcePoll],
        );
        let surface = MiddlewareSurface::SourcePoll(SourcePollSurface { stage_id });
        let protected_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::SourcePoll(SourcePollUnitId),
        };
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
        };
        BoundaryRetryOwner {
            attachment_id: MiddlewareAttachmentId::from_declaration_and_request(
                &declaration,
                &request,
            ),
            stage_id,
            writer_id: WriterId::from(stage_id),
            protected_unit_label: "orders_source".to_string(),
            sink_configured_target_id: None,
            policy: BoundaryRetryPolicy {
                max_attempts: NonZeroU32::new(max_attempts).expect("test attempts are non-zero"),
                backoff: BackoffStrategy::Fixed { delay },
                max_single_delay,
                max_total_wall_time: Duration::from_secs(30),
            },
        }
    }

    fn retry_event(event: &ChainEvent) -> &RetryEvent {
        match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::Retry(retry),
            )) => retry,
            other => panic!("expected retry lifecycle row, got {other:?}"),
        }
    }

    #[test]
    fn source_error_disposition_matrix_is_exhaustive_and_preserves_retry_hints() {
        let retry_after = Duration::from_millis(725);
        let cases = [
            (
                SourceError::Timeout("timeout".to_string()),
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::Timeout,
                    retry_after: None,
                },
            ),
            (
                SourceError::Transport("transport".to_string()),
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::Remote,
                    retry_after: None,
                },
            ),
            (
                SourceError::RateLimited {
                    message: "rate limited with hint".to_string(),
                    retry_after: Some(retry_after),
                },
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::RateLimited,
                    retry_after: Some(retry_after),
                },
            ),
            (
                SourceError::RateLimited {
                    message: "rate limited without hint".to_string(),
                    retry_after: None,
                },
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::RateLimited,
                    retry_after: None,
                },
            ),
            (
                SourceError::Deserialization("malformed".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Deserialization,
                },
            ),
            (
                SourceError::PermanentFailure("permanent".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::PermanentFailure,
                },
            ),
            (
                SourceError::Other("unknown".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
        ];

        for (error, expected) in cases {
            let report = SourcePollReport {
                result: Err(error),
                poll_duration: Duration::ZERO,
            };
            assert_eq!(source_attempt_disposition(&report), expected);
        }

        for completion in [
            SourcePollCompletion::Batch(Vec::new()),
            SourcePollCompletion::Eof,
        ] {
            let report = SourcePollReport {
                result: Ok(completion),
                poll_duration: Duration::ZERO,
            };
            assert_eq!(
                source_attempt_disposition(&report),
                AttemptDisposition::Completed
            );
        }
    }

    struct RetryContextMarker;

    impl MiddlewareContextKey for RetryContextMarker {
        type Value = ();
        const LABEL: &'static str = "source_retry_test_marker";
    }

    struct RetryOwnerPolicy {
        owner: BoundaryRetryOwner,
        log: Arc<Mutex<Vec<String>>>,
        admissions: Arc<AtomicUsize>,
        guard_drops: Arc<AtomicUsize>,
        recovery_allowed: Arc<AtomicBool>,
        observed: Option<Arc<Notify>>,
    }

    #[async_trait]
    impl SourcePolicy for RetryOwnerPolicy {
        fn label(&self) -> &'static str {
            "retry_owner"
        }

        async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            let middleware = ctx.middleware_context_mut();
            assert!(
                !middleware.contains::<RetryContextMarker>(),
                "each retry attempt must receive a fresh source policy context"
            );
            middleware.insert::<RetryContextMarker>(());
            let attempt = self.admissions.fetch_add(1, Ordering::SeqCst) + 1;
            self.log
                .lock()
                .unwrap()
                .push(format!("admit:owner:{attempt}"));
            SourceAdmission::Admit(Some(Box::new(DropCountingGuard {
                dropped: self.guard_drops.clone(),
            })))
        }

        fn commit_execution(&self, _ctx: &mut SourcePolicyCtx) {
            self.log.lock().unwrap().push("commit:owner".to_string());
        }

        fn observe(&self, outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            let suffix = match outcome {
                SourcePollOutcome::Failed { .. } => "failed",
                SourcePollOutcome::Delivered { .. } => "delivered",
                SourcePollOutcome::Empty { .. } => "empty",
                SourcePollOutcome::Eof { .. } => "eof",
                SourcePollOutcome::RejectedBy { .. } => "rejected",
                SourcePollOutcome::NotExecuted { .. } => "not_executed",
            };
            self.log
                .lock()
                .unwrap()
                .push(format!("observe:owner:{suffix}"));
            if let Some(observed) = &self.observed {
                observed.notify_one();
            }
        }

        fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
            Some(self.owner.clone())
        }

        fn recovery_allowed_after_settlement(&self, _ctx: &SourcePolicyCtx) -> bool {
            self.recovery_allowed.load(Ordering::SeqCst)
        }
    }

    struct InnerOnionPolicy {
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SourcePolicy for InnerOnionPolicy {
        fn label(&self) -> &'static str {
            "inner"
        }

        async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            assert!(ctx
                .middleware_context_mut()
                .contains::<RetryContextMarker>());
            self.log.lock().unwrap().push("admit:inner".to_string());
            SourceAdmission::Admit(None)
        }

        fn commit_execution(&self, _ctx: &mut SourcePolicyCtx) {
            self.log.lock().unwrap().push("commit:inner".to_string());
        }

        fn observe(&self, outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            let suffix = if matches!(outcome, SourcePollOutcome::Failed { .. }) {
                "failed"
            } else {
                "delivered"
            };
            self.log
                .lock()
                .unwrap()
                .push(format!("observe:inner:{suffix}"));
        }
    }

    struct ObserveOnlyAfterDeadlinePolicy {
        after_poll_calls: Arc<AtomicUsize>,
        observations: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourcePolicy for ObserveOnlyAfterDeadlinePolicy {
        fn label(&self) -> &'static str {
            "observe_only_after_deadline"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            SourceAdmission::Admit(None)
        }

        async fn after_poll(
            &self,
            _batch: SourceBatchFacts,
            _ctx: &mut SourcePolicyCtx,
        ) -> SourceAfterPoll {
            self.after_poll_calls.fetch_add(1, Ordering::SeqCst);
            SourceAfterPoll::Proceed
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            self.observations.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct BlockingAfterPollRetryOwnerPolicy {
        owner: BoundaryRetryOwner,
        after_poll_started: Arc<Notify>,
        observations: Arc<AtomicUsize>,
    }

    struct ControlledAfterPollRetryOwnerPolicy {
        owner: BoundaryRetryOwner,
        after_poll_started: Arc<Notify>,
        after_poll_release: Arc<Notify>,
        after_poll_calls: Arc<AtomicUsize>,
        observations: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourcePolicy for BlockingAfterPollRetryOwnerPolicy {
        fn label(&self) -> &'static str {
            "blocking_after_poll_retry_owner"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            SourceAdmission::Admit(None)
        }

        async fn after_poll(
            &self,
            _batch: SourceBatchFacts,
            _ctx: &mut SourcePolicyCtx,
        ) -> SourceAfterPoll {
            self.after_poll_started.notify_one();
            std::future::pending::<SourceAfterPoll>().await
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            self.observations.fetch_add(1, Ordering::SeqCst);
        }

        fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
            Some(self.owner.clone())
        }
    }

    #[async_trait]
    impl SourcePolicy for ControlledAfterPollRetryOwnerPolicy {
        fn label(&self) -> &'static str {
            "controlled_after_poll_retry_owner"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            SourceAdmission::Admit(None)
        }

        async fn after_poll(
            &self,
            _batch: SourceBatchFacts,
            _ctx: &mut SourcePolicyCtx,
        ) -> SourceAfterPoll {
            self.after_poll_calls.fetch_add(1, Ordering::SeqCst);
            self.after_poll_started.notify_one();
            self.after_poll_release.notified().await;
            SourceAfterPoll::Proceed
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            self.observations.fetch_add(1, Ordering::SeqCst);
        }

        fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
            Some(self.owner.clone())
        }
    }

    struct SequenceSourceExecutor {
        outcomes: VecDeque<Result<SourcePollCompletion, SourceError>>,
        ordinals: Arc<Mutex<Vec<u32>>>,
    }

    struct ControlledSourceExecutor {
        outcome: Option<Result<SourcePollCompletion, SourceError>>,
        ordinals: Arc<Mutex<Vec<u32>>>,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct PendingSourceExecutor {
        calls: Arc<AtomicUsize>,
        started: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
    }

    struct SourceFutureDropCounter(Arc<AtomicUsize>);

    impl Drop for SourceFutureDropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl SourcePollExecutor for PendingSourceExecutor {
        fn attempt<'a>(&'a mut self, _attempt: NonZeroU32) -> SourcePollExecution<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let started = self.started.clone();
            let dropped = self.dropped.clone();
            Box::pin(async move {
                let _drop_counter = SourceFutureDropCounter(dropped);
                started.notify_one();
                std::future::pending::<SourcePollReport>().await
            })
        }
    }

    impl SourcePollExecutor for SequenceSourceExecutor {
        fn attempt<'a>(&'a mut self, attempt: NonZeroU32) -> SourcePollExecution<'a> {
            self.ordinals.lock().unwrap().push(attempt.get());
            let result = self
                .outcomes
                .pop_front()
                .expect("source test executor has an outcome for every attempt");
            Box::pin(async move {
                SourcePollReport {
                    result,
                    poll_duration: Duration::from_millis(2),
                }
            })
        }
    }

    impl SourcePollExecutor for ControlledSourceExecutor {
        fn attempt<'a>(&'a mut self, attempt: NonZeroU32) -> SourcePollExecution<'a> {
            self.ordinals.lock().unwrap().push(attempt.get());
            let outcome = self
                .outcome
                .take()
                .expect("controlled source executor runs exactly once");
            let started = self.started.clone();
            let release = self.release.clone();
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                SourcePollReport {
                    result: outcome,
                    poll_duration: Duration::from_millis(2),
                }
            })
        }
    }

    fn retry_boundary(
        owner: BoundaryRetryOwner,
        log: Arc<Mutex<Vec<String>>>,
        admissions: Arc<AtomicUsize>,
        guard_drops: Arc<AtomicUsize>,
        recovery_allowed: Arc<AtomicBool>,
        include_inner: bool,
    ) -> PerSourcePolicyBoundary {
        retry_boundary_with_observer(
            owner,
            log,
            admissions,
            guard_drops,
            recovery_allowed,
            include_inner,
            None,
        )
    }

    fn retry_boundary_with_observer(
        owner: BoundaryRetryOwner,
        log: Arc<Mutex<Vec<String>>>,
        admissions: Arc<AtomicUsize>,
        guard_drops: Arc<AtomicUsize>,
        recovery_allowed: Arc<AtomicBool>,
        include_inner: bool,
        observed: Option<Arc<Notify>>,
    ) -> PerSourcePolicyBoundary {
        let mut policies: Vec<Arc<dyn SourcePolicy>> = vec![Arc::new(RetryOwnerPolicy {
            owner,
            log: log.clone(),
            admissions,
            guard_drops,
            recovery_allowed,
            observed,
        })];
        if include_inner {
            policies.push(Arc::new(InnerOnionPolicy { log }));
        }
        PerSourcePolicyBoundary::new(
            policies,
            WriterId::from(StageId::from_ulid(Ulid::from(0x115_0001_u128))),
        )
    }

    #[tokio::test]
    async fn source_boundary_composes_forward_admission_and_single_reverse_settlement() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = PerSourcePolicyBoundary::new(
            vec![
                Arc::new(RecordingPolicy {
                    label: "a",
                    log: log.clone(),
                }),
                Arc::new(RecordingPolicy {
                    label: "b",
                    log: log.clone(),
                }),
            ],
            WriterId::from(StageId::new()),
        );

        let report = boundary
            .around_poll(Box::pin(async {
                SourcePollReport {
                    result: Ok(SourcePollCompletion::Batch(vec![test_event()])),
                    poll_duration: Duration::from_millis(1),
                }
            }))
            .await;

        assert!(matches!(report.outcome, SourceBoundaryOutcome::Polled(_)));
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "admit:a",
                "admit:b",
                "after:b",
                "observe:b",
                "after:a",
                "observe:a",
            ]
        );
    }

    struct RejectingPolicy {
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SourcePolicy for RejectingPolicy {
        fn label(&self) -> &'static str {
            "rejecting"
        }

        async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.log.lock().unwrap().push("admit:rejecting".to_string());
            ctx.write_control_event(test_event());
            SourceAdmission::Reject {
                reason: "not now".to_string(),
            }
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            panic!("rejecting policy should not observe its own rejection")
        }
    }

    struct RejectObservingPolicy {
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SourcePolicy for RejectObservingPolicy {
        fn label(&self) -> &'static str {
            "observer"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.log.lock().unwrap().push("admit:observer".to_string());
            SourceAdmission::Admit(None)
        }

        fn observe(&self, outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            match outcome {
                SourcePollOutcome::RejectedBy { policy, reason } => {
                    self.log
                        .lock()
                        .unwrap()
                        .push(format!("observe:rejected_by:{policy}:{reason}"));
                }
                SourcePollOutcome::Empty { .. } => {
                    self.log.lock().unwrap().push("observe:empty".to_string());
                }
                _ => self.log.lock().unwrap().push("observe:other".to_string()),
            }
        }
    }

    #[tokio::test]
    async fn source_boundary_reject_observes_not_polled_and_returns_outbox() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let executed = Arc::new(AtomicUsize::new(0));
        let boundary = PerSourcePolicyBoundary::new(
            vec![
                Arc::new(RejectObservingPolicy { log: log.clone() }),
                Arc::new(RejectingPolicy { log: log.clone() }),
            ],
            WriterId::from(StageId::new()),
        );

        let executed_in_future = executed.clone();
        let report = boundary
            .around_poll(Box::pin(async move {
                executed_in_future.fetch_add(1, Ordering::SeqCst);
                SourcePollReport {
                    result: Ok(SourcePollCompletion::Batch(vec![test_event()])),
                    poll_duration: Duration::from_millis(1),
                }
            }))
            .await;

        assert_eq!(executed.load(Ordering::SeqCst), 0);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Rejected { ref reason } if reason == "not now"
        ));
        assert_eq!(report.control_events.len(), 1);
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "admit:observer",
                "admit:rejecting",
                "observe:rejected_by:rejecting:not now",
            ]
        );
    }

    struct DropCountingGuard {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for DropCountingGuard {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct GuardPolicy {
        admitted: Arc<AtomicUsize>,
        dropped: Arc<AtomicUsize>,
        observed: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl SourcePolicy for GuardPolicy {
        fn label(&self) -> &'static str {
            "guard_policy"
        }

        async fn admit(&self, _ctx: &mut SourcePolicyCtx) -> SourceAdmission {
            self.admitted.fetch_add(1, Ordering::SeqCst);
            SourceAdmission::Admit(Some(Box::new(DropCountingGuard {
                dropped: self.dropped.clone(),
            })))
        }

        fn observe(&self, _outcome: &SourcePollOutcome<'_>, _ctx: &mut SourcePolicyCtx) {
            self.observed.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn source_boundary_drop_releases_guard_without_observe() {
        let admitted = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let observed = Arc::new(AtomicUsize::new(0));
        let boundary = Arc::new(PerSourcePolicyBoundary::new(
            vec![Arc::new(GuardPolicy {
                admitted: admitted.clone(),
                dropped: dropped.clone(),
                observed: observed.clone(),
            })],
            WriterId::from(StageId::new()),
        ));

        let boundary_task = boundary.clone();
        let handle = tokio::spawn(async move {
            boundary_task
                .around_poll(Box::pin(std::future::pending::<SourcePollReport>()))
                .await
        });

        for _ in 0..10 {
            if admitted.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(admitted.load(Ordering::SeqCst), 1);

        handle.abort();
        match handle.await {
            Ok(_) => panic!("boundary task should be cancelled"),
            Err(join) => assert!(join.is_cancelled()),
        }
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(observed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn retryable_source_transient_then_success_is_one_logical_outcome_with_onion_rows() {
        let owner = source_retry_owner(3, Duration::from_millis(25), Duration::from_secs(1));
        let log = Arc::new(Mutex::new(Vec::new()));
        let admissions = Arc::new(AtomicUsize::new(0));
        let guard_drops = Arc::new(AtomicUsize::new(0));
        let recovery_allowed = Arc::new(AtomicBool::new(true));
        let boundary = retry_boundary(
            owner.clone(),
            log.clone(),
            admissions.clone(),
            guard_drops.clone(),
            recovery_allowed,
            true,
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();

        let task = tokio::spawn(async move {
            let mut executor = SequenceSourceExecutor {
                outcomes: VecDeque::from([
                    Err(SourceError::Timeout("first attempt".to_string())),
                    Ok(SourcePollCompletion::Batch(vec![test_event()])),
                ]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
                .await
        });

        for _ in 0..10 {
            if guard_drops.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            guard_drops.load(Ordering::SeqCst),
            1,
            "attempt guards must drop before the retry delay"
        );
        assert_eq!(*ordinals.lock().unwrap(), vec![1]);

        tokio::time::advance(Duration::from_millis(25)).await;
        let report = task.await.expect("source retry task completes");

        match report.outcome {
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Ok(SourcePollCompletion::Batch(batch)),
                ..
            }) => assert_eq!(batch.len(), 1),
            _ => panic!("only the successful terminal poll outcome must escape"),
        }
        assert_eq!(*ordinals.lock().unwrap(), vec![1, 2]);
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert_eq!(guard_drops.load(Ordering::SeqCst), 2);
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "admit:owner:1",
                "admit:inner",
                "commit:owner",
                "commit:inner",
                "observe:inner:failed",
                "observe:owner:failed",
                "admit:owner:2",
                "admit:inner",
                "commit:owner",
                "commit:inner",
                "observe:inner:delivered",
                "observe:owner:delivered",
            ]
        );
        assert_eq!(report.control_events.len(), 2);

        let context = match retry_event(&report.control_events[0]) {
            RetryEvent::AttemptFailed {
                context: Some(context),
                attempt_number,
                max_attempts,
                error_kind,
                delay_ms,
                ..
            } => {
                assert_eq!(*attempt_number, 1);
                assert_eq!(*max_attempts, 3);
                assert_eq!(*error_kind, Some(ErrorKind::Timeout));
                assert_eq!(*delay_ms, Some(25));
                assert_eq!(context.stage_id, owner.stage_id);
                assert_eq!(context.attachment_id, owner.attachment_id.as_ulid());
                assert_eq!(context.protected_unit, RetryProtectedUnit::SourcePoll);
                match context.invocation {
                    RetryInvocation::SourcePoll { poll_id } => {
                        assert_eq!(report.control_events[0].causality.parent_ids, vec![poll_id]);
                    }
                    ref other => panic!("expected source-poll invocation, got {other:?}"),
                }
                context.clone()
            }
            other => panic!("expected attempt_failed as the first row, got {other:?}"),
        };
        match retry_event(&report.control_events[1]) {
            RetryEvent::SucceededAfterRetry {
                context: Some(success_context),
                total_attempts,
                ..
            } => {
                assert_eq!(*total_attempts, 2);
                assert_eq!(success_context, &context);
            }
            other => panic!("expected succeeded_after_retry as the terminal row, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn threshold_one_breaker_keeps_rate_limited_source_retryable_by_default() {
        let owner = source_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
        let breaker: Arc<dyn SourcePolicy> = Arc::new(CircuitBreakerSourcePolicy {
            breaker: Arc::new(CircuitBreakerMiddleware::new(1)),
        });
        let admissions = Arc::new(AtomicUsize::new(0));
        let boundary = PerSourcePolicyBoundary::new(
            vec![
                breaker,
                Arc::new(RetryOwnerPolicy {
                    owner,
                    log: Arc::new(Mutex::new(Vec::new())),
                    admissions: admissions.clone(),
                    guard_drops: Arc::new(AtomicUsize::new(0)),
                    recovery_allowed: Arc::new(AtomicBool::new(true)),
                    observed: None,
                }),
            ],
            WriterId::from(StageId::new()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([
                Err(SourceError::RateLimited {
                    message: "slow down".to_string(),
                    retry_after: None,
                }),
                Ok(SourcePollCompletion::Batch(vec![test_event()])),
            ]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Ok(SourcePollCompletion::Batch(_)),
                ..
            })
        ));
        assert_eq!(*ordinals.lock().unwrap(), [1, 2]);
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert!(matches!(
            retry_event(&report.control_events[1]),
            RetryEvent::SucceededAfterRetry {
                total_attempts: 2,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn half_open_rate_limited_source_probe_is_single_shot() {
        let owner = source_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
        let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
        breaker.force_half_open_for_test();
        let breaker_policy: Arc<dyn SourcePolicy> = Arc::new(CircuitBreakerSourcePolicy {
            breaker: breaker.clone(),
        });
        let admissions = Arc::new(AtomicUsize::new(0));
        let boundary = PerSourcePolicyBoundary::new(
            vec![
                breaker_policy,
                Arc::new(RetryOwnerPolicy {
                    owner,
                    log: Arc::new(Mutex::new(Vec::new())),
                    admissions: admissions.clone(),
                    guard_drops: Arc::new(AtomicUsize::new(0)),
                    recovery_allowed: Arc::new(AtomicBool::new(true)),
                    observed: None,
                }),
            ],
            WriterId::from(StageId::new()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([
                Err(SourceError::RateLimited {
                    message: "slow down".to_string(),
                    retry_after: None,
                }),
                Ok(SourcePollCompletion::Batch(vec![test_event()])),
            ]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Err(SourceError::RateLimited { .. }),
                ..
            })
        ));
        assert_eq!(*ordinals.lock().unwrap(), [1]);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert!(matches!(
            retry_event(report.control_events.last().expect("terminal retry row")),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::PolicyRejected),
                ..
            }
        ));
    }

    #[tokio::test]
    async fn retryable_source_max_attempts_one_emits_only_terminal_exhaustion() {
        let owner = source_retry_owner(1, Duration::ZERO, Duration::from_secs(1));
        let boundary = retry_boundary(
            owner,
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(true)),
            false,
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([Err(SourceError::Transport("offline".to_string()))]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(report.control_events.len(), 1);
        match retry_event(&report.control_events[0]) {
            RetryEvent::Exhausted {
                context: Some(context),
                total_attempts,
                exhaustion_cause,
                last_error_kind,
                ..
            } => {
                assert_eq!(*total_attempts, 1);
                assert_eq!(*exhaustion_cause, Some(RetryExhaustionCause::MaxAttempts));
                assert_eq!(*last_error_kind, Some(ErrorKind::Remote));
                assert!(matches!(
                    context.invocation,
                    RetryInvocation::SourcePoll { .. }
                ));
            }
            other => panic!("expected one exhausted row, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn source_policy_opening_after_settlement_suppresses_the_next_physical_poll() {
        let boundary = retry_boundary(
            source_retry_owner(3, Duration::ZERO, Duration::from_secs(1)),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(false)),
            false,
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([
                Err(SourceError::Timeout("opens breaker".to_string())),
                Ok(SourcePollCompletion::Batch(vec![test_event()])),
            ]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(report.control_events.len(), 1);
        match retry_event(&report.control_events[0]) {
            RetryEvent::Exhausted {
                total_attempts,
                exhaustion_cause,
                ..
            } => {
                assert_eq!(*total_attempts, 1);
                assert_eq!(
                    *exhaustion_cause,
                    Some(RetryExhaustionCause::PolicyRejected)
                );
            }
            other => panic!("expected policy-rejected exhaustion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn source_retry_after_above_the_ceiling_never_starts_a_second_attempt() {
        let boundary = retry_boundary(
            source_retry_owner(3, Duration::from_millis(5), Duration::from_millis(50)),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(true)),
            false,
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceSourceExecutor {
            outcomes: VecDeque::from([Err(SourceError::RateLimited {
                message: "slow down".to_string(),
                retry_after: Some(Duration::from_millis(51)),
            })]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(report.control_events.len(), 1);
        match retry_event(&report.control_events[0]) {
            RetryEvent::Exhausted {
                total_attempts,
                exhaustion_cause,
                last_error_kind,
                ..
            } => {
                assert_eq!(*total_attempts, 1);
                assert_eq!(
                    *exhaustion_cause,
                    Some(RetryExhaustionCause::RetryHintExceedsLimit)
                );
                assert_eq!(*last_error_kind, Some(ErrorKind::RateLimited));
            }
            other => panic!("expected retry-hint exhaustion, got {other:?}"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn active_source_deadline_returns_cancelled_poll_after_one_charged_attempt() {
        let mut owner = source_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        owner.policy.max_total_wall_time = Duration::from_millis(50);
        let log = Arc::new(Mutex::new(Vec::new()));
        let admissions = Arc::new(AtomicUsize::new(0));
        let guard_drops = Arc::new(AtomicUsize::new(0));
        let boundary = retry_boundary(
            owner,
            log.clone(),
            admissions.clone(),
            guard_drops.clone(),
            Arc::new(AtomicBool::new(true)),
            false,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let calls_for_task = calls.clone();
        let started_for_task = started.clone();
        let dropped_for_task = dropped.clone();

        let task = tokio::spawn(async move {
            let mut executor = PendingSourceExecutor {
                calls: calls_for_task,
                started: started_for_task,
                dropped: dropped_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
                .await
        });

        started.notified().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        let report = task.await.expect("source deadline resolves active poll");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            *log.lock().unwrap(),
            vec!["admit:owner:1", "commit:owner", "observe:owner:failed"]
        );
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Err(SourceError::Timeout(ref message)),
                ..
            }) if message == "source_poll_cancelled"
        ));
        assert_eq!(report.control_events.len(), 1);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::TotalWallTime),
                last_error_kind: Some(ErrorKind::Timeout),
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn force_abort_drops_active_source_and_admission_guard_without_rows() {
        let log = Arc::new(Mutex::new(Vec::new()));
        let admissions = Arc::new(AtomicUsize::new(0));
        let guard_drops = Arc::new(AtomicUsize::new(0));
        let boundary = retry_boundary(
            source_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            log.clone(),
            admissions.clone(),
            guard_drops.clone(),
            Arc::new(AtomicBool::new(true)),
            false,
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
        let calls_for_task = calls.clone();
        let started_for_task = started.clone();
        let dropped_for_task = dropped.clone();

        let task = tokio::spawn(async move {
            let mut executor = PendingSourceExecutor {
                calls: calls_for_task,
                started: started_for_task,
                dropped: dropped_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, receiver)
                .await
        });

        started.notified().await;
        controller.request_abort();
        let report = task.await.expect("force abort cancels active source poll");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
        assert_eq!(*log.lock().unwrap(), vec!["admit:owner:1", "commit:owner"]);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Rejected { ref reason }
                if reason == "force abort requested during source poll"
        ));
        assert!(report.control_events.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_drain_during_active_retryable_source_starts_no_next_attempt() {
        let admissions = Arc::new(AtomicUsize::new(0));
        let guard_drops = Arc::new(AtomicUsize::new(0));
        let boundary = retry_boundary(
            source_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            Arc::new(Mutex::new(Vec::new())),
            admissions.clone(),
            guard_drops.clone(),
            Arc::new(AtomicBool::new(true)),
            false,
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
        let ordinals_for_task = ordinals.clone();
        let started_for_task = started.clone();
        let release_for_task = release.clone();

        let task = tokio::spawn(async move {
            let mut executor = ControlledSourceExecutor {
                outcome: Some(Err(SourceError::Timeout("retryable".to_string()))),
                ordinals: ordinals_for_task,
                started: started_for_task,
                release: release_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, receiver)
                .await
        });

        started.notified().await;
        controller.request_drain();
        release.notify_one();
        let report = task.await.expect("active source poll settles during drain");

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Err(SourceError::Timeout(ref message)),
                ..
            }) if message == "retryable"
        ));
        assert_eq!(report.control_events.len(), 1);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
                last_error_kind: Some(ErrorKind::Timeout),
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_drain_during_source_backoff_records_drain_and_starts_no_next_attempt() {
        let observed = Arc::new(Notify::new());
        let boundary = retry_boundary_with_observer(
            source_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(true)),
            false,
            Some(observed.clone()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

        let task = tokio::spawn(async move {
            let mut executor = SequenceSourceExecutor {
                outcomes: VecDeque::from([
                    Err(SourceError::Timeout("retryable".to_string())),
                    Ok(SourcePollCompletion::Batch(Vec::new())),
                ]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, receiver)
                .await
        });

        observed.notified().await;
        controller.request_drain();
        let report = task.await.expect("drain interrupts source backoff");

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Err(SourceError::Timeout(ref message)),
                ..
            }) if message == "retryable"
        ));
        assert_eq!(report.control_events.len(), 2);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::AttemptFailed {
                attempt_number: 1,
                delay_ms: Some(10_000),
                ..
            }
        ));
        assert!(matches!(
            retry_event(&report.control_events[1]),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
                last_error_kind: Some(ErrorKind::Timeout),
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn after_poll_deadline_preserves_known_batch_and_observes_every_policy() {
        let mut owner = source_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        owner.policy.max_total_wall_time = Duration::from_millis(50);
        let skipped_after_poll_calls = Arc::new(AtomicUsize::new(0));
        let outer_observations = Arc::new(AtomicUsize::new(0));
        let owner_observations = Arc::new(AtomicUsize::new(0));
        let after_poll_started = Arc::new(Notify::new());
        let boundary = PerSourcePolicyBoundary::new(
            vec![
                Arc::new(ObserveOnlyAfterDeadlinePolicy {
                    after_poll_calls: skipped_after_poll_calls.clone(),
                    observations: outer_observations.clone(),
                }),
                Arc::new(BlockingAfterPollRetryOwnerPolicy {
                    owner,
                    after_poll_started: after_poll_started.clone(),
                    observations: owner_observations.clone(),
                }),
            ],
            WriterId::from(StageId::new()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();

        let task = tokio::spawn(async move {
            let mut executor = SequenceSourceExecutor {
                outcomes: VecDeque::from([Ok(SourcePollCompletion::Batch(vec![test_event()]))]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
                .await
        });

        after_poll_started.notified().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        let report = task.await.expect("source settlement deadline resolves");

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(skipped_after_poll_calls.load(Ordering::SeqCst), 0);
        assert_eq!(owner_observations.load(Ordering::SeqCst), 1);
        assert_eq!(outer_observations.load(Ordering::SeqCst), 1);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Ok(SourcePollCompletion::Batch(ref batch)),
                ..
            }) if batch.len() == 1
        ));
        assert!(report.control_events.is_empty());
    }

    #[tokio::test]
    async fn graceful_drain_during_after_poll_settles_the_known_batch() {
        let after_poll_started = Arc::new(Notify::new());
        let after_poll_release = Arc::new(Notify::new());
        let after_poll_calls = Arc::new(AtomicUsize::new(0));
        let observations = Arc::new(AtomicUsize::new(0));
        let boundary = PerSourcePolicyBoundary::new(
            vec![Arc::new(ControlledAfterPollRetryOwnerPolicy {
                owner: source_retry_owner(3, Duration::ZERO, Duration::from_secs(1)),
                after_poll_started: after_poll_started.clone(),
                after_poll_release: after_poll_release.clone(),
                after_poll_calls: after_poll_calls.clone(),
                observations: observations.clone(),
            })],
            WriterId::from(StageId::new()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

        let task = tokio::spawn(async move {
            let mut executor = SequenceSourceExecutor {
                outcomes: VecDeque::from([Ok(SourcePollCompletion::Batch(vec![test_event()]))]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, receiver)
                .await
        });

        after_poll_started.notified().await;
        controller.request_drain();
        tokio::task::yield_now().await;
        assert!(
            !task.is_finished(),
            "graceful drain must not cancel settlement of a known source result"
        );
        after_poll_release.notify_one();
        let report = task
            .await
            .expect("source settlement completes during drain");

        assert_eq!(*ordinals.lock().unwrap(), [1]);
        assert_eq!(after_poll_calls.load(Ordering::SeqCst), 1);
        assert_eq!(observations.load(Ordering::SeqCst), 1);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Ok(SourcePollCompletion::Batch(ref batch)),
                ..
            }) if batch.len() == 1
        ));
        assert!(report.control_events.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn source_backoff_deadline_starts_no_next_attempt() {
        let mut owner = source_retry_owner(3, Duration::from_millis(50), Duration::from_millis(50));
        owner.policy.max_total_wall_time = Duration::from_millis(100);
        let observed = Arc::new(Notify::new());
        let boundary = retry_boundary_with_observer(
            owner,
            Arc::new(Mutex::new(Vec::new())),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(AtomicBool::new(true)),
            false,
            Some(observed.clone()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();
        let task = tokio::spawn(async move {
            let mut executor = SequenceSourceExecutor {
                outcomes: VecDeque::from([
                    Err(SourceError::Timeout("retryable".to_string())),
                    Ok(SourcePollCompletion::Batch(Vec::new())),
                ]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_poll(&mut executor, BoundaryStopReceiver::default())
                .await
        });

        observed.notified().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        let report = task.await.expect("deadline interrupts source backoff");

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert!(matches!(
            report.outcome,
            SourceBoundaryOutcome::Polled(SourcePollReport {
                result: Err(SourceError::Timeout(ref message)),
                ..
            }) if message == "retryable"
        ));
        assert_eq!(report.control_events.len(), 2);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::AttemptFailed {
                attempt_number: 1,
                delay_ms: Some(50),
                ..
            }
        ));
        assert!(matches!(
            retry_event(&report.control_events[1]),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::TotalWallTime),
                last_error_kind: Some(ErrorKind::Timeout),
                ..
            }
        ));
    }
}
