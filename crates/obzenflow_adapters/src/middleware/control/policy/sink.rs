// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Adapter-owned sink-delivery policy boundary (FLOWIP-115b).
//!
//! The runtime sees only `SinkDeliveryBoundary`. This module owns the middleware
//! policy onion hidden behind that seam: admission runs forward, the delivery
//! attempt runs once, and observation runs in reverse over the raw outcome,
//! mirroring the source (FLOWIP-115a) and effect (FLOWIP-120c) boundaries.

use super::retry::{
    attempt_failed_event, await_active_execution, await_before_execution, exhausted_event,
    succeeded_event, AttemptDisposition, RetryBudget, RetryWaitError,
};
use crate::middleware::{BoundaryRetryOwner, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryInvocation, RetryLifecycleContext, RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::SinkConsumeReport;
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptOutcome, SinkDeliveryBoundary, SinkDeliveryBoundaryOutcome,
    SinkDeliveryBoundaryReport, SinkDeliveryExecutor, SinkDeliveryRejection,
};
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

/// RAII guard returned by sink-policy admission for a reserved resource (such as
/// a half-open probe slot), held by the boundary across the delivery attempt.
pub trait SinkAdmissionGuard: Send + Sync {}

impl<T: Send + Sync> SinkAdmissionGuard for T {}

/// Admission decision from one sink-delivery policy.
pub enum SinkAdmission {
    /// Admit the delivery, optionally holding a guard across the attempt.
    Admit(Option<Box<dyn SinkAdmissionGuard>>),
    /// Reject before delivery. The supervisor maps this to a failed delivery
    /// receipt, never a successful `Noop`.
    Reject { reason: String },
}

/// Raw sink-delivery outcome shown independently to each admitted policy.
pub enum SinkDeliveryPolicyOutcome<'a> {
    /// The handler ran and returned a consume report.
    Delivered { report: &'a SinkConsumeReport },
    /// The handler errored or panicked. Typed health-classification facts are
    /// retained separately in the policy context, leaving retry eligibility
    /// to the boundary coordinator.
    Failed,
    /// A later policy rejected before delivery; the protected call never went out.
    RejectedBy {
        policy: &'static str,
        reason: &'a str,
    },
}

/// Sink-shaped policy context. It owns the observability outbox returned by the
/// boundary report and never crosses into the runtime supervisor.
pub struct SinkPolicyCtx {
    middleware_ctx: MiddlewareContext,
    attempt_failure: Option<(ErrorKind, Option<Duration>)>,
}

impl Default for SinkPolicyCtx {
    fn default() -> Self {
        Self::new()
    }
}

impl SinkPolicyCtx {
    pub fn new() -> Self {
        Self {
            middleware_ctx: MiddlewareContext::with_scope(
                MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
            ),
            attempt_failure: None,
        }
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

    pub(crate) fn set_attempt_failure(&mut self, kind: ErrorKind, retry_after: Option<Duration>) {
        self.attempt_failure = Some((kind, retry_after));
    }

    pub(crate) fn attempt_failure(&self) -> Option<&(ErrorKind, Option<Duration>)> {
        self.attempt_failure.as_ref()
    }
}

/// A sink-delivery resilience policy behind the adapter-owned boundary.
///
/// The boundary owns typed delivery identity and attempt facts; policies see
/// only the control outcome facts they actually consume. Future sink policies
/// may extend this contract with a same-slice reader and proof.
#[async_trait]
pub trait SinkPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission;

    /// Commit any execution-based reservations immediately before the sink
    /// delivery executor starts. Default policies reserve no such resource.
    fn commit_execution(&self, _ctx: &mut SinkPolicyCtx) {}

    fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, ctx: &mut SinkPolicyCtx);

    #[doc(hidden)]
    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        None
    }

    #[doc(hidden)]
    fn recovery_allowed_after_settlement(&self, _ctx: &SinkPolicyCtx) -> bool {
        true
    }
}

/// Sink-delivery boundary backed by a declared-order policy chain.
pub struct PerSinkDeliveryPolicyBoundary {
    policies: Arc<Vec<Arc<dyn SinkPolicy>>>,
}

impl PerSinkDeliveryPolicyBoundary {
    pub fn new(policies: Vec<Arc<dyn SinkPolicy>>) -> Self {
        Self {
            policies: Arc::new(policies),
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

type SinkAdmitGuard = Option<Box<dyn SinkAdmissionGuard>>;

#[async_trait]
impl SinkDeliveryBoundary for PerSinkDeliveryPolicyBoundary {
    async fn around_sink_delivery(
        &self,
        execute: &mut dyn SinkDeliveryExecutor,
    ) -> SinkDeliveryBoundaryReport {
        if self.policies.is_empty() {
            return SinkDeliveryBoundaryReport {
                outcome: SinkDeliveryBoundaryOutcome::Attempted(execute.attempt().await),
                control_events: Vec::new(),
            };
        }

        let mut ctx = SinkPolicyCtx::new();
        let mut admitted: Vec<(&Arc<dyn SinkPolicy>, SinkAdmitGuard)> = Vec::new();

        for policy in self.policies.iter() {
            match policy.admit(&mut ctx).await {
                SinkAdmission::Admit(guard) => admitted.push((policy, guard)),
                SinkAdmission::Reject { reason } => {
                    let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
                        policy: policy.label(),
                        reason: &reason,
                    };
                    for (prior, _) in admitted.iter().rev() {
                        prior.observe(&outcome, &mut ctx);
                    }
                    return SinkDeliveryBoundaryReport {
                        outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                            policy: policy.label().to_string(),
                            reason,
                        }),
                        control_events: ctx.take_control_events(),
                    };
                }
            }
        }

        for (policy, _) in &admitted {
            policy.commit_execution(&mut ctx);
        }
        let attempt_outcome = execute.attempt().await;
        let policy_outcome = match &attempt_outcome {
            SinkDeliveryAttemptOutcome::Delivered(Ok(report)) => {
                SinkDeliveryPolicyOutcome::Delivered { report }
            }
            SinkDeliveryAttemptOutcome::Delivered(Err(error)) => {
                ctx.set_attempt_failure(error.kind(), handler_retry_after(error));
                SinkDeliveryPolicyOutcome::Failed
            }
            SinkDeliveryAttemptOutcome::Panicked { .. } => {
                ctx.set_attempt_failure(ErrorKind::Unknown, None);
                SinkDeliveryPolicyOutcome::Failed
            }
        };
        for (policy, _) in admitted.iter().rev() {
            policy.observe(&policy_outcome, &mut ctx);
        }

        SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
            control_events: ctx.take_control_events(),
        }
    }

    async fn around_retryable_sink_delivery(
        &self,
        execute: &mut dyn SinkDeliveryExecutor,
        mut stop: BoundaryStopReceiver,
    ) -> SinkDeliveryBoundaryReport {
        let Some(owner) = self.retry_owner() else {
            return self.around_sink_delivery(execute).await;
        };
        let Some(budget) = RetryBudget::new(owner.policy.clone()) else {
            return SinkDeliveryBoundaryReport {
                outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                    policy: "circuit_breaker".to_string(),
                    reason: format!(
                        "circuit_breaker retry rejected for sink-delivery '{}': max_total_wall_time cannot be represented from the current monotonic instant; choose a smaller limit",
                        owner.protected_unit_label
                    ),
                }),
                control_events: Vec::new(),
            };
        };
        let parent_event_id = execute.parent_event_id();
        let lifecycle = RetryLifecycleContext {
            stage_id: owner.stage_id,
            attachment_id: owner.attachment_id.as_ulid(),
            protected_unit: RetryProtectedUnit::SinkDelivery {
                configured_target_id: owner.sink_configured_target_id,
            },
            invocation: RetryInvocation::SinkDelivery { parent_event_id },
        };
        let mut attempt = NonZeroU32::MIN;
        let mut recovery_started = false;
        let mut invocation_events = Vec::new();

        loop {
            let mut ctx = SinkPolicyCtx::new();
            let mut admitted: Vec<(&Arc<dyn SinkPolicy>, SinkAdmitGuard)> = Vec::new();
            for policy in self.policies.iter() {
                let admission =
                    await_before_execution(policy.admit(&mut ctx), budget.deadline(), &mut stop)
                        .await;
                match admission {
                    Ok(SinkAdmission::Admit(guard)) => admitted.push((policy, guard)),
                    Ok(SinkAdmission::Reject { reason }) => {
                        let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
                            policy: policy.label(),
                            reason: &reason,
                        };
                        for (prior, _) in admitted.iter().rev() {
                            prior.observe(&outcome, &mut ctx);
                        }
                        invocation_events.extend(ctx.take_control_events());
                        if recovery_started {
                            invocation_events.push(exhausted_event(
                                &owner,
                                &lifecycle,
                                attempt.get().saturating_sub(1),
                                RetryExhaustionCause::PolicyRejected,
                                None,
                                &budget,
                                Some(parent_event_id),
                            ));
                        }
                        return SinkDeliveryBoundaryReport {
                            outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                                policy: policy.label().to_string(),
                                reason,
                            }),
                            control_events: invocation_events,
                        };
                    }
                    Err(wait_error) => {
                        let reason = match wait_error {
                            RetryWaitError::Deadline => {
                                "retry total-wall deadline expired during sink policy admission"
                            }
                            RetryWaitError::Drain => {
                                "graceful drain requested during sink policy admission"
                            }
                            RetryWaitError::Abort => {
                                "force abort requested during sink policy admission"
                            }
                        };
                        let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
                            policy: "retry_coordinator",
                            reason,
                        };
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
                                if matches!(wait_error, RetryWaitError::Deadline) {
                                    RetryExhaustionCause::TotalWallTime
                                } else {
                                    RetryExhaustionCause::DrainRequested
                                },
                                None,
                                &budget,
                                Some(parent_event_id),
                            ));
                        }
                        return SinkDeliveryBoundaryReport {
                            outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                                policy: "retry_coordinator".to_string(),
                                reason: reason.to_string(),
                            }),
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
                        "retry total-wall deadline expired before sink executor start"
                    }
                    RetryWaitError::Drain => "graceful drain requested before sink executor start",
                    RetryWaitError::Abort => "force abort requested before sink executor start",
                };
                let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
                    policy: "retry_coordinator",
                    reason,
                };
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
                        Some(parent_event_id),
                    ));
                }
                return SinkDeliveryBoundaryReport {
                    outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                        policy: "retry_coordinator".to_string(),
                        reason: reason.to_string(),
                    }),
                    control_events: if matches!(barrier, RetryWaitError::Abort) {
                        Vec::new()
                    } else {
                        invocation_events
                    },
                };
            }

            let active = await_active_execution(
                || {
                    for (policy, _) in &admitted {
                        policy.commit_execution(&mut ctx);
                    }
                    execute.attempt()
                },
                budget.deadline(),
                &mut stop,
            )
            .await;
            let (attempt_outcome, drain_requested, active_deadline) = match active {
                Ok((outcome, drain_requested)) => (outcome, drain_requested, false),
                Err(RetryWaitError::Deadline) => (
                    SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Timeout(
                        "deadline_outcome_unknown".to_string(),
                    ))),
                    false,
                    true,
                ),
                Err(RetryWaitError::Drain) => {
                    let reason = "graceful drain requested before sink executor start";
                    let outcome = SinkDeliveryPolicyOutcome::RejectedBy {
                        policy: "retry_coordinator",
                        reason,
                    };
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
                            Some(parent_event_id),
                        ));
                    }
                    return SinkDeliveryBoundaryReport {
                        outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                            policy: "retry_coordinator".to_string(),
                            reason: reason.to_string(),
                        }),
                        control_events: invocation_events,
                    };
                }
                Err(RetryWaitError::Abort) => {
                    return SinkDeliveryBoundaryReport {
                        outcome: SinkDeliveryBoundaryOutcome::Rejected(SinkDeliveryRejection {
                            policy: "retry_coordinator".to_string(),
                            reason: "force abort requested during sink delivery".to_string(),
                        }),
                        control_events: Vec::new(),
                    };
                }
            };
            let policy_outcome = match &attempt_outcome {
                SinkDeliveryAttemptOutcome::Delivered(Ok(report)) => {
                    SinkDeliveryPolicyOutcome::Delivered { report }
                }
                SinkDeliveryAttemptOutcome::Delivered(Err(error)) => {
                    ctx.set_attempt_failure(error.kind(), handler_retry_after(error));
                    SinkDeliveryPolicyOutcome::Failed
                }
                SinkDeliveryAttemptOutcome::Panicked { .. } => {
                    ctx.set_attempt_failure(ErrorKind::Unknown, None);
                    SinkDeliveryPolicyOutcome::Failed
                }
            };
            for (policy, _) in admitted.iter().rev() {
                policy.observe(&policy_outcome, &mut ctx);
            }
            invocation_events.extend(ctx.take_control_events());
            let recovery_allowed = self
                .policies
                .iter()
                .all(|policy| policy.recovery_allowed_after_settlement(&ctx));
            drop(admitted);
            drop(ctx);

            if active_deadline {
                invocation_events.push(exhausted_event(
                    &owner,
                    &lifecycle,
                    attempt.get(),
                    RetryExhaustionCause::TotalWallTime,
                    Some(ErrorKind::Timeout),
                    &budget,
                    Some(parent_event_id),
                ));
                return SinkDeliveryBoundaryReport {
                    outcome: SinkDeliveryBoundaryOutcome::DeadlineOutcomeUnknown {
                        message:
                            "sink deadline expired while the external outcome remained in doubt"
                                .to_string(),
                    },
                    control_events: invocation_events,
                };
            }

            match sink_attempt_disposition(&attempt_outcome) {
                AttemptDisposition::Completed => {
                    if recovery_started {
                        invocation_events.push(succeeded_event(
                            &owner,
                            &lifecycle,
                            attempt.get(),
                            &budget,
                            Some(parent_event_id),
                        ));
                    }
                    return SinkDeliveryBoundaryReport {
                        outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
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
                            Some(parent_event_id),
                        ));
                    }
                    return SinkDeliveryBoundaryReport {
                        outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
                        control_events: invocation_events,
                    };
                }
                AttemptDisposition::RetryableFailure { kind, retry_after } => {
                    let barrier = if drain_requested {
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
                            Some(parent_event_id),
                        ));
                        return SinkDeliveryBoundaryReport {
                            outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
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
                                Some(parent_event_id),
                            ));
                            return SinkDeliveryBoundaryReport {
                                outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
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
                        Some(parent_event_id),
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
                        Err(wait_error @ (RetryWaitError::Deadline | RetryWaitError::Drain)) => {
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
                                Some(parent_event_id),
                            ));
                            return SinkDeliveryBoundaryReport {
                                outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
                                control_events: invocation_events,
                            };
                        }
                        Err(RetryWaitError::Abort) => {
                            return SinkDeliveryBoundaryReport {
                                outcome: SinkDeliveryBoundaryOutcome::Attempted(attempt_outcome),
                                control_events: Vec::new(),
                            };
                        }
                    }
                }
                AttemptDisposition::NotExecuted => unreachable!(),
            }
        }
    }
}

fn sink_attempt_disposition(outcome: &SinkDeliveryAttemptOutcome) -> AttemptDisposition {
    match outcome {
        SinkDeliveryAttemptOutcome::Delivered(Ok(_)) => AttemptDisposition::Completed,
        SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Timeout(_))) => {
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Timeout,
                retry_after: None,
            }
        }
        SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(_))) => {
            AttemptDisposition::RetryableFailure {
                kind: ErrorKind::Remote,
                retry_after: None,
            }
        }
        SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::RateLimited {
            retry_after,
            ..
        })) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::RateLimited,
            retry_after: *retry_after,
        },
        SinkDeliveryAttemptOutcome::Delivered(Err(error)) => {
            AttemptDisposition::TerminalFailure { kind: error.kind() }
        }
        SinkDeliveryAttemptOutcome::Panicked { .. } => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Unknown,
        },
    }
}

fn handler_retry_after(error: &HandlerError) -> Option<Duration> {
    match error {
        HandlerError::RateLimited { retry_after, .. } => *retry_after,
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::circuit_breaker::{
        CircuitBreakerBuilder, CircuitBreakerMiddleware, CircuitBreakerSinkPolicy,
    };
    use crate::middleware::control::rate_limiter::RateLimiterFactory;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::{
        BoundaryRetryPolicy, ControlMiddlewareRole, Middleware, MiddlewareAttachmentId,
        MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareDeclarationIndex,
        MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
        MiddlewareMaterializationContext, MiddlewareOrigin, MiddlewareOverrideKey,
        MiddlewarePlanContribution, MiddlewareSurface, MiddlewareSurfaceAttachment,
        MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId, SinkDeliverySurface,
        SinkDeliveryTarget, SinkDeliveryUnitId, TopologyMiddlewareConfigSlot,
    };
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::payloads::observability_payload::{
        MiddlewareLifecycle, ObservabilityPayload, RetryEvent,
    };
    use obzenflow_core::event::{ChainEventContent, EventId};
    use obzenflow_core::{MiddlewareContextKey, StageId, Ulid, WriterId};
    use obzenflow_runtime::control_plane::ControlPlaneProvider;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
    use std::collections::VecDeque;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Notify;

    fn sink_retry_owner(
        max_attempts: u32,
        delay: Duration,
        max_single_delay: Duration,
    ) -> BoundaryRetryOwner {
        let stage_id = StageId::from_ulid(Ulid::from(0x115_0003_u128));
        let declaration = MiddlewareDeclaration::control(
            "test.sink_retry_owner",
            vec![MiddlewareSurfaceKind::SinkDelivery],
        );
        let surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
            stage_id,
            configured_target: None,
        });
        let protected_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
                target: SinkDeliveryTarget::Stage,
            }),
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
            protected_unit_label: "payments_sink".to_string(),
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
    fn sink_disposition_matrix_is_exhaustive_and_preserves_retry_hints() {
        let retry_after = Duration::from_millis(725);
        let errors = [
            (
                HandlerError::Timeout("timeout".to_string()),
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::Timeout,
                    retry_after: None,
                },
            ),
            (
                HandlerError::Remote("remote".to_string()),
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::Remote,
                    retry_after: None,
                },
            ),
            (
                HandlerError::RateLimited {
                    message: "rate limited with hint".to_string(),
                    retry_after: Some(retry_after),
                },
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::RateLimited,
                    retry_after: Some(retry_after),
                },
            ),
            (
                HandlerError::RateLimited {
                    message: "rate limited without hint".to_string(),
                    retry_after: None,
                },
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::RateLimited,
                    retry_after: None,
                },
            ),
            (
                HandlerError::PermanentFailure("permanent".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::PermanentFailure,
                },
            ),
            (
                HandlerError::Deserialization("malformed".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Deserialization,
                },
            ),
            (
                HandlerError::Validation("invalid".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Validation,
                },
            ),
            (
                HandlerError::Domain("domain".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Domain,
                },
            ),
            (
                HandlerError::Other("unknown".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
        ];

        for (error, expected) in errors {
            assert_eq!(
                sink_attempt_disposition(&SinkDeliveryAttemptOutcome::Delivered(Err(error))),
                expected
            );
        }

        let reports = [
            DeliveryPayload::success(DeliveryMethod::Noop, None),
            DeliveryPayload::buffered(DeliveryMethod::Noop, None),
            DeliveryPayload::failed(DeliveryMethod::Noop, "terminal", "failed", true),
            DeliveryPayload::partial(DeliveryMethod::Noop, 1, 1, "partial", None),
        ];
        for payload in reports {
            let outcome = SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(
                SinkConsumeReport::new(payload),
            )));
            assert_eq!(
                sink_attempt_disposition(&outcome),
                AttemptDisposition::Completed
            );
        }

        assert_eq!(
            sink_attempt_disposition(&SinkDeliveryAttemptOutcome::Panicked {
                message: "panic".to_string(),
            }),
            AttemptDisposition::TerminalFailure {
                kind: ErrorKind::Unknown,
            }
        );
    }

    struct SinkRetryContextMarker;

    impl MiddlewareContextKey for SinkRetryContextMarker {
        type Value = ();
        const LABEL: &'static str = "sink_retry_test_marker";
    }

    struct SinkRetryOwnerPolicy {
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
        guard_drops: Option<Arc<AtomicUsize>>,
        observed: Option<Arc<Notify>>,
    }

    #[async_trait]
    impl SinkPolicy for SinkRetryOwnerPolicy {
        fn label(&self) -> &'static str {
            "sink_retry_owner"
        }

        async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission {
            let middleware = ctx.middleware_context_mut();
            assert!(
                !middleware.contains::<SinkRetryContextMarker>(),
                "each sink attempt must receive a fresh policy context"
            );
            middleware.insert::<SinkRetryContextMarker>(());
            let attempt = self.admissions.fetch_add(1, Ordering::SeqCst) + 1;
            self.log.lock().unwrap().push(format!("admit:{attempt}"));
            let guard = self.guard_drops.as_ref().map(|drops| {
                Box::new(SinkDropCounter(drops.clone())) as Box<dyn SinkAdmissionGuard>
            });
            SinkAdmission::Admit(guard)
        }

        fn commit_execution(&self, _ctx: &mut SinkPolicyCtx) {
            self.log.lock().unwrap().push("commit".to_string());
        }

        fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, _ctx: &mut SinkPolicyCtx) {
            let label = match outcome {
                SinkDeliveryPolicyOutcome::Delivered { .. } => "delivered",
                SinkDeliveryPolicyOutcome::Failed => "failed",
                SinkDeliveryPolicyOutcome::RejectedBy { .. } => "rejected",
            };
            self.log.lock().unwrap().push(format!("observe:{label}"));
            if let Some(observed) = &self.observed {
                observed.notify_one();
            }
        }

        fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
            Some(self.owner.clone())
        }
    }

    struct SequenceSinkExecutor {
        parent_event_id: EventId,
        outcomes: VecDeque<SinkDeliveryAttemptOutcome>,
        calls: Arc<AtomicUsize>,
    }

    struct ControlledSinkExecutor {
        parent_event_id: EventId,
        outcome: Option<SinkDeliveryAttemptOutcome>,
        calls: Arc<AtomicUsize>,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct PendingSinkExecutor {
        parent_event_id: EventId,
        calls: Arc<AtomicUsize>,
        started: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
    }

    struct SinkDropCounter(Arc<AtomicUsize>);

    /// Test-only decorator around a real surface adapter. The wrapped limiter
    /// and breaker still own every admission, commit, settlement, metric, and
    /// retry decision; this records only the boundary calls made to them.
    struct TracedSinkPolicy {
        inner: Arc<dyn SinkPolicy>,
        trace: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl SinkPolicy for TracedSinkPolicy {
        fn label(&self) -> &'static str {
            self.inner.label()
        }

        async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission {
            self.trace
                .lock()
                .unwrap()
                .push(format!("admit:{}", self.inner.label()));
            self.inner.admit(ctx).await
        }

        fn commit_execution(&self, ctx: &mut SinkPolicyCtx) {
            self.trace
                .lock()
                .unwrap()
                .push(format!("commit:{}", self.inner.label()));
            self.inner.commit_execution(ctx);
        }

        fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, ctx: &mut SinkPolicyCtx) {
            let outcome_label = match outcome {
                SinkDeliveryPolicyOutcome::Delivered { .. } => "delivered",
                SinkDeliveryPolicyOutcome::Failed => "failed",
                SinkDeliveryPolicyOutcome::RejectedBy { .. } => "not_executed",
            };
            self.trace
                .lock()
                .unwrap()
                .push(format!("observe:{}:{outcome_label}", self.inner.label()));
            self.inner.observe(outcome, ctx);
        }

        fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
            self.inner.retry_owner()
        }

        fn recovery_allowed_after_settlement(&self, ctx: &SinkPolicyCtx) -> bool {
            self.inner.recovery_allowed_after_settlement(ctx)
        }
    }

    impl Drop for SinkDropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[async_trait]
    impl SinkDeliveryExecutor for PendingSinkExecutor {
        fn parent_event_id(&self) -> EventId {
            self.parent_event_id
        }

        async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let _drop_counter = SinkDropCounter(self.dropped.clone());
            self.started.notify_one();
            std::future::pending::<SinkDeliveryAttemptOutcome>().await
        }
    }

    #[async_trait]
    impl SinkDeliveryExecutor for SequenceSinkExecutor {
        fn parent_event_id(&self) -> EventId {
            self.parent_event_id
        }

        async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.outcomes
                .pop_front()
                .expect("sink test executor has an outcome for every attempt")
        }
    }

    #[async_trait]
    impl SinkDeliveryExecutor for ControlledSinkExecutor {
        fn parent_event_id(&self) -> EventId {
            self.parent_event_id
        }

        async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let outcome = self
                .outcome
                .take()
                .expect("controlled sink executor runs exactly once");
            self.started.notify_one();
            self.release.notified().await;
            outcome
        }
    }

    fn retry_sink_boundary(
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
    ) -> PerSinkDeliveryPolicyBoundary {
        retry_sink_boundary_with_observer_and_guard(owner, admissions, log, None, None)
    }

    fn retry_sink_boundary_with_guard(
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
        guard_drops: Option<Arc<AtomicUsize>>,
    ) -> PerSinkDeliveryPolicyBoundary {
        retry_sink_boundary_with_observer_and_guard(owner, admissions, log, None, guard_drops)
    }

    fn retry_sink_boundary_with_observer(
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
        observed: Arc<Notify>,
    ) -> PerSinkDeliveryPolicyBoundary {
        retry_sink_boundary_with_observer_and_guard(owner, admissions, log, Some(observed), None)
    }

    fn retry_sink_boundary_with_observer_and_guard(
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
        observed: Option<Arc<Notify>>,
        guard_drops: Option<Arc<AtomicUsize>>,
    ) -> PerSinkDeliveryPolicyBoundary {
        PerSinkDeliveryPolicyBoundary::new(vec![Arc::new(SinkRetryOwnerPolicy {
            owner,
            admissions,
            log,
            guard_drops,
            observed,
        })])
    }

    fn materialize_sink_policy(
        factory: &dyn MiddlewareFactory,
        config: &StageConfig,
        control: &Arc<ControlMiddlewareAggregator>,
        declaration_index: usize,
    ) -> Arc<dyn SinkPolicy> {
        let surface = MiddlewareSurface::SinkDelivery(SinkDeliverySurface {
            stage_id: config.stage_id,
            configured_target: None,
        });
        let protected_unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::SinkDelivery(SinkDeliveryUnitId {
                target: SinkDeliveryTarget::Stage,
            }),
        };
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(declaration_index),
        };
        let materialization = MiddlewareMaterializationContext {
            config,
            control_middleware: control,
            stage_type: obzenflow_core::event::context::StageType::Sink,
        };

        match factory
            .materialize(request, &materialization)
            .expect("real sink policy should materialize")
        {
            MiddlewareSurfaceAttachment::SinkDelivery(policy) => policy,
            _ => panic!("factory declared a sink-delivery surface but returned another surface"),
        }
    }

    fn traced_sink_policy(
        inner: Arc<dyn SinkPolicy>,
        trace: &Arc<Mutex<Vec<String>>>,
    ) -> Arc<dyn SinkPolicy> {
        Arc::new(TracedSinkPolicy {
            inner,
            trace: trace.clone(),
        })
    }

    fn sink_stage_config(stage_id: StageId, name: &str) -> StageConfig {
        StageConfig {
            stage_id,
            name: name.to_string(),
            flow_name: "retry_policy_order_test".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            resolved_policies: Default::default(),
        }
    }

    /// A third-party (non-breaker) sink policy that always rejects.
    struct AlwaysRejectPolicy;

    #[async_trait]
    impl SinkPolicy for AlwaysRejectPolicy {
        fn label(&self) -> &'static str {
            "third_party_reject"
        }

        async fn admit(&self, _ctx: &mut SinkPolicyCtx) -> SinkAdmission {
            SinkAdmission::Reject {
                reason: "third party policy".to_string(),
            }
        }

        fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, _ctx: &mut SinkPolicyCtx) {}
    }

    /// An executor that must never run when a policy rejects before delivery.
    struct PanicExecutor;

    #[async_trait]
    impl SinkDeliveryExecutor for PanicExecutor {
        fn parent_event_id(&self) -> obzenflow_core::EventId {
            obzenflow_core::EventId::new()
        }

        async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
            panic!("executor must not run when a sink policy rejects");
        }
    }

    /// Override-key family marker for the third-party test factory.
    struct ThirdPartyFamily;

    /// A minimal third-party `MiddlewareFactory` that is NOT the circuit breaker
    /// (its `control_role` is `None`) yet attaches through the same carrier by
    /// declaring a control surface and implementing `materialize`.
    struct ThirdPartySinkFactory;

    impl MiddlewareFactory for ThirdPartySinkFactory {
        fn label(&self) -> &'static str {
            "third_party_sink"
        }

        fn control_role(&self) -> ControlMiddlewareRole {
            ControlMiddlewareRole::None
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            MiddlewareOverrideKey::of::<ThirdPartyFamily>("third_party_sink")
        }

        fn plan_contribution(&self) -> MiddlewarePlanContribution {
            MiddlewarePlanContribution::None
        }

        fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
            None
        }

        fn create(
            &self,
            _config: &StageConfig,
            _control: Arc<ControlMiddlewareAggregator>,
        ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
            // Hook-bound only: it is placed via `materialize`, never the legacy
            // handler-shell `create` path.
            Err(MiddlewareFactoryError::not_hook_bound(self.label()))
        }

        fn declaration(&self) -> MiddlewareDeclaration {
            MiddlewareDeclaration::control(self.label(), vec![MiddlewareSurfaceKind::SinkDelivery])
        }

        fn materialize(
            &self,
            request: MiddlewareAttachmentRequest<'_>,
            _ctx: &MiddlewareMaterializationContext<'_>,
        ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
            match request.surface {
                MiddlewareSurface::SinkDelivery(_) => Ok(
                    MiddlewareSurfaceAttachment::SinkDelivery(Arc::new(AlwaysRejectPolicy)),
                ),
                other => Err(MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    "third_party_test",
                    std::io::Error::other(format!("unsupported surface {:?}", other.kind())),
                )),
            }
        }
    }

    #[tokio::test]
    async fn real_limiter_and_breaker_retry_onion_preserves_both_declared_orders() {
        for limiter_first in [true, false] {
            let stage_id = StageId::new();
            let config = sink_stage_config(stage_id, "ordered_sink");
            let control = Arc::new(ControlMiddlewareAggregator::new());
            let limiter_factory = RateLimiterFactory::new(100_000.0).with_burst(3.0);
            let breaker_factory = CircuitBreakerBuilder::new(10)
                .with_retry_fixed(Duration::ZERO, 3)
                .build();
            let limiter = materialize_sink_policy(&limiter_factory, &config, &control, 0);
            let breaker = materialize_sink_policy(breaker_factory.as_ref(), &config, &control, 1);
            let trace = Arc::new(Mutex::new(Vec::new()));
            let limiter = traced_sink_policy(limiter, &trace);
            let breaker = traced_sink_policy(breaker, &trace);
            let policies = if limiter_first {
                vec![limiter, breaker]
            } else {
                vec![breaker, limiter]
            };
            let boundary = PerSinkDeliveryPolicyBoundary::new(policies);
            let calls = Arc::new(AtomicUsize::new(0));
            let mut executor = SequenceSinkExecutor {
                parent_event_id: EventId::new(),
                outcomes: VecDeque::from([
                    SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                        "first transient failure".to_string(),
                    ))),
                    SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                        "second transient failure".to_string(),
                    ))),
                    SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                        DeliveryPayload::success(DeliveryMethod::Noop, None),
                    )))),
                ]),
                calls: calls.clone(),
            };

            let report = boundary
                .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
                .await;

            assert!(matches!(
                report.outcome,
                SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(
                    _
                )))
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 3);

            let expected = if limiter_first {
                concat!(
                    "admit:rate_limiter|admit:circuit_breaker|",
                    "commit:rate_limiter|commit:circuit_breaker|",
                    "observe:circuit_breaker:failed|observe:rate_limiter:failed|",
                    "admit:rate_limiter|admit:circuit_breaker|",
                    "commit:rate_limiter|commit:circuit_breaker|",
                    "observe:circuit_breaker:failed|observe:rate_limiter:failed|",
                    "admit:rate_limiter|admit:circuit_breaker|",
                    "commit:rate_limiter|commit:circuit_breaker|",
                    "observe:circuit_breaker:delivered|observe:rate_limiter:delivered"
                )
            } else {
                concat!(
                    "admit:circuit_breaker|admit:rate_limiter|",
                    "commit:circuit_breaker|commit:rate_limiter|",
                    "observe:rate_limiter:failed|observe:circuit_breaker:failed|",
                    "admit:circuit_breaker|admit:rate_limiter|",
                    "commit:circuit_breaker|commit:rate_limiter|",
                    "observe:rate_limiter:failed|observe:circuit_breaker:failed|",
                    "admit:circuit_breaker|admit:rate_limiter|",
                    "commit:circuit_breaker|commit:rate_limiter|",
                    "observe:rate_limiter:delivered|observe:circuit_breaker:delivered"
                )
            };
            assert_eq!(trace.lock().unwrap().join("|"), expected);

            let limiter_snapshotter = control
                .rate_limiter_snapshotter(&stage_id)
                .expect("real limiter registers a metrics snapshotter");
            let limiter_metrics = limiter_snapshotter();
            assert_eq!(limiter_metrics.events_total, 3);
            assert_eq!(limiter_metrics.tokens_consumed_total, 3.0);

            let breaker_snapshotter = control
                .circuit_breaker_snapshotter(&stage_id)
                .expect("real breaker registers a metrics snapshotter");
            let breaker_metrics = breaker_snapshotter();
            assert_eq!(breaker_metrics.requests_total, 3);
            assert_eq!(breaker_metrics.failures_total, 2);
            assert_eq!(breaker_metrics.successes_total, 1);
        }
    }

    #[tokio::test]
    async fn real_open_breaker_rejection_refunds_earlier_limiter_reservation() {
        let stage_id = StageId::new();
        let config = sink_stage_config(stage_id, "refund_sink");
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let breaker_factory = CircuitBreakerBuilder::new(1)
            .with_retry_fixed(Duration::ZERO, 2)
            .build();
        let breaker = materialize_sink_policy(breaker_factory.as_ref(), &config, &control, 1);

        // One real physical failure opens the real breaker. Settlement then
        // suppresses recovery, so the scripted executor needs no second item.
        let warm_calls = Arc::new(AtomicUsize::new(0));
        let mut warm_executor = SequenceSinkExecutor {
            parent_event_id: EventId::new(),
            outcomes: VecDeque::from([SinkDeliveryAttemptOutcome::Delivered(Err(
                HandlerError::Remote("open the breaker".to_string()),
            ))]),
            calls: warm_calls.clone(),
        };
        let warm_boundary = PerSinkDeliveryPolicyBoundary::new(vec![breaker.clone()]);
        let warm_report = warm_boundary
            .around_retryable_sink_delivery(&mut warm_executor, BoundaryStopReceiver::default())
            .await;
        assert!(matches!(
            warm_report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Err(
                HandlerError::Remote(_)
            )))
        ));
        assert_eq!(warm_calls.load(Ordering::SeqCst), 1);

        let limiter_factory = RateLimiterFactory::new(100_000.0).with_burst(1.0);
        let limiter = materialize_sink_policy(&limiter_factory, &config, &control, 0);
        let trace = Arc::new(Mutex::new(Vec::new()));
        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![
            traced_sink_policy(limiter, &trace),
            traced_sink_policy(breaker, &trace),
        ]);
        let mut executor = PanicExecutor;

        let report = boundary
            .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Rejected(ref rejection)
                if rejection.policy == "circuit_breaker"
                    && rejection.reason == "circuit breaker open"
        ));
        assert_eq!(
            trace.lock().unwrap().join("|"),
            concat!(
                "admit:rate_limiter|admit:circuit_breaker|",
                "observe:rate_limiter:not_executed"
            )
        );

        let limiter_snapshotter = control
            .rate_limiter_snapshotter(&stage_id)
            .expect("real limiter registers a metrics snapshotter");
        let limiter_metrics = limiter_snapshotter();
        assert_eq!(limiter_metrics.events_total, 0);
        assert_eq!(limiter_metrics.tokens_consumed_total, 0.0);
        assert_eq!(limiter_metrics.bucket_tokens, 1.0);

        let breaker_snapshotter = control
            .circuit_breaker_snapshotter(&stage_id)
            .expect("real breaker registers a metrics snapshotter");
        let breaker_metrics = breaker_snapshotter();
        assert_eq!(breaker_metrics.requests_total, 1);
        assert_eq!(breaker_metrics.failures_total, 1);
        assert!(breaker_metrics.state.is_open());
    }

    #[tokio::test]
    async fn third_party_factory_uses_the_sink_carrier_and_rejects() {
        // FLOWIP-115b: the carrier is a general abstraction, not breaker-special.
        // A non-breaker factory (control_role None) routes through the same
        // `materialize` path purely by declaring a control surface, and its
        // policy composes in the same onion the breaker uses, short-circuiting
        // delivery on rejection.
        let factory = ThirdPartySinkFactory;
        assert!(factory.declaration().is_control());
        assert!(factory
            .declaration()
            .supports(MiddlewareSurfaceKind::SinkDelivery));
        assert_eq!(factory.control_role(), ControlMiddlewareRole::None);

        let config = StageConfig {
            stage_id: StageId::new(),
            name: "third_party".to_string(),
            flow_name: "test".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            resolved_policies: Default::default(),
        };
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let surface = MiddlewareSurface::SinkDelivery(crate::middleware::SinkDeliverySurface {
            stage_id: config.stage_id,
            configured_target: None,
        });
        let unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::SinkDelivery(crate::middleware::SinkDeliveryUnitId {
                target: crate::middleware::SinkDeliveryTarget::Stage,
            }),
        };
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            origin: &origin,
            declaration_index: crate::middleware::MiddlewareDeclarationIndex::resolved(0),
        };
        let materialization = MiddlewareMaterializationContext {
            config: &config,
            control_middleware: &control,
            stage_type: obzenflow_core::event::context::StageType::Sink,
        };
        let attachment = factory
            .materialize(request, &materialization)
            .expect("third-party factory should materialize a sink policy");
        let policy = match attachment {
            MiddlewareSurfaceAttachment::SinkDelivery(policy) => policy,
            _ => panic!("expected a SinkDelivery attachment from the third-party factory"),
        };

        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![policy]);
        let mut executor = PanicExecutor;
        let report = boundary.around_sink_delivery(&mut executor).await;

        match report.outcome {
            SinkDeliveryBoundaryOutcome::Rejected(rejection) => {
                assert_eq!(rejection.policy, "third_party_reject");
                assert_eq!(rejection.reason, "third party policy");
            }
            _ => panic!("expected the third-party policy to reject delivery"),
        }
    }

    #[tokio::test]
    async fn retryable_sink_typed_remote_error_then_success_returns_one_terminal_delivery() {
        let owner = sink_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        let admissions = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = retry_sink_boundary(owner.clone(), admissions.clone(), log.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let parent_event_id = EventId::from(Ulid::from(0x115_1003_u128));
        let mut executor = SequenceSinkExecutor {
            parent_event_id,
            outcomes: VecDeque::from([
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                    "temporary".to_string(),
                ))),
                SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                    DeliveryPayload::success(DeliveryMethod::Noop, None),
                )))),
            ]),
            calls: calls.clone(),
        };

        let report = boundary
            .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "admit:1",
                "commit",
                "observe:failed",
                "admit:2",
                "commit",
                "observe:delivered",
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
                assert_eq!(*error_kind, Some(ErrorKind::Remote));
                assert_eq!(*delay_ms, Some(0));
                assert_eq!(context.stage_id, owner.stage_id);
                assert_eq!(context.attachment_id, owner.attachment_id.as_ulid());
                assert_eq!(
                    context.protected_unit,
                    RetryProtectedUnit::SinkDelivery {
                        configured_target_id: None
                    }
                );
                assert_eq!(
                    context.invocation,
                    RetryInvocation::SinkDelivery { parent_event_id }
                );
                assert_eq!(
                    report.control_events[0].causality.parent_ids,
                    vec![parent_event_id]
                );
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
    async fn threshold_one_breaker_keeps_rate_limited_sink_retryable_by_default() {
        let owner = sink_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
        let admissions = Arc::new(AtomicUsize::new(0));
        let breaker: Arc<dyn SinkPolicy> = Arc::new(CircuitBreakerSinkPolicy {
            breaker: Arc::new(CircuitBreakerMiddleware::new(1)),
        });
        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![
            breaker,
            Arc::new(SinkRetryOwnerPolicy {
                owner,
                admissions: admissions.clone(),
                log: Arc::new(Mutex::new(Vec::new())),
                guard_drops: None,
                observed: None,
            }),
        ]);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut executor = SequenceSinkExecutor {
            parent_event_id: EventId::new(),
            outcomes: VecDeque::from([
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::RateLimited {
                    message: "slow down".to_string(),
                    retry_after: None,
                })),
                SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                    DeliveryPayload::success(DeliveryMethod::Noop, None),
                )))),
            ]),
            calls: calls.clone(),
        };

        let report = boundary
            .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
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
    async fn half_open_rate_limited_sink_probe_is_single_shot() {
        let owner = sink_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
        let admissions = Arc::new(AtomicUsize::new(0));
        let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
        breaker.force_half_open_for_test();
        let breaker_policy: Arc<dyn SinkPolicy> = Arc::new(CircuitBreakerSinkPolicy {
            breaker: breaker.clone(),
        });
        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![
            breaker_policy,
            Arc::new(SinkRetryOwnerPolicy {
                owner,
                admissions: admissions.clone(),
                log: Arc::new(Mutex::new(Vec::new())),
                guard_drops: None,
                observed: None,
            }),
        ]);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut executor = SequenceSinkExecutor {
            parent_event_id: EventId::new(),
            outcomes: VecDeque::from([
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::RateLimited {
                    message: "slow down".to_string(),
                    retry_after: None,
                })),
                SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                    DeliveryPayload::success(DeliveryMethod::Noop, None),
                )))),
            ]),
            calls: calls.clone(),
        };

        let report = boundary
            .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
            .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Err(
                HandlerError::RateLimited { .. }
            )))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
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
    async fn sink_returned_buffered_failed_and_partial_reports_are_terminal_not_retry_signals() {
        let cases = [
            (
                "buffered",
                DeliveryPayload::buffered(DeliveryMethod::Noop, Some(1)),
            ),
            (
                "failed",
                DeliveryPayload::failed(
                    DeliveryMethod::Noop,
                    "destination",
                    "typed terminal failure",
                    true,
                ),
            ),
            (
                "partial",
                DeliveryPayload::partial(
                    DeliveryMethod::Noop,
                    2,
                    1,
                    "one item failed",
                    Some(vec!["item-3".to_string()]),
                ),
            ),
        ];

        for (case, payload) in cases {
            let boundary = retry_sink_boundary(
                sink_retry_owner(3, Duration::ZERO, Duration::from_secs(1)),
                Arc::new(AtomicUsize::new(0)),
                Arc::new(Mutex::new(Vec::new())),
            );
            let calls = Arc::new(AtomicUsize::new(0));
            let mut executor = SequenceSinkExecutor {
                parent_event_id: EventId::new(),
                outcomes: VecDeque::from([SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(
                    SinkConsumeReport::new(payload),
                )))]),
                calls: calls.clone(),
            };

            let report = boundary
                .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
                .await;

            assert!(
                matches!(
                    report.outcome,
                    SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(
                        Ok(_)
                    ))
                ),
                "{case} remains the logical terminal delivery report"
            );
            assert_eq!(
                calls.load(Ordering::SeqCst),
                1,
                "{case} is not an executor error and must never be retried"
            );
            assert!(
                report.control_events.is_empty(),
                "{case} emits no retry lifecycle rows"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn active_sink_deadline_is_outcome_unknown_after_one_charged_attempt() {
        let mut owner = sink_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        owner.policy.max_total_wall_time = Duration::from_millis(50);
        let admissions = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = retry_sink_boundary(owner, admissions.clone(), log.clone());
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let calls_for_task = calls.clone();
        let started_for_task = started.clone();
        let dropped_for_task = dropped.clone();

        let task = tokio::spawn(async move {
            let mut executor = PendingSinkExecutor {
                parent_event_id: EventId::new(),
                calls: calls_for_task,
                started: started_for_task,
                dropped: dropped_for_task,
            };
            boundary
                .around_retryable_sink_delivery(&mut executor, BoundaryStopReceiver::default())
                .await
        });

        started.notified().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        let report = task.await.expect("sink deadline resolves active delivery");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(
            *log.lock().unwrap(),
            vec!["admit:1", "commit", "observe:failed"]
        );
        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::DeadlineOutcomeUnknown { ref message }
                if message
                    == "sink deadline expired while the external outcome remained in doubt"
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
    async fn force_abort_drops_active_sink_and_admission_guard_without_rows() {
        let admissions = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let guard_drops = Arc::new(AtomicUsize::new(0));
        let boundary = retry_sink_boundary_with_guard(
            sink_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            admissions.clone(),
            log.clone(),
            Some(guard_drops.clone()),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
        let calls_for_task = calls.clone();
        let started_for_task = started.clone();
        let dropped_for_task = dropped.clone();

        let task = tokio::spawn(async move {
            let mut executor = PendingSinkExecutor {
                parent_event_id: EventId::new(),
                calls: calls_for_task,
                started: started_for_task,
                dropped: dropped_for_task,
            };
            boundary
                .around_retryable_sink_delivery(&mut executor, receiver)
                .await
        });

        started.notified().await;
        controller.request_abort();
        let report = task
            .await
            .expect("force abort cancels active sink delivery");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(guard_drops.load(Ordering::SeqCst), 1);
        assert_eq!(*log.lock().unwrap(), vec!["admit:1", "commit"]);
        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Rejected(ref rejection)
                if rejection.policy == "retry_coordinator"
                    && rejection.reason == "force abort requested during sink delivery"
        ));
        assert!(report.control_events.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_drain_during_active_retryable_sink_starts_no_next_attempt() {
        let admissions = Arc::new(AtomicUsize::new(0));
        let boundary = retry_sink_boundary(
            sink_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            admissions.clone(),
            Arc::new(Mutex::new(Vec::new())),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
        let calls_for_task = calls.clone();
        let started_for_task = started.clone();
        let release_for_task = release.clone();

        let task = tokio::spawn(async move {
            let mut executor = ControlledSinkExecutor {
                parent_event_id: EventId::new(),
                outcome: Some(SinkDeliveryAttemptOutcome::Delivered(Err(
                    HandlerError::Remote("retryable".to_string()),
                ))),
                calls: calls_for_task,
                started: started_for_task,
                release: release_for_task,
            };
            boundary
                .around_retryable_sink_delivery(&mut executor, receiver)
                .await
        });

        started.notified().await;
        controller.request_drain();
        release.notify_one();
        let report = task
            .await
            .expect("active sink delivery settles during drain");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(ref message)))
            ) if message == "retryable"
        ));
        assert_eq!(report.control_events.len(), 1);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
                last_error_kind: Some(ErrorKind::Remote),
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_drain_during_sink_backoff_records_drain_and_starts_no_next_attempt() {
        let observed = Arc::new(Notify::new());
        let admissions = Arc::new(AtomicUsize::new(0));
        let boundary = retry_sink_boundary_with_observer(
            sink_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            admissions.clone(),
            Arc::new(Mutex::new(Vec::new())),
            observed.clone(),
        );
        let calls = Arc::new(AtomicUsize::new(0));
        let calls_for_task = calls.clone();
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

        let task = tokio::spawn(async move {
            let mut executor = SequenceSinkExecutor {
                parent_event_id: EventId::new(),
                outcomes: VecDeque::from([
                    SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(
                        "retryable".to_string(),
                    ))),
                    SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                        DeliveryPayload::success(DeliveryMethod::Noop, None),
                    )))),
                ]),
                calls: calls_for_task,
            };
            boundary
                .around_retryable_sink_delivery(&mut executor, receiver)
                .await
        });

        observed.notified().await;
        controller.request_drain();
        let report = task.await.expect("drain interrupts sink backoff");

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(ref message)))
            ) if message == "retryable"
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
                last_error_kind: Some(ErrorKind::Remote),
                ..
            }
        ));
    }
}
