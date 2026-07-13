// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Per-effect policy composition at the effect boundary (FLOWIP-120c).
//!
//! One policy instance guards one protected dependency, which at the effect
//! boundary means one declared effect. Policies compose as wrappers around
//! the effect execution future: admission runs in declaration order and may
//! await (a rate limiter awaits its permit instead of blocking the worker),
//! and every policy that admitted observes how the attempt ended on the way
//! out, whichever arm ended it, so lifecycle finalization is structural
//! rather than a hook each middleware must remember (gap G8).

use super::retry::{
    attempt_failed_event, await_active_execution, await_before_execution, exhausted_event,
    succeeded_event, AttemptDisposition, RetryBudget, RetryWaitError,
};
use crate::middleware::{
    BoundaryRetryOwner, Middleware, MiddlewareAbortCause, MiddlewareAction, MiddlewareContext,
};
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    RetryExhaustionCause, RetryInvocation, RetryLifecycleContext, RetryProtectedUnit,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    EffectFailureCause, EffectFailureCode, EffectFailureSource, RetryDisposition,
};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectError,
    EffectExecution, EffectExecutor, EffectIdentity,
};
use obzenflow_runtime::stages::common::BoundaryStopReceiver;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::Arc;

/// Admission decision from one per-effect policy.
pub enum PolicyAdmission {
    /// Let the effect execute (or hand off to the next policy).
    Admit,
    /// Short-circuit execution with synthesized fallback results, recorded
    /// as a `MiddlewareSynthesized` outcome group under the effect cursor.
    Synthesize {
        results: Vec<ChainEvent>,
        cause: Option<MiddlewareAbortCause>,
    },
    /// Reject execution outright, recorded as a `Failed` outcome under the
    /// effect cursor so strict replay reproduces the rejection.
    Reject(MiddlewareAbortCause),
}

/// How a guarded attempt ended, shown to every policy that admitted it.
pub enum EffectAttemptOutcome<'a> {
    /// The effect executed; on success the events are observation-grade
    /// derived copies of the authored facts.
    Executed(&'a Result<Vec<ChainEvent>, EffectError>),
    /// A later policy synthesized fallback results; the protected call never
    /// went out.
    SkippedBy(&'a str),
    /// A later policy rejected execution; the protected call never went out.
    RejectedBy(&'a MiddlewareAbortCause),
}

/// A resilience policy guarding one declared effect (FLOWIP-120c).
#[async_trait]
pub trait EffectPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    /// Admission for one effect invocation. May await; must not block the
    /// worker thread (the FLOWIP-114o boundary slice).
    async fn admit(&self, ctx: &mut MiddlewareContext) -> PolicyAdmission;

    /// Commit any execution-based reservations immediately before the
    /// protected executor starts. Default policies reserve no such resource.
    fn commit_execution(&self, _ctx: &mut MiddlewareContext) {}

    /// Observation of how the attempt ended. Runs for every policy that
    /// admitted, regardless of which arm ended the attempt.
    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, ctx: &mut MiddlewareContext);

    #[doc(hidden)]
    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        None
    }

    #[doc(hidden)]
    fn recovery_allowed_after_settlement(&self, _ctx: &MiddlewareContext) -> bool {
        true
    }
}

/// Event-aware effect policy for middleware that genuinely needs parent-event
/// access to classify, synthesize, or derive output facts.
#[async_trait]
pub trait EventAwareEffectPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> PolicyAdmission;

    /// Commit any execution-based reservations immediately before the
    /// protected executor starts. Default policies reserve no such resource.
    fn commit_execution(&self, _ctx: &mut MiddlewareContext) {}

    fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    );

    #[doc(hidden)]
    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        None
    }

    #[doc(hidden)]
    fn recovery_allowed_after_settlement(&self, _ctx: &MiddlewareContext) -> bool {
        true
    }
}

#[derive(Clone)]
pub enum EffectPolicyAttachment {
    Neutral(Arc<dyn EffectPolicy>),
    EventAware(Arc<dyn EventAwareEffectPolicy>),
}

impl EffectPolicyAttachment {
    pub fn neutral(policy: Arc<dyn EffectPolicy>) -> Self {
        Self::Neutral(policy)
    }

    pub fn event_aware(policy: Arc<dyn EventAwareEffectPolicy>) -> Self {
        Self::EventAware(policy)
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Neutral(policy) => policy.label(),
            Self::EventAware(policy) => policy.label(),
        }
    }

    async fn admit(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> PolicyAdmission {
        match self {
            Self::Neutral(policy) => policy.admit(ctx).await,
            Self::EventAware(policy) => policy.admit(event, ctx).await,
        }
    }

    fn commit_execution(&self, ctx: &mut MiddlewareContext) {
        match self {
            Self::Neutral(policy) => policy.commit_execution(ctx),
            Self::EventAware(policy) => policy.commit_execution(ctx),
        }
    }

    fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match self {
            Self::Neutral(policy) => policy.observe(attempt, ctx),
            Self::EventAware(policy) => policy.observe(event, attempt, ctx),
        }
    }

    fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
        match self {
            Self::Neutral(policy) => policy.retry_owner(),
            Self::EventAware(policy) => policy.retry_owner(),
        }
    }

    fn recovery_allowed_after_settlement(&self, ctx: &MiddlewareContext) -> bool {
        match self {
            Self::Neutral(policy) => policy.recovery_allowed_after_settlement(ctx),
            Self::EventAware(policy) => policy.recovery_allowed_after_settlement(ctx),
        }
    }
}

/// Effect boundary backed by per-effect policy chains, keyed by the declared
/// effect type. Effects with no declared policies execute unguarded.
pub struct PerEffectPolicyBoundary {
    chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>>,
}

impl PerEffectPolicyBoundary {
    pub fn new(chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>>) -> Self {
        Self { chains }
    }

    fn retry_owner(&self, effect_type: &'static str) -> Option<BoundaryRetryOwner> {
        let chain = self.chains.get(effect_type)?;
        let mut owners = chain.iter().filter_map(EffectPolicyAttachment::retry_owner);
        let owner = owners.next()?;
        debug_assert!(
            owners.next().is_none(),
            "binder must reject multiple retry owners"
        );
        Some(owner)
    }
}

/// Adapt a chain middleware instance into a per-effect policy.
///
/// The bridge is event-aware because the generic `Middleware` trait is
/// event-shaped.
pub fn effect_policy_from_middleware(instance: Arc<dyn Middleware>) -> EffectPolicyAttachment {
    EffectPolicyAttachment::event_aware(Arc::new(ChainSurfacePolicy { inner: instance }))
}

/// Generic per-effect adapter over the chain middleware surface.
struct ChainSurfacePolicy {
    inner: Arc<dyn Middleware>,
}

#[async_trait]
impl EventAwareEffectPolicy for ChainSurfacePolicy {
    fn label(&self) -> &'static str {
        self.inner.label()
    }

    async fn admit(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> PolicyAdmission {
        match self.inner.pre_handle(event, ctx) {
            MiddlewareAction::Continue => PolicyAdmission::Admit,
            MiddlewareAction::Skip { results, cause } => {
                PolicyAdmission::Synthesize { results, cause }
            }
            MiddlewareAction::Abort { cause } => {
                PolicyAdmission::Reject(cause.unwrap_or_else(|| MiddlewareAbortCause {
                    source: obzenflow_core::event::EffectFailureSource::new(self.inner.label()),
                    code: obzenflow_core::event::EffectFailureCode::new("aborted"),
                    message: format!(
                        "middleware '{}' aborted effect execution",
                        self.inner.label()
                    ),
                    retry: obzenflow_core::event::RetryDisposition::NotRetryable,
                    event: None,
                }))
            }
        }
    }

    fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match attempt {
            EffectAttemptOutcome::Executed(Ok(outputs)) => {
                self.inner.post_handle(event, outputs, ctx);
            }
            EffectAttemptOutcome::Executed(Err(err)) => {
                let error_event = event.clone().mark_as_error(
                    err.to_string(),
                    obzenflow_core::event::status::processing_status::ErrorKind::Remote,
                );
                self.inner
                    .post_handle(event, std::slice::from_ref(&error_event), ctx);
            }
            EffectAttemptOutcome::SkippedBy(_) | EffectAttemptOutcome::RejectedBy(_) => {
                // The protected call never went out; nothing to observe.
            }
        }
    }
}

fn abort_reason_from_cause(cause: MiddlewareAbortCause) -> EffectAbortReason {
    EffectAbortReason {
        cause: EffectFailureCause {
            source: cause.source,
            code: cause.code,
        },
        message: cause.message,
        retry: cause.retry,
    }
}

#[async_trait]
impl EffectBoundary for PerEffectPolicyBoundary {
    async fn around_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        execute: EffectExecution,
    ) -> EffectBoundaryReport {
        let Some(chain) = self.chains.get(identity.effect_type) else {
            return EffectBoundaryReport {
                outcome: EffectBoundaryOutcome::Executed(execute.await),
                control_events: Vec::new(),
            };
        };

        // The boundary is reached only when the effect executes live, so the
        // scope is structural (FLOWIP-120c): no per-policy replay check.
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

        for policy in chain.iter() {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted.push(policy),
                PolicyAdmission::Synthesize { results, cause: _ } => {
                    let attempt = EffectAttemptOutcome::SkippedBy(policy.label());
                    for prior in admitted.iter().rev() {
                        prior.observe(event, &attempt, &mut ctx);
                    }
                    let control_events = ctx.take_control_events();
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Skipped {
                            results,
                            source: Some(policy.label().to_string()),
                        },
                        control_events,
                    };
                }
                PolicyAdmission::Reject(cause) => {
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    for prior in admitted.iter().rev() {
                        prior.observe(event, &attempt, &mut ctx);
                    }
                    let control_events = ctx.take_control_events();
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                        control_events,
                    };
                }
            }
        }

        let call_started = tokio::time::Instant::now();
        for policy in &admitted {
            policy.commit_execution(&mut ctx);
        }
        let result = execute.await;
        // FLOWIP-115f: record the protected call's wall-clock duration on the
        // boundary context so a slow-call policy (circuit breaker) reads it here,
        // rather than from a per-event `processing_time` field that is now stamped
        // at commit time, after this observe pass.
        ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
            call_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        );
        let attempt = EffectAttemptOutcome::Executed(&result);
        for policy in admitted.iter().rev() {
            policy.observe(event, &attempt, &mut ctx);
        }
        let control_events = ctx.take_control_events();
        EffectBoundaryReport {
            outcome: EffectBoundaryOutcome::Executed(result),
            control_events,
        }
    }

    async fn around_retryable_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        execute: &mut dyn EffectExecutor,
        mut stop: BoundaryStopReceiver,
    ) -> EffectBoundaryReport {
        let Some(owner) = self.retry_owner(identity.effect_type) else {
            return self
                .around_effect(identity, event, execute.attempt(NonZeroU32::MIN))
                .await;
        };
        let Some(budget) = RetryBudget::new(owner.policy.clone()) else {
            return EffectBoundaryReport {
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
            };
        };
        let parent_id = event.id;
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
        let Some(chain) = self.chains.get(identity.effect_type) else {
            unreachable!("retry owner implies an effect policy chain")
        };
        let mut attempt = NonZeroU32::MIN;
        let mut recovery_started = false;
        let mut invocation_events = Vec::new();

        loop {
            let mut ctx =
                MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
            let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

            for policy in chain.iter() {
                let admission = await_before_execution(
                    policy.admit(event, &mut ctx),
                    budget.deadline(),
                    &mut stop,
                )
                .await;
                match admission {
                    Ok(PolicyAdmission::Admit) => admitted.push(policy),
                    Ok(PolicyAdmission::Synthesize { results, cause: _ }) => {
                        let outcome = EffectAttemptOutcome::SkippedBy(policy.label());
                        for prior in admitted.iter().rev() {
                            prior.observe(event, &outcome, &mut ctx);
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
                                Some(parent_id),
                            ));
                        }
                        return EffectBoundaryReport {
                            outcome: EffectBoundaryOutcome::Skipped {
                                results,
                                source: Some(policy.label().to_string()),
                            },
                            control_events: invocation_events,
                        };
                    }
                    Ok(PolicyAdmission::Reject(cause)) => {
                        let outcome = EffectAttemptOutcome::RejectedBy(&cause);
                        for prior in admitted.iter().rev() {
                            prior.observe(event, &outcome, &mut ctx);
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
                                Some(parent_id),
                            ));
                        }
                        return EffectBoundaryReport {
                            outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                            control_events: invocation_events,
                        };
                    }
                    Err(wait_error) => {
                        let (code, message) = match wait_error {
                            RetryWaitError::Deadline => (
                                "retry_total_wall_time",
                                "retry total-wall deadline expired during effect policy admission",
                            ),
                            RetryWaitError::Drain => (
                                "drain_requested",
                                "graceful drain requested during effect policy admission",
                            ),
                            RetryWaitError::Abort => (
                                "force_aborted",
                                "force abort requested during effect policy admission",
                            ),
                        };
                        let not_executed = MiddlewareAbortCause {
                            source: EffectFailureSource::new("effect_boundary"),
                            code: EffectFailureCode::new(code),
                            message: message.to_string(),
                            retry: RetryDisposition::NotRetryable,
                            event: None,
                        };
                        let outcome = EffectAttemptOutcome::RejectedBy(&not_executed);
                        for prior in admitted.iter().rev() {
                            prior.observe(event, &outcome, &mut ctx);
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
                                Some(parent_id),
                            ));
                        }
                        return EffectBoundaryReport {
                            outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(
                                not_executed,
                            )),
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
                let (code, message) = match barrier {
                    RetryWaitError::Deadline => (
                        "retry_total_wall_time",
                        "retry total-wall deadline expired before effect executor start",
                    ),
                    RetryWaitError::Drain => (
                        "drain_requested",
                        "graceful drain requested before effect executor start",
                    ),
                    RetryWaitError::Abort => (
                        "force_aborted",
                        "force abort requested before effect executor start",
                    ),
                };
                let not_executed = MiddlewareAbortCause {
                    source: EffectFailureSource::new("effect_boundary"),
                    code: EffectFailureCode::new(code),
                    message: message.to_string(),
                    retry: RetryDisposition::NotRetryable,
                    event: None,
                };
                let outcome = EffectAttemptOutcome::RejectedBy(&not_executed);
                for prior in admitted.iter().rev() {
                    prior.observe(event, &outcome, &mut ctx);
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
                        Some(parent_id),
                    ));
                }
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(not_executed)),
                    control_events: if matches!(barrier, RetryWaitError::Abort) {
                        Vec::new()
                    } else {
                        invocation_events
                    },
                };
            }

            let mut call_started = None;
            let active = await_active_execution(
                || {
                    for policy in &admitted {
                        policy.commit_execution(&mut ctx);
                    }
                    call_started = Some(tokio::time::Instant::now());
                    execute.attempt(attempt)
                },
                budget.deadline(),
                &mut stop,
            )
            .await;
            let (result, drain_requested, active_deadline) = match active {
                Ok((result, drain_requested)) => (result, drain_requested, false),
                Err(RetryWaitError::Deadline) => (
                    Err(EffectError::BoundaryRejected {
                        rejected_by: EffectFailureSource::new("effect_boundary"),
                        code: EffectFailureCode::new("deadline_outcome_unknown"),
                        message:
                            "effect deadline expired while the external outcome remained in doubt"
                                .to_string(),
                        retry: RetryDisposition::NotRetryable,
                    }),
                    false,
                    true,
                ),
                Err(RetryWaitError::Drain) => {
                    let not_executed = MiddlewareAbortCause {
                        source: EffectFailureSource::new("effect_boundary"),
                        code: EffectFailureCode::new("drain_requested"),
                        message: "graceful drain requested before effect executor start"
                            .to_string(),
                        retry: RetryDisposition::NotRetryable,
                        event: None,
                    };
                    let outcome = EffectAttemptOutcome::RejectedBy(&not_executed);
                    for prior in admitted.iter().rev() {
                        prior.observe(event, &outcome, &mut ctx);
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
                            Some(parent_id),
                        ));
                    }
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(
                            not_executed,
                        )),
                        control_events: invocation_events,
                    };
                }
                Err(RetryWaitError::Abort) => {
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(EffectAbortReason {
                            cause: EffectFailureCause {
                                source: EffectFailureSource::new("effect_boundary"),
                                code: EffectFailureCode::new("force_aborted"),
                            },
                            message: "force abort requested during effect execution".to_string(),
                            retry: RetryDisposition::NotRetryable,
                        }),
                        control_events: Vec::new(),
                    };
                }
            };
            ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
                call_started
                    .expect("successful active execution must record its start")
                    .elapsed()
                    .as_nanos()
                    .min(u64::MAX as u128) as u64,
            );
            let policy_outcome = EffectAttemptOutcome::Executed(&result);
            for policy in admitted.iter().rev() {
                policy.observe(event, &policy_outcome, &mut ctx);
            }
            invocation_events.extend(ctx.take_control_events());
            let recovery_allowed = chain
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
                    Some(parent_id),
                ));
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(EffectAbortReason {
                        cause: EffectFailureCause {
                            source: EffectFailureSource::new("effect_boundary"),
                            code: EffectFailureCode::new("deadline_outcome_unknown"),
                        },
                        message:
                            "effect deadline expired while the external outcome remained in doubt"
                                .to_string(),
                        retry: RetryDisposition::NotRetryable,
                    }),
                    control_events: invocation_events,
                };
            }

            match effect_attempt_disposition(&result) {
                AttemptDisposition::Completed => {
                    if recovery_started {
                        invocation_events.push(succeeded_event(
                            &owner,
                            &lifecycle,
                            attempt.get(),
                            &budget,
                            Some(parent_id),
                        ));
                    }
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Executed(result),
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
                            Some(parent_id),
                        ));
                    }
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Executed(result),
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
                            Some(parent_id),
                        ));
                        return EffectBoundaryReport {
                            outcome: EffectBoundaryOutcome::Executed(result),
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
                                Some(parent_id),
                            ));
                            return EffectBoundaryReport {
                                outcome: EffectBoundaryOutcome::Executed(result),
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
                        Some(parent_id),
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
                                Some(parent_id),
                            ));
                            return EffectBoundaryReport {
                                outcome: EffectBoundaryOutcome::Executed(result),
                                control_events: invocation_events,
                            };
                        }
                        Err(RetryWaitError::Abort) => {
                            return EffectBoundaryReport {
                                outcome: EffectBoundaryOutcome::Executed(result),
                                control_events: Vec::new(),
                            };
                        }
                    }
                }
                AttemptDisposition::NotExecuted => unreachable!(),
            }
        }
    }

    fn retry_enabled(&self, identity: &EffectIdentity) -> bool {
        self.retry_owner(identity.effect_type).is_some()
    }
}

fn effect_attempt_disposition(result: &Result<Vec<ChainEvent>, EffectError>) -> AttemptDisposition {
    match result {
        Ok(_) => AttemptDisposition::Completed,
        Err(EffectError::TransientExecution(_)) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::Remote,
            retry_after: None,
        },
        Err(EffectError::RateLimited { retry_after, .. }) => AttemptDisposition::RetryableFailure {
            kind: ErrorKind::RateLimited,
            retry_after: *retry_after,
        },
        Err(EffectError::Serialization(_)) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Deserialization,
        },
        Err(
            EffectError::Execution(_)
            | EffectError::Journal(_)
            | EffectError::MissingRecordedEffect { .. }
            | EffectError::DuplicateRecordedEffect { .. }
            | EffectError::DescriptorMismatch { .. }
            | EffectError::RecordedFailure { .. }
            | EffectError::BoundaryRejected { .. }
            | EffectError::TypedOutcomeCoordination { .. }
            | EffectError::EffectProvenanceMismatch(_)
            | EffectError::IncompleteOutcomeGroup { .. }
            | EffectError::MissingIdempotencyKey { .. }
            | EffectError::UndeclaredEffect { .. }
            | EffectError::UndeclaredOutput { .. }
            | EffectError::EmitUnsupported { .. }
            | EffectError::MissingEffectPort { .. }
            | EffectError::TransactionalCommitMissing { .. }
            | EffectError::ReplayArchive(_),
        ) => AttemptDisposition::TerminalFailure {
            kind: ErrorKind::Unknown,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::circuit_breaker::CircuitBreakerMiddleware;
    use crate::middleware::{
        BoundaryRetryPolicy, EffectSurface, EffectTypeKey, EffectUnitId, MiddlewareAttachmentId,
        MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareDeclarationIndex,
        MiddlewareOrigin, MiddlewareSurface, MiddlewareSurfaceKind, ProtectedUnit, ProtectedUnitId,
    };
    use obzenflow_core::event::payloads::observability_payload::{
        MiddlewareLifecycle, ObservabilityPayload, RetryEvent,
    };
    use obzenflow_core::event::{
        ChainEventContent, ChainEventFactory, EffectFailureCode, EffectFailureSource,
        RetryDisposition,
    };
    use obzenflow_core::{MiddlewareContextKey, StageId, Ulid, WriterId};
    use obzenflow_runtime::effects::{EffectCursor, EffectDescriptorHash, EffectSafety};
    use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
    use serde_json::json;
    use std::collections::VecDeque;
    use std::num::NonZeroU32;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::{Barrier, Notify};

    fn identity_for(effect_type: &'static str) -> EffectIdentity {
        EffectIdentity {
            effect_type,
            safety: EffectSafety::Idempotent,
            cursor: EffectCursor::new("test_flow", "test_stage", 1, 0),
            idempotency_key: None,
        }
    }

    fn data_event() -> ChainEvent {
        ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.input", json!({}))
    }

    fn effect_retry_owner(
        max_attempts: u32,
        delay: Duration,
        max_single_delay: Duration,
    ) -> BoundaryRetryOwner {
        let stage_id = StageId::from_ulid(Ulid::from(0x115_0002_u128));
        let effect_type = EffectTypeKey::from("effect.retry_test");
        let declaration = MiddlewareDeclaration::control(
            "test.effect_retry_owner",
            vec![MiddlewareSurfaceKind::Effect],
        );
        let surface = MiddlewareSurface::Effect(EffectSurface {
            stage_id,
            effect_type: effect_type.clone(),
        });
        let protected_unit = ProtectedUnitId {
            stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId { effect_type }),
        };
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::effect_policy(0),
        };
        BoundaryRetryOwner {
            attachment_id: MiddlewareAttachmentId::from_declaration_and_request(
                &declaration,
                &request,
            ),
            stage_id,
            writer_id: WriterId::from(stage_id),
            protected_unit_label: "effect.retry_test".to_string(),
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
    fn effect_error_disposition_matrix_is_exhaustive_and_preserves_retry_hints() {
        let cursor = EffectCursor::new("test_flow", "test_stage", 1, 0);
        let retry_after = Duration::from_millis(725);
        let cases = [
            (
                EffectError::TransientExecution("transient".to_string()),
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::Remote,
                    retry_after: None,
                },
            ),
            (
                EffectError::RateLimited {
                    message: "rate limited with hint".to_string(),
                    retry_after: Some(retry_after),
                },
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::RateLimited,
                    retry_after: Some(retry_after),
                },
            ),
            (
                EffectError::RateLimited {
                    message: "rate limited without hint".to_string(),
                    retry_after: None,
                },
                AttemptDisposition::RetryableFailure {
                    kind: ErrorKind::RateLimited,
                    retry_after: None,
                },
            ),
            (
                EffectError::Serialization("serialization".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Deserialization,
                },
            ),
            (
                EffectError::Execution("execution".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::Journal("journal".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::MissingRecordedEffect {
                    cursor: cursor.clone(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::DuplicateRecordedEffect {
                    cursor: cursor.clone(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::DescriptorMismatch {
                    cursor: cursor.clone(),
                    expected: EffectDescriptorHash::new("expected"),
                    recorded: EffectDescriptorHash::new("recorded"),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::RecordedFailure {
                    error_type: "recorded".into(),
                    error_message: "recorded failure".to_string(),
                    retry: RetryDisposition::Retryable,
                    cause: None,
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::BoundaryRejected {
                    rejected_by: EffectFailureSource::new("test"),
                    code: EffectFailureCode::new("rejected"),
                    message: "boundary rejected".to_string(),
                    retry: RetryDisposition::Retryable,
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::TypedOutcomeCoordination {
                    stage_key: "test_stage".to_string(),
                    effect_type: "effect.retry_test".to_string(),
                    message: "coordination".to_string(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::EffectProvenanceMismatch("provenance".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::IncompleteOutcomeGroup {
                    cursor: cursor.clone(),
                    expected: 2,
                    present: 1,
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::MissingIdempotencyKey {
                    effect_type: "effect.retry_test".to_string(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::UndeclaredEffect {
                    stage_key: "test_stage".to_string(),
                    effect_type: "effect.retry_test".to_string(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::UndeclaredOutput {
                    stage_key: "test_stage".to_string(),
                    event_type: "test.output".to_string(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::EmitUnsupported {
                    stage_key: "test_stage".to_string(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::MissingEffectPort {
                    type_name: "TestPort",
                    name: "test".to_string(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::TransactionalCommitMissing {
                    effect_type: "effect.retry_test".to_string(),
                    executor: "test_executor".to_string(),
                },
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
            (
                EffectError::ReplayArchive("archive".to_string()),
                AttemptDisposition::TerminalFailure {
                    kind: ErrorKind::Unknown,
                },
            ),
        ];

        for (error, expected) in cases {
            assert_eq!(effect_attempt_disposition(&Err(error)), expected);
        }
        assert_eq!(
            effect_attempt_disposition(&Ok(Vec::new())),
            AttemptDisposition::Completed
        );
    }

    struct EffectRetryContextMarker;

    impl MiddlewareContextKey for EffectRetryContextMarker {
        type Value = ();
        const LABEL: &'static str = "effect_retry_test_marker";
    }

    struct EffectRetryOwnerPolicy {
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
        observed: Option<Arc<Notify>>,
    }

    #[async_trait]
    impl EffectPolicy for EffectRetryOwnerPolicy {
        fn label(&self) -> &'static str {
            "effect_retry_owner"
        }

        async fn admit(&self, ctx: &mut MiddlewareContext) -> PolicyAdmission {
            assert!(
                !ctx.contains::<EffectRetryContextMarker>(),
                "each effect attempt must receive a fresh middleware context"
            );
            ctx.insert::<EffectRetryContextMarker>(());
            let attempt = self.admissions.fetch_add(1, Ordering::SeqCst) + 1;
            self.log.lock().unwrap().push(format!("admit:{attempt}"));
            PolicyAdmission::Admit
        }

        fn commit_execution(&self, _ctx: &mut MiddlewareContext) {
            self.log.lock().unwrap().push("commit".to_string());
        }

        fn observe(&self, attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {
            let outcome = match attempt {
                EffectAttemptOutcome::Executed(Ok(_)) => "ok",
                EffectAttemptOutcome::Executed(Err(_)) => "error",
                EffectAttemptOutcome::SkippedBy(_) => "skipped",
                EffectAttemptOutcome::RejectedBy(_) => "rejected",
            };
            self.log.lock().unwrap().push(format!("observe:{outcome}"));
            if let Some(observed) = &self.observed {
                observed.notify_one();
            }
        }

        fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
            Some(self.owner.clone())
        }
    }

    struct SequenceEffectExecutor {
        outcomes: VecDeque<Result<Vec<ChainEvent>, EffectError>>,
        ordinals: Arc<Mutex<Vec<u32>>>,
    }

    struct ControlledEffectExecutor {
        outcome: Option<Result<Vec<ChainEvent>, EffectError>>,
        ordinals: Arc<Mutex<Vec<u32>>>,
        started: Arc<Notify>,
        release: Arc<Notify>,
    }

    struct EffectFutureDropCounter(Arc<AtomicUsize>);

    impl Drop for EffectFutureDropCounter {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct PendingEffectExecutor {
        ordinals: Arc<Mutex<Vec<u32>>>,
        started: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
    }

    struct LockstepEffectExecutor {
        ordinals: Arc<Mutex<Vec<u32>>>,
        attempt_barrier: Arc<Barrier>,
    }

    impl EffectExecutor for PendingEffectExecutor {
        fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
            self.ordinals.lock().unwrap().push(attempt.get());
            let started = self.started.clone();
            let dropped = self.dropped.clone();
            Box::pin(async move {
                let _drop_counter = EffectFutureDropCounter(dropped);
                started.notify_one();
                std::future::pending::<Result<Vec<ChainEvent>, EffectError>>().await
            })
        }
    }

    impl EffectExecutor for LockstepEffectExecutor {
        fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
            self.ordinals.lock().unwrap().push(attempt.get());
            let attempt_barrier = self.attempt_barrier.clone();
            Box::pin(async move {
                // Both logical invocations must reach each physical ordinal
                // before either can finish it. A sequential implementation
                // deadlocks here, so this is an honest interleaving proof.
                attempt_barrier.wait().await;
                if attempt == NonZeroU32::MIN {
                    Err(EffectError::TransientExecution(
                        "lockstep transient".to_string(),
                    ))
                } else {
                    Ok(Vec::new())
                }
            })
        }
    }

    impl EffectExecutor for ControlledEffectExecutor {
        fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
            self.ordinals.lock().unwrap().push(attempt.get());
            let outcome = self
                .outcome
                .take()
                .expect("controlled effect executor runs exactly once");
            let started = self.started.clone();
            let release = self.release.clone();
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                outcome
            })
        }
    }

    struct DelayedAdmissionEffectPolicy {
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        started: Arc<Notify>,
        delay: Duration,
    }

    #[async_trait]
    impl EffectPolicy for DelayedAdmissionEffectPolicy {
        fn label(&self) -> &'static str {
            "delayed_admission_retry_owner"
        }

        async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
            self.admissions.fetch_add(1, Ordering::SeqCst);
            self.started.notify_one();
            tokio::time::sleep(self.delay).await;
            PolicyAdmission::Admit
        }

        fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}

        fn retry_owner(&self) -> Option<BoundaryRetryOwner> {
            Some(self.owner.clone())
        }
    }

    impl EffectExecutor for SequenceEffectExecutor {
        fn attempt(&mut self, attempt: NonZeroU32) -> EffectExecution {
            self.ordinals.lock().unwrap().push(attempt.get());
            let outcome = self
                .outcomes
                .pop_front()
                .expect("effect test executor has an outcome for every attempt");
            Box::pin(async move { outcome })
        }
    }

    fn retry_effect_boundary(
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
    ) -> PerEffectPolicyBoundary {
        retry_effect_boundary_with_observer(owner, admissions, log, None)
    }

    fn retry_effect_boundary_with_observer(
        owner: BoundaryRetryOwner,
        admissions: Arc<AtomicUsize>,
        log: Arc<Mutex<Vec<String>>>,
        observed: Option<Arc<Notify>>,
    ) -> PerEffectPolicyBoundary {
        let mut chains = HashMap::new();
        chains.insert(
            "effect.retry_test",
            Arc::new(vec![EffectPolicyAttachment::neutral(Arc::new(
                EffectRetryOwnerPolicy {
                    owner,
                    admissions,
                    log,
                    observed,
                },
            ))]),
        );
        PerEffectPolicyBoundary::new(chains)
    }

    fn ok_execute() -> EffectExecution {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn failing_execute() -> EffectExecution {
        Box::pin(async { Err(EffectError::Execution("simulated_failure".to_string())) })
    }

    struct RecordingPolicy {
        label: &'static str,
        observed: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl EffectPolicy for RecordingPolicy {
        fn label(&self) -> &'static str {
            self.label
        }

        async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
            PolicyAdmission::Admit
        }

        fn observe(&self, attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {
            let kind = match attempt {
                EffectAttemptOutcome::Executed(Ok(_)) => "executed_ok".to_string(),
                EffectAttemptOutcome::Executed(Err(_)) => "executed_err".to_string(),
                EffectAttemptOutcome::SkippedBy(label) => format!("skipped_by:{label}"),
                EffectAttemptOutcome::RejectedBy(cause) => {
                    format!("rejected_by:{}", cause.source)
                }
            };
            self.observed.lock().unwrap().push(kind);
        }
    }

    struct RejectingPolicy;

    #[async_trait]
    impl EffectPolicy for RejectingPolicy {
        fn label(&self) -> &'static str {
            "test.rejecting"
        }

        async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
            PolicyAdmission::Reject(MiddlewareAbortCause {
                source: EffectFailureSource::new("test.rejecting"),
                code: EffectFailureCode::new("rejected"),
                message: "test rejection".to_string(),
                retry: RetryDisposition::NotRetryable,
                event: None,
            })
        }

        fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
    }

    /// FLOWIP-120c gap G8: finalization is structural. A policy that
    /// admitted observes the attempt even when a later policy rejects it.
    #[tokio::test]
    async fn admitted_policies_observe_rejection_by_later_policy() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let chain: Arc<Vec<EffectPolicyAttachment>> = Arc::new(vec![
            EffectPolicyAttachment::neutral(Arc::new(RecordingPolicy {
                label: "test.recording",
                observed: observed.clone(),
            })),
            EffectPolicyAttachment::neutral(Arc::new(RejectingPolicy)),
        ]);
        let mut chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>> = HashMap::new();
        chains.insert("effect.a", chain);
        let boundary = PerEffectPolicyBoundary::new(chains);

        let report = boundary
            .around_effect(&identity_for("effect.a"), &data_event(), ok_execute())
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Aborted(ref reason)
                if reason.cause.source == "test.rejecting"
        ));
        assert_eq!(
            observed.lock().unwrap().as_slice(),
            ["rejected_by:test.rejecting"],
            "the admitted policy observes how the attempt ended"
        );
    }

    /// FLOWIP-120c gap G3: one policy instance per protected dependency.
    /// Failures of effect A open A's breaker and leave B's untouched.
    #[tokio::test]
    async fn per_effect_breakers_do_not_cross_trip() {
        let breaker_a = Arc::new(CircuitBreakerMiddleware::new(1));
        let breaker_b = Arc::new(CircuitBreakerMiddleware::new(1));
        let mut chains: HashMap<&'static str, Arc<Vec<EffectPolicyAttachment>>> = HashMap::new();
        chains.insert(
            "effect.a",
            Arc::new(vec![EffectPolicyAttachment::event_aware(breaker_a)]),
        );
        chains.insert(
            "effect.b",
            Arc::new(vec![EffectPolicyAttachment::event_aware(breaker_b)]),
        );
        let boundary = PerEffectPolicyBoundary::new(chains);

        // One failure opens effect A's breaker (threshold 1).
        let report = boundary
            .around_effect(&identity_for("effect.a"), &data_event(), failing_execute())
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(_))
        ));

        // Effect A is now rejected at admission.
        let report = boundary
            .around_effect(&identity_for("effect.a"), &data_event(), ok_execute())
            .await;
        assert!(
            matches!(report.outcome, EffectBoundaryOutcome::Aborted(_)),
            "effect A's breaker must reject after its failure"
        );

        // Effect B's breaker never saw A's failure and still admits.
        let report = boundary
            .around_effect(&identity_for("effect.b"), &data_event(), ok_execute())
            .await;
        assert!(
            matches!(report.outcome, EffectBoundaryOutcome::Executed(Ok(_))),
            "effect B must stay admitted; breakers are per effect"
        );

        // An effect with no declared policies executes unguarded.
        let report = boundary
            .around_effect(&identity_for("effect.c"), &data_event(), ok_execute())
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
    }

    #[tokio::test]
    async fn concurrent_retry_invocations_keep_cursor_context_and_attempts_isolated() {
        let owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        let admissions = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = Arc::new(retry_effect_boundary(owner, admissions.clone(), log));
        let attempt_barrier = Arc::new(Barrier::new(2));
        let ordinals_a = Arc::new(Mutex::new(Vec::new()));
        let ordinals_b = Arc::new(Mutex::new(Vec::new()));

        let run_a = {
            let boundary = boundary.clone();
            let attempt_barrier = attempt_barrier.clone();
            let ordinals = ordinals_a.clone();
            async move {
                let mut identity = identity_for("effect.retry_test");
                identity.cursor = EffectCursor::new("test_flow", "test_stage", 11, 0);
                let expected_cursor = identity.cursor.clone();
                let mut executor = LockstepEffectExecutor {
                    ordinals,
                    attempt_barrier,
                };
                let report = boundary
                    .around_retryable_effect(
                        &identity,
                        &data_event(),
                        &mut executor,
                        BoundaryStopReceiver::default(),
                    )
                    .await;
                (expected_cursor, report)
            }
        };
        let run_b = {
            let boundary = boundary.clone();
            let attempt_barrier = attempt_barrier.clone();
            let ordinals = ordinals_b.clone();
            async move {
                let mut identity = identity_for("effect.retry_test");
                identity.cursor = EffectCursor::new("test_flow", "test_stage", 22, 0);
                let expected_cursor = identity.cursor.clone();
                let mut executor = LockstepEffectExecutor {
                    ordinals,
                    attempt_barrier,
                };
                let report = boundary
                    .around_retryable_effect(
                        &identity,
                        &data_event(),
                        &mut executor,
                        BoundaryStopReceiver::default(),
                    )
                    .await;
                (expected_cursor, report)
            }
        };

        let ((cursor_a, report_a), (cursor_b, report_b)) = tokio::join!(run_a, run_b);

        assert_eq!(*ordinals_a.lock().unwrap(), vec![1, 2]);
        assert_eq!(*ordinals_b.lock().unwrap(), vec![1, 2]);
        assert_eq!(admissions.load(Ordering::SeqCst), 4);

        let mut invocation_contexts = Vec::new();
        for (expected_cursor, report) in [(cursor_a, report_a), (cursor_b, report_b)] {
            assert!(matches!(
                report.outcome,
                EffectBoundaryOutcome::Executed(Ok(_))
            ));
            assert_eq!(report.control_events.len(), 2);

            let failed_context = match retry_event(&report.control_events[0]) {
                RetryEvent::AttemptFailed {
                    context: Some(context),
                    attempt_number: 1,
                    ..
                } => context.clone(),
                other => panic!("expected first-attempt failure, got {other:?}"),
            };
            let succeeded_context = match retry_event(&report.control_events[1]) {
                RetryEvent::SucceededAfterRetry {
                    context: Some(context),
                    total_attempts: 2,
                    ..
                } => context.clone(),
                other => panic!("expected second-attempt recovery, got {other:?}"),
            };
            assert_eq!(failed_context, succeeded_context);
            assert_eq!(
                failed_context.invocation,
                RetryInvocation::Effect {
                    cursor: expected_cursor
                }
            );
            invocation_contexts.push(failed_context);
        }

        assert_ne!(invocation_contexts[0], invocation_contexts[1]);
    }

    #[tokio::test]
    async fn retryable_effect_transient_then_success_uses_fresh_context_and_terminal_rows() {
        let owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        let admissions = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = retry_effect_boundary(owner.clone(), admissions.clone(), log.clone());
        let identity = identity_for("effect.retry_test");
        let parent = data_event();
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([
                Err(EffectError::TransientExecution("temporary".to_string())),
                Ok(vec![data_event()]),
            ]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_effect(
                &identity,
                &parent,
                &mut executor,
                BoundaryStopReceiver::default(),
            )
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(ref events)) if events.len() == 1
        ));
        assert_eq!(*ordinals.lock().unwrap(), vec![1, 2]);
        assert_eq!(admissions.load(Ordering::SeqCst), 2);
        assert_eq!(
            *log.lock().unwrap(),
            vec![
                "admit:1",
                "commit",
                "observe:error",
                "admit:2",
                "commit",
                "observe:ok"
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
                    RetryProtectedUnit::Effect {
                        effect_type: "effect.retry_test".into()
                    }
                );
                assert_eq!(
                    context.invocation,
                    RetryInvocation::Effect {
                        cursor: identity.cursor.clone()
                    }
                );
                assert_eq!(
                    report.control_events[0].causality.parent_ids,
                    vec![parent.id]
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
    async fn retryable_effect_transient_then_permanent_has_exact_terminal_attempt_count() {
        let owner = effect_retry_owner(4, Duration::ZERO, Duration::from_secs(1));
        let boundary = retry_effect_boundary(
            owner,
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        );
        let identity = identity_for("effect.retry_test");
        let parent = data_event();
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([
                Err(EffectError::TransientExecution("temporary".to_string())),
                Err(EffectError::Execution("permanent".to_string())),
            ]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_effect(
                &identity,
                &parent,
                &mut executor,
                BoundaryStopReceiver::default(),
            )
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Execution(ref message)))
                if message == "permanent"
        ));
        assert_eq!(*ordinals.lock().unwrap(), vec![1, 2]);
        assert_eq!(report.control_events.len(), 2);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::AttemptFailed {
                attempt_number: 1,
                ..
            }
        ));
        match retry_event(&report.control_events[1]) {
            RetryEvent::Exhausted {
                context: Some(context),
                total_attempts,
                exhaustion_cause,
                last_error_kind,
                ..
            } => {
                assert_eq!(*total_attempts, 2);
                assert_eq!(
                    *exhaustion_cause,
                    Some(RetryExhaustionCause::TerminalFailure)
                );
                assert_eq!(*last_error_kind, Some(ErrorKind::Unknown));
                assert_eq!(
                    context.invocation,
                    RetryInvocation::Effect {
                        cursor: identity.cursor.clone()
                    }
                );
            }
            other => panic!("expected terminal_failure exhaustion, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn effect_retry_after_above_the_ceiling_exhausts_without_another_call() {
        let boundary = retry_effect_boundary(
            effect_retry_owner(3, Duration::from_millis(5), Duration::from_millis(40)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        );
        let identity = identity_for("effect.retry_test");
        let parent = data_event();
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([Err(EffectError::RateLimited {
                message: "retry later".to_string(),
                retry_after: Some(Duration::from_millis(41)),
            })]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_effect(
                &identity,
                &parent,
                &mut executor,
                BoundaryStopReceiver::default(),
            )
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

    #[tokio::test]
    async fn threshold_one_breaker_keeps_rate_limited_effect_retryable_by_default() {
        let owner = effect_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
        let admissions = Arc::new(AtomicUsize::new(0));
        let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
        let mut chains = HashMap::new();
        chains.insert(
            "effect.retry_test",
            Arc::new(vec![
                EffectPolicyAttachment::event_aware(breaker),
                EffectPolicyAttachment::neutral(Arc::new(EffectRetryOwnerPolicy {
                    owner,
                    admissions: admissions.clone(),
                    log: Arc::new(Mutex::new(Vec::new())),
                    observed: None,
                })),
            ]),
        );
        let boundary = PerEffectPolicyBoundary::new(chains);
        let identity = identity_for("effect.retry_test");
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([
                Err(EffectError::RateLimited {
                    message: "slow down".to_string(),
                    retry_after: None,
                }),
                Ok(Vec::new()),
            ]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_effect(
                &identity,
                &data_event(),
                &mut executor,
                BoundaryStopReceiver::default(),
            )
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
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
    async fn half_open_rate_limited_effect_probe_is_single_shot() {
        let owner = effect_retry_owner(2, Duration::ZERO, Duration::from_secs(1));
        let admissions = Arc::new(AtomicUsize::new(0));
        let breaker = Arc::new(CircuitBreakerMiddleware::new(1));
        breaker.force_half_open_for_test();
        let mut chains = HashMap::new();
        chains.insert(
            "effect.retry_test",
            Arc::new(vec![
                EffectPolicyAttachment::event_aware(breaker.clone()),
                EffectPolicyAttachment::neutral(Arc::new(EffectRetryOwnerPolicy {
                    owner,
                    admissions: admissions.clone(),
                    log: Arc::new(Mutex::new(Vec::new())),
                    observed: None,
                })),
            ]),
        );
        let boundary = PerEffectPolicyBoundary::new(chains);
        let identity = identity_for("effect.retry_test");
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([
                Err(EffectError::RateLimited {
                    message: "slow down".to_string(),
                    retry_after: None,
                }),
                Ok(Vec::new()),
            ]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_effect(
                &identity,
                &data_event(),
                &mut executor,
                BoundaryStopReceiver::default(),
            )
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::RateLimited { .. }))
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

    #[tokio::test(start_paused = true)]
    async fn initial_admission_ready_at_the_exact_deadline_expires_without_execution() {
        let mut owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        owner.policy.max_total_wall_time = Duration::from_millis(50);
        let admissions = Arc::new(AtomicUsize::new(0));
        let admission_started = Arc::new(Notify::new());
        let mut chains = HashMap::new();
        chains.insert(
            "effect.retry_test",
            Arc::new(vec![EffectPolicyAttachment::neutral(Arc::new(
                DelayedAdmissionEffectPolicy {
                    owner,
                    admissions: admissions.clone(),
                    started: admission_started.clone(),
                    delay: Duration::from_millis(50),
                },
            ))]),
        );
        let boundary = PerEffectPolicyBoundary::new(chains);
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();

        let task = tokio::spawn(async move {
            let mut executor = SequenceEffectExecutor {
                outcomes: VecDeque::from([Ok(Vec::new())]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_effect(
                    &identity_for("effect.retry_test"),
                    &data_event(),
                    &mut executor,
                    BoundaryStopReceiver::default(),
                )
                .await
        });

        admission_started.notified().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        let report = task.await.expect("deadline resolves the admission wait");

        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert!(ordinals.lock().unwrap().is_empty());
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Aborted(ref reason)
                if reason.cause.code == "retry_total_wall_time"
        ));
        assert_eq!(report.control_events.len(), 1);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::Exhausted {
                total_attempts: 0,
                exhaustion_cause: Some(RetryExhaustionCause::TotalWallTime),
                total_duration_ms: 50,
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn active_effect_deadline_is_outcome_unknown_after_one_charged_attempt() {
        let mut owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        owner.policy.max_total_wall_time = Duration::from_millis(50);
        let admissions = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = retry_effect_boundary(owner, admissions.clone(), log.clone());
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let ordinals_for_task = ordinals.clone();
        let started_for_task = started.clone();

        let task = tokio::spawn(async move {
            let mut executor = ControlledEffectExecutor {
                outcome: Some(Ok(Vec::new())),
                ordinals: ordinals_for_task,
                started: started_for_task,
                release,
            };
            boundary
                .around_retryable_effect(
                    &identity_for("effect.retry_test"),
                    &data_event(),
                    &mut executor,
                    BoundaryStopReceiver::default(),
                )
                .await
        });

        started.notified().await;
        tokio::time::advance(Duration::from_millis(50)).await;
        let report = task.await.expect("effect deadline resolves active call");

        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(
            *log.lock().unwrap(),
            vec!["admit:1", "commit", "observe:error"]
        );
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Aborted(ref reason)
                if reason.cause.code == "deadline_outcome_unknown"
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
    async fn force_abort_drops_active_effect_and_emits_no_buffered_rows() {
        let admissions = Arc::new(AtomicUsize::new(0));
        let log = Arc::new(Mutex::new(Vec::new()));
        let boundary = retry_effect_boundary(
            effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            admissions.clone(),
            log.clone(),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
        let ordinals_for_task = ordinals.clone();
        let started_for_task = started.clone();
        let dropped_for_task = dropped.clone();

        let task = tokio::spawn(async move {
            let mut executor = PendingEffectExecutor {
                ordinals: ordinals_for_task,
                started: started_for_task,
                dropped: dropped_for_task,
            };
            boundary
                .around_retryable_effect(
                    &identity_for("effect.retry_test"),
                    &data_event(),
                    &mut executor,
                    receiver,
                )
                .await
        });

        started.notified().await;
        controller.request_abort();
        let report = task.await.expect("force abort cancels active effect");

        assert_eq!(admissions.load(Ordering::SeqCst), 1);
        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        assert_eq!(*log.lock().unwrap(), vec!["admit:1", "commit"]);
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Aborted(ref reason)
                if reason.cause.code == "force_aborted"
        ));
        assert!(report.control_events.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_drain_during_active_retryable_effect_starts_no_next_attempt() {
        let boundary = retry_effect_boundary(
            effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let started = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();
        let ordinals_for_task = ordinals.clone();
        let started_for_task = started.clone();
        let release_for_task = release.clone();

        let task = tokio::spawn(async move {
            let mut executor = ControlledEffectExecutor {
                outcome: Some(Err(EffectError::TransientExecution(
                    "retryable".to_string(),
                ))),
                ordinals: ordinals_for_task,
                started: started_for_task,
                release: release_for_task,
            };
            boundary
                .around_retryable_effect(
                    &identity_for("effect.retry_test"),
                    &data_event(),
                    &mut executor,
                    receiver,
                )
                .await
        });

        started.notified().await;
        controller.request_drain();
        release.notify_one();
        let report = task.await.expect("active effect settles during drain");

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::TransientExecution(_)))
        ));
        assert_eq!(report.control_events.len(), 1);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn graceful_drain_during_effect_backoff_records_drain_and_starts_no_next_attempt() {
        let observed = Arc::new(Notify::new());
        let boundary = retry_effect_boundary_with_observer(
            effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
            Some(observed.clone()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

        let task = tokio::spawn(async move {
            let mut executor = SequenceEffectExecutor {
                outcomes: VecDeque::from([
                    Err(EffectError::TransientExecution("retryable".to_string())),
                    Ok(Vec::new()),
                ]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_effect(
                    &identity_for("effect.retry_test"),
                    &data_event(),
                    &mut executor,
                    receiver,
                )
                .await
        });

        observed.notified().await;
        controller.request_drain();
        let report = task.await.expect("drain interrupts effect backoff");

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert_eq!(report.control_events.len(), 2);
        assert!(matches!(
            retry_event(&report.control_events[0]),
            RetryEvent::AttemptFailed {
                attempt_number: 1,
                ..
            }
        ));
        assert!(matches!(
            retry_event(&report.control_events[1]),
            RetryEvent::Exhausted {
                total_attempts: 1,
                exhaustion_cause: Some(RetryExhaustionCause::DrainRequested),
                ..
            }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn force_abort_during_effect_backoff_discards_buffered_attempt_row() {
        let observed = Arc::new(Notify::new());
        let boundary = retry_effect_boundary_with_observer(
            effect_retry_owner(3, Duration::from_secs(10), Duration::from_secs(10)),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(Vec::new())),
            Some(observed.clone()),
        );
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let ordinals_for_task = ordinals.clone();
        let (controller, receiver) = obzenflow_runtime::stages::common::boundary_stop_channel();

        let task = tokio::spawn(async move {
            let mut executor = SequenceEffectExecutor {
                outcomes: VecDeque::from([
                    Err(EffectError::TransientExecution("retryable".to_string())),
                    Ok(Vec::new()),
                ]),
                ordinals: ordinals_for_task,
            };
            boundary
                .around_retryable_effect(
                    &identity_for("effect.retry_test"),
                    &data_event(),
                    &mut executor,
                    receiver,
                )
                .await
        });

        // Observation is synchronous and the coordinator's next await is the
        // backoff, after it has buffered AttemptFailed for attempt one.
        observed.notified().await;
        controller.request_abort();
        let report = task.await.expect("force abort interrupts effect backoff");

        assert_eq!(*ordinals.lock().unwrap(), vec![1]);
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::TransientExecution(_)))
        ));
        assert!(report.control_events.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn unrepresentable_deadline_rejects_before_admission_and_emits_no_retry_rows() {
        let mut owner = effect_retry_owner(3, Duration::ZERO, Duration::from_secs(1));
        owner.policy.max_total_wall_time = Duration::MAX;
        let admissions = Arc::new(AtomicUsize::new(0));
        let boundary =
            retry_effect_boundary(owner, admissions.clone(), Arc::new(Mutex::new(Vec::new())));
        let ordinals = Arc::new(Mutex::new(Vec::new()));
        let mut executor = SequenceEffectExecutor {
            outcomes: VecDeque::from([Ok(Vec::new())]),
            ordinals: ordinals.clone(),
        };

        let report = boundary
            .around_retryable_effect(
                &identity_for("effect.retry_test"),
                &data_event(),
                &mut executor,
                BoundaryStopReceiver::default(),
            )
            .await;

        assert_eq!(admissions.load(Ordering::SeqCst), 0);
        assert!(ordinals.lock().unwrap().is_empty());
        assert!(report.control_events.is_empty());
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Aborted(ref reason)
                if reason.cause.code == "retry_deadline_unrepresentable"
        ));
    }

    /// FLOWIP-120c H2 kind agreement: a factory and the instance it creates
    /// must report the same middleware kind, because build-time guards read
    /// the factory while chain runners enforce on the instance.
    #[test]
    fn factory_and_instance_kinds_agree() {
        use crate::middleware::control::rate_limiter::RateLimiterBuilder;
        use crate::middleware::control::ControlMiddlewareAggregator;
        use crate::middleware::{MiddlewareFactory, MiddlewareKind};
        use obzenflow_runtime::pipeline::config::StageConfig;

        let config = StageConfig {
            stage_id: StageId::new(),
            name: "kind_agreement".to_string(),
            flow_name: "kind_agreement_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            resolved_policies: Default::default(),
        };

        let factories: Vec<(Box<dyn MiddlewareFactory>, MiddlewareKind, bool)> = vec![
            (
                crate::middleware::control::circuit_breaker::circuit_breaker(3),
                MiddlewareKind::Policy,
                true,
            ),
            (
                RateLimiterBuilder::new(5.0).build(),
                MiddlewareKind::Policy,
                false,
            ),
        ];

        for (factory, expected, supports_generic_create) in factories {
            assert_eq!(
                factory.kind(),
                expected,
                "factory '{}' kind mismatch",
                factory.label()
            );
            if !supports_generic_create {
                assert!(
                    factory
                        .create(&config, Arc::new(ControlMiddlewareAggregator::new()))
                        .is_err(),
                    "factory '{}' should fail closed on generic create",
                    factory.label()
                );
                continue;
            }
            let instance = factory
                .create(&config, Arc::new(ControlMiddlewareAggregator::new()))
                .expect("factory should materialize");
            assert_eq!(
                instance.kind(),
                factory.kind(),
                "instance kind for '{}' must agree with its factory",
                factory.label()
            );
        }
    }
}
