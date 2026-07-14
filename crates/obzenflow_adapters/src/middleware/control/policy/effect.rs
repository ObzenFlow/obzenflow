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

use crate::middleware::control::circuit_breaker::{
    CircuitBreakerMiddleware, FailureClassification,
};
use crate::middleware::{Middleware, MiddlewareAbortCause, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerHealthClassification, CircuitBreakerRetryStopReason,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::event::EffectFailureCause;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectError,
    EffectIdentity, EffectOperation,
};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant as StdInstant};

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

    /// Observation of how the attempt ended. Runs for every policy that
    /// admitted, regardless of which arm ended the attempt.
    fn observe(&self, attempt: &EffectAttemptOutcome<'_>, ctx: &mut MiddlewareContext);
}

/// Event-aware effect policy for middleware that genuinely needs parent-event
/// access to classify, synthesize, or derive output facts.
#[async_trait]
pub trait EventAwareEffectPolicy: Send + Sync {
    fn label(&self) -> &'static str;

    async fn admit(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> PolicyAdmission;

    fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    );
}

#[derive(Clone)]
pub struct EffectPolicyAttachment {
    kind: EffectPolicyAttachmentKind,
}

#[derive(Clone)]
enum EffectPolicyAttachmentKind {
    Neutral(Arc<dyn EffectPolicy>),
    EventAware(Arc<dyn EventAwareEffectPolicy>),
    CircuitBreaker(Arc<CircuitBreakerMiddleware>),
}

impl EffectPolicyAttachment {
    pub fn neutral(policy: Arc<dyn EffectPolicy>) -> Self {
        Self {
            kind: EffectPolicyAttachmentKind::Neutral(policy),
        }
    }

    pub fn event_aware(policy: Arc<dyn EventAwareEffectPolicy>) -> Self {
        Self {
            kind: EffectPolicyAttachmentKind::EventAware(policy),
        }
    }

    pub(crate) fn circuit_breaker(policy: Arc<CircuitBreakerMiddleware>) -> Self {
        Self {
            kind: EffectPolicyAttachmentKind::CircuitBreaker(policy),
        }
    }

    fn circuit_breaker_policy(&self) -> Option<&Arc<CircuitBreakerMiddleware>> {
        match &self.kind {
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => Some(policy),
            EffectPolicyAttachmentKind::Neutral(_) | EffectPolicyAttachmentKind::EventAware(_) => {
                None
            }
        }
    }

    fn label(&self) -> &'static str {
        match &self.kind {
            EffectPolicyAttachmentKind::Neutral(policy) => policy.label(),
            EffectPolicyAttachmentKind::EventAware(policy) => policy.label(),
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => {
                Middleware::label(policy.as_ref())
            }
        }
    }

    async fn admit(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> PolicyAdmission {
        match &self.kind {
            EffectPolicyAttachmentKind::Neutral(policy) => policy.admit(ctx).await,
            EffectPolicyAttachmentKind::EventAware(policy) => policy.admit(event, ctx).await,
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => policy.admit(event, ctx).await,
        }
    }

    fn observe(
        &self,
        event: &ChainEvent,
        attempt: &EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match &self.kind {
            EffectPolicyAttachmentKind::Neutral(policy) => policy.observe(attempt, ctx),
            EffectPolicyAttachmentKind::EventAware(policy) => policy.observe(event, attempt, ctx),
            EffectPolicyAttachmentKind::CircuitBreaker(policy) => {
                policy.observe(event, attempt, ctx)
            }
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
                let error_event = effect_error_event(event, err);
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

#[derive(Clone, Copy)]
enum RawRecoveryEligibility {
    Eligible { rate_limit_floor: Duration },
    Ineligible,
}

fn raw_recovery_eligibility(error: &EffectError) -> RawRecoveryEligibility {
    match error {
        EffectError::Timeout(_) | EffectError::Transport(_) => RawRecoveryEligibility::Eligible {
            rate_limit_floor: Duration::ZERO,
        },
        EffectError::RateLimited { retry_after, .. } => RawRecoveryEligibility::Eligible {
            rate_limit_floor: *retry_after,
        },
        EffectError::Serialization(_)
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
        | EffectError::Execution(_)
        | EffectError::Permanent(_)
        | EffectError::Validation(_)
        | EffectError::Domain(_)
        | EffectError::ReplayArchive(_) => RawRecoveryEligibility::Ineligible,
    }
}

fn effect_error_kind(error: &EffectError) -> ErrorKind {
    match error {
        EffectError::Timeout(_) => ErrorKind::Timeout,
        EffectError::Transport(_) => ErrorKind::Remote,
        EffectError::RateLimited { .. } => ErrorKind::RateLimited,
        EffectError::Validation(_) => ErrorKind::Validation,
        EffectError::Domain(_) => ErrorKind::Domain,
        EffectError::Serialization(_)
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
        | EffectError::Execution(_)
        | EffectError::Permanent(_)
        | EffectError::ReplayArchive(_) => ErrorKind::PermanentFailure,
    }
}

pub(crate) fn effect_error_event(event: &ChainEvent, error: &EffectError) -> ChainEvent {
    event
        .clone()
        .mark_as_error(error.to_string(), effect_error_kind(error))
}

fn effect_observation(
    event: &ChainEvent,
    result: &Result<Vec<ChainEvent>, EffectError>,
) -> Vec<ChainEvent> {
    match result {
        Ok(outputs) => outputs.clone(),
        Err(error) => vec![effect_error_event(event, error)],
    }
}

fn prepare_classification_context(
    result: &Result<Vec<ChainEvent>, EffectError>,
    ctx: &mut MiddlewareContext,
) {
    use crate::middleware::context_keys::CircuitBreakerRetryAfterMs;

    ctx.remove::<CircuitBreakerRetryAfterMs>();
    if let Err(EffectError::RateLimited { retry_after, .. }) = result {
        ctx.insert::<CircuitBreakerRetryAfterMs>(
            retry_after.as_millis().min(u64::MAX as u128) as u64
        );
    }
}

fn retry_classification_allows(classification: &FailureClassification) -> bool {
    matches!(
        classification,
        FailureClassification::TransientFailure | FailureClassification::RateLimited(_)
    )
}

fn evidence_classification(
    classification: &FailureClassification,
) -> CircuitBreakerHealthClassification {
    match classification {
        FailureClassification::Success => CircuitBreakerHealthClassification::Success,
        FailureClassification::TransientFailure => {
            CircuitBreakerHealthClassification::TransientFailure
        }
        FailureClassification::PermanentFailure => {
            CircuitBreakerHealthClassification::PermanentFailure
        }
        FailureClassification::RateLimited(_) => CircuitBreakerHealthClassification::RateLimited,
        FailureClassification::PartialSuccess { .. } => {
            CircuitBreakerHealthClassification::PartialSuccess
        }
    }
}

fn observe_reverse(
    policies: &[&EffectPolicyAttachment],
    event: &ChainEvent,
    attempt: &EffectAttemptOutcome<'_>,
    ctx: &mut MiddlewareContext,
) {
    for policy in policies.iter().rev() {
        policy.observe(event, attempt, ctx);
    }
}

async fn execute_chain_once(
    chain: &[EffectPolicyAttachment],
    event: &ChainEvent,
    operation: &mut EffectOperation,
) -> EffectBoundaryReport {
    let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
    let mut admitted: Vec<&EffectPolicyAttachment> = Vec::new();

    for policy in chain {
        match policy.admit(event, &mut ctx).await {
            PolicyAdmission::Admit => admitted.push(policy),
            PolicyAdmission::Synthesize { results, cause: _ } => {
                let attempt = EffectAttemptOutcome::SkippedBy(policy.label());
                observe_reverse(&admitted, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Skipped {
                        results,
                        source: Some(policy.label().to_string()),
                    },
                    control_events: ctx.take_control_events(),
                };
            }
            PolicyAdmission::Reject(cause) => {
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                    control_events: ctx.take_control_events(),
                };
            }
        }
    }

    let call_started = StdInstant::now();
    let result = operation.execute().await;
    ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
        call_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
    );
    let attempt = EffectAttemptOutcome::Executed(&result);
    observe_reverse(&admitted, event, &attempt, &mut ctx);
    EffectBoundaryReport {
        outcome: EffectBoundaryOutcome::Executed(result),
        control_events: ctx.take_control_events(),
    }
}

#[async_trait]
impl EffectBoundary for PerEffectPolicyBoundary {
    async fn around_effect(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        mut operation: EffectOperation,
    ) -> EffectBoundaryReport {
        let Some(chain) = self.chains.get(identity.effect_type) else {
            return EffectBoundaryReport {
                outcome: EffectBoundaryOutcome::Executed(operation.execute().await),
                control_events: Vec::new(),
            };
        };

        let breaker_positions: Vec<_> = chain
            .iter()
            .enumerate()
            .filter_map(|(index, policy)| policy.circuit_breaker_policy().map(|_| index))
            .collect();
        if breaker_positions.is_empty() {
            return execute_chain_once(chain, event, &mut operation).await;
        }
        debug_assert_eq!(
            breaker_positions.len(),
            1,
            "one breaker may guard one effect"
        );

        let breaker_index = breaker_positions[0];
        let breaker_attachment = &chain[breaker_index];
        let breaker = breaker_attachment
            .circuit_breaker_policy()
            .expect("breaker position contains the privileged breaker node");
        let outer = &chain[..breaker_index];
        let inner = &chain[breaker_index + 1..];

        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        let mut admitted_outer: Vec<&EffectPolicyAttachment> = Vec::new();
        for policy in outer {
            match policy.admit(event, &mut ctx).await {
                PolicyAdmission::Admit => admitted_outer.push(policy),
                PolicyAdmission::Synthesize { results, cause: _ } => {
                    let attempt = EffectAttemptOutcome::SkippedBy(policy.label());
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Skipped {
                            results,
                            source: Some(policy.label().to_string()),
                        },
                        control_events: ctx.take_control_events(),
                    };
                }
                PolicyAdmission::Reject(cause) => {
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                        control_events: ctx.take_control_events(),
                    };
                }
            }
        }

        match breaker_attachment.admit(event, &mut ctx).await {
            PolicyAdmission::Admit => {}
            PolicyAdmission::Synthesize { results, cause: _ } => {
                let attempt = EffectAttemptOutcome::SkippedBy(breaker_attachment.label());
                observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Skipped {
                        results,
                        source: Some(breaker_attachment.label().to_string()),
                    },
                    control_events: ctx.take_control_events(),
                };
            }
            PolicyAdmission::Reject(cause) => {
                let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                return EffectBoundaryReport {
                    outcome: EffectBoundaryOutcome::Aborted(abort_reason_from_cause(cause)),
                    control_events: ctx.take_control_events(),
                };
            }
        }

        let mut control_events = ctx.take_control_events();
        let session_started = tokio::time::Instant::now();
        let retry_config = breaker.effect_retry_config();
        let recovery_allowed = !breaker.is_effect_probe(&ctx);
        let mut attempts = 0u32;

        loop {
            let attempt_report = execute_chain_once(inner, event, &mut operation).await;
            control_events.extend(attempt_report.control_events);
            let result = match attempt_report.outcome {
                EffectBoundaryOutcome::Executed(result) => result,
                EffectBoundaryOutcome::Skipped { results, source } => {
                    breaker.settle_not_executed(&mut ctx);
                    let label = source.as_deref().unwrap_or("inner_effect_policy");
                    let attempt = EffectAttemptOutcome::SkippedBy(label);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    control_events.extend(ctx.take_control_events());
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Skipped { results, source },
                        control_events,
                    };
                }
                EffectBoundaryOutcome::Aborted(reason) => {
                    breaker.settle_not_executed(&mut ctx);
                    let cause = MiddlewareAbortCause {
                        source: reason.cause.source.clone(),
                        code: reason.cause.code.clone(),
                        message: reason.message.clone(),
                        retry: reason.retry,
                        event: None,
                    };
                    let attempt = EffectAttemptOutcome::RejectedBy(&cause);
                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                    control_events.extend(ctx.take_control_events());
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Aborted(reason),
                        control_events,
                    };
                }
            };
            attempts = attempts.saturating_add(1);

            prepare_classification_context(&result, &mut ctx);
            let observation = effect_observation(event, &result);
            let (classification, _, _) = breaker.classify_call(event, &observation, &ctx);

            let terminal = match &result {
                Ok(_) => true,
                Err(error) => match retry_config.as_ref() {
                    None => true,
                    Some(_) if !recovery_allowed => true,
                    Some((retry_policy, retry_limits)) => {
                        let eligibility = raw_recovery_eligibility(error);
                        let allows = retry_classification_allows(&classification);
                        if !matches!(eligibility, RawRecoveryEligibility::Eligible { .. })
                            || !allows
                        {
                            if attempts > 1 {
                                ctx.write_control_event(
                                    ChainEventFactory::circuit_breaker_retry_stopped_non_retryable(
                                        breaker.evidence_writer_id(),
                                        identity.cursor.clone(),
                                        attempts,
                                        event.id,
                                    ),
                                );
                            }
                            true
                        } else if attempts >= retry_policy.max_attempts {
                            ctx.write_control_event(
                                ChainEventFactory::circuit_breaker_retry_exhausted(
                                    breaker.evidence_writer_id(),
                                    identity.cursor.clone(),
                                    attempts,
                                    CircuitBreakerRetryStopReason::AttemptLimit,
                                    event.id,
                                ),
                            );
                            true
                        } else if !breaker.is_closed_for_effect_recovery() {
                            ctx.write_control_event(
                                ChainEventFactory::circuit_breaker_retry_exhausted(
                                    breaker.evidence_writer_id(),
                                    identity.cursor.clone(),
                                    attempts,
                                    CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
                                    event.id,
                                ),
                            );
                            control_events.extend(ctx.take_control_events());
                            let attempt = EffectAttemptOutcome::Executed(&result);
                            observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                            control_events.extend(ctx.take_control_events());
                            return EffectBoundaryReport {
                                outcome: EffectBoundaryOutcome::Executed(result),
                                control_events,
                            };
                        } else {
                            let generated = retry_policy
                                .calculate_delay(attempts.saturating_sub(1) as usize)
                                .min(retry_limits.max_single_delay);
                            let raw_floor = match eligibility {
                                RawRecoveryEligibility::Eligible { rate_limit_floor } => {
                                    rate_limit_floor
                                }
                                RawRecoveryEligibility::Ineligible => Duration::ZERO,
                            };
                            let classification_floor = match &classification {
                                FailureClassification::RateLimited(delay) => *delay,
                                _ => Duration::ZERO,
                            };
                            let delay = generated.max(raw_floor).max(classification_floor);
                            if session_started.elapsed().saturating_add(delay)
                                >= retry_limits.max_attempt_start_window
                            {
                                ctx.write_control_event(
                                    ChainEventFactory::circuit_breaker_retry_exhausted(
                                        breaker.evidence_writer_id(),
                                        identity.cursor.clone(),
                                        attempts,
                                        CircuitBreakerRetryStopReason::AttemptStartWindow,
                                        event.id,
                                    ),
                                );
                                true
                            } else {
                                ctx.write_control_event(
                                    ChainEventFactory::circuit_breaker_retry_scheduled(
                                        breaker.evidence_writer_id(),
                                        identity.cursor.clone(),
                                        attempts.saturating_add(1),
                                        delay.as_millis().min(u64::MAX as u128) as u64,
                                        event.id,
                                    ),
                                );
                                control_events.extend(ctx.take_control_events());
                                tokio::time::sleep(delay).await;

                                let stop_reason = if session_started.elapsed()
                                    >= retry_limits.max_attempt_start_window
                                {
                                    Some(CircuitBreakerRetryStopReason::AttemptStartWindow)
                                } else if !breaker.is_closed_for_effect_recovery() {
                                    Some(CircuitBreakerRetryStopReason::CircuitNoLongerClosed)
                                } else {
                                    None
                                };
                                if let Some(reason) = stop_reason {
                                    ctx.write_control_event(
                                        ChainEventFactory::circuit_breaker_retry_exhausted(
                                            breaker.evidence_writer_id(),
                                            identity.cursor.clone(),
                                            attempts,
                                            reason,
                                            event.id,
                                        ),
                                    );
                                    control_events.extend(ctx.take_control_events());
                                    let attempt = EffectAttemptOutcome::Executed(&result);
                                    observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
                                    control_events.extend(ctx.take_control_events());
                                    return EffectBoundaryReport {
                                        outcome: EffectBoundaryOutcome::Executed(result),
                                        control_events,
                                    };
                                }
                                false
                            }
                        }
                    }
                },
            };

            if !terminal {
                continue;
            }

            ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
                session_started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
            breaker.post_handle(event, &observation, &mut ctx);
            if attempts > 1 && result.is_ok() {
                ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_succeeded(
                    breaker.evidence_writer_id(),
                    identity.cursor.clone(),
                    attempts,
                    evidence_classification(&classification),
                    event.id,
                ));
            }

            let attempt = EffectAttemptOutcome::Executed(&result);
            observe_reverse(&admitted_outer, event, &attempt, &mut ctx);
            control_events.extend(ctx.take_control_events());
            return EffectBoundaryReport {
                outcome: EffectBoundaryOutcome::Executed(result),
                control_events,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::circuit_breaker::{
        CircuitBreakerBuilder, CircuitBreakerMiddleware, RetryLimits,
    };
    use crate::middleware::control::rate_limiter::RateLimiterBuilder;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::{
        EffectSurface, EffectTypeKey, EffectUnitId, MiddlewareAttachmentRequest,
        MiddlewareDeclarationIndex, MiddlewareFactory, MiddlewareMaterializationContext,
        MiddlewareOrigin, MiddlewareSurface, MiddlewareSurfaceAttachment, ProtectedUnit,
        ProtectedUnitId,
    };
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::observability_payload::{
        CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
    };
    use obzenflow_core::event::ChainEventContent;
    use obzenflow_core::event::{
        ChainEventFactory, EffectFailureCode, EffectFailureSource, RetryDisposition,
    };
    use obzenflow_core::{StageId, WriterId};
    use obzenflow_runtime::control_plane::ControlPlaneProvider;
    use obzenflow_runtime::effects::{EffectCursor, EffectSafety};
    use obzenflow_runtime::pipeline::config::StageConfig;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

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

    fn ok_execute() -> EffectOperation {
        EffectOperation::new(|| async { Ok(Vec::new()) })
    }

    fn failing_execute() -> EffectOperation {
        EffectOperation::new(|| async {
            Err(EffectError::Execution("simulated_failure".to_string()))
        })
    }

    fn test_stage_config() -> StageConfig {
        StageConfig {
            stage_id: StageId::new(),
            name: "retrying_breaker_test".to_string(),
            flow_name: "retrying_breaker_test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            resolved_policies: Default::default(),
        }
    }

    fn materialize_effect_attachment(
        factory: &dyn MiddlewareFactory,
        config: &StageConfig,
        control: &Arc<ControlMiddlewareAggregator>,
        declaration_index: usize,
        safety: EffectSafety,
    ) -> Result<EffectPolicyAttachment, String> {
        let surface = MiddlewareSurface::Effect(EffectSurface {
            stage_id: config.stage_id,
            effect_type: EffectTypeKey::from("effect.retry"),
            safety,
        });
        let protected_unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId {
                effect_type: EffectTypeKey::from("effect.retry"),
            }),
        };
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::effect_policy(declaration_index),
        };
        let context = MiddlewareMaterializationContext {
            config,
            control_middleware: control,
            stage_type: StageType::Transform,
        };
        match factory
            .materialize(request, &context)
            .map_err(|error| error.to_string())?
        {
            MiddlewareSurfaceAttachment::Effect(attachment) => Ok(attachment),
            _ => panic!("circuit breaker should materialize an effect attachment"),
        }
    }

    fn retrying_breaker_fixture(
        builder: CircuitBreakerBuilder,
    ) -> (
        EffectPolicyAttachment,
        Arc<ControlMiddlewareAggregator>,
        StageId,
    ) {
        let factory = builder.build();
        let config = test_stage_config();
        let stage_id = config.stage_id;
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let attachment = materialize_effect_attachment(
            factory.as_ref(),
            &config,
            &control,
            0,
            EffectSafety::Idempotent,
        )
        .expect("retrying breaker should materialize for an idempotent effect");
        (attachment, control, stage_id)
    }

    fn retrying_breaker_attachment(builder: CircuitBreakerBuilder) -> EffectPolicyAttachment {
        retrying_breaker_fixture(builder).0
    }

    fn rate_limiter_fixture() -> (
        EffectPolicyAttachment,
        Arc<ControlMiddlewareAggregator>,
        StageId,
    ) {
        let factory = RateLimiterBuilder::new(1_000.0).with_burst(10.0).build();
        let config = test_stage_config();
        let stage_id = config.stage_id;
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let attachment = materialize_effect_attachment(
            factory.as_ref(),
            &config,
            &control,
            1,
            EffectSafety::Idempotent,
        )
        .expect("rate limiter should materialize for an effect");
        (attachment, control, stage_id)
    }

    fn boundary_with_chain(chain: Vec<EffectPolicyAttachment>) -> PerEffectPolicyBoundary {
        let mut chains = HashMap::new();
        chains.insert("effect.retry", Arc::new(chain));
        PerEffectPolicyBoundary::new(chains)
    }

    fn retry_events(report: &EffectBoundaryReport) -> Vec<&CircuitBreakerEvent> {
        report
            .control_events
            .iter()
            .filter_map(|event| match &event.content {
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::CircuitBreaker(event),
                )) if matches!(
                    event,
                    CircuitBreakerEvent::RetryScheduled { .. }
                        | CircuitBreakerEvent::RetrySucceeded { .. }
                        | CircuitBreakerEvent::RetryExhausted { .. }
                        | CircuitBreakerEvent::RetryStoppedNonRetryable { .. }
                ) =>
                {
                    Some(event)
                }
                _ => None,
            })
            .collect()
    }

    fn scheduled_delays(report: &EffectBoundaryReport) -> Vec<u64> {
        retry_events(report)
            .into_iter()
            .filter_map(|event| match event {
                CircuitBreakerEvent::RetryScheduled { delay_ms, .. } => Some(*delay_ms),
                _ => None,
            })
            .collect()
    }

    fn effect_limiter_events(control: &ControlMiddlewareAggregator, stage_id: StageId) -> u64 {
        let snapshots = control.effect_rate_limiter_snapshotters(&stage_id);
        assert_eq!(
            snapshots.len(),
            1,
            "one effect limiter should be registered"
        );
        snapshots[0].1().events_total
    }

    fn scripted_operation(
        calls: Arc<AtomicUsize>,
        result_for_call: impl Fn(usize) -> Result<Vec<ChainEvent>, EffectError> + Send + Sync + 'static,
    ) -> EffectOperation {
        let result_for_call = Arc::new(result_for_call);
        EffectOperation::new(move || {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            let result_for_call = result_for_call.clone();
            async move { result_for_call(call) }
        })
    }

    #[tokio::test]
    async fn retrying_breaker_recovers_inside_one_boundary_invocation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3)
                .with_retry_fixed(Duration::ZERO, 3)
                .with_retry_limits(RetryLimits {
                    max_single_delay: Duration::from_secs(1),
                    max_attempt_start_window: Duration::from_secs(5),
                }),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |call| {
            if call <= 2 {
                Err(EffectError::Timeout("scripted timeout".to_string()))
            } else {
                Ok(Vec::new())
            }
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        let events = retry_events(&report);
        assert_eq!(
            events
                .iter()
                .filter(|event| matches!(event, CircuitBreakerEvent::RetryScheduled { .. }))
                .count(),
            2
        );
        assert!(events.iter().any(|event| matches!(
            event,
            CircuitBreakerEvent::RetrySucceeded {
                total_attempts: 3,
                terminal_classification: CircuitBreakerHealthClassification::Success,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn concurrent_effect_cursors_keep_independent_attempt_state() {
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(10).with_retry_fixed(Duration::ZERO, 2),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let first_identity = identity_for("effect.retry");
        let second_identity = EffectIdentity {
            cursor: EffectCursor::new("test_flow", "test_stage", 1, 1),
            ..identity_for("effect.retry")
        };
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let first_event = data_event();
        let second_event = data_event();

        let (first_report, second_report) = tokio::join!(
            boundary.around_effect(
                &first_identity,
                &first_event,
                scripted_operation(first_calls.clone(), |call| {
                    if call == 1 {
                        Err(EffectError::Timeout("first cursor".to_string()))
                    } else {
                        Ok(Vec::new())
                    }
                }),
            ),
            boundary.around_effect(
                &second_identity,
                &second_event,
                scripted_operation(second_calls.clone(), |call| {
                    if call == 1 {
                        Err(EffectError::Timeout("second cursor".to_string()))
                    } else {
                        Ok(Vec::new())
                    }
                }),
            ),
        );

        assert_eq!(first_calls.load(Ordering::SeqCst), 2);
        assert_eq!(second_calls.load(Ordering::SeqCst), 2);
        for (report, expected_cursor) in [
            (&first_report, &first_identity.cursor),
            (&second_report, &second_identity.cursor),
        ] {
            let events = retry_events(report);
            assert!(events.iter().any(|event| matches!(
                event,
                CircuitBreakerEvent::RetrySucceeded {
                    total_attempts: 2,
                    ..
                }
            )));
            assert!(events.iter().all(|event| match event {
                CircuitBreakerEvent::RetryScheduled { cursor, .. }
                | CircuitBreakerEvent::RetrySucceeded { cursor, .. }
                | CircuitBreakerEvent::RetryExhausted { cursor, .. }
                | CircuitBreakerEvent::RetryStoppedNonRetryable { cursor, .. } => {
                    cursor == expected_cursor
                }
                _ => unreachable!("retry_events returns only retry evidence"),
            }));
        }
    }

    #[tokio::test]
    async fn opaque_execution_failure_is_never_promoted_to_retry() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3)
                .with_retry_fixed(Duration::ZERO, 3)
                .with_failure_classification_classifier(|_, _| {
                    FailureClassification::TransientFailure
                }),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |_| {
            Err(EffectError::Execution("opaque failure".to_string()))
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Execution(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(retry_events(&report).is_empty());
    }

    #[tokio::test]
    async fn raw_success_is_not_reexecuted_when_classifier_marks_it_transient() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(1)
                .with_retry_fixed(Duration::ZERO, 3)
                .with_failure_classification_classifier(|_, _| {
                    FailureClassification::TransientFailure
                }),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |_| Ok(Vec::new()));

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(retry_events(&report).is_empty());
    }

    #[tokio::test]
    async fn retry_exhaustion_returns_the_exact_last_failure() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3).with_retry_fixed(Duration::ZERO, 3),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |call| {
            Err(EffectError::Timeout(format!("timeout-{call}")))
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(ref message)))
                if message == "timeout-3"
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(retry_events(&report).iter().any(|event| matches!(
            event,
            CircuitBreakerEvent::RetryExhausted {
                total_attempts: 3,
                reason: CircuitBreakerRetryStopReason::AttemptLimit,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn later_permanent_failure_stops_recovery() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3).with_retry_fixed(Duration::ZERO, 3),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |call| {
            if call == 1 {
                Err(EffectError::Transport("disconnected".to_string()))
            } else {
                Err(EffectError::Permanent("credentials rejected".to_string()))
            }
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Permanent(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(retry_events(&report).iter().any(|event| matches!(
            event,
            CircuitBreakerEvent::RetryStoppedNonRetryable {
                total_attempts: 2,
                ..
            }
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_backoff_waits_exactly_before_each_continuation() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3)
                .with_retry_fixed(Duration::from_millis(7), 3)
                .with_retry_limits(RetryLimits {
                    max_single_delay: Duration::from_secs(1),
                    max_attempt_start_window: Duration::from_secs(1),
                }),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let started = tokio::time::Instant::now();
        let operation = scripted_operation(calls.clone(), |call| {
            if call < 3 {
                Err(EffectError::Timeout("try again".to_string()))
            } else {
                Ok(Vec::new())
            }
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(scheduled_delays(&report), [7, 7]);
        assert_eq!(started.elapsed(), Duration::from_millis(14));
    }

    #[tokio::test(start_paused = true)]
    async fn exponential_backoff_applies_jitter_before_the_single_delay_cap() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(5)
                .with_retry_exponential(4)
                .with_retry_jitter_samples(vec![0.0, 0.0, 0.5])
                .with_retry_limits(RetryLimits {
                    max_single_delay: Duration::from_millis(500),
                    max_attempt_start_window: Duration::from_secs(5),
                }),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |call| {
            if call < 4 {
                Err(EffectError::Transport("try again".to_string()))
            } else {
                Ok(Vec::new())
            }
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        let delays = scheduled_delays(&report);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(delays.len(), 3);
        assert_eq!(delays, [225, 450, 500]);
    }

    #[tokio::test(start_paused = true)]
    async fn raw_rate_limit_floor_cannot_be_shortened_by_classifier_or_backoff_cap() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3)
                .with_retry_fixed(Duration::from_millis(1), 2)
                .with_retry_limits(RetryLimits {
                    max_single_delay: Duration::from_millis(5),
                    max_attempt_start_window: Duration::from_secs(1),
                })
                .with_failure_classification_classifier(|_, _| {
                    FailureClassification::RateLimited(Duration::from_millis(10))
                }),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |call| {
            if call == 1 {
                Err(EffectError::RateLimited {
                    message: "quota".to_string(),
                    retry_after: Duration::from_millis(50),
                })
            } else {
                Ok(Vec::new())
            }
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(scheduled_delays(&report), [50]);
    }

    #[tokio::test(start_paused = true)]
    async fn rate_limit_floor_at_start_window_prevents_another_attempt() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3)
                .with_retry_fixed(Duration::ZERO, 2)
                .with_retry_limits(RetryLimits {
                    max_single_delay: Duration::from_millis(1),
                    max_attempt_start_window: Duration::from_millis(100),
                }),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = scripted_operation(calls.clone(), |_| {
            Err(EffectError::RateLimited {
                message: "quota".to_string(),
                retry_after: Duration::from_millis(100),
            })
        });

        let report = boundary
            .around_effect(&identity_for("effect.retry"), &data_event(), operation)
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(scheduled_delays(&report).is_empty());
        assert!(retry_events(&report).iter().any(|event| matches!(
            event,
            CircuitBreakerEvent::RetryExhausted {
                total_attempts: 1,
                reason: CircuitBreakerRetryStopReason::AttemptStartWindow,
                ..
            }
        )));
    }

    #[tokio::test]
    async fn custom_classifier_can_veto_but_not_promote_recovery() {
        for classification in [
            FailureClassification::Success,
            FailureClassification::PermanentFailure,
            FailureClassification::PartialSuccess { failed_ratio: 0.5 },
        ] {
            let timeout_calls = Arc::new(AtomicUsize::new(0));
            let vetoing_breaker = retrying_breaker_attachment(
                CircuitBreakerBuilder::new(3)
                    .with_retry_fixed(Duration::ZERO, 3)
                    .with_failure_classification_classifier(move |_, _| classification.clone()),
            );
            let veto_report = boundary_with_chain(vec![vetoing_breaker])
                .around_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(timeout_calls.clone(), |_| {
                        Err(EffectError::Timeout("classifier veto".to_string()))
                    }),
                )
                .await;
            assert_eq!(timeout_calls.load(Ordering::SeqCst), 1);
            assert!(retry_events(&veto_report).is_empty());
        }

        for error in [
            EffectError::Permanent("permanent".to_string()),
            EffectError::Validation("invalid".to_string()),
            EffectError::Domain("declined".to_string()),
            EffectError::Serialization("bad payload".to_string()),
        ] {
            let calls = Arc::new(AtomicUsize::new(0));
            let breaker = retrying_breaker_attachment(
                CircuitBreakerBuilder::new(3)
                    .with_retry_fixed(Duration::ZERO, 3)
                    .with_failure_classification_classifier(|_, _| {
                        FailureClassification::TransientFailure
                    }),
            );
            let slot = Arc::new(Mutex::new(Some(error)));
            let report = boundary_with_chain(vec![breaker])
                .around_effect(
                    &identity_for("effect.retry"),
                    &data_event(),
                    scripted_operation(calls.clone(), move |_| {
                        Err(slot.lock().unwrap().take().expect("called once"))
                    }),
                )
                .await;
            assert!(matches!(
                report.outcome,
                EffectBoundaryOutcome::Executed(Err(_))
            ));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert!(retry_events(&report).is_empty());
        }
    }

    #[test]
    fn typed_effect_failures_map_to_structured_breaker_kinds() {
        assert_eq!(
            effect_error_kind(&EffectError::Timeout("timeout".to_string())),
            ErrorKind::Timeout
        );
        assert_eq!(
            effect_error_kind(&EffectError::Transport("offline".to_string())),
            ErrorKind::Remote
        );
        assert_eq!(
            effect_error_kind(&EffectError::RateLimited {
                message: "quota".to_string(),
                retry_after: Duration::from_secs(1),
            }),
            ErrorKind::RateLimited
        );
        assert_eq!(
            effect_error_kind(&EffectError::Permanent("denied".to_string())),
            ErrorKind::PermanentFailure
        );
        assert_eq!(
            effect_error_kind(&EffectError::Validation("invalid".to_string())),
            ErrorKind::Validation
        );
        assert_eq!(
            effect_error_kind(&EffectError::Domain("declined".to_string())),
            ErrorKind::Domain
        );
        assert_eq!(
            effect_error_kind(&EffectError::Execution("opaque".to_string())),
            ErrorKind::PermanentFailure
        );
    }

    #[tokio::test]
    async fn cancelling_during_backoff_drops_the_sequence_without_a_later_call() {
        let calls = Arc::new(AtomicUsize::new(0));
        let first_call = Arc::new(tokio::sync::Notify::new());
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3).with_retry_fixed(Duration::from_secs(60), 2),
        );
        let boundary = boundary_with_chain(vec![breaker]);
        let operation = {
            let first_call = first_call.clone();
            scripted_operation(calls.clone(), move |_| {
                first_call.notify_one();
                Err(EffectError::Timeout("wait".to_string()))
            })
        };
        let task = tokio::spawn(async move {
            boundary
                .around_effect(&identity_for("effect.retry"), &data_event(), operation)
                .await
        });

        first_call.notified().await;
        tokio::task::yield_now().await;
        task.abort();
        let cancellation = task.await;
        assert!(matches!(cancellation, Err(error) if error.is_cancelled()));
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn pending_continuation_stops_when_another_invocation_opens_the_circuit() {
        let (breaker, control, stage_id) = retrying_breaker_fixture(
            CircuitBreakerBuilder::new(1)
                .with_retry_fixed(Duration::from_secs(1), 2)
                .with_retry_limits(RetryLimits {
                    max_single_delay: Duration::from_secs(1),
                    max_attempt_start_window: Duration::from_secs(5),
                }),
        );
        let boundary = Arc::new(boundary_with_chain(vec![breaker]));
        let first_attempt_finished = Arc::new(tokio::sync::Notify::new());
        let pending_calls = Arc::new(AtomicUsize::new(0));
        let pending_task = {
            let boundary = boundary.clone();
            let first_attempt_finished = first_attempt_finished.clone();
            let pending_calls = pending_calls.clone();
            tokio::spawn(async move {
                boundary
                    .around_effect(
                        &identity_for("effect.retry"),
                        &data_event(),
                        scripted_operation(pending_calls, move |_| {
                            first_attempt_finished.notify_one();
                            Err(EffectError::Timeout("pending".to_string()))
                        }),
                    )
                    .await
            })
        };

        first_attempt_finished.notified().await;
        tokio::task::yield_now().await;

        let opening_calls = Arc::new(AtomicUsize::new(0));
        let opening_report = boundary
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(opening_calls.clone(), |_| {
                    Err(EffectError::Permanent("open now".to_string()))
                }),
            )
            .await;
        assert!(matches!(
            opening_report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Permanent(_)))
        ));
        assert_eq!(opening_calls.load(Ordering::SeqCst), 1);

        tokio::time::advance(Duration::from_secs(1)).await;
        let pending_report = pending_task
            .await
            .expect("pending invocation should finish");
        assert!(matches!(
            pending_report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
        ));
        assert_eq!(pending_calls.load(Ordering::SeqCst), 1);
        assert!(retry_events(&pending_report).iter().any(|event| matches!(
            event,
            CircuitBreakerEvent::RetryExhausted {
                total_attempts: 1,
                reason: CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
                ..
            }
        )));

        let snapshots = control.effect_circuit_breaker_snapshotters(&stage_id);
        let metrics = snapshots[0].1();
        assert_eq!(metrics.requests_total, 1);
        assert_eq!(metrics.failures_total, 1);
    }

    struct GateSecondAdmission {
        admissions: AtomicUsize,
        second_admitted: Arc<tokio::sync::Notify>,
        release_second: Arc<tokio::sync::Notify>,
    }

    #[async_trait]
    impl EffectPolicy for GateSecondAdmission {
        fn label(&self) -> &'static str {
            "test.gate_second_admission"
        }

        async fn admit(&self, _ctx: &mut MiddlewareContext) -> PolicyAdmission {
            if self.admissions.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                self.second_admitted.notify_one();
                self.release_second.notified().await;
            }
            PolicyAdmission::Admit
        }

        fn observe(&self, _attempt: &EffectAttemptOutcome<'_>, _ctx: &mut MiddlewareContext) {}
    }

    #[tokio::test]
    async fn continuation_already_admitted_by_an_inner_policy_completes_after_opening() {
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(1).with_retry_fixed(Duration::ZERO, 2),
        );
        let second_admitted = Arc::new(tokio::sync::Notify::new());
        let release_second = Arc::new(tokio::sync::Notify::new());
        let gate = EffectPolicyAttachment::neutral(Arc::new(GateSecondAdmission {
            admissions: AtomicUsize::new(0),
            second_admitted: second_admitted.clone(),
            release_second: release_second.clone(),
        }));
        let boundary = Arc::new(boundary_with_chain(vec![breaker, gate]));

        let recovering_calls = Arc::new(AtomicUsize::new(0));
        let recovering_task = {
            let boundary = boundary.clone();
            let recovering_calls = recovering_calls.clone();
            tokio::spawn(async move {
                boundary
                    .around_effect(
                        &identity_for("effect.retry"),
                        &data_event(),
                        scripted_operation(recovering_calls, |call| {
                            if call == 1 {
                                Err(EffectError::Timeout("recover".to_string()))
                            } else {
                                Ok(Vec::new())
                            }
                        }),
                    )
                    .await
            })
        };

        second_admitted.notified().await;
        let opening_calls = Arc::new(AtomicUsize::new(0));
        let opening_report = boundary
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(opening_calls.clone(), |_| {
                    Err(EffectError::Permanent("open now".to_string()))
                }),
            )
            .await;
        assert!(matches!(
            opening_report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Permanent(_)))
        ));
        assert_eq!(opening_calls.load(Ordering::SeqCst), 1);

        release_second.notify_one();
        let recovering_report = recovering_task
            .await
            .expect("admitted continuation should finish");
        assert!(matches!(
            recovering_report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(recovering_calls.load(Ordering::SeqCst), 2);
        assert!(retry_events(&recovering_report)
            .iter()
            .any(|event| matches!(event, CircuitBreakerEvent::RetrySucceeded { .. })));
    }

    #[tokio::test]
    async fn policy_position_controls_logical_vs_physical_admission_count() {
        let inner_calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3).with_retry_fixed(Duration::ZERO, 3),
        );
        let (inner_limiter, inner_control, inner_stage) = rate_limiter_fixture();
        let report = boundary_with_chain(vec![breaker, inner_limiter])
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(inner_calls.clone(), |call| {
                    if call < 3 {
                        Err(EffectError::Timeout("again".to_string()))
                    } else {
                        Ok(Vec::new())
                    }
                }),
            )
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(effect_limiter_events(&inner_control, inner_stage), 3);

        let outer_calls = Arc::new(AtomicUsize::new(0));
        let (outer_limiter, outer_control, outer_stage) = rate_limiter_fixture();
        let breaker = retrying_breaker_attachment(
            CircuitBreakerBuilder::new(3).with_retry_fixed(Duration::ZERO, 3),
        );
        let report = boundary_with_chain(vec![outer_limiter, breaker])
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(outer_calls.clone(), |call| {
                    if call < 3 {
                        Err(EffectError::Timeout("again".to_string()))
                    } else {
                        Ok(Vec::new())
                    }
                }),
            )
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        assert_eq!(effect_limiter_events(&outer_control, outer_stage), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn recovered_session_settles_breaker_health_once_and_can_be_slow() {
        let calls = Arc::new(AtomicUsize::new(0));
        let (breaker, control, stage_id) = retrying_breaker_fixture(
            CircuitBreakerBuilder::new(3)
                .rate_based_over_last_n_calls(1, 1.0)
                .slow_call(Duration::from_millis(5), 1.0)
                .with_retry_fixed(Duration::from_millis(10), 2),
        );
        let report = boundary_with_chain(vec![breaker])
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(calls.clone(), |call| {
                    if call == 1 {
                        Err(EffectError::Timeout("again".to_string()))
                    } else {
                        Ok(Vec::new())
                    }
                }),
            )
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Ok(_))
        ));
        let snapshots = control.effect_circuit_breaker_snapshotters(&stage_id);
        assert_eq!(snapshots.len(), 1);
        let metrics = snapshots[0].1();
        assert_eq!(metrics.requests_total, 1);
        assert_eq!(metrics.successes_total, 1);
        assert_eq!(metrics.failures_total, 0);
        assert_eq!(
            metrics.opened_total, 1,
            "the whole 10 ms session is one slow sample"
        );
    }

    #[tokio::test]
    async fn open_circuit_makes_no_call_and_half_open_probe_never_retries() {
        let (open_breaker, _, _) = retrying_breaker_fixture(
            CircuitBreakerBuilder::new(1)
                .cooldown(Duration::from_secs(60))
                .with_retry_fixed(Duration::ZERO, 3),
        );
        let open_boundary = boundary_with_chain(vec![open_breaker]);
        let first_calls = Arc::new(AtomicUsize::new(0));
        open_boundary
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(first_calls, |_| {
                    Err(EffectError::Permanent("open".to_string()))
                }),
            )
            .await;
        let rejected_calls = Arc::new(AtomicUsize::new(0));
        let report = open_boundary
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(rejected_calls.clone(), |_| Ok(Vec::new())),
            )
            .await;
        assert!(matches!(report.outcome, EffectBoundaryOutcome::Aborted(_)));
        assert_eq!(rejected_calls.load(Ordering::SeqCst), 0);

        let (half_open_breaker, _, _) = retrying_breaker_fixture(
            CircuitBreakerBuilder::new(1)
                .cooldown(Duration::ZERO)
                .with_retry_fixed(Duration::ZERO, 3),
        );
        let half_open_boundary = boundary_with_chain(vec![half_open_breaker]);
        half_open_boundary
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(Arc::new(AtomicUsize::new(0)), |_| {
                    Err(EffectError::Permanent("open".to_string()))
                }),
            )
            .await;
        let probe_calls = Arc::new(AtomicUsize::new(0));
        let report = half_open_boundary
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(probe_calls.clone(), |_| {
                    Err(EffectError::Timeout("probe failed".to_string()))
                }),
            )
            .await;
        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
        ));
        assert_eq!(probe_calls.load(Ordering::SeqCst), 1);
        assert!(retry_events(&report).is_empty());
    }

    #[test]
    #[should_panic(expected = "max_attempts must be greater than zero")]
    fn zero_attempt_configuration_is_rejected() {
        let _ = CircuitBreakerBuilder::new(3).with_retry_fixed(Duration::ZERO, 0);
    }

    #[tokio::test]
    async fn health_only_breaker_calls_a_failing_effect_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let breaker = retrying_breaker_attachment(CircuitBreakerBuilder::new(3));
        let report = boundary_with_chain(vec![breaker])
            .around_effect(
                &identity_for("effect.retry"),
                &data_event(),
                scripted_operation(calls.clone(), |_| {
                    Err(EffectError::Timeout("no recovery configured".to_string()))
                }),
            )
            .await;

        assert!(matches!(
            report.outcome,
            EffectBoundaryOutcome::Executed(Err(EffectError::Timeout(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(retry_events(&report).is_empty());
    }

    #[test]
    fn retry_materialization_is_limited_to_safe_declared_effects() {
        let config = test_stage_config();
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let factory = CircuitBreakerBuilder::new(3)
            .with_retry_fixed(Duration::ZERO, 2)
            .build();
        let error = match materialize_effect_attachment(
            factory.as_ref(),
            &config,
            &control,
            0,
            EffectSafety::Transactional,
        ) {
            Err(error) => error,
            Ok(_) => panic!("unsafe recovery attachment must fail"),
        };
        assert!(error.contains("retry is not eligible"), "{error}");

        let config = test_stage_config();
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let factory = CircuitBreakerBuilder::new(3)
            .with_retry_fixed(Duration::ZERO, 2)
            .build();
        materialize_effect_attachment(
            factory.as_ref(),
            &config,
            &control,
            0,
            EffectSafety::NonIdempotentRequiresKey,
        )
        .expect("a keyed non-idempotent effect is recovery-eligible");
    }

    #[test]
    fn a_second_retrying_breaker_for_one_effect_fails_materialization() {
        let config = test_stage_config();
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let first = CircuitBreakerBuilder::new(3)
            .with_retry_fixed(Duration::ZERO, 2)
            .build();
        let second = CircuitBreakerBuilder::new(3)
            .with_retry_fixed(Duration::ZERO, 2)
            .build();

        materialize_effect_attachment(
            first.as_ref(),
            &config,
            &control,
            0,
            EffectSafety::Idempotent,
        )
        .expect("first breaker should materialize");
        let error = match materialize_effect_attachment(
            second.as_ref(),
            &config,
            &control,
            1,
            EffectSafety::Idempotent,
        ) {
            Err(error) => error,
            Ok(_) => panic!("duplicate breaker must be rejected, not replace the first"),
        };
        assert!(error.contains("already registered"), "{error}");
    }

    #[test]
    fn ai_breaker_helper_is_health_only() {
        let snapshot = crate::middleware::control::ai_circuit_breaker()
            .config_snapshot()
            .expect("AI breaker should expose its configuration");
        assert!(snapshot["retry"].is_null());
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
