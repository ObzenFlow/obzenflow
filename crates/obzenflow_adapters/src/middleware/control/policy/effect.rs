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

use crate::middleware::{Middleware, MiddlewareAbortCause, MiddlewareAction, MiddlewareContext};
use async_trait::async_trait;
use obzenflow_core::event::EffectFailureCause;
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectError,
    EffectExecution, EffectIdentity,
};
use std::collections::HashMap;
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

        let result = execute.await;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::circuit_breaker::CircuitBreakerMiddleware;
    use obzenflow_core::event::{
        ChainEventFactory, EffectFailureCode, EffectFailureSource, RetryDisposition,
    };
    use obzenflow_core::{StageId, WriterId};
    use obzenflow_runtime::effects::{EffectCursor, EffectSafety};
    use serde_json::json;
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

    /// FLOWIP-120c H2 kind agreement: a factory and the instance it creates
    /// must report the same middleware kind, because build-time guards read
    /// the factory while chain runners enforce on the instance.
    #[test]
    fn factory_and_instance_kinds_agree() {
        use crate::middleware::control::rate_limiter::RateLimiterBuilder;
        use crate::middleware::control::ControlMiddlewareAggregator;
        use crate::middleware::{backpressure::backpressure, MiddlewareFactory, MiddlewareKind};
        use obzenflow_runtime::pipeline::config::StageConfig;

        let config = StageConfig {
            stage_id: StageId::new(),
            name: "kind_agreement".to_string(),
            flow_name: "kind_agreement_flow".to_string(),
            cycle_guard: None,
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
            (backpressure(64), MiddlewareKind::Structural, true),
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
