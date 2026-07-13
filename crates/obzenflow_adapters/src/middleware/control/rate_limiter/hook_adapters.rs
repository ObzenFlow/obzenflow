// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: carrier-bound surface adapters for the token-bucket rate
//! limiter.
//!
//! Each adapter is a thin shim that turns a live-I/O boundary attempt into an
//! admission reservation against the shared [`super::RateLimiterMiddleware`]
//! (and its [`super::admission_core::RateLimiterCore`]). The source, effect, and
//! sink-delivery adapters await capacity, then commit only at executor start;
//! the ingress adapter is fail-fast and commits at the hosted listener edge.
//! Production placement goes through [`super::factory`].

use super::RateLimiterMiddleware;
use crate::middleware::{
    MiddlewareContext, SinkAdmission, SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx,
    SourceAdmission, SourceAfterPoll, SourceBatchFacts, SourcePolicy, SourcePolicyCtx,
    SourcePollOutcome,
};
use obzenflow_core::ingress::{
    IngressAdmissionDecision, IngressAdmissionOutcome, IngressAttemptContext,
    IngressBoundaryMiddleware,
};
use std::sync::Arc;

/// Per-effect policy adapter (FLOWIP-120c): one limiter instance guards one
/// declared effect. It awaits and reserves capacity at the live effect boundary
/// instead of blocking a worker thread. Physical-call accounting commits only
/// when the boundary starts the executor.
#[async_trait::async_trait]
impl crate::middleware::EffectPolicy for RateLimiterMiddleware {
    fn label(&self) -> &'static str {
        self.label()
    }

    async fn admit(&self, ctx: &mut MiddlewareContext) -> crate::middleware::PolicyAdmission {
        // FLOWIP-114o: shared with the source pacing path. Replay suppression at
        // the effect boundary happens before `admit` is consulted, so this path
        // carries no scope guard of its own.
        self.reserve_permit_async(ctx).await;
        crate::middleware::PolicyAdmission::Admit
    }

    fn commit_execution(&self, ctx: &mut MiddlewareContext) {
        self.commit_permit_reservations(ctx);
    }

    fn observe(
        &self,
        _attempt: &crate::middleware::EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        self.maybe_emit_activity_pulse(ctx);
        self.maybe_emit_summary(ctx);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SourceRateLimitPosition {
    PrePoll,
    AfterPoll,
}

/// Source-boundary policy for the token-bucket rate limiter (FLOWIP-115a).
///
/// Pre-poll pacing reserves with a cancellable Tokio sleep and commits at poll
/// start. Finite sources retain their after-poll charge when no boundary retry
/// is active; a retry physical call instead reserves and commits at executor
/// start so failed attempts cannot bypass accounting.
pub(super) struct RateLimiterSourcePolicy {
    inner: Arc<RateLimiterMiddleware>,
    charge_at: SourceRateLimitPosition,
}

impl RateLimiterSourcePolicy {
    pub(super) fn new(
        inner: Arc<RateLimiterMiddleware>,
        charge_at: SourceRateLimitPosition,
    ) -> Self {
        Self { inner, charge_at }
    }

    async fn reserve(&self, ctx: &mut SourcePolicyCtx) {
        // FLOWIP-115d: the core admits by protected unit and typed metadata, so
        // the source path no longer fabricates a synthetic `ChainEvent` to drive
        // admission.
        self.inner
            .reserve_permit_async(ctx.middleware_context_mut())
            .await;
    }

    async fn acquire_committed(&self, ctx: &mut SourcePolicyCtx) {
        self.inner
            .acquire_committed_permit_async(ctx.middleware_context_mut())
            .await;
    }
}

#[async_trait::async_trait]
impl SourcePolicy for RateLimiterSourcePolicy {
    fn label(&self) -> &'static str {
        self.inner.label()
    }

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        if self.charge_at == SourceRateLimitPosition::PrePoll || ctx.retry_physical_call() {
            self.reserve(ctx).await;
        }
        SourceAdmission::Admit(None)
    }

    fn commit_execution(&self, ctx: &mut SourcePolicyCtx) {
        if self.charge_at == SourceRateLimitPosition::PrePoll || ctx.retry_physical_call() {
            self.inner
                .commit_permit_reservations(ctx.middleware_context_mut());
        }
    }

    async fn after_poll(
        &self,
        batch: SourceBatchFacts,
        ctx: &mut SourcePolicyCtx,
    ) -> SourceAfterPoll {
        if self.charge_at == SourceRateLimitPosition::AfterPoll
            && !ctx.retry_physical_call()
            && !batch.has_error_marked
        {
            self.acquire_committed(ctx).await;
        }
        SourceAfterPoll::Proceed
    }

    fn observe(&self, _outcome: &SourcePollOutcome<'_>, ctx: &mut SourcePolicyCtx) {
        let middleware_ctx = ctx.middleware_context_mut();
        self.inner.maybe_emit_activity_pulse(middleware_ctx);
        self.inner.maybe_emit_summary(middleware_ctx);
    }
}

/// Sink-delivery boundary policy for the token-bucket rate limiter (FLOWIP-115d).
///
/// It awaits and reserves capacity before the sink consume operation, then
/// commits one token at delivery-executor start. A later policy rejection drops
/// and refunds the reservation without physical-call accounting.
pub(super) struct RateLimiterSinkPolicy {
    inner: Arc<RateLimiterMiddleware>,
}

impl RateLimiterSinkPolicy {
    pub(super) fn new(inner: Arc<RateLimiterMiddleware>) -> Self {
        Self { inner }
    }
}

#[async_trait::async_trait]
impl SinkPolicy for RateLimiterSinkPolicy {
    fn label(&self) -> &'static str {
        self.inner.label()
    }

    async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission {
        self.inner
            .reserve_permit_async(ctx.middleware_context_mut())
            .await;
        SinkAdmission::Admit(None)
    }

    fn commit_execution(&self, ctx: &mut SinkPolicyCtx) {
        self.inner
            .commit_permit_reservations(ctx.middleware_context_mut());
    }

    fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, ctx: &mut SinkPolicyCtx) {
        let middleware_ctx = ctx.middleware_context_mut();
        self.inner.maybe_emit_activity_pulse(middleware_ctx);
        self.inner.maybe_emit_summary(middleware_ctx);
    }
}

/// Ingress boundary policy for the token-bucket rate limiter (FLOWIP-115d).
///
/// Fail-fast admission at the hosted listener edge: it never waits for a token
/// while holding a listener request. A token exhaustion maps to a `RateLimited`
/// reject (`429`), never a wait. Edge-shed outcomes (not-ready, buffer-full,
/// channel-closed, overloaded) are owned by infra before this policy runs.
pub(super) struct RateLimiterIngressPolicy {
    inner: Arc<RateLimiterMiddleware>,
}

impl RateLimiterIngressPolicy {
    pub(super) fn new(inner: Arc<RateLimiterMiddleware>) -> Self {
        Self { inner }
    }
}

impl IngressBoundaryMiddleware for RateLimiterIngressPolicy {
    fn label(&self) -> &'static str {
        self.inner.label()
    }

    fn on_ingress(&self, attempt: &IngressAttemptContext) -> IngressAdmissionDecision {
        // FLOWIP-115d: charge one token per validation-accepted event so `/batch`
        // cannot bypass the shared admission scope used by `/events`.
        match self.inner.try_admit_ingress(attempt.event_count) {
            None => IngressAdmissionDecision::Accept,
            Some(retry_after) => IngressAdmissionDecision::Reject {
                retry_after: Some(retry_after),
            },
        }
    }

    fn observe(&self, _attempt: &IngressAttemptContext, _outcome: IngressAdmissionOutcome) {
        // FLOWIP-115d: ingress runs at the hosted edge, outside the supervisor,
        // so there is no boundary outbox to emit into here. Durable reject/shed
        // evidence is owned by the hosting evidence recorder (the DSL-to-hosting
        // wiring); live limiter telemetry is updated by the admission charge.
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_middleware;
    use super::*;
    use crate::middleware::PerSinkDeliveryPolicyBoundary;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::{ChainEventFactory, WriterId};
    use obzenflow_core::{EventId, StageId};
    use obzenflow_runtime::stages::common::handlers::SinkConsumeReport;
    use obzenflow_runtime::stages::sink::journal_sink::{
        SinkDeliveryAttemptOutcome, SinkDeliveryBoundary, SinkDeliveryBoundaryOutcome,
        SinkDeliveryExecutor,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    struct RejectAfterLimiter;

    #[async_trait::async_trait]
    impl SinkPolicy for RejectAfterLimiter {
        fn label(&self) -> &'static str {
            "reject_after_limiter"
        }

        async fn admit(&self, _ctx: &mut SinkPolicyCtx) -> SinkAdmission {
            SinkAdmission::Reject {
                reason: "later policy rejected".to_string(),
            }
        }

        fn observe(&self, _outcome: &SinkDeliveryPolicyOutcome<'_>, _ctx: &mut SinkPolicyCtx) {}
    }

    struct NeverExecute {
        parent_event_id: EventId,
    }

    struct SuccessfulExecute {
        parent_event_id: EventId,
        calls: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl SinkDeliveryExecutor for NeverExecute {
        fn parent_event_id(&self) -> EventId {
            self.parent_event_id
        }

        async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
            panic!("later-policy rejection must prevent executor start")
        }
    }

    #[async_trait::async_trait]
    impl SinkDeliveryExecutor for SuccessfulExecute {
        fn parent_event_id(&self) -> EventId {
            self.parent_event_id
        }

        async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
            self.calls.fetch_add(1, Ordering::SeqCst);
            SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(SinkConsumeReport::new(
                DeliveryPayload::success(DeliveryMethod::Noop, None),
            ))))
        }
    }

    #[tokio::test]
    async fn later_sink_policy_rejection_refunds_uncommitted_limiter_reservation() {
        let limiter = Arc::new(test_middleware(StageId::new(), 10.0, Some(1.0), 1.0));
        let rate_policy: Arc<dyn SinkPolicy> =
            Arc::new(RateLimiterSinkPolicy::new(limiter.clone()));
        let reject_policy: Arc<dyn SinkPolicy> = Arc::new(RejectAfterLimiter);
        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![rate_policy, reject_policy]);
        let mut execute = NeverExecute {
            parent_event_id: EventId::new(),
        };

        let report = boundary.around_sink_delivery(&mut execute).await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Rejected(_)
        ));
        assert_eq!(
            limiter.core.bucket_for_test().lock().unwrap().tokens,
            1.0,
            "NotExecuted settlement/context drop must refund the reservation"
        );
        let stats = limiter.core.stats_for_test().lock().unwrap();
        assert_eq!(stats.events_total, 0);
        assert_eq!(stats.tokens_consumed_total, 0.0);
    }

    #[tokio::test]
    async fn sink_executor_start_commits_reservation_once() {
        let limiter = Arc::new(test_middleware(StageId::new(), 10.0, Some(1.0), 1.0));
        let policy = RateLimiterSinkPolicy::new(limiter.clone());
        let mut ctx = SinkPolicyCtx::new();

        assert!(matches!(
            policy.admit(&mut ctx).await,
            SinkAdmission::Admit(_)
        ));
        assert_eq!(
            limiter.core.stats_for_test().lock().unwrap().events_total,
            0,
            "admission must only reserve"
        );

        policy.commit_execution(&mut ctx);
        policy.commit_execution(&mut ctx);
        drop(ctx);

        let stats = limiter.core.stats_for_test().lock().unwrap();
        assert_eq!(stats.events_total, 1);
        assert_eq!(stats.tokens_consumed_total, 1.0);
        drop(stats);
        assert_eq!(
            limiter.core.bucket_for_test().lock().unwrap().tokens,
            0.0,
            "a committed physical call remains charged after context drop"
        );
    }

    #[tokio::test]
    async fn sink_boundary_commits_limiter_immediately_before_executor() {
        let limiter = Arc::new(test_middleware(StageId::new(), 10.0, Some(1.0), 1.0));
        let rate_policy: Arc<dyn SinkPolicy> =
            Arc::new(RateLimiterSinkPolicy::new(limiter.clone()));
        let boundary = PerSinkDeliveryPolicyBoundary::new(vec![rate_policy]);
        let calls = Arc::new(AtomicUsize::new(0));
        let mut execute = SuccessfulExecute {
            parent_event_id: EventId::new(),
            calls: calls.clone(),
        };

        let report = boundary.around_sink_delivery(&mut execute).await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let stats = limiter.core.stats_for_test().lock().unwrap();
        assert_eq!(stats.events_total, 1);
        assert_eq!(stats.tokens_consumed_total, 1.0);
    }

    /// FLOWIP-115a / FLOWIP-114m: the source-boundary rate-limiter policy charges
    /// a permit only at its declared position, and never on an error-marked
    /// delivery. Asserted in isolation through the monotonic
    /// `tokens_consumed_total`, which the admission path bumps by `cost_per_event`
    /// on every charge (refill-independent, unlike `available_tokens`).
    #[tokio::test]
    async fn source_rate_limiter_policy_charges_by_position_and_skips_error_marked() {
        use obzenflow_core::event::status::processing_status::ErrorKind;

        let writer = WriterId::from(StageId::new());
        let clean_batch = vec![ChainEventFactory::data_event(
            writer,
            "test.event",
            json!({ "index": 0 }),
        )];
        let error_batch =
            vec![
                ChainEventFactory::data_event(writer, "test.event", json!({ "index": 0 }))
                    .mark_as_error("boom", ErrorKind::Remote),
            ];

        let clean_facts = SourceBatchFacts::from_events(&clean_batch);
        let error_facts = SourceBatchFacts::from_events(&error_batch);
        assert!(!clean_facts.has_error_marked);
        assert!(error_facts.has_error_marked);

        let charged = |mw: &Arc<RateLimiterMiddleware>| {
            mw.core
                .stats_for_test()
                .lock()
                .expect("stats lock")
                .tokens_consumed_total
        };

        // Finite source (AfterPoll): admit is a no-op; after_poll charges a clean
        // non-empty delivery.
        let finite = Arc::new(test_middleware(StageId::new(), 100.0, Some(5.0), 1.0));
        let finite_policy =
            RateLimiterSourcePolicy::new(finite.clone(), SourceRateLimitPosition::AfterPoll);
        let mut ctx = SourcePolicyCtx::new(writer);
        let _ = finite_policy.admit(&mut ctx).await;
        assert_eq!(charged(&finite), 0.0, "AfterPoll admit must not charge");
        finite_policy.after_poll(clean_facts, &mut ctx).await;
        assert_eq!(
            charged(&finite),
            1.0,
            "AfterPoll must charge a clean non-empty delivery"
        );

        // Error-marked delivery: after_poll must not charge.
        let finite_err = Arc::new(test_middleware(StageId::new(), 100.0, Some(5.0), 1.0));
        let finite_err_policy =
            RateLimiterSourcePolicy::new(finite_err.clone(), SourceRateLimitPosition::AfterPoll);
        let mut ctx_err = SourcePolicyCtx::new(writer);
        finite_err_policy
            .after_poll(error_facts, &mut ctx_err)
            .await;
        assert_eq!(
            charged(&finite_err),
            0.0,
            "FLOWIP-114m: an error-marked delivery must not be charged"
        );

        // Infinite source (PrePoll): admission reserves, executor start
        // commits, and after_poll is a no-op.
        let infinite = Arc::new(test_middleware(StageId::new(), 100.0, Some(5.0), 1.0));
        let infinite_policy =
            RateLimiterSourcePolicy::new(infinite.clone(), SourceRateLimitPosition::PrePoll);
        let mut ctx_inf = SourcePolicyCtx::new(writer);
        let _ = infinite_policy.admit(&mut ctx_inf).await;
        assert_eq!(
            charged(&infinite),
            0.0,
            "PrePoll admit reserves but must not charge before executor start"
        );
        infinite_policy.commit_execution(&mut ctx_inf);
        assert_eq!(
            charged(&infinite),
            1.0,
            "PrePoll executor start must commit the charge"
        );
        infinite_policy.after_poll(clean_facts, &mut ctx_inf).await;
        assert_eq!(
            charged(&infinite),
            1.0,
            "PrePoll after_poll must be a no-op"
        );
    }

    #[tokio::test]
    async fn finite_source_retry_charges_each_physical_poll_at_executor_start() {
        let writer = WriterId::from(StageId::new());
        let limiter = Arc::new(test_middleware(StageId::new(), 100.0, Some(5.0), 1.0));
        let policy =
            RateLimiterSourcePolicy::new(limiter.clone(), SourceRateLimitPosition::AfterPoll);
        let mut ctx = SourcePolicyCtx::new_retry_physical_call(writer);

        assert!(matches!(
            policy.admit(&mut ctx).await,
            SourceAdmission::Admit(_)
        ));
        assert_eq!(
            limiter.core.stats_for_test().lock().unwrap().events_total,
            0,
            "retry admission reserves without charging"
        );

        policy.commit_execution(&mut ctx);
        assert_eq!(
            limiter.core.stats_for_test().lock().unwrap().events_total,
            1,
            "physical poll start commits exactly one charge even if the poll fails"
        );

        let batch = SourceBatchFacts {
            event_count: 1,
            has_error_marked: false,
        };
        policy.after_poll(batch, &mut ctx).await;
        assert_eq!(
            limiter.core.stats_for_test().lock().unwrap().events_total,
            1,
            "retry-mode settlement must not double-charge a successful batch"
        );
    }

    /// FLOWIP-120c (the FLOWIP-114o boundary slice): the per-effect policy
    /// admission awaits its permit instead of blocking the worker thread.
    /// On a current-thread runtime a blocking wait would starve the side
    /// task until admission finished; an awaited wait lets it run first.
    #[tokio::test]
    async fn effect_policy_admit_awaits_permit_without_blocking_the_runtime() {
        use crate::middleware::{EffectPolicy, PolicyAdmission};

        let limiter = test_middleware(StageId::new(), 10.0, Some(1.0), 1.0);
        let mut ctx = MiddlewareContext::with_scope(
            obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
        );

        // The single burst token reserves immediately, then executor start
        // commits its physical-call accounting.
        assert!(matches!(
            limiter.admit(&mut ctx).await,
            PolicyAdmission::Admit
        ));
        assert_eq!(
            limiter.core.stats_for_test().lock().unwrap().events_total,
            0
        );
        limiter.commit_execution(&mut ctx);

        // The second admission waits ~100ms for the refill. A side task on
        // the same current-thread runtime must make progress during the wait.
        let order: Arc<std::sync::Mutex<Vec<&'static str>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let side_order = order.clone();
        let side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            side_order.lock().unwrap().push("side");
        });

        let admission = limiter.admit(&mut ctx).await;
        order.lock().unwrap().push("admitted");
        side.await.expect("side task completes");

        assert!(matches!(admission, PolicyAdmission::Admit));
        limiter.commit_execution(&mut ctx);
        assert_eq!(
            order.lock().unwrap().as_slice(),
            ["side", "admitted"],
            "the side task must run while admission awaits its permit"
        );

        let stats = limiter.core.stats_for_test().lock().unwrap();
        assert_eq!(stats.events_total, 2);
        assert_eq!(stats.delayed_total, 1);
        assert!(stats.delay_seconds_total > 0.0);
    }
}
