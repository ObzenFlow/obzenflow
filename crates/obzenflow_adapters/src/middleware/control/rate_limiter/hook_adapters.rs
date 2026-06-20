// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: carrier-bound surface adapters for the token-bucket rate
//! limiter.
//!
//! Each adapter is a thin shim that turns a live-I/O boundary attempt into an
//! admission charge against the shared [`super::RateLimiterMiddleware`] (and its
//! [`super::admission_core::RateLimiterCore`]). The source, effect, and
//! sink-delivery adapters await their permit through the shared cancellable
//! acquisition helper; the ingress adapter is fail-fast at the hosted listener
//! edge. The residual generic [`Middleware`] shell performs one non-blocking
//! admission check and is unreachable from production placement (FLOWIP-115d
//! AC10/AC50); production placement goes through [`super::factory`].

use super::admission_core::AdmissionDecision;
use super::RateLimiterMiddleware;
use crate::middleware::{
    batch_has_error_marked, ErrorAction, Middleware, MiddlewareAction, MiddlewareContext,
    SinkAdmission, SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx, SourceAdmission,
    SourceAfterPoll, SourceMiddlewarePhase, SourcePolicy, SourcePolicyCtx, SourcePollOutcome,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::ingress::{
    IngressAdmissionDecision, IngressAdmissionOutcome, IngressAttemptContext,
    IngressBoundaryMiddleware,
};
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptContext, SinkDeliveryIdentity,
};
use std::sync::Arc;
use tokio::time::Instant;

impl Middleware for RateLimiterMiddleware {
    fn label(&self) -> &'static str {
        "rate_limiter"
    }

    // FLOWIP-115d: placement is carrier-driven (the limiter materializes onto the
    // SourcePoll/Effect/SinkDelivery/Ingress surfaces), so it no longer claims a
    // special source-ordering phase or exposes downcast hooks. `source_phase` is a
    // required trait method, so it returns the neutral `Ordinary`.
    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Policy
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // FLOWIP-120a: during deterministic replay the stage is reconstructed
        // from recorded events and performs no live external admission, so the
        // limiter must not consume a token, block, mark the event delayed, mutate
        // admission state, or emit a lifecycle record. Replay reproduces identical
        // values; pacing it would only change timing, and any delay/utilization
        // records already exist in the archive being replayed.
        if ctx.execution_scope().is_deterministic_replay() {
            return MiddlewareAction::Continue;
        }

        if event.is_control() || event.is_lifecycle() {
            return MiddlewareAction::Continue;
        }

        // FLOWIP-115d AC10/AC50: the generic handler-shell path must never block a
        // worker thread. Hook-bound source, effect, and sink-delivery placement
        // own paced (cancellable) admission; handler pacing is retired, and
        // production placement on a handler shell is rejected at binding
        // (FLOWIP-115j owns any future hook-bound handler pacing). This residual
        // shell path performs a single non-blocking admission check and never
        // sleeps.
        let cost = self.core.cost_per_event();
        if let AdmissionDecision::Admitted = self.core.try_admit_at(cost, Instant::now()) {
            self.core.record_admitted(cost);
        }
        MiddlewareAction::Continue
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _outputs: &[ChainEvent],
        ctx: &mut MiddlewareContext,
    ) {
        // FLOWIP-120a: replay reconstruction performs no live admission, so the
        // periodic activity-pulse and window-summary emissions (which also reset
        // window counters and move limiter mode state) must be suppressed.
        if ctx.execution_scope().is_deterministic_replay() {
            return;
        }
        self.maybe_emit_activity_pulse(ctx);
        self.maybe_emit_summary(ctx);
    }

    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Don't consume tokens for errors
        ErrorAction::Propagate
    }
}

/// Per-effect policy adapter (FLOWIP-120c): one limiter instance guards one
/// declared effect. It awaits its permit at the live effect boundary instead
/// of blocking a worker thread, while reusing the same `RateLimiterCore`,
/// accounting, and lifecycle-event helpers as the synchronous `Middleware`
/// implementation.
#[async_trait::async_trait]
impl crate::middleware::EffectPolicy for RateLimiterMiddleware {
    fn label(&self) -> &'static str {
        Middleware::label(self)
    }

    async fn admit(
        &self,
        _identity: &obzenflow_runtime::effects::EffectIdentity,
        _event: &ChainEvent,
        ctx: &mut MiddlewareContext,
    ) -> crate::middleware::PolicyAdmission {
        // FLOWIP-114o: shared with the source pacing path. Replay suppression at
        // the effect boundary happens before `admit` is consulted, so this path
        // carries no scope guard of its own.
        self.acquire_permit_async(ctx).await;
        crate::middleware::PolicyAdmission::Admit
    }

    fn observe(
        &self,
        _identity: &obzenflow_runtime::effects::EffectIdentity,
        _event: &ChainEvent,
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
/// It reuses [`RateLimiterMiddleware::acquire_permit_async`], so source pacing
/// waits internally with a cancellable Tokio sleep, matching FLOWIP-114o. Finite
/// sources charge after a clean non-empty delivery; infinite sources charge
/// before polling.
pub(super) struct RateLimiterSourcePolicy {
    inner: Arc<RateLimiterMiddleware>,
    charge_at: SourceRateLimitPosition,
}

impl RateLimiterSourcePolicy {
    pub(super) fn new(inner: Arc<RateLimiterMiddleware>, charge_at: SourceRateLimitPosition) -> Self {
        Self { inner, charge_at }
    }

    async fn acquire(&self, ctx: &mut SourcePolicyCtx) {
        // FLOWIP-115d: the core admits by protected unit and typed metadata, so
        // the source path no longer fabricates a synthetic `ChainEvent` to drive
        // admission.
        self.inner
            .acquire_permit_async(ctx.middleware_context_mut())
            .await;
    }
}

#[async_trait::async_trait]
impl SourcePolicy for RateLimiterSourcePolicy {
    fn label(&self) -> &'static str {
        Middleware::label(self.inner.as_ref())
    }

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        if self.charge_at == SourceRateLimitPosition::PrePoll {
            self.acquire(ctx).await;
        }
        SourceAdmission::Admit(None)
    }

    async fn after_poll(&self, batch: &[ChainEvent], ctx: &mut SourcePolicyCtx) -> SourceAfterPoll {
        if self.charge_at == SourceRateLimitPosition::AfterPoll && !batch_has_error_marked(batch) {
            self.acquire(ctx).await;
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
/// It awaits a permit before the sink consume operation through the shared
/// cancellable acquisition helper, protecting the delivery unit rather than the
/// event's prior transformation. The typed delivery identity and attempt
/// context are available for proof rows; the v1 limiter charges one token per
/// delivery attempt regardless of payload.
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
        Middleware::label(self.inner.as_ref())
    }

    async fn admit(
        &self,
        _identity: &SinkDeliveryIdentity,
        _attempt: &SinkDeliveryAttemptContext,
        ctx: &mut SinkPolicyCtx,
    ) -> SinkAdmission {
        self.inner
            .acquire_permit_async(ctx.middleware_context_mut())
            .await;
        SinkAdmission::Admit(None)
    }

    fn observe(
        &self,
        _identity: &SinkDeliveryIdentity,
        _attempt: &SinkDeliveryAttemptContext,
        _outcome: &SinkDeliveryPolicyOutcome<'_>,
        ctx: &mut SinkPolicyCtx,
    ) {
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
        Middleware::label(self.inner.as_ref())
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
    use obzenflow_core::event::{ChainEventFactory, WriterId};
    use obzenflow_core::StageId;
    use serde_json::json;
    use std::time::Duration;

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

        // The rules read raw facts, so an error-marked batch is an error delivery.
        assert!(batch_has_error_marked(&error_batch));
        assert!(!batch_has_error_marked(&clean_batch));

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
        finite_policy.after_poll(&clean_batch, &mut ctx).await;
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
            .after_poll(&error_batch, &mut ctx_err)
            .await;
        assert_eq!(
            charged(&finite_err),
            0.0,
            "FLOWIP-114m: an error-marked delivery must not be charged"
        );

        // Infinite source (PrePoll): admit charges; after_poll is a no-op.
        let infinite = Arc::new(test_middleware(StageId::new(), 100.0, Some(5.0), 1.0));
        let infinite_policy =
            RateLimiterSourcePolicy::new(infinite.clone(), SourceRateLimitPosition::PrePoll);
        let mut ctx_inf = SourcePolicyCtx::new(writer);
        let _ = infinite_policy.admit(&mut ctx_inf).await;
        assert_eq!(charged(&infinite), 1.0, "PrePoll admit must charge");
        infinite_policy.after_poll(&clean_batch, &mut ctx_inf).await;
        assert_eq!(
            charged(&infinite),
            1.0,
            "PrePoll after_poll must be a no-op"
        );
    }

    /// FLOWIP-120c (the FLOWIP-114o boundary slice): the per-effect policy
    /// admission awaits its permit instead of blocking the worker thread.
    /// On a current-thread runtime a blocking wait would starve the side
    /// task until admission finished; an awaited wait lets it run first.
    #[tokio::test]
    async fn effect_policy_admit_awaits_permit_without_blocking_the_runtime() {
        use crate::middleware::{EffectPolicy, PolicyAdmission};
        use obzenflow_runtime::effects::{EffectCursor, EffectIdentity, EffectSafety};

        let limiter = test_middleware(StageId::new(), 10.0, Some(1.0), 1.0);
        let identity = EffectIdentity {
            effect_type: "test.effect",
            safety: EffectSafety::Idempotent,
            cursor: EffectCursor::new("test_flow", "test_stage", 1, 0),
            idempotency_key: None,
        };
        let event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.input", json!({}));
        let mut ctx = MiddlewareContext::with_scope(
            obzenflow_core::MiddlewareExecutionScope::LiveEffectBoundary,
        );

        // The single burst token admits immediately.
        assert!(matches!(
            limiter.admit(&identity, &event, &mut ctx).await,
            PolicyAdmission::Admit
        ));

        // The second admission waits ~100ms for the refill. A side task on
        // the same current-thread runtime must make progress during the wait.
        let order: Arc<std::sync::Mutex<Vec<&'static str>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let side_order = order.clone();
        let side = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            side_order.lock().unwrap().push("side");
        });

        let admission = limiter.admit(&identity, &event, &mut ctx).await;
        order.lock().unwrap().push("admitted");
        side.await.expect("side task completes");

        assert!(matches!(admission, PolicyAdmission::Admit));
        assert_eq!(
            order.lock().unwrap().as_slice(),
            ["side", "admitted"],
            "the side task must run while admission awaits its permit"
        );

        let stats = limiter.core.stats_for_test().lock().unwrap();
        assert_eq!(stats.delayed_total, 1);
        assert!(stats.delay_seconds_total > 0.0);
    }
}
