// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rate limiting middleware: a token-bucket limiter that creates natural
//! backpressure when out of tokens, ensuring no events are lost.
//!
//! ## Module layout (FLOWIP-115d)
//!
//! The surface-neutral admission core lives in [`admission_core`] (`RateLimiterCore`,
//! the token bucket, counters, deadline calculation, summary/pulse window
//! state, snapshot projection, and the shared cancellable `acquire_admission`
//! helper). Configuration validation lives in [`config`]. This root module
//! holds the `Middleware`/`EffectPolicy`/source-pacing shells that turn core
//! admission decisions into `ChainEvent` lifecycle facts, plus the public
//! builder and factory. The carrier-bound source/effect/sink/ingress adapters
//! land in FLOWIP-115d's later phases.
//!
//! ## Wait mechanism by placement (FLOWIP-114o)
//!
//! The limiter waits in one of two ways, depending on where it is attached.
//! Async live I/O units (the effect boundary via [`crate::middleware::EffectPolicy`]
//! and the source boundary via [`crate::middleware::SourcePolicy`]) await their permit
//! through the shared [`admission_core::acquire_admission`] helper with `tokio::time::sleep`,
//! a cancellable future, so a drain, EOF, cancellation, or shutdown abandons an
//! in-flight wait. Sync handler chains and sync sources keep the blocking
//! `pre_handle` loop (it blocks via `block_in_place` off the worker), retained
//! and documented as a known limitation; async sources are the recommended path
//! for cancellable paced ingestion. All paths share one token-bucket,
//! accounting, and lifecycle-event implementation.
//!
//! ## No global bucket (FLOWIP-114o Q6, FLOWIP-050d)
//!
//! Each limiter instance owns its own `RateLimiterCore`. Flow-level
//! `rate_limit(N)` materialises one instance per stage, and attaching the same
//! builder to a source and to an effect yields independent buckets. There is
//! deliberately no process-wide shared bucket. Quotas are per protected
//! dependency, matching the FLOWIP-120c per-effect model.
//!
//! ## Admission accounting (FLOWIP-114m, FLOWIP-114o)
//!
//! Counters increment at admission, not at committed output. On finite sources
//! the limiter charges only successful non-empty batches; EOF, empty batches,
//! and source errors consume no token, increment no admission counter, and emit
//! no `Delayed` event. On async sources a drain may abandon an in-flight wait
//! after the one-shot `Delayed` event has been emitted but before a token is
//! consumed, so the abandoned poll charges no token; `delayed_total` and
//! `events_total` are independent counters.

mod admission_core;
mod config;

use crate::middleware::{
    batch_has_error_marked, SourceAdmission, SourceAfterPoll, SourcePolicy, SourcePolicyCtx,
    SourcePollOutcome,
};
use crate::middleware::{
    validate_attachment_request, ControlMiddlewareRole, EffectPolicy, EffectTypeKey, ErrorAction,
    Middleware, MiddlewareAction, MiddlewareAttachmentRequest, MiddlewareContext,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewarePlanContribution,
    MiddlewareSafety, MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
    SinkAdmission, SinkDeliveryPolicyOutcome, SinkPolicy, SinkPolicyCtx, SourceMiddlewarePhase,
    SourcePollAttachment, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
};
use obzenflow_core::ingress::{
    IngressAdmissionDecision, IngressAdmissionOutcome, IngressAttemptContext,
    IngressBoundaryMiddleware,
};
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::control_plane::{RateLimiterMetrics, RateLimiterSnapshotter};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::sink::journal_sink::{
    SinkDeliveryAttemptContext, SinkDeliveryIdentity,
};
use std::sync::Arc;
// FLOWIP-114o: the limiter's token-bucket refill and stats windows read
// `tokio::time::Instant` so Tokio paused time advances them in deterministic
// tests. Under a normal runtime this tracks real time; outside a runtime
// `Instant::now()` falls back to the std clock. `Duration` stays from std.
use tokio::time::Instant;
use tracing::info;

use admission_core::{acquire_admission, AdmissionDecision, RateLimitDelayEvent, RateLimiterCore};
use config::{
    validated_rate_limiter_config, RateLimiterConfigError, ValidatedRateLimiterConfig,
    DEFAULT_COST_PER_EVENT,
};

pub struct RateLimiterFamily;

/// Build the durable observability `ChainEvent` for one rate-limiter lifecycle
/// fact. Lives in the shell because the core is `ChainEvent`-free.
fn rate_limiter_event(writer_id: WriterId, event: RateLimiterEvent) -> ChainEvent {
    ChainEventFactory::observability_event(
        writer_id,
        ObservabilityPayload::Middleware(MiddlewareLifecycle::RateLimiter(event)),
    )
}

fn delayed_event(writer_id: WriterId, info: RateLimitDelayEvent) -> ChainEvent {
    rate_limiter_event(
        writer_id,
        RateLimiterEvent::Delayed {
            delay_ms: info.delay_ms,
            current_rate: info.current_rate,
            limit_rate: info.limit_rate,
        },
    )
}

/// Rate limiting middleware using token bucket algorithm.
///
/// A thin shell over [`RateLimiterCore`]: it owns the writer identity and turns
/// the core's plain-data admission and summary outputs into durable
/// `ChainEvent` lifecycle facts.
pub struct RateLimiterMiddleware {
    core: Arc<RateLimiterCore>,
    /// Writer identity used for durable observability/control events.
    ///
    /// This must match the stage's writer_id so vector-clock watermarks and
    /// stage attribution remain correct in downstream consumers.
    writer_id: WriterId,
}

impl RateLimiterMiddleware {
    fn new(
        stage_id: StageId,
        config: ValidatedRateLimiterConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> Self {
        Self::new_keyed(stage_id, config, control_middleware, None)
    }

    /// Construct a limiter registered under a per-effect key (FLOWIP-120c):
    /// one policy instance per protected dependency.
    fn new_keyed(
        stage_id: StageId,
        config: ValidatedRateLimiterConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
        effect_type: Option<EffectTypeKey>,
    ) -> Self {
        // FLOWIP-120i: under strict replay the limiter is constructed because
        // topology and contracts must match the recorded run, but it consumes
        // no live tokens and moves no admission state. The setup log must not
        // read like live policy activity.
        let strict_replay = crate::middleware::strict_replay_active();
        info!(
            events_per_second = config.events_per_second,
            burst_capacity = config.burst_capacity,
            cost_per_event = config.cost_per_event,
            initial_tokens = config.burst_capacity,
            run_mode = if strict_replay { "replay" } else { "live" },
            strict_replay,
            "{}",
            if strict_replay {
                "Created rate limiter middleware (configured for topology validation; live accounting suppressed)"
            } else {
                "Created rate limiter middleware"
            }
        );

        let core = Arc::new(RateLimiterCore::new(config));

        let snapshotter: std::sync::Arc<RateLimiterSnapshotter> = Arc::new({
            let core = core.clone();
            move || {
                let snap = core.snapshot();
                RateLimiterMetrics {
                    events_total: snap.events_total,
                    delayed_total: snap.delayed_total,
                    tokens_consumed_total: snap.tokens_consumed_total,
                    delay_seconds_total: snap.delay_seconds_total,
                    bucket_tokens: snap.bucket_tokens,
                    bucket_capacity: snap.bucket_capacity,
                }
            }
        });
        match effect_type {
            Some(effect_type) => control_middleware.register_rate_limiter_for_effect(
                stage_id,
                effect_type,
                snapshotter,
            ),
            None => control_middleware.register_rate_limiter(stage_id, snapshotter),
        }

        Self {
            core,
            writer_id: WriterId::from(stage_id),
        }
    }

    fn limit_rate(&self) -> f64 {
        self.core.limit_rate()
    }

    fn maybe_emit_activity_pulse(&self, ctx: &mut MiddlewareContext) {
        if let Some(pulse) = self.core.take_due_pulse(Instant::now()) {
            ctx.write_control_event(rate_limiter_event(
                self.writer_id,
                RateLimiterEvent::ActivityPulse {
                    window_ms: pulse.window_ms,
                    delayed_events: pulse.delayed_events,
                    delay_ms_total: pulse.delay_ms_total,
                    delay_ms_max: pulse.delay_ms_max,
                    limit_rate: self.limit_rate(),
                },
            ));
        }
    }

    /// Check if we should emit a summary and do so if needed.
    fn maybe_emit_summary(&self, ctx: &mut MiddlewareContext) {
        if let Some(summary) = self.core.take_due_summary(Instant::now()) {
            if let Some((from, to)) = summary.mode_change {
                ctx.write_control_event(rate_limiter_event(
                    self.writer_id,
                    RateLimiterEvent::ModeChange {
                        mode_from: from.to_string(),
                        mode_to: to.to_string(),
                        limit_rate: self.limit_rate(),
                    },
                ));
            }
            ctx.write_control_event(rate_limiter_event(
                self.writer_id,
                RateLimiterEvent::WindowUtilization {
                    utilization_percent: summary.utilization_percent,
                    events_in_window: summary.events_in_window,
                    window_size_ms: summary.window_size_ms,
                },
            ));
        }
    }

    /// Shared async token-bucket acquisition. Both the effect-boundary
    /// `EffectPolicy::admit` and the source-boundary `SourcePolicy` path
    /// delegate here through [`core::acquire_admission`], so the two live I/O
    /// surfaces share one accounting and lifecycle-event implementation. It
    /// awaits its permit with a cancellable future instead of blocking the
    /// worker thread. The synchronous `Middleware::pre_handle` keeps its own
    /// blocking loop for sync handler chains and sync sources.
    async fn acquire_permit_async(&self, ctx: &mut MiddlewareContext) {
        let writer_id = self.writer_id;
        acquire_admission(&self.core, |info: RateLimitDelayEvent| {
            ctx.write_control_event(delayed_event(writer_id, info));
        })
        .await;
    }

    /// FLOWIP-115d: fail-fast ingress admission. Non-blocking: it never waits for
    /// a token while a listener request is held. Charges one token per
    /// validation-accepted event on accept. Returns `None` when admitted (token
    /// consumed) or `Some(retry_after)` when the token bucket would wait.
    fn try_admit_ingress(&self, event_count: u64) -> Option<std::time::Duration> {
        let cost = (event_count.max(1)) as f64 * self.core.cost_per_event();
        match self.core.try_admit_at(cost, Instant::now()) {
            AdmissionDecision::Admitted => {
                self.core.record_admitted(cost);
                None
            }
            AdmissionDecision::WouldWait { retry_after } => Some(retry_after),
        }
    }
}

impl Middleware for RateLimiterMiddleware {
    fn label(&self) -> &'static str {
        "rate_limiter"
    }

    // FLOWIP-115d: placement is carrier-driven (the limiter materializes onto the
    // SourcePoll/Effect/SinkDelivery/Ingress surfaces), so it no longer claims a
    // special source-ordering phase or exposes the `as_effect_policy` /
    // `as_source_pacer` downcast hooks. `source_phase` is a required trait method,
    // so it returns the neutral `Ordinary`.
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
enum SourceRateLimitPosition {
    PrePoll,
    AfterPoll,
}

/// Source-boundary policy for the token-bucket rate limiter (FLOWIP-115a).
///
/// It reuses [`RateLimiterMiddleware::acquire_permit_async`], so source pacing
/// waits internally with a cancellable Tokio sleep, matching FLOWIP-114o. Finite
/// sources charge after a clean non-empty delivery; infinite sources charge
/// before polling.
struct RateLimiterSourcePolicy {
    inner: Arc<RateLimiterMiddleware>,
    charge_at: SourceRateLimitPosition,
}

impl RateLimiterSourcePolicy {
    fn new(inner: Arc<RateLimiterMiddleware>, charge_at: SourceRateLimitPosition) -> Self {
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
/// event's prior transformation. The typed delivery identity, attempt context,
/// and parent event are available for proof rows; the v1 limiter charges one
/// token per delivery attempt regardless of payload.
struct RateLimiterSinkPolicy {
    inner: Arc<RateLimiterMiddleware>,
}

impl RateLimiterSinkPolicy {
    fn new(inner: Arc<RateLimiterMiddleware>) -> Self {
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
        _event: &ChainEvent,
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
        _event: &ChainEvent,
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
struct RateLimiterIngressPolicy {
    inner: Arc<RateLimiterMiddleware>,
}

impl RateLimiterIngressPolicy {
    fn new(inner: Arc<RateLimiterMiddleware>) -> Self {
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

/// Builder for constructing rate limiter middleware factories.
///
/// Every factory built here produces independent per-attachment buckets; see the
/// module docs for the no-global-bucket decision (FLOWIP-114o Q6), the
/// async-await vs blocking wait by placement, and the admission accounting
/// contract.
#[derive(Clone)]
pub struct RateLimiterBuilder {
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
}

impl RateLimiterBuilder {
    /// Create a basic rate limiter builder.
    pub fn new(events_per_second: f64) -> Self {
        Self {
            events_per_second,
            burst_capacity: None,
            cost_per_event: DEFAULT_COST_PER_EVENT,
        }
    }

    /// Set burst capacity (defaults to `events_per_second`).
    pub fn with_burst(mut self, capacity: f64) -> Self {
        self.burst_capacity = Some(capacity);
        self
    }

    /// Set cost per event (for weighted rate limiting).
    pub fn with_cost_per_event(mut self, cost: f64) -> Self {
        self.cost_per_event = cost;
        self
    }

    /// Backward-compatible alias for `with_cost_per_event`.
    pub fn with_cost(self, cost: f64) -> Self {
        self.with_cost_per_event(cost)
    }

    /// Build the boxed middleware factory.
    pub fn build(self) -> Box<dyn MiddlewareFactory> {
        Box::new(RateLimiterFactory::from(self))
    }
}

/// Factory for creating rate limiter middleware.
///
/// `RateLimiterFactory` remains available for direct construction and testing,
/// but caller-facing configuration should prefer `RateLimiterBuilder`.
#[derive(Clone)]
pub struct RateLimiterFactory {
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
}

impl RateLimiterFactory {
    /// Create a basic rate limiter
    pub fn new(events_per_second: f64) -> Self {
        Self {
            events_per_second,
            burst_capacity: None,
            cost_per_event: DEFAULT_COST_PER_EVENT,
        }
    }

    /// Set burst capacity (defaults to events_per_second)
    pub fn with_burst(mut self, capacity: f64) -> Self {
        self.burst_capacity = Some(capacity);
        self
    }

    /// Set cost per event (for weighted rate limiting).
    pub fn with_cost_per_event(mut self, cost: f64) -> Self {
        self.cost_per_event = cost;
        self
    }

    /// Backward-compatible alias for `with_cost_per_event`.
    pub fn with_cost(self, cost: f64) -> Self {
        self.with_cost_per_event(cost)
    }

    fn validated_config(&self) -> Result<ValidatedRateLimiterConfig, RateLimiterConfigError> {
        validated_rate_limiter_config(
            self.events_per_second,
            self.burst_capacity,
            self.cost_per_event,
        )
    }
}

impl From<RateLimiterBuilder> for RateLimiterFactory {
    fn from(builder: RateLimiterBuilder) -> Self {
        Self {
            events_per_second: builder.events_per_second,
            burst_capacity: builder.burst_capacity,
            cost_per_event: builder.cost_per_event,
        }
    }
}

impl MiddlewareFactory for RateLimiterFactory {
    fn label(&self) -> &'static str {
        "rate_limiter"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<RateLimiterFamily>("rate_limiter")
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        // FLOWIP-115d (AC45): placement is carrier-driven; the hook-bound rate
        // limiter no longer routes through the legacy role. Topology and
        // instrumentation metadata use the role-independent `topology_config_slot`
        // signal instead.
        ControlMiddlewareRole::None
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Policy
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(TopologyMiddlewareConfigSlot::RateLimiter)
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        // FLOWIP-115d: the rate limiter is hook-bound control middleware that
        // attaches to the live-I/O boundary surfaces. The binder picks the
        // concrete surface per call site and routes it through `materialize`.
        MiddlewareDeclaration::control_with_family(
            self.label(),
            self.override_key().family_label(),
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
                MiddlewareSurfaceKind::SinkDelivery,
                MiddlewareSurfaceKind::Ingress,
            ],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let declaration = self.declaration();
        let _attachment_id =
            validate_attachment_request(&declaration, &request).map_err(|err| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    err,
                )
            })?;

        let validated = self.validated_config().map_err(|err| {
            MiddlewareFactoryError::invalid_configuration(self.label(), &context.config.name, err)
        })?;

        match request.surface {
            MiddlewareSurface::SourcePoll(_) => {
                // FLOWIP-114m: an infinite source paces pre-poll; a finite source
                // charges after a clean non-empty delivery.
                let charge_at = match context.stage_type {
                    StageType::InfiniteSource => SourceRateLimitPosition::PrePoll,
                    _ => SourceRateLimitPosition::AfterPoll,
                };
                let middleware = Arc::new(RateLimiterMiddleware::new(
                    context.config.stage_id,
                    validated,
                    context.control_middleware.clone(),
                ));
                let policy: Arc<dyn SourcePolicy> =
                    Arc::new(RateLimiterSourcePolicy::new(middleware, charge_at));
                Ok(MiddlewareSurfaceAttachment::SourcePoll(
                    SourcePollAttachment {
                        policy,
                        completion_gate: None,
                    },
                ))
            }
            MiddlewareSurface::Effect(effect_surface) => {
                // FLOWIP-120c: one limiter instance guards one declared effect,
                // registered under the per-effect key for metrics.
                let middleware = RateLimiterMiddleware::new_keyed(
                    context.config.stage_id,
                    validated,
                    context.control_middleware.clone(),
                    Some(effect_surface.effect_type.clone()),
                );
                let policy: Arc<dyn EffectPolicy> = Arc::new(middleware);
                Ok(MiddlewareSurfaceAttachment::Effect(policy))
            }
            MiddlewareSurface::SinkDelivery(_) => {
                let middleware = Arc::new(RateLimiterMiddleware::new(
                    context.config.stage_id,
                    validated,
                    context.control_middleware.clone(),
                ));
                let policy: Arc<dyn SinkPolicy> = Arc::new(RateLimiterSinkPolicy::new(middleware));
                Ok(MiddlewareSurfaceAttachment::SinkDelivery(policy))
            }
            MiddlewareSurface::Ingress(_) => {
                // FLOWIP-115d: source-backed hosted ingress. One core per hosted
                // protected unit; the adapter is fail-fast at the listener edge.
                let middleware = Arc::new(RateLimiterMiddleware::new(
                    context.config.stage_id,
                    validated,
                    context.control_middleware.clone(),
                ));
                let policy: Arc<dyn IngressBoundaryMiddleware> =
                    Arc::new(RateLimiterIngressPolicy::new(middleware));
                Ok(MiddlewareSurfaceAttachment::Ingress(policy))
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!(
                    "rate limiter materialize is not implemented for surface {:?}",
                    other.kind()
                )),
            )),
        }
    }

    fn create(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        let validated = self.validated_config().map_err(|err| {
            MiddlewareFactoryError::invalid_configuration(self.label(), &config.name, err)
        })?;

        Ok(Box::new(RateLimiterMiddleware::new(
            config.stage_id,
            validated,
            control_middleware,
        )))
    }

    fn create_for_effect(
        &self,
        _config: &StageConfig,
        _control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
        _effect_type: &str,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        // FLOWIP-115d legacy-route containment (AC46): the hook-bound rate limiter
        // is placed on the Effect surface through `materialize`; the binder routes
        // a control middleware that declares the Effect surface there, never here.
        // Fail closed so a direct caller cannot construct a second, off-carrier
        // effect limiter with its own bucket.
        Err(MiddlewareFactoryError::not_hook_bound(self.label()))
    }

    fn register_source_policy(
        &self,
        _config: &StageConfig,
        _stage_type: obzenflow_core::event::context::StageType,
        _control_middleware: &std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> crate::middleware::MiddlewareFactoryResult<()> {
        // FLOWIP-115d legacy-route containment (AC46): source-poll rate limiting
        // is placed through `materialize` onto the SourcePoll surface and
        // registered as a ready `SourcePolicy`. The binder never routes the
        // hook-bound rate limiter through this legacy factory method. Fail closed.
        Err(MiddlewareFactoryError::not_hook_bound(self.label()))
    }

    fn supported_stage_types(&self) -> &[StageType] {
        // Rate limiting makes sense for all stage types, including joins where the
        // single stage-local bucket is shared across both join inputs (FLOWIP-114m).
        &[
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
            StageType::Join,
        ]
    }

    fn safety_level(&self) -> MiddlewareSafety {
        // Rate limiting on sinks can cause backpressure
        MiddlewareSafety::Advanced
    }

    fn hints(&self) -> crate::middleware::MiddlewareHints {
        crate::middleware::MiddlewareHints {
            rate_limits: true,
            ..Default::default()
        }
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        let validated = self.validated_config().ok()?;
        let mut snapshot = serde_json::json!({
            "tokens_per_sec": validated.events_per_second,
            "burst_capacity": validated.burst_capacity,
            "cost_per_event": validated.cost_per_event,
            "limit_rate": validated.limit_rate(),
        });
        if let Some(configured_burst_capacity) = validated.configured_burst_capacity {
            snapshot["configured_burst_capacity"] = serde_json::json!(configured_burst_capacity);
        }
        Some(snapshot)
    }
}

/// Attach a token-bucket rate limiter admitting `events_per_second`.
///
/// Each attachment owns its own bucket: flow-level `rate_limit(N)` materialises
/// one instance per stage (FLOWIP-050d), and there is no process-wide shared
/// bucket (FLOWIP-114o Q6). On async sources and the effect boundary the limiter
/// awaits its permit as a cancellable future; on sync sources and handler chains
/// it blocks. Counters increment at admission with no refund on a downstream
/// `Skip`/`Abort` (FLOWIP-114m, FLOWIP-114o). See the module docs for the full
/// wait, bucket, and accounting contract.
pub fn rate_limit(events_per_second: f64) -> Box<dyn MiddlewareFactory> {
    RateLimiterBuilder::new(events_per_second).build()
}

/// Attach a rate limiter with an explicit burst capacity. Same per-instance
/// bucket, wait, and accounting contract as [`rate_limit`].
pub fn rate_limit_with_burst(events_per_second: f64, burst: f64) -> Box<dyn MiddlewareFactory> {
    RateLimiterBuilder::new(events_per_second)
        .with_burst(burst)
        .build()
}

#[cfg(test)]
mod tests {
    use super::admission_core::RateLimiterMode;
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use std::time::Duration;

    use obzenflow_core::event::chain_event::ChainEventContent;
    use obzenflow_core::event::{ChainEventFactory, WriterId};
    use obzenflow_runtime::control_plane::ControlPlaneProvider;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use serde_json::json;

    fn test_stage_config(name: &str) -> StageConfig {
        StageConfig {
            stage_id: StageId::new(),
            name: name.to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
        }
    }

    fn materialize_err(factory: RateLimiterFactory) -> MiddlewareFactoryError {
        match factory.create(
            &test_stage_config("test_stage"),
            Arc::new(ControlMiddlewareAggregator::new()),
        ) {
            Ok(_) => {
                panic!("invalid rate limiter configuration should fail during materialisation")
            }
            Err(err) => err,
        }
    }

    fn test_middleware(
        stage_id: StageId,
        events_per_second: f64,
        burst_capacity: Option<f64>,
        cost_per_event: f64,
    ) -> RateLimiterMiddleware {
        let validated =
            validated_rate_limiter_config(events_per_second, burst_capacity, cost_per_event)
                .expect("test middleware configuration should be valid");
        RateLimiterMiddleware::new(
            stage_id,
            validated,
            Arc::new(ControlMiddlewareAggregator::new()),
        )
    }

    #[test]
    fn test_rate_limiter_allows_bursts() {
        // Create middleware directly (we only need a stage_id for writer attribution)
        let middleware = test_middleware(StageId::new(), 10.0, Some(20.0), 1.0);

        let mut ctx = MiddlewareContext::live_handler();

        // Should allow burst of 20 events
        for i in 0..20 {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test.event",
                json!({ "index": i }),
            );

            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => {}
                other => panic!("Expected Continue for event {i}, got {other:?}"),
            }
        }

        // 21st event would block, but we can't easily test blocking in unit tests
        // The blocking behavior is tested in integration tests
    }

    #[test]
    fn test_rate_limiter_control_events_pass_through() {
        // Create middleware directly (we only need a stage_id for writer attribution)
        let middleware = test_middleware(StageId::new(), 1.0, None, 1.0);

        let mut ctx = MiddlewareContext::live_handler();

        // Consume the one available token
        let data_event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.event", json!({}));

        middleware.pre_handle(&data_event, &mut ctx);

        // Control event should still pass through without blocking
        let eof = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);

        match middleware.pre_handle(&eof, &mut ctx) {
            MiddlewareAction::Continue => {}
            other => panic!("Expected Continue for EOF, got {other:?}"),
        }
    }

    #[test]
    fn test_rate_limiter_lifecycle_events_pass_through() {
        let middleware = test_middleware(StageId::new(), 1.0, None, 1.0);

        let mut ctx = MiddlewareContext::live_handler();

        // Consume the one available token
        let data_event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.event", json!({}));
        middleware.pre_handle(&data_event, &mut ctx);

        let lifecycle_event = ChainEventFactory::observability_event(
            WriterId::from(StageId::new()),
            ObservabilityPayload::Middleware(MiddlewareLifecycle::RateLimiter(
                RateLimiterEvent::WindowUtilization {
                    utilization_percent: 0.0,
                    events_in_window: 0,
                    window_size_ms: 1000,
                },
            )),
        );

        match middleware.pre_handle(&lifecycle_event, &mut ctx) {
            MiddlewareAction::Continue => {}
            other => panic!("Expected Continue for lifecycle event, got {other:?}"),
        }
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

    #[test]
    fn test_rate_limiter_supported_stage_types_includes_join() {
        let factory = RateLimiterFactory::new(10.0);
        let supported = factory.supported_stage_types();
        assert!(
            supported.contains(&StageType::Join),
            "FLOWIP-114m: Join must be a supported stage type for rate_limiter"
        );
        for expected in [
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
            StageType::Join,
        ] {
            assert!(
                supported.contains(&expected),
                "missing supported stage type: {expected:?}"
            );
        }
    }

    #[test]
    fn test_rate_limiter_does_not_register_signal_control_point() {
        let factory = RateLimiterFactory::new(100.0).with_burst(500.0);

        // FLOWIP-115c: the dead `create_control_strategy` lane is gone. A rate
        // limiter declares no inbound-signal control point.
        assert!(
            !factory.control_points().signal,
            "Rate limiter should not register a signal control point"
        );
    }

    #[test]
    fn test_rate_limiter_builder_preserves_config() {
        let factory = RateLimiterFactory::from(
            RateLimiterBuilder::new(100.0)
                .with_burst(500.0)
                .with_cost_per_event(2.0),
        );

        assert_eq!(factory.events_per_second, 100.0);
        assert_eq!(factory.burst_capacity, Some(500.0));
        assert_eq!(factory.cost_per_event, 2.0);
    }

    #[test]
    fn test_rate_limit_helpers_use_builder_defaults() {
        assert_eq!(
            rate_limit(25.0).config_snapshot(),
            Some(json!({
                "tokens_per_sec": 25.0,
                "burst_capacity": 25.0,
                "cost_per_event": 1.0,
                "limit_rate": 25.0,
            }))
        );
        assert_eq!(
            rate_limit_with_burst(25.0, 50.0).config_snapshot(),
            Some(json!({
                "tokens_per_sec": 25.0,
                "burst_capacity": 50.0,
                "configured_burst_capacity": 50.0,
                "cost_per_event": 1.0,
                "limit_rate": 25.0,
            }))
        );
    }

    #[test]
    fn test_rate_limiter_rejects_zero_rate() {
        let err = materialize_err(RateLimiterFactory::new(0.0));
        assert!(matches!(
            err,
            MiddlewareFactoryError::InvalidConfiguration { .. }
        ));
        assert!(err.to_string().contains("events_per_second"));
    }

    #[test]
    fn test_rate_limiter_rejects_negative_rate() {
        let err = materialize_err(RateLimiterFactory::new(-1.0));
        assert!(matches!(
            err,
            MiddlewareFactoryError::InvalidConfiguration { .. }
        ));
        assert!(err.to_string().contains("events_per_second"));
    }

    #[test]
    fn test_rate_limiter_rejects_zero_cost() {
        let err = materialize_err(RateLimiterFactory::new(10.0).with_cost_per_event(0.0));
        assert!(matches!(
            err,
            MiddlewareFactoryError::InvalidConfiguration { .. }
        ));
        assert!(err.to_string().contains("cost_per_event"));
    }

    #[test]
    fn test_rate_limiter_rejects_non_finite_values() {
        let inf_err = materialize_err(RateLimiterFactory::new(f64::INFINITY));
        assert!(matches!(
            inf_err,
            MiddlewareFactoryError::InvalidConfiguration { .. }
        ));
        assert!(inf_err.to_string().contains("events_per_second"));

        let nan_err = materialize_err(RateLimiterFactory::new(10.0).with_cost_per_event(f64::NAN));
        assert!(matches!(
            nan_err,
            MiddlewareFactoryError::InvalidConfiguration { .. }
        ));
        assert!(nan_err.to_string().contains("cost_per_event"));
    }

    #[test]
    fn test_rate_limiter_default_effective_capacity_is_at_least_one_weighted_event() {
        let middleware = test_middleware(StageId::new(), 2.0, None, 5.0);

        let bucket = middleware.core.bucket_for_test().lock().unwrap();
        assert!((bucket.capacity - 5.0).abs() < f64::EPSILON);
        assert!((bucket.tokens - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_rate_limiter_rejects_explicit_burst_smaller_than_cost() {
        let err = materialize_err(
            RateLimiterFactory::new(10.0)
                .with_burst(2.0)
                .with_cost_per_event(5.0),
        );
        assert!(matches!(
            err,
            MiddlewareFactoryError::InvalidConfiguration { .. }
        ));
        assert!(err.to_string().contains("burst_capacity"));
        assert!(err.to_string().contains("cost_per_event"));
    }

    #[test]
    fn test_rate_limiter_config_snapshot_uses_effective_capacity_for_low_rates() {
        let snapshot = RateLimiterFactory::new(0.5)
            .config_snapshot()
            .expect("valid low-rate config should expose a snapshot");
        assert_eq!(snapshot["burst_capacity"], json!(1.0));
        assert_eq!(snapshot["cost_per_event"], json!(1.0));
        assert_eq!(snapshot["limit_rate"], json!(0.5));
        assert!(snapshot.get("configured_burst_capacity").is_none());
    }

    #[test]
    fn test_rate_limiter_config_snapshot_exposes_weighted_effective_fields() {
        let snapshot = RateLimiterFactory::new(2.0)
            .with_cost_per_event(5.0)
            .config_snapshot()
            .expect("valid weighted config should expose a snapshot");
        assert_eq!(snapshot["tokens_per_sec"], json!(2.0));
        assert_eq!(snapshot["burst_capacity"], json!(5.0));
        assert_eq!(snapshot["cost_per_event"], json!(5.0));
        assert_eq!(snapshot["limit_rate"], json!(0.4));
    }

    #[test]
    fn test_rate_limiter_limit_rate_matches_weighted_config() {
        let middleware = test_middleware(StageId::new(), 10.0, Some(20.0), 2.0);

        assert!((middleware.limit_rate() - 5.0).abs() < 1e-6);
    }

    #[test]
    fn test_rate_limiter_pre_handle_does_not_hold_bucket_while_waiting_on_stats() {
        let middleware = Arc::new(test_middleware(StageId::new(), 10.0, Some(10.0), 1.0));
        let stats_guard = middleware.core.stats_for_test().lock().unwrap();
        let middleware_for_thread = middleware.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test.event",
                json!({}),
            );
            let mut ctx = MiddlewareContext::live_handler();
            let action = middleware_for_thread.pre_handle(&event, &mut ctx);
            tx.send(action).unwrap();
        });

        std::thread::sleep(Duration::from_millis(25));

        let bucket = middleware
            .core
            .bucket_for_test()
            .try_lock()
            .expect("pre_handle should not hold bucket while blocked on stats");
        assert!(
            bucket.tokens < bucket.capacity,
            "expected pre_handle to consume tokens before blocking on stats"
        );
        drop(bucket);

        drop(stats_guard);

        let action = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("pre_handle should complete once stats lock is released");
        assert!(matches!(action, MiddlewareAction::Continue));
        handle.join().unwrap();
    }

    #[test]
    fn test_rate_limiter_snapshotter_does_not_hold_stats_while_waiting_on_bucket() {
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let stage_id = StageId::new();
        let middleware = RateLimiterMiddleware::new(
            stage_id,
            validated_rate_limiter_config(10.0, Some(10.0), 1.0)
                .expect("snapshotter test configuration should be valid"),
            control.clone(),
        );
        let snapshotter = control
            .rate_limiter_snapshotter(&stage_id)
            .expect("rate limiter snapshotter should be registered");
        let bucket_guard = middleware.core.bucket_for_test().lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            tx.send(snapshotter()).unwrap();
        });

        std::thread::sleep(Duration::from_millis(25));

        let stats_guard = middleware
            .core
            .stats_for_test()
            .try_lock()
            .expect("snapshotter should not hold stats while waiting on bucket");
        drop(stats_guard);

        drop(bucket_guard);

        let metrics = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("snapshotter should complete once bucket lock is released");
        assert!((metrics.bucket_capacity - 10.0).abs() < f64::EPSILON);
        handle.join().unwrap();
    }

    #[test]
    fn test_rate_limiter_summary_does_not_require_bucket_lock() {
        let middleware = Arc::new(test_middleware(StageId::new(), 10.0, Some(10.0), 1.0));
        {
            let mut stats = middleware.core.stats_for_test().lock().unwrap();
            stats.events_window = 1;
            stats.last_summary = Instant::now() - Duration::from_secs(11);
        }

        let bucket_guard = middleware.core.bucket_for_test().lock().unwrap();
        let middleware_for_thread = middleware.clone();
        let (tx, rx) = std::sync::mpsc::channel();

        let handle = std::thread::spawn(move || {
            let mut ctx = MiddlewareContext::live_handler();
            middleware_for_thread.maybe_emit_summary(&mut ctx);
            tx.send(ctx.control_events().len()).unwrap();
        });

        let emitted = rx
            .recv_timeout(Duration::from_millis(100))
            .expect("summary emission should not block on bucket lock");
        assert_eq!(emitted, 1);

        drop(bucket_guard);
        handle.join().unwrap();
    }

    #[test]
    fn test_rate_limiter_mode_hysteresis_transitions() {
        let middleware = test_middleware(StageId::new(), 100.0, Some(1000.0), 1.0);

        // ---- Window 1: utilization 120% -> Normal -> Limiting ----
        {
            let mut stats = middleware.core.stats_for_test().lock().unwrap();
            stats.events_window = 1200;
            stats.tokens_consumed_window = 1200.0;
            stats.last_summary = Instant::now() - Duration::from_secs(10);
        }

        let mut ctx = MiddlewareContext::live_handler();
        middleware.maybe_emit_summary(&mut ctx);
        assert_eq!(ctx.control_events().len(), 2);

        match &ctx.control_events()[0].content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::RateLimiter(RateLimiterEvent::ModeChange {
                    mode_from,
                    mode_to,
                    limit_rate,
                }),
            )) => {
                assert_eq!(mode_from, "normal");
                assert_eq!(mode_to, "limiting");
                assert!((limit_rate - 100.0).abs() < 1e-6);
            }
            other => panic!("Expected mode change event, got {other:?}"),
        }

        match &ctx.control_events()[1].content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::RateLimiter(RateLimiterEvent::WindowUtilization {
                    utilization_percent,
                    events_in_window,
                    window_size_ms,
                }),
            )) => {
                assert!((utilization_percent - 120.0).abs() < 0.1);
                assert_eq!(*events_in_window, 1200);
                assert!(*window_size_ms >= 10_000);
            }
            other => panic!("Expected window utilization event, got {other:?}"),
        }

        // ---- Window 2: utilization 70% -> Limiting (hold=1) ----
        {
            let mut stats = middleware.core.stats_for_test().lock().unwrap();
            stats.events_window = 700;
            stats.tokens_consumed_window = 700.0;
            stats.last_summary = Instant::now() - Duration::from_secs(10);
        }

        let mut ctx = MiddlewareContext::live_handler();
        middleware.maybe_emit_summary(&mut ctx);
        assert_eq!(ctx.control_events().len(), 1);
        match &ctx.control_events()[0].content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::RateLimiter(RateLimiterEvent::WindowUtilization { .. }),
            )) => {}
            other => panic!("Expected window utilization event, got {other:?}"),
        }

        // ---- Window 3: utilization 70% -> Limiting -> Normal (hold=2) ----
        {
            let mut stats = middleware.core.stats_for_test().lock().unwrap();
            stats.events_window = 700;
            stats.tokens_consumed_window = 700.0;
            stats.last_summary = Instant::now() - Duration::from_secs(10);
        }

        let mut ctx = MiddlewareContext::live_handler();
        middleware.maybe_emit_summary(&mut ctx);
        assert_eq!(ctx.control_events().len(), 2);
        match &ctx.control_events()[0].content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::RateLimiter(RateLimiterEvent::ModeChange {
                    mode_from,
                    mode_to,
                    ..
                }),
            )) => {
                assert_eq!(mode_from, "limiting");
                assert_eq!(mode_to, "normal");
            }
            other => panic!("Expected mode change event, got {other:?}"),
        }

        let stats = middleware.core.stats_for_test().lock().unwrap();
        assert_eq!(stats.mode, RateLimiterMode::Normal);
        assert_eq!(stats.exit_hold_count, 0);
    }

    #[test]
    fn test_rate_limiter_activity_pulse_emission() {
        let middleware = test_middleware(StageId::new(), 5.0, Some(10.0), 1.0);

        {
            let mut stats = middleware.core.stats_for_test().lock().unwrap();
            stats.pulse_window_start = Instant::now() - Duration::from_secs(1);
            stats.pulse_delayed_events = 3;
            stats.pulse_delay_ms_total = 450;
            stats.pulse_delay_ms_max = 200;
        }

        let mut ctx = MiddlewareContext::live_handler();
        middleware.maybe_emit_activity_pulse(&mut ctx);
        assert_eq!(ctx.control_events().len(), 1);

        match &ctx.control_events()[0].content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::RateLimiter(RateLimiterEvent::ActivityPulse {
                    window_ms,
                    delayed_events,
                    delay_ms_total,
                    delay_ms_max,
                    limit_rate,
                }),
            )) => {
                assert_eq!(*window_ms, super::admission_core::ACTIVITY_PULSE_WINDOW_MS);
                assert_eq!(*delayed_events, 3);
                assert_eq!(*delay_ms_total, 450);
                assert_eq!(*delay_ms_max, 200);
                assert!((limit_rate - 5.0).abs() < 1e-6);
            }
            other => panic!("Expected activity pulse event, got {other:?}"),
        }

        let stats = middleware.core.stats_for_test().lock().unwrap();
        assert_eq!(stats.pulse_delayed_events, 0);
        assert_eq!(stats.pulse_delay_ms_total, 0);
        assert_eq!(stats.pulse_delay_ms_max, 0);
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

    /// FLOWIP-115d: the rate limiter materialized onto the `Ingress` surface
    /// admits while the bucket has tokens and then fails fast with a
    /// `RateLimited` reject once the bucket is exhausted, never waiting.
    #[test]
    fn rate_limiter_ingress_admits_then_rejects_fail_fast() {
        use crate::middleware::{
            HostedIngressSurfaceKey, HostedIngressTargetKey, IngressRouteScope, IngressStageKey,
            IngressSurface, IngressUnitId, MiddlewareAttachmentRequest, MiddlewareDeclarationIndex,
            MiddlewareMaterializationContext, MiddlewareOrigin, ProtectedUnit, ProtectedUnitId,
            SourceStageIngressOwner,
        };
        use obzenflow_core::ingress::{
            IngressAdmissionDecision, IngressAttemptContext, IngressAttemptSeq,
        };

        let control = Arc::new(ControlMiddlewareAggregator::new());
        let config = test_stage_config("accounts");
        let stage_key = IngressStageKey("accounts".to_string());
        let target = HostedIngressTargetKey {
            surface: HostedIngressSurfaceKey("/api/bank/accounts".to_string()),
            scope: IngressRouteScope::Admission,
        };
        let surface = MiddlewareSurface::Ingress(IngressSurface {
            owner: SourceStageIngressOwner {
                stage_id: config.stage_id,
                stage_key: stage_key.clone(),
            },
            target: target.clone(),
        });
        let unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::Ingress(IngressUnitId {
                source_stage_key: stage_key,
                target,
            }),
        };
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
        };
        let ctx = MiddlewareMaterializationContext {
            config: &config,
            control_middleware: &control,
            stage_type: StageType::InfiniteSource,
        };

        // Burst capacity 1 (events_per_second defaults the burst), 1 event/sec.
        let factory = RateLimiterFactory::new(1.0);
        let boundary = match factory
            .materialize(request, &ctx)
            .expect("ingress materialize")
        {
            MiddlewareSurfaceAttachment::Ingress(boundary) => boundary,
            _ => panic!("expected an Ingress attachment"),
        };

        let attempt = IngressAttemptContext {
            attempt_seq: IngressAttemptSeq(0),
            request_count: 1,
            event_count: 1,
            batch_count: 0,
        };
        assert!(
            matches!(
                boundary.on_ingress(&attempt),
                IngressAdmissionDecision::Accept
            ),
            "the burst token admits the first attempt"
        );
        assert!(
            matches!(
                boundary.on_ingress(&attempt),
                IngressAdmissionDecision::Reject { .. }
            ),
            "an exhausted bucket fails fast with a rate-limited reject, never waiting"
        );
    }
}
