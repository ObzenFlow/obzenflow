// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rate limiting middleware: a token-bucket limiter that paces admission to a
//! protected live-I/O unit.
//!
//! ## Module layout (FLOWIP-115d)
//!
//! - [`admission_core`]: the surface-neutral admission core (`RateLimiterCore`,
//!   the token bucket, counters, deadline calculation, summary/pulse window
//!   state, snapshot projection, and the shared cancellable `acquire_admission`
//!   helper). It is `ChainEvent`-free (AC20).
//! - [`config`]: configuration validation.
//! - [`telemetry`]: turns the core's plain-data outputs into durable
//!   observability `ChainEvent` lifecycle facts.
//! - [`hook_adapters`]: the carrier-bound source/effect/sink-delivery/ingress
//!   adapters.
//! - [`factory`]: the public `RateLimiterBuilder`/`RateLimiterFactory` and the
//!   `rate_limit`/`rate_limit_with_burst` constructors; declaration and
//!   materialization are the sole production placement authority.
//!
//! This root module holds [`RateLimiterMiddleware`], the shared shell state (the
//! `RateLimiterCore` plus writer identity) and the inherent helpers the surface
//! adapters delegate into.
//!
//! ## Wait mechanism by placement (FLOWIP-114o, FLOWIP-115d)
//!
//! The limiter waits in one of two ways, depending on where it is attached. The
//! async live-I/O units (the effect, source, and sink-delivery boundaries) await
//! their permit through the shared [`admission_core::acquire_admission`] helper
//! with `tokio::time::sleep`, a cancellable future, so a drain, EOF,
//! cancellation, or shutdown abandons an in-flight wait. Hosted ingress is
//! fail-fast and never waits while holding a listener request. Handler-shell
//! pacing is retired; production placement is exclusively carrier-bound
//! (AC10/AC50). All paths share one token-bucket, accounting, and
//! lifecycle-event implementation.
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
mod factory;
mod hook_adapters;
mod telemetry;

pub(in crate::middleware::control) use admission_core::RateLimitReservation;
use admission_core::{
    acquire_admission, acquire_reservation, AdmissionDecision, RateLimitDelayEvent, RateLimiterCore,
};
use config::ValidatedRateLimiterConfig;
use telemetry::{delayed_event, rate_limiter_event};

use crate::middleware::{EffectTypeKey, MiddlewareContext};
use obzenflow_core::event::payloads::observability_payload::RateLimiterEvent;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::control_plane::{RateLimiterMetrics, RateLimiterSnapshotter};
use std::sync::Arc;
// FLOWIP-114o: the limiter's token-bucket refill and stats windows read
// `tokio::time::Instant` so Tokio paused time advances them in deterministic
// tests. Under a normal runtime this tracks real time; outside a runtime
// `Instant::now()` falls back to the std clock. `Duration` stays from std.
use tokio::time::Instant;
use tracing::info;

pub use config::RateLimiterConfigError;
pub use factory::{
    rate_limit, rate_limit_with_burst, RateLimiter, RateLimiterBuilder, RateLimiterFactory,
};

/// Marker type for the rate-limiter override-key family. Kept in the module root
/// so its type path (and the derived family label) is stable across the
/// FLOWIP-115d decomposition.
pub struct RateLimiterFamily;

/// Rate limiting middleware using token bucket algorithm.
///
/// A thin shell over [`RateLimiterCore`]: it owns the writer identity and turns
/// the core's plain-data admission and summary outputs into durable
/// `ChainEvent` lifecycle facts. The surface adapters in [`hook_adapters`] hold
/// an `Arc<RateLimiterMiddleware>` and delegate into the inherent helpers below.
pub struct RateLimiterMiddleware {
    core: Arc<RateLimiterCore>,
    /// Writer identity used for durable observability/control events.
    ///
    /// This must match the stage's writer_id so vector-clock watermarks and
    /// stage attribution remain correct in downstream consumers.
    writer_id: WriterId,
}

impl RateLimiterMiddleware {
    pub(crate) fn label(&self) -> &'static str {
        "rate_limiter"
    }

    fn new(
        stage_id: StageId,
        config: ValidatedRateLimiterConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> Result<Self, String> {
        Self::new_keyed(stage_id, config, control_middleware, None)
    }

    /// Construct a limiter registered under a per-effect key (FLOWIP-120c):
    /// one policy instance per protected dependency.
    pub(in crate::middleware::control) fn new_keyed(
        stage_id: StageId,
        config: ValidatedRateLimiterConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
        effect_type: Option<EffectTypeKey>,
    ) -> Result<Self, String> {
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
        }?;

        Ok(Self {
            core,
            writer_id: WriterId::from(stage_id),
        })
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

    /// Shared async token-bucket acquisition. The effect-boundary
    /// `EffectPolicy::admit`, the source-boundary `SourcePolicy` path, and the
    /// sink-delivery `SinkPolicy::admit` all delegate here through
    /// [`admission_core::acquire_admission`], so the live-I/O surfaces share one
    /// accounting and lifecycle-event implementation. It awaits its permit with
    /// a cancellable future instead of blocking the worker thread.
    async fn acquire_permit_async(&self, ctx: &mut MiddlewareContext) {
        let writer_id = self.writer_id;
        acquire_admission(&self.core, |info: RateLimitDelayEvent| {
            ctx.write_control_event(delayed_event(writer_id, info));
        })
        .await;
    }

    pub(in crate::middleware::control) async fn reserve_permit_async(
        &self,
        ctx: &mut MiddlewareContext,
    ) -> RateLimitReservation {
        let writer_id = self.writer_id;
        acquire_reservation(&self.core, |info: RateLimitDelayEvent| {
            ctx.write_control_event(delayed_event(writer_id, info));
        })
        .await
    }

    pub(in crate::middleware::control) fn observe_resilience_attempt(
        &self,
        ctx: &mut MiddlewareContext,
    ) {
        self.maybe_emit_activity_pulse(ctx);
        self.maybe_emit_summary(ctx);
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

/// Shared test helper: build a `RateLimiterMiddleware` directly from a validated
/// config. Lives at the module root so the core-behaviour tests here and the
/// adapter tests in [`hook_adapters`] both use one constructor.
#[cfg(test)]
fn test_middleware(
    stage_id: StageId,
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
) -> RateLimiterMiddleware {
    let validated =
        config::validated_rate_limiter_config(events_per_second, burst_capacity, cost_per_event)
            .expect("test middleware configuration should be valid");
    RateLimiterMiddleware::new(
        stage_id,
        validated,
        Arc::new(super::ControlMiddlewareAggregator::new()),
    )
    .expect("test rate limiter registration should be unique")
}

#[cfg(test)]
mod tests {
    use super::admission_core::RateLimiterMode;
    use super::config::validated_rate_limiter_config;
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use std::time::Duration;

    use obzenflow_core::event::chain_event::ChainEventContent;
    use obzenflow_core::event::payloads::observability_payload::{
        MiddlewareLifecycle, ObservabilityPayload,
    };
    use obzenflow_runtime::control_plane::ControlPlaneProvider;

    #[test]
    fn test_rate_limiter_default_effective_capacity_is_at_least_one_weighted_event() {
        let middleware = test_middleware(StageId::new(), 2.0, None, 5.0);

        let bucket = middleware.core.bucket_for_test().lock().unwrap();
        assert!((bucket.capacity - 5.0).abs() < f64::EPSILON);
        assert!((bucket.tokens - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_rate_limiter_limit_rate_matches_weighted_config() {
        let middleware = test_middleware(StageId::new(), 10.0, Some(20.0), 2.0);

        assert!((middleware.limit_rate() - 5.0).abs() < 1e-6);
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
        )
        .expect("snapshotter test registration should be unique");
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
}
