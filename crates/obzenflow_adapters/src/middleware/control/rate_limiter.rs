//! Rate limiting middleware with blocking implementation
//!
//! This middleware implements a blocking rate limiter that creates natural
//! backpressure by blocking when out of tokens, ensuring no events are lost.

use crate::middleware::{
    ErrorAction, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory,
    MiddlewareSafety,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload, RateLimiterEvent,
};
use obzenflow_core::{StageId, WriterId};
use obzenflow_core::control_middleware::{RateLimiterMetrics, RateLimiterSnapshotter};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_runtime_services::stages::common::control_strategies::{
    ControlEventStrategy, WindowingStrategy,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{debug, info, trace};

/// Token bucket rate limiter implementation
#[derive(Debug)]
struct TokenBucket {
    /// Maximum tokens (burst capacity)
    capacity: f64,
    /// Current tokens available
    tokens: f64,
    /// Tokens added per second
    refill_rate: f64,
    /// Last time tokens were refilled
    last_refill: Instant,
    /// Track if we've crossed threshold
    was_exhausted: bool,
}

#[derive(Debug)]
struct RateLimiterStats {
    // ---- Cumulative counters (never reset) ----
    events_total: u64,
    delayed_total: u64,
    tokens_consumed_total: f64,
    delay_seconds_total: f64,

    // ---- Window counters (reset on summary emission) ----
    events_window: u64,
    delayed_window: u64,
    tokens_consumed_window: f64,
    last_summary: Instant,
}

impl Default for RateLimiterStats {
    fn default() -> Self {
        Self {
            events_total: 0,
            delayed_total: 0,
            tokens_consumed_total: 0.0,
            delay_seconds_total: 0.0,
            events_window: 0,
            delayed_window: 0,
            tokens_consumed_window: 0.0,
            last_summary: Instant::now(),
        }
    }
}

impl TokenBucket {
    fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            capacity,
            tokens: capacity, // Start full
            refill_rate,
            last_refill: Instant::now(),
            was_exhausted: false,
        }
    }

    /// Try to consume tokens, returns true if successful
    fn try_consume(&mut self, tokens: f64) -> bool {
        self.refill();

        trace!(
            "try_consume: requested={}, available={}, capacity={}",
            tokens,
            self.tokens,
            self.capacity
        );

        if self.tokens >= tokens {
            self.tokens -= tokens;
            trace!("try_consume: SUCCESS, remaining tokens={}", self.tokens);
            true
        } else {
            trace!("try_consume: FAILED, insufficient tokens");
            false
        }
    }

    /// Get time until enough tokens are available
    fn time_until_available(&mut self, tokens: f64) -> Option<Duration> {
        self.refill();

        if self.tokens >= tokens {
            return None; // Already available
        }

        let needed = tokens - self.tokens;
        let seconds_needed = needed / self.refill_rate;
        Some(Duration::from_secs_f64(seconds_needed))
    }

    /// Refill tokens based on elapsed time
    fn refill(&mut self) {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill);

        let tokens_to_add = elapsed.as_secs_f64() * self.refill_rate;
        let old_tokens = self.tokens;
        self.tokens = (self.tokens + tokens_to_add).min(self.capacity);
        self.last_refill = now;

        if tokens_to_add > 0.0 {
            trace!(
                "refill: elapsed={:?}, added={}, old={}, new={}",
                elapsed,
                tokens_to_add,
                old_tokens,
                self.tokens
            );
        }
    }

    /// Get current token count (for monitoring)
    fn available_tokens(&mut self) -> f64 {
        self.refill();
        self.tokens
    }

    /// Check if we've crossed the exhaustion threshold (< 10% capacity)
    fn is_exhausted(&self) -> bool {
        self.tokens < self.capacity * 0.1
    }

    /// Check if we've crossed a threshold and should emit control event
    fn check_threshold_crossed(&mut self) -> Option<(&'static str, &'static str)> {
        let exhausted = self.is_exhausted();

        if exhausted && !self.was_exhausted {
            self.was_exhausted = true;
            Some(("normal", "exhausted"))
        } else if !exhausted && self.was_exhausted {
            self.was_exhausted = false;
            Some(("exhausted", "normal"))
        } else {
            None
        }
    }
}

/// Rate limiting middleware using token bucket algorithm with blocking
pub struct RateLimiterMiddleware {
    bucket: Arc<Mutex<TokenBucket>>,
    /// Cost per event (default 1.0)
    cost_per_event: f64,
    /// Statistics for periodic summaries
    stats: Arc<Mutex<RateLimiterStats>>,
    /// Writer identity used for durable observability/control events.
    ///
    /// This must match the stage's writer_id so vector-clock watermarks and
    /// stage attribution remain correct in downstream consumers.
    writer_id: WriterId,
}

impl RateLimiterMiddleware {
    fn new(
        stage_id: StageId,
        events_per_second: f64,
        burst_capacity: Option<f64>,
        cost_per_event: f64,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> Self {
        // For very low rates, ensure we have at least 1 token capacity
        let capacity = burst_capacity.unwrap_or(events_per_second.max(1.0));
        let bucket = TokenBucket::new(capacity, events_per_second);

        info!(
            events_per_second,
            burst_capacity = capacity,
            cost_per_event,
            initial_tokens = capacity,
            "Created rate limiter middleware"
        );

        let stats = Arc::new(Mutex::new(RateLimiterStats::default()));
        let bucket = Arc::new(Mutex::new(bucket));

        let snapshotter: std::sync::Arc<RateLimiterSnapshotter> = Arc::new({
            let stats = stats.clone();
            let bucket = bucket.clone();
            move || {
                let stats_snapshot = stats.lock().ok();
                let bucket_snapshot = bucket.lock().ok();

                match (stats_snapshot, bucket_snapshot) {
                    (Some(s), Some(b)) => RateLimiterMetrics {
                        events_total: s.events_total,
                        delayed_total: s.delayed_total,
                        tokens_consumed_total: s.tokens_consumed_total,
                        delay_seconds_total: s.delay_seconds_total,
                        bucket_tokens: b.tokens,
                        bucket_capacity: b.capacity,
                    },
                    (Some(s), None) => RateLimiterMetrics {
                        events_total: s.events_total,
                        delayed_total: s.delayed_total,
                        tokens_consumed_total: s.tokens_consumed_total,
                        delay_seconds_total: s.delay_seconds_total,
                        bucket_tokens: 0.0,
                        bucket_capacity: 0.0,
                    },
                    _ => RateLimiterMetrics::default(),
                }
            }
        });
        control_middleware.register_rate_limiter(stage_id, snapshotter);

        Self {
            bucket,
            cost_per_event,
            stats,
            writer_id: WriterId::from(stage_id),
        }
    }

    /// Check if we should emit a summary and do so if needed
    fn maybe_emit_summary(&self, ctx: &mut MiddlewareContext) {
        let mut stats = self.stats.lock().unwrap();
        let bucket = self.bucket.lock().unwrap();

        // Emit summary every 10 seconds or every 1000 processed events.
        //
        // Note: `requests_allowed` is incremented once per event that successfully
        // consumes tokens (including events that were previously delayed), so it
        // represents the true per-window event count. `requests_delayed` tracks
        // how many events experienced backpressure and is used separately for
        // delay-rate calculations.
        let should_emit =
            stats.last_summary.elapsed() >= Duration::from_secs(10) || stats.events_window >= 1000;

        if should_emit {
            let consumption_rate = if stats.last_summary.elapsed().as_secs() > 0 {
                stats.tokens_consumed_window / stats.last_summary.elapsed().as_secs_f64()
            } else {
                0.0
            };

            let utilization = 1.0 - (bucket.tokens / bucket.capacity);

            info!(
                window_duration_s = stats.last_summary.elapsed().as_secs(),
                requests_allowed = stats.events_window,
                requests_delayed = stats.delayed_window,
                tokens_consumed = stats.tokens_consumed_window,
                consumption_rate,
                utilization_pct = format!("{:.1}%", utilization * 100.0),
                "Rate limiter summary"
            );

            let events_in_window = stats.events_window;
            let event = ChainEventFactory::observability_event(
                self.writer_id,
                ObservabilityPayload::Middleware(MiddlewareLifecycle::RateLimiter(
                    RateLimiterEvent::WindowUtilization {
                        utilization_percent: utilization * 100.0,
                        events_in_window,
                        window_size_ms: stats.last_summary.elapsed().as_millis() as u64,
                    },
                )),
            );
            ctx.write_control_event(event);

            // Reset stats
            stats.events_window = 0;
            stats.delayed_window = 0;
            stats.tokens_consumed_window = 0.0;
            stats.last_summary = Instant::now();
        }
    }
}

impl Middleware for RateLimiterMiddleware {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        if event.is_control() || event.is_lifecycle() {
            trace!(
                event_id = %event.id,
                event_type = %event.event_type(),
                "Control/lifecycle event bypassing rate limiter"
            );
            return MiddlewareAction::Continue;
        }

        let event_id = event.id.clone();
        let event_type = event.event_type();
        trace!(event_id = %event_id, event_type = %event_type, "Rate limiter processing event");

        // Blocking loop - wait until we have tokens
        let mut delayed_this_event = false;
        loop {
            let mut bucket = self.bucket.lock().unwrap();

            if bucket.try_consume(self.cost_per_event) {
                // Track successful consumption
                if let Ok(mut stats) = self.stats.lock() {
                    stats.events_total += 1;
                    stats.tokens_consumed_total += self.cost_per_event;
                    stats.events_window += 1;
                    stats.tokens_consumed_window += self.cost_per_event;
                }

                let available = bucket.available_tokens();
                debug!(
                    event_id = %event_id,
                    event_type = %event_type,
                    available_tokens = available,
                    cost = self.cost_per_event,
                    "Rate limit passed - processing event immediately"
                );

                // Check for threshold crossing
                if let Some((from, to)) = bucket.check_threshold_crossed() {
                    info!(
                        from_state = from,
                        to_state = to,
                        available_tokens = bucket.tokens,
                        capacity = bucket.capacity,
                        "Rate limiter state transition"
                    );

                    // Emit a window utilization event for state changes
                    let utilization = 1.0 - (bucket.tokens / bucket.capacity);
                    let event = ChainEventFactory::observability_event(
                        self.writer_id,
                        ObservabilityPayload::Middleware(MiddlewareLifecycle::RateLimiter(
                            RateLimiterEvent::WindowUtilization {
                                utilization_percent: utilization * 100.0,
                                events_in_window: 0, // This is a state transition event
                                window_size_ms: 1000, // Default window
                            },
                        )),
                    );
                    ctx.write_control_event(event);
                }

                // We have tokens, allow the event
                ctx.emit_event(
                    "rate_limiter",
                    "event_allowed",
                    json!({
                        "event_id": event_id.to_string(),
                        "available_tokens": bucket.available_tokens(),
                    }),
                );

                drop(bucket); // Release lock before returning
                return MiddlewareAction::Continue;
            }

            // No tokens available - calculate wait time
            let wait_time = bucket
                .time_until_available(self.cost_per_event)
                .unwrap_or(Duration::from_millis(10));

            let available = bucket.available_tokens();
            info!(
                event_id = %event_id,
                event_type = %event_type,
                wait_ms = wait_time.as_millis(),
                available_tokens = available,
                needed_tokens = self.cost_per_event,
                "Rate limited - blocking for {:?}",
                wait_time
            );

            // Track this as a delayed request
            if !delayed_this_event {
                delayed_this_event = true;
                if let Ok(mut stats) = self.stats.lock() {
                    stats.delayed_total += 1;
                    stats.delayed_window += 1;
                }

                // Emit a durable delayed event so downstream observers (e.g. MetricsAggregator)
                // can count delays without per-execution tracing.
            let (current_rate, limit_rate) = {
                let limit_rate = if self.cost_per_event > 0.0 {
                    bucket.refill_rate / self.cost_per_event
                } else {
                    0.0
                };

                let current_rate = if let Ok(stats) = self.stats.lock() {
                    let elapsed_s = stats.last_summary.elapsed().as_secs_f64();
                    if elapsed_s > 0.0 && self.cost_per_event > 0.0 {
                        (stats.tokens_consumed_window / elapsed_s) / self.cost_per_event
                    } else {
                        0.0
                    }
                } else {
                    0.0
                };

                (current_rate, limit_rate)
            };

            ctx.write_control_event(ChainEventFactory::observability_event(
                self.writer_id,
                ObservabilityPayload::Middleware(MiddlewareLifecycle::RateLimiter(
                    RateLimiterEvent::Delayed {
                        delay_ms: wait_time.as_millis() as u64,
                        current_rate,
                        limit_rate,
                    },
                )),
            ));
            }

            // Check for threshold crossing
            if let Some((from, to)) = bucket.check_threshold_crossed() {
                info!(
                    from_state = from,
                    to_state = to,
                    available_tokens = bucket.tokens,
                    capacity = bucket.capacity,
                    "Rate limiter state transition (exhausted)"
                );

                // Emit a window utilization event when exhausted
                let event = ChainEventFactory::observability_event(
                    self.writer_id,
                    ObservabilityPayload::Middleware(MiddlewareLifecycle::RateLimiter(
                        RateLimiterEvent::WindowUtilization {
                            utilization_percent: 100.0, // Exhausted = 100% utilized
                            events_in_window: 0,        // This is a state transition event
                            window_size_ms: 1000,       // Default window
                        },
                    )),
                );
                ctx.write_control_event(event);
            }

            ctx.emit_event(
                "rate_limiter",
                "event_blocked",
                json!({
                    "event_id": event_id.to_string(),
                    "wait_time_ms": wait_time.as_millis(),
                    "available_tokens": bucket.available_tokens(),
                }),
            );

            // Release lock before sleeping
            drop(bucket);

            // Block until tokens should be available
            // For longer waits, use block_in_place to avoid blocking tokio worker threads
            let wait_start = Instant::now();
            if wait_time > Duration::from_millis(1) {
                trace!(event_id = %event_id, "Using block_in_place for wait > 1ms");
                tokio::task::block_in_place(|| {
                    std::thread::sleep(wait_time);
                });
            } else {
                // For very short waits, just yield to scheduler
                trace!(event_id = %event_id, "Using yield_now for wait <= 1ms");
                std::thread::yield_now();
            }
            let waited = wait_start.elapsed();
            if delayed_this_event {
                if let Ok(mut stats) = self.stats.lock() {
                    stats.delay_seconds_total += waited.as_secs_f64();
                }
            }

            info!(
                event_id = %event_id,
                event_type = %event_type,
                "Rate limit released - attempting to process event"
            );

            // Loop back to try again
        }
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        _outputs: &[ChainEvent],
        ctx: &mut MiddlewareContext,
    ) {
        // Check if we should emit a summary
        self.maybe_emit_summary(ctx);
    }

    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Don't consume tokens for errors
        ErrorAction::Propagate
    }
}

/// Factory for creating rate limiter middleware
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
            cost_per_event: 1.0,
        }
    }

    /// Set burst capacity (defaults to events_per_second)
    pub fn with_burst(mut self, capacity: f64) -> Self {
        self.burst_capacity = Some(capacity);
        self
    }

    /// Set cost per event (for weighted rate limiting)
    pub fn with_cost(mut self, cost: f64) -> Self {
        self.cost_per_event = cost;
        self
    }
}

impl MiddlewareFactory for RateLimiterFactory {
    fn create(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> Box<dyn Middleware> {
        Box::new(RateLimiterMiddleware::new(
            config.stage_id,
            self.events_per_second,
            self.burst_capacity,
            self.cost_per_event,
            control_middleware,
        ))
    }

    fn name(&self) -> &str {
        "rate_limiter"
    }

    fn create_control_strategy(&self) -> Option<Box<dyn ControlEventStrategy>> {
        // Need delay strategy to ensure we can flush delayed events before EOF
        // Calculate max delay based on burst capacity and rate
        let capacity = self.burst_capacity.unwrap_or(self.events_per_second);
        let max_drain_time = capacity / self.events_per_second;

        Some(Box::new(WindowingStrategy::new(Duration::from_secs_f64(
            max_drain_time,
        ))))
    }

    fn supported_stage_types(&self) -> &[StageType] {
        // Rate limiting makes sense for all stage types
        &[
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
        ]
    }

    fn safety_level(&self) -> MiddlewareSafety {
        // Rate limiting on sinks can cause backpressure
        MiddlewareSafety::Advanced
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "tokens_per_sec": self.events_per_second,
            "burst_capacity": self.burst_capacity.unwrap_or(self.events_per_second),
        }))
    }
}

/// Helper function for common module
pub fn rate_limit(events_per_second: f64) -> Box<dyn MiddlewareFactory> {
    Box::new(RateLimiterFactory::new(events_per_second))
}

/// Helper function with burst capacity
pub fn rate_limit_with_burst(events_per_second: f64, burst: f64) -> Box<dyn MiddlewareFactory> {
    Box::new(RateLimiterFactory::new(events_per_second).with_burst(burst))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use obzenflow_core::event::{ChainEventFactory, EventId, WriterId};

    #[test]
    fn test_token_bucket_basic() {
        let mut bucket = TokenBucket::new(10.0, 5.0); // 10 capacity, 5/sec refill

        // Should start full
        assert!(bucket.try_consume(5.0));
        assert!(bucket.try_consume(5.0));
        assert!(!bucket.try_consume(5.0)); // Should fail

        // Wait a bit and check refill
        std::thread::sleep(Duration::from_millis(200)); // 0.2 sec = 1 token
        assert!(bucket.try_consume(1.0)); // Should succeed
        assert!(!bucket.try_consume(1.0)); // Should fail again
    }

    #[test]
    fn test_token_bucket_time_until_available() {
        let mut bucket = TokenBucket::new(10.0, 2.0); // 10 capacity, 2/sec refill

        // Consume all tokens
        assert!(bucket.try_consume(10.0));

        // Should need 2.5 seconds to get 5 tokens
        let wait = bucket.time_until_available(5.0).unwrap();
        assert!((wait.as_secs_f64() - 2.5).abs() < 0.1);
    }

    #[test]
    fn test_rate_limiter_allows_bursts() {
        // Create middleware directly (we only need a stage_id for writer attribution)
        let middleware = RateLimiterMiddleware::new(
            StageId::new(),
            10.0,
            Some(20.0),
            1.0,
            Arc::new(ControlMiddlewareAggregator::new()),
        );

        let mut ctx = MiddlewareContext::new();

        // Should allow burst of 20 events
        for i in 0..20 {
            let event = ChainEventFactory::data_event(
                WriterId::from(StageId::new()),
                "test.event",
                json!({ "index": i }),
            );

            match middleware.pre_handle(&event, &mut ctx) {
                MiddlewareAction::Continue => {}
                other => panic!("Expected Continue for event {}, got {:?}", i, other),
            }
        }

        // 21st event would block, but we can't easily test blocking in unit tests
        // The blocking behavior is tested in integration tests
    }

    #[test]
    fn test_rate_limiter_control_events_pass_through() {
        // Create middleware directly (we only need a stage_id for writer attribution)
        let middleware = RateLimiterMiddleware::new(
            StageId::new(),
            1.0,
            None,
            1.0,
            Arc::new(ControlMiddlewareAggregator::new()),
        );

        let mut ctx = MiddlewareContext::new();

        // Consume the one available token
        let data_event =
            ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.event", json!({}));

        middleware.pre_handle(&data_event, &mut ctx);

        // Control event should still pass through without blocking
        let eof = ChainEventFactory::eof_event(WriterId::from(StageId::new()), true);

        match middleware.pre_handle(&eof, &mut ctx) {
            MiddlewareAction::Continue => {}
            other => panic!("Expected Continue for EOF, got {:?}", other),
        }
    }

    #[test]
    fn test_rate_limiter_lifecycle_events_pass_through() {
        let middleware = RateLimiterMiddleware::new(
            StageId::new(),
            1.0,
            None,
            1.0,
            Arc::new(ControlMiddlewareAggregator::new()),
        );

        let mut ctx = MiddlewareContext::new();

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
            other => panic!("Expected Continue for lifecycle event, got {:?}", other),
        }
    }

    #[test]
    fn test_rate_limiter_strategy_requirement() {
        let factory = RateLimiterFactory::new(100.0).with_burst(500.0);

        let strategy = factory.create_control_strategy();
        assert!(
            strategy.is_some(),
            "Expected windowing strategy for rate limiter"
        );
        // Can't easily test the window duration without exposing internals
    }
}
