//! Circuit breaker middleware for fail-fast behavior
//!
//! This middleware implements the circuit breaker pattern to prevent
//! cascading failures. It emits raw events that can be consumed by
//! monitoring and SLI middleware.

use crate::middleware::{Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{EventId, StageId, WriterId};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use serde_json::json;
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum CircuitState {
    /// Normal operation - requests pass through
    Closed = 0,
    /// Circuit is open - requests are rejected
    Open = 1,
    /// Testing if the circuit can be closed - limited requests allowed
    HalfOpen = 2,
}

impl From<u8> for CircuitState {
    fn from(value: u8) -> Self {
        match value {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed,
        }
    }
}

/// Circuit breaker middleware that prevents cascading failures
pub struct CircuitBreakerMiddleware {
    /// Current state of the circuit breaker
    state: Arc<AtomicU8>,
    /// Number of consecutive successes
    success_count: Arc<AtomicUsize>,
    /// Number of consecutive failures
    failure_count: Arc<AtomicUsize>,
    /// Failure threshold before opening circuit
    threshold: usize,
    /// Duration to wait before attempting half-open
    cooldown: Duration,
    /// When the circuit was opened
    opened_at: Arc<Mutex<Option<Instant>>>,
    /// Whether a probe request is in flight (for half-open state)
    probe_in_flight: Arc<AtomicU8>,
    /// Statistics for periodic summaries
    stats: Arc<Mutex<CircuitBreakerStats>>,
    /// When the last state change occurred
    last_state_change: Arc<Mutex<Instant>>,
}

#[derive(Debug)]
struct CircuitBreakerStats {
    requests_processed: u64,
    requests_rejected: u64,
    last_summary: Instant,
}

impl Default for CircuitBreakerStats {
    fn default() -> Self {
        Self {
            requests_processed: 0,
            requests_rejected: 0,
            last_summary: Instant::now(),
        }
    }
}

impl CircuitBreakerMiddleware {
    /// Create a new circuit breaker with the given failure threshold
    pub fn new(threshold: usize) -> Self {
        Self::with_cooldown(threshold, Duration::from_secs(60))
    }

    /// Create a circuit breaker with custom cooldown duration
    pub fn with_cooldown(threshold: usize, cooldown: Duration) -> Self {
        Self {
            state: Arc::new(AtomicU8::new(CircuitState::Closed as u8)),
            success_count: Arc::new(AtomicUsize::new(0)),
            failure_count: Arc::new(AtomicUsize::new(0)),
            threshold,
            cooldown,
            opened_at: Arc::new(Mutex::new(None)),
            probe_in_flight: Arc::new(AtomicU8::new(0)),
            stats: Arc::new(Mutex::new(CircuitBreakerStats {
                requests_processed: 0,
                requests_rejected: 0,
                last_summary: Instant::now(),
            })),
            last_state_change: Arc::new(Mutex::new(Instant::now())),
        }
    }

    fn current_state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::SeqCst))
    }

    fn transition_to(&self, new_state: CircuitState, ctx: &mut MiddlewareContext) {
        let old_state = self.current_state();
        self.state.store(new_state as u8, Ordering::SeqCst);

        // Update last state change
        *self.last_state_change.lock().unwrap() = Instant::now();

        // Track when we open the circuit
        if new_state == CircuitState::Open {
            *self.opened_at.lock().unwrap() = Some(Instant::now());
        }

        // Emit lifecycle event for state transition
        let event = match (old_state, new_state) {
            (CircuitState::Closed, CircuitState::Open) => {
                let failure_count = self.failure_count.load(Ordering::Relaxed) as u64;
                let success_count = self.success_count.load(Ordering::Relaxed) as u64;
                let total = failure_count + success_count;
                let error_rate = if total > 0 {
                    failure_count as f64 / total as f64
                } else {
                    0.0
                };

                ChainEventFactory::circuit_breaker_opened(
                    WriterId::from(StageId::new()),
                    error_rate,
                    failure_count,
                )
            }
            (CircuitState::Open, CircuitState::HalfOpen) => ChainEventFactory::observability_event(
                WriterId::from(StageId::new()),
                ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                    CircuitBreakerEvent::HalfOpen {
                        test_request_count: 0,
                    },
                )),
            ),
            (CircuitState::HalfOpen, CircuitState::Closed) => {
                let success_count = self.success_count.load(Ordering::Relaxed) as u64;
                let recovery_duration_ms =
                    self.last_state_change.lock().unwrap().elapsed().as_millis() as u64;

                ChainEventFactory::observability_event(
                    WriterId::from(StageId::new()),
                    ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                        CircuitBreakerEvent::Closed {
                            success_count,
                            recovery_duration_ms,
                        },
                    )),
                )
            }
            _ => {
                // For other transitions, use a generic metrics event
                ChainEventFactory::metrics_state_snapshot(
                    WriterId::from(StageId::new()),
                    json!({
                        "circuit_breaker": {
                            "from_state": format!("{:?}", old_state),
                            "to_state": format!("{:?}", new_state),
                            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                        }
                    }),
                )
            }
        };

        ctx.write_control_event(event);

        tracing::info!(
            "Circuit breaker state transition: {:?} -> {:?}",
            old_state,
            new_state
        );
    }

    fn should_attempt_reset(&self) -> bool {
        if let Some(opened_at) = *self.opened_at.lock().unwrap() {
            opened_at.elapsed() >= self.cooldown
        } else {
            false
        }
    }

    fn maybe_emit_summary(&self, ctx: &mut MiddlewareContext) {
        let mut stats = self.stats.lock().unwrap();

        // Emit summary every 10 seconds or every 1000 requests
        let should_emit = stats.last_summary.elapsed() >= Duration::from_secs(10)
            || stats.requests_processed + stats.requests_rejected >= 1000;

        if should_emit {
            // Emit a circuit breaker summary event
            let event = ChainEventFactory::circuit_breaker_summary(
                WriterId::from(StageId::new()),
                stats.last_summary.elapsed().as_secs(),
                stats.requests_processed,
                stats.requests_rejected,
                format!("{:?}", self.current_state()),
                self.failure_count.load(Ordering::SeqCst),
                if stats.requests_processed + stats.requests_rejected > 0 {
                    stats.requests_rejected as f64
                        / (stats.requests_processed + stats.requests_rejected) as f64
                } else {
                    0.0
                },
            );
            ctx.write_control_event(event);

            // Reset stats
            stats.requests_processed = 0;
            stats.requests_rejected = 0;
            stats.last_summary = Instant::now();
        }
    }
}

impl Middleware for CircuitBreakerMiddleware {
    fn pre_handle(&self, _event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        match self.current_state() {
            CircuitState::Closed => {
                // Normal operation
                MiddlewareAction::Continue
            }

            CircuitState::Open => {
                // Check if we should transition to half-open
                if self.should_attempt_reset() {
                    self.transition_to(CircuitState::HalfOpen, ctx);
                    self.probe_in_flight.store(0, Ordering::SeqCst);
                    // Continue to half-open handling
                    self.pre_handle(_event, ctx)
                } else {
                    // Reject the request and emit event
                    let cooldown_remaining =
                        if let Some(opened_at) = *self.opened_at.lock().unwrap() {
                            self.cooldown.saturating_sub(opened_at.elapsed())
                        } else {
                            self.cooldown
                        };

                    ctx.emit_event(
                        "circuit_breaker",
                        "rejected",
                        json!({
                            "reason": "circuit_open",
                            "consecutive_failures": self.failure_count.load(Ordering::SeqCst),
                            "threshold": self.threshold,
                            "cooldown_remaining_ms": cooldown_remaining.as_millis()
                        }),
                    );

                    // Track rejection
                    self.stats.lock().unwrap().requests_rejected += 1;

                    MiddlewareAction::Skip(vec![])
                }
            }

            CircuitState::HalfOpen => {
                // Allow one probe request through
                if self
                    .probe_in_flight
                    .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
                {
                    // This is the probe request
                    ctx.emit_event(
                        "circuit_breaker",
                        "probe_started",
                        json!({
                            "reason": "testing_recovery"
                        }),
                    );
                    ctx.set_baggage("circuit_breaker.is_probe", json!(true));
                    MiddlewareAction::Continue
                } else {
                    // Another probe is already in flight, reject this request
                    ctx.emit_event(
                        "circuit_breaker",
                        "rejected",
                        json!({
                            "reason": "probe_in_progress"
                        }),
                    );

                    // Track rejection
                    self.stats.lock().unwrap().requests_rejected += 1;

                    MiddlewareAction::Skip(vec![])
                }
            }
        }
    }

    fn post_handle(
        &self,
        _event: &ChainEvent,
        outputs: &[ChainEvent],
        ctx: &mut MiddlewareContext,
    ) {
        let is_success = !outputs.is_empty();
        let is_probe = ctx
            .get_baggage("circuit_breaker.is_probe")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Track successful processing
        if is_success {
            self.stats.lock().unwrap().requests_processed += 1;
        }

        match self.current_state() {
            CircuitState::Closed => {
                if is_success {
                    // Reset failure count on success
                    self.failure_count.store(0, Ordering::SeqCst);
                } else {
                    // Increment failure count
                    let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;

                    if failures >= self.threshold {
                        // Open the circuit
                        self.transition_to(CircuitState::Open, ctx);

                        // Emit event about circuit opening
                        ctx.emit_event(
                            "circuit_breaker",
                            "opened",
                            json!({
                                "consecutive_failures": failures,
                                "threshold": self.threshold,
                                "reason": "failure_threshold_exceeded"
                            }),
                        );

                        tracing::warn!(
                            "Circuit breaker opened after {} consecutive failures",
                            failures
                        );
                    }
                }
            }

            CircuitState::HalfOpen => {
                if is_probe {
                    if is_success {
                        // Probe succeeded, close the circuit
                        self.transition_to(CircuitState::Closed, ctx);
                        self.failure_count.store(0, Ordering::SeqCst);
                        self.probe_in_flight.store(0, Ordering::SeqCst);

                        ctx.emit_event(
                            "circuit_breaker",
                            "closed",
                            json!({
                                "reason": "probe_succeeded"
                            }),
                        );

                        tracing::info!("Circuit breaker probe succeeded, circuit closed");
                    } else {
                        // Probe failed, reopen the circuit
                        self.transition_to(CircuitState::Open, ctx);
                        self.probe_in_flight.store(0, Ordering::SeqCst);

                        ctx.emit_event(
                            "circuit_breaker",
                            "reopened",
                            json!({
                                "reason": "probe_failed"
                            }),
                        );

                        tracing::warn!("Circuit breaker probe failed, circuit reopened");
                    }
                }
            }

            CircuitState::Open => {
                // Nothing to do in post-handle for open state
            }
        }

        // Check if we should emit a summary
        self.maybe_emit_summary(ctx);
    }
}

/// Builder for circuit breaker middleware
pub struct CircuitBreakerBuilder {
    threshold: usize,
    cooldown: Duration,
}

impl CircuitBreakerBuilder {
    /// Create a new circuit breaker builder with the given threshold
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            cooldown: Duration::from_secs(60),
        }
    }

    /// Set the cooldown duration before attempting to close the circuit
    pub fn cooldown(mut self, duration: Duration) -> Self {
        self.cooldown = duration;
        self
    }

    /// Build the circuit breaker middleware factory
    pub fn build(self) -> Box<dyn MiddlewareFactory> {
        Box::new(CircuitBreakerFactory {
            threshold: self.threshold,
            cooldown: self.cooldown,
        })
    }
}

/// Factory for creating circuit breaker middleware
pub struct CircuitBreakerFactory {
    threshold: usize,
    cooldown: Duration,
}

impl CircuitBreakerFactory {
    /// Create a new circuit breaker factory with the given threshold
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            cooldown: Duration::from_secs(60),
        }
    }

    /// Set the cooldown duration before attempting to close the circuit
    pub fn with_cooldown(mut self, duration: Duration) -> Self {
        self.cooldown = duration;
        self
    }
}

impl MiddlewareFactory for CircuitBreakerFactory {
    fn create(&self, _config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(CircuitBreakerMiddleware::with_cooldown(
            self.threshold,
            self.cooldown,
        ))
    }

    fn name(&self) -> &str {
        "circuit_breaker"
    }
}

/// Create a circuit breaker factory with default settings
pub fn circuit_breaker(threshold: usize) -> Box<dyn MiddlewareFactory> {
    Box::new(CircuitBreakerFactory::new(threshold))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_event() -> ChainEvent {
        ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}))
    }

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let cb = CircuitBreakerMiddleware::new(3);

        // First 2 failures shouldn't open the circuit
        for _ in 0..2 {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::new();
            assert!(matches!(
                cb.pre_handle(&event, &mut ctx),
                MiddlewareAction::Continue
            ));
            cb.post_handle(&event, &vec![], &mut ctx); // Empty outputs = failure
        }

        // Third failure should open the circuit
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx),
            MiddlewareAction::Continue
        ));
        cb.post_handle(&event, &vec![], &mut ctx); // This triggers the opening

        // Next request should be rejected
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx),
            MiddlewareAction::Skip(_)
        ));
        assert!(ctx.has_event("circuit_breaker", "rejected"));
    }

    #[test]
    fn test_circuit_breaker_success_resets_count() {
        let cb = CircuitBreakerMiddleware::new(3);

        // Two failures
        for _ in 0..2 {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::new();
            let _ = cb.pre_handle(&event, &mut ctx);
            cb.post_handle(&event, &vec![], &mut ctx);
        }

        // Success should reset the count
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        let _ = cb.pre_handle(&event, &mut ctx);
        let outputs = vec![create_test_event()]; // Non-empty = success
        cb.post_handle(&event, &outputs, &mut ctx);

        // Should now need 3 more failures to open
        for _ in 0..2 {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::new();
            assert!(matches!(
                cb.pre_handle(&event, &mut ctx),
                MiddlewareAction::Continue
            ));
            cb.post_handle(&event, &vec![], &mut ctx);
        }

        // Still closed
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx),
            MiddlewareAction::Continue
        ));
    }
}
