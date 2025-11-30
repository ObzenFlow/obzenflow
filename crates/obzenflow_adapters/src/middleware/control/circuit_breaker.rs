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
use obzenflow_core::TypedPayload;
use obzenflow_core::{
    circuit_breaker_contract_registry, circuit_breaker_registry, CircuitBreakerContractMode,
    EventId, StageId, WriterId,
};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type FailureClassifier =
    Arc<dyn Fn(&ChainEvent, &[ChainEvent]) -> bool + Send + Sync>;

/// How the circuit breaker decides when to open while in the Closed state.
#[derive(Debug, Clone)]
enum CircuitBreakerFailureMode {
    /// Simple consecutive-failures threshold (current default behaviour).
    Consecutive { max_failures: NonZeroU32 },
    /// Rate-based mode over a sliding window of recent calls.
    RateBased {
        window: FailureWindow,
        failure_rate_threshold: f32,
        slow_call_rate_threshold: Option<f32>,
        slow_call_duration_threshold: Option<Duration>,
        minimum_calls: NonZeroU32,
    },
}

/// Sliding window shape for rate-based failure detection.
#[derive(Debug, Clone)]
enum FailureWindow {
    /// Sliding window over the last `size` calls.
    Count { size: u32 },
    /// Time-based window over the last `duration`, with a minimum number of calls.
    Time { duration: Duration, minimum_calls: u32 },
}

/// Behaviour while the circuit breaker is in the Open state.
#[derive(Debug, Clone)]
pub enum OpenPolicy {
    /// Emit degraded responses via fallback when configured; otherwise behave
    /// like a simple rejection/skip. This matches the existing default
    /// behaviour from 051b‑part‑2.
    EmitFallback,
    /// Fail fast with an explicit rejection instead of attempting to
    /// synthesize degraded responses.
    FailFast,
    /// Drop events while Open (best‑effort fire‑and‑forget or sampling
    /// scenarios). This is powerful but should be used with care when
    /// contracts are strict.
    Skip,
}

impl Default for OpenPolicy {
    fn default() -> Self {
        OpenPolicy::EmitFallback
    }
}

/// Behaviour while the circuit breaker is in the HalfOpen state.
#[derive(Debug, Clone)]
pub struct HalfOpenPolicy {
    /// Maximum number of concurrent probe calls allowed while HalfOpen.
    permitted_probes: NonZeroU32,
    /// Policy applied to non‑probe calls that arrive while all probe slots
    /// are already in use.
    on_rejected: OpenPolicy,
}

impl Default for HalfOpenPolicy {
    fn default() -> Self {
        Self {
            // Matches existing single‑probe behaviour.
            permitted_probes: NonZeroU32::new(1)
                .expect("permitted_probes default must be non‑zero"),
            // Non‑probe calls behave like Open with EmitFallback by default.
            on_rejected: OpenPolicy::EmitFallback,
        }
    }
}

impl HalfOpenPolicy {
    /// Create a new HalfOpenPolicy with the given probe limit and
    /// behaviour for non‑probe calls.
    pub fn new(permitted_probes: NonZeroU32, on_rejected: OpenPolicy) -> Self {
        Self {
            permitted_probes,
            on_rejected,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct CallSample {
    timestamp: Instant,
    is_failure: bool,
    is_slow: bool,
}

#[derive(Debug)]
struct FailureWindowState {
    samples: Vec<Option<CallSample>>,
    index: usize,
    count: usize,
}

impl FailureWindowState {
    fn new(capacity: usize) -> Self {
        Self {
            samples: vec![None; capacity.max(1)],
            index: 0,
            count: 0,
        }
    }

    fn capacity(&self) -> usize {
        self.samples.len()
    }

    fn push(&mut self, sample: CallSample) {
        let cap = self.capacity();
        if cap == 0 {
            return;
        }
        self.samples[self.index] = Some(sample);
        self.index = (self.index + 1) % cap;
        if self.count < cap {
            self.count += 1;
        }
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = CallSample> + 'a {
        let cap = self.capacity();
        (0..self.count).filter_map(move |i| {
            let idx = if self.count < cap {
                i
            } else {
                (self.index + i) % cap
            };
            self.samples[idx]
        })
    }
}

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
    /// Failure mode for deciding when to open while Closed.
    failure_mode: CircuitBreakerFailureMode,
    /// Sliding window state for rate-based failure detection (when enabled).
    rate_window: Option<Mutex<FailureWindowState>>,
    /// Duration to wait before attempting half-open
    cooldown: Duration,
    /// When the circuit was opened
    opened_at: Arc<Mutex<Option<Instant>>>,
    /// Number of probe requests in flight (for half-open state)
    probe_in_flight: Arc<AtomicU32>,
    /// Statistics for periodic summaries
    stats: Arc<Mutex<CircuitBreakerStats>>,
    /// When the last state change occurred
    last_state_change: Arc<Mutex<Instant>>,
    /// Stage identifier for this breaker (when known).
    stage_id: Option<StageId>,
    /// Optional fallback generator used when the circuit is open.
    ///
    /// When configured, requests that would normally be rejected in the
    /// Open or HalfOpen (non‑probe) states will instead be short‑circuited
    /// to these synthetic results via `MiddlewareAction::Skip(results)`.
    ///
    /// This keeps the handler itself unaware of circuit breaker policy while
    /// allowing flows to provide domain‑specific degraded responses purely
    /// via circuit breaker configuration.
    fallback: Option<Arc<dyn Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync>>,
    /// Optional classifier that decides whether a given call should be counted
    /// as a failure for breaker purposes based on the input event and the
    /// outputs produced by the handler.
    failure_classifier: Option<FailureClassifier>,
    /// Policy controlling behaviour while the circuit is Open.
    open_policy: OpenPolicy,
    /// Policy controlling behaviour while the circuit is HalfOpen.
    half_open_policy: HalfOpenPolicy,
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
        Self::with_cooldown_and_fallback(threshold, Duration::from_secs(60), None, None, None)
    }

    /// Create a circuit breaker with custom cooldown duration
    pub fn with_cooldown(threshold: usize, cooldown: Duration) -> Self {
        Self::with_cooldown_and_fallback(threshold, cooldown, None, None, None)
    }

    /// Create a circuit breaker with custom cooldown and optional fallback.
    ///
    /// This is primarily used by CircuitBreakerFactory so that flows can
    /// configure domain‑specific fallback behavior via the builder API
    /// without coupling handler logic to circuit breaker internals.
    pub fn with_cooldown_and_fallback(
        threshold: usize,
        cooldown: Duration,
        fallback: Option<Arc<dyn Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync>>,
        failure_classifier: Option<FailureClassifier>,
        stage_id: Option<StageId>,
    ) -> Self {
        debug_assert!(
            threshold > 0 && threshold <= u32::MAX as usize,
            "CircuitBreaker threshold must be in 1..=u32::MAX"
        );
        let max_failures = NonZeroU32::new(threshold as u32)
            .expect("CircuitBreaker threshold must be greater than zero");
        let failure_mode = CircuitBreakerFailureMode::Consecutive { max_failures };
        Self {
            state: Arc::new(AtomicU8::new(CircuitState::Closed as u8)),
            success_count: Arc::new(AtomicUsize::new(0)),
            failure_count: Arc::new(AtomicUsize::new(0)),
            threshold,
            failure_mode,
            rate_window: None,
            cooldown,
            opened_at: Arc::new(Mutex::new(None)),
            probe_in_flight: Arc::new(AtomicU32::new(0)),
            stats: Arc::new(Mutex::new(CircuitBreakerStats {
                requests_processed: 0,
                requests_rejected: 0,
                last_summary: Instant::now(),
            })),
            last_state_change: Arc::new(Mutex::new(Instant::now())),
            stage_id,
            fallback,
            failure_classifier,
            open_policy: OpenPolicy::default(),
            half_open_policy: HalfOpenPolicy::default(),
        }
    }

    fn current_state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::SeqCst))
    }

    fn transition_to(&self, new_state: CircuitState, ctx: &mut MiddlewareContext) {
        let old_state = self.current_state();
        self.state.store(new_state as u8, Ordering::SeqCst);

        // Update last state change; if the mutex is poisoned we log and continue
        if let Ok(mut last) = self.last_state_change.lock() {
            *last = Instant::now();
        }

        // Track when we open the circuit
        if new_state == CircuitState::Open {
            if let Ok(mut opened_at) = self.opened_at.lock() {
                *opened_at = Some(Instant::now());
            }
            if let Some(stage_id) = self.stage_id {
                circuit_breaker_contract_registry::mark_stage_opened(stage_id);
            }
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
                let recovery_duration_ms = if let Ok(last) = self.last_state_change.lock() {
                    last.elapsed().as_millis() as u64
                } else {
                    0
                };

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
        if let Ok(opened_at_guard) = self.opened_at.lock() {
            if let Some(opened_at) = *opened_at_guard {
                opened_at.elapsed() >= self.cooldown
            } else {
                false
            }
        } else {
            false
        }
    }

    /// Apply an Open-like policy (Open or HalfOpen non-probe behaviour).
    fn handle_open_like(
        &self,
        event: &ChainEvent,
        ctx: &mut MiddlewareContext,
        policy: &OpenPolicy,
    ) -> MiddlewareAction {
        // Track rejection for summaries.
        if let Ok(mut stats) = self.stats.lock() {
            stats.requests_rejected += 1;
        }

        match policy {
            OpenPolicy::EmitFallback => {
                if let Some(fallback) = &self.fallback {
                    let results = (fallback)(event);
                    MiddlewareAction::Skip(results)
                } else {
                    MiddlewareAction::Skip(vec![])
                }
            }
            OpenPolicy::FailFast => {
                // For now, FailFast behaves like a pure rejection from the
                // middleware perspective (no synthetic data). Future work
                // (051b‑part‑3c / 082h) can introduce a dedicated error
                // payload or ErrorKind for this case.
                MiddlewareAction::Skip(vec![])
            }
            OpenPolicy::Skip => MiddlewareAction::Skip(vec![]),
        }
    }

    fn maybe_emit_summary(&self, ctx: &mut MiddlewareContext) {
        let mut stats = match self.stats.lock() {
            Ok(stats) => stats,
            Err(_) => {
                // If stats are poisoned we skip summary emission rather than panicking.
                return;
            }
        };

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
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
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
                    self.pre_handle(event, ctx)
                } else {
                    // Reject the request and emit event
                    let cooldown_remaining =
                        if let Ok(opened_at_guard) = self.opened_at.lock() {
                            if let Some(opened_at) = *opened_at_guard {
                                self.cooldown.saturating_sub(opened_at.elapsed())
                            } else {
                                self.cooldown
                            }
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

                    self.handle_open_like(event, ctx, &self.open_policy)
                }
            }

            CircuitState::HalfOpen => {
                // Allow up to `permitted_probes` concurrent probe requests.
                let permitted = self.half_open_policy.permitted_probes.get() as u32;
                let mut current = self.probe_in_flight.load(Ordering::SeqCst);

                loop {
                    if current >= permitted {
                        // All probe slots are in use; treat this call
                        // according to the HalfOpen on_rejected policy.
                        ctx.emit_event(
                            "circuit_breaker",
                            "rejected",
                            json!({
                                "reason": "probe_in_progress"
                            }),
                        );
                        return self.handle_open_like(
                            event,
                            ctx,
                            &self.half_open_policy.on_rejected,
                        );
                    }

                    match self.probe_in_flight.compare_exchange(
                        current,
                        current + 1,
                        Ordering::SeqCst,
                        Ordering::SeqCst,
                    ) {
                        Ok(_) => {
                            // This call successfully reserved a probe slot.
                            ctx.emit_event(
                                "circuit_breaker",
                                "probe_started",
                                json!({
                                    "reason": "testing_recovery"
                                }),
                            );
                            ctx.set_baggage("circuit_breaker.is_probe", json!(true));
                            return MiddlewareAction::Continue;
                        }
                        Err(actual) => {
                            current = actual;
                        }
                    }
                }
            }
        }
    }

    fn post_handle(
        &self,
        event: &ChainEvent,
        outputs: &[ChainEvent],
        ctx: &mut MiddlewareContext,
    ) {
        // Determine whether this call should be treated as a failure from the
        // breaker’s perspective. By default we treat any output marked with
        // ProcessingStatus::Error as a failure, but flows can override this
        // behaviour via `with_failure_classifier` when they need finer-grained
        // control (for example, ignoring pure validation failures).
        let has_error = if let Some(classifier) = &self.failure_classifier {
            classifier(event, outputs)
        } else {
            outputs.iter().any(|e| {
                matches!(
                    e.processing_info.status,
                    obzenflow_core::event::status::processing_status::ProcessingStatus::Error(_)
                )
            })
        };
        let is_success = !has_error;
        let is_probe = ctx
            .get_baggage("circuit_breaker.is_probe")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Track successful processing for metrics regardless of failure mode
        if is_success {
            if let Ok(mut stats) = self.stats.lock() {
                stats.requests_processed += 1;
            }
        }

        // Best-effort measurement of call duration based on handler results.
        // When multiple outputs are produced, we use the maximum processing
        // time as a conservative estimate.
        let call_duration: Option<Duration> = outputs
            .iter()
            .map(|e| e.processing_info.processing_time.into())
            .max();

        match self.current_state() {
            CircuitState::Closed => {
                // Update failure tracking depending on configured failure mode.
                match &self.failure_mode {
                    CircuitBreakerFailureMode::Consecutive { max_failures } => {
                        if is_success {
                            // Reset failure count on success
                            self.failure_count.store(0, Ordering::SeqCst);
                        } else {
                            // Increment failure count
                            let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;

                            if failures as u32 >= max_failures.get() {
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
                    CircuitBreakerFailureMode::RateBased {
                        window,
                        failure_rate_threshold,
                        slow_call_rate_threshold,
                        slow_call_duration_threshold,
                        minimum_calls,
                    } => {
                        let now = Instant::now();
                        let is_slow = match (slow_call_duration_threshold, call_duration) {
                            (Some(threshold), Some(dur)) => dur >= *threshold,
                            _ => false,
                        };

                        // Update rate window state.
                        if let Some(state_mutex) = &self.rate_window {
                            if let Ok(mut state) = state_mutex.lock() {
                                let capacity = state.capacity();
                                if capacity > 0 {
                                    state.push(CallSample {
                                        timestamp: now,
                                        is_failure: has_error,
                                        is_slow,
                                    });
                                }

                                // Compute effective window contents based on policy.
                                let mut observed = 0usize;
                                let mut failures = 0usize;
                                let mut slow_calls = 0usize;

                                match window {
                                    FailureWindow::Count { size } => {
                                        let max = (*size as usize).min(state.capacity());
                                        // For count-based windows we just consider the
                                        // most recent `max` samples.
                                        let mut idx = 0usize;
                                        for sample in state.iter() {
                                            if idx >= max {
                                                break;
                                            }
                                            observed += 1;
                                            if sample.is_failure {
                                                failures += 1;
                                            }
                                            if sample.is_slow {
                                                slow_calls += 1;
                                            }
                                            idx += 1;
                                        }
                                    }
                                    FailureWindow::Time { duration, .. } => {
                                        for sample in state.iter() {
                                            if now.duration_since(sample.timestamp) <= *duration {
                                                observed += 1;
                                                if sample.is_failure {
                                                    failures += 1;
                                                }
                                                if sample.is_slow {
                                                    slow_calls += 1;
                                                }
                                            }
                                        }
                                    }
                                }

                                if (observed as u32) >= minimum_calls.get() {
                                    let denom = (observed as f32).max(1.0);
                                    let failure_rate = failures as f32 / denom;
                                    let slow_rate = slow_calls as f32 / denom;

                                    let open_on_failures =
                                        failure_rate >= *failure_rate_threshold;
                                    let open_on_slow = match slow_call_rate_threshold {
                                        Some(threshold) if *threshold > 0.0 => {
                                            slow_rate >= *threshold
                                        }
                                        _ => false,
                                    };

                                    if open_on_failures || open_on_slow {
                                        self.transition_to(CircuitState::Open, ctx);
                                        ctx.emit_event(
                                            "circuit_breaker",
                                            "opened",
                                            json!({
                                                "reason": "rate_based_threshold_exceeded",
                                                "observed_calls": observed,
                                                "failure_rate": failure_rate,
                                                "slow_rate": slow_rate,
                                            }),
                                        );

                                        tracing::warn!(
                                            "Circuit breaker opened (rate-based) after {} calls (failures: {})",
                                            observed,
                                            failures
                                        );
                                    }
                                }
                            }
                        }
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

fn copy_metadata_from(target: &mut ChainEvent, source: &ChainEvent) {
    target.flow_context = source.flow_context.clone();
    target.processing_info = source.processing_info.clone();
    target.causality = source.causality.clone();
    target.correlation_id = source.correlation_id.clone();
    target.correlation_payload = source.correlation_payload.clone();
    target.runtime_context = source.runtime_context.clone();
    target.observability = source.observability.clone();
}

fn build_typed_fallback_event<In, Out, F>(f: &F, event: &ChainEvent) -> Vec<ChainEvent>
where
    In: TypedPayload + DeserializeOwned,
    Out: TypedPayload + Serialize,
    F: Fn(&In) -> Out + Send + Sync + 'static,
{
    use obzenflow_core::event::status::processing_status::ProcessingStatus;

    // 1. Deserialize input payload into In
    let payload = event.payload();
    let input: In = match serde_json::from_value(payload.clone()) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status = ProcessingStatus::error(format!(
                "cb_fallback_deserialize_failed: {err}"
            ));
            return vec![clone];
        }
    };

    // 2. Call user-provided closure
    let out = f(&input);

    // 3. Serialize Out
    let out_value = match serde_json::to_value(&out) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status = ProcessingStatus::error(format!(
                "cb_fallback_serialize_failed: {err}"
            ));
            return vec![clone];
        }
    };

    // 4. Wrap into a ChainEvent, copying metadata
    let mut ev =
        ChainEventFactory::data_event(event.writer_id.clone(), Out::EVENT_TYPE, out_value);
    copy_metadata_from(&mut ev, event);

    vec![ev]
}

/// Builder for circuit breaker middleware
pub struct CircuitBreakerBuilder {
    threshold: usize,
    cooldown: Duration,
    fallback: Option<Arc<dyn Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync>>,
    contract_mode: Option<CircuitBreakerContractMode>,
    failure_classifier: Option<FailureClassifier>,
    failure_mode: Option<CircuitBreakerFailureMode>,
    open_policy: Option<OpenPolicy>,
    half_open_policy: Option<HalfOpenPolicy>,
}

impl CircuitBreakerBuilder {
    /// Create a new circuit breaker builder with the given threshold
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            cooldown: Duration::from_secs(60),
            fallback: None,
            contract_mode: None,
            failure_classifier: None,
            failure_mode: None,
            open_policy: None,
            half_open_policy: None,
        }
    }

    /// Set the cooldown duration before attempting to close the circuit
    pub fn cooldown(mut self, duration: Duration) -> Self {
        self.cooldown = duration;
        self
    }

    /// Configure a fallback factory used when the circuit is open.
    ///
    /// The closure receives the original input event and is expected to
    /// produce a set of synthetic result events that represent a degraded
    /// but well-defined outcome (e.g., cached response, sentinel value).
    ///
    /// This keeps circuit breaker concerns encapsulated in configuration so
    /// handler implementations remain oblivious to failure policy.
    pub fn with_fallback<F>(mut self, f: F) -> Self
    where
        F: Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync + 'static,
    {
        self.fallback = Some(Arc::new(f));
        self
    }

    /// Configure how downstream contracts should interpret breaker activity
    /// for this stage.
    pub fn with_contract_mode(mut self, mode: CircuitBreakerContractMode) -> Self {
        self.contract_mode = Some(mode);
        self
    }

    /// Configure how this breaker should behave while Open.
    pub fn open_policy(mut self, policy: OpenPolicy) -> Self {
        self.open_policy = Some(policy);
        self
    }

    /// Configure how this breaker should behave while HalfOpen.
    pub fn half_open_policy(mut self, policy: HalfOpenPolicy) -> Self {
        self.half_open_policy = Some(policy);
        self
    }

    /// Configure a typed fallback factory used when the circuit is open.
    ///
    /// This adapter lets flows express degraded responses purely in terms of
    /// domain types (`In` -> `Out`) while the circuit breaker handles all
    /// `ChainEvent` plumbing (serde and metadata copying).
    pub fn with_typed_fallback<In, Out, F>(mut self, f: F) -> Self
    where
        In: TypedPayload + DeserializeOwned,
        Out: TypedPayload + Serialize,
        F: Fn(&In) -> Out + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent| -> Vec<ChainEvent> {
            build_typed_fallback_event::<In, Out, F>(&f, event)
        };

        self.fallback = Some(Arc::new(adapter));
        self
    }

    /// Configure a custom failure classifier used to decide whether a given
    /// call should be treated as a breaker failure based on the input event
    /// and the outputs produced by the handler.
    pub fn with_failure_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&ChainEvent, &[ChainEvent]) -> bool + Send + Sync + 'static,
    {
        self.failure_classifier = Some(Arc::new(f));
        self
    }

    /// Configure the failure mode explicitly. If not set, the breaker uses
    /// a simple consecutive-failure threshold equal to `threshold`.
    pub fn failure_mode_consecutive(mut self, max_failures: u32) -> Self {
        let nz = NonZeroU32::new(max_failures)
            .expect("CircuitBreaker consecutive max_failures must be greater than zero");
        self.failure_mode = Some(CircuitBreakerFailureMode::Consecutive { max_failures: nz });
        self
    }

    /// Internal helper to configure rate-based failure detection over a sliding window.
    /// User-facing APIs should use the `rate_based_over_*` helpers instead of this
    /// lower-level variant that takes a `FailureWindow` directly.
    pub(crate) fn failure_mode_rate_based_internal(
        mut self,
        window: FailureWindow,
        failure_rate_threshold: f32,
        minimum_calls: u32,
    ) -> Self {
        let minimum_calls = NonZeroU32::new(minimum_calls)
            .expect("CircuitBreaker rate-based minimum_calls must be greater than zero");
        self.failure_mode = Some(CircuitBreakerFailureMode::RateBased {
            window,
            failure_rate_threshold,
            slow_call_rate_threshold: None,
            slow_call_duration_threshold: None,
            minimum_calls,
        });
        self
    }

    /// Configure rate-based failure mode using a sliding window over the last N calls.
    /// The circuit opens when the failure rate within this window is greater than or
    /// equal to `failure_rate_threshold` (0.0 < threshold <= 1.0). By default the
    /// breaker waits for at least `window_size` calls before evaluating the threshold.
    pub fn rate_based_over_last_n_calls(
        self,
        window_size: u32,
        failure_rate_threshold: f32,
    ) -> Self {
        assert!(
            window_size > 0,
            "CircuitBreaker rate_based_over_last_n_calls: window_size must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&failure_rate_threshold) && failure_rate_threshold > 0.0,
            "CircuitBreaker rate_based_over_last_n_calls: failure_rate_threshold must be in (0.0, 1.0], got {}",
            failure_rate_threshold
        );

        self.failure_mode_rate_based_internal(
            FailureWindow::Count { size: window_size },
            failure_rate_threshold,
            window_size,
        )
    }

    /// Configure rate-based failure mode using a sliding time window. The circuit opens
    /// when the failure rate within `window_duration` is greater than or equal to
    /// `failure_rate_threshold` (0.0 < threshold <= 1.0). By default the breaker waits
    /// for at least 10 calls before evaluating the threshold; flows can override this
    /// via `minimum_calls(..)`.
    pub fn rate_based_over_duration(
        self,
        window_duration: Duration,
        failure_rate_threshold: f32,
    ) -> Self {
        assert!(
            !window_duration.is_zero(),
            "CircuitBreaker rate_based_over_duration: window_duration must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&failure_rate_threshold) && failure_rate_threshold > 0.0,
            "CircuitBreaker rate_based_over_duration: failure_rate_threshold must be in (0.0, 1.0], got {}",
            failure_rate_threshold
        );

        // Default minimum_calls for time windows; can be overridden via `minimum_calls`.
        const DEFAULT_MIN_CALLS: u32 = 10;

        self.failure_mode_rate_based_internal(
            FailureWindow::Time {
                duration: window_duration,
                minimum_calls: DEFAULT_MIN_CALLS,
            },
            failure_rate_threshold,
            DEFAULT_MIN_CALLS,
        )
    }

    /// Override the minimum number of calls before a rate-based breaker
    /// considers opening. No-op for consecutive mode.
    pub fn minimum_calls(mut self, min_calls: u32) -> Self {
        if let Some(CircuitBreakerFailureMode::RateBased {
            ref mut minimum_calls,
            ..
        }) = self.failure_mode
        {
            if let Some(nz) = NonZeroU32::new(min_calls) {
                *minimum_calls = nz;
            }
        }
        self
    }

    /// Configure slow-call contribution for rate-based failure detection.
    ///
    /// This requires a rate-based failure mode (configured via one of the
    /// `rate_based_over_*` helpers). The breaker will treat calls whose
    /// processing time is greater than or equal to `duration` as "slow",
    /// and will open when the fraction of slow calls in the window is
    /// greater than or equal to `rate_threshold`.
    pub fn slow_call(mut self, duration: Duration, rate_threshold: f32) -> Self {
        assert!(
            !duration.is_zero(),
            "CircuitBreaker slow_call: duration must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&rate_threshold) && rate_threshold > 0.0,
            "CircuitBreaker slow_call: rate_threshold must be in (0.0, 1.0], got {}",
            rate_threshold
        );

        match &mut self.failure_mode {
            Some(CircuitBreakerFailureMode::RateBased {
                slow_call_rate_threshold,
                slow_call_duration_threshold,
                ..
            }) => {
                *slow_call_rate_threshold = Some(rate_threshold);
                *slow_call_duration_threshold = Some(duration);
            }
            _ => {
                panic!(
                    "CircuitBreaker slow_call requires a rate-based failure mode; \
                     call rate_based_over_last_n_calls or rate_based_over_duration first"
                );
            }
        }

        self
    }

    /// Build the circuit breaker middleware factory
    pub fn build(self) -> Box<dyn MiddlewareFactory> {
        Box::new(CircuitBreakerFactory {
            threshold: self.threshold,
            cooldown: self.cooldown,
            fallback: self.fallback,
            contract_mode: self.contract_mode,
            failure_classifier: self.failure_classifier,
            failure_mode: self.failure_mode,
             open_policy: self.open_policy,
             half_open_policy: self.half_open_policy,
        })
    }
}

/// Factory for creating circuit breaker middleware
pub struct CircuitBreakerFactory {
    threshold: usize,
    cooldown: Duration,
    fallback: Option<Arc<dyn Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync>>,
    contract_mode: Option<CircuitBreakerContractMode>,
    failure_classifier: Option<FailureClassifier>,
    failure_mode: Option<CircuitBreakerFailureMode>,
    open_policy: Option<OpenPolicy>,
    half_open_policy: Option<HalfOpenPolicy>,
}

impl CircuitBreakerFactory {
    /// Create a new circuit breaker factory with the given threshold
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            cooldown: Duration::from_secs(60),
            fallback: None,
            contract_mode: None,
            failure_classifier: None,
            failure_mode: None,
            open_policy: None,
            half_open_policy: None,
        }
    }

    /// Set the cooldown duration before attempting to close the circuit
    pub fn with_cooldown(mut self, duration: Duration) -> Self {
        self.cooldown = duration;
        self
    }

    /// Configure the fallback factory for this circuit breaker.
    pub fn with_fallback<F>(mut self, f: F) -> Self
    where
        F: Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync + 'static,
    {
        self.fallback = Some(Arc::new(f));
        self
    }

    /// Configure the breaker-aware contract mode for this stage.
    pub fn with_contract_mode(mut self, mode: CircuitBreakerContractMode) -> Self {
        self.contract_mode = Some(mode);
        self
    }

    /// Configure a custom failure classifier used to decide whether a given
    /// call should be treated as a breaker failure based on the input event
    /// and the outputs produced by the handler.
    pub fn with_failure_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&ChainEvent, &[ChainEvent]) -> bool + Send + Sync + 'static,
    {
        self.failure_classifier = Some(Arc::new(f));
        self
    }

    /// Configure how this breaker should behave while Open.
    pub fn open_policy(mut self, policy: OpenPolicy) -> Self {
        self.open_policy = Some(policy);
        self
    }

    /// Configure how this breaker should behave while HalfOpen.
    pub fn half_open_policy(mut self, policy: HalfOpenPolicy) -> Self {
        self.half_open_policy = Some(policy);
        self
    }

    /// Configure the failure mode explicitly. If not set, the breaker uses
    /// a simple consecutive-failure threshold equal to `threshold`.
    pub fn failure_mode_consecutive(mut self, max_failures: u32) -> Self {
        let nz = NonZeroU32::new(max_failures)
            .expect("CircuitBreaker consecutive max_failures must be greater than zero");
        self.failure_mode = Some(CircuitBreakerFailureMode::Consecutive { max_failures: nz });
        self
    }

    /// Configure rate-based failure detection over a sliding window.
    pub(crate) fn failure_mode_rate_based_internal(
        mut self,
        window: FailureWindow,
        failure_rate_threshold: f32,
        minimum_calls: u32,
    ) -> Self {
        self.failure_mode = Some(CircuitBreakerFailureMode::RateBased {
            window,
            failure_rate_threshold,
            slow_call_rate_threshold: None,
            slow_call_duration_threshold: None,
            minimum_calls: NonZeroU32::new(minimum_calls)
                .expect("CircuitBreaker rate-based minimum_calls must be greater than zero"),
        });
        self
    }

    /// Override the minimum number of calls before a rate-based breaker
    /// considers opening. No-op for consecutive mode.
    pub fn minimum_calls(mut self, min_calls: u32) -> Self {
        if let Some(CircuitBreakerFailureMode::RateBased {
            ref mut minimum_calls,
            ..
        }) = self.failure_mode
        {
            if let Some(nz) = NonZeroU32::new(min_calls) {
                *minimum_calls = nz;
            }
        }
        self
    }

    /// Configure rate-based failure mode using a sliding window over the last N calls.
    pub fn rate_based_over_last_n_calls(
        self,
        window_size: u32,
        failure_rate_threshold: f32,
    ) -> Self {
        assert!(
            window_size > 0,
            "CircuitBreaker rate_based_over_last_n_calls: window_size must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&failure_rate_threshold) && failure_rate_threshold > 0.0,
            "CircuitBreaker rate_based_over_last_n_calls: failure_rate_threshold must be in (0.0, 1.0], got {}",
            failure_rate_threshold
        );

        self.failure_mode_rate_based_internal(
            FailureWindow::Count { size: window_size },
            failure_rate_threshold,
            window_size,
        )
    }

    /// Configure rate-based failure mode using a sliding time window.
    pub fn rate_based_over_duration(
        self,
        window_duration: Duration,
        failure_rate_threshold: f32,
    ) -> Self {
        assert!(
            !window_duration.is_zero(),
            "CircuitBreaker rate_based_over_duration: window_duration must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&failure_rate_threshold) && failure_rate_threshold > 0.0,
            "CircuitBreaker rate_based_over_duration: failure_rate_threshold must be in (0.0, 1.0], got {}",
            failure_rate_threshold
        );

        const DEFAULT_MIN_CALLS: u32 = 10;

        self.failure_mode_rate_based_internal(
            FailureWindow::Time {
                duration: window_duration,
                minimum_calls: DEFAULT_MIN_CALLS,
            },
            failure_rate_threshold,
            DEFAULT_MIN_CALLS,
        )
    }

    /// Configure slow-call contribution for rate-based failure detection.
    ///
    /// This requires a rate-based failure mode (configured via one of the
    /// `rate_based_over_*` helpers). The breaker will treat calls whose
    /// processing time is greater than or equal to `duration` as "slow",
    /// and will open when the fraction of slow calls in the window is
    /// greater than or equal to `rate_threshold`.
    pub fn slow_call(mut self, duration: Duration, rate_threshold: f32) -> Self {
        assert!(
            !duration.is_zero(),
            "CircuitBreaker slow_call: duration must be > 0"
        );
        assert!(
            (0.0..=1.0).contains(&rate_threshold) && rate_threshold > 0.0,
            "CircuitBreaker slow_call: rate_threshold must be in (0.0, 1.0], got {}",
            rate_threshold
        );

        match &mut self.failure_mode {
            Some(CircuitBreakerFailureMode::RateBased {
                slow_call_rate_threshold,
                slow_call_duration_threshold,
                ..
            }) => {
                *slow_call_rate_threshold = Some(rate_threshold);
                *slow_call_duration_threshold = Some(duration);
            }
            _ => {
                panic!(
                    "CircuitBreaker slow_call requires a rate-based failure mode; \
                     call rate_based_over_last_n_calls or rate_based_over_duration first"
                );
            }
        }

        self
    }
}

impl MiddlewareFactory for CircuitBreakerFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        // Determine the effective contract mode for this stage. BreakerAware
        // without any fallback configured is unsafe because it could mask
        // genuine data loss. In that case we degrade to Strict semantics and
        // emit a loud warning rather than silently registering BreakerAware.
        let mut effective_mode = self.contract_mode;
        if let Some(CircuitBreakerContractMode::BreakerAware) = self.contract_mode {
            if self.fallback.is_none() {
                tracing::warn!(
                    target: "flowip-051b-part-2",
                    stage_id = ?config.stage_id,
                    "CircuitBreaker configured with BreakerAware contract mode but no fallback; \
                     degrading to Strict mode to avoid masking genuine data loss"
                );
                effective_mode = Some(CircuitBreakerContractMode::Strict);
            }
        }

        // Determine failure mode; default to a consecutive-failure threshold
        // equal to `threshold` when not explicitly configured.
        let failure_mode = self.failure_mode.clone().unwrap_or_else(|| {
            assert!(
                self.threshold > 0 && self.threshold <= u32::MAX as usize,
                "CircuitBreaker threshold must be in 1..=u32::MAX"
            );
            let max_failures = NonZeroU32::new(self.threshold as u32)
                .expect("CircuitBreaker threshold must be greater than zero");
            CircuitBreakerFailureMode::Consecutive { max_failures }
        });

        // If rate-based mode is enabled, allocate a simple sliding window
        // state for recent call outcomes.
        let rate_window = match &failure_mode {
            CircuitBreakerFailureMode::RateBased { window, .. } => {
                let cap = match window {
                    FailureWindow::Count { size } => (*size as usize).max(1),
                    FailureWindow::Time { .. } => 1024,
                };
                Some(Mutex::new(FailureWindowState::new(cap)))
            }
            _ => None,
        };

        // Determine Open / HalfOpen policies, defaulting to the legacy
        // behaviour when not explicitly configured.
        let open_policy = self
            .open_policy
            .clone()
            .unwrap_or_else(OpenPolicy::default);
        let half_open_policy = self
            .half_open_policy
            .clone()
            .unwrap_or_else(HalfOpenPolicy::default);

        // Create middleware instance
        let mut middleware = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            self.threshold,
            self.cooldown,
            self.fallback.clone(),
            self.failure_classifier.clone(),
            Some(config.stage_id),
        );
        middleware.failure_mode = failure_mode;
        middleware.rate_window = rate_window;
        middleware.open_policy = open_policy;
        middleware.half_open_policy = half_open_policy;

        // Register its state handle in the global registry so runtime
        // strategies (e.g. CircuitBreakerSourceStrategy) can observe
        // breaker state for this stage without a direct dependency.
        circuit_breaker_registry::register_stage_state(config.stage_id, middleware.state.clone());

        // Register the configured contract mode (if any) so contract policies
        // can interpret TransportContract results in a breaker-aware way for
        // this stage's downstream edges.
        if let Some(mode) = effective_mode {
            let has_fallback = self.fallback.is_some();
            circuit_breaker_contract_registry::register_stage_mode(
                config.stage_id,
                mode,
                has_fallback,
            );
        }

        Box::new(middleware)
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
    use obzenflow_core::event::status::processing_status::ProcessingStatus;
    use obzenflow_core::time::MetricsDuration;
    use std::num::NonZeroU32;
    use std::time::Duration as StdDuration;

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
            // Mark output as an explicit error so the breaker treats this as a failure.
            let mut failed_output = create_test_event();
            failed_output.processing_info.status =
                ProcessingStatus::error("simulated_failure_closed_to_open");
            cb.post_handle(&event, &[failed_output], &mut ctx);
        }

        // Third failure should open the circuit
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx),
            MiddlewareAction::Continue
        ));
        let mut failed_output = create_test_event();
        failed_output.processing_info.status =
            ProcessingStatus::error("simulated_failure_closed_to_open");
        cb.post_handle(&event, &[failed_output], &mut ctx); // This triggers the opening

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
            let mut failed_output = create_test_event();
            failed_output.processing_info.status =
                ProcessingStatus::error("simulated_failure_success_resets");
            cb.post_handle(&event, &[failed_output], &mut ctx);
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

    #[test]
    fn builder_rate_based_over_last_n_calls_configures_mode() {
        let builder = CircuitBreakerBuilder::new(3)
            .rate_based_over_last_n_calls(100, 0.5);

        match builder.failure_mode {
            Some(CircuitBreakerFailureMode::RateBased {
                window,
                failure_rate_threshold,
                minimum_calls,
                ..
            }) => {
                match window {
                    FailureWindow::Count { size } => assert_eq!(size, 100),
                    _ => panic!("expected count-based window"),
                }
                assert!(
                    (failure_rate_threshold - 0.5).abs() < f32::EPSILON,
                    "unexpected failure_rate_threshold: {}",
                    failure_rate_threshold
                );
                assert_eq!(minimum_calls.get(), 100);
            }
            other => panic!("expected rate-based mode, got {:?}", other),
        }
    }

    #[test]
    fn builder_open_and_half_open_policies_configure() {
        let half_open = HalfOpenPolicy::new(
            NonZeroU32::new(2).unwrap(),
            OpenPolicy::Skip,
        );

        let builder = CircuitBreakerBuilder::new(3)
            .open_policy(OpenPolicy::FailFast)
            .half_open_policy(half_open);

        match builder.open_policy {
            Some(OpenPolicy::FailFast) => {}
            other => panic!("expected FailFast open policy, got {:?}", other),
        }

        match builder.half_open_policy {
            Some(policy) => {
                assert_eq!(policy.permitted_probes.get(), 2);
                assert!(matches!(policy.on_rejected, OpenPolicy::Skip));
            }
            None => panic!("expected HalfOpenPolicy to be set"),
        }
    }

    #[test]
    fn builder_rate_based_over_duration_configures_mode_with_default_min_calls() {
        let duration = StdDuration::from_secs(60);
        let builder = CircuitBreakerBuilder::new(3)
            .rate_based_over_duration(duration, 0.5);

        match builder.failure_mode {
            Some(CircuitBreakerFailureMode::RateBased {
                window,
                failure_rate_threshold,
                minimum_calls,
                ..
            }) => {
                match window {
                    FailureWindow::Time {
                        duration: win_dur,
                        minimum_calls: win_min,
                    } => {
                        assert_eq!(win_dur, duration);
                        assert_eq!(win_min, 10);
                    }
                    _ => panic!("expected time-based window"),
                }
                assert!(
                    (failure_rate_threshold - 0.5).abs() < f32::EPSILON,
                    "unexpected failure_rate_threshold: {}",
                    failure_rate_threshold
                );
                assert_eq!(minimum_calls.get(), 10);
            }
            other => panic!("expected rate-based mode, got {:?}", other),
        }
    }

    #[test]
    fn rate_based_count_window_opens_after_failure_rate_threshold() {
        // Configure a rate-based breaker with a count window of 5 and a 60% failure threshold.
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            1,
            StdDuration::from_secs(60),
            None,
            None,
            None,
        );

        cb.failure_mode = CircuitBreakerFailureMode::RateBased {
            window: FailureWindow::Count { size: 5 },
            failure_rate_threshold: 0.6,
            slow_call_rate_threshold: None,
            slow_call_duration_threshold: None,
            minimum_calls: NonZeroU32::new(5).unwrap(),
        };
        cb.rate_window = Some(Mutex::new(FailureWindowState::new(5)));

        // Pattern: F, F, S, F, S over 5 calls => 3/5 = 0.6 failures.
        let patterns = [true, true, false, true, false];

        for (idx, is_failure) in patterns.iter().enumerate() {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::new();
            let action = cb.pre_handle(&event, &mut ctx);
            assert!(
                matches!(action, MiddlewareAction::Continue),
                "expected Continue in Closed state"
            );

            let output = if *is_failure {
                let mut failed = create_test_event();
                failed.processing_info.status =
                    ProcessingStatus::error(format!("simulated_failure_{}", idx));
                vec![failed]
            } else {
                vec![create_test_event()]
            };

            cb.post_handle(&event, &output, &mut ctx);
        }

        // After the fifth call the failure rate crosses the threshold and the breaker should open.
        assert!(
            matches!(cb.current_state(), CircuitState::Open),
            "expected circuit to be Open after rate-based threshold exceeded"
        );
    }

    #[test]
    fn rate_based_slow_call_opens_after_slow_threshold() {
        // Configure a rate-based breaker with a count window of 5.
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            1,
            StdDuration::from_secs(60),
            None,
            None,
            None,
        );

        cb.failure_mode = CircuitBreakerFailureMode::RateBased {
            window: FailureWindow::Count { size: 5 },
            // Require 100% actual failures so that only slow-call rate can open.
            failure_rate_threshold: 1.0,
            slow_call_rate_threshold: Some(0.6),
            slow_call_duration_threshold: Some(StdDuration::from_millis(50)),
            minimum_calls: NonZeroU32::new(5).unwrap(),
        };
        cb.rate_window = Some(Mutex::new(FailureWindowState::new(5)));

        // Pattern: slow, slow, fast, slow, fast => 3/5 slow = 0.6
        let pattern = [true, true, false, true, false];

        for is_slow in pattern.iter() {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::new();
            let action = cb.pre_handle(&event, &mut ctx);
            assert!(
                matches!(action, MiddlewareAction::Continue),
                "expected Continue in Closed state"
            );

            let mut out = create_test_event();
            out.processing_info.processing_time = if *is_slow {
                MetricsDuration::from_millis(100)
            } else {
                MetricsDuration::from_millis(10)
            };

            cb.post_handle(&event, &[out], &mut ctx);
        }

        assert!(
            matches!(cb.current_state(), CircuitState::Open),
            "expected circuit to be Open after slow-call rate threshold exceeded"
        );
    }

    #[test]
    fn open_policy_skip_drops_requests_while_open() {
        let mut cb = CircuitBreakerMiddleware::new(1);
        // Force the breaker into the Open state without an opened_at timestamp
        // so that it does not immediately transition to HalfOpen.
        cb.state
            .store(CircuitState::Open as u8, Ordering::SeqCst);
        cb.open_policy = OpenPolicy::Skip;

        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        let action = cb.pre_handle(&event, &mut ctx);

        match action {
            MiddlewareAction::Skip(results) => assert!(results.is_empty()),
            other => panic!("expected Skip action while Open, got {:?}", other),
        }
        assert!(ctx.has_event("circuit_breaker", "rejected"));
    }

    #[test]
    fn half_open_on_rejected_uses_configured_policy() {
        let mut cb = CircuitBreakerMiddleware::new(1);
        cb.half_open_policy = HalfOpenPolicy::new(
            NonZeroU32::new(1).unwrap(),
            OpenPolicy::Skip,
        );

        // Force HalfOpen with one probe already in flight so that any
        // additional calls are treated as non-probe requests.
        cb.state
            .store(CircuitState::HalfOpen as u8, Ordering::SeqCst);
        cb.probe_in_flight.store(1, Ordering::SeqCst);

        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        let action = cb.pre_handle(&event, &mut ctx);

        match action {
            MiddlewareAction::Skip(results) => assert!(results.is_empty()),
            other => panic!(
                "expected Skip action for HalfOpen non-probe, got {:?}",
                other
            ),
        }
        assert!(ctx.has_event("circuit_breaker", "rejected"));
    }
}
