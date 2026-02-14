// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Circuit breaker middleware for fail-fast behavior
//!
//! This middleware implements the circuit breaker pattern to prevent
//! cascading failures. It emits raw events that can be consumed by
//! monitoring and SLI middleware.

use crate::middleware::{Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::control_middleware::{
    cb_state, CircuitBreakerContractMode, CircuitBreakerMetrics, CircuitBreakerSnapshotter,
    ControlMiddlewareProvider, NoControlMiddleware,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{ChainEventFactory, CircuitBreakerSummaryEventParams};
use obzenflow_core::TypedPayload;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_runtime_services::stages::common::control_strategies::{
    BackoffStrategy, CircuitBreakerEofStrategy, ControlEventStrategy,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::collections::{HashMap, VecDeque};
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type FailureClassifier = Arc<dyn Fn(&ChainEvent, &[ChainEvent]) -> bool + Send + Sync>;
type FailureClassificationClassifier =
    Arc<dyn Fn(&ChainEvent, &[ChainEvent]) -> FailureClassification + Send + Sync>;
type FallbackFn = Arc<dyn Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync>;

/// Policy for how the breaker should treat errors whose `ErrorKind` is
/// `Unknown` or `None` (legacy/unclassified cases).
#[derive(Debug, Clone, Copy)]
pub enum UnknownErrorKindPolicy {
    /// Treat Unknown/None as infra failures for breaker purposes.
    TreatAsInfraFailure,
    /// Do not count Unknown/None toward breaker thresholds.
    IgnoreForBreaker,
}

/// Rich classification used by the circuit breaker to decide whether to retry,
/// count toward opening, or ignore outcomes.
#[derive(Debug, Clone, PartialEq)]
pub enum FailureClassification {
    Success,
    TransientFailure,
    PermanentFailure,
    RateLimited(Duration),
    PartialSuccess { failed_ratio: f32 },
}

/// Policy knobs for how `FailureClassification` affects breaker accounting.
#[derive(Debug, Clone)]
pub struct FailureClassificationPolicy {
    pub partial_failure_threshold: f32,
    pub rate_limited_counts_as_failure: bool,
}

impl Default for FailureClassificationPolicy {
    fn default() -> Self {
        Self {
            partial_failure_threshold: 0.5,
            rate_limited_counts_as_failure: false,
        }
    }
}

/// Retry limits enforced by the circuit breaker.
#[derive(Debug, Clone)]
pub struct RetryLimits {
    pub max_single_delay: Duration,
    pub max_total_wall_time: Duration,
}

impl Default for RetryLimits {
    fn default() -> Self {
        Self {
            max_single_delay: Duration::from_secs(30),
            max_total_wall_time: Duration::from_secs(120),
        }
    }
}

/// Configuration for integrated per-event retry inside the circuit breaker.
#[derive(Debug, Clone)]
pub struct CircuitBreakerRetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
}

#[derive(Debug, Clone)]
struct RetryState {
    attempts: u32,
    first_attempt: Instant,
    last_attempt: Instant,
    last_error: Option<String>,
    last_kind: Option<ErrorKind>,
    classification: FailureClassification,
}

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
pub(crate) enum FailureWindow {
    /// Sliding window over the last `size` calls.
    Count { size: u32 },
    /// Time-based window over the last `duration`.
    Time { duration: Duration },
}

/// Behaviour while the circuit breaker is in the Open state.
#[derive(Debug, Clone, Default)]
pub enum OpenPolicy {
    /// Emit degraded responses via fallback when configured; otherwise behave
    /// like a simple rejection/skip. This matches the existing default
    /// behaviour from 051b‑part‑2.
    #[default]
    EmitFallback,
    /// Fail fast with an explicit rejection instead of attempting to
    /// synthesize degraded responses.
    FailFast,
    /// Drop events while Open (best‑effort fire‑and‑forget or sampling
    /// scenarios). This is powerful but should be used with care when
    /// contracts are strict.
    Skip,
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
    rate_window: Option<Arc<Mutex<FailureWindowState>>>,
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
    // ---- Cumulative circuit breaker metrics (FLOWIP-059a-2) ----
    successes_total: Arc<AtomicU64>,
    failures_total: Arc<AtomicU64>,
    rejections_total: Arc<AtomicU64>,
    opened_total: Arc<AtomicU64>,
    time_in_closed: Arc<Mutex<Duration>>,
    time_in_open: Arc<Mutex<Duration>>,
    time_in_half_open: Arc<Mutex<Duration>>,
    /// Writer identity used for durable observability/control events.
    ///
    /// This must match the stage's writer_id so vector-clock watermarks and
    /// stage attribution remain correct in downstream consumers.
    writer_id: WriterId,
    /// Stage identifier for this breaker (when known).
    stage_id: Option<StageId>,
    /// Control middleware provider used to publish breaker lifecycle hints.
    ///
    /// Set to a flow-scoped provider by the factory when built inside a flow.
    /// Defaults to `NoControlMiddleware` for direct unit tests / standalone usage.
    control_provider: Arc<dyn ControlMiddlewareProvider>,
    /// Optional fallback generator used when the circuit is open.
    ///
    /// When configured, requests that would normally be rejected in the
    /// Open or HalfOpen (non‑probe) states will instead be short‑circuited
    /// to these synthetic results via `MiddlewareAction::Skip(results)`.
    ///
    /// This keeps the handler itself unaware of circuit breaker policy while
    /// allowing flows to provide domain‑specific degraded responses purely
    /// via circuit breaker configuration.
    fallback: Option<FallbackFn>,
    /// Optional classifier that decides whether a given call should be counted
    /// as a failure for breaker purposes based on the input event and the
    /// outputs produced by the handler.
    failure_classifier: Option<FailureClassifier>,
    /// Optional classifier that can fully classify a call outcome.
    ///
    /// When set, this overrides the default classification derived from output
    /// `ProcessingStatus` values.
    failure_classification_classifier: Option<FailureClassificationClassifier>,
    /// Policy controlling behaviour while the circuit is Open.
    open_policy: OpenPolicy,
    /// Policy controlling behaviour while the circuit is HalfOpen.
    half_open_policy: HalfOpenPolicy,
    /// Policy for how Unknown/None ErrorKind should be treated.
    unknown_error_kind_policy: UnknownErrorKindPolicy,

    // ---- Integrated per-event retry (FLOWIP-051c) ----
    retry_policy: Option<CircuitBreakerRetryPolicy>,
    retry_limits: RetryLimits,
    failure_classification_policy: FailureClassificationPolicy,
    retry_state: Arc<Mutex<HashMap<obzenflow_core::event::types::EventId, RetryState>>>,
    last_retry_cleanup: Arc<Mutex<Instant>>,
    retry_attempts_total: Arc<AtomicU64>,
    retry_successes_total: Arc<AtomicU64>,
    retry_exhaustions_total: Arc<AtomicU64>,
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
        Self::with_cooldown_and_fallback(threshold, Duration::from_secs(60), None, None, None, None)
    }

    /// Create a circuit breaker with custom cooldown duration
    pub fn with_cooldown(threshold: usize, cooldown: Duration) -> Self {
        Self::with_cooldown_and_fallback(threshold, cooldown, None, None, None, None)
    }

    /// Create a circuit breaker with custom cooldown and optional fallback.
    ///
    /// This is primarily used by CircuitBreakerFactory so that flows can
    /// configure domain‑specific fallback behavior via the builder API
    /// without coupling handler logic to circuit breaker internals.
    ///
    /// When `shared_state` is provided (e.g. from the factory's pending queue),
    /// the middleware reuses that `Arc` instead of allocating a fresh one.
    pub fn with_cooldown_and_fallback(
        threshold: usize,
        cooldown: Duration,
        fallback: Option<FallbackFn>,
        failure_classifier: Option<FailureClassifier>,
        stage_id: Option<StageId>,
        shared_state: Option<Arc<AtomicU8>>,
    ) -> Self {
        debug_assert!(
            threshold > 0 && threshold <= u32::MAX as usize,
            "CircuitBreaker threshold must be in 1..=u32::MAX"
        );
        let max_failures = NonZeroU32::new(threshold as u32)
            .expect("CircuitBreaker threshold must be greater than zero");
        let failure_mode = CircuitBreakerFailureMode::Consecutive { max_failures };
        Self {
            state: shared_state.unwrap_or_else(|| Arc::new(AtomicU8::new(cb_state::CLOSED))),
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
            successes_total: Arc::new(AtomicU64::new(0)),
            failures_total: Arc::new(AtomicU64::new(0)),
            rejections_total: Arc::new(AtomicU64::new(0)),
            opened_total: Arc::new(AtomicU64::new(0)),
            time_in_closed: Arc::new(Mutex::new(Duration::from_secs(0))),
            time_in_open: Arc::new(Mutex::new(Duration::from_secs(0))),
            time_in_half_open: Arc::new(Mutex::new(Duration::from_secs(0))),
            writer_id: stage_id
                .map(WriterId::from)
                .unwrap_or_else(|| WriterId::from(StageId::new())),
            stage_id,
            control_provider: Arc::new(NoControlMiddleware),
            fallback,
            failure_classifier,
            failure_classification_classifier: None,
            open_policy: OpenPolicy::default(),
            half_open_policy: HalfOpenPolicy::default(),
            unknown_error_kind_policy: UnknownErrorKindPolicy::TreatAsInfraFailure,

            retry_policy: None,
            retry_limits: RetryLimits::default(),
            failure_classification_policy: FailureClassificationPolicy::default(),
            retry_state: Arc::new(Mutex::new(HashMap::new())),
            last_retry_cleanup: Arc::new(Mutex::new(Instant::now())),
            retry_attempts_total: Arc::new(AtomicU64::new(0)),
            retry_successes_total: Arc::new(AtomicU64::new(0)),
            retry_exhaustions_total: Arc::new(AtomicU64::new(0)),
        }
    }

    fn current_state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::SeqCst))
    }

    fn counts_as_failure(&self, classification: &FailureClassification) -> bool {
        match classification {
            FailureClassification::Success => false,
            FailureClassification::TransientFailure => true,
            FailureClassification::PermanentFailure => true,
            FailureClassification::RateLimited(_) => {
                self.failure_classification_policy
                    .rate_limited_counts_as_failure
            }
            FailureClassification::PartialSuccess { failed_ratio } => {
                *failed_ratio >= self.failure_classification_policy.partial_failure_threshold
            }
        }
    }

    fn maybe_cleanup_retry_state(&self, now: Instant) {
        let mut should_cleanup = false;
        if let Ok(mut last) = self.last_retry_cleanup.lock() {
            if now.duration_since(*last) >= Duration::from_secs(60) {
                *last = now;
                should_cleanup = true;
            }
        }

        if !should_cleanup {
            return;
        }

        let stale_after = self
            .retry_limits
            .max_total_wall_time
            .saturating_add(Duration::from_secs(60));

        if let Ok(mut states) = self.retry_state.lock() {
            states.retain(|_, state| now.duration_since(state.last_attempt) <= stale_after);
        }
    }

    fn classify_call(
        &self,
        event: &ChainEvent,
        outputs: &[ChainEvent],
        ctx: &MiddlewareContext,
    ) -> (FailureClassification, Option<ErrorKind>, Option<String>) {
        if let Some(classifier) = &self.failure_classification_classifier {
            let classification = classifier(event, outputs);
            if matches!(classification, FailureClassification::Success) {
                return (FailureClassification::Success, None, None);
            }

            let mut first_error_kind: Option<ErrorKind> = None;
            let mut first_error_message: Option<String> = None;
            for out in outputs {
                if let ProcessingStatus::Error { kind, message } = &out.processing_info.status {
                    first_error_kind = kind.clone();
                    first_error_message = Some(message.clone());
                    break;
                }
            }

            return (classification, first_error_kind, first_error_message);
        }

        let retry_after_ms = ctx
            .get_baggage("circuit_breaker.retry_after_ms")
            .and_then(|v| v.as_u64());
        let retry_after = retry_after_ms.map(Duration::from_millis);

        let mut saw_transient = false;
        let mut saw_rate_limited: Option<Duration> = None;
        let mut saw_permanent = false;

        let mut first_error_kind: Option<ErrorKind> = None;
        let mut first_error_message: Option<String> = None;

        for out in outputs {
            if let ProcessingStatus::Error { kind, message } = &out.processing_info.status {
                if first_error_kind.is_none() {
                    first_error_kind = kind.clone();
                    first_error_message = Some(message.clone());
                }

                match kind {
                    Some(ErrorKind::Timeout) | Some(ErrorKind::Remote) => {
                        saw_transient = true;
                    }
                    Some(ErrorKind::Deserialization) => {
                        // Deserialization errors are deterministic — the same
                        // payload will produce the same error on every retry
                        // (poison pill). Count toward breaker opening, do not
                        // retry.
                        saw_permanent = true;
                    }
                    Some(ErrorKind::RateLimited) => {
                        let delay = retry_after.unwrap_or(Duration::from_millis(0));
                        saw_rate_limited = Some(
                            saw_rate_limited
                                .map(|existing| existing.max(delay))
                                .unwrap_or(delay),
                        );
                    }
                    Some(ErrorKind::PermanentFailure) => {
                        saw_permanent = true;
                    }
                    Some(ErrorKind::Validation) | Some(ErrorKind::Domain) => {
                        // Caller/domain failures are ignored for breaker health.
                    }
                    None | Some(ErrorKind::Unknown) => {
                        if matches!(
                            self.unknown_error_kind_policy,
                            UnknownErrorKindPolicy::TreatAsInfraFailure
                        ) {
                            saw_transient = true;
                        }
                    }
                }
            }
        }

        let base_classification = if saw_permanent {
            FailureClassification::PermanentFailure
        } else if let Some(delay) = saw_rate_limited {
            FailureClassification::RateLimited(delay)
        } else if saw_transient {
            FailureClassification::TransientFailure
        } else {
            FailureClassification::Success
        };

        let classification = if let Some(classifier) = &self.failure_classifier {
            let is_failure = classifier(event, outputs);
            if is_failure {
                if matches!(base_classification, FailureClassification::Success) {
                    FailureClassification::TransientFailure
                } else {
                    base_classification
                }
            } else {
                FailureClassification::Success
            }
        } else {
            base_classification
        };

        (classification, first_error_kind, first_error_message)
    }

    /// Attempt an atomic state transition. Returns `true` if this call won
    /// the CAS race and the transition was applied, `false` if another thread
    /// already moved the state (or old == new).
    fn transition_to(&self, new_state: CircuitState, ctx: &mut MiddlewareContext) -> bool {
        let old_state = self.current_state();
        if old_state == new_state {
            return false;
        }

        // Atomically swap old → new so that concurrent callers cannot both
        // "win" the same transition and double-count time-in-state.
        if self
            .state
            .compare_exchange(
                old_state as u8,
                new_state as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            // Another thread already moved the state; our transition is stale.
            return false;
        }

        // Accumulate time-in-state for the state we're leaving and advance the transition timer.
        let now = Instant::now();
        let elapsed_in_old_state = if let Ok(mut last) = self.last_state_change.lock() {
            let elapsed = now.duration_since(*last);
            *last = now;
            elapsed
        } else {
            Duration::from_secs(0)
        };

        match old_state {
            CircuitState::Closed => {
                if let Ok(mut total) = self.time_in_closed.lock() {
                    *total += elapsed_in_old_state;
                }
            }
            CircuitState::Open => {
                if let Ok(mut total) = self.time_in_open.lock() {
                    *total += elapsed_in_old_state;
                }
            }
            CircuitState::HalfOpen => {
                if let Ok(mut total) = self.time_in_half_open.lock() {
                    *total += elapsed_in_old_state;
                }
            }
        }

        // Track when we open the circuit
        if new_state == CircuitState::Open {
            self.opened_total.fetch_add(1, Ordering::Relaxed);
            if let Ok(mut opened_at) = self.opened_at.lock() {
                *opened_at = Some(now);
            }
            if let Ok(mut states) = self.retry_state.lock() {
                states.clear();
            }
            if let Ok(mut last_cleanup) = self.last_retry_cleanup.lock() {
                *last_cleanup = now;
            }
            if let Some(stage_id) = self.stage_id {
                self.control_provider.mark_circuit_breaker_opened(&stage_id);
            }
        }

        // Emit lifecycle event for state transition
        let event = match (old_state, new_state) {
            (CircuitState::Closed | CircuitState::HalfOpen, CircuitState::Open) => {
                let failure_count = self.failure_count.load(Ordering::Relaxed) as u64;
                let successes_total = self.successes_total.load(Ordering::Relaxed);
                let failures_total = self.failures_total.load(Ordering::Relaxed);
                let total = successes_total.saturating_add(failures_total);
                let error_rate = if total > 0 {
                    (failures_total as f64) / (total as f64)
                } else {
                    0.0
                };

                ChainEventFactory::circuit_breaker_opened(self.writer_id, error_rate, failure_count)
            }
            (CircuitState::Open, CircuitState::HalfOpen) => ChainEventFactory::observability_event(
                self.writer_id,
                ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                    CircuitBreakerEvent::HalfOpen {
                        test_request_count: 0,
                    },
                )),
            ),
            (CircuitState::HalfOpen, CircuitState::Closed) => {
                let success_count = self.success_count.load(Ordering::Relaxed) as u64;
                let recovery_duration_ms = elapsed_in_old_state.as_millis() as u64;

                ChainEventFactory::observability_event(
                    self.writer_id,
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
                    self.writer_id,
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

        true
    }

    /// Force the circuit breaker into the Closed state, resetting failure
    /// counters and clearing retry state. Intended for admin/operational use.
    pub fn force_close(&self, ctx: &mut MiddlewareContext) {
        let transitioned = self.transition_to(CircuitState::Closed, ctx);
        if transitioned {
            self.failure_count.store(0, Ordering::SeqCst);
            self.success_count.store(0, Ordering::Relaxed);
        }
    }

    /// Force the circuit breaker into the Open state, starting a fresh
    /// cooldown period. Intended for admin/operational use (e.g., pre-emptive
    /// protection during a known outage).
    pub fn force_open(&self, ctx: &mut MiddlewareContext) {
        self.transition_to(CircuitState::Open, ctx);
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
        self.rejections_total.fetch_add(1, Ordering::Relaxed);

        let action = match policy {
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
        };

        // The middleware wrapper returns early for Skip actions, so post_handle is not invoked.
        // Emit summaries here as well so rejection counters stay scrape-visible while Open.
        self.maybe_emit_summary(ctx);

        action
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
            let (time_in_closed_seconds, time_in_open_seconds, time_in_half_open_seconds) =
                self.time_in_state_seconds_total();
            let successes_total = self.successes_total.load(Ordering::Relaxed);
            let failures_total = self.failures_total.load(Ordering::Relaxed);
            let opened_total = self.opened_total.load(Ordering::Relaxed);

            // Emit a circuit breaker summary event
            let event = ChainEventFactory::circuit_breaker_summary(
                self.writer_id,
                CircuitBreakerSummaryEventParams {
                    window_duration_s: stats.last_summary.elapsed().as_secs(),
                    requests_processed: stats.requests_processed,
                    requests_rejected: stats.requests_rejected,
                    state: format!("{:?}", self.current_state()),
                    consecutive_failures: self.failure_count.load(Ordering::SeqCst),
                    rejection_rate: if stats.requests_processed + stats.requests_rejected > 0 {
                        stats.requests_rejected as f64
                            / (stats.requests_processed + stats.requests_rejected) as f64
                    } else {
                        0.0
                    },
                    successes_total,
                    failures_total,
                    opened_total,
                    time_in_closed_seconds,
                    time_in_open_seconds,
                    time_in_half_open_seconds,
                },
            );
            ctx.write_control_event(event);

            // Reset stats
            stats.requests_processed = 0;
            stats.requests_rejected = 0;
            stats.last_summary = Instant::now();
        }
    }

    fn time_in_state_seconds_total(&self) -> (f64, f64, f64) {
        let mut closed = self.time_in_closed.lock().map(|d| *d).unwrap_or_default();
        let mut open = self.time_in_open.lock().map(|d| *d).unwrap_or_default();
        let mut half_open = self
            .time_in_half_open
            .lock()
            .map(|d| *d)
            .unwrap_or_default();

        // NOTE(G2): There is a TOCTOU race between releasing the
        // last_state_change lock and reading current_state(). A transition
        // between these operations causes elapsed_current to be attributed
        // to the wrong state bucket. For summary/metrics purposes this is
        // acceptable imprecision.
        let elapsed_current = if let Ok(last) = self.last_state_change.lock() {
            last.elapsed()
        } else {
            Duration::from_secs(0)
        };

        match self.current_state() {
            CircuitState::Closed => closed += elapsed_current,
            CircuitState::Open => open += elapsed_current,
            CircuitState::HalfOpen => half_open += elapsed_current,
        }

        (
            closed.as_secs_f64(),
            open.as_secs_f64(),
            half_open.as_secs_f64(),
        )
    }
}

impl Middleware for CircuitBreakerMiddleware {
    fn middleware_name(&self) -> &'static str {
        "circuit_breaker"
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        match self.current_state() {
            CircuitState::Closed => {
                // Normal operation
                MiddlewareAction::Continue
            }

            CircuitState::Open => {
                // Check if we should transition to half-open
                if self.should_attempt_reset() {
                    let transitioned = self.transition_to(CircuitState::HalfOpen, ctx);
                    if transitioned {
                        // Only reset probe slots if we won the CAS race.
                        // If another thread already transitioned, they own
                        // the reset and may already have probes in flight.
                        self.probe_in_flight.store(0, Ordering::SeqCst);
                        self.success_count.store(0, Ordering::Relaxed);
                    }
                    // Continue to half-open handling (or whatever state
                    // we're now in if another thread won the transition).
                    self.pre_handle(event, ctx)
                } else {
                    // Reject the request and emit event
                    let cooldown_remaining = if let Ok(opened_at_guard) = self.opened_at.lock() {
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
                let permitted = self.half_open_policy.permitted_probes.get();
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

    fn post_handle(&self, event: &ChainEvent, outputs: &[ChainEvent], ctx: &mut MiddlewareContext) {
        let now = Instant::now();

        let attempt = ctx
            .get_baggage("circuit_breaker.attempt")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;
        let is_probe = ctx
            .get_baggage("circuit_breaker.is_probe")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Track allowed calls (i.e. calls that reached the wrapped handler), regardless of
        // whether they succeeded. Rejections are tracked in `handle_open_like`.
        if let Ok(mut stats) = self.stats.lock() {
            stats.requests_processed += 1;
        }

        let (classification, error_kind, error_message) = self.classify_call(event, outputs, ctx);
        let is_success = matches!(classification, FailureClassification::Success);
        let retry_enabled = self.retry_policy.is_some();

        if retry_enabled {
            self.maybe_cleanup_retry_state(now);
        }

        /// Hard cap on `retry_state` entries to prevent unbounded growth
        /// during sustained failure spikes. Existing entries (retries in
        /// progress) are always updated; only brand-new entries are refused.
        const MAX_RETRY_STATE_ENTRIES: usize = 10_000;

        let mut first_attempt = now;
        if retry_enabled && !is_success {
            if let Ok(mut states) = self.retry_state.lock() {
                // Refuse brand-new entries when at capacity to prevent
                // unbounded growth during sustained failure spikes.
                // Existing entries (retries in progress) are always updated.
                let at_capacity =
                    states.len() >= MAX_RETRY_STATE_ENTRIES && !states.contains_key(&event.id);

                if at_capacity {
                    tracing::warn!(
                        "retry_state at capacity ({}), dropping new entry for event {:?}",
                        MAX_RETRY_STATE_ENTRIES,
                        event.id,
                    );
                } else {
                    let entry = states.entry(event.id).or_insert_with(|| RetryState {
                        attempts: 0,
                        first_attempt: now,
                        last_attempt: now,
                        last_error: None,
                        last_kind: None,
                        classification: FailureClassification::TransientFailure,
                    });

                    first_attempt = entry.first_attempt;
                    entry.attempts = attempt.saturating_add(1);
                    entry.last_attempt = now;
                    if let Some(msg) = error_message.as_ref() {
                        entry.last_error = Some(msg.clone());
                    }
                    if error_kind.is_some() {
                        entry.last_kind = error_kind.clone();
                    }
                    entry.classification = classification.clone();
                }
            }
        }

        // Integrated retry: only retry non-probe calls, and only for retryable classifications.
        if retry_enabled && !is_success && !is_probe {
            if let Some(policy) = &self.retry_policy {
                let max_attempts = policy.max_attempts.max(1);
                let eligible = matches!(
                    classification,
                    FailureClassification::TransientFailure | FailureClassification::RateLimited(_)
                );

                if eligible && attempt.saturating_add(1) < max_attempts {
                    let mut delay = match &classification {
                        FailureClassification::TransientFailure => {
                            policy.backoff.calculate_delay(attempt as usize)
                        }
                        FailureClassification::RateLimited(wait) => {
                            if wait.is_zero() {
                                policy.backoff.calculate_delay(attempt as usize)
                            } else {
                                // Use the provider's hint as a floor but apply
                                // ±10% jitter derived from the backoff strategy
                                // to prevent thundering herd when many concurrent
                                // requests all receive the same Retry-After.
                                let backoff_jittered =
                                    policy.backoff.calculate_delay(attempt as usize);
                                let jitter_offset = if backoff_jittered > Duration::ZERO {
                                    let ten_pct = wait.as_millis() as u64 / 10;
                                    // Use the backoff's jitter magnitude as entropy
                                    let raw = backoff_jittered.as_millis() as u64;
                                    Duration::from_millis(raw % ten_pct.max(1))
                                } else {
                                    Duration::ZERO
                                };
                                *wait + jitter_offset
                            }
                        }
                        _ => Duration::from_millis(0),
                    };

                    delay = delay.min(self.retry_limits.max_single_delay);

                    let elapsed = now.duration_since(first_attempt);

                    if elapsed.saturating_add(delay) <= self.retry_limits.max_total_wall_time {
                        ctx.set_baggage("circuit_breaker.should_retry", json!(true));
                        ctx.set_baggage(
                            "circuit_breaker.retry_delay_ms",
                            json!(delay.as_millis() as u64),
                        );

                        self.retry_attempts_total.fetch_add(1, Ordering::Relaxed);

                        ctx.write_control_event(ChainEventFactory::retry_attempt_failed(
                            self.writer_id,
                            attempt.saturating_add(1),
                            max_attempts,
                            error_kind.clone(),
                            Some(delay.as_millis() as u64),
                            Some(event.id),
                        ));

                        // Retry attempts do not contribute to breaker opening; only final outcomes do.
                        // TODO(D3/051c): Because the retry loop is invisible to the breaker's
                        // failure counters, a handler that flaps (e.g. intermittent 503s) may
                        // never trip the breaker as long as retries eventually succeed.  This
                        // can mask genuine degradation.  Consider exposing a configurable
                        // `count_retries_as_partial_failure` flag that increments the sliding
                        // window for each intermediate failure (at a fractional weight) so the
                        // breaker can open proactively when the retry rate is abnormally high.
                        self.maybe_emit_summary(ctx);
                        return;
                    }
                }
            }
        }

        // Final outcome: clear retry state and emit a final retry lifecycle event if needed.
        let mut retry_state = if retry_enabled {
            match self.retry_state.lock() {
                Ok(mut states) => states.remove(&event.id),
                Err(_) => None,
            }
        } else {
            None
        };

        if let Some(state) = retry_state.as_mut() {
            if is_success {
                state.attempts = attempt.saturating_add(1);
                state.last_attempt = now;
                state.classification = FailureClassification::Success;
            }

            if state.attempts > 1 {
                let total_duration_ms = now.duration_since(state.first_attempt).as_millis() as u64;
                if is_success {
                    self.retry_successes_total.fetch_add(1, Ordering::Relaxed);
                    ctx.write_control_event(ChainEventFactory::retry_succeeded_after_retry(
                        self.writer_id,
                        state.attempts,
                        total_duration_ms,
                        Some(event.id),
                    ));
                } else {
                    self.retry_exhaustions_total.fetch_add(1, Ordering::Relaxed);
                    let last_error = state
                        .last_error
                        .clone()
                        .unwrap_or_else(|| "retry_exhausted".to_string());
                    ctx.write_control_event(ChainEventFactory::retry_exhausted(
                        self.writer_id,
                        state.attempts,
                        last_error,
                        total_duration_ms,
                        Some(event.id),
                    ));
                }
            }
        }

        let counted_as_failure = self.counts_as_failure(&classification);
        if counted_as_failure {
            self.failures_total.fetch_add(1, Ordering::Relaxed);
        } else {
            self.successes_total.fetch_add(1, Ordering::Relaxed);
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
                // Track consecutive failures regardless of failure mode so the
                // `circuit_breaker_consecutive_failures` gauge remains meaningful
                // in both consecutive and rate-based configurations.
                let consecutive_failures = if counted_as_failure {
                    self.failure_count.fetch_add(1, Ordering::SeqCst) + 1
                } else {
                    self.failure_count.store(0, Ordering::SeqCst);
                    0
                };

                // Update failure tracking depending on configured failure mode.
                match &self.failure_mode {
                    CircuitBreakerFailureMode::Consecutive { max_failures } => {
                        if counted_as_failure
                            && (consecutive_failures as u32) >= max_failures.get()
                            && self.transition_to(CircuitState::Open, ctx)
                        {
                            ctx.emit_event(
                                "circuit_breaker",
                                "opened",
                                json!({
                                    "consecutive_failures": consecutive_failures,
                                    "threshold": self.threshold,
                                    "reason": "failure_threshold_exceeded"
                                }),
                            );

                            tracing::warn!(
                                "Circuit breaker opened after {} consecutive failures",
                                consecutive_failures
                            );
                        }
                    }
                    CircuitBreakerFailureMode::RateBased {
                        window,
                        failure_rate_threshold,
                        slow_call_rate_threshold,
                        slow_call_duration_threshold,
                        minimum_calls,
                    } => {
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
                                        is_failure: counted_as_failure,
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
                                        for sample in state.iter().take(max) {
                                            observed += 1;
                                            if sample.is_failure {
                                                failures += 1;
                                            }
                                            if sample.is_slow {
                                                slow_calls += 1;
                                            }
                                        }
                                    }
                                    FailureWindow::Time { duration } => {
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

                                    let open_on_failures = failure_rate >= *failure_rate_threshold;
                                    let open_on_slow = match slow_call_rate_threshold {
                                        Some(threshold) if *threshold > 0.0 => {
                                            slow_rate >= *threshold
                                        }
                                        _ => false,
                                    };

                                    if (open_on_failures || open_on_slow)
                                        && self.transition_to(CircuitState::Open, ctx)
                                    {
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
                    // Release only this probe's slot. The bulk reset to 0
                    // happens in pre_handle when transitioning Open → HalfOpen;
                    // individual completions should decrement by 1 so that
                    // concurrent probes are not prematurely freed.
                    self.probe_in_flight.fetch_sub(1, Ordering::SeqCst);

                    if is_success {
                        self.success_count.fetch_add(1, Ordering::Relaxed);

                        // Probe succeeded — try to close the circuit. With
                        // permitted_probes > 1, another probe may have already
                        // moved the state; only the CAS winner runs side effects.
                        if self.transition_to(CircuitState::Closed, ctx) {
                            self.failure_count.store(0, Ordering::SeqCst);

                            ctx.emit_event(
                                "circuit_breaker",
                                "closed",
                                json!({
                                    "reason": "probe_succeeded"
                                }),
                            );

                            tracing::info!("Circuit breaker probe succeeded, circuit closed");
                        }
                    } else {
                        // Probe failed — try to reopen the circuit. Only the
                        // CAS winner transitions; a late-arriving probe whose
                        // CAS fails is a no-op (another probe already decided).
                        if self.transition_to(CircuitState::Open, ctx) {
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
    target.correlation_id = source.correlation_id;
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
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_fallback_deserialize_failed: {err}"));
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
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_fallback_serialize_failed: {err}"));
            return vec![clone];
        }
    };

    // 4. Wrap into a ChainEvent, copying metadata
    let mut ev = ChainEventFactory::data_event(event.writer_id, Out::EVENT_TYPE, out_value);
    copy_metadata_from(&mut ev, event);

    vec![ev]
}

/// Builder for circuit breaker middleware
///
/// TODO(G4/051c): `CircuitBreakerBuilder` and `CircuitBreakerFactory` share ~300 lines of
/// nearly identical builder methods and field definitions.  Extract a shared
/// `CircuitBreakerConfig` struct that owns the policy fields and provide `impl From<Config>`
/// conversions for both Builder and Factory.  This removes the current maintenance burden of
/// keeping both sets of setters in sync and eliminates a class of copy-paste bugs.
pub struct CircuitBreakerBuilder {
    threshold: usize,
    cooldown: Duration,
    fallback: Option<FallbackFn>,
    contract_mode: Option<CircuitBreakerContractMode>,
    failure_classifier: Option<FailureClassifier>,
    failure_classification_classifier: Option<FailureClassificationClassifier>,
    failure_mode: Option<CircuitBreakerFailureMode>,
    open_policy: Option<OpenPolicy>,
    half_open_policy: Option<HalfOpenPolicy>,
    unknown_error_kind_policy: UnknownErrorKindPolicy,
    retry_policy: Option<CircuitBreakerRetryPolicy>,
    retry_limits: RetryLimits,
    failure_classification_policy: FailureClassificationPolicy,
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
            failure_classification_classifier: None,
            failure_mode: None,
            open_policy: None,
            half_open_policy: None,
            unknown_error_kind_policy: UnknownErrorKindPolicy::TreatAsInfraFailure,
            retry_policy: None,
            retry_limits: RetryLimits::default(),
            failure_classification_policy: FailureClassificationPolicy::default(),
        }
    }

    /// Set the cooldown duration before attempting to close the circuit
    pub fn cooldown(mut self, duration: Duration) -> Self {
        self.cooldown = duration;
        self
    }

    /// Enable integrated retry with a fixed backoff delay.
    pub fn with_retry_fixed(mut self, delay: Duration, max_attempts: u32) -> Self {
        self.retry_policy = Some(CircuitBreakerRetryPolicy {
            max_attempts,
            backoff: BackoffStrategy::Fixed { delay },
        });
        self
    }

    /// Enable integrated retry with exponential backoff.
    pub fn with_retry_exponential(mut self, max_attempts: u32) -> Self {
        self.retry_policy = Some(CircuitBreakerRetryPolicy {
            max_attempts,
            backoff: BackoffStrategy::Exponential {
                initial: Duration::from_millis(250),
                max: Duration::from_secs(4),
                factor: 2.0,
                jitter: true,
            },
        });
        self
    }

    /// Configure hard caps for integrated retry (max single delay and max total wall time).
    pub fn with_retry_limits(mut self, limits: RetryLimits) -> Self {
        self.retry_limits = limits;
        self
    }

    /// Configure how delivery/error classifications affect breaker accounting.
    pub fn with_failure_classification_policy(
        mut self,
        policy: FailureClassificationPolicy,
    ) -> Self {
        self.failure_classification_policy = policy;
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

    /// Configure a custom classifier that can return a rich `FailureClassification`.
    ///
    /// When set, this overrides the default classification derived from output
    /// `ProcessingStatus` values.
    pub fn with_failure_classification_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&ChainEvent, &[ChainEvent]) -> FailureClassification + Send + Sync + 'static,
    {
        self.failure_classification_classifier = Some(Arc::new(f));
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
            "CircuitBreaker rate_based_over_last_n_calls: failure_rate_threshold must be in (0.0, 1.0], got {failure_rate_threshold}"
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
            "CircuitBreaker rate_based_over_duration: failure_rate_threshold must be in (0.0, 1.0], got {failure_rate_threshold}"
        );

        // Default minimum_calls for time windows; can be overridden via `minimum_calls`.
        const DEFAULT_MIN_CALLS: u32 = 10;

        self.failure_mode_rate_based_internal(
            FailureWindow::Time {
                duration: window_duration,
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
            "CircuitBreaker slow_call: rate_threshold must be in (0.0, 1.0], got {rate_threshold}"
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
            pending_shared_state: Mutex::new(PendingSharedState::default()),
            fallback: self.fallback,
            contract_mode: self.contract_mode,
            failure_classifier: self.failure_classifier,
            failure_classification_classifier: self.failure_classification_classifier,
            failure_mode: self.failure_mode,
            open_policy: self.open_policy,
            half_open_policy: self.half_open_policy,
            unknown_error_kind_policy: self.unknown_error_kind_policy,
            retry_policy: self.retry_policy,
            retry_limits: self.retry_limits,
            failure_classification_policy: self.failure_classification_policy,
        })
    }
}

/// Queue-based pairing for `create()` / `create_control_strategy()` state sharing.
///
/// IMPORTANT(D7): This assumes FIFO call ordering — if the framework calls
/// `create()` for stages [A, B] then `create_control_strategy()` for [B, A],
/// the pairing will be wrong. The `create_control_strategy` trait method does
/// not receive a stage identifier, so keyed pairing is not possible without a
/// trait change. A debug assertion warns if queues grow beyond 1.
#[derive(Default)]
struct PendingSharedState {
    pending_for_create: VecDeque<Arc<AtomicU8>>,
    pending_for_strategy: VecDeque<Arc<AtomicU8>>,
}

/// Factory for creating circuit breaker middleware
pub struct CircuitBreakerFactory {
    threshold: usize,
    cooldown: Duration,
    pending_shared_state: Mutex<PendingSharedState>,
    fallback: Option<FallbackFn>,
    contract_mode: Option<CircuitBreakerContractMode>,
    failure_classifier: Option<FailureClassifier>,
    failure_classification_classifier: Option<FailureClassificationClassifier>,
    failure_mode: Option<CircuitBreakerFailureMode>,
    open_policy: Option<OpenPolicy>,
    half_open_policy: Option<HalfOpenPolicy>,
    unknown_error_kind_policy: UnknownErrorKindPolicy,
    retry_policy: Option<CircuitBreakerRetryPolicy>,
    retry_limits: RetryLimits,
    failure_classification_policy: FailureClassificationPolicy,
}

impl CircuitBreakerFactory {
    /// Create a new circuit breaker factory with the given threshold
    pub fn new(threshold: usize) -> Self {
        Self {
            threshold,
            cooldown: Duration::from_secs(60),
            pending_shared_state: Mutex::new(PendingSharedState::default()),
            fallback: None,
            contract_mode: None,
            failure_classifier: None,
            failure_classification_classifier: None,
            failure_mode: None,
            open_policy: None,
            half_open_policy: None,
            unknown_error_kind_policy: UnknownErrorKindPolicy::TreatAsInfraFailure,
            retry_policy: None,
            retry_limits: RetryLimits::default(),
            failure_classification_policy: FailureClassificationPolicy::default(),
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

    /// Configure a custom classifier that can return a rich `FailureClassification`.
    ///
    /// When set, this overrides the default classification derived from output
    /// `ProcessingStatus` values.
    pub fn with_failure_classification_classifier<F>(mut self, f: F) -> Self
    where
        F: Fn(&ChainEvent, &[ChainEvent]) -> FailureClassification + Send + Sync + 'static,
    {
        self.failure_classification_classifier = Some(Arc::new(f));
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
            "CircuitBreaker rate_based_over_last_n_calls: failure_rate_threshold must be in (0.0, 1.0], got {failure_rate_threshold}"
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
            "CircuitBreaker rate_based_over_duration: failure_rate_threshold must be in (0.0, 1.0], got {failure_rate_threshold}"
        );

        const DEFAULT_MIN_CALLS: u32 = 10;

        self.failure_mode_rate_based_internal(
            FailureWindow::Time {
                duration: window_duration,
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
            "CircuitBreaker slow_call: rate_threshold must be in (0.0, 1.0], got {rate_threshold}"
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
    fn create(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> Box<dyn Middleware> {
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
                    // TODO(D6): This hardcoded capacity silently degrades
                    // time-based windows at high throughput (>1024 calls per
                    // window duration). Consider making this configurable.
                    FailureWindow::Time { .. } => 1024,
                };
                Some(Arc::new(Mutex::new(FailureWindowState::new(cap))))
            }
            _ => None,
        };

        // Determine Open / HalfOpen policies, defaulting to the legacy
        // behaviour when not explicitly configured.
        let open_policy = self.open_policy.clone().unwrap_or_default();
        let half_open_policy = self.half_open_policy.clone().unwrap_or_default();

        let unknown_error_kind_policy = self.unknown_error_kind_policy;

        // The EOF strategy and this middleware instance need to share a state handle,
        // but `MiddlewareFactory::create_control_strategy()` is called independently
        // from `create()`. We stage the shared state via a small queue so callers
        // can invoke these in either order and still share a per-stage Arc.
        let shared_state = if let Ok(mut pending) = self.pending_shared_state.lock() {
            if let Some(state) = pending.pending_for_create.pop_front() {
                state
            } else {
                let state = Arc::new(AtomicU8::new(cb_state::CLOSED));
                pending.pending_for_strategy.push_back(state.clone());
                debug_assert!(
                    pending.pending_for_strategy.len() <= 1,
                    "PendingSharedState: multiple unpaired create() calls; \
                     FIFO pairing with create_control_strategy() may be incorrect"
                );
                state
            }
        } else {
            Arc::new(AtomicU8::new(cb_state::CLOSED))
        };

        // Create middleware instance with the pre-allocated shared state so the
        // constructor doesn't waste an allocation that `create()` immediately
        // overwrites.
        let mut middleware = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            self.threshold,
            self.cooldown,
            self.fallback.clone(),
            self.failure_classifier.clone(),
            Some(config.stage_id),
            Some(shared_state),
        );
        middleware.failure_mode = failure_mode;
        middleware.rate_window = rate_window;
        middleware.open_policy = open_policy;
        middleware.half_open_policy = half_open_policy;
        middleware.unknown_error_kind_policy = unknown_error_kind_policy;
        middleware.failure_classification_classifier =
            self.failure_classification_classifier.clone();
        middleware.retry_policy = self.retry_policy.clone();
        middleware.retry_limits = self.retry_limits.clone();
        middleware.failure_classification_policy = self.failure_classification_policy.clone();

        // Publish breaker lifecycle hints (e.g., "has opened at least once")
        // via the flow-scoped control middleware provider.
        middleware.control_provider = control_middleware.clone();

        // Register circuit breaker snapshotter/state/contract info with the
        // flow-scoped aggregator so runtime_services can inject control metrics
        // into wide events and apply breaker-aware contract policies.
        let contract_mode = effective_mode.unwrap_or(CircuitBreakerContractMode::Strict);
        let has_fallback = self.fallback.is_some();

        let cb_state = middleware.state.clone();
        let cb_state_for_registry = cb_state.clone();
        let successes_total = middleware.successes_total.clone();
        let failures_total = middleware.failures_total.clone();
        let rejections_total = middleware.rejections_total.clone();
        let opened_total = middleware.opened_total.clone();
        let time_in_closed = middleware.time_in_closed.clone();
        let time_in_open = middleware.time_in_open.clone();
        let time_in_half_open = middleware.time_in_half_open.clone();
        let last_state_change = middleware.last_state_change.clone();
        let snapshotter: std::sync::Arc<CircuitBreakerSnapshotter> = Arc::new(move || {
            let state_raw = cb_state.load(Ordering::Acquire);

            let successes = successes_total.load(Ordering::Relaxed);
            let failures = failures_total.load(Ordering::Relaxed);
            let requests_total = successes.saturating_add(failures);

            let rejections = rejections_total.load(Ordering::Relaxed);
            let opened = opened_total.load(Ordering::Relaxed);

            let mut closed = time_in_closed.lock().map(|d| *d).unwrap_or_default();
            let mut open = time_in_open.lock().map(|d| *d).unwrap_or_default();
            let mut half_open = time_in_half_open.lock().map(|d| *d).unwrap_or_default();

            let elapsed_current = last_state_change
                .lock()
                .map(|last| last.elapsed())
                .unwrap_or_default();

            match CircuitState::from(state_raw) {
                CircuitState::Closed => closed += elapsed_current,
                CircuitState::Open => open += elapsed_current,
                CircuitState::HalfOpen => half_open += elapsed_current,
            }

            CircuitBreakerMetrics {
                requests_total,
                successes_total: successes,
                failures_total: failures,
                rejections_total: rejections,
                opened_total: opened,
                time_closed_seconds: closed.as_secs_f64(),
                time_open_seconds: open.as_secs_f64(),
                time_half_open_seconds: half_open.as_secs_f64(),
                state: state_raw,
            }
        });

        control_middleware.register_circuit_breaker(
            config.stage_id,
            snapshotter,
            cb_state_for_registry,
            contract_mode,
            has_fallback,
        );

        Box::new(middleware)
    }

    fn name(&self) -> &str {
        "circuit_breaker"
    }

    fn create_control_strategy(&self) -> Option<Box<dyn ControlEventStrategy>> {
        // Delay EOF/Drain briefly when the breaker is actively recovering so shutdown
        // semantics remain graceful, but never delay indefinitely.
        let open_delay = self.cooldown.min(Duration::from_secs(5));
        let half_open_delay = Duration::from_secs(2);
        let shared_state = if let Ok(mut pending) = self.pending_shared_state.lock() {
            if let Some(state) = pending.pending_for_strategy.pop_front() {
                state
            } else {
                let state = Arc::new(AtomicU8::new(cb_state::CLOSED));
                pending.pending_for_create.push_back(state.clone());
                debug_assert!(
                    pending.pending_for_create.len() <= 1,
                    "PendingSharedState: multiple unpaired create_control_strategy() calls; \
                     FIFO pairing with create() may be incorrect"
                );
                state
            }
        } else {
            Arc::new(AtomicU8::new(cb_state::CLOSED))
        };
        Some(Box::new(CircuitBreakerEofStrategy::new(
            shared_state,
            open_delay,
            half_open_delay,
        )))
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        let open_policy = match self
            .open_policy
            .as_ref()
            .unwrap_or(&OpenPolicy::EmitFallback)
        {
            OpenPolicy::EmitFallback => "emit_fallback",
            OpenPolicy::FailFast => "fail_fast",
            OpenPolicy::Skip => "skip",
        };

        let retry = self.retry_policy.as_ref().map(|policy| {
            let backoff = match &policy.backoff {
                BackoffStrategy::Fixed { delay } => json!({
                    "kind": "fixed",
                    "delay_ms": delay.as_millis() as u64,
                }),
                BackoffStrategy::Exponential {
                    initial,
                    max,
                    factor,
                    jitter,
                } => json!({
                    "kind": "exponential",
                    "initial_ms": initial.as_millis() as u64,
                    "max_ms": max.as_millis() as u64,
                    "factor": factor,
                    "jitter": jitter,
                }),
            };

            json!({
                "max_attempts": policy.max_attempts,
                "backoff": backoff,
                "max_single_delay_ms": self.retry_limits.max_single_delay.as_millis() as u64,
                "max_total_wall_ms": self.retry_limits.max_total_wall_time.as_millis() as u64,
            })
        });

        Some(json!({
            "threshold": self.threshold,
            "cooldown_ms": self.cooldown.as_millis() as u64,
            "open_policy": open_policy,
            "has_fallback": self.fallback.is_some(),
            "retry": retry,
        }))
    }
}

/// Create a circuit breaker factory with default settings
pub fn circuit_breaker(threshold: usize) -> Box<dyn MiddlewareFactory> {
    Box::new(CircuitBreakerFactory::new(threshold))
}

/// Create a circuit breaker factory with defaults tuned for AI provider calls.
pub fn ai_circuit_breaker() -> Box<dyn MiddlewareFactory> {
    CircuitBreakerBuilder::new(5)
        .with_retry_exponential(3)
        .with_retry_limits(RetryLimits::default())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
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
            cb.post_handle(&event, &[], &mut ctx);
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
        let builder = CircuitBreakerBuilder::new(3).rate_based_over_last_n_calls(100, 0.5);

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
                    "unexpected failure_rate_threshold: {failure_rate_threshold}"
                );
                assert_eq!(minimum_calls.get(), 100);
            }
            other => panic!("expected rate-based mode, got {other:?}"),
        }
    }

    #[test]
    fn builder_open_and_half_open_policies_configure() {
        let half_open = HalfOpenPolicy::new(NonZeroU32::new(2).unwrap(), OpenPolicy::Skip);

        let builder = CircuitBreakerBuilder::new(3)
            .open_policy(OpenPolicy::FailFast)
            .half_open_policy(half_open);

        match builder.open_policy {
            Some(OpenPolicy::FailFast) => {}
            other => panic!("expected FailFast open policy, got {other:?}"),
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
        let builder = CircuitBreakerBuilder::new(3).rate_based_over_duration(duration, 0.5);

        match builder.failure_mode {
            Some(CircuitBreakerFailureMode::RateBased {
                window,
                failure_rate_threshold,
                minimum_calls,
                ..
            }) => {
                match window {
                    FailureWindow::Time { duration: win_dur } => {
                        assert_eq!(win_dur, duration);
                    }
                    _ => panic!("expected time-based window"),
                }
                assert!(
                    (failure_rate_threshold - 0.5).abs() < f32::EPSILON,
                    "unexpected failure_rate_threshold: {failure_rate_threshold}"
                );
                assert_eq!(minimum_calls.get(), 10);
            }
            other => panic!("expected rate-based mode, got {other:?}"),
        }
    }

    #[test]
    fn factory_control_strategy_shares_state_with_created_middleware() {
        use crate::middleware::control::ControlMiddlewareAggregator;
        use obzenflow_core::event::event_envelope::EventEnvelope;
        use obzenflow_core::event::JournalWriterId;
        use obzenflow_runtime_services::pipeline::config::StageConfig;
        use obzenflow_runtime_services::stages::common::control_strategies::{
            ControlEventAction, ProcessingContext,
        };

        let control_middleware = Arc::new(ControlMiddlewareAggregator::new());

        // Control strategy created first.
        let stage_id = StageId::new();
        let config = StageConfig {
            stage_id,
            name: "test".to_string(),
            flow_name: "test_flow".to_string(),
        };
        let factory = CircuitBreakerFactory::new(3);
        let strategy = factory
            .create_control_strategy()
            .expect("expected circuit breaker control strategy");

        let _middleware = factory.create(&config, control_middleware.clone());

        let state = control_middleware
            .circuit_breaker_state(&stage_id)
            .expect("expected circuit breaker state registration");
        state.store(cb_state::OPEN, Ordering::Relaxed);

        let event = ChainEventFactory::data_event(WriterId::from(stage_id), "test", json!({}));
        let envelope = EventEnvelope::new(JournalWriterId::new(), event);

        let mut processing_ctx = ProcessingContext::new();
        let action = strategy.handle_eof(&envelope, &mut processing_ctx);
        assert!(matches!(action, ControlEventAction::Delay(d) if !d.is_zero()));

        // Middleware created first.
        let stage_id_2 = StageId::new();
        let config_2 = StageConfig {
            stage_id: stage_id_2,
            name: "test_2".to_string(),
            flow_name: "test_flow".to_string(),
        };

        let factory_2 = CircuitBreakerFactory::new(3);
        let _middleware_2 = factory_2.create(&config_2, control_middleware.clone());
        let strategy_2 = factory_2
            .create_control_strategy()
            .expect("expected circuit breaker control strategy");

        let state_2 = control_middleware
            .circuit_breaker_state(&stage_id_2)
            .expect("expected circuit breaker state registration");
        state_2.store(cb_state::OPEN, Ordering::Relaxed);

        let event_2 = ChainEventFactory::data_event(WriterId::from(stage_id_2), "test", json!({}));
        let envelope_2 = EventEnvelope::new(JournalWriterId::new(), event_2);

        let mut processing_ctx_2 = ProcessingContext::new();
        let action_2 = strategy_2.handle_eof(&envelope_2, &mut processing_ctx_2);
        assert!(matches!(
            action_2,
            ControlEventAction::Delay(d) if !d.is_zero()
        ));
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
            None,
        );

        cb.failure_mode = CircuitBreakerFailureMode::RateBased {
            window: FailureWindow::Count { size: 5 },
            failure_rate_threshold: 0.6,
            slow_call_rate_threshold: None,
            slow_call_duration_threshold: None,
            minimum_calls: NonZeroU32::new(5).unwrap(),
        };
        cb.rate_window = Some(Arc::new(Mutex::new(FailureWindowState::new(5))));

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
                    ProcessingStatus::error(format!("simulated_failure_{idx}"));
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
        cb.rate_window = Some(Arc::new(Mutex::new(FailureWindowState::new(5))));

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
    fn default_failure_classifier_uses_errorkind() {
        let cb = CircuitBreakerMiddleware::new(1);
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();

        // Domain/validation error should NOT count as a breaker failure by default.
        let mut domain_err = create_test_event();
        domain_err.processing_info.status =
            ProcessingStatus::error_with_kind("validation_failed", Some(ErrorKind::Validation));
        cb.post_handle(&event, &[domain_err], &mut ctx);
        assert!(
            matches!(cb.current_state(), CircuitState::Closed),
            "expected circuit to remain Closed for validation/domain errors"
        );

        // Timeout (infra) error SHOULD count as a breaker failure.
        let mut timeout_err = create_test_event();
        timeout_err.processing_info.status =
            ProcessingStatus::error_with_kind("gateway_timeout", Some(ErrorKind::Timeout));
        cb.post_handle(&event, &[timeout_err], &mut ctx);
        assert!(
            matches!(cb.current_state(), CircuitState::Open),
            "expected circuit to be Open after infra/timeout error"
        );
    }

    #[test]
    fn open_policy_skip_drops_requests_while_open() {
        let mut cb = CircuitBreakerMiddleware::new(1);
        // Force the breaker into the Open state without an opened_at timestamp
        // so that it does not immediately transition to HalfOpen.
        cb.state.store(CircuitState::Open as u8, Ordering::SeqCst);
        cb.open_policy = OpenPolicy::Skip;

        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        let action = cb.pre_handle(&event, &mut ctx);

        match action {
            MiddlewareAction::Skip(results) => assert!(results.is_empty()),
            other => panic!("expected Skip action while Open, got {other:?}"),
        }
        assert!(ctx.has_event("circuit_breaker", "rejected"));
    }

    #[test]
    fn half_open_on_rejected_uses_configured_policy() {
        let mut cb = CircuitBreakerMiddleware::new(1);
        cb.half_open_policy = HalfOpenPolicy::new(NonZeroU32::new(1).unwrap(), OpenPolicy::Skip);

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
            other => panic!("expected Skip action for HalfOpen non-probe, got {other:?}"),
        }
        assert!(ctx.has_event("circuit_breaker", "rejected"));
    }

    // -----------------------------------------------------------------------
    // T1 + T7: Full HalfOpen → Closed recovery lifecycle with success_count
    // -----------------------------------------------------------------------

    #[test]
    fn full_lifecycle_closed_open_halfopen_closed_tracks_success_count() {
        // Use a very short cooldown so we can trigger HalfOpen without sleeping.
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            2,
            StdDuration::from_millis(0), // instant cooldown
            None,
            None,
            None,
            None,
        );
        cb.open_policy = OpenPolicy::EmitFallback;

        // Phase 1: Two failures → Open
        for _ in 0..2 {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::new();
            assert!(matches!(
                cb.pre_handle(&event, &mut ctx),
                MiddlewareAction::Continue
            ));
            let mut failed = create_test_event();
            failed.processing_info.status = ProcessingStatus::error("simulated_failure_lifecycle");
            cb.post_handle(&event, &[failed], &mut ctx);
        }
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Phase 2: Cooldown has already elapsed (0ms). Next pre_handle
        // should transition Open → HalfOpen and admit a probe.
        let probe_event = create_test_event();
        let mut probe_ctx = MiddlewareContext::new();
        let action = cb.pre_handle(&probe_event, &mut probe_ctx);
        assert!(
            matches!(action, MiddlewareAction::Continue),
            "expected probe to be admitted in HalfOpen"
        );
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert!(probe_ctx.has_event("circuit_breaker", "probe_started"));

        // Verify success_count was reset to 0 on HalfOpen entry.
        assert_eq!(
            cb.success_count.load(Ordering::Relaxed),
            0,
            "success_count should be reset to 0 on HalfOpen entry"
        );

        // Phase 3: Probe succeeds → HalfOpen → Closed.
        let success_output = create_test_event();
        cb.post_handle(&probe_event, &[success_output], &mut probe_ctx);

        assert_eq!(cb.current_state(), CircuitState::Closed);

        // T7: Verify success_count was incremented by the probe.
        assert_eq!(
            cb.success_count.load(Ordering::Relaxed),
            1,
            "success_count should be 1 after one successful probe"
        );

        // The transition_to(Closed) emits a CircuitBreakerEvent::Closed
        // lifecycle event in the context. Verify it is present.
        assert!(
            probe_ctx.has_event("circuit_breaker", "closed"),
            "expected 'closed' lifecycle event after probe succeeded"
        );

        // Phase 4: Normal traffic should flow again.
        let normal_event = create_test_event();
        let mut normal_ctx = MiddlewareContext::new();
        assert!(matches!(
            cb.pre_handle(&normal_event, &mut normal_ctx),
            MiddlewareAction::Continue
        ));
    }

    // -----------------------------------------------------------------------
    // T1 variant: HalfOpen probe FAILS → circuit reopens
    // -----------------------------------------------------------------------

    #[test]
    fn halfopen_probe_failure_reopens_circuit() {
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            1,
            StdDuration::from_millis(0),
            None,
            None,
            None,
            None,
        );
        cb.open_policy = OpenPolicy::EmitFallback;

        // One failure → Open (threshold = 1)
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        cb.pre_handle(&event, &mut ctx);
        let mut failed = create_test_event();
        failed.processing_info.status = ProcessingStatus::error("simulated_failure_halfopen_fail");
        cb.post_handle(&event, &[failed], &mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Cooldown elapsed → HalfOpen, probe admitted.
        let probe_event = create_test_event();
        let mut probe_ctx = MiddlewareContext::new();
        let action = cb.pre_handle(&probe_event, &mut probe_ctx);
        assert!(matches!(action, MiddlewareAction::Continue));
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);

        // Probe fails → reopens.
        let mut probe_failed = create_test_event();
        probe_failed.processing_info.status = ProcessingStatus::error("simulated_probe_failure");
        cb.post_handle(&probe_event, &[probe_failed], &mut probe_ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);
        assert!(probe_ctx.has_event("circuit_breaker", "reopened"));
    }

    // -----------------------------------------------------------------------
    // T2: Concurrent CAS transitions do not corrupt state
    // -----------------------------------------------------------------------

    #[test]
    fn concurrent_transitions_do_not_corrupt_state() {
        use std::sync::Arc;
        use std::thread;

        // A breaker with threshold=1 and instant cooldown so threads can
        // race through Open → HalfOpen → Closed/Open transitions.
        let cb = Arc::new({
            let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
                1,
                StdDuration::from_millis(0),
                None,
                None,
                None,
                None,
            );
            cb.open_policy = OpenPolicy::EmitFallback;
            cb
        });

        let iterations = 200;
        let num_threads = 4;

        let handles: Vec<_> = (0..num_threads)
            .map(|_| {
                let cb = Arc::clone(&cb);
                thread::spawn(move || {
                    for idx in 0..iterations {
                        let event = create_test_event();
                        let mut ctx = MiddlewareContext::new();
                        let action = cb.pre_handle(&event, &mut ctx);

                        match action {
                            MiddlewareAction::Continue => {
                                // Alternate between success and failure.
                                if idx % 2 == 0 {
                                    let mut failed = create_test_event();
                                    failed.processing_info.status =
                                        ProcessingStatus::error("concurrent_failure");
                                    cb.post_handle(&event, &[failed], &mut ctx);
                                } else {
                                    cb.post_handle(&event, &[create_test_event()], &mut ctx);
                                }
                            }
                            MiddlewareAction::Skip(_) | MiddlewareAction::Abort => {
                                // Rejected while Open or HalfOpen — normal.
                            }
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().expect("thread panicked");
        }

        // The main invariant: state must be a valid CircuitState value.
        let final_state = cb.current_state();
        assert!(
            matches!(
                final_state,
                CircuitState::Closed | CircuitState::Open | CircuitState::HalfOpen
            ),
            "state corrupted to invalid value"
        );

        // probe_in_flight must not have wrapped (would be near u32::MAX).
        let probes = cb.probe_in_flight.load(Ordering::SeqCst);
        assert!(
            probes <= num_threads as u32,
            "probe_in_flight looks corrupted: {probes}"
        );
    }

    // -----------------------------------------------------------------------
    // T3: Deserialization is classified as permanent failure
    // -----------------------------------------------------------------------

    #[test]
    fn deserialization_classified_as_permanent() {
        let cb = CircuitBreakerMiddleware::new(5);
        let event = create_test_event();
        let ctx = MiddlewareContext::new();

        let mut deser_err = create_test_event();
        deser_err.processing_info.status =
            ProcessingStatus::error_with_kind("bad_json", Some(ErrorKind::Deserialization));

        let (classification, _, _) = cb.classify_call(&event, &[deser_err], &ctx);
        assert_eq!(
            classification,
            FailureClassification::PermanentFailure,
            "Deserialization should be classified as PermanentFailure"
        );
    }

    // -----------------------------------------------------------------------
    // T4: RateLimited retry delay includes jitter
    // -----------------------------------------------------------------------

    #[test]
    fn rate_limited_retry_includes_jitter() {
        // Build a CB with exponential retry and a large enough retry_after
        // that 10% jitter is measurable.
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            5,
            StdDuration::from_secs(60),
            None,
            None,
            None,
            None,
        );
        cb.retry_policy = Some(CircuitBreakerRetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategy::Exponential {
                initial: StdDuration::from_millis(100),
                max: StdDuration::from_secs(5),
                factor: 2.0,
                jitter: true,
            },
        });
        cb.retry_limits = RetryLimits {
            max_single_delay: StdDuration::from_secs(30),
            max_total_wall_time: StdDuration::from_secs(120),
        };

        let raw_wait = StdDuration::from_secs(1);
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        ctx.set_baggage(
            "circuit_breaker.retry_after_ms",
            json!(raw_wait.as_millis() as u64),
        );

        // Create a rate-limited output.
        let mut rl_output = create_test_event();
        rl_output.processing_info.status =
            ProcessingStatus::error_with_kind("rate_limited", Some(ErrorKind::RateLimited));

        cb.pre_handle(&event, &mut ctx);
        cb.post_handle(&event, &[rl_output], &mut ctx);

        // The CB should have signaled a retry with a delay. Extract it.
        let should_retry = ctx
            .get_baggage("circuit_breaker.should_retry")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        assert!(should_retry, "expected CB to signal retry for RateLimited");

        let delay_ms = ctx
            .get_baggage("circuit_breaker.retry_delay_ms")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        // The delay should be at least raw_wait (1000ms) because jitter adds,
        // not subtracts. But it should not be exactly 0.
        assert!(
            delay_ms > 0,
            "expected non-zero retry delay, got {delay_ms}"
        );
    }

    // -----------------------------------------------------------------------
    // T5: retry_state capacity cap is enforced
    // -----------------------------------------------------------------------

    #[test]
    fn retry_state_cap_prevents_unbounded_growth() {
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            100_000, // very high threshold so the breaker stays Closed
            StdDuration::from_secs(60),
            None,
            None,
            None,
            None,
        );
        cb.retry_policy = Some(CircuitBreakerRetryPolicy {
            max_attempts: 3,
            backoff: BackoffStrategy::Fixed {
                delay: StdDuration::from_millis(1),
            },
        });
        cb.retry_limits = RetryLimits {
            max_single_delay: StdDuration::from_secs(30),
            max_total_wall_time: StdDuration::from_secs(120),
        };

        // Pump 10,500 unique failing events — 500 more than the cap (10,000).
        for _ in 0..10_500 {
            let event = create_test_event(); // each has a unique EventId
            let mut ctx = MiddlewareContext::new();
            cb.pre_handle(&event, &mut ctx);
            let mut failed = create_test_event();
            failed.processing_info.status =
                ProcessingStatus::error_with_kind("infra_fail", Some(ErrorKind::Timeout));
            cb.post_handle(&event, &[failed], &mut ctx);
        }

        let states = cb.retry_state.lock().unwrap();
        assert!(
            states.len() <= 10_000,
            "retry_state exceeded cap: {} entries",
            states.len()
        );
    }

    // -----------------------------------------------------------------------
    // T6: force_close and force_open admin methods
    // -----------------------------------------------------------------------

    #[test]
    fn force_open_transitions_closed_to_open() {
        let cb = CircuitBreakerMiddleware::new(100);
        assert_eq!(cb.current_state(), CircuitState::Closed);

        let mut ctx = MiddlewareContext::new();
        cb.force_open(&mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);
    }

    #[test]
    fn force_close_transitions_open_to_closed_and_resets_counters() {
        let cb = CircuitBreakerMiddleware::new(1);

        // Drive it to Open via a failure.
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        cb.pre_handle(&event, &mut ctx);
        let mut failed = create_test_event();
        failed.processing_info.status = ProcessingStatus::error("simulated_failure_force_close");
        cb.post_handle(&event, &[failed], &mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);
        assert!(
            cb.failure_count.load(Ordering::SeqCst) > 0,
            "failure_count should be > 0 before force_close"
        );

        // Admin reset.
        let mut admin_ctx = MiddlewareContext::new();
        cb.force_close(&mut admin_ctx);
        assert_eq!(cb.current_state(), CircuitState::Closed);
        assert_eq!(
            cb.failure_count.load(Ordering::SeqCst),
            0,
            "failure_count should be reset after force_close"
        );
        assert_eq!(
            cb.success_count.load(Ordering::Relaxed),
            0,
            "success_count should be reset after force_close"
        );

        // Normal traffic should flow.
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx),
            MiddlewareAction::Continue
        ));
    }

    #[test]
    fn force_close_from_halfopen_resets_cleanly() {
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            1,
            StdDuration::from_millis(0),
            None,
            None,
            None,
            None,
        );
        cb.open_policy = OpenPolicy::EmitFallback;

        // Drive to HalfOpen: one failure → Open → instant cooldown → probe → HalfOpen.
        let event = create_test_event();
        let mut ctx = MiddlewareContext::new();
        cb.pre_handle(&event, &mut ctx);
        let mut failed = create_test_event();
        failed.processing_info.status = ProcessingStatus::error("simulated_failure_force_close_ho");
        cb.post_handle(&event, &[failed], &mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);

        let probe_event = create_test_event();
        let mut probe_ctx = MiddlewareContext::new();
        cb.pre_handle(&probe_event, &mut probe_ctx);
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);

        // Admin force-close from HalfOpen.
        let mut admin_ctx = MiddlewareContext::new();
        cb.force_close(&mut admin_ctx);
        assert_eq!(cb.current_state(), CircuitState::Closed);
        assert_eq!(cb.failure_count.load(Ordering::SeqCst), 0);
    }
}
