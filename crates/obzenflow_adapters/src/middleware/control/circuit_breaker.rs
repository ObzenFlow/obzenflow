// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Circuit breaker middleware for fail-fast behavior
//!
//! This middleware implements the circuit breaker pattern to prevent
//! cascading failures. It emits raw events that can be consumed by
//! monitoring and SLI middleware.

use crate::middleware::{
    context_keys::{
        CircuitBreakerAttempt, CircuitBreakerIsProbe, CircuitBreakerProbeGeneration,
        CircuitBreakerProbeSlot, CircuitBreakerProbeSlotGuard, CircuitBreakerRetryAfterMs,
        CircuitBreakerRetryDelayMs, CircuitBreakerShouldRetry,
    },
    ControlMiddlewareRole, Middleware, MiddlewareAbortCause, MiddlewareAction, MiddlewareContext,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareHints, MiddlewareOverrideKey,
    MiddlewarePlanContribution, MiddlewareSafety, SourceMiddlewarePhase,
    TopologyMiddlewareConfigSlot,
};
use obzenflow_core::control_middleware::{
    cb_state, CircuitBreakerMetrics, CircuitBreakerSnapshotter,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerRejectionReason, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::schema::TypedFactType;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{
    ChainEventFactory, CircuitBreakerSummaryEventParams, EffectFailureCode, EffectFailureSource,
    RetryDisposition,
};
use obzenflow_core::MiddlewareExecutionScope;
use obzenflow_core::TypedPayload;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use thiserror::Error;

type FailureClassifier = Arc<dyn Fn(&ChainEvent, &[ChainEvent]) -> bool + Send + Sync>;
type FailureClassificationClassifier =
    Arc<dyn Fn(&ChainEvent, &[ChainEvent]) -> FailureClassification + Send + Sync>;
type FallbackFn = Arc<dyn Fn(&ChainEvent) -> Vec<ChainEvent> + Send + Sync>;
type RejectionFn =
    Arc<dyn Fn(&ChainEvent, CircuitBreakerRejectionReason) -> Vec<ChainEvent> + Send + Sync>;

const CIRCUIT_BREAKER_ABORT_SOURCE: &str = "circuit_breaker";

#[derive(Debug, Clone, Copy)]
enum CircuitBreakerAbortCode {
    CircuitOpen,
    ProbeInProgress,
    Rejected,
}

impl CircuitBreakerAbortCode {
    fn from_rejection_reason(reason: CircuitBreakerRejectionReason) -> Self {
        match reason {
            CircuitBreakerRejectionReason::CircuitOpen => Self::CircuitOpen,
            CircuitBreakerRejectionReason::ProbeInProgress => Self::ProbeInProgress,
            CircuitBreakerRejectionReason::Unknown => Self::Rejected,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::CircuitOpen => "rejected_circuit_open",
            Self::ProbeInProgress => "rejected_probe_in_progress",
            Self::Rejected => "rejected",
        }
    }

    fn effect_code(self) -> EffectFailureCode {
        EffectFailureCode::new(self.as_str())
    }

    fn message(self) -> String {
        format!(
            "circuit breaker rejected effect execution: {}",
            self.as_str()
        )
    }
}

/// Typed-outcome configuration (FLOWIP-120h): when the breaker is declared in
/// the `output_middleware:` lane and the handler performs the guarded
/// wrapper, a rejection synthesizes the author-named rejection fact instead of
/// aborting, so the recorded group is success-shaped and replays as the
/// `Rejected` branch.
#[derive(Clone)]
pub(crate) struct TypedOutcomeConfig {
    pub(crate) build_rejection: RejectionFn,
}

pub struct CircuitBreakerFamily;

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

/// Circuit breaker middleware that prevents cascading failures.
///
/// FLOWIP-114o (no-refund note): the breaker and the rate limiter are
/// independent instances with independent buckets. A breaker rejection does not
/// refund a rate-limiter token already consumed earlier in the chain: an event
/// that the limiter admitted (and possibly delayed) and that the breaker then
/// rejects still counted its limiter admission. Admission accounting is
/// per-policy and is not reconciled across policies.
pub struct CircuitBreakerMiddleware {
    /// Current state of the circuit breaker
    state: Arc<AtomicU8>,
    /// Number of consecutive successes
    success_count: Arc<AtomicUsize>,
    /// Number of consecutive failures
    failure_count: Arc<AtomicUsize>,
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
    /// Serialises half-open epoch changes with probe admission.
    probe_gate: Arc<Mutex<()>>,
    /// Monotonic epoch for half-open probe outcomes.
    probe_generation: Arc<AtomicU64>,
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
    /// Optional fallback generator used when the circuit is open.
    ///
    /// When configured, requests that would normally be rejected in the
    /// Open or HalfOpen (non‑probe) states will instead be short‑circuited
    /// to these synthetic results via `MiddlewareAction::Skip { results, .. }`.
    ///
    /// This keeps the handler itself unaware of circuit breaker policy while
    /// allowing flows to provide domain‑specific degraded responses purely
    /// via circuit breaker configuration.
    fallback: Option<FallbackFn>,
    /// Typed-outcome mode (FLOWIP-120h): rejection branch synthesis for
    /// stages that perform the guarded wrapper.
    typed_outcome: Option<TypedOutcomeConfig>,
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

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
enum CircuitBreakerThresholdError {
    #[error("CircuitBreaker threshold must be in 1..=u32::MAX, got {threshold}")]
    InvalidThreshold { threshold: usize },
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
    /// When `shared_state` is provided, the middleware reuses that `Arc`
    /// instead of allocating a fresh one.
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
            failure_mode,
            rate_window: None,
            cooldown,
            opened_at: Arc::new(Mutex::new(None)),
            probe_in_flight: Arc::new(AtomicU32::new(0)),
            probe_gate: Arc::new(Mutex::new(())),
            probe_generation: Arc::new(AtomicU64::new(0)),
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
            fallback,
            typed_outcome: None,
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

        let retry_after_ms = ctx.get::<CircuitBreakerRetryAfterMs>().copied();
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
    ///
    /// At the live effect boundary a rejection without fallback data must abort
    /// with a structured cause so the runtime records a failure under the
    /// effect cursor and strict replay reproduces the same rejection. An empty
    /// skip there would leave the input with no recorded outcome (FLOWIP-120h).
    fn handle_open_like(
        &self,
        event: &ChainEvent,
        ctx: &mut MiddlewareContext,
        policy: &OpenPolicy,
        reason: CircuitBreakerRejectionReason,
    ) -> MiddlewareAction {
        // Track rejection for summaries.
        if let Ok(mut stats) = self.stats.lock() {
            stats.requests_rejected += 1;
        }
        self.rejections_total.fetch_add(1, Ordering::Relaxed);

        let at_effect_boundary =
            ctx.execution_scope() == MiddlewareExecutionScope::LiveEffectBoundary;

        // Typed-outcome mode (FLOWIP-120h): at the effect boundary a rejection
        // synthesizes the author-named rejection fact, a success-shaped group
        // the guarded carrier decodes as `Rejected`, so the input completes
        // and strict replay reconstructs the same branch.
        let typed_rejection = |reason: CircuitBreakerRejectionReason| -> Option<MiddlewareAction> {
            if !at_effect_boundary {
                return None;
            }
            self.typed_outcome
                .as_ref()
                .map(|typed| MiddlewareAction::Skip {
                    results: (typed.build_rejection)(event, reason),
                    cause: None,
                })
        };

        let action = match policy {
            OpenPolicy::EmitFallback => {
                if let Some(fallback) = &self.fallback {
                    let results = (fallback)(event);
                    MiddlewareAction::Skip {
                        results,
                        cause: None,
                    }
                } else if let Some(action) = typed_rejection(reason) {
                    action
                } else if at_effect_boundary {
                    MiddlewareAction::Abort {
                        cause: Some(self.rejection_abort_cause(reason)),
                    }
                } else {
                    MiddlewareAction::Skip {
                        results: vec![],
                        cause: None,
                    }
                }
            }
            OpenPolicy::FailFast => {
                if let Some(action) = typed_rejection(reason) {
                    action
                } else if at_effect_boundary {
                    MiddlewareAction::Abort {
                        cause: Some(self.rejection_abort_cause(reason)),
                    }
                } else {
                    MiddlewareAction::Skip {
                        results: vec![],
                        cause: None,
                    }
                }
            }
            OpenPolicy::Skip => {
                if at_effect_boundary {
                    // Transport truncation is incoherent at the effect boundary;
                    // build validation rejects this configuration on effectful
                    // stages, and this arm is the defensive backstop.
                    MiddlewareAction::Abort {
                        cause: Some(self.rejection_abort_cause(reason)),
                    }
                } else {
                    MiddlewareAction::Skip {
                        results: vec![],
                        cause: None,
                    }
                }
            }
        };

        // The middleware wrapper returns early for Skip actions, so post_handle is not invoked.
        // Emit summaries here as well so rejection counters stay scrape-visible while Open.
        self.maybe_emit_summary(ctx);

        action
    }

    fn rejection_abort_cause(&self, reason: CircuitBreakerRejectionReason) -> MiddlewareAbortCause {
        let code = CircuitBreakerAbortCode::from_rejection_reason(reason);
        MiddlewareAbortCause {
            source: EffectFailureSource::new(CIRCUIT_BREAKER_ABORT_SOURCE),
            code: code.effect_code(),
            message: code.message(),
            retry: RetryDisposition::Retryable,
            event: None,
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
    fn label(&self) -> &'static str {
        "circuit_breaker"
    }

    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::CircuitBreakerGate
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Policy
    }

    fn as_effect_policy(
        self: std::sync::Arc<Self>,
    ) -> Option<std::sync::Arc<dyn crate::middleware::EffectPolicy>> {
        Some(self)
    }

    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // FLOWIP-120a: during deterministic replay the stage is reconstructed
        // from recorded events. The guarded effect is suppressed (its recorded
        // outcome is returned before the effect boundary is consulted), so this
        // handler-level breaker must not reject, reserve a probe slot, transition
        // state, or emit. The live run already recorded the breaker's decisions.
        // Live effect execution runs through the effect boundary under
        // `LiveEffectBoundary`, which is not deterministic replay, so this never
        // disables protection of a live effect.
        if ctx.execution_scope().is_deterministic_replay() {
            return MiddlewareAction::Continue;
        }

        match self.current_state() {
            CircuitState::Closed => {
                // Normal operation
                MiddlewareAction::Continue
            }

            CircuitState::Open => {
                // Check if we should transition to half-open
                if self.should_attempt_reset() {
                    {
                        let _probe_gate = self.probe_gate.lock().ok();
                        let transitioned = self.transition_to(CircuitState::HalfOpen, ctx);
                        if transitioned {
                            // Start a new half-open epoch. Do not reset
                            // probe_in_flight here: probes from the previous
                            // epoch may still be running and must release
                            // their own slots instead of racing with a bulk
                            // counter reset.
                            self.probe_generation.fetch_add(1, Ordering::SeqCst);
                            self.success_count.store(0, Ordering::Relaxed);
                        }
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

                    ctx.emit_ephemeral_event(ChainEventFactory::observability_event(
                        self.writer_id,
                        ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                            CircuitBreakerEvent::Rejected {
                                reason: CircuitBreakerRejectionReason::CircuitOpen,
                                cooldown_remaining_ms: Some(cooldown_remaining.as_millis() as u64),
                                circuit_open_duration_ms: None,
                            },
                        )),
                    ));

                    self.handle_open_like(
                        event,
                        ctx,
                        &self.open_policy,
                        CircuitBreakerRejectionReason::CircuitOpen,
                    )
                }
            }

            CircuitState::HalfOpen => {
                // Allow up to `permitted_probes` concurrent probe requests.
                let _probe_gate = self.probe_gate.lock().ok();
                let probe_generation = self.probe_generation.load(Ordering::SeqCst);
                let permitted = self.half_open_policy.permitted_probes.get();
                let mut current = self.probe_in_flight.load(Ordering::SeqCst);

                loop {
                    if current >= permitted {
                        // All probe slots are in use; treat this call
                        // according to the HalfOpen on_rejected policy.
                        ctx.emit_ephemeral_event(ChainEventFactory::observability_event(
                            self.writer_id,
                            ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                                CircuitBreakerEvent::Rejected {
                                    reason: CircuitBreakerRejectionReason::ProbeInProgress,
                                    cooldown_remaining_ms: None,
                                    circuit_open_duration_ms: None,
                                },
                            )),
                        ));
                        return self.handle_open_like(
                            event,
                            ctx,
                            &self.half_open_policy.on_rejected,
                            CircuitBreakerRejectionReason::ProbeInProgress,
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
                            ctx.insert::<CircuitBreakerIsProbe>(true);
                            ctx.insert::<CircuitBreakerProbeGeneration>(probe_generation);
                            ctx.insert::<CircuitBreakerProbeSlot>(
                                CircuitBreakerProbeSlotGuard::new(self.probe_in_flight.clone()),
                            );
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
        // FLOWIP-120a: replay reconstruction observes recorded outputs, not a live
        // call, so the breaker must not classify the outcome, count successes or
        // failures, push to the rate window, transition state, run retry logic, or
        // emit lifecycle/summary records. All of that was recorded by the live run.
        if ctx.execution_scope().is_deterministic_replay() {
            return;
        }

        let now = Instant::now();

        let attempt = ctx.get::<CircuitBreakerAttempt>().copied().unwrap_or(0);
        let is_probe = ctx.get::<CircuitBreakerIsProbe>().copied().unwrap_or(false);

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
                        ctx.insert::<CircuitBreakerShouldRetry>(true);
                        ctx.insert::<CircuitBreakerRetryDelayMs>(delay.as_millis() as u64);

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

        if is_probe {
            let probe_generation = ctx.get::<CircuitBreakerProbeGeneration>().copied();
            let _probe_gate = self.probe_gate.lock().ok();
            drop(ctx.remove::<CircuitBreakerProbeSlot>());

            if probe_generation == Some(self.probe_generation.load(Ordering::SeqCst))
                && matches!(self.current_state(), CircuitState::HalfOpen)
            {
                if is_success {
                    self.success_count.fetch_add(1, Ordering::Relaxed);

                    // Probe succeeded — try to close the circuit. With
                    // permitted_probes > 1, another probe may have already
                    // moved the state; only the CAS winner runs side effects.
                    if self.transition_to(CircuitState::Closed, ctx) {
                        self.failure_count.store(0, Ordering::SeqCst);

                        tracing::info!("Circuit breaker probe succeeded, circuit closed");
                    }
                } else {
                    // Probe failed — try to reopen the circuit. Only the
                    // CAS winner transitions; a late-arriving probe whose
                    // CAS fails is a no-op (another probe already decided).
                    if self.transition_to(CircuitState::Open, ctx) {
                        tracing::warn!("Circuit breaker probe failed, circuit reopened");
                    }
                }
            }

            self.maybe_emit_summary(ctx);
            return;
        }

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
                // Non-probe calls cannot be admitted while HalfOpen.
            }

            CircuitState::Open => {
                // Nothing to do in post-handle for open state
            }
        }

        // Check if we should emit a summary
        self.maybe_emit_summary(ctx);
    }
}

/// Per-effect policy adapter (FLOWIP-120c): the breaker guards one declared
/// effect, reusing its admission state machine and outcome classification.
/// Admission never waits, so the sync `pre_handle` is reused directly under
/// the boundary scope the policy chain establishes.
#[async_trait::async_trait]
impl crate::middleware::EffectPolicy for CircuitBreakerMiddleware {
    fn label(&self) -> &'static str {
        Middleware::label(self)
    }

    async fn admit(
        &self,
        _identity: &obzenflow_runtime::effects::EffectIdentity,
        event: &ChainEvent,
        ctx: &mut MiddlewareContext,
    ) -> crate::middleware::PolicyAdmission {
        match self.pre_handle(event, ctx) {
            MiddlewareAction::Continue => crate::middleware::PolicyAdmission::Admit,
            MiddlewareAction::Skip { results, cause } => {
                crate::middleware::PolicyAdmission::Synthesize { results, cause }
            }
            MiddlewareAction::Abort { cause } => {
                crate::middleware::PolicyAdmission::Reject(cause.unwrap_or_else(|| {
                    self.rejection_abort_cause(CircuitBreakerRejectionReason::CircuitOpen)
                }))
            }
        }
    }

    fn observe(
        &self,
        _identity: &obzenflow_runtime::effects::EffectIdentity,
        event: &ChainEvent,
        attempt: &crate::middleware::EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match attempt {
            crate::middleware::EffectAttemptOutcome::Executed(Ok(outputs)) => {
                self.post_handle(event, outputs, ctx);
            }
            crate::middleware::EffectAttemptOutcome::Executed(Err(err)) => {
                let error_event = event
                    .clone()
                    .mark_as_error(err.to_string(), ErrorKind::Remote);
                self.post_handle(event, std::slice::from_ref(&error_event), ctx);
            }
            crate::middleware::EffectAttemptOutcome::SkippedBy(_)
            | crate::middleware::EffectAttemptOutcome::RejectedBy(_) => {
                // The protected call never went out: nothing to classify.
                // The probe-slot guard releases with the context.
            }
        }
    }
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

    // 4. Wrap into a ChainEvent derived from the guarded input, so causality
    //    records the input as the fallback's parent (FLOWIP-120h lineage fix).
    let event_type = Out::versioned_event_type();
    let ev = ChainEventFactory::derived_data_event(event.writer_id, event, &event_type, out_value);

    vec![ev]
}

/// Build the outcome-shaped fallback events (FLOWIP-120m): the closure's
/// `E::Outcome` carrier lowers through `into_facts`, one derived data event
/// per fact, each parented on the protected input. The runtime's boundary
/// skip path validates the group against the effect's declared carrier and
/// commits it with contiguous outcome ordinals and middleware-synthesized
/// origin, so multi-fact product outcomes ride the ordinary grouped path.
fn build_outcome_fallback_events<E, In, F>(f: &F, event: &ChainEvent) -> Vec<ChainEvent>
where
    E: obzenflow_runtime::effects::Effect,
    In: TypedPayload + DeserializeOwned,
    F: Fn(&In) -> E::Outcome + Send + Sync + 'static,
{
    use obzenflow_core::event::schema::TypedFactSet;
    use obzenflow_core::event::status::processing_status::ProcessingStatus;

    let payload = event.payload();
    let input: In = match serde_json::from_value(payload.clone()) {
        Ok(value) => value,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_outcome_fallback_deserialize_failed: {err}"));
            return vec![clone];
        }
    };

    let outcome = f(&input);
    let facts = match outcome.into_facts() {
        Ok(facts) => facts,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_outcome_fallback_serialize_failed: {err}"));
            return vec![clone];
        }
    };

    facts
        .into_iter()
        .map(|fact| {
            ChainEventFactory::derived_data_event(
                event.writer_id,
                event,
                fact.event_type.as_str(),
                fact.payload,
            )
        })
        .collect()
}

/// Build the typed rejection fact for typed-outcome mode (FLOWIP-120h),
/// derived from the guarded input so causality records it as the parent.
fn build_typed_rejection_event<In, R, F>(
    f: &F,
    event: &ChainEvent,
    reason: CircuitBreakerRejectionReason,
) -> Vec<ChainEvent>
where
    In: TypedPayload + DeserializeOwned,
    R: TypedPayload + Serialize,
    F: Fn(&In, CircuitBreakerRejectionReason) -> R + Send + Sync + 'static,
{
    use obzenflow_core::event::status::processing_status::ProcessingStatus;

    let payload = event.payload();
    let input: In = match serde_json::from_value(payload.clone()) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_rejection_deserialize_failed: {err}"));
            return vec![clone];
        }
    };

    let rejection = f(&input, reason);

    let rejection_value = match serde_json::to_value(&rejection) {
        Ok(v) => v,
        Err(err) => {
            let mut clone = event.clone();
            clone.processing_info.status =
                ProcessingStatus::error(format!("cb_rejection_serialize_failed: {err}"));
            return vec![clone];
        }
    };

    let event_type = R::versioned_event_type();
    let ev =
        ChainEventFactory::derived_data_event(event.writer_id, event, &event_type, rejection_value);

    vec![ev]
}

/// Builder for circuit breaker middleware
///
/// Captured configuration of an outcome-shaped fallback (FLOWIP-120m): which
/// effect the closure targets and the carrier fact types it may synthesize.
struct OutcomeFallbackConfig {
    effect_type: &'static str,
    fact_types: Vec<TypedFactType>,
}

/// TODO(G4/051c): `CircuitBreakerBuilder` and `CircuitBreakerFactory` share ~300 lines of
/// nearly identical builder methods and field definitions.  Extract a shared
/// `CircuitBreakerConfig` struct that owns the policy fields and provide `impl From<Config>`
/// conversions for both Builder and Factory.  This removes the current maintenance burden of
/// keeping both sets of setters in sync and eliminates a class of copy-paste bugs.
pub struct CircuitBreakerBuilder {
    threshold: usize,
    cooldown: Duration,
    fallback: Option<FallbackFn>,
    fallback_fact_type: Option<TypedFactType>,
    typed_outcome: Option<TypedOutcomeConfig>,
    rejection_fact_type: Option<TypedFactType>,
    outcome_fallback: Option<OutcomeFallbackConfig>,
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
            fallback_fact_type: None,
            typed_outcome: None,
            rejection_fact_type: None,
            outcome_fallback: None,
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

    /// Configure the branch-shaped fallback fact (FLOWIP-120h, renamed by
    /// FLOWIP-120m from `with_typed_fallback`).
    ///
    /// This adapter lets flows express degraded responses purely in terms of
    /// domain types (`In` -> `Out`) while the circuit breaker handles all
    /// `ChainEvent` plumbing (serde and metadata copying). `In` is the
    /// protected input payload, never an output contract member; `Out` is the
    /// distinct branch fact the carrier decodes as the `Fallback` branch.
    pub fn with_fallback_fact<In, Out, F>(mut self, f: F) -> Self
    where
        In: TypedPayload + DeserializeOwned,
        Out: TypedPayload + Serialize + 'static,
        F: Fn(&In) -> Out + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent| -> Vec<ChainEvent> {
            build_typed_fallback_event::<In, Out, F>(&f, event)
        };

        self.fallback = Some(Arc::new(adapter));
        self.fallback_fact_type = Some(TypedFactType::of::<Out>());
        self
    }

    /// Configure the branch-shaped rejection fact (FLOWIP-120h, renamed by
    /// FLOWIP-120m from `typed_outcome_with`). When the breaker rejects
    /// without fallback data at the effect boundary in typed-outcome mode,
    /// this closure synthesizes the author-named rejection fact from the
    /// guarded input and the rejection reason. The reason must end up in the
    /// fact payload; strict replay reconstructs branches from facts alone.
    pub fn with_rejection_fact<In, R, F>(mut self, f: F) -> Self
    where
        In: TypedPayload + DeserializeOwned,
        R: TypedPayload + Serialize + 'static,
        F: Fn(&In, CircuitBreakerRejectionReason) -> R + Send + Sync + 'static,
    {
        let adapter =
            move |event: &ChainEvent, reason: CircuitBreakerRejectionReason| -> Vec<ChainEvent> {
                build_typed_rejection_event::<In, R, F>(&f, event, reason)
            };

        self.typed_outcome = Some(TypedOutcomeConfig {
            build_rejection: Arc::new(adapter),
        });
        self.rejection_fact_type = Some(TypedFactType::of::<R>());
        self
    }

    /// Configure the outcome-shaped fallback (FLOWIP-120m): while the breaker
    /// prevents the call, synthesize the protected effect's own outcome
    /// carrier instead of a distinct branch fact. The closure returns
    /// `E::Outcome`; the carrier lowers through `into_facts`, so multi-fact
    /// product outcomes commit as ordinary recorded groups. The handler
    /// performs the plain effect, with no `Guarded` wrapper: every branch
    /// resumes `fx.perform` with an outcome value.
    ///
    /// This is for fallbacks that are genuine alternative outcomes (a cached
    /// decision, a stubbed authorization in a test profile). A fallback that
    /// records non-performance must stay branch-shaped and never manufacture
    /// an outcome value.
    pub fn with_outcome_fallback<E, In, F>(mut self, f: F) -> Self
    where
        E: obzenflow_runtime::effects::Effect,
        In: TypedPayload + DeserializeOwned,
        F: Fn(&In) -> E::Outcome + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent| -> Vec<ChainEvent> {
            build_outcome_fallback_events::<E, In, F>(&f, event)
        };

        self.fallback = Some(Arc::new(adapter));
        self.outcome_fallback = Some(OutcomeFallbackConfig {
            effect_type: E::EFFECT_TYPE,
            fact_types: <E::Outcome as obzenflow_core::event::schema::TypedFactSet>::fact_types(),
        });
        self
    }

    /// Build for the `output_middleware:` macro lane (FLOWIP-120h), naming
    /// the fallback and rejection branch fact types. Both branches must have
    /// producers: `with_fallback_fact` for `F` and `with_rejection_fact` for
    /// `R`, with matching fact types. A mismatch is reported as a flow build
    /// error rather than a silent misconfiguration.
    pub fn build_typed<F, R>(self) -> crate::middleware::TypeShapingMiddleware<F, R>
    where
        F: TypedPayload + 'static,
        R: TypedPayload + 'static,
    {
        let expected_fallback = TypedFactType::of::<F>();
        let expected_rejection = TypedFactType::of::<R>();

        let config_error = match (&self.fallback_fact_type, &self.rejection_fact_type) {
            _ if self.outcome_fallback.is_some() => Some(
                "a breaker declares one fallback shape: with_outcome_fallback requires \
                 build_outcome, not build_typed"
                    .to_string(),
            ),
            (None, _) => Some(
                "build_typed requires with_fallback_fact so the Fallback branch has a producer"
                    .to_string(),
            ),
            (_, None) => Some(
                "build_typed requires with_rejection_fact so the Rejected branch has a producer"
                    .to_string(),
            ),
            (Some(fallback), _) if fallback.event_type != expected_fallback.event_type => {
                Some(format!(
                    "with_fallback_fact produces '{}' but build_typed names fallback '{}'",
                    fallback.event_type, expected_fallback.event_type,
                ))
            }
            (_, Some(rejection)) if rejection.event_type != expected_rejection.event_type => {
                Some(format!(
                    "with_rejection_fact produces '{}' but build_typed names rejection '{}'",
                    rejection.event_type, expected_rejection.event_type,
                ))
            }
            _ => None,
        };

        let factory = self.build();
        crate::middleware::TypeShapingMiddleware::new(factory, "circuit_breaker", config_error)
    }

    /// Build for the `output_middleware:` macro lane as an outcome-shaped
    /// registration (FLOWIP-120m). The breaker may synthesize the protected
    /// effect's own outcome facts; the handler performs the plain effect.
    /// Branch-shaped producers cannot combine with this shape on one breaker.
    pub fn build_outcome<E>(self) -> crate::middleware::OutcomeShapingMiddleware
    where
        E: obzenflow_runtime::effects::Effect,
    {
        let config_error = match &self.outcome_fallback {
            None => Some(
                "build_outcome requires with_outcome_fallback so the synthesized outcome has \
                 a producer"
                    .to_string(),
            ),
            Some(config) if config.effect_type != E::EFFECT_TYPE => Some(format!(
                "with_outcome_fallback targets effect '{}' but build_outcome names '{}'",
                config.effect_type,
                E::EFFECT_TYPE,
            )),
            Some(_)
                if self.fallback_fact_type.is_some()
                    || self.rejection_fact_type.is_some()
                    || self.typed_outcome.is_some() =>
            {
                Some(
                    "a breaker declares one fallback shape: with_outcome_fallback cannot \
                     combine with branch-shaped producers (with_fallback_fact / \
                     with_rejection_fact)"
                        .to_string(),
                )
            }
            Some(_) => None,
        };

        let registration = obzenflow_runtime::effects::SynthesizedOutcomeRegistration {
            effect_type: Some(E::EFFECT_TYPE.to_string()),
            fact_types: self
                .outcome_fallback
                .as_ref()
                .map(|config| config.fact_types.clone())
                .unwrap_or_default(),
            source_label: "circuit_breaker".to_string(),
            kind: obzenflow_runtime::effects::SynthesizedOutcomeKind::OutcomeShaped,
        };
        let factory = self.build();
        crate::middleware::OutcomeShapingMiddleware::new(factory, registration, config_error)
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
            fallback: self.fallback,
            typed_outcome: self.typed_outcome,
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

/// Factory for creating circuit breaker middleware
pub struct CircuitBreakerFactory {
    threshold: usize,
    cooldown: Duration,
    fallback: Option<FallbackFn>,
    typed_outcome: Option<TypedOutcomeConfig>,
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
            fallback: None,
            typed_outcome: None,
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

    fn validated_threshold(&self) -> Result<NonZeroU32, CircuitBreakerThresholdError> {
        u32::try_from(self.threshold)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or(CircuitBreakerThresholdError::InvalidThreshold {
                threshold: self.threshold,
            })
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

impl CircuitBreakerFactory {
    /// Shared materialization for stage-level and per-effect instances
    /// (FLOWIP-120c): the key decides where the snapshotter registers.
    fn create_keyed(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
        effect_type: Option<String>,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        let validated_threshold = self.validated_threshold().map_err(|err| {
            MiddlewareFactoryError::invalid_configuration(self.label(), &config.name, err)
        })?;

        // Determine failure mode; default to a consecutive-failure threshold
        // equal to `threshold` when not explicitly configured.
        let failure_mode =
            self.failure_mode
                .clone()
                .unwrap_or(CircuitBreakerFailureMode::Consecutive {
                    max_failures: validated_threshold,
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

        let mut middleware = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            self.threshold,
            self.cooldown,
            self.fallback.clone(),
            self.failure_classifier.clone(),
            Some(config.stage_id),
            None,
        );
        middleware.failure_mode = failure_mode;
        middleware.rate_window = rate_window;
        middleware.typed_outcome = self.typed_outcome.clone();
        middleware.open_policy = open_policy;
        middleware.half_open_policy = half_open_policy;
        middleware.unknown_error_kind_policy = unknown_error_kind_policy;
        middleware.failure_classification_classifier =
            self.failure_classification_classifier.clone();
        middleware.retry_policy = self.retry_policy.clone();
        middleware.retry_limits = self.retry_limits.clone();
        middleware.failure_classification_policy = self.failure_classification_policy.clone();

        // Register circuit breaker snapshotter/state with the flow-scoped
        // aggregator so runtime_services can inject control metrics into wide
        // events and source strategies can observe breaker state.
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

        match effect_type {
            Some(effect_type) => control_middleware.register_circuit_breaker_for_effect(
                config.stage_id,
                effect_type,
                snapshotter,
                cb_state_for_registry,
            ),
            None => control_middleware.register_circuit_breaker(
                config.stage_id,
                snapshotter,
                cb_state_for_registry,
            ),
        }

        Ok(Box::new(middleware))
    }
}

impl MiddlewareFactory for CircuitBreakerFactory {
    fn label(&self) -> &'static str {
        "circuit_breaker"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<CircuitBreakerFamily>("circuit_breaker")
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        ControlMiddlewareRole::CircuitBreaker
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Policy
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        MiddlewarePlanContribution::None
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(TopologyMiddlewareConfigSlot::CircuitBreaker)
    }

    fn create(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        self.create_keyed(config, control_middleware, None)
    }

    fn create_for_effect(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<super::ControlMiddlewareAggregator>,
        effect_type: &str,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        self.create_keyed(config, control_middleware, Some(effect_type.to_string()))
    }

    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Advanced
    }

    fn hints(&self) -> MiddlewareHints {
        // Circuit breakers can embed retry behaviour; publish a retry hint when configured.
        let retry = self.retry_policy.as_ref().map(|policy| {
            use crate::middleware::{Attempts, BackoffKind, RetryHint};
            let backoff = match &policy.backoff {
                BackoffStrategy::Fixed { delay } => BackoffKind::Fixed { delay: *delay },
                BackoffStrategy::Exponential {
                    initial,
                    max,
                    factor,
                    jitter: _,
                } => BackoffKind::Exponential {
                    base: *initial,
                    factor: *factor,
                    max: Some(*max),
                },
            };

            RetryHint {
                max_attempts: Attempts::Finite(policy.max_attempts.max(1) as usize),
                backoff,
            }
        });

        let effect_boundary_truncates = matches!(self.open_policy, Some(OpenPolicy::Skip))
            || self
                .half_open_policy
                .as_ref()
                .is_some_and(|policy| matches!(policy.on_rejected, OpenPolicy::Skip));

        MiddlewareHints {
            retry,
            effect_boundary_truncates,
            ..Default::default()
        }
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
    use obzenflow_core::control_middleware::ControlMiddlewareProvider;
    use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
    use obzenflow_core::time::MetricsDuration;
    use std::num::NonZeroU32;
    use std::time::Duration as StdDuration;

    fn create_test_event() -> ChainEvent {
        ChainEventFactory::data_event(WriterId::from(StageId::new()), "test", json!({}))
    }

    // ── FLOWIP-120m: outcome-shaped fallback ────────────────────────────────

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct OutcomeInput {
        value: u64,
    }

    impl TypedPayload for OutcomeInput {
        const EVENT_TYPE: &'static str = "cb_outcome.input";
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct OutcomeFirst {
        value: u64,
    }

    impl TypedPayload for OutcomeFirst {
        const EVENT_TYPE: &'static str = "cb_outcome.first";
    }

    #[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
    struct OutcomeSecond {
        value: u64,
    }

    impl TypedPayload for OutcomeSecond {
        const EVENT_TYPE: &'static str = "cb_outcome.second";
    }

    #[derive(Clone, Debug, PartialEq, obzenflow_core::EffectOutcomeFacts)]
    struct DemoProductOutcome {
        first: OutcomeFirst,
        second: OutcomeSecond,
    }

    #[derive(Clone, Debug)]
    struct DemoOutcomeEffect;

    #[async_trait::async_trait]
    impl obzenflow_runtime::effects::Effect for DemoOutcomeEffect {
        const EFFECT_TYPE: &'static str = "cb_outcome.demo";
        const SCHEMA_VERSION: u32 = 1;
        const SAFETY: obzenflow_runtime::effects::EffectSafety =
            obzenflow_runtime::effects::EffectSafety::Idempotent;

        type Outcome = DemoProductOutcome;

        fn label(&self) -> &str {
            "demo_outcome"
        }

        fn canonical_input(&self) -> serde_json::Value {
            json!({})
        }

        async fn execute(
            &self,
            _ctx: &mut obzenflow_runtime::effects::EffectContext,
        ) -> Result<Self::Outcome, obzenflow_runtime::effects::EffectError> {
            Ok(DemoProductOutcome {
                first: OutcomeFirst { value: 1 },
                second: OutcomeSecond { value: 2 },
            })
        }
    }

    #[test]
    fn build_outcome_requires_outcome_fallback_producer() {
        let (_, registration, config_error) = CircuitBreakerBuilder::new(1)
            .open_policy(OpenPolicy::EmitFallback)
            .build_outcome::<DemoOutcomeEffect>()
            .into_registration_parts();

        assert_eq!(
            registration.kind,
            obzenflow_runtime::effects::SynthesizedOutcomeKind::OutcomeShaped
        );
        let error = config_error.expect("missing producer must be a config error");
        assert!(error.contains("with_outcome_fallback"), "got: {error}");
    }

    #[test]
    fn build_outcome_rejects_effect_type_mismatch() {
        #[derive(Clone, Debug)]
        struct OtherEffect;

        #[async_trait::async_trait]
        impl obzenflow_runtime::effects::Effect for OtherEffect {
            const EFFECT_TYPE: &'static str = "cb_outcome.other";
            const SCHEMA_VERSION: u32 = 1;
            const SAFETY: obzenflow_runtime::effects::EffectSafety =
                obzenflow_runtime::effects::EffectSafety::Idempotent;

            type Outcome = OutcomeFirst;

            fn label(&self) -> &str {
                "other"
            }

            fn canonical_input(&self) -> serde_json::Value {
                json!({})
            }

            async fn execute(
                &self,
                _ctx: &mut obzenflow_runtime::effects::EffectContext,
            ) -> Result<Self::Outcome, obzenflow_runtime::effects::EffectError> {
                Ok(OutcomeFirst { value: 1 })
            }
        }

        let (_, _, config_error) = CircuitBreakerBuilder::new(1)
            .open_policy(OpenPolicy::EmitFallback)
            .with_outcome_fallback::<DemoOutcomeEffect, OutcomeInput, _>(|input| {
                DemoProductOutcome {
                    first: OutcomeFirst {
                        value: input.value + 900,
                    },
                    second: OutcomeSecond {
                        value: input.value + 1900,
                    },
                }
            })
            .build_outcome::<OtherEffect>()
            .into_registration_parts();

        let error = config_error.expect("effect-type mismatch must be a config error");
        assert!(error.contains("targets effect"), "got: {error}");
    }

    #[test]
    fn build_outcome_rejects_mixed_shape_builder() {
        let (_, _, config_error) = CircuitBreakerBuilder::new(1)
            .open_policy(OpenPolicy::EmitFallback)
            .with_fallback_fact::<OutcomeInput, OutcomeFirst, _>(|input| OutcomeFirst {
                value: input.value + 900,
            })
            .with_outcome_fallback::<DemoOutcomeEffect, OutcomeInput, _>(|input| {
                DemoProductOutcome {
                    first: OutcomeFirst {
                        value: input.value + 900,
                    },
                    second: OutcomeSecond {
                        value: input.value + 1900,
                    },
                }
            })
            .build_outcome::<DemoOutcomeEffect>()
            .into_registration_parts();

        let error = config_error.expect("mixing fallback shapes must be a config error");
        assert!(error.contains("one fallback shape"), "got: {error}");
    }

    #[test]
    fn outcome_fallback_builds_one_derived_event_per_fact() {
        let input_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            OutcomeInput::EVENT_TYPE,
            json!(OutcomeInput { value: 7 }),
        );

        let closure = |input: &OutcomeInput| DemoProductOutcome {
            first: OutcomeFirst {
                value: input.value + 900,
            },
            second: OutcomeSecond {
                value: input.value + 1900,
            },
        };
        let events = build_outcome_fallback_events::<DemoOutcomeEffect, OutcomeInput, _>(
            &closure,
            &input_event,
        );

        assert_eq!(events.len(), 2, "one derived event per carrier fact");
        assert_eq!(events[0].event_type(), "cb_outcome.first.v1");
        assert_eq!(events[1].event_type(), "cb_outcome.second.v1");
        assert_eq!(events[0].payload()["value"], 907);
        assert_eq!(events[1].payload()["value"], 1907);
        for event in &events {
            assert!(
                event.causality.parent_ids.contains(&input_event.id),
                "fallback facts must be parented on the protected input"
            );
        }
    }

    fn ctx_has_rejection(ctx: &MiddlewareContext) -> bool {
        ctx.ephemeral_events().iter().any(|event| {
            matches!(
                &event.content,
                obzenflow_core::event::ChainEventContent::Observability(
                    ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                        CircuitBreakerEvent::Rejected { .. }
                    ))
                )
            )
        })
    }

    #[test]
    fn test_circuit_breaker_closed_to_open() {
        let cb = CircuitBreakerMiddleware::new(3);

        // First 2 failures shouldn't open the circuit
        for _ in 0..2 {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::live_handler();
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
        let mut ctx = MiddlewareContext::live_handler();
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
        let mut ctx = MiddlewareContext::live_handler();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx),
            MiddlewareAction::Skip { .. }
        ));
        assert!(ctx_has_rejection(&ctx));
    }

    #[test]
    fn test_circuit_breaker_success_resets_count() {
        let cb = CircuitBreakerMiddleware::new(3);

        // Two failures
        for _ in 0..2 {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::live_handler();
            let _ = cb.pre_handle(&event, &mut ctx);
            let mut failed_output = create_test_event();
            failed_output.processing_info.status =
                ProcessingStatus::error("simulated_failure_success_resets");
            cb.post_handle(&event, &[failed_output], &mut ctx);
        }

        // Success should reset the count
        let event = create_test_event();
        let mut ctx = MiddlewareContext::live_handler();
        let _ = cb.pre_handle(&event, &mut ctx);
        let outputs = vec![create_test_event()]; // Non-empty = success
        cb.post_handle(&event, &outputs, &mut ctx);

        // Should now need 3 more failures to open
        for _ in 0..2 {
            let event = create_test_event();
            let mut ctx = MiddlewareContext::live_handler();
            assert!(matches!(
                cb.pre_handle(&event, &mut ctx),
                MiddlewareAction::Continue
            ));
            cb.post_handle(&event, &[], &mut ctx);
        }

        // Still closed
        let event = create_test_event();
        let mut ctx = MiddlewareContext::live_handler();
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
    fn factory_does_not_export_control_strategy() {
        use crate::middleware::control::ControlMiddlewareAggregator;
        use obzenflow_runtime::pipeline::config::StageConfig;

        let control_middleware = Arc::new(ControlMiddlewareAggregator::new());

        let stage_id = StageId::new();
        let config = StageConfig {
            stage_id,
            name: "test".to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
        };
        let factory = CircuitBreakerFactory::new(3);
        assert!(
            !factory.control_points().signal,
            "circuit breaker should not register a signal control point"
        );

        let _middleware = factory
            .create(&config, control_middleware.clone())
            .expect("circuit breaker middleware should materialize");

        assert!(
            !factory.control_points().signal,
            "circuit breaker still registers no signal control point after materialization"
        );

        let _state = control_middleware
            .circuit_breaker_state(&stage_id)
            .expect("expected circuit breaker state registration");
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
            let mut ctx = MiddlewareContext::live_handler();
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
            let mut ctx = MiddlewareContext::live_handler();
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
        let mut ctx = MiddlewareContext::live_handler();

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
        let mut ctx = MiddlewareContext::live_handler();
        let action = cb.pre_handle(&event, &mut ctx);

        match action {
            MiddlewareAction::Skip { results, .. } => assert!(results.is_empty()),
            other => panic!("expected Skip action while Open, got {other:?}"),
        }
        assert!(ctx_has_rejection(&ctx));
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
        let mut ctx = MiddlewareContext::live_handler();
        let action = cb.pre_handle(&event, &mut ctx);

        match action {
            MiddlewareAction::Skip { results, .. } => assert!(results.is_empty()),
            other => panic!("expected Skip action for HalfOpen non-probe, got {other:?}"),
        }
        assert!(ctx_has_rejection(&ctx));
    }

    #[test]
    fn halfopen_probe_slot_is_released_when_context_drops_before_post_handle() {
        let mut cb = CircuitBreakerMiddleware::new(1);
        cb.half_open_policy = HalfOpenPolicy::new(NonZeroU32::new(1).unwrap(), OpenPolicy::Skip);

        cb.state
            .store(CircuitState::HalfOpen as u8, Ordering::SeqCst);
        cb.probe_in_flight.store(0, Ordering::SeqCst);

        let event = create_test_event();

        {
            let mut ctx = MiddlewareContext::live_handler();
            assert!(matches!(
                cb.pre_handle(&event, &mut ctx),
                MiddlewareAction::Continue
            ));
            assert_eq!(ctx.get::<CircuitBreakerIsProbe>().copied(), Some(true));
        }

        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 0);

        let mut ctx2 = MiddlewareContext::live_handler();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx2),
            MiddlewareAction::Continue
        ));
    }

    #[test]
    fn stale_halfopen_probe_does_not_corrupt_new_generation() {
        let mut cb = CircuitBreakerMiddleware::with_cooldown_and_fallback(
            1,
            StdDuration::from_millis(0),
            None,
            None,
            None,
            None,
        );
        cb.open_policy = OpenPolicy::Skip;
        cb.half_open_policy = HalfOpenPolicy::new(NonZeroU32::new(1).unwrap(), OpenPolicy::Skip);

        let event = create_test_event();
        let mut ctx = MiddlewareContext::live_handler();
        assert!(matches!(
            cb.pre_handle(&event, &mut ctx),
            MiddlewareAction::Continue
        ));
        let mut failed = create_test_event();
        failed.processing_info.status = ProcessingStatus::error("initial_failure");
        cb.post_handle(&event, &[failed], &mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);

        let stale_probe = create_test_event();
        let mut stale_ctx = MiddlewareContext::live_handler();
        assert!(matches!(
            cb.pre_handle(&stale_probe, &mut stale_ctx),
            MiddlewareAction::Continue
        ));
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 1);

        let mut reopen_ctx = MiddlewareContext::live_handler();
        assert!(cb.transition_to(CircuitState::Open, &mut reopen_ctx));

        let blocked_probe = create_test_event();
        let mut blocked_ctx = MiddlewareContext::live_handler();
        assert!(matches!(
            cb.pre_handle(&blocked_probe, &mut blocked_ctx),
            MiddlewareAction::Skip { results, .. } if results.is_empty()
        ));
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 1);

        cb.post_handle(&stale_probe, &[create_test_event()], &mut stale_ctx);
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 0);

        let fresh_probe = create_test_event();
        let mut fresh_ctx = MiddlewareContext::live_handler();
        assert!(matches!(
            cb.pre_handle(&fresh_probe, &mut fresh_ctx),
            MiddlewareAction::Continue
        ));
        cb.post_handle(&fresh_probe, &[create_test_event()], &mut fresh_ctx);

        assert_eq!(cb.current_state(), CircuitState::Closed);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 0);
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
            let mut ctx = MiddlewareContext::live_handler();
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
        let mut probe_ctx = MiddlewareContext::live_handler();
        let action = cb.pre_handle(&probe_event, &mut probe_ctx);
        assert!(
            matches!(action, MiddlewareAction::Continue),
            "expected probe to be admitted in HalfOpen"
        );
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(
            probe_ctx.get::<CircuitBreakerIsProbe>().copied(),
            Some(true)
        );

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

        // The transition_to(Closed) emits a CircuitBreakerEvent::Closed control event.
        assert!(
            probe_ctx.control_events().iter().any(|event| matches!(
                &event.content,
                obzenflow_core::event::ChainEventContent::Observability(
                    ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                        CircuitBreakerEvent::Closed { .. }
                    ))
                )
            )),
            "expected CircuitBreakerEvent::Closed control event after probe succeeded"
        );

        // Phase 4: Normal traffic should flow again.
        let normal_event = create_test_event();
        let mut normal_ctx = MiddlewareContext::live_handler();
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
        let mut ctx = MiddlewareContext::live_handler();
        cb.pre_handle(&event, &mut ctx);
        let mut failed = create_test_event();
        failed.processing_info.status = ProcessingStatus::error("simulated_failure_halfopen_fail");
        cb.post_handle(&event, &[failed], &mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);

        // Cooldown elapsed → HalfOpen, probe admitted.
        let probe_event = create_test_event();
        let mut probe_ctx = MiddlewareContext::live_handler();
        let action = cb.pre_handle(&probe_event, &mut probe_ctx);
        assert!(matches!(action, MiddlewareAction::Continue));
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);

        // Probe fails → reopens.
        let mut probe_failed = create_test_event();
        probe_failed.processing_info.status = ProcessingStatus::error("simulated_probe_failure");
        cb.post_handle(&probe_event, &[probe_failed], &mut probe_ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);
        assert!(
            probe_ctx.control_events().iter().any(|event| matches!(
                &event.content,
                obzenflow_core::event::ChainEventContent::Observability(
                    ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                        CircuitBreakerEvent::Opened { .. }
                    ))
                )
            )),
            "expected CircuitBreakerEvent::Opened control event after probe failed and circuit reopened"
        );
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
                        let mut ctx = MiddlewareContext::live_handler();
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
                            MiddlewareAction::Skip { .. } | MiddlewareAction::Abort { .. } => {
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
        let ctx = MiddlewareContext::live_handler();

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
        let mut ctx = MiddlewareContext::live_handler();
        ctx.insert::<CircuitBreakerRetryAfterMs>(raw_wait.as_millis() as u64);

        // Create a rate-limited output.
        let mut rl_output = create_test_event();
        rl_output.processing_info.status =
            ProcessingStatus::error_with_kind("rate_limited", Some(ErrorKind::RateLimited));

        cb.pre_handle(&event, &mut ctx);
        cb.post_handle(&event, &[rl_output], &mut ctx);

        // The CB should have signaled a retry with a delay. Extract it.
        let should_retry = ctx
            .get::<CircuitBreakerShouldRetry>()
            .copied()
            .unwrap_or(false);

        assert!(should_retry, "expected CB to signal retry for RateLimited");

        let delay_ms = ctx
            .get::<CircuitBreakerRetryDelayMs>()
            .copied()
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
            let mut ctx = MiddlewareContext::live_handler();
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
    // FLOWIP-120h: rejection recording at the effect boundary
    // -----------------------------------------------------------------------

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FallbackIn {
        n: u32,
    }

    impl TypedPayload for FallbackIn {
        const EVENT_TYPE: &'static str = "test.fallback_in";
    }

    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
    struct FallbackOut {
        n: u32,
    }

    impl TypedPayload for FallbackOut {
        const EVENT_TYPE: &'static str = "test.fallback_out";
    }

    #[test]
    fn open_breaker_failfast_aborts_with_cause_at_effect_boundary() {
        let mut cb = CircuitBreakerMiddleware::new(1);
        cb.open_policy = OpenPolicy::FailFast;
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        cb.force_open(&mut ctx);

        match cb.pre_handle(&create_test_event(), &mut ctx) {
            MiddlewareAction::Abort { cause: Some(cause) } => {
                assert_eq!(cause.source, "circuit_breaker");
                assert_eq!(cause.code, "rejected_circuit_open");
                assert!(cause.retry.is_retryable());
            }
            other => panic!("expected Abort with cause at effect boundary, got {other:?}"),
        }
    }

    #[test]
    fn open_breaker_emit_fallback_without_fallback_aborts_at_effect_boundary() {
        let cb = CircuitBreakerMiddleware::new(1); // default EmitFallback, no fallback configured
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        cb.force_open(&mut ctx);

        assert!(matches!(
            cb.pre_handle(&create_test_event(), &mut ctx),
            MiddlewareAction::Abort { cause: Some(_) }
        ));
    }

    #[test]
    fn open_breaker_rejection_keeps_legacy_skip_on_handler_lane() {
        let mut cb = CircuitBreakerMiddleware::new(1);
        cb.open_policy = OpenPolicy::FailFast;
        let mut ctx = MiddlewareContext::live_handler();
        cb.force_open(&mut ctx);

        match cb.pre_handle(&create_test_event(), &mut ctx) {
            MiddlewareAction::Skip { results, .. } => assert!(results.is_empty()),
            other => panic!("expected legacy empty Skip on the handler lane, got {other:?}"),
        }
    }

    #[test]
    fn probe_rejection_cause_distinguishes_probe_in_progress() {
        let cb = CircuitBreakerMiddleware::new(1);
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);

        match cb.handle_open_like(
            &create_test_event(),
            &mut ctx,
            &OpenPolicy::FailFast,
            CircuitBreakerRejectionReason::ProbeInProgress,
        ) {
            MiddlewareAction::Abort { cause: Some(cause) } => {
                assert_eq!(cause.code, "rejected_probe_in_progress");
            }
            other => panic!("expected Abort with probe reason, got {other:?}"),
        }
    }

    #[test]
    fn typed_fallback_event_records_input_as_causality_parent() {
        let input_event = ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            FallbackIn::EVENT_TYPE,
            json!({ "n": 7 }),
        );
        let f = |input: &FallbackIn| FallbackOut { n: input.n };

        let events = build_typed_fallback_event::<FallbackIn, FallbackOut, _>(&f, &input_event);
        assert_eq!(events.len(), 1);
        let fallback = &events[0];
        assert_eq!(
            fallback.event_type(),
            FallbackOut::versioned_event_type().as_str()
        );
        assert!(
            fallback.causality.parent_ids.contains(&input_event.id),
            "fallback event must record the guarded input as its causality parent"
        );
    }

    // -----------------------------------------------------------------------
    // T6: force_close and force_open admin methods
    // -----------------------------------------------------------------------

    #[test]
    fn force_open_transitions_closed_to_open() {
        let cb = CircuitBreakerMiddleware::new(100);
        assert_eq!(cb.current_state(), CircuitState::Closed);

        let mut ctx = MiddlewareContext::live_handler();
        cb.force_open(&mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);
    }

    #[test]
    fn force_close_transitions_open_to_closed_and_resets_counters() {
        let cb = CircuitBreakerMiddleware::new(1);

        // Drive it to Open via a failure.
        let event = create_test_event();
        let mut ctx = MiddlewareContext::live_handler();
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
        let mut admin_ctx = MiddlewareContext::live_handler();
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
        let mut ctx = MiddlewareContext::live_handler();
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
        let mut ctx = MiddlewareContext::live_handler();
        cb.pre_handle(&event, &mut ctx);
        let mut failed = create_test_event();
        failed.processing_info.status = ProcessingStatus::error("simulated_failure_force_close_ho");
        cb.post_handle(&event, &[failed], &mut ctx);
        assert_eq!(cb.current_state(), CircuitState::Open);

        let probe_event = create_test_event();
        let mut probe_ctx = MiddlewareContext::live_handler();
        cb.pre_handle(&probe_event, &mut probe_ctx);
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);

        // Admin force-close from HalfOpen.
        let mut admin_ctx = MiddlewareContext::live_handler();
        cb.force_close(&mut admin_ctx);
        assert_eq!(cb.current_state(), CircuitState::Closed);
        assert_eq!(cb.failure_count.load(Ordering::SeqCst), 0);
    }
}
