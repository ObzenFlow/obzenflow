// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Circuit breaker middleware for fail-fast behavior
//!
//! This middleware implements the circuit breaker pattern to prevent
//! cascading failures. It emits raw events that can be consumed by
//! monitoring and SLI middleware.

use crate::middleware::{
    context_keys::{CircuitBreakerIsProbe, CircuitBreakerProbeSlot, CircuitBreakerRetryAfterMs},
    MiddlewareAbortCause, MiddlewareAction, MiddlewareContext,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerRejectionReason, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::{
    ChainEventFactory, CircuitBreakerSummaryEventParams, EffectFailureCode, EffectFailureSource,
    RetryDisposition,
};
use obzenflow_core::MiddlewareExecutionScope;
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::control_plane::cb_state;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod classifier;
mod config;
mod factory;
mod fallback;
mod hook_adapters;
mod retry;
mod state;
mod window;

pub use classifier::{FailureClassification, FailureClassificationPolicy, UnknownErrorKindPolicy};
pub use config::{HalfOpenPolicy, OpenPolicy};
pub use factory::{
    ai_circuit_breaker, circuit_breaker, CircuitBreakerBuilder, CircuitBreakerFactory,
};
pub use retry::RetryLimits;
pub(crate) use classifier::effect_error_event;
pub(crate) use retry::{EffectRecoverySession, RecoveryDirective};

use classifier::{FailureClassificationClassifier, FailureClassifier};
use config::CircuitBreakerFailureMode;
#[cfg(test)]
use hook_adapters::CircuitBreakerSourcePolicy;
use hook_adapters::{SourceAdmit, SourceOutcome, SourceProbeGuard};
use retry::CircuitBreakerRetryPolicy;
use state::CircuitState;
use window::{CallSample, FailureWindow, FailureWindowState};

type FallbackFn = Arc<
    dyn Fn(&ChainEvent, obzenflow_core::config::LineagePolicy) -> Vec<ChainEvent> + Send + Sync,
>;
type RejectionFn = Arc<
    dyn Fn(
            &ChainEvent,
            CircuitBreakerRejectionReason,
            obzenflow_core::config::LineagePolicy,
        ) -> Vec<ChainEvent>
        + Send
        + Sync,
>;

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
    /// FLOWIP-115a: the probe generation reserved by the source boundary policy,
    /// read by observation to classify the probe. The RAII guard releases the
    /// slot and clears this marker on cancellation.
    source_pending_probe: Arc<Mutex<Option<u64>>>,
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
    /// FLOWIP-010 §7: build-resolved lineage policy, set by the factory from
    /// `StageConfig.lineage`; consumed by the typed fallback builders.
    lineage: obzenflow_core::config::LineagePolicy,
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

    // ---- Effect-bound recovery configuration (FLOWIP-115h) ----
    retry_policy: Option<CircuitBreakerRetryPolicy>,
    retry_limits: RetryLimits,
    failure_classification_policy: FailureClassificationPolicy,
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
            source_pending_probe: Arc::new(Mutex::new(None)),
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
            lineage: obzenflow_core::config::LineagePolicy::default(),
            failure_classifier,
            failure_classification_classifier: None,
            open_policy: OpenPolicy::default(),
            half_open_policy: HalfOpenPolicy::default(),
            unknown_error_kind_policy: UnknownErrorKindPolicy::TreatAsInfraFailure,

            retry_policy: None,
            retry_limits: RetryLimits::default(),
            failure_classification_policy: FailureClassificationPolicy::default(),
        }
    }

    fn current_state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::SeqCst))
    }

    /// Open one logical invocation's recovery session (FLOWIP-115h AR1). The
    /// session is the boundary's whole seam into recovery: every gate, delay,
    /// evidence, and settlement decision lives behind it.
    pub(crate) fn begin_effect_recovery<'a>(
        &'a self,
        ctx: &MiddlewareContext,
        cursor: obzenflow_runtime::effects::EffectCursor,
        cause: obzenflow_core::event::types::EventId,
    ) -> EffectRecoverySession<'a> {
        EffectRecoverySession::new(self, ctx, cursor, cause)
    }

    fn effect_retry_config(&self) -> Option<(CircuitBreakerRetryPolicy, RetryLimits)> {
        self.retry_policy
            .clone()
            .map(|policy| (policy, self.retry_limits.clone()))
    }

    fn is_closed_for_effect_recovery(&self) -> bool {
        matches!(self.current_state(), CircuitState::Closed)
    }

    fn is_effect_probe(&self, ctx: &MiddlewareContext) -> bool {
        ctx.get::<CircuitBreakerIsProbe>().copied().unwrap_or(false)
    }

    fn evidence_writer_id(&self) -> WriterId {
        self.writer_id
    }

    fn record_closed_outcome(
        &self,
        counted_as_failure: bool,
        call_duration: Option<Duration>,
        now: Instant,
    ) -> Option<ChainEvent> {
        let consecutive_failures = if counted_as_failure {
            self.failure_count.fetch_add(1, Ordering::SeqCst) + 1
        } else {
            self.failure_count.store(0, Ordering::SeqCst);
            0
        };

        match &self.failure_mode {
            CircuitBreakerFailureMode::Consecutive { max_failures } => {
                if counted_as_failure && (consecutive_failures as u32) >= max_failures.get() {
                    let event = self.transition_to_inner(CircuitState::Open).1;
                    if event.is_some() {
                        tracing::warn!(
                            "Circuit breaker opened after {} consecutive failures",
                            consecutive_failures
                        );
                    }
                    return event;
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
                    (Some(threshold), Some(duration)) => duration >= *threshold,
                    _ => false,
                };

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

                        let mut observed = 0usize;
                        let mut failures = 0usize;
                        let mut slow_calls = 0usize;

                        match window {
                            FailureWindow::Count { size } => {
                                let max = (*size as usize).min(state.capacity());
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
                                Some(threshold) if *threshold > 0.0 => slow_rate >= *threshold,
                                _ => false,
                            };

                            if open_on_failures || open_on_slow {
                                let event = self.transition_to_inner(CircuitState::Open).1;
                                if event.is_some() {
                                    tracing::warn!(
                                        "Circuit breaker opened (rate-based) after {} calls (failures: {})",
                                        observed,
                                        failures
                                    );
                                }
                                return event;
                            }
                        }
                    }
                }
            }
        }

        None
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
    fn transition_to_inner(&self, new_state: CircuitState) -> (bool, Option<ChainEvent>) {
        let old_state = self.current_state();
        if old_state == new_state {
            return (false, None);
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
            return (false, None);
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

        tracing::info!(
            "Circuit breaker state transition: {:?} -> {:?}",
            old_state,
            new_state
        );

        (true, Some(event))
    }

    /// Context-aware wrapper around [`transition_to_inner`] that writes the
    /// lifecycle event into the middleware context (the effect and handler
    /// paths). The FLOWIP-115a source control ports call `transition_to_inner`
    /// directly and defer the lifecycle event; the metrics snapshotter still
    /// reflects the transition.
    fn transition_to(&self, new_state: CircuitState, ctx: &mut MiddlewareContext) -> bool {
        let (transitioned, event) = self.transition_to_inner(new_state);
        if let Some(event) = event {
            ctx.write_control_event(event);
        }
        transitioned
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

    /// FLOWIP-115a: source-boundary admission, reusing the same state machine as
    /// `pre_handle` without exposing middleware policy details to the runtime.
    /// Reserves a probe slot in HalfOpen and returns an RAII guard that releases
    /// the slot on normal return or cancellation.
    fn source_admit(&self) -> SourceAdmit {
        match self.current_state() {
            CircuitState::Closed => SourceAdmit::Continue {
                guard: None,
                event: None,
            },
            CircuitState::Open => {
                if self.should_attempt_reset() {
                    let transition_event = {
                        let _probe_gate = self.probe_gate.lock().ok();
                        let (transitioned, event) =
                            self.transition_to_inner(CircuitState::HalfOpen);
                        if transitioned {
                            self.probe_generation.fetch_add(1, Ordering::SeqCst);
                            self.success_count.store(0, Ordering::Relaxed);
                        }
                        event
                    };
                    self.source_reserve_probe(transition_event)
                } else {
                    let cooldown_remaining = self
                        .opened_at
                        .lock()
                        .ok()
                        .and_then(|guard| *guard)
                        .map(|opened_at| self.cooldown.saturating_sub(opened_at.elapsed()))
                        .unwrap_or(self.cooldown);
                    SourceAdmit::Pause(cooldown_remaining)
                }
            }
            CircuitState::HalfOpen => self.source_reserve_probe(None),
        }
    }

    /// Reserve a half-open probe slot via the same CAS loop as `pre_handle`,
    /// or back off briefly when all slots are in use.
    fn source_reserve_probe(&self, event: Option<ChainEvent>) -> SourceAdmit {
        let _probe_gate = self.probe_gate.lock().ok();
        let generation = self.probe_generation.load(Ordering::SeqCst);
        let permitted = self.half_open_policy.permitted_probes.get();
        let mut current = self.probe_in_flight.load(Ordering::SeqCst);
        loop {
            if current >= permitted {
                return SourceAdmit::Pause(Duration::from_millis(1));
            }
            match self.probe_in_flight.compare_exchange(
                current,
                current + 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    if let Ok(mut pending) = self.source_pending_probe.lock() {
                        *pending = Some(generation);
                    }
                    return SourceAdmit::Continue {
                        guard: Some(SourceProbeGuard::new(
                            self.probe_in_flight.clone(),
                            self.source_pending_probe.clone(),
                            generation,
                        )),
                        event: event.map(Box::new),
                    };
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// FLOWIP-115a: classify one source-boundary poll outcome. A probe closes
    /// on success and reopens on failure; a Closed-state poll error counts
    /// toward opening through the configured failure mode.
    fn source_settle(&self, outcome: SourceOutcome) -> Option<ChainEvent> {
        if matches!(
            outcome,
            SourceOutcome::Success { .. } | SourceOutcome::Failure { .. }
        ) {
            if let Ok(mut stats) = self.stats.lock() {
                stats.requests_processed += 1;
            }
        }

        let pending = self
            .source_pending_probe
            .lock()
            .ok()
            .and_then(|mut pending| pending.take());

        if let Some(generation) = pending {
            let _probe_gate = self.probe_gate.lock().ok();

            if generation == self.probe_generation.load(Ordering::SeqCst)
                && matches!(self.current_state(), CircuitState::HalfOpen)
            {
                return match outcome {
                    SourceOutcome::Success { .. } => {
                        self.success_count.fetch_add(1, Ordering::Relaxed);
                        self.successes_total.fetch_add(1, Ordering::Relaxed);
                        let (transitioned, event) = self.transition_to_inner(CircuitState::Closed);
                        if transitioned {
                            self.failure_count.store(0, Ordering::SeqCst);
                        }
                        event
                    }
                    SourceOutcome::Failure { .. } => {
                        self.failures_total.fetch_add(1, Ordering::Relaxed);
                        self.transition_to_inner(CircuitState::Open).1
                    }
                    SourceOutcome::Inconclusive | SourceOutcome::NotExecuted => None,
                };
            }
            return None;
        }

        match outcome {
            SourceOutcome::Success { poll_duration } => {
                self.successes_total.fetch_add(1, Ordering::Relaxed);
                if matches!(self.current_state(), CircuitState::Closed) {
                    return self.record_closed_outcome(false, Some(poll_duration), Instant::now());
                }
                None
            }
            SourceOutcome::Failure { poll_duration } => {
                self.failures_total.fetch_add(1, Ordering::Relaxed);
                if matches!(self.current_state(), CircuitState::Closed) {
                    return self.record_closed_outcome(true, Some(poll_duration), Instant::now());
                }
                None
            }
            SourceOutcome::Inconclusive | SourceOutcome::NotExecuted => None,
        }
    }

    /// Settle an admitted effect probe whose protected call was skipped or
    /// rejected by a later policy. No breaker outcome is classified.
    fn settle_not_executed(&self, ctx: &mut MiddlewareContext) {
        if ctx.get::<CircuitBreakerIsProbe>().copied().unwrap_or(false) {
            let _probe_gate = self.probe_gate.lock().ok();
            drop(ctx.remove::<CircuitBreakerProbeSlot>());
        }
        self.maybe_emit_summary(ctx);
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
                    results: (typed.build_rejection)(event, reason, self.lineage),
                    cause: None,
                })
        };

        let action = match policy {
            OpenPolicy::EmitFallback => {
                if let Some(fallback) = &self.fallback {
                    let results = (fallback)(event, self.lineage);
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

#[cfg(test)]
mod tests {
    use super::fallback::{build_outcome_fallback_events, build_typed_fallback_event};
    use super::*;
    use crate::middleware::{
        Middleware, MiddlewareFactory, SourceBatchFacts, SourcePolicy, SourcePolicyCtx,
        SourcePollOutcome,
    };
    use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::control_plane::ControlPlaneProvider;
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
            obzenflow_core::config::LineagePolicy::default(),
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
            lineage: obzenflow_core::config::LineagePolicy::default(),
            resolved_policies: Default::default(),
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
            .circuit_breaker_state_view(&stage_id)
            .expect("expected circuit breaker state view registration");
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

            // FLOWIP-115f: the breaker reads the protected call's wall-clock
            // duration from the effect-boundary context, not from the output's
            // `processing_time` (which is now stamped at commit, after observe).
            ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(if *is_slow {
                StdDuration::from_millis(100).as_nanos() as u64
            } else {
                StdDuration::from_millis(10).as_nanos() as u64
            });

            let out = create_test_event();
            cb.post_handle(&event, &[out], &mut ctx);
        }

        assert!(
            matches!(cb.current_state(), CircuitState::Open),
            "expected circuit to be Open after slow-call rate threshold exceeded"
        );
    }

    #[test]
    fn source_rate_based_count_window_opens_after_failure_rate_threshold() {
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

        for is_failure in [true, true, false, true, false] {
            let outcome = if is_failure {
                SourceOutcome::Failure {
                    poll_duration: StdDuration::from_millis(1),
                }
            } else {
                SourceOutcome::Success {
                    poll_duration: StdDuration::from_millis(1),
                }
            };
            cb.source_settle(outcome);
        }

        assert!(
            matches!(cb.current_state(), CircuitState::Open),
            "expected source circuit to open after rate-based failure threshold"
        );
    }

    #[test]
    fn source_rate_based_slow_call_opens_after_slow_threshold() {
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
            failure_rate_threshold: 1.0,
            slow_call_rate_threshold: Some(0.6),
            slow_call_duration_threshold: Some(StdDuration::from_millis(50)),
            minimum_calls: NonZeroU32::new(5).unwrap(),
        };
        cb.rate_window = Some(Arc::new(Mutex::new(FailureWindowState::new(5))));

        for is_slow in [true, true, false, true, false] {
            cb.source_settle(SourceOutcome::Success {
                poll_duration: if is_slow {
                    StdDuration::from_millis(100)
                } else {
                    StdDuration::from_millis(10)
                },
            });
        }

        assert!(
            matches!(cb.current_state(), CircuitState::Open),
            "expected source circuit to open after slow-call rate threshold"
        );
    }

    #[test]
    fn source_policy_error_marked_delivery_counts_as_breaker_failure() {
        let breaker = Arc::new(CircuitBreakerMiddleware::with_cooldown(
            1,
            StdDuration::from_secs(60),
        ));
        let policy = CircuitBreakerSourcePolicy {
            breaker: breaker.clone(),
        };
        let mut failed = create_test_event();
        failed.processing_info.status = ProcessingStatus::error("source_error_marked_delivery");
        let mut ctx = SourcePolicyCtx::new(WriterId::from(StageId::new()));

        policy.observe(
            &SourcePollOutcome::Delivered {
                batch: SourceBatchFacts::from_events(std::slice::from_ref(&failed)),
                poll_duration: StdDuration::from_millis(1),
            },
            &mut ctx,
        );

        assert!(
            matches!(breaker.current_state(), CircuitState::Open),
            "error-marked delivered source batch must count as a breaker failure"
        );
        assert!(
            ctx.take_control_events().iter().any(|event| {
                matches!(
                    &event.content,
                    obzenflow_core::event::ChainEventContent::Observability(
                        ObservabilityPayload::Middleware(MiddlewareLifecycle::CircuitBreaker(
                            CircuitBreakerEvent::Opened { .. }
                        ))
                    )
                )
            }),
            "source breaker opening must be returned through the boundary outbox"
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

        let events = build_typed_fallback_event::<FallbackIn, FallbackOut, _>(
            &f,
            &input_event,
            obzenflow_core::config::LineagePolicy::default(),
        );
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

    /// FLOWIP-115a (Risk 1): the source probe slot, reserved by `source_admit`
    /// and released by `source_settle`, must return to zero on every terminal
    /// outcome including an aborted attempt, now that the slot rides the runtime
    /// attempt rather than a middleware-context guard. A leak would wedge the
    /// breaker in HalfOpen; a double-release would underflow the counter.
    #[test]
    fn source_breaker_probe_slot_released_on_every_outcome() {
        // Threshold 1 opens on a single Closed-state failure; zero cooldown makes
        // HalfOpen reachable immediately, so the test needs no wall-clock wait.
        let open_breaker = || {
            let cb = CircuitBreakerMiddleware::with_cooldown(1, Duration::ZERO);
            cb.source_settle(SourceOutcome::Failure {
                poll_duration: Duration::from_millis(1),
            });
            assert_eq!(
                cb.current_state(),
                CircuitState::Open,
                "one Closed-state failure opens a threshold-1 breaker"
            );
            assert_eq!(
                cb.probe_in_flight.load(Ordering::SeqCst),
                0,
                "no probe slot is reserved while Open"
            );
            cb
        };

        let admit_probe = |cb: &CircuitBreakerMiddleware| match cb.source_admit() {
            SourceAdmit::Continue {
                guard: Some(guard),
                event: _,
            } => guard,
            SourceAdmit::Continue { guard: None, .. } => {
                panic!("expected source admission to reserve a probe slot")
            }
            SourceAdmit::Pause(delay) => {
                panic!("expected source admission to proceed, got pause {delay:?}")
            }
        };

        // A successful probe closes the breaker and releases the slot.
        let cb = open_breaker();
        let guard = admit_probe(&cb);
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(
            cb.probe_in_flight.load(Ordering::SeqCst),
            1,
            "admitting a probe reserves exactly one slot"
        );
        cb.source_settle(SourceOutcome::Success {
            poll_duration: Duration::from_millis(1),
        });
        drop(guard);
        assert_eq!(
            cb.current_state(),
            CircuitState::Closed,
            "a successful probe closes the breaker"
        );
        assert_eq!(
            cb.probe_in_flight.load(Ordering::SeqCst),
            0,
            "the probe slot is released on success"
        );

        // A failed probe reopens the breaker and releases the slot.
        let cb = open_breaker();
        let guard = admit_probe(&cb);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 1);
        cb.source_settle(SourceOutcome::Failure {
            poll_duration: Duration::from_millis(1),
        });
        drop(guard);
        assert_eq!(
            cb.current_state(),
            CircuitState::Open,
            "a failed probe reopens the breaker"
        );
        assert_eq!(
            cb.probe_in_flight.load(Ordering::SeqCst),
            0,
            "the probe slot is released on failure"
        );

        // An empty probe is inconclusive: it releases the slot without changing
        // state, leaving the breaker HalfOpen for a retry.
        let cb = open_breaker();
        let guard = admit_probe(&cb);
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 1);
        cb.source_settle(SourceOutcome::Inconclusive);
        drop(guard);
        assert_eq!(
            cb.current_state(),
            CircuitState::HalfOpen,
            "an inconclusive probe leaves the breaker HalfOpen for the next probe"
        );
        assert_eq!(
            cb.probe_in_flight.load(Ordering::SeqCst),
            0,
            "the probe slot is released on inconclusive poll"
        );

        // A policy rejection after breaker admission is explicitly settled as
        // not-executed: the probe lease is released without classifying state.
        let cb = open_breaker();
        let guard = admit_probe(&cb);
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 1);
        cb.source_settle(SourceOutcome::NotExecuted);
        drop(guard);
        assert_eq!(
            cb.current_state(),
            CircuitState::HalfOpen,
            "a not-executed probe leaves the breaker HalfOpen for the next probe"
        );
        assert_eq!(
            cb.probe_in_flight.load(Ordering::SeqCst),
            0,
            "the probe slot is released on not-executed settlement"
        );

        // A cancelled boundary drops the guard without observing an outcome;
        // the state remains HalfOpen and the slot is still released.
        let cb = open_breaker();
        let guard = admit_probe(&cb);
        assert_eq!(cb.current_state(), CircuitState::HalfOpen);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 1);
        drop(guard);
        assert_eq!(
            cb.current_state(),
            CircuitState::HalfOpen,
            "a cancelled probe leaves the breaker HalfOpen for the next probe"
        );
        assert_eq!(
            cb.probe_in_flight.load(Ordering::SeqCst),
            0,
            "the probe slot is released when the source boundary future is dropped"
        );

        // A fresh probe can be reserved after cancellation, proving the slot is
        // genuinely free (not just decremented past a leaked reservation).
        let _guard = admit_probe(&cb);
        assert_eq!(cb.probe_in_flight.load(Ordering::SeqCst), 1);
    }
}
