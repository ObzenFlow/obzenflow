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
        CircuitBreakerIsProbe, CircuitBreakerProbeSlot, CircuitBreakerRecoveryOpenEpoch,
        CircuitBreakerRetryAfterMs,
    },
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
use obzenflow_core::{StageId, WriterId};
use obzenflow_runtime::control_plane::cb_state;
use obzenflow_runtime::effects::EffectError;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod classifier;
mod config;
mod factory;
mod hook_adapters;
mod retry;
mod state;
mod window;

pub(crate) use classifier::effect_error_event;
pub(in crate::middleware::control) use classifier::FailureClassification;
pub use classifier::FailureHealth;
pub use config::CircuitBreakerConfigError;
use config::HalfOpenPolicy;
pub(in crate::middleware::control) use factory::CircuitBreakerFactory;
pub use factory::{ai_circuit_breaker, CheckedCircuitBreakerBuilder, CircuitBreaker};
pub use retry::Retry;

use classifier::FailureClassificationClassifier;
pub(in crate::middleware::control) use config::CircuitBreakerFailureMode;
use hook_adapters::{SourceAdmit, SourceOutcome, SourceProbeGuard};
use state::CircuitState;
pub(in crate::middleware::control) use window::FailureWindow;
use window::{CallSample, FailureWindowState};

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
            Self::CircuitOpen => "circuit_open",
            Self::ProbeInProgress => "probe_in_progress",
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

pub struct CircuitBreakerFamily;

/// Circuit breaker middleware that prevents cascading failures.
///
/// Stage-level breaker and limiter instances remain independent controls. At
/// the effect boundary, [`EffectResilience`](super::super::EffectResilience)
/// instead owns an affine limiter reservation and commits it only immediately
/// before an admitted physical call, so an open-circuit rejection consumes no
/// permit.
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
    #[cfg(test)]
    half_open_race_test_hook: Option<HalfOpenRaceTestHook>,
    /// Serialises breaker transitions with recovery admission/continuation
    /// checks. This makes the open epoch and state one logical snapshot for
    /// effect recovery without changing the externally shared state view.
    effect_recovery_state_gate: Mutex<()>,
    /// Monotonic count of successful transitions into Open. Recovery sessions
    /// capture the value at initial admission and reject any later physical
    /// continuation after it changes, even if the circuit is Closed again.
    effect_recovery_open_epoch: AtomicU64,
    /// FLOWIP-115a: the probe generation reserved by the source boundary policy,
    /// read by observation to classify the probe. The RAII guard releases the
    /// slot and clears this marker on cancellation.
    source_pending_probe: Arc<Mutex<Option<u64>>>,
    /// Statistics for periodic summaries
    stats: Arc<Mutex<CircuitBreakerStats>>,
    /// When the last state change occurred
    last_state_change: Arc<Mutex<Instant>>,
    // ---- Cumulative circuit breaker metrics (FLOWIP-059a-2) ----
    requests_total: Arc<AtomicU64>,
    successes_total: Arc<AtomicU64>,
    failures_total: Arc<AtomicU64>,
    slow_total: Arc<AtomicU64>,
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
    /// Optional typed failure-only health override.
    failure_classification_classifier: Option<FailureClassificationClassifier>,
    /// Policy controlling behaviour while the circuit is HalfOpen.
    half_open_policy: HalfOpenPolicy,
    rate_limited_counts_as_failure: bool,
}

#[derive(Debug)]
struct CircuitBreakerStats {
    requests_processed: u64,
    requests_rejected: u64,
    last_summary: Instant,
}

/// Deterministic coordination points for the stale half-open waiter
/// regression. Absent from non-test builds.
#[cfg(test)]
struct HalfOpenRaceTestHook {
    waiter_observed_half_open: Arc<std::sync::Barrier>,
    settlement_holds_probe_gate: Arc<std::sync::Barrier>,
    release_settlement: Arc<std::sync::Barrier>,
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
        Self::construct(threshold, Duration::from_secs(60), None, None)
    }

    /// Create a circuit breaker with custom cooldown duration
    pub fn with_cooldown(threshold: usize, cooldown: Duration) -> Self {
        Self::construct(threshold, cooldown, None, None)
    }

    pub(in crate::middleware::control) fn with_cooldown_for_stage(
        threshold: usize,
        cooldown: Duration,
        stage_id: StageId,
    ) -> Self {
        Self::construct(threshold, cooldown, Some(stage_id), None)
    }

    fn construct(
        threshold: usize,
        cooldown: Duration,
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
            #[cfg(test)]
            half_open_race_test_hook: None,
            effect_recovery_state_gate: Mutex::new(()),
            effect_recovery_open_epoch: AtomicU64::new(0),
            source_pending_probe: Arc::new(Mutex::new(None)),
            stats: Arc::new(Mutex::new(CircuitBreakerStats {
                requests_processed: 0,
                requests_rejected: 0,
                last_summary: Instant::now(),
            })),
            last_state_change: Arc::new(Mutex::new(Instant::now())),
            requests_total: Arc::new(AtomicU64::new(0)),
            successes_total: Arc::new(AtomicU64::new(0)),
            failures_total: Arc::new(AtomicU64::new(0)),
            slow_total: Arc::new(AtomicU64::new(0)),
            rejections_total: Arc::new(AtomicU64::new(0)),
            opened_total: Arc::new(AtomicU64::new(0)),
            time_in_closed: Arc::new(Mutex::new(Duration::from_secs(0))),
            time_in_open: Arc::new(Mutex::new(Duration::from_secs(0))),
            time_in_half_open: Arc::new(Mutex::new(Duration::from_secs(0))),
            writer_id: stage_id
                .map(WriterId::from)
                .unwrap_or_else(|| WriterId::from(StageId::new())),
            failure_classification_classifier: None,
            half_open_policy: HalfOpenPolicy::default(),
            rate_limited_counts_as_failure: false,
        }
    }

    fn current_state(&self) -> CircuitState {
        CircuitState::from(self.state.load(Ordering::SeqCst))
    }

    fn effect_recovery_open_epoch_at_admission(&self, ctx: &mut MiddlewareContext) -> bool {
        let _state_gate = self
            .effect_recovery_state_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !matches!(self.current_state(), CircuitState::Closed) {
            return false;
        }
        ctx.insert::<CircuitBreakerRecoveryOpenEpoch>(
            self.effect_recovery_open_epoch.load(Ordering::SeqCst),
        );
        true
    }

    fn effect_recovery_epoch_is_current(&self, admitted_epoch: u64) -> bool {
        let _state_gate = self
            .effect_recovery_state_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        matches!(self.current_state(), CircuitState::Closed)
            && self.effect_recovery_open_epoch.load(Ordering::SeqCst) == admitted_epoch
    }

    pub(in crate::middleware::control) fn is_effect_probe(&self, ctx: &MiddlewareContext) -> bool {
        ctx.get::<CircuitBreakerIsProbe>().copied().unwrap_or(false)
    }

    pub(in crate::middleware::control) fn effect_retry_may_continue(
        &self,
        ctx: &MiddlewareContext,
    ) -> bool {
        ctx.get::<CircuitBreakerRecoveryOpenEpoch>()
            .copied()
            .is_some_and(|epoch| self.effect_recovery_epoch_is_current(epoch))
    }

    pub(in crate::middleware::control) fn evidence_writer_id(&self) -> WriterId {
        self.writer_id
    }

    fn record_closed_outcome(
        &self,
        counted_as_failure: bool,
        call_duration: Option<Duration>,
        _now: Instant,
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
                                is_failure: counted_as_failure,
                                is_slow,
                            });
                        }

                        let mut observed = 0usize;
                        let mut failures = 0usize;
                        let mut slow_calls = 0usize;

                        let FailureWindow::Count { size } = window;
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
            FailureClassification::RateLimited(_) => self.rate_limited_counts_as_failure,
            FailureClassification::Ignored => false,
        }
    }

    pub(in crate::middleware::control) fn classify_call(
        &self,
        _event: &ChainEvent,
        outputs: &[ChainEvent],
        ctx: &MiddlewareContext,
    ) -> (FailureClassification, Option<ErrorKind>, Option<String>) {
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
                    None | Some(ErrorKind::Unknown) => saw_transient = true,
                }
            }
        }

        let base_classification = if saw_permanent {
            FailureClassification::PermanentFailure
        } else if let Some(delay) = saw_rate_limited {
            FailureClassification::RateLimited(delay)
        } else if saw_transient {
            FailureClassification::TransientFailure
        } else if first_error_kind.is_some() {
            // Validation and domain outcomes are real dependency results, but
            // neither success nor dependency-health failure.
            FailureClassification::Ignored
        } else {
            FailureClassification::Success
        };

        (base_classification, first_error_kind, first_error_message)
    }

    pub(in crate::middleware::control) fn classify_effect_error(
        &self,
        event: &ChainEvent,
        error: &EffectError,
        ctx: &MiddlewareContext,
    ) -> FailureClassification {
        let error_event = effect_error_event(event, error);
        let default = self
            .classify_call(event, std::slice::from_ref(&error_event), ctx)
            .0;
        let Some(classifier) = &self.failure_classification_classifier else {
            return default;
        };
        match classifier(error) {
            FailureHealth::Ignored => FailureClassification::Ignored,
            FailureHealth::CountedFailure => match default {
                FailureClassification::TransientFailure
                | FailureClassification::PermanentFailure => default,
                FailureClassification::RateLimited(_) if self.rate_limited_counts_as_failure => {
                    default
                }
                FailureClassification::RateLimited(_) => FailureClassification::TransientFailure,
                FailureClassification::Ignored | FailureClassification::Success => {
                    FailureClassification::PermanentFailure
                }
            },
        }
    }

    pub(in crate::middleware::control) fn is_slow_dependency_call(
        &self,
        dependency_elapsed: Duration,
    ) -> bool {
        match &self.failure_mode {
            CircuitBreakerFailureMode::RateBased {
                slow_call_duration_threshold: Some(threshold),
                ..
            } => dependency_elapsed >= *threshold,
            _ => false,
        }
    }

    pub(in crate::middleware::control) fn settle_unobserved_call(
        &self,
        ctx: &mut MiddlewareContext,
    ) {
        // The protected operation did execute, so it is an admitted request,
        // but framework/journal/coordination failures must not manufacture a
        // breaker success or failure sample.
        if let Ok(mut stats) = self.stats.lock() {
            stats.requests_processed += 1;
        }
        self.requests_total.fetch_add(1, Ordering::Relaxed);
        let call_duration = ctx
            .get::<crate::middleware::context_keys::EffectCallDurationNanos>()
            .copied()
            .map(Duration::from_nanos);
        if call_duration.is_some_and(|duration| self.is_slow_dependency_call(duration)) {
            self.slow_total.fetch_add(1, Ordering::Relaxed);
        }

        if ctx.get::<CircuitBreakerIsProbe>().copied().unwrap_or(false) {
            let _probe_gate = self
                .probe_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            drop(ctx.remove::<CircuitBreakerProbeSlot>());
        }
        self.maybe_emit_summary(ctx);
    }

    pub(in crate::middleware::control) fn settle_cancelled_in_flight(&self) {
        // Cancellation has no async outbox on which to emit evidence. It is
        // nevertheless an admitted physical attempt once the runtime receipt
        // reached Started, so count the request without inventing a health
        // sample. Any half-open probe lease is released by its context guard.
        if let Ok(mut stats) = self.stats.lock() {
            stats.requests_processed += 1;
        }
        self.requests_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Attempt an atomic state transition. Returns `true` if this call won
    /// the CAS race and the transition was applied, `false` if another thread
    /// already moved the state (or old == new).
    fn transition_to_inner(&self, new_state: CircuitState) -> (bool, Option<ChainEvent>) {
        let _recovery_state_gate = self
            .effect_recovery_state_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
            self.effect_recovery_open_epoch
                .fetch_add(1, Ordering::SeqCst);
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
            self.requests_total.fetch_add(1, Ordering::Relaxed);
        }
        if matches!(
            outcome,
            SourceOutcome::Success { poll_duration }
                | SourceOutcome::Failure { poll_duration }
                if self.is_slow_dependency_call(poll_duration)
        ) {
            self.slow_total.fetch_add(1, Ordering::Relaxed);
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
    pub(in crate::middleware::control) fn settle_not_executed(&self, ctx: &mut MiddlewareContext) {
        if ctx.get::<CircuitBreakerIsProbe>().copied().unwrap_or(false) {
            let _probe_gate = self.probe_gate.lock().ok();
            drop(ctx.remove::<CircuitBreakerProbeSlot>());
        }
        self.maybe_emit_summary(ctx);
    }

    /// Reject Open and probe-busy calls with one stable framework cause.
    fn handle_open_like(
        &self,
        ctx: &mut MiddlewareContext,
        reason: CircuitBreakerRejectionReason,
    ) -> MiddlewareAction {
        // Track rejection for summaries.
        if let Ok(mut stats) = self.stats.lock() {
            stats.requests_rejected += 1;
        }
        self.rejections_total.fetch_add(1, Ordering::Relaxed);

        self.maybe_emit_summary(ctx);
        MiddlewareAction::Abort {
            cause: Some(self.rejection_abort_cause(reason)),
        }
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
    use super::*;
    use crate::middleware::{Middleware, MiddlewareAction};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::MiddlewareExecutionScope;
    use serde_json::json;

    fn event() -> ChainEvent {
        ChainEventFactory::data_event(WriterId::from(StageId::new()), "test.input", json!({}))
    }

    #[test]
    fn consecutive_failures_open_the_circuit() {
        let breaker = CircuitBreakerMiddleware::new(2);
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);

        breaker.settle_classified_call(&FailureClassification::TransientFailure, &mut ctx);
        assert_eq!(breaker.current_state(), CircuitState::Closed);
        breaker.settle_classified_call(&FailureClassification::TransientFailure, &mut ctx);

        assert_eq!(breaker.current_state(), CircuitState::Open);
        assert_eq!(breaker.requests_total.load(Ordering::Relaxed), 2);
        assert_eq!(breaker.failures_total.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn slow_calls_are_counted_per_physical_attempt() {
        let mut breaker = CircuitBreakerMiddleware::new(5);
        breaker.failure_mode = CircuitBreakerFailureMode::RateBased {
            window: FailureWindow::Count { size: 5 },
            failure_rate_threshold: 2.0,
            slow_call_rate_threshold: Some(1.0),
            slow_call_duration_threshold: Some(Duration::from_millis(10)),
            minimum_calls: NonZeroU32::new(1).unwrap(),
        };
        breaker.rate_window = Some(Arc::new(Mutex::new(FailureWindowState::new(5))));
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        ctx.insert::<crate::middleware::context_keys::EffectCallDurationNanos>(
            Duration::from_millis(10).as_nanos() as u64,
        );

        breaker.settle_classified_call(&FailureClassification::Success, &mut ctx);

        assert_eq!(breaker.slow_total.load(Ordering::Relaxed), 1);
        assert_eq!(breaker.current_state(), CircuitState::Open);
    }

    #[test]
    fn open_effect_path_returns_a_stable_framework_rejection() {
        let breaker = CircuitBreakerMiddleware::new(1);
        breaker.state.store(cb_state::OPEN, Ordering::SeqCst);
        *breaker.opened_at.lock().unwrap() = Some(Instant::now());
        let mut ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);

        match Middleware::pre_handle(&breaker, &event(), &mut ctx) {
            MiddlewareAction::Abort { cause: Some(cause) } => {
                assert_eq!(cause.source.as_str(), CIRCUIT_BREAKER_ABORT_SOURCE);
                assert_eq!(cause.code.as_str(), "circuit_open");
            }
            _ => panic!("open breaker must abort with a typed cause"),
        }
    }

    #[test]
    fn busy_half_open_probe_returns_a_stable_framework_rejection() {
        let breaker = CircuitBreakerMiddleware::new(1);
        breaker.state.store(cb_state::HALF_OPEN, Ordering::SeqCst);
        let input = event();
        let mut first = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        let mut second =
            MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);

        assert!(matches!(
            breaker.effect_admit(&input, &mut first),
            crate::middleware::PolicyAdmission::Admit
        ));
        match breaker.effect_admit(&input, &mut second) {
            crate::middleware::PolicyAdmission::Reject(cause) => {
                assert_eq!(cause.source.as_str(), CIRCUIT_BREAKER_ABORT_SOURCE);
                assert_eq!(cause.code.as_str(), "probe_in_progress");
            }
            crate::middleware::PolicyAdmission::Admit => {
                panic!("a second concurrent half-open probe must be rejected")
            }
        }

        drop(first);
        let mut replacement =
            MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        assert!(matches!(
            breaker.effect_admit(&input, &mut replacement),
            crate::middleware::PolicyAdmission::Admit
        ));
    }

    #[test]
    fn health_callback_observes_only_errors_and_cannot_reclassify_success() {
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut breaker = CircuitBreakerMiddleware::new(2);
        breaker.failure_classification_classifier = Some(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            FailureHealth::Ignored
        }));
        let ctx = MiddlewareContext::with_scope(MiddlewareExecutionScope::LiveEffectBoundary);
        let input = event();

        assert_eq!(
            breaker.classify_call(&input, &[], &ctx).0,
            FailureClassification::Success
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        assert_eq!(
            breaker.classify_effect_error(&input, &EffectError::Timeout("slow".to_string()), &ctx,),
            FailureClassification::Ignored
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}
