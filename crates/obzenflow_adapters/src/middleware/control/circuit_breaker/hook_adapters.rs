// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::classifier::FailureClassification;
use super::state::CircuitState;
use super::CircuitBreakerMiddleware;
use crate::middleware::context_keys::{
    CircuitBreakerIsProbe, CircuitBreakerProbeGeneration, CircuitBreakerProbeSlot,
    CircuitBreakerProbeSlotGuard,
};
use crate::middleware::{
    EventAwareEffectPolicy, SinkAdmission, SinkAdmissionGuard, SinkDeliveryPolicyOutcome,
    SinkPolicy, SinkPolicyCtx, SourceAdmission, SourcePolicy, SourcePolicyCtx, SourcePollOutcome,
};
use crate::middleware::{Middleware, MiddlewareAction, MiddlewareContext, SourceMiddlewarePhase};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerEvent, CircuitBreakerRejectionReason, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_runtime::control_plane::CircuitBreakerStateView;
use obzenflow_runtime::stages::source::strategies::{
    CompletionContext, CompletionDecision, CompletionGate,
};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// RAII guard for one source half-open probe slot.
pub(super) struct SourceProbeGuard {
    probe_in_flight: Arc<AtomicU32>,
    source_pending_probe: Arc<Mutex<Option<u64>>>,
    generation: u64,
    released: bool,
}

impl SourceProbeGuard {
    pub(super) fn new(
        probe_in_flight: Arc<AtomicU32>,
        source_pending_probe: Arc<Mutex<Option<u64>>>,
        generation: u64,
    ) -> Self {
        Self {
            probe_in_flight,
            source_pending_probe,
            generation,
            released: false,
        }
    }
}

impl Drop for SourceProbeGuard {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        self.released = true;
        if let Ok(mut pending) = self.source_pending_probe.lock() {
            if pending.as_ref() == Some(&self.generation) {
                *pending = None;
            }
        }
        self.probe_in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// FLOWIP-115a: the source-boundary admission answer from
/// [`CircuitBreakerMiddleware::source_admit`].
pub(super) enum SourceAdmit {
    Continue {
        guard: Option<SourceProbeGuard>,
        event: Option<Box<ChainEvent>>,
    },
    Pause(Duration),
}

/// FLOWIP-115a: the source-path outcome handed to
/// [`CircuitBreakerMiddleware::source_settle`].
#[derive(Debug, Clone, Copy)]
pub(super) enum SourceOutcome {
    /// The poll produced output or reached a clean EOF.
    Success { poll_duration: Duration },
    /// The poll returned an error.
    Failure { poll_duration: Duration },
    /// The poll produced no data (drain, shutdown) before an outcome; the
    /// probe slot is released but breaker state is unchanged.
    Inconclusive,
    /// The breaker admitted the protected unit, but a later policy rejected or
    /// synthesized before the protected call executed.
    NotExecuted,
}

/// FLOWIP-115a: source-boundary policy for the circuit breaker.
pub(super) struct CircuitBreakerSourcePolicy {
    pub(super) breaker: Arc<CircuitBreakerMiddleware>,
}

#[async_trait::async_trait]
impl SourcePolicy for CircuitBreakerSourcePolicy {
    fn label(&self) -> &'static str {
        Middleware::label(self.breaker.as_ref())
    }

    async fn admit(&self, ctx: &mut SourcePolicyCtx) -> SourceAdmission {
        loop {
            match self.breaker.source_admit() {
                SourceAdmit::Continue { guard, event } => {
                    if let Some(event) = event {
                        ctx.write_control_event(*event);
                    }
                    return SourceAdmission::Admit(guard.map(|guard| {
                        Box::new(guard) as Box<dyn crate::middleware::SourceAdmissionGuard>
                    }));
                }
                SourceAdmit::Pause(delay) => {
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    fn observe(&self, outcome: &SourcePollOutcome<'_>, ctx: &mut SourcePolicyCtx) {
        let source_outcome = match outcome {
            SourcePollOutcome::Delivered {
                batch,
                poll_duration,
            } if batch.has_error_marked => SourceOutcome::Failure {
                poll_duration: *poll_duration,
            },
            SourcePollOutcome::Delivered { poll_duration, .. }
            | SourcePollOutcome::Eof { poll_duration } => SourceOutcome::Success {
                poll_duration: *poll_duration,
            },
            SourcePollOutcome::Failed { poll_duration, .. } => SourceOutcome::Failure {
                poll_duration: *poll_duration,
            },
            SourcePollOutcome::Empty { .. } => SourceOutcome::Inconclusive,
            SourcePollOutcome::RejectedBy { .. } => SourceOutcome::NotExecuted,
        };
        if let Some(event) = self.breaker.source_settle(source_outcome) {
            ctx.write_control_event(event);
        }
        self.breaker
            .maybe_emit_summary(ctx.middleware_context_mut());
    }
}

/// FLOWIP-115b: source-completion companion for the circuit breaker.
///
/// Replaces the old `CircuitBreakerSourceStrategy`, which re-looked-up breaker
/// state from the control provider as a second acquisition. This gate reads the
/// same `CircuitBreakerStateView` the source-poll boundary policy published, so
/// completion and the boundary share one state authority (FLOWIP-115b AC26). It
/// is read-only and never drives breaker transitions; runtime still owns EOF,
/// drain, poison semantics, and terminal emission.
#[derive(Debug)]
pub(crate) struct CircuitBreakerCompletionGate {
    view: Arc<dyn CircuitBreakerStateView>,
}

impl CircuitBreakerCompletionGate {
    pub(crate) fn new(view: Arc<dyn CircuitBreakerStateView>) -> Self {
        Self { view }
    }
}

impl CompletionGate for CircuitBreakerCompletionGate {
    fn on_natural_completion(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
        if self.view.is_open() {
            CompletionDecision::PoisonEof
        } else {
            CompletionDecision::DefaultEof
        }
    }

    fn on_begin_drain(&self, ctx: &mut CompletionContext) -> CompletionDecision {
        self.on_natural_completion(ctx)
    }
}

/// FLOWIP-115b: sink-delivery boundary policy for the circuit breaker.
///
/// Reuses the breaker's admission state machine. Unlike the source policy, which
/// idles while the breaker is open and shapes completion through the completion
/// gate, the sink policy fails fast: an open breaker rejects the delivery, which
/// the supervisor maps to a failed delivery receipt.
pub(super) struct CircuitBreakerSinkPolicy {
    pub(super) breaker: Arc<CircuitBreakerMiddleware>,
}

#[async_trait::async_trait]
impl SinkPolicy for CircuitBreakerSinkPolicy {
    fn label(&self) -> &'static str {
        Middleware::label(self.breaker.as_ref())
    }

    async fn admit(&self, ctx: &mut SinkPolicyCtx) -> SinkAdmission {
        match self.breaker.source_admit() {
            SourceAdmit::Continue { guard, event } => {
                if let Some(event) = event {
                    ctx.write_control_event(*event);
                }
                SinkAdmission::Admit(
                    guard.map(|guard| Box::new(guard) as Box<dyn SinkAdmissionGuard>),
                )
            }
            SourceAdmit::Pause(_) => SinkAdmission::Reject {
                reason: "circuit breaker open".to_string(),
            },
        }
    }

    fn observe(&self, outcome: &SinkDeliveryPolicyOutcome<'_>, ctx: &mut SinkPolicyCtx) {
        let source_outcome = match outcome {
            SinkDeliveryPolicyOutcome::Delivered { .. } => SourceOutcome::Success {
                poll_duration: Duration::ZERO,
            },
            SinkDeliveryPolicyOutcome::Failed => SourceOutcome::Failure {
                poll_duration: Duration::ZERO,
            },
            SinkDeliveryPolicyOutcome::RejectedBy { .. } => SourceOutcome::NotExecuted,
        };
        if let Some(event) = self.breaker.source_settle(source_outcome) {
            ctx.write_control_event(event);
        }
        self.breaker
            .maybe_emit_summary(ctx.middleware_context_mut());
    }
}

impl Middleware for CircuitBreakerMiddleware {
    fn label(&self) -> &'static str {
        "circuit_breaker"
    }

    fn kind(&self) -> crate::middleware::MiddlewareKind {
        crate::middleware::MiddlewareKind::Policy
    }

    // FLOWIP-115b Phase 6: placement is carrier-driven (the breaker materializes
    // onto the SourcePoll/Effect/SinkDelivery surfaces), so it no longer claims
    // a special source-ordering phase. `source_phase` is a required trait
    // method, so it returns the neutral `Ordinary`. The `EffectPolicy` impl
    // stays; `materialize()` returns it directly.
    fn source_phase(&self) -> SourceMiddlewarePhase {
        SourceMiddlewarePhase::Ordinary
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
                // Linearise Closed admission with transitions into Open and
                // capture the epoch that owns any later effect recovery.
                if self.effect_recovery_open_epoch_at_admission(ctx) {
                    MiddlewareAction::Continue
                } else {
                    self.pre_handle(event, ctx)
                }
            }

            CircuitState::Open => {
                // Check if we should transition to half-open
                if self.should_attempt_reset() {
                    {
                        let _probe_gate = self
                            .probe_gate
                            .lock()
                            .unwrap_or_else(|poisoned| poisoned.into_inner());
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
                #[cfg(test)]
                if let Some(hook) = &self.half_open_race_test_hook {
                    hook.waiter_observed_half_open.wait();
                }
                let _probe_gate = self
                    .probe_gate
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                // The state read that selected this branch can become stale
                // while waiting for a settling probe. Never reserve from that
                // stale observation: release the gate and re-enter the current
                // state path instead.
                if !matches!(self.current_state(), CircuitState::HalfOpen) {
                    drop(_probe_gate);
                    return self.pre_handle(event, ctx);
                }
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

        let (classification, _error_kind, _error_message) = self.classify_call(event, outputs, ctx);
        self.settle_classified_call(&classification, ctx);
    }
}

impl CircuitBreakerMiddleware {
    /// Read-only fast path used before a limiter wait. Only a circuit that is
    /// definitely still Open rejects here. Cooldown transitions and half-open
    /// probe ownership remain deferred to authoritative final admission.
    pub(in crate::middleware::control) async fn effect_precheck(
        &self,
        event: &ChainEvent,
        ctx: &mut MiddlewareContext,
    ) -> crate::middleware::PolicyAdmission {
        if matches!(self.current_state(), CircuitState::Open) && !self.should_attempt_reset() {
            EventAwareEffectPolicy::admit(self, event, ctx).await
        } else {
            crate::middleware::PolicyAdmission::Admit
        }
    }

    /// Settle breaker health from an already final classification. Recovery
    /// uses this path so a stateful custom classifier is invoked exactly once
    /// per physical result and that same value drives retry, health, and
    /// terminal evidence.
    pub(in crate::middleware::control) fn settle_classified_call(
        &self,
        classification: &FailureClassification,
        ctx: &mut MiddlewareContext,
    ) {
        let now = Instant::now();
        let is_probe = ctx.get::<CircuitBreakerIsProbe>().copied().unwrap_or(false);

        // Track allowed calls (i.e. calls that reached the wrapped handler), regardless of
        // whether they succeeded. Rejections are tracked in `handle_open_like`.
        if let Ok(mut stats) = self.stats.lock() {
            stats.requests_processed += 1;
        }
        self.requests_total.fetch_add(1, Ordering::Relaxed);

        let is_success = matches!(classification, FailureClassification::Success);
        let counted_as_failure = self.counts_as_failure(classification);
        let contributes_health = is_success || counted_as_failure;
        if counted_as_failure {
            self.failures_total.fetch_add(1, Ordering::Relaxed);
        } else if is_success {
            self.successes_total.fetch_add(1, Ordering::Relaxed);
        }

        // FLOWIP-115f: the protected call's wall-clock duration is measured at the
        // effect boundary and threaded through the context, rather than read back
        // from a per-event `processing_time` field (which is now stamped at commit
        // time, after this observe pass).
        let call_duration: Option<Duration> = ctx
            .get::<crate::middleware::context_keys::EffectCallDurationNanos>()
            .copied()
            .map(Duration::from_nanos);

        if is_probe {
            let probe_generation = ctx.get::<CircuitBreakerProbeGeneration>().copied();
            let _probe_gate = self
                .probe_gate
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            #[cfg(test)]
            if let Some(hook) = &self.half_open_race_test_hook {
                hook.settlement_holds_probe_gate.wait();
                hook.release_settlement.wait();
            }
            drop(ctx.remove::<CircuitBreakerProbeSlot>());

            if contributes_health
                && probe_generation == Some(self.probe_generation.load(Ordering::SeqCst))
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
                if contributes_health {
                    if let Some(event) =
                        self.record_closed_outcome(counted_as_failure, call_duration, now)
                    {
                        ctx.write_control_event(event);
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
impl EventAwareEffectPolicy for CircuitBreakerMiddleware {
    fn label(&self) -> &'static str {
        Middleware::label(self)
    }

    async fn admit(
        &self,
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
        event: &ChainEvent,
        attempt: &crate::middleware::EffectAttemptOutcome<'_>,
        ctx: &mut MiddlewareContext,
    ) {
        match attempt {
            crate::middleware::EffectAttemptOutcome::Executed(Ok(outputs)) => {
                self.post_handle(event, outputs, ctx);
            }
            crate::middleware::EffectAttemptOutcome::Executed(Err(err)) => {
                let error_event = super::classifier::effect_error_event(event, err);
                self.post_handle(event, std::slice::from_ref(&error_event), ctx);
            }
            crate::middleware::EffectAttemptOutcome::SkippedBy(_)
            | crate::middleware::EffectAttemptOutcome::RejectedBy(_) => {
                // The protected call never went out: release any probe lease
                // explicitly and do not classify breaker state.
                self.settle_not_executed(ctx);
            }
        }
    }
}
