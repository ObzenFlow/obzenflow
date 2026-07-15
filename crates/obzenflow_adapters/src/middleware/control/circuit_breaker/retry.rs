// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::classifier::{effect_error_event, FailureClassification};
use super::CircuitBreakerMiddleware;
use crate::middleware::context_keys::{CircuitBreakerRetryAfterMs, EffectCallDurationNanos};
use crate::middleware::{Middleware, MiddlewareContext};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerHealthClassification, CircuitBreakerRetryStopReason,
};
use obzenflow_core::event::types::EventId;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_runtime::effects::{EffectCursor, EffectError};
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::time::Duration;

/// Recovery configuration for a breaker guarding a declared effect
/// (FLOWIP-115h). `attempts` counts every physical call including the first
/// and defaults to 3; eligibility stays gated by effect safety and the typed
/// failure allowlist at materialisation and in the recovery session.
#[derive(Debug, Clone)]
pub struct Retry {
    pub(super) policy: CircuitBreakerRetryPolicy,
    pub(super) limits: RetryLimits,
}

impl Retry {
    /// Retry with a fixed delay before every later attempt.
    pub fn fixed(delay: Duration) -> Self {
        Self::with_backoff(BackoffStrategy::Fixed { delay })
    }

    /// Retry with the default exponential backoff (250 ms initial, factor
    /// 2.0, 4 s strategy cap, jitter).
    pub fn exponential() -> Self {
        Self::with_backoff(BackoffStrategy::Exponential {
            initial: Duration::from_millis(250),
            max: Duration::from_secs(4),
            factor: 2.0,
            jitter: true,
        })
    }

    fn with_backoff(backoff: BackoffStrategy) -> Self {
        Self {
            policy: CircuitBreakerRetryPolicy {
                max_attempts: 3,
                backoff,
                #[cfg(test)]
                deterministic_jitter_samples: None,
            },
            limits: RetryLimits::default(),
        }
    }

    /// Total physical calls, including the first. Zero is invalid.
    pub fn attempts(mut self, attempts: u32) -> Self {
        assert!(
            attempts > 0,
            "circuit-breaker max_attempts must be greater than zero"
        );
        self.policy.max_attempts = attempts;
        self
    }

    /// Cap the breaker-generated delay between calls. A provider's rate-limit
    /// floor may be longer and is never shortened by this cap.
    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.limits.max_single_delay = delay;
        self
    }

    /// The window, measured from the first call, in which another physical
    /// call may start. It gates attempt starts; it does not cancel a call
    /// already in flight.
    pub fn start_window(mut self, window: Duration) -> Self {
        self.limits.max_attempt_start_window = window;
        self
    }

    #[cfg(test)]
    pub(crate) fn jitter_samples(mut self, samples: Vec<f64>) -> Self {
        self.policy.use_deterministic_jitter_samples(samples);
        self
    }
}

/// Retry limits enforced by the circuit breaker.
#[derive(Debug, Clone)]
pub(crate) struct RetryLimits {
    /// Maximum breaker-generated delay between physical calls. A provider's
    /// rate-limit floor may be longer and is never shortened by this cap.
    pub max_single_delay: Duration,
    /// Maximum elapsed time at which another physical call may start.
    pub max_attempt_start_window: Duration,
}

impl Default for RetryLimits {
    fn default() -> Self {
        Self {
            max_single_delay: Duration::from_secs(30),
            max_attempt_start_window: Duration::from_secs(120),
        }
    }
}

/// Configuration for integrated per-event retry inside the circuit breaker.
#[derive(Debug, Clone)]
pub(crate) struct CircuitBreakerRetryPolicy {
    pub max_attempts: u32,
    pub backoff: BackoffStrategy,
    #[cfg(test)]
    pub(super) deterministic_jitter_samples: Option<Vec<f64>>,
}

impl CircuitBreakerRetryPolicy {
    pub(crate) fn calculate_delay(&self, attempt: usize) -> Duration {
        #[cfg(test)]
        if let Some(samples) = &self.deterministic_jitter_samples {
            let sample = samples
                .get(attempt)
                .copied()
                .expect("deterministic jitter sample for every tested continuation");
            return calculate_delay_with_jitter_sample(&self.backoff, attempt, sample);
        }

        self.backoff.calculate_delay(attempt)
    }

    #[cfg(test)]
    pub(crate) fn use_deterministic_jitter_samples(&mut self, samples: Vec<f64>) {
        assert!(samples.iter().all(|sample| (0.0..1.0).contains(sample)));
        self.deterministic_jitter_samples = Some(samples);
    }
}

/// What the breaker directs the boundary to do with one executed attempt.
pub(crate) enum RecoveryDirective {
    Return,
    RetryAfter(Duration),
}

#[derive(Clone, Copy)]
enum RawRecoveryEligibility {
    Eligible { rate_limit_floor: Duration },
    Ineligible,
}

fn raw_recovery_eligibility(error: &EffectError) -> RawRecoveryEligibility {
    match error {
        EffectError::Timeout(_) | EffectError::Transport(_) => RawRecoveryEligibility::Eligible {
            rate_limit_floor: Duration::ZERO,
        },
        EffectError::RateLimited { retry_after, .. } => RawRecoveryEligibility::Eligible {
            rate_limit_floor: *retry_after,
        },
        EffectError::Serialization(_)
        | EffectError::Journal(_)
        | EffectError::MissingRecordedEffect { .. }
        | EffectError::DuplicateRecordedEffect { .. }
        | EffectError::DescriptorMismatch { .. }
        | EffectError::RecordedFailure { .. }
        | EffectError::BoundaryRejected { .. }
        | EffectError::TypedOutcomeCoordination { .. }
        | EffectError::EffectProvenanceMismatch(_)
        | EffectError::IncompleteOutcomeGroup { .. }
        | EffectError::MissingIdempotencyKey { .. }
        | EffectError::UndeclaredEffect { .. }
        | EffectError::UndeclaredOutput { .. }
        | EffectError::EmitUnsupported { .. }
        | EffectError::MissingEffectPort { .. }
        | EffectError::TransactionalCommitMissing { .. }
        | EffectError::Execution(_)
        | EffectError::Permanent(_)
        | EffectError::Validation(_)
        | EffectError::Domain(_)
        | EffectError::ReplayArchive(_) => RawRecoveryEligibility::Ineligible,
    }
}

fn retry_classification_allows(classification: &FailureClassification) -> bool {
    matches!(
        classification,
        FailureClassification::TransientFailure | FailureClassification::RateLimited(_)
    )
}

fn evidence_classification(
    classification: &FailureClassification,
) -> CircuitBreakerHealthClassification {
    match classification {
        FailureClassification::Success => CircuitBreakerHealthClassification::Success,
        FailureClassification::TransientFailure => {
            CircuitBreakerHealthClassification::TransientFailure
        }
        FailureClassification::PermanentFailure => {
            CircuitBreakerHealthClassification::PermanentFailure
        }
        FailureClassification::RateLimited(_) => CircuitBreakerHealthClassification::RateLimited,
        FailureClassification::PartialSuccess { .. } => {
            CircuitBreakerHealthClassification::PartialSuccess
        }
    }
}

fn prepare_classification_context(
    result: &Result<Vec<ChainEvent>, EffectError>,
    ctx: &mut MiddlewareContext,
) {
    ctx.remove::<CircuitBreakerRetryAfterMs>();
    if let Err(EffectError::RateLimited { retry_after, .. }) = result {
        ctx.insert::<CircuitBreakerRetryAfterMs>(
            retry_after.as_millis().min(u64::MAX as u128) as u64
        );
    }
}

fn effect_observation(
    event: &ChainEvent,
    result: &Result<Vec<ChainEvent>, EffectError>,
) -> Vec<ChainEvent> {
    match result {
        Ok(outputs) => outputs.clone(),
        Err(error) => vec![effect_error_event(event, error)],
    }
}

/// One logical invocation's recovery decisions (FLOWIP-115h AR1): attempt
/// state, both eligibility gates, delay and floor arithmetic, evidence, and
/// terminal settlement. The boundary owns only the attempt bracket, the
/// sleep, and outer observation.
pub(crate) struct EffectRecoverySession<'a> {
    breaker: &'a CircuitBreakerMiddleware,
    cursor: EffectCursor,
    cause: EventId,
    started: tokio::time::Instant,
    config: Option<(CircuitBreakerRetryPolicy, RetryLimits)>,
    recovery_allowed: bool,
    attempts: u32,
    circuit_open_stop: bool,
    last_observation: Vec<ChainEvent>,
    last_classification: FailureClassification,
    last_result_ok: bool,
}

impl<'a> EffectRecoverySession<'a> {
    pub(super) fn new(
        breaker: &'a CircuitBreakerMiddleware,
        ctx: &MiddlewareContext,
        cursor: EffectCursor,
        cause: EventId,
    ) -> Self {
        Self {
            breaker,
            cursor,
            cause,
            started: tokio::time::Instant::now(),
            config: breaker.effect_retry_config(),
            recovery_allowed: !breaker.is_effect_probe(ctx),
            attempts: 0,
            circuit_open_stop: false,
            last_observation: Vec::new(),
            last_classification: FailureClassification::Success,
            last_result_ok: false,
        }
    }

    /// Assess one executed attempt: classify it, then either direct another
    /// continuation after a delay or settle with this result.
    pub(crate) fn assess(
        &mut self,
        event: &ChainEvent,
        result: &Result<Vec<ChainEvent>, EffectError>,
        ctx: &mut MiddlewareContext,
    ) -> RecoveryDirective {
        self.attempts = self.attempts.saturating_add(1);
        prepare_classification_context(result, ctx);
        self.last_observation = effect_observation(event, result);
        let (classification, _, _) = self
            .breaker
            .classify_call(event, &self.last_observation, ctx);
        self.last_classification = classification;
        self.last_result_ok = result.is_ok();

        let error = match result {
            Ok(_) => return RecoveryDirective::Return,
            Err(error) => error,
        };
        let Some((retry_policy, retry_limits)) = self.config.as_ref() else {
            return RecoveryDirective::Return;
        };
        if !self.recovery_allowed {
            return RecoveryDirective::Return;
        }

        let eligibility = raw_recovery_eligibility(error);
        if !matches!(eligibility, RawRecoveryEligibility::Eligible { .. })
            || !retry_classification_allows(&self.last_classification)
        {
            if self.attempts > 1 {
                ctx.write_control_event(
                    ChainEventFactory::circuit_breaker_retry_stopped_non_retryable(
                        self.breaker.evidence_writer_id(),
                        self.cursor.clone(),
                        self.attempts,
                        self.cause,
                    ),
                );
            }
            return RecoveryDirective::Return;
        }
        if self.attempts >= retry_policy.max_attempts {
            self.write_exhausted(CircuitBreakerRetryStopReason::AttemptLimit, ctx);
            return RecoveryDirective::Return;
        }
        if !self.breaker.is_closed_for_effect_recovery() {
            self.write_exhausted(CircuitBreakerRetryStopReason::CircuitNoLongerClosed, ctx);
            self.circuit_open_stop = true;
            return RecoveryDirective::Return;
        }

        let generated = retry_policy
            .calculate_delay(self.attempts.saturating_sub(1) as usize)
            .min(retry_limits.max_single_delay);
        let raw_floor = match eligibility {
            RawRecoveryEligibility::Eligible { rate_limit_floor } => rate_limit_floor,
            RawRecoveryEligibility::Ineligible => Duration::ZERO,
        };
        let classification_floor = match &self.last_classification {
            FailureClassification::RateLimited(delay) => *delay,
            _ => Duration::ZERO,
        };
        let delay = generated.max(raw_floor).max(classification_floor);
        if self.started.elapsed().saturating_add(delay) >= retry_limits.max_attempt_start_window {
            self.write_exhausted(CircuitBreakerRetryStopReason::AttemptStartWindow, ctx);
            return RecoveryDirective::Return;
        }
        ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_scheduled(
            self.breaker.evidence_writer_id(),
            self.cursor.clone(),
            self.attempts.saturating_add(1),
            delay.as_millis().min(u64::MAX as u128) as u64,
            self.cause,
        ));
        RecoveryDirective::RetryAfter(delay)
    }

    /// Post-sleep re-check; `true` means the next attempt may start.
    pub(crate) fn recheck_after_delay(&mut self, ctx: &mut MiddlewareContext) -> bool {
        let window = self
            .config
            .as_ref()
            .expect("a retry was scheduled, so retry config exists")
            .1
            .max_attempt_start_window;
        let stop_reason = if self.started.elapsed() >= window {
            Some(CircuitBreakerRetryStopReason::AttemptStartWindow)
        } else if !self.breaker.is_closed_for_effect_recovery() {
            Some(CircuitBreakerRetryStopReason::CircuitNoLongerClosed)
        } else {
            None
        };
        let Some(reason) = stop_reason else {
            return true;
        };
        self.write_exhausted(reason, ctx);
        self.circuit_open_stop =
            matches!(reason, CircuitBreakerRetryStopReason::CircuitNoLongerClosed);
        false
    }

    /// Terminal settlement. Every stop reason converges on the boundary's
    /// terminal tail so evidence and outer observation cannot depend on where
    /// the stop was detected; a circuit-open stop settles no health, because
    /// the invocation that opened the circuit already counted.
    pub(crate) fn settle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) {
        if !self.circuit_open_stop {
            ctx.insert::<EffectCallDurationNanos>(
                self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64,
            );
            self.breaker.post_handle(event, &self.last_observation, ctx);
        }
        if self.attempts > 1 && self.last_result_ok {
            ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_succeeded(
                self.breaker.evidence_writer_id(),
                self.cursor.clone(),
                self.attempts,
                evidence_classification(&self.last_classification),
                self.cause,
            ));
        }
    }

    /// Settle an attempt whose protected call never went out because an inner
    /// policy skipped or rejected it.
    pub(crate) fn settle_not_executed(&self, ctx: &mut MiddlewareContext) {
        self.breaker.settle_not_executed(ctx);
    }

    fn write_exhausted(&self, reason: CircuitBreakerRetryStopReason, ctx: &mut MiddlewareContext) {
        ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_exhausted(
            self.breaker.evidence_writer_id(),
            self.cursor.clone(),
            self.attempts,
            reason,
            self.cause,
        ));
    }
}

#[cfg(test)]
fn calculate_delay_with_jitter_sample(
    backoff: &BackoffStrategy,
    attempt: usize,
    sample: f64,
) -> Duration {
    match backoff {
        BackoffStrategy::Fixed { delay } => *delay,
        BackoffStrategy::Exponential {
            initial,
            max,
            factor,
            jitter,
        } => {
            let base_delay = initial.as_millis() as f64 * factor.powi(attempt as i32);
            let capped_delay = base_delay.min(max.as_millis() as f64);
            let final_delay = if *jitter {
                capped_delay * (1.0 + (sample - 0.5) * 0.2)
            } else {
                capped_delay
            };
            Duration::from_millis(final_delay as u64)
        }
    }
}
