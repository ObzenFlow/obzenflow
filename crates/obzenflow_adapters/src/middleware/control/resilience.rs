// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Fixed effect-bound resilience aggregate (FLOWIP-115n).

use super::circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfigError, CircuitBreakerFactory, CircuitBreakerFailureMode,
    CircuitBreakerMiddleware, EffectAdmissionEpoch, EffectAdmissionFence, FailureClassification,
    FailureWindow, Retry,
};
use super::rate_limiter::{
    RateLimitReservation, RateLimiter, RateLimiterConfigError, RateLimiterMiddleware,
};
use crate::middleware::context_keys::{CircuitBreakerRetryAfterMs, EffectCallDurationNanos};
use crate::middleware::{
    validate_attachment_request, EffectPolicyAttachment, MaterializationClaim,
    MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareHints, MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety,
    MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceAttachmentKind,
    PolicyAdmission,
};
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerHealthClassification, CircuitBreakerRetryStopReason,
};
use obzenflow_core::event::{
    ChainEventFactory, CircuitBreakerAttemptSettledEventParams,
    CircuitBreakerRecoveryCompletedEventParams, EffectFailureCause,
};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
#[cfg(test)]
use obzenflow_runtime::effects::EffectCursor;
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundaryOutcome, EffectBoundaryReport, EffectError, EffectIdentity,
    PhysicalCallObservation, PhysicalCallOutcome, PhysicalCallReceipt, RepeatableEffectOperation,
    SingleUseEffectBoundaryReport, SingleUseEffectOperation,
};
use obzenflow_runtime::runtime_config::{
    ConfigValue, DslConfigDefault, RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY,
    RESILIENCE_BREAKER_COUNT_WINDOW_KEY, RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
    RESILIENCE_BREAKER_MINIMUM_CALLS_KEY, RESILIENCE_BREAKER_MODE_KEY,
    RESILIENCE_BREAKER_OPEN_FOR_MS_KEY, RESILIENCE_BREAKER_PROBES_KEY,
    RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
    RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY, RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
    RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY, RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY,
    RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY, RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY,
    RESILIENCE_RETRY_FIXED_DELAY_MS_KEY, RESILIENCE_RETRY_KIND_KEY,
    RESILIENCE_RETRY_MAX_ATTEMPTS_KEY, RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY,
};
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

pub struct EffectResilience;
pub struct EffectResilienceFamily;

pub struct EffectResilienceBuilder {
    breaker: CircuitBreaker,
    retry: Option<Retry>,
    rate_limiter: Option<RateLimiter>,
}

#[derive(Debug, Error)]
pub enum EffectResilienceConfigError {
    #[error(transparent)]
    CircuitBreaker(#[from] CircuitBreakerConfigError),
    #[error(transparent)]
    RateLimiter(#[from] RateLimiterConfigError),
    #[error("retry max_attempts must be greater than zero")]
    ZeroRetryAttempts,
    #[error("retry max_backoff must be greater than zero")]
    ZeroMaxBackoff,
    #[error("retry attempt_start_window must be greater than zero")]
    ZeroAttemptStartWindow,
    #[error("fixed retry delay must be greater than zero")]
    ZeroFixedDelay,
}

impl EffectResilience {
    pub fn with_breaker(breaker: CircuitBreaker) -> EffectResilienceBuilder {
        EffectResilienceBuilder {
            breaker,
            retry: None,
            rate_limiter: None,
        }
    }
}

impl EffectResilienceBuilder {
    pub fn retry(mut self, retry: impl Into<Option<Retry>>) -> Self {
        self.retry = retry.into();
        self
    }

    pub fn rate_limit_each_attempt(mut self, rate_limiter: RateLimiter) -> Self {
        self.rate_limiter = Some(rate_limiter);
        self
    }

    pub fn build(self) -> Result<Box<dyn MiddlewareFactory>, EffectResilienceConfigError> {
        if let Some(retry) = &self.retry {
            validate_retry(retry)?;
        }
        if let Some(limiter) = &self.rate_limiter {
            limiter.validate()?;
        }
        Ok(Box::new(EffectResilienceFactory {
            breaker: self.breaker,
            retry: self.retry,
            rate_limiter: self.rate_limiter,
        }))
    }
}

fn validate_retry(retry: &Retry) -> Result<(), EffectResilienceConfigError> {
    if retry.policy.max_attempts == 0 {
        return Err(EffectResilienceConfigError::ZeroRetryAttempts);
    }
    if retry.limits.max_single_delay.is_zero() {
        return Err(EffectResilienceConfigError::ZeroMaxBackoff);
    }
    if retry.limits.max_attempt_start_window.is_zero() {
        return Err(EffectResilienceConfigError::ZeroAttemptStartWindow);
    }
    if matches!(retry.policy.backoff, BackoffStrategy::Fixed { delay } if delay.is_zero()) {
        return Err(EffectResilienceConfigError::ZeroFixedDelay);
    }
    Ok(())
}

struct EffectResilienceFactory {
    breaker: CircuitBreaker,
    retry: Option<Retry>,
    rate_limiter: Option<RateLimiter>,
}

impl EffectResilienceFactory {
    fn breaker_defaults(&self) -> Vec<DslConfigDefault> {
        let mut defaults = Vec::new();
        match &self.breaker.config.failure_mode {
            CircuitBreakerFailureMode::Consecutive { max_failures } => {
                defaults.push(default_text(RESILIENCE_BREAKER_MODE_KEY, "consecutive"));
                defaults.push(default_u64(
                    RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY,
                    max_failures.get() as u64,
                ));
            }
            CircuitBreakerFailureMode::RateBased {
                window,
                failure_rate_threshold,
                slow_call_rate_threshold,
                slow_call_duration_threshold,
                minimum_calls,
            } => {
                defaults.push(default_text(RESILIENCE_BREAKER_MODE_KEY, "rate_based"));
                let FailureWindow::Count { size } = window;
                defaults.push(default_u64(
                    RESILIENCE_BREAKER_COUNT_WINDOW_KEY,
                    *size as u64,
                ));
                defaults.push(default_u64(
                    RESILIENCE_BREAKER_MINIMUM_CALLS_KEY,
                    minimum_calls.get() as u64,
                ));
                if let Some(threshold) = failure_rate_threshold {
                    defaults.push(default_f64(
                        RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
                        threshold.get(),
                    ));
                }
                if let Some(duration) = slow_call_duration_threshold {
                    defaults.push(default_u64(
                        RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY,
                        duration_ms(*duration),
                    ));
                }
                if let Some(threshold) = slow_call_rate_threshold {
                    defaults.push(default_f64(
                        RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
                        threshold.get(),
                    ));
                }
            }
        }
        defaults.push(default_u64(
            RESILIENCE_BREAKER_OPEN_FOR_MS_KEY,
            duration_ms(self.breaker.config.open_for),
        ));
        defaults.push(default_u64(
            RESILIENCE_BREAKER_PROBES_KEY,
            self.breaker.config.probes.get() as u64,
        ));
        defaults.push(DslConfigDefault {
            key_path: RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
            value: ConfigValue::Bool(self.breaker.config.rate_limited_counts_as_failure),
        });
        defaults
    }

    fn retry_defaults(&self) -> Vec<DslConfigDefault> {
        let Some(retry) = &self.retry else {
            return Vec::new();
        };
        let mut defaults = match retry.policy.backoff {
            BackoffStrategy::Fixed { delay } => vec![
                default_text(RESILIENCE_RETRY_KIND_KEY, "fixed"),
                default_u64(RESILIENCE_RETRY_FIXED_DELAY_MS_KEY, duration_ms(delay)),
            ],
            BackoffStrategy::Exponential { .. } => {
                vec![default_text(RESILIENCE_RETRY_KIND_KEY, "exponential")]
            }
        };
        defaults.extend([
            default_u64(
                RESILIENCE_RETRY_MAX_ATTEMPTS_KEY,
                retry.policy.max_attempts as u64,
            ),
            default_u64(
                RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY,
                duration_ms(retry.limits.max_single_delay),
            ),
            default_u64(
                RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY,
                duration_ms(retry.limits.max_attempt_start_window),
            ),
        ]);
        defaults
    }

    fn limiter_defaults(&self) -> Vec<DslConfigDefault> {
        let Some(limiter) = &self.rate_limiter else {
            return Vec::new();
        };
        let mut defaults = vec![
            default_f64(
                RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY,
                limiter.events_per_second,
            ),
            default_f64(
                RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY,
                limiter.cost_per_attempt,
            ),
        ];
        if let Some(burst) = limiter.burst_capacity {
            defaults.push(default_f64(
                RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY,
                burst,
            ));
        }
        defaults
    }

    fn resolved_breaker(
        &self,
        view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    ) -> Result<CircuitBreaker, CircuitBreakerConfigError> {
        let mode = text_value(view, RESILIENCE_BREAKER_MODE_KEY).unwrap_or_default();
        let mut builder = CircuitBreaker::builder()
            .open_for(Duration::from_millis(required_u64(
                view,
                RESILIENCE_BREAKER_OPEN_FOR_MS_KEY,
            )))
            .probes(required_u64(view, RESILIENCE_BREAKER_PROBES_KEY) as u32)
            .rate_limited_counts_as_failure(
                bool_value(view, RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY)
                    .unwrap_or(false),
            );
        match mode {
            "consecutive" => {
                builder = builder.consecutive_failures(required_u64(
                    view,
                    RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY,
                ) as u32);
            }
            "rate_based" => {
                builder = builder
                    .count_window(required_u64(view, RESILIENCE_BREAKER_COUNT_WINDOW_KEY) as u32)
                    .minimum_calls(required_u64(view, RESILIENCE_BREAKER_MINIMUM_CALLS_KEY) as u32);
                if let Some(value) = f64_value(view, RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY)
                {
                    builder = builder.failure_rate_threshold(value);
                }
                if let Some(value) = u64_value(view, RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY) {
                    builder = builder.slow_call_duration(Duration::from_millis(value));
                }
                if let Some(value) =
                    f64_value(view, RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY)
                {
                    builder = builder.slow_call_rate_threshold(value);
                }
            }
            _ => return Err(CircuitBreakerConfigError::MissingMode),
        }
        builder
            .build()
            .map(|breaker| breaker.inherit_classifier_from(&self.breaker))
    }

    fn resolved_retry(
        &self,
        view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    ) -> Result<Option<Retry>, EffectResilienceConfigError> {
        if self.retry.is_none() {
            return Ok(None);
        }
        let mut retry = match text_value(view, RESILIENCE_RETRY_KIND_KEY) {
            Some("fixed") => Retry::fixed(Duration::from_millis(required_u64(
                view,
                RESILIENCE_RETRY_FIXED_DELAY_MS_KEY,
            ))),
            Some("exponential") => Retry::exponential(),
            _ => Retry::fixed(Duration::ZERO),
        };
        retry = retry
            .max_attempts(required_u64(view, RESILIENCE_RETRY_MAX_ATTEMPTS_KEY) as u32)
            .max_backoff(Duration::from_millis(required_u64(
                view,
                RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY,
            )))
            .attempt_start_window(Duration::from_millis(required_u64(
                view,
                RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY,
            )));
        validate_retry(&retry)?;
        Ok(Some(retry))
    }

    fn resolved_limiter(
        &self,
        view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    ) -> Result<Option<RateLimiter>, RateLimiterConfigError> {
        if self.rate_limiter.is_none() {
            return Ok(None);
        }
        let limiter = RateLimiter {
            events_per_second: f64_value(view, RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY)
                .unwrap_or(0.0),
            burst_capacity: f64_value(view, RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY),
            cost_per_attempt: f64_value(view, RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY)
                .unwrap_or(0.0),
        };
        limiter.validate()?;
        Ok(Some(limiter))
    }
}

impl MiddlewareFactory for EffectResilienceFactory {
    fn label(&self) -> &'static str {
        "effect_resilience"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<EffectResilienceFamily>("effect_resilience")
    }

    fn dsl_config_defaults(&self) -> Vec<DslConfigDefault> {
        let mut defaults = self.breaker_defaults();
        defaults.extend(self.limiter_defaults());
        defaults.extend(self.retry_defaults());
        defaults
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        let mut keys = vec![
            RESILIENCE_BREAKER_MODE_KEY,
            RESILIENCE_BREAKER_CONSECUTIVE_FAILURES_KEY,
            RESILIENCE_BREAKER_COUNT_WINDOW_KEY,
            RESILIENCE_BREAKER_MINIMUM_CALLS_KEY,
            RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
            RESILIENCE_BREAKER_SLOW_CALL_DURATION_MS_KEY,
            RESILIENCE_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
            RESILIENCE_BREAKER_OPEN_FOR_MS_KEY,
            RESILIENCE_BREAKER_PROBES_KEY,
            RESILIENCE_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
        ];
        if self.retry.is_some() {
            keys.extend([
                RESILIENCE_RETRY_KIND_KEY,
                RESILIENCE_RETRY_FIXED_DELAY_MS_KEY,
                RESILIENCE_RETRY_MAX_ATTEMPTS_KEY,
                RESILIENCE_RETRY_MAX_BACKOFF_MS_KEY,
                RESILIENCE_RETRY_ATTEMPT_START_WINDOW_MS_KEY,
            ]);
        }
        if self.rate_limiter.is_some() {
            keys.extend([
                RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY,
                RESILIENCE_RATE_LIMITER_BURST_CAPACITY_KEY,
                RESILIENCE_RATE_LIMITER_COST_PER_ATTEMPT_KEY,
            ]);
        }
        keys
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::effect_resilience(self.label(), self.override_key().family_label())
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let declaration = self.declaration();
        validate_attachment_request(&declaration, &request).map_err(|err| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, err)
        })?;
        context
            .authorize_materialization(
                MaterializationClaim::EffectResilience,
                &declaration,
                &request,
            )
            .map_err(|error| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    error,
                )
            })?;
        let MiddlewareSurface::Effect(effect) = request.surface else {
            return Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other("EffectResilience attaches only to declared effects"),
            ));
        };
        let view = context.config_view();
        let breaker = self.resolved_breaker(&view).map_err(|err| {
            MiddlewareFactoryError::invalid_configuration(self.label(), &context.config.name, err)
        })?;
        let retry = self.resolved_retry(&view).map_err(|err| {
            MiddlewareFactoryError::invalid_configuration(self.label(), &context.config.name, err)
        })?;
        if retry.is_some()
            && !matches!(
                effect.safety,
                obzenflow_runtime::effects::EffectSafety::Idempotent
                    | obzenflow_runtime::effects::EffectSafety::NonIdempotentRequiresKey
            )
        {
            return Err(MiddlewareFactoryError::invalid_configuration(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!(
                    "EffectResilience retry is not eligible for effect '{}' with safety {:?}",
                    effect.effect_type.as_str(),
                    effect.safety,
                )),
            ));
        }
        let limiter = self.resolved_limiter(&view).map_err(|err| {
            MiddlewareFactoryError::invalid_configuration(self.label(), &context.config.name, err)
        })?;

        let (breaker, _) = CircuitBreakerFactory::from_effect_breaker(&breaker)
            .build_middleware_keyed(
                context.config,
                context,
                MaterializationClaim::EffectResilience,
                Some(effect.effect_type.clone()),
            )?;
        let breaker = Arc::new(breaker);
        let limiter = limiter
            .map(|config| {
                RateLimiterMiddleware::new_keyed(
                    context.config.stage_id,
                    config.validate().expect("resolved limiter was validated"),
                    context,
                    MaterializationClaim::EffectResilience,
                    Some(effect.effect_type.clone()),
                )
                .map(Arc::new)
            })
            .transpose()
            .map_err(|message| {
                MiddlewareFactoryError::invalid_configuration(
                    self.label(),
                    &context.config.name,
                    std::io::Error::other(message),
                )
            })?;

        MiddlewareSurfaceAttachment::claimed(
            MiddlewareSurfaceAttachmentKind::Effect(EffectPolicyAttachment::effect_resilience(
                Arc::new(EffectResilienceMiddleware {
                    breaker,
                    retry,
                    limiter,
                    #[cfg(test)]
                    final_admission_test_gate: std::sync::Mutex::new(None),
                }),
            )),
            MaterializationClaim::EffectResilience,
            context,
        )
        .map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })
    }

    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Advanced
    }

    fn hints(&self) -> MiddlewareHints {
        MiddlewareHints {
            rate_limits: self.rate_limiter.is_some(),
            ..Default::default()
        }
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        Some(serde_json::json!({
            "kind": "effect_resilience",
            "breaker": self.breaker_defaults().into_iter().map(|d| (d.key_path.to_string(), d.value.to_json())).collect::<serde_json::Map<_, _>>(),
            "retry": self.retry.as_ref().map(|_| self.retry_defaults().into_iter().map(|d| (d.key_path.to_string(), d.value.to_json())).collect::<serde_json::Map<_, _>>()),
            "rate_limiter": self.rate_limiter.as_ref().map(|_| self.limiter_defaults().into_iter().map(|d| (d.key_path.to_string(), d.value.to_json())).collect::<serde_json::Map<_, _>>()),
        }))
    }
}

pub(in crate::middleware::control) struct EffectResilienceMiddleware {
    breaker: Arc<CircuitBreakerMiddleware>,
    retry: Option<Retry>,
    limiter: Option<Arc<RateLimiterMiddleware>>,
    #[cfg(test)]
    final_admission_test_gate: std::sync::Mutex<Option<FinalAdmissionTestGate>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryCancellationState {
    Idle,
    WaitingForLimiter,
    Reserved,
    InFlight,
    Complete,
}

enum RecoveryTerminalDecision {
    BoundaryRejected(Box<crate::middleware::MiddlewareAbortCause>),
    ReturnLastPhysicalError,
    ReturnPhysicalResult,
}

#[derive(Debug, Clone, Copy)]
struct RecoveryCompletion {
    total_attempts: u32,
    backoff_elapsed: Duration,
    recovery_elapsed: Duration,
}

/// Invocation-local authority for effect recovery.
///
/// In particular, the initial breaker epoch is write-once and the affine
/// limiter reservation lives here until it is committed immediately before a
/// physical call or cancelled by any terminal path.
struct EffectRecoveryController {
    session_started: Instant,
    first_physical_call_started: Option<Instant>,
    attempt_start_deadline: Option<Instant>,
    initial_breaker_epoch: Option<EffectAdmissionEpoch>,
    reservation_epoch: Option<EffectAdmissionEpoch>,
    attempt_ordinal: u32,
    retry_count: u32,
    cumulative_backoff: Duration,
    current_reservation: Option<RateLimitReservation>,
    cancellation_state: RecoveryCancellationState,
    last_physical_error: Option<EffectError>,
    terminal_decision: Option<RecoveryTerminalDecision>,
    completion: Option<RecoveryCompletion>,
}

impl EffectRecoveryController {
    fn new() -> Self {
        Self {
            session_started: Instant::now(),
            first_physical_call_started: None,
            attempt_start_deadline: None,
            initial_breaker_epoch: None,
            reservation_epoch: None,
            attempt_ordinal: 0,
            retry_count: 0,
            cumulative_backoff: Duration::ZERO,
            current_reservation: None,
            cancellation_state: RecoveryCancellationState::Idle,
            last_physical_error: None,
            terminal_decision: None,
            completion: None,
        }
    }

    fn initial_epoch(&self) -> Option<EffectAdmissionEpoch> {
        self.initial_breaker_epoch
    }

    fn observe_precheck(&mut self, reservation_epoch: EffectAdmissionEpoch) {
        match self.initial_breaker_epoch {
            Some(initial) => debug_assert_eq!(initial, reservation_epoch),
            None => self.initial_breaker_epoch = Some(reservation_epoch),
        }
        self.reservation_epoch = Some(reservation_epoch);
    }

    fn admission_fence(&self) -> EffectAdmissionFence {
        EffectAdmissionFence::new(
            self.initial_breaker_epoch
                .expect("precheck must capture the initial breaker epoch"),
            self.reservation_epoch
                .expect("precheck must capture the reservation breaker epoch"),
        )
    }

    fn begin_limiter_wait(&mut self) {
        debug_assert!(self.current_reservation.is_none());
        debug_assert_eq!(self.cancellation_state, RecoveryCancellationState::Idle);
        self.cancellation_state = RecoveryCancellationState::WaitingForLimiter;
    }

    fn install_reservation(&mut self, reservation: Option<RateLimitReservation>) {
        debug_assert!(self.current_reservation.is_none());
        debug_assert_eq!(
            self.cancellation_state,
            RecoveryCancellationState::WaitingForLimiter
        );
        self.current_reservation = reservation;
        self.cancellation_state = RecoveryCancellationState::Reserved;
    }

    fn cancel_reservation(&mut self) {
        drop(self.current_reservation.take());
        self.cancellation_state = RecoveryCancellationState::Idle;
    }

    fn later_attempt_may_start(&self) -> bool {
        self.attempt_ordinal == 0
            || self
                .attempt_start_deadline
                .is_some_and(|deadline| Instant::now() < deadline)
    }

    fn backoff_fits_attempt_window(&self, delay: Duration) -> bool {
        debug_assert!(self.attempt_ordinal > 0);
        self.attempt_start_deadline.is_some_and(|deadline| {
            Instant::now()
                .checked_add(delay)
                .is_some_and(|wake| wake < deadline)
        })
    }

    fn begin_physical_attempt(&mut self, attempt_start_window: Option<Duration>) -> u32 {
        debug_assert_eq!(self.cancellation_state, RecoveryCancellationState::Reserved);
        let now = Instant::now();
        if self.first_physical_call_started.is_none() {
            self.first_physical_call_started = Some(now);
            self.attempt_start_deadline =
                attempt_start_window.and_then(|window| now.checked_add(window));
        }
        self.attempt_ordinal = self.attempt_ordinal.saturating_add(1);
        self.retry_count = self.attempt_ordinal.saturating_sub(1);
        self.cancellation_state = RecoveryCancellationState::InFlight;
        if let Some(reservation) = self.current_reservation.take() {
            reservation.commit();
        }
        self.attempt_ordinal
    }

    fn finish_physical_attempt(&mut self, result: &Result<Vec<ChainEvent>, EffectError>) {
        debug_assert_eq!(self.cancellation_state, RecoveryCancellationState::InFlight);
        if let Err(error) = result {
            self.last_physical_error = Some(error.clone());
        }
        self.cancellation_state = RecoveryCancellationState::Idle;
    }

    fn record_backoff(&mut self, elapsed: Duration) {
        self.cumulative_backoff = self.cumulative_backoff.saturating_add(elapsed);
    }

    fn attempts(&self) -> u32 {
        self.attempt_ordinal
    }

    fn next_attempt(&self) -> u32 {
        self.attempt_ordinal.saturating_add(1)
    }

    fn finish_from_admission(
        &mut self,
        cause: Box<crate::middleware::MiddlewareAbortCause>,
    ) -> RecoveryTerminalDecision {
        self.cancel_reservation();
        self.terminal_decision = Some(if self.attempt_ordinal == 0 {
            RecoveryTerminalDecision::BoundaryRejected(cause)
        } else {
            debug_assert!(self.last_physical_error.is_some());
            drop(cause);
            RecoveryTerminalDecision::ReturnLastPhysicalError
        });
        self.take_terminal_decision()
    }

    fn finish_with_physical_result(&mut self) -> RecoveryTerminalDecision {
        self.cancel_reservation();
        self.terminal_decision = Some(RecoveryTerminalDecision::ReturnPhysicalResult);
        self.take_terminal_decision()
    }

    fn finish_with_last_physical_error(&mut self) -> RecoveryTerminalDecision {
        self.cancel_reservation();
        self.terminal_decision = Some(RecoveryTerminalDecision::ReturnLastPhysicalError);
        self.take_terminal_decision()
    }

    fn take_last_physical_error(&mut self) -> EffectError {
        self.last_physical_error
            .take()
            .expect("a continuation terminal must preserve a prior physical error")
    }

    fn take_terminal_decision(&mut self) -> RecoveryTerminalDecision {
        self.cancellation_state = RecoveryCancellationState::Complete;
        let completion = RecoveryCompletion {
            total_attempts: self.attempt_ordinal,
            backoff_elapsed: self.cumulative_backoff,
            recovery_elapsed: self.session_started.elapsed(),
        };
        self.completion = Some(completion);
        tracing::trace!(
            attempts = self.attempt_ordinal,
            retries = self.retry_count,
            recovery_elapsed_ms = duration_ms(completion.recovery_elapsed),
            backoff_elapsed_ms = duration_ms(completion.backoff_elapsed),
            "effect recovery reached a terminal decision"
        );
        self.terminal_decision
            .take()
            .expect("terminal decision must be installed before it is taken")
    }

    fn take_completion(&mut self) -> RecoveryCompletion {
        self.completion
            .take()
            .expect("normal terminal selection must capture recovery clocks exactly once")
    }
}

#[cfg(test)]
#[derive(Clone)]
pub(in crate::middleware::control) struct FinalAdmissionTestGate {
    target_cursor: EffectCursor,
    completed_attempts: u32,
    reached: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}

#[cfg(test)]
impl FinalAdmissionTestGate {
    pub(in crate::middleware::control) fn new(
        target_cursor: EffectCursor,
        completed_attempts: u32,
    ) -> Self {
        Self {
            target_cursor,
            completed_attempts,
            reached: Arc::new(tokio::sync::Semaphore::new(0)),
            release: Arc::new(tokio::sync::Semaphore::new(0)),
        }
    }

    pub(in crate::middleware::control) async fn wait_until_reached(&self) {
        self.reached
            .acquire()
            .await
            .expect("final-admission test gate must remain open")
            .forget();
    }

    pub(in crate::middleware::control) fn release(&self) {
        self.release.add_permits(1);
    }

    async fn pause_if_target(&self, cursor: &EffectCursor, completed_attempts: u32) {
        if &self.target_cursor != cursor || self.completed_attempts != completed_attempts {
            return;
        }
        self.reached.add_permits(1);
        self.release
            .acquire()
            .await
            .expect("final-admission test gate must remain open")
            .forget();
    }
}

/// Synchronous affine settlement for cancellation while the physical future
/// is owned by the aggregate. Normal completion disarms it after reading the
/// runtime-owned receipt; task drop maps only Started to CancelledInFlight.
struct AttemptSettlementGuard {
    breaker: Arc<CircuitBreakerMiddleware>,
    receipt: PhysicalCallReceipt,
    armed: bool,
}

impl AttemptSettlementGuard {
    fn new(
        breaker: Arc<CircuitBreakerMiddleware>,
        receipt: PhysicalCallReceipt,
    ) -> AttemptSettlementGuard {
        Self {
            breaker,
            receipt,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for AttemptSettlementGuard {
    fn drop(&mut self) {
        if self.armed
            && matches!(
                self.receipt.observation(),
                PhysicalCallObservation::Started { .. }
            )
        {
            self.breaker.settle_cancelled_in_flight();
        }
    }
}

impl EffectResilienceMiddleware {
    #[cfg(test)]
    pub(in crate::middleware::control) fn set_final_admission_test_gate(
        &self,
        gate: FinalAdmissionTestGate,
    ) {
        *self
            .final_admission_test_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(gate);
    }

    #[cfg(test)]
    pub(in crate::middleware::control) fn expire_breaker_cooldown_for_test(&self) {
        self.breaker.expire_effect_cooldown_for_test();
    }

    #[cfg(test)]
    async fn pause_before_final_admission(&self, cursor: &EffectCursor, completed_attempts: u32) {
        let gate = self
            .final_admission_test_gate
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone();
        if let Some(gate) = gate {
            gate.pause_if_target(cursor, completed_attempts).await;
        }
    }

    pub(in crate::middleware::control) async fn execute_repeatable(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        ctx: &mut crate::middleware::MiddlewareContext,
        operation: &mut RepeatableEffectOperation,
    ) -> EffectBoundaryReport {
        debug_assert_eq!(
            ctx.execution_scope(),
            MiddlewareExecutionScope::LiveEffectBoundary
        );
        let mut recovery = EffectRecoveryController::new();

        loop {
            if recovery.attempts() > 0 && !recovery.later_attempt_may_start() {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    CircuitBreakerRetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }

            let reservation_epoch =
                match self.breaker.effect_precheck(ctx, recovery.initial_epoch()) {
                    Ok(epoch) => epoch,
                    Err(cause) => {
                        return repeatable_admission_rejected(
                            &mut recovery,
                            self.breaker.as_ref(),
                            identity,
                            cause,
                            event,
                            ctx,
                        )
                    }
                };
            recovery.observe_precheck(reservation_epoch);

            let admission_started = Instant::now();
            recovery.begin_limiter_wait();
            let reservation = match &self.limiter {
                Some(limiter) => Some(limiter.reserve_permit_async(ctx).await),
                None => None,
            };
            recovery.install_reservation(reservation);

            #[cfg(test)]
            self.pause_before_final_admission(&identity.cursor, recovery.attempts())
                .await;

            // The attempt-start window is anchored at the first physical call,
            // not at session creation. Recheck after every limiter wait, while
            // the reservation is still affine and refundable.
            if recovery.attempts() > 0 && !recovery.later_attempt_may_start() {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    CircuitBreakerRetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }

            if let PolicyAdmission::Reject(cause) =
                self.breaker.effect_admit(ctx, recovery.admission_fence())
            {
                return repeatable_admission_rejected(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    cause,
                    event,
                    ctx,
                );
            }

            // Keep the final time decision adjacent to physical admission. If
            // the tiny breaker critical section crossed the deadline, release
            // any probe lease and refund the limiter reservation.
            if recovery.attempts() > 0 && !recovery.later_attempt_may_start() {
                self.breaker.settle_not_executed(ctx);
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    CircuitBreakerRetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }
            let admission_wait = admission_started.elapsed();

            let prepared = operation.prepare();
            let receipt = prepared.receipt();
            let mut settlement_guard =
                AttemptSettlementGuard::new(self.breaker.clone(), receipt.clone());
            let attempt = recovery.begin_physical_attempt(
                self.retry
                    .as_ref()
                    .map(|retry| retry.limits.max_attempt_start_window),
            );
            let result = prepared.execute().await;
            let observation = receipt.observation();
            settlement_guard.disarm();
            recovery.finish_physical_attempt(&result);
            let (physical_outcome, dependency_elapsed) = match observation {
                PhysicalCallObservation::Completed {
                    outcome,
                    dependency_elapsed,
                } => (outcome, dependency_elapsed),
                PhysicalCallObservation::Prepared | PhysicalCallObservation::Started { .. } => {
                    self.breaker.settle_not_executed(ctx);
                    return repeatable_physical_terminal(
                        &mut recovery,
                        self.breaker.as_ref(),
                        identity,
                        result,
                        event,
                        ctx,
                    );
                }
            };
            ctx.insert::<EffectCallDurationNanos>(
                dependency_elapsed.as_nanos().min(u64::MAX as u128) as u64,
            );
            prepare_retry_context(&result, ctx);
            let classification = classify_physical_result(
                self.breaker.as_ref(),
                event,
                &result,
                physical_outcome,
                ctx,
            );
            if let Some(classification) = classification.as_ref() {
                self.breaker.settle_classified_call(classification, ctx);
            } else {
                self.breaker.settle_unobserved_call(ctx);
            }
            if let Some(limiter) = &self.limiter {
                limiter.observe_resilience_attempt(ctx);
            }
            ctx.write_control_event(ChainEventFactory::circuit_breaker_attempt_settled(
                self.breaker.evidence_writer_id(),
                CircuitBreakerAttemptSettledEventParams {
                    cursor: identity.cursor.clone(),
                    attempt,
                    health_classification: evidence_classification(classification.as_ref()),
                    slow: self.breaker.is_slow_dependency_call(dependency_elapsed),
                    dependency_elapsed_ms: duration_ms(dependency_elapsed),
                    admission_wait_ms: duration_ms(admission_wait),
                },
                event.id,
            ));

            let Some(retry) = &self.retry else {
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    result,
                    event,
                    ctx,
                );
            };
            let Err(error) = &result else {
                if recovery.attempts() > 1 {
                    ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_succeeded(
                        self.breaker.evidence_writer_id(),
                        identity.cursor.clone(),
                        recovery.attempts(),
                        evidence_classification(classification.as_ref()),
                        event.id,
                    ));
                }
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    result,
                    event,
                    ctx,
                );
            };
            if physical_outcome != PhysicalCallOutcome::Failed || !retryable_error(error) {
                if recovery.attempts() > 1 {
                    ctx.write_control_event(
                        ChainEventFactory::circuit_breaker_retry_stopped_non_retryable(
                            self.breaker.evidence_writer_id(),
                            identity.cursor.clone(),
                            recovery.attempts(),
                            event.id,
                        ),
                    );
                }
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    result,
                    event,
                    ctx,
                );
            }
            if recovery.attempts() >= retry.policy.max_attempts {
                write_exhausted(
                    self.breaker.as_ref(),
                    identity,
                    recovery.attempts(),
                    CircuitBreakerRetryStopReason::AttemptLimit,
                    event,
                    ctx,
                );
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    result,
                    event,
                    ctx,
                );
            }
            if self.breaker.is_effect_probe(ctx)
                || !self.breaker.effect_recovery_epoch_is_current(
                    recovery
                        .initial_epoch()
                        .expect("a physical attempt must have an initial epoch"),
                )
            {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
                    event,
                    ctx,
                );
            }

            let delay = retry_delay(retry, recovery.attempts(), error);
            if !recovery.backoff_fits_attempt_window(delay) {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    CircuitBreakerRetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }
            ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_scheduled(
                self.breaker.evidence_writer_id(),
                identity.cursor.clone(),
                recovery.next_attempt(),
                duration_ms(delay),
                event.id,
            ));
            let backoff_started = Instant::now();
            tokio::time::sleep(delay).await;
            recovery.record_backoff(backoff_started.elapsed());
            if !recovery.later_attempt_may_start() {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    CircuitBreakerRetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }
            if !self.breaker.effect_recovery_epoch_is_current(
                recovery
                    .initial_epoch()
                    .expect("a physical attempt must have an initial epoch"),
            ) {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
                    event,
                    ctx,
                );
            }
        }
    }

    pub(in crate::middleware::control) async fn execute_single_use(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        ctx: &mut crate::middleware::MiddlewareContext,
        operation: SingleUseEffectOperation,
    ) -> SingleUseEffectBoundaryReport {
        debug_assert_eq!(
            identity.safety,
            obzenflow_runtime::effects::EffectSafety::Transactional
        );
        debug_assert!(self.retry.is_none());
        let mut recovery = EffectRecoveryController::new();

        let reservation_epoch = match self.breaker.effect_precheck(ctx, None) {
            Ok(epoch) => epoch,
            Err(cause) => {
                let decision = recovery.finish_from_admission(cause);
                write_recovery_completed(
                    &mut recovery,
                    self.breaker.as_ref(),
                    identity,
                    event,
                    ctx,
                );
                return single_use_admission_terminal(decision, ctx, operation);
            }
        };
        recovery.observe_precheck(reservation_epoch);

        let admission_started = Instant::now();
        recovery.begin_limiter_wait();
        let reservation = match &self.limiter {
            Some(limiter) => Some(limiter.reserve_permit_async(ctx).await),
            None => None,
        };
        recovery.install_reservation(reservation);

        #[cfg(test)]
        self.pause_before_final_admission(&identity.cursor, recovery.attempts())
            .await;

        if let PolicyAdmission::Reject(cause) =
            self.breaker.effect_admit(ctx, recovery.admission_fence())
        {
            let decision = recovery.finish_from_admission(cause);
            write_recovery_completed(&mut recovery, self.breaker.as_ref(), identity, event, ctx);
            return single_use_admission_terminal(decision, ctx, operation);
        }
        let admission_wait = admission_started.elapsed();

        let prepared = operation.prepare();
        let receipt = prepared.receipt();
        let mut settlement_guard =
            AttemptSettlementGuard::new(self.breaker.clone(), receipt.clone());
        let attempt = recovery.begin_physical_attempt(None);
        let execution = prepared.execute().await;
        settlement_guard.disarm();
        recovery.finish_physical_attempt(execution.result());
        let PhysicalCallObservation::Completed {
            outcome,
            dependency_elapsed,
        } = receipt.observation()
        else {
            self.breaker.settle_not_executed(ctx);
            let terminal = recovery.finish_with_physical_result();
            debug_assert!(matches!(
                terminal,
                RecoveryTerminalDecision::ReturnPhysicalResult
            ));
            write_recovery_completed(&mut recovery, self.breaker.as_ref(), identity, event, ctx);
            return execution.into_report(ctx.take_control_events());
        };

        ctx.insert::<EffectCallDurationNanos>(
            dependency_elapsed.as_nanos().min(u64::MAX as u128) as u64
        );
        prepare_retry_context(execution.result(), ctx);
        let classification = classify_physical_result(
            self.breaker.as_ref(),
            event,
            execution.result(),
            outcome,
            ctx,
        );
        if let Some(classification) = classification.as_ref() {
            self.breaker.settle_classified_call(classification, ctx);
        } else {
            self.breaker.settle_unobserved_call(ctx);
        }
        if let Some(limiter) = &self.limiter {
            limiter.observe_resilience_attempt(ctx);
        }
        ctx.write_control_event(ChainEventFactory::circuit_breaker_attempt_settled(
            self.breaker.evidence_writer_id(),
            CircuitBreakerAttemptSettledEventParams {
                cursor: identity.cursor.clone(),
                attempt,
                health_classification: evidence_classification(classification.as_ref()),
                slow: self.breaker.is_slow_dependency_call(dependency_elapsed),
                dependency_elapsed_ms: duration_ms(dependency_elapsed),
                admission_wait_ms: duration_ms(admission_wait),
            },
            event.id,
        ));
        let terminal = recovery.finish_with_physical_result();
        debug_assert!(matches!(
            terminal,
            RecoveryTerminalDecision::ReturnPhysicalResult
        ));
        write_recovery_completed(&mut recovery, self.breaker.as_ref(), identity, event, ctx);
        execution.into_report(ctx.take_control_events())
    }
}

fn write_recovery_completed(
    recovery: &mut EffectRecoveryController,
    breaker: &CircuitBreakerMiddleware,
    identity: &EffectIdentity,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) {
    let completion = recovery.take_completion();
    ctx.write_control_event(ChainEventFactory::circuit_breaker_recovery_completed(
        breaker.evidence_writer_id(),
        CircuitBreakerRecoveryCompletedEventParams {
            cursor: identity.cursor.clone(),
            total_attempts: completion.total_attempts,
            backoff_elapsed_ms: duration_ms(completion.backoff_elapsed),
            recovery_elapsed_ms: duration_ms(completion.recovery_elapsed),
        },
        event.id,
    ));
}

fn repeatable_admission_rejected(
    recovery: &mut EffectRecoveryController,
    breaker: &CircuitBreakerMiddleware,
    identity: &EffectIdentity,
    cause: Box<crate::middleware::MiddlewareAbortCause>,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    let decision = recovery.finish_from_admission(cause);
    write_recovery_completed(recovery, breaker, identity, event, ctx);
    match decision {
        RecoveryTerminalDecision::BoundaryRejected(cause) => {
            report_from_admission(PolicyAdmission::Reject(cause), ctx)
        }
        RecoveryTerminalDecision::ReturnLastPhysicalError => {
            write_exhausted(
                breaker,
                identity,
                recovery.attempts(),
                CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
                event,
                ctx,
            );
            executed(Err(recovery.take_last_physical_error()), ctx)
        }
        RecoveryTerminalDecision::ReturnPhysicalResult => {
            unreachable!("admission rejection cannot return a new physical result")
        }
    }
}

fn repeatable_retry_exhausted(
    recovery: &mut EffectRecoveryController,
    breaker: &CircuitBreakerMiddleware,
    identity: &EffectIdentity,
    reason: CircuitBreakerRetryStopReason,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    write_exhausted(breaker, identity, recovery.attempts(), reason, event, ctx);
    let decision = recovery.finish_with_last_physical_error();
    write_recovery_completed(recovery, breaker, identity, event, ctx);
    match decision {
        RecoveryTerminalDecision::ReturnLastPhysicalError => {
            executed(Err(recovery.take_last_physical_error()), ctx)
        }
        RecoveryTerminalDecision::BoundaryRejected(_)
        | RecoveryTerminalDecision::ReturnPhysicalResult => {
            unreachable!("retry exhaustion must preserve the last physical error")
        }
    }
}

fn repeatable_physical_terminal(
    recovery: &mut EffectRecoveryController,
    breaker: &CircuitBreakerMiddleware,
    identity: &EffectIdentity,
    result: Result<Vec<ChainEvent>, EffectError>,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    let decision = recovery.finish_with_physical_result();
    write_recovery_completed(recovery, breaker, identity, event, ctx);
    match decision {
        RecoveryTerminalDecision::ReturnPhysicalResult => executed(result, ctx),
        RecoveryTerminalDecision::BoundaryRejected(_)
        | RecoveryTerminalDecision::ReturnLastPhysicalError => {
            unreachable!("a completed physical result must retain terminal precedence")
        }
    }
}

fn single_use_admission_terminal(
    decision: RecoveryTerminalDecision,
    ctx: &mut crate::middleware::MiddlewareContext,
    operation: SingleUseEffectOperation,
) -> SingleUseEffectBoundaryReport {
    match decision {
        RecoveryTerminalDecision::BoundaryRejected(cause) => {
            single_use_report_from_admission(PolicyAdmission::Reject(cause), ctx, operation)
        }
        RecoveryTerminalDecision::ReturnLastPhysicalError
        | RecoveryTerminalDecision::ReturnPhysicalResult => {
            unreachable!("single-use admission occurs before any physical call")
        }
    }
}

fn single_use_report_from_admission(
    admission: PolicyAdmission,
    ctx: &mut crate::middleware::MiddlewareContext,
    operation: SingleUseEffectOperation,
) -> SingleUseEffectBoundaryReport {
    match admission {
        PolicyAdmission::Admit => unreachable!("admitted calls do not return early"),
        PolicyAdmission::Reject(cause) => {
            let cause = *cause;
            operation.abort(
                EffectAbortReason {
                    cause: EffectFailureCause {
                        source: cause.source,
                        code: cause.code,
                    },
                    message: cause.message,
                    retry: cause.retry,
                },
                ctx.take_control_events(),
            )
        }
    }
}

fn report_from_admission(
    admission: PolicyAdmission,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    let outcome = match admission {
        PolicyAdmission::Admit => unreachable!("admitted calls do not return early"),
        PolicyAdmission::Reject(cause) => {
            let cause = *cause;
            EffectBoundaryOutcome::Aborted(EffectAbortReason {
                cause: EffectFailureCause {
                    source: cause.source,
                    code: cause.code,
                },
                message: cause.message,
                retry: cause.retry,
            })
        }
    };
    EffectBoundaryReport {
        outcome,
        control_events: ctx.take_control_events(),
    }
}

fn executed(
    result: Result<Vec<ChainEvent>, EffectError>,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    EffectBoundaryReport {
        outcome: EffectBoundaryOutcome::Executed(result),
        control_events: ctx.take_control_events(),
    }
}

fn prepare_retry_context(
    result: &Result<Vec<ChainEvent>, EffectError>,
    ctx: &mut crate::middleware::MiddlewareContext,
) {
    ctx.remove::<CircuitBreakerRetryAfterMs>();
    if let Err(EffectError::RateLimited { retry_after, .. }) = result {
        ctx.insert::<CircuitBreakerRetryAfterMs>(duration_ms(*retry_after));
    }
}

fn classify_physical_result(
    breaker: &CircuitBreakerMiddleware,
    event: &ChainEvent,
    result: &Result<Vec<ChainEvent>, EffectError>,
    physical_outcome: PhysicalCallOutcome,
    ctx: &crate::middleware::MiddlewareContext,
) -> Option<FailureClassification> {
    match (physical_outcome, result) {
        (PhysicalCallOutcome::Succeeded, Ok(outputs)) => {
            Some(breaker.classify_call(event, outputs, ctx).0)
        }
        // Dependency success followed by decomposition/materialisation failure
        // is healthy for the breaker and is never retried.
        (PhysicalCallOutcome::Succeeded, Err(EffectError::TransactionalCommitMissing { .. })) => {
            None
        }
        (PhysicalCallOutcome::Succeeded, Err(_)) => Some(FailureClassification::Success),
        (PhysicalCallOutcome::Failed, Err(error)) if error_has_health_observation(error) => {
            Some(breaker.classify_effect_error(event, error, ctx))
        }
        (PhysicalCallOutcome::Failed, Err(_)) => None,
        (PhysicalCallOutcome::Failed, Ok(outputs)) => {
            Some(breaker.classify_call(event, outputs, ctx).0)
        }
    }
}

fn error_has_health_observation(error: &EffectError) -> bool {
    match error {
        EffectError::Timeout(_)
        | EffectError::Transport(_)
        | EffectError::RateLimited { .. }
        | EffectError::Permanent(_)
        | EffectError::Validation(_)
        | EffectError::Domain(_)
        | EffectError::Execution(_) => true,
        EffectError::RecordedFailure { error_type, .. } => matches!(
            error_type.as_str(),
            "timeout"
                | "transport"
                | "rate_limited"
                | "permanent"
                | "validation"
                | "domain"
                | "execution"
        ),
        EffectError::Serialization(_)
        | EffectError::Journal(_)
        | EffectError::MissingRecordedEffect { .. }
        | EffectError::DuplicateRecordedEffect { .. }
        | EffectError::DescriptorMismatch { .. }
        | EffectError::BoundaryRejected { .. }
        | EffectError::EffectProvenanceMismatch(_)
        | EffectError::IncompleteOutcomeGroup { .. }
        | EffectError::MissingIdempotencyKey { .. }
        | EffectError::UndeclaredEffect { .. }
        | EffectError::UndeclaredOutput { .. }
        | EffectError::EmitUnsupported { .. }
        | EffectError::CompletedWithoutOutput { .. }
        | EffectError::CompletedEmptyWithOutput { .. }
        | EffectError::MissingEffectPort { .. }
        | EffectError::TransactionalCommitMissing { .. }
        | EffectError::ReplayArchive(_) => false,
    }
}

fn retryable_error(error: &EffectError) -> bool {
    matches!(
        error,
        EffectError::Timeout(_) | EffectError::Transport(_) | EffectError::RateLimited { .. }
    )
}

fn retry_delay(retry: &Retry, completed_attempts: u32, error: &EffectError) -> Duration {
    let generated = retry
        .policy
        .calculate_delay(completed_attempts.saturating_sub(1) as usize)
        .min(retry.limits.max_single_delay);
    let provider_floor = match error {
        EffectError::RateLimited { retry_after, .. } => *retry_after,
        _ => Duration::ZERO,
    };
    generated.max(provider_floor)
}

fn evidence_classification(
    classification: Option<&FailureClassification>,
) -> CircuitBreakerHealthClassification {
    match classification {
        Some(FailureClassification::Success) => CircuitBreakerHealthClassification::Success,
        Some(FailureClassification::TransientFailure) => {
            CircuitBreakerHealthClassification::TransientFailure
        }
        Some(FailureClassification::PermanentFailure) => {
            CircuitBreakerHealthClassification::PermanentFailure
        }
        Some(FailureClassification::RateLimited(_)) => {
            CircuitBreakerHealthClassification::RateLimited
        }
        Some(FailureClassification::Ignored) => CircuitBreakerHealthClassification::Ignored,
        None => CircuitBreakerHealthClassification::NoObservation,
    }
}

fn write_exhausted(
    breaker: &CircuitBreakerMiddleware,
    identity: &EffectIdentity,
    attempts: u32,
    reason: CircuitBreakerRetryStopReason,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) {
    ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_exhausted(
        breaker.evidence_writer_id(),
        identity.cursor.clone(),
        attempts,
        reason,
        event.id,
    ));
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn default_u64(key_path: &'static str, value: u64) -> DslConfigDefault {
    DslConfigDefault {
        key_path,
        value: ConfigValue::U64(value),
    }
}

fn default_f64(key_path: &'static str, value: f64) -> DslConfigDefault {
    DslConfigDefault {
        key_path,
        value: ConfigValue::F64(value),
    }
}

fn default_text(key_path: &'static str, value: &str) -> DslConfigDefault {
    DslConfigDefault {
        key_path,
        value: ConfigValue::Text(value.to_string()),
    }
}

fn u64_value(
    view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    key: &str,
) -> Option<u64> {
    view.get(key).and_then(|resolved| resolved.value.as_u64())
}

fn required_u64(view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>, key: &str) -> u64 {
    u64_value(view, key).unwrap_or(0)
}

fn f64_value(
    view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    key: &str,
) -> Option<f64> {
    view.get(key).and_then(|resolved| resolved.value.as_f64())
}

fn bool_value(
    view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    key: &str,
) -> Option<bool> {
    view.get(key).and_then(|resolved| resolved.value.as_bool())
}

fn text_value<'a>(
    view: &'a obzenflow_runtime::runtime_config::ExactConfigView<'a>,
    key: &str,
) -> Option<&'a str> {
    view.get(key).and_then(|resolved| resolved.value.as_text())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::MiddlewareSurfaceKind;

    fn valid_breaker() -> CircuitBreaker {
        CircuitBreaker::builder()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(0.6)
            .slow_call_duration(Duration::from_millis(250))
            .slow_call_rate_threshold(0.5)
            .open_for(Duration::from_secs(5))
            .build()
            .expect("test breaker should be valid")
    }

    #[test]
    fn checked_breaker_builder_rejects_ambiguous_and_incomplete_modes() {
        assert!(matches!(
            CircuitBreaker::builder()
                .consecutive_failures(3)
                .count_window(5)
                .minimum_calls(5)
                .failure_rate_threshold(0.5)
                .build(),
            Err(CircuitBreakerConfigError::MixedModes)
        ));
        assert!(matches!(
            CircuitBreaker::builder()
                .count_window(5)
                .minimum_calls(5)
                .slow_call_duration(Duration::from_millis(10))
                .build(),
            Err(CircuitBreakerConfigError::IncompleteSlowCallTrigger)
        ));
        assert!(matches!(
            CircuitBreaker::builder()
                .count_window(3)
                .minimum_calls(4)
                .failure_rate_threshold(0.5)
                .build(),
            Err(CircuitBreakerConfigError::MinimumCallsExceedsWindow { .. })
        ));
    }

    #[test]
    fn aggregate_validates_retry_without_panicking() {
        assert!(matches!(
            EffectResilience::with_breaker(valid_breaker())
                .retry(Retry::fixed(Duration::ZERO))
                .build(),
            Err(EffectResilienceConfigError::ZeroFixedDelay)
        ));
        assert!(matches!(
            EffectResilience::with_breaker(valid_breaker())
                .retry(Retry::fixed(Duration::from_millis(1)).max_attempts(0))
                .build(),
            Err(EffectResilienceConfigError::ZeroRetryAttempts)
        ));
        assert!(matches!(
            EffectResilience::with_breaker(valid_breaker())
                .retry(Retry::fixed(Duration::from_millis(1)).max_backoff(Duration::ZERO))
                .build(),
            Err(EffectResilienceConfigError::ZeroMaxBackoff)
        ));
        assert!(matches!(
            EffectResilience::with_breaker(valid_breaker())
                .retry(Retry::fixed(Duration::from_millis(1)).attempt_start_window(Duration::ZERO))
                .build(),
            Err(EffectResilienceConfigError::ZeroAttemptStartWindow)
        ));
    }

    #[test]
    fn health_classification_cannot_veto_or_promote_retry() {
        assert!(retryable_error(&EffectError::Timeout("slow".to_string())));
        assert!(retryable_error(&EffectError::Transport(
            "offline".to_string()
        )));
        assert!(!retryable_error(&EffectError::Permanent(
            "denied".to_string()
        )));
        assert!(!retryable_error(&EffectError::Domain(
            "declined".to_string()
        )));
    }

    #[test]
    fn provider_retry_after_is_the_only_delay_floor_and_is_not_capped() {
        let retry = Retry::fixed(Duration::from_millis(50)).max_backoff(Duration::from_millis(10));
        assert_eq!(
            retry_delay(&retry, 1, &EffectError::Timeout("slow".to_string())),
            Duration::from_millis(10)
        );
        assert_eq!(
            retry_delay(
                &retry,
                1,
                &EffectError::RateLimited {
                    message: "slow down".to_string(),
                    retry_after: Duration::from_millis(250),
                },
            ),
            Duration::from_millis(250)
        );
    }

    #[test]
    fn aggregate_contributes_one_namespaced_configuration_unit() {
        let factory = EffectResilience::with_breaker(valid_breaker())
            .retry(
                Retry::fixed(Duration::from_millis(10))
                    .max_attempts(3)
                    .max_backoff(Duration::from_secs(1))
                    .attempt_start_window(Duration::from_secs(2)),
            )
            .rate_limit_each_attempt(RateLimiter::per_second(20.0).unwrap())
            .build()
            .unwrap();

        assert_eq!(factory.label(), "effect_resilience");
        assert_eq!(
            factory.declaration().surfaces,
            vec![MiddlewareSurfaceKind::Effect]
        );
        let defaults = factory.dsl_config_defaults();
        assert!(defaults
            .iter()
            .all(|default| default.key_path.starts_with("effects.resilience.")));
        let keys = defaults
            .iter()
            .map(|default| default.key_path)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(keys.contains(RESILIENCE_BREAKER_MODE_KEY));
        assert!(keys.contains(RESILIENCE_RETRY_MAX_ATTEMPTS_KEY));
        assert!(keys.contains(RESILIENCE_RATE_LIMITER_EVENTS_PER_SECOND_KEY));
    }
}
