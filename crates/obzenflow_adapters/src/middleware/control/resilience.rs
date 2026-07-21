// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Fixed effect-bound resilience aggregate (FLOWIP-115n).

use super::circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfigError, CircuitBreakerFactory, CircuitBreakerFailureMode,
    CircuitBreakerMiddleware, FailureClassification, FailureWindow, Retry,
};
use super::rate_limiter::{RateLimiter, RateLimiterConfigError, RateLimiterMiddleware};
use crate::middleware::context_keys::{CircuitBreakerRetryAfterMs, EffectCallDurationNanos};
use crate::middleware::{
    validate_attachment_request, EffectPolicyAttachment, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError, MiddlewareHints,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, PolicyAdmission,
};
use obzenflow_core::event::payloads::observability_payload::{
    CircuitBreakerHealthClassification, CircuitBreakerRetryStopReason,
};
use obzenflow_core::event::{
    ChainEventFactory, CircuitBreakerAttemptSettledEventParams, EffectFailureCause,
};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
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
                if *failure_rate_threshold <= 1.0 {
                    defaults.push(default_f64(
                        RESILIENCE_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
                        *failure_rate_threshold as f64,
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
                        *threshold as f64,
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

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control_with_family(
            self.label(),
            self.override_key().family_label(),
            vec![MiddlewareSurfaceKind::Effect],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|err| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, err)
        })?;
        let MiddlewareSurface::Effect(effect) = request.surface else {
            return Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other("EffectResilience attaches only to declared effects"),
            ));
        };
        let view = context.config_view(request.protected_unit);
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

        let breaker = Arc::new(
            CircuitBreakerFactory::from_effect_breaker(&breaker).build_middleware_keyed(
                context.config,
                context.control_middleware.clone(),
                Some(effect.effect_type.clone()),
            )?,
        );
        let limiter = limiter
            .map(|config| {
                RateLimiterMiddleware::new_keyed(
                    context.config.stage_id,
                    config.validate().expect("resolved limiter was validated"),
                    context.control_middleware.clone(),
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

        Ok(MiddlewareSurfaceAttachment::Effect(
            EffectPolicyAttachment::effect_resilience(Arc::new(EffectResilienceMiddleware {
                breaker,
                retry,
                limiter,
            })),
        ))
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
        let started = Instant::now();
        let mut attempts = 0u32;

        loop {
            let precheck = self.breaker.effect_precheck(event, ctx).await;
            if !matches!(precheck, PolicyAdmission::Admit) {
                return report_from_admission(precheck, ctx);
            }

            let admission_started = Instant::now();
            let reservation = match &self.limiter {
                Some(limiter) => Some(limiter.reserve_permit_async(ctx).await),
                None => None,
            };

            let final_admission = self.breaker.effect_admit(event, ctx);
            if !matches!(final_admission, PolicyAdmission::Admit) {
                drop(reservation);
                return report_from_admission(final_admission, ctx);
            }
            let admission_wait = admission_started.elapsed();

            let prepared = operation.prepare();
            let receipt = prepared.receipt();
            let mut settlement_guard =
                AttemptSettlementGuard::new(self.breaker.clone(), receipt.clone());
            if let Some(reservation) = reservation {
                reservation.commit();
            }
            attempts = attempts.saturating_add(1);
            let result = prepared.execute().await;
            let observation = receipt.observation();
            settlement_guard.disarm();
            let (physical_outcome, dependency_elapsed) = match observation {
                PhysicalCallObservation::Completed {
                    outcome,
                    dependency_elapsed,
                } => (outcome, dependency_elapsed),
                PhysicalCallObservation::Prepared | PhysicalCallObservation::Started { .. } => {
                    self.breaker.settle_not_executed(ctx);
                    return EffectBoundaryReport {
                        outcome: EffectBoundaryOutcome::Executed(result),
                        control_events: ctx.take_control_events(),
                    };
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
                    attempt: attempts,
                    health_classification: evidence_classification(classification.as_ref()),
                    slow: self.breaker.is_slow_dependency_call(dependency_elapsed),
                    dependency_elapsed_ms: duration_ms(dependency_elapsed),
                    admission_wait_ms: duration_ms(admission_wait),
                },
                event.id,
            ));

            let Some(retry) = &self.retry else {
                return executed(result, ctx);
            };
            let Err(error) = &result else {
                if attempts > 1 {
                    ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_succeeded(
                        self.breaker.evidence_writer_id(),
                        identity.cursor.clone(),
                        attempts,
                        evidence_classification(classification.as_ref()),
                        event.id,
                    ));
                }
                return executed(result, ctx);
            };
            if physical_outcome != PhysicalCallOutcome::Failed || !retryable_error(error) {
                if attempts > 1 {
                    ctx.write_control_event(
                        ChainEventFactory::circuit_breaker_retry_stopped_non_retryable(
                            self.breaker.evidence_writer_id(),
                            identity.cursor.clone(),
                            attempts,
                            event.id,
                        ),
                    );
                }
                return executed(result, ctx);
            }
            if attempts >= retry.policy.max_attempts {
                write_exhausted(
                    self.breaker.as_ref(),
                    identity,
                    attempts,
                    CircuitBreakerRetryStopReason::AttemptLimit,
                    event,
                    ctx,
                );
                return executed(result, ctx);
            }
            if self.breaker.is_effect_probe(ctx) || !self.breaker.effect_retry_may_continue(ctx) {
                write_exhausted(
                    self.breaker.as_ref(),
                    identity,
                    attempts,
                    CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
                    event,
                    ctx,
                );
                return executed(result, ctx);
            }

            let delay = retry_delay(retry, attempts, error);
            if started.elapsed().saturating_add(delay) >= retry.limits.max_attempt_start_window {
                write_exhausted(
                    self.breaker.as_ref(),
                    identity,
                    attempts,
                    CircuitBreakerRetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
                return executed(result, ctx);
            }
            ctx.write_control_event(ChainEventFactory::circuit_breaker_retry_scheduled(
                self.breaker.evidence_writer_id(),
                identity.cursor.clone(),
                attempts.saturating_add(1),
                duration_ms(delay),
                event.id,
            ));
            tokio::time::sleep(delay).await;
            if started.elapsed() >= retry.limits.max_attempt_start_window {
                write_exhausted(
                    self.breaker.as_ref(),
                    identity,
                    attempts,
                    CircuitBreakerRetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
                return executed(result, ctx);
            }
            if !self.breaker.effect_retry_may_continue(ctx) {
                write_exhausted(
                    self.breaker.as_ref(),
                    identity,
                    attempts,
                    CircuitBreakerRetryStopReason::CircuitNoLongerClosed,
                    event,
                    ctx,
                );
                return executed(result, ctx);
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

        let precheck = self.breaker.effect_precheck(event, ctx).await;
        if !matches!(precheck, PolicyAdmission::Admit) {
            return single_use_report_from_admission(precheck, ctx, operation);
        }

        let admission_started = Instant::now();
        let reservation = match &self.limiter {
            Some(limiter) => Some(limiter.reserve_permit_async(ctx).await),
            None => None,
        };
        let final_admission = self.breaker.effect_admit(event, ctx);
        if !matches!(final_admission, PolicyAdmission::Admit) {
            drop(reservation);
            return single_use_report_from_admission(final_admission, ctx, operation);
        }
        let admission_wait = admission_started.elapsed();

        let prepared = operation.prepare();
        let receipt = prepared.receipt();
        let mut settlement_guard =
            AttemptSettlementGuard::new(self.breaker.clone(), receipt.clone());
        if let Some(reservation) = reservation {
            reservation.commit();
        }
        let execution = prepared.execute().await;
        settlement_guard.disarm();
        let PhysicalCallObservation::Completed {
            outcome,
            dependency_elapsed,
        } = receipt.observation()
        else {
            self.breaker.settle_not_executed(ctx);
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
                attempt: 1,
                health_classification: evidence_classification(classification.as_ref()),
                slow: self.breaker.is_slow_dependency_call(dependency_elapsed),
                dependency_elapsed_ms: duration_ms(dependency_elapsed),
                admission_wait_ms: duration_ms(admission_wait),
            },
            event.id,
        ));
        execution.into_report(ctx.take_control_events())
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
