// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Fixed effect-bound resilience aggregate (FLOWIP-115n).

use super::circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfigError, CircuitBreakerFactory, CircuitBreakerMiddleware,
    EffectAdmissionEpoch, EffectAdmissionFence, FailureClassification, Retry,
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
use obzenflow_core::event::payloads::execution_payload::{
    CircuitBreakerHealthClassification, RetryStopReason,
};
use obzenflow_core::event::{
    ChainEventFactory, CircuitBreakerAttemptSettledEventParams, EffectFailureCause,
    RecoveryCompletedEventParams,
};
#[cfg(feature = "test-support")]
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_core::{ChainEvent, MiddlewareExecutionScope};
#[cfg(test)]
use obzenflow_runtime::effects::EffectCursor;
use obzenflow_runtime::effects::{
    EffectAbortReason, EffectBoundaryOutcome, EffectBoundaryReport, EffectError, EffectIdentity,
    PhysicalCallObservation, PhysicalCallOutcome, PhysicalCallReceipt, RepeatableEffectOperation,
    SingleUseEffectBoundaryReport, SingleUseEffectOperation,
};
use obzenflow_runtime::runtime_config::{
    ConfigValue, DslConfigDefault, CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
    CIRCUIT_BREAKER_COUNT_WINDOW_KEY, CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
    CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, CIRCUIT_BREAKER_MODE_KEY, CIRCUIT_BREAKER_OPEN_FOR_MS_KEY,
    CIRCUIT_BREAKER_PROBES_KEY, CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
    CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY, CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
    RATE_LIMITER_BURST_CAPACITY_KEY, RATE_LIMITER_COST_PER_ATTEMPT_KEY,
    RATE_LIMITER_EVENTS_PER_SECOND_KEY, RETRY_ATTEMPT_START_WINDOW_MS_KEY,
    RETRY_FIXED_DELAY_MS_KEY, RETRY_KIND_KEY, RETRY_MAX_ATTEMPTS_KEY, RETRY_MAX_BACKOFF_MS_KEY,
};
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::time::Instant;

pub(in crate::middleware::control) struct EffectResilienceFamily;

#[derive(Debug, Error)]
pub enum ControlConfigurationError {
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
    #[error("retry kind must be 'fixed' or 'exponential', got '{kind}'")]
    UnknownRetryKind { kind: String },
}

pub(in crate::middleware::control) fn validate_retry(
    retry: &Retry,
) -> Result<(), ControlConfigurationError> {
    if retry.policy.max_attempts == 0 {
        return Err(ControlConfigurationError::ZeroRetryAttempts);
    }
    if retry.limits.max_single_delay.is_zero() {
        return Err(ControlConfigurationError::ZeroMaxBackoff);
    }
    if retry.limits.max_attempt_start_window.is_zero() {
        return Err(ControlConfigurationError::ZeroAttemptStartWindow);
    }
    if matches!(retry.policy.backoff, BackoffStrategy::Fixed { delay } if delay.is_zero()) {
        return Err(ControlConfigurationError::ZeroFixedDelay);
    }
    Ok(())
}

#[derive(Clone)]
pub(in crate::middleware::control) struct EffectPlanFactory {
    pub(in crate::middleware::control) breaker: Option<CircuitBreaker>,
    pub(in crate::middleware::control) retry: Option<Retry>,
    pub(in crate::middleware::control) rate_limiter: Option<RateLimiter>,
    pub(in crate::middleware::control) sites: Vec<(
        super::composition::BuiltinControlFamily,
        crate::middleware::MiddlewareAttachmentSite,
    )>,
    #[cfg(feature = "test-support")]
    pub(in crate::middleware::control) reject_affine_recovery_for_test: bool,
}

pub(in crate::middleware::control) fn breaker_config_keys() -> Vec<&'static str> {
    vec![
        CIRCUIT_BREAKER_MODE_KEY,
        CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
        CIRCUIT_BREAKER_COUNT_WINDOW_KEY,
        CIRCUIT_BREAKER_MINIMUM_CALLS_KEY,
        CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
        CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY,
        CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
        CIRCUIT_BREAKER_OPEN_FOR_MS_KEY,
        CIRCUIT_BREAKER_PROBES_KEY,
        CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
    ]
}

pub(in crate::middleware::control) fn breaker_defaults(
    value: &CircuitBreaker,
) -> Vec<DslConfigDefault> {
    let mut defaults = vec![
        default_text(
            CIRCUIT_BREAKER_MODE_KEY,
            if value.consecutive_failures.is_some() {
                "consecutive"
            } else {
                "rate_based"
            },
        ),
        default_u64(CIRCUIT_BREAKER_OPEN_FOR_MS_KEY, duration_ms(value.open_for)),
        default_u64(CIRCUIT_BREAKER_PROBES_KEY, value.probes as u64),
        DslConfigDefault {
            key_path: CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY,
            value: ConfigValue::Bool(value.rate_limited_counts_as_failure),
        },
    ];
    for (key, v) in [
        (
            CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
            value.consecutive_failures,
        ),
        (CIRCUIT_BREAKER_COUNT_WINDOW_KEY, value.count_window),
        (CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, value.minimum_calls),
    ] {
        if let Some(v) = v {
            defaults.push(default_u64(key, v as u64));
        }
    }
    for (key, v) in [
        (
            CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY,
            value.failure_rate_threshold,
        ),
        (
            CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY,
            value.slow_call_rate_threshold,
        ),
    ] {
        if let Some(v) = v {
            defaults.push(default_f64(key, v));
        }
    }
    if let Some(v) = value.slow_call_duration {
        defaults.push(default_u64(
            CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY,
            duration_ms(v),
        ));
    }
    defaults
}

pub(in crate::middleware::control) fn resolve_breaker(
    value: &CircuitBreaker,
    view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
) -> Result<super::circuit_breaker::ValidatedCircuitBreaker, CircuitBreakerConfigError> {
    value.validate()?;
    let mut resolved = value.clone();
    let mode = text_value(view, CIRCUIT_BREAKER_MODE_KEY).unwrap_or(
        if value.consecutive_failures.is_some() {
            "consecutive"
        } else {
            "rate_based"
        },
    );
    let checked_count =
        |key, fallback| -> Result<Option<u32>, CircuitBreakerConfigError> {
            match u64_value(view, key) {
                Some(v) => u32::try_from(v).map(Some).map_err(|_| {
                    CircuitBreakerConfigError::InvalidCount {
                        field: key,
                        value: v,
                    }
                }),
                None => Ok(fallback),
            }
        };
    match mode {
        "consecutive" => {
            resolved.consecutive_failures = checked_count(
                CIRCUIT_BREAKER_CONSECUTIVE_FAILURES_KEY,
                value.consecutive_failures,
            )?;
            resolved.count_window = None;
            resolved.minimum_calls = None;
            resolved.failure_rate_threshold = None;
            resolved.slow_call_duration = None;
            resolved.slow_call_rate_threshold = None;
        }
        "rate_based" => {
            resolved.consecutive_failures = None;
            resolved.count_window =
                checked_count(CIRCUIT_BREAKER_COUNT_WINDOW_KEY, value.count_window)?;
            resolved.minimum_calls =
                checked_count(CIRCUIT_BREAKER_MINIMUM_CALLS_KEY, value.minimum_calls)?;
            resolved.failure_rate_threshold =
                f64_value(view, CIRCUIT_BREAKER_FAILURE_RATE_THRESHOLD_KEY)
                    .or(value.failure_rate_threshold);
            resolved.slow_call_rate_threshold =
                f64_value(view, CIRCUIT_BREAKER_SLOW_CALL_RATE_THRESHOLD_KEY)
                    .or(value.slow_call_rate_threshold);
            resolved.slow_call_duration =
                u64_value(view, CIRCUIT_BREAKER_SLOW_CALL_DURATION_MS_KEY)
                    .map(Duration::from_millis)
                    .or(value.slow_call_duration);
        }
        _ => {
            return Err(CircuitBreakerConfigError::UnknownMode {
                value: mode.to_string(),
            })
        }
    }
    resolved.open_for = u64_value(view, CIRCUIT_BREAKER_OPEN_FOR_MS_KEY)
        .map(Duration::from_millis)
        .unwrap_or(value.open_for);
    resolved.probes =
        checked_count(CIRCUIT_BREAKER_PROBES_KEY, Some(value.probes))?.unwrap_or(value.probes);
    resolved.rate_limited_counts_as_failure =
        bool_value(view, CIRCUIT_BREAKER_RATE_LIMITED_COUNTS_AS_FAILURE_KEY)
            .unwrap_or(value.rate_limited_counts_as_failure);
    resolved.validate()
}

impl EffectPlanFactory {
    pub(in crate::middleware::control) fn empty() -> Self {
        Self {
            breaker: None,
            retry: None,
            rate_limiter: None,
            sites: Vec::new(),
            #[cfg(feature = "test-support")]
            reject_affine_recovery_for_test: false,
        }
    }
    #[cfg(test)]
    pub(in crate::middleware::control) fn with_breaker(breaker: CircuitBreaker) -> Self {
        let mut plan = Self::empty();
        plan.breaker = Some(breaker);
        plan
    }
    #[cfg(test)]
    pub(in crate::middleware::control) fn retry(mut self, retry: Retry) -> Self {
        self.retry = Some(retry);
        self
    }
    #[cfg(test)]
    pub(in crate::middleware::control) fn rate_limit_each_attempt(
        mut self,
        limiter: RateLimiter,
    ) -> Self {
        self.rate_limiter = Some(limiter);
        self
    }
    #[cfg(test)]
    pub(in crate::middleware::control) fn build(
        self,
    ) -> Result<Box<dyn MiddlewareFactory>, ControlConfigurationError> {
        if let Some(value) = &self.breaker {
            value.validate()?;
        }
        if let Some(value) = &self.retry {
            validate_retry(value)?;
        }
        if let Some(value) = &self.rate_limiter {
            value.validate()?;
        }
        Ok(Box::new(self))
    }

    fn retry_defaults(&self) -> Vec<DslConfigDefault> {
        let Some(retry) = &self.retry else {
            return Vec::new();
        };
        let mut defaults = match retry.policy.backoff {
            BackoffStrategy::Fixed { delay } => vec![
                default_text(RETRY_KIND_KEY, "fixed"),
                default_u64(RETRY_FIXED_DELAY_MS_KEY, duration_ms(delay)),
            ],
            BackoffStrategy::Exponential { .. } => {
                vec![default_text(RETRY_KIND_KEY, "exponential")]
            }
        };
        defaults.extend([
            default_u64(RETRY_MAX_ATTEMPTS_KEY, retry.policy.max_attempts as u64),
            default_u64(
                RETRY_MAX_BACKOFF_MS_KEY,
                duration_ms(retry.limits.max_single_delay),
            ),
            default_u64(
                RETRY_ATTEMPT_START_WINDOW_MS_KEY,
                duration_ms(retry.limits.max_attempt_start_window),
            ),
        ]);
        defaults
    }
    fn resolved_retry(
        &self,
        view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    ) -> Result<Option<Retry>, ControlConfigurationError> {
        let Some(authored) = &self.retry else {
            return Ok(None);
        };
        validate_retry(authored)?;
        let mut value = match text_value(view, RETRY_KIND_KEY).unwrap_or("exponential") {
            "fixed" => super::circuit_breaker::retry().fixed_delay(Duration::from_millis(
                required_u64(view, RETRY_FIXED_DELAY_MS_KEY),
            )),
            "exponential" => super::circuit_breaker::retry(),
            kind => {
                return Err(ControlConfigurationError::UnknownRetryKind {
                    kind: kind.to_string(),
                })
            }
        };
        value = value.max_attempts(
            u32::try_from(
                u64_value(view, RETRY_MAX_ATTEMPTS_KEY)
                    .unwrap_or(authored.policy.max_attempts as u64),
            )
            .map_err(|_| ControlConfigurationError::ZeroRetryAttempts)?,
        );
        value = value.max_backoff(
            u64_value(view, RETRY_MAX_BACKOFF_MS_KEY)
                .map(Duration::from_millis)
                .unwrap_or(authored.limits.max_single_delay),
        );
        value = value.attempt_start_window(
            u64_value(view, RETRY_ATTEMPT_START_WINDOW_MS_KEY)
                .map(Duration::from_millis)
                .unwrap_or(authored.limits.max_attempt_start_window),
        );
        validate_retry(&value)?;
        Ok(Some(value))
    }
    fn resolved_limiter(
        &self,
        view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    ) -> Result<Option<RateLimiter>, RateLimiterConfigError> {
        let Some(authored) = &self.rate_limiter else {
            return Ok(None);
        };
        authored.validate()?;
        let mut value = super::rate_limiter::rate_limit(
            f64_value(view, RATE_LIMITER_EVENTS_PER_SECOND_KEY)
                .unwrap_or(authored.events_per_second),
        )
        .cost(
            f64_value(view, RATE_LIMITER_COST_PER_ATTEMPT_KEY).unwrap_or(authored.cost_per_attempt),
        );
        value.burst_capacity =
            f64_value(view, RATE_LIMITER_BURST_CAPACITY_KEY).or(authored.burst_capacity);
        value.validate()?;
        Ok(Some(value))
    }
    fn validate_plan(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<()> {
        let MiddlewareSurface::Effect(effect) = request.surface else {
            return Err(MiddlewareFactoryError::invalid_configuration(
                self.label(),
                &context.config.name,
                std::io::Error::other("effect controls require a declared effect"),
            ));
        };
        if self.retry.is_some()
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
                    "retry is not eligible for effect '{}' with safety {:?}",
                    effect.effect_type.as_str(),
                    effect.safety
                )),
            ));
        }
        let view = context.config_view();
        if let Some(breaker) = &self.breaker {
            resolve_breaker(breaker, &view).map_err(|e| {
                MiddlewareFactoryError::invalid_configuration(
                    "circuit_breaker",
                    &context.config.name,
                    e,
                )
            })?;
        }
        self.resolved_retry(&view).map_err(|e| {
            MiddlewareFactoryError::invalid_configuration("retry", &context.config.name, e)
        })?;
        self.resolved_limiter(&view).map_err(|e| {
            MiddlewareFactoryError::invalid_configuration("rate_limiter", &context.config.name, e)
        })?;
        Ok(())
    }
}

impl MiddlewareFactory for EffectPlanFactory {
    fn builtin_control(&self) -> Option<super::composition::BuiltinControlContribution> {
        Some(super::composition::BuiltinControlContribution::from_plan(
            self,
        ))
    }
    fn label(&self) -> &'static str {
        "effect_resilience"
    }
    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<EffectResilienceFamily>("effect_resilience")
    }
    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::effect_resilience(self.label(), self.override_key().family_label())
    }
    fn dsl_config_defaults(&self) -> Vec<DslConfigDefault> {
        let mut defaults = self
            .breaker
            .as_ref()
            .map(breaker_defaults)
            .unwrap_or_default();
        if let Some(limiter) = &self.rate_limiter {
            defaults.extend(limiter.dsl_config_defaults());
        }
        defaults.extend(self.retry_defaults());
        defaults
    }
    fn consumed_config_keys(&self) -> Vec<&'static str> {
        let mut keys = if self.breaker.is_some() {
            breaker_config_keys()
        } else {
            Vec::new()
        };
        if let Some(limiter) = &self.rate_limiter {
            keys.extend(limiter.consumed_config_keys());
        }
        if self.retry.is_some() {
            keys.extend([
                RETRY_KIND_KEY,
                RETRY_FIXED_DELAY_MS_KEY,
                RETRY_MAX_ATTEMPTS_KEY,
                RETRY_MAX_BACKOFF_MS_KEY,
                RETRY_ATTEMPT_START_WINDOW_MS_KEY,
            ]);
        }
        keys
    }
    fn validate_configuration(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        config: &obzenflow_runtime::pipeline::config::StageConfig,
        stage_type: obzenflow_core::event::context::StageType,
    ) -> crate::middleware::MiddlewareFactoryResult<()> {
        let declaration = self.declaration();
        self.validate_plan(
            request,
            &MiddlewareMaterializationContext::new(config, stage_type, &declaration, &request),
        )
    }
    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        self.validate_plan(request, context)?;
        let declaration = self.declaration();
        validate_attachment_request(&declaration, &request).map_err(|e| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, e)
        })?;
        context
            .authorize_materialization(
                MaterializationClaim::EffectResilience,
                &declaration,
                &request,
            )
            .map_err(|e| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    e,
                )
            })?;
        let MiddlewareSurface::Effect(effect) = request.surface else {
            unreachable!("validated effect plan")
        };
        let view = context.config_view();
        let breaker = self
            .breaker
            .as_ref()
            .map(|value| resolve_breaker(value, &view))
            .transpose()
            .map_err(|e| {
                MiddlewareFactoryError::invalid_configuration(
                    "circuit_breaker",
                    &context.config.name,
                    e,
                )
            })?;
        let retry = self.resolved_retry(&view).map_err(|e| {
            MiddlewareFactoryError::invalid_configuration("retry", &context.config.name, e)
        })?;
        let limiter = self.resolved_limiter(&view).map_err(|e| {
            MiddlewareFactoryError::invalid_configuration("rate_limiter", &context.config.name, e)
        })?;
        let breaker = breaker
            .as_ref()
            .map(|value| {
                CircuitBreakerFactory::from_effect_breaker(value)
                    .build_middleware_keyed(
                        context.config,
                        context,
                        MaterializationClaim::EffectResilience,
                        Some(effect.effect_type.clone()),
                    )
                    .map(|(state, _)| Arc::new(state))
            })
            .transpose()?;
        let limiter = limiter
            .map(|value| {
                let config = value.validate().map_err(|e| {
                    MiddlewareFactoryError::invalid_configuration(
                        "rate_limiter",
                        &context.config.name,
                        e,
                    )
                })?;
                RateLimiterMiddleware::new_keyed(
                    context.config.stage_id,
                    config,
                    context,
                    MaterializationClaim::EffectResilience,
                    Some(effect.effect_type.clone()),
                )
                .map(Arc::new)
                .map_err(|e| {
                    MiddlewareFactoryError::invalid_configuration(
                        "rate_limiter",
                        &context.config.name,
                        std::io::Error::other(e),
                    )
                })
            })
            .transpose()?;
        MiddlewareSurfaceAttachment::claimed(
            MiddlewareSurfaceAttachmentKind::Effect(EffectPolicyAttachment::effect_resilience(
                Arc::new(EffectResilienceMiddleware {
                    writer_id: obzenflow_core::WriterId::from(context.config.stage_id),
                    breaker,
                    retry,
                    limiter,
                    #[cfg(feature = "test-support")]
                    reject_affine_recovery_for_test: self.reject_affine_recovery_for_test,
                    #[cfg(test)]
                    final_admission_test_gate: std::sync::Mutex::new(None),
                }),
            )),
            MaterializationClaim::EffectResilience,
            context,
        )
        .map_err(|e| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, e)
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
        let family = |defaults: Vec<DslConfigDefault>| {
            serde_json::Value::Object(
                defaults
                    .into_iter()
                    .map(|value| (value.key_path.to_string(), value.value.to_json()))
                    .collect(),
            )
        };
        let mut snapshot = serde_json::Map::new();
        snapshot.insert("kind".into(), "effect_resilience".into());
        if let Some(breaker) = &self.breaker {
            snapshot.insert("breaker".into(), family(breaker_defaults(breaker)));
        }
        if let Some(limiter) = &self.rate_limiter {
            snapshot.insert("rate_limiter".into(), family(limiter.dsl_config_defaults()));
        }
        if self.retry.is_some() {
            snapshot.insert("retry".into(), family(self.retry_defaults()));
        }
        Some(serde_json::Value::Object(snapshot))
    }
}

pub(in crate::middleware::control) struct EffectResilienceMiddleware {
    writer_id: obzenflow_core::WriterId,
    breaker: Option<Arc<CircuitBreakerMiddleware>>,
    retry: Option<Retry>,
    limiter: Option<Arc<RateLimiterMiddleware>>,
    #[cfg(feature = "test-support")]
    reject_affine_recovery_for_test: bool,
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
/// In particular, the initial breaker epoch is write-once and the limiter
/// reservation lives here until it is transferred to the physical-call
/// settlement guard or cancelled by a terminal path.
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

    fn with_attempt_base(highest_prior_attempt: u32) -> Self {
        let mut controller = Self::new();
        controller.attempt_ordinal = highest_prior_attempt;
        controller.retry_count = highest_prior_attempt.saturating_sub(1);
        controller
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
        let attempt = self.begin_physical_attempt_state(attempt_start_window);
        if let Some(reservation) = self.current_reservation.take() {
            reservation.commit();
        }
        attempt
    }

    fn begin_affine_attempt(
        &mut self,
        attempt_start_window: Option<Duration>,
    ) -> (u32, Option<RateLimitReservation>) {
        let attempt = self.begin_physical_attempt_state(attempt_start_window);
        (attempt, self.current_reservation.take())
    }

    fn begin_physical_attempt_state(&mut self, attempt_start_window: Option<Duration>) -> u32 {
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

    fn finish_affine_admission(
        &mut self,
        cause: Box<crate::middleware::MiddlewareAbortCause>,
    ) -> RecoveryTerminalDecision {
        self.cancel_reservation();
        self.terminal_decision = Some(RecoveryTerminalDecision::BoundaryRejected(cause));
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

/// The affine runtime publishes its durable Start inside the prepared future,
/// before it marks the protected dependency as started. Keep the limiter
/// reservation refundable across that publication cut, then account it
/// exactly once as soon as the runtime-owned receipt proves the physical call
/// began. Drop performs the same decision for cancellation.
struct AffineLimiterSettlementGuard {
    receipt: PhysicalCallReceipt,
    reservation: Option<RateLimitReservation>,
}

impl AffineLimiterSettlementGuard {
    fn new(receipt: PhysicalCallReceipt, reservation: Option<RateLimitReservation>) -> Self {
        Self {
            receipt,
            reservation,
        }
    }

    fn settle(&mut self) {
        let Some(reservation) = self.reservation.take() else {
            return;
        };
        match self.receipt.observation() {
            PhysicalCallObservation::Started { .. } | PhysicalCallObservation::Completed { .. } => {
                reservation.commit()
            }
            PhysicalCallObservation::Prepared => drop(reservation),
        }
    }
}

impl Drop for AffineLimiterSettlementGuard {
    fn drop(&mut self) {
        self.settle();
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
        self.breaker
            .as_ref()
            .expect("test requires a breaker")
            .expire_effect_cooldown_for_test();
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
                    self.writer_id,
                    identity,
                    RetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }

            if let Some(breaker) = &self.breaker {
                let reservation_epoch = match breaker.effect_precheck(ctx, recovery.initial_epoch())
                {
                    Ok(epoch) => epoch,
                    Err(cause) => {
                        return repeatable_admission_rejected(
                            &mut recovery,
                            self.writer_id,
                            identity,
                            cause,
                            event,
                            ctx,
                        )
                    }
                };
                recovery.observe_precheck(reservation_epoch);
            }

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
                    self.writer_id,
                    identity,
                    RetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }

            if let Some(PolicyAdmission::Reject(cause)) = self
                .breaker
                .as_ref()
                .map(|breaker| breaker.effect_admit(ctx, recovery.admission_fence()))
            {
                return repeatable_admission_rejected(
                    &mut recovery,
                    self.writer_id,
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
                if let Some(breaker) = &self.breaker {
                    breaker.settle_not_executed(ctx);
                }
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    RetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }
            let admission_wait = admission_started.elapsed();

            let prepared = operation.prepare();
            let receipt = prepared.receipt();
            let mut settlement_guard = self
                .breaker
                .as_ref()
                .map(|breaker| AttemptSettlementGuard::new(breaker.clone(), receipt.clone()));
            let attempt = recovery.begin_physical_attempt(
                self.retry
                    .as_ref()
                    .map(|retry| retry.limits.max_attempt_start_window),
            );
            let result = prepared.execute().await;
            let observation = receipt.observation();
            if let Some(guard) = &mut settlement_guard {
                guard.disarm();
            }
            recovery.finish_physical_attempt(&result);
            let (physical_outcome, dependency_elapsed) = match observation {
                PhysicalCallObservation::Completed {
                    outcome,
                    dependency_elapsed,
                } => (outcome, dependency_elapsed),
                PhysicalCallObservation::Prepared | PhysicalCallObservation::Started { .. } => {
                    if let Some(breaker) = &self.breaker {
                        breaker.settle_not_executed(ctx);
                    }
                    return repeatable_physical_terminal(
                        &mut recovery,
                        self.writer_id,
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
            if self.breaker.is_some() {
                prepare_retry_context(&result, ctx);
            }
            let classification = self.breaker.as_ref().and_then(|breaker| {
                classify_physical_result(breaker, event, &result, physical_outcome, ctx)
            });
            if let Some(breaker) = &self.breaker {
                if let Some(classification) = classification.as_ref() {
                    breaker.settle_classified_call(classification, ctx);
                } else {
                    breaker.settle_unobserved_call(ctx);
                }
                ctx.write_control_event(ChainEventFactory::circuit_breaker_attempt_settled(
                    self.writer_id,
                    CircuitBreakerAttemptSettledEventParams {
                        cursor: identity.cursor.clone(),
                        attempt,
                        health_classification: evidence_classification(classification.as_ref()),
                        slow: breaker.is_slow_dependency_call(dependency_elapsed),
                        dependency_elapsed_ms: duration_ms(dependency_elapsed),
                        admission_wait_ms: duration_ms(admission_wait),
                    },
                    event.id,
                ));
            } else {
                ctx.write_control_event(ChainEventFactory::recovery_attempt_completed(
                    self.writer_id,
                    identity.cursor.clone(),
                    attempt,
                    duration_ms(dependency_elapsed),
                    duration_ms(admission_wait),
                    event.id,
                ));
            }

            if let Some(limiter) = &self.limiter {
                limiter.observe_resilience_attempt(ctx);
            }

            let Some(retry) = &self.retry else {
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    result,
                    event,
                    ctx,
                );
            };
            let Err(error) = &result else {
                if recovery.attempts() > 1 {
                    ctx.write_control_event(ChainEventFactory::retry_succeeded(
                        self.writer_id,
                        identity.cursor.clone(),
                        recovery.attempts(),
                        event.id,
                    ));
                }
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    result,
                    event,
                    ctx,
                );
            };
            if physical_outcome != PhysicalCallOutcome::Failed || !retryable_error(error) {
                if recovery.attempts() > 1 {
                    ctx.write_control_event(ChainEventFactory::retry_stopped_non_retryable(
                        self.writer_id,
                        identity.cursor.clone(),
                        recovery.attempts(),
                        event.id,
                    ));
                }
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    result,
                    event,
                    ctx,
                );
            }
            if recovery.attempts() >= retry.policy.max_attempts {
                write_exhausted(
                    self.writer_id,
                    identity,
                    recovery.attempts(),
                    RetryStopReason::AttemptLimit,
                    event,
                    ctx,
                );
                return repeatable_physical_terminal(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    result,
                    event,
                    ctx,
                );
            }
            if self.breaker.as_ref().is_some_and(|breaker| {
                breaker.is_effect_probe(ctx)
                    || !breaker.effect_recovery_epoch_is_current(
                        recovery
                            .initial_epoch()
                            .expect("a breaker attempt captures its epoch"),
                    )
            }) {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    RetryStopReason::CircuitNoLongerClosed,
                    event,
                    ctx,
                );
            }

            let delay = retry_delay(retry, recovery.attempts(), error);
            if !recovery.backoff_fits_attempt_window(delay) {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    RetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }
            ctx.write_control_event(ChainEventFactory::retry_scheduled(
                self.writer_id,
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
                    self.writer_id,
                    identity,
                    RetryStopReason::AttemptStartWindow,
                    event,
                    ctx,
                );
            }
            if self.breaker.as_ref().is_some_and(|breaker| {
                !breaker.effect_recovery_epoch_is_current(
                    recovery
                        .initial_epoch()
                        .expect("a breaker attempt captures its epoch"),
                )
            }) {
                return repeatable_retry_exhausted(
                    &mut recovery,
                    self.writer_id,
                    identity,
                    RetryStopReason::CircuitNoLongerClosed,
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

        if let Some(breaker) = &self.breaker {
            let reservation_epoch = match breaker.effect_precheck(ctx, None) {
                Ok(epoch) => epoch,
                Err(cause) => {
                    let decision = recovery.finish_from_admission(cause);
                    write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
                    return single_use_admission_terminal(decision, ctx, operation);
                }
            };
            recovery.observe_precheck(reservation_epoch);
        }

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

        if let Some(PolicyAdmission::Reject(cause)) = self
            .breaker
            .as_ref()
            .map(|breaker| breaker.effect_admit(ctx, recovery.admission_fence()))
        {
            let decision = recovery.finish_from_admission(cause);
            write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
            return single_use_admission_terminal(decision, ctx, operation);
        }
        let admission_wait = admission_started.elapsed();

        let prepared = operation.prepare();
        let receipt = prepared.receipt();
        let mut settlement_guard = self
            .breaker
            .as_ref()
            .map(|breaker| AttemptSettlementGuard::new(breaker.clone(), receipt.clone()));
        let attempt = recovery.begin_physical_attempt(None);
        let execution = prepared.execute().await;
        if let Some(guard) = &mut settlement_guard {
            guard.disarm();
        }
        recovery.finish_physical_attempt(execution.result());
        let PhysicalCallObservation::Completed {
            outcome,
            dependency_elapsed,
        } = receipt.observation()
        else {
            if let Some(breaker) = &self.breaker {
                breaker.settle_not_executed(ctx);
            }
            let terminal = recovery.finish_with_physical_result();
            debug_assert!(matches!(
                terminal,
                RecoveryTerminalDecision::ReturnPhysicalResult
            ));
            write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
            return execution.into_report(ctx.take_control_events());
        };

        ctx.insert::<EffectCallDurationNanos>(
            dependency_elapsed.as_nanos().min(u64::MAX as u128) as u64
        );
        if self.breaker.is_some() {
            prepare_retry_context(execution.result(), ctx);
        }
        let classification = self.breaker.as_ref().and_then(|breaker| {
            classify_physical_result(breaker, event, execution.result(), outcome, ctx)
        });
        if let Some(breaker) = &self.breaker {
            if let Some(classification) = classification.as_ref() {
                breaker.settle_classified_call(classification, ctx);
            } else {
                breaker.settle_unobserved_call(ctx);
            }
            ctx.write_control_event(ChainEventFactory::circuit_breaker_attempt_settled(
                self.writer_id,
                CircuitBreakerAttemptSettledEventParams {
                    cursor: identity.cursor.clone(),
                    attempt,
                    health_classification: evidence_classification(classification.as_ref()),
                    slow: breaker.is_slow_dependency_call(dependency_elapsed),
                    dependency_elapsed_ms: duration_ms(dependency_elapsed),
                    admission_wait_ms: duration_ms(admission_wait),
                },
                event.id,
            ));
        } else {
            ctx.write_control_event(ChainEventFactory::recovery_attempt_completed(
                self.writer_id,
                identity.cursor.clone(),
                attempt,
                duration_ms(dependency_elapsed),
                duration_ms(admission_wait),
                event.id,
            ));
        }

        if let Some(limiter) = &self.limiter {
            limiter.observe_resilience_attempt(ctx);
        }

        let terminal = recovery.finish_with_physical_result();
        debug_assert!(matches!(
            terminal,
            RecoveryTerminalDecision::ReturnPhysicalResult
        ));
        write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
        execution.into_report(ctx.take_control_events())
    }

    pub(in crate::middleware::control) async fn execute_affine(
        &self,
        identity: &EffectIdentity,
        event: &ChainEvent,
        ctx: &mut crate::middleware::MiddlewareContext,
        operation: obzenflow_runtime::effects::AffineEffectOperation,
    ) -> obzenflow_runtime::effects::AffineEffectBoundaryReport {
        debug_assert_eq!(
            identity.safety,
            obzenflow_runtime::effects::EffectSafety::NonIdempotentAtLeastOnce
        );
        debug_assert!(self.retry.is_none());
        let highest_prior_attempt = operation.highest_prior_attempt();
        let mut recovery = EffectRecoveryController::with_attempt_base(highest_prior_attempt);

        #[cfg(feature = "test-support")]
        if self.reject_affine_recovery_for_test && highest_prior_attempt > 0 {
            let decision = recovery.finish_affine_admission(Box::new(
                crate::middleware::MiddlewareAbortCause {
                    source: EffectFailureSource::new("circuit_breaker"),
                    code: EffectFailureCode::new("circuit_open"),
                    message: "test fixture rejected affine recovery".to_string(),
                    retry: RetryDisposition::Retryable,
                    event: None,
                },
            ));
            write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
            return affine_admission_terminal(decision, ctx, operation);
        }

        if let Some(breaker) = &self.breaker {
            let reservation_epoch = match breaker.effect_precheck(ctx, None) {
                Ok(epoch) => epoch,
                Err(cause) => {
                    let decision = recovery.finish_affine_admission(cause);
                    write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
                    return affine_admission_terminal(decision, ctx, operation);
                }
            };
            recovery.observe_precheck(reservation_epoch);
        }

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

        if let Some(PolicyAdmission::Reject(cause)) = self
            .breaker
            .as_ref()
            .map(|breaker| breaker.effect_admit(ctx, recovery.admission_fence()))
        {
            let decision = recovery.finish_affine_admission(cause);
            write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
            return affine_admission_terminal(decision, ctx, operation);
        }
        let admission_wait = admission_started.elapsed();

        let prepared = operation.prepare();
        let receipt = prepared.receipt();
        let mut settlement_guard = self
            .breaker
            .as_ref()
            .map(|breaker| AttemptSettlementGuard::new(breaker.clone(), receipt.clone()));
        let (attempt, reservation) = recovery.begin_affine_attempt(None);
        let mut limiter_settlement =
            AffineLimiterSettlementGuard::new(receipt.clone(), reservation);
        let execution = prepared.execute().await;
        limiter_settlement.settle();
        if let Some(guard) = &mut settlement_guard {
            guard.disarm();
        }
        recovery.finish_physical_attempt(execution.result());
        let PhysicalCallObservation::Completed {
            outcome,
            dependency_elapsed,
        } = receipt.observation()
        else {
            if let Some(breaker) = &self.breaker {
                breaker.settle_not_executed(ctx);
            }
            let terminal = recovery.finish_with_physical_result();
            debug_assert!(matches!(
                terminal,
                RecoveryTerminalDecision::ReturnPhysicalResult
            ));
            write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
            return execution.into_report(ctx.take_control_events());
        };

        ctx.insert::<EffectCallDurationNanos>(
            dependency_elapsed.as_nanos().min(u64::MAX as u128) as u64
        );
        if self.breaker.is_some() {
            prepare_retry_context(execution.result(), ctx);
        }
        let classification = self.breaker.as_ref().and_then(|breaker| {
            classify_physical_result(breaker, event, execution.result(), outcome, ctx)
        });
        if let Some(breaker) = &self.breaker {
            if let Some(classification) = classification.as_ref() {
                breaker.settle_classified_call(classification, ctx);
            } else {
                breaker.settle_unobserved_call(ctx);
            }
            ctx.write_control_event(ChainEventFactory::circuit_breaker_attempt_settled(
                self.writer_id,
                CircuitBreakerAttemptSettledEventParams {
                    cursor: identity.cursor.clone(),
                    attempt,
                    health_classification: evidence_classification(classification.as_ref()),
                    slow: breaker.is_slow_dependency_call(dependency_elapsed),
                    dependency_elapsed_ms: duration_ms(dependency_elapsed),
                    admission_wait_ms: duration_ms(admission_wait),
                },
                event.id,
            ));
        } else {
            ctx.write_control_event(ChainEventFactory::recovery_attempt_completed(
                self.writer_id,
                identity.cursor.clone(),
                attempt,
                duration_ms(dependency_elapsed),
                duration_ms(admission_wait),
                event.id,
            ));
        }

        if let Some(limiter) = &self.limiter {
            limiter.observe_resilience_attempt(ctx);
        }

        let terminal = recovery.finish_with_physical_result();
        debug_assert!(matches!(
            terminal,
            RecoveryTerminalDecision::ReturnPhysicalResult
        ));
        write_recovery_completed(&mut recovery, self.writer_id, identity, event, ctx);
        execution.into_report(ctx.take_control_events())
    }
}

fn write_recovery_completed(
    recovery: &mut EffectRecoveryController,
    writer_id: obzenflow_core::WriterId,
    identity: &EffectIdentity,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) {
    let completion = recovery.take_completion();
    ctx.write_control_event(ChainEventFactory::recovery_completed(
        writer_id,
        RecoveryCompletedEventParams {
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
    writer_id: obzenflow_core::WriterId,
    identity: &EffectIdentity,
    cause: Box<crate::middleware::MiddlewareAbortCause>,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    let decision = recovery.finish_from_admission(cause);
    write_recovery_completed(recovery, writer_id, identity, event, ctx);
    match decision {
        RecoveryTerminalDecision::BoundaryRejected(cause) => {
            report_from_admission(PolicyAdmission::Reject(cause), ctx)
        }
        RecoveryTerminalDecision::ReturnLastPhysicalError => {
            write_exhausted(
                writer_id,
                identity,
                recovery.attempts(),
                RetryStopReason::CircuitNoLongerClosed,
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
    writer_id: obzenflow_core::WriterId,
    identity: &EffectIdentity,
    reason: RetryStopReason,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    write_exhausted(writer_id, identity, recovery.attempts(), reason, event, ctx);
    let decision = recovery.finish_with_last_physical_error();
    write_recovery_completed(recovery, writer_id, identity, event, ctx);
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
    writer_id: obzenflow_core::WriterId,
    identity: &EffectIdentity,
    result: Result<Vec<ChainEvent>, EffectError>,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) -> EffectBoundaryReport {
    let decision = recovery.finish_with_physical_result();
    write_recovery_completed(recovery, writer_id, identity, event, ctx);
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

fn affine_admission_terminal(
    decision: RecoveryTerminalDecision,
    ctx: &mut crate::middleware::MiddlewareContext,
    operation: obzenflow_runtime::effects::AffineEffectOperation,
) -> obzenflow_runtime::effects::AffineEffectBoundaryReport {
    match decision {
        RecoveryTerminalDecision::BoundaryRejected(cause) => {
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
        RecoveryTerminalDecision::ReturnLastPhysicalError
        | RecoveryTerminalDecision::ReturnPhysicalResult => {
            unreachable!("affine admission occurs before the next physical call")
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
        (PhysicalCallOutcome::Failed, Err(EffectError::EffectTargetInvariantViolation { .. })) => {
            Some(FailureClassification::Ignored)
        }
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
        EffectError::BindingAuthority { .. }
        | EffectError::Serialization(_)
        | EffectError::Journal(_)
        | EffectError::MissingRecordedEffect { .. }
        | EffectError::EffectInDoubt { .. }
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
        | EffectError::EffectTargetInvariantViolation { .. }
        | EffectError::DependencyFailed { .. }
        | EffectError::RecoveryAbandoned { .. }
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
    writer_id: obzenflow_core::WriterId,
    identity: &EffectIdentity,
    attempts: u32,
    reason: RetryStopReason,
    event: &ChainEvent,
    ctx: &mut crate::middleware::MiddlewareContext,
) {
    ctx.write_control_event(ChainEventFactory::retry_exhausted(
        writer_id,
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
        crate::middleware::control::circuit_breaker()
            .count_window(5)
            .minimum_calls(5)
            .failure_rate_threshold(0.6)
            .slow_call_duration(Duration::from_millis(250))
            .slow_call_rate_threshold(0.5)
            .open_for(Duration::from_secs(5))
    }

    #[test]
    fn checked_breaker_builder_rejects_ambiguous_and_incomplete_modes() {
        assert!(matches!(
            crate::middleware::control::circuit_breaker()
                .consecutive_failures(3)
                .count_window(5)
                .minimum_calls(5)
                .failure_rate_threshold(0.5)
                .validate(),
            Err(CircuitBreakerConfigError::MixedModes)
        ));
        assert!(matches!(
            crate::middleware::control::circuit_breaker()
                .count_window(5)
                .minimum_calls(5)
                .slow_call_duration(Duration::from_millis(10))
                .validate(),
            Err(CircuitBreakerConfigError::IncompleteSlowCallTrigger)
        ));
        assert!(matches!(
            crate::middleware::control::circuit_breaker()
                .count_window(3)
                .minimum_calls(4)
                .failure_rate_threshold(0.5)
                .validate(),
            Err(CircuitBreakerConfigError::MinimumCallsExceedsWindow { .. })
        ));
    }

    #[test]
    fn aggregate_validates_retry_without_panicking() {
        assert!(matches!(
            EffectPlanFactory::with_breaker(valid_breaker())
                .retry(crate::middleware::control::retry().fixed_delay(Duration::ZERO))
                .build(),
            Err(ControlConfigurationError::ZeroFixedDelay)
        ));
        assert!(matches!(
            EffectPlanFactory::with_breaker(valid_breaker())
                .retry(
                    crate::middleware::control::retry()
                        .fixed_delay(Duration::from_millis(1))
                        .max_attempts(0)
                )
                .build(),
            Err(ControlConfigurationError::ZeroRetryAttempts)
        ));
        assert!(matches!(
            EffectPlanFactory::with_breaker(valid_breaker())
                .retry(
                    crate::middleware::control::retry()
                        .fixed_delay(Duration::from_millis(1))
                        .max_backoff(Duration::ZERO)
                )
                .build(),
            Err(ControlConfigurationError::ZeroMaxBackoff)
        ));
        assert!(matches!(
            EffectPlanFactory::with_breaker(valid_breaker())
                .retry(
                    crate::middleware::control::retry()
                        .fixed_delay(Duration::from_millis(1))
                        .attempt_start_window(Duration::ZERO)
                )
                .build(),
            Err(ControlConfigurationError::ZeroAttemptStartWindow)
        ));
    }

    #[test]
    fn aggregate_builder_defaults_to_absent_retry_and_accepts_concrete_retry() {
        let builder = EffectPlanFactory::with_breaker(valid_breaker());
        assert!(builder.retry.is_none());

        let builder = builder
            .retry(crate::middleware::control::retry().fixed_delay(Duration::from_millis(10)));
        assert!(builder.retry.is_some());
        builder
            .build()
            .expect("concrete retry configuration should build");
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
    fn post_start_port_invariant_has_fixed_ignored_health() {
        let breaker = Arc::new(CircuitBreakerMiddleware::new(5));
        let event = ChainEventFactory::data_event(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
            "test.effect_input",
            std::num::NonZeroU32::MIN,
            serde_json::json!({}),
        );
        let result = Err(EffectError::target_invariant_violation(
            obzenflow_runtime::effects::EffectPortSlot::<()>::new("chat"),
        ));
        let ctx = crate::middleware::MiddlewareContext::with_scope(
            obzenflow_core::MiddlewareExecutionScope::LiveHandler,
        );

        assert!(matches!(
            classify_physical_result(
                breaker.as_ref(),
                &event,
                &result,
                PhysicalCallOutcome::Failed,
                &ctx,
            ),
            Some(FailureClassification::Ignored)
        ));
    }

    #[test]
    fn provider_retry_after_is_the_only_delay_floor_and_is_not_capped() {
        let retry = crate::middleware::control::retry()
            .fixed_delay(Duration::from_millis(50))
            .max_backoff(Duration::from_millis(10));
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
        let factory = EffectPlanFactory::with_breaker(valid_breaker())
            .retry(
                crate::middleware::control::retry()
                    .fixed_delay(Duration::from_millis(10))
                    .max_attempts(3)
                    .max_backoff(Duration::from_secs(1))
                    .attempt_start_window(Duration::from_secs(2)),
            )
            .rate_limit_each_attempt(crate::middleware::control::rate_limit(20.0))
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
            .all(|default| default.key_path.starts_with("middleware.")));
        let keys = defaults
            .iter()
            .map(|default| default.key_path)
            .collect::<std::collections::BTreeSet<_>>();
        assert!(keys.contains(CIRCUIT_BREAKER_MODE_KEY));
        assert!(keys.contains(RETRY_MAX_ATTEMPTS_KEY));
        assert!(keys.contains(RATE_LIMITER_EVENTS_PER_SECOND_KEY));
    }

    fn aggregate_snapshot(with_retry: bool, with_limiter: bool) -> serde_json::Value {
        let mut builder = EffectPlanFactory::with_breaker(valid_breaker());
        if with_retry {
            builder = builder.retry(
                crate::middleware::control::retry()
                    .fixed_delay(Duration::from_millis(10))
                    .max_attempts(3)
                    .max_backoff(Duration::from_secs(1))
                    .attempt_start_window(Duration::from_secs(2)),
            );
        }
        if with_limiter {
            builder = builder.rate_limit_each_attempt(crate::middleware::control::rate_limit(20.0));
        }
        builder
            .build()
            .expect("snapshot aggregate should be valid")
            .config_snapshot()
            .expect("effect resilience exposes aggregate-local introspection")
    }

    #[test]
    fn aggregate_snapshot_omits_absent_optional_components() {
        let breaker_only = aggregate_snapshot(false, false);
        assert_eq!(breaker_only["kind"], "effect_resilience");
        assert!(breaker_only["breaker"].is_object());
        assert!(breaker_only.get("retry").is_none());
        assert!(breaker_only.get("rate_limiter").is_none());

        let retry_only = aggregate_snapshot(true, false);
        assert_eq!(retry_only["breaker"], breaker_only["breaker"]);
        assert_eq!(
            retry_only["retry"],
            serde_json::json!({
                (RETRY_KIND_KEY): "fixed",
                (RETRY_FIXED_DELAY_MS_KEY): 10,
                (RETRY_MAX_ATTEMPTS_KEY): 3,
                (RETRY_MAX_BACKOFF_MS_KEY): 1_000,
                (RETRY_ATTEMPT_START_WINDOW_MS_KEY): 2_000,
            })
        );
        assert!(retry_only.get("rate_limiter").is_none());

        let limiter_only = aggregate_snapshot(false, true);
        assert_eq!(limiter_only["breaker"], breaker_only["breaker"]);
        assert!(limiter_only.get("retry").is_none());
        assert_eq!(
            limiter_only["rate_limiter"],
            serde_json::json!({
                (RATE_LIMITER_EVENTS_PER_SECOND_KEY): 20.0,
                (RATE_LIMITER_COST_PER_ATTEMPT_KEY): 1.0,
            })
        );

        let complete = aggregate_snapshot(true, true);
        assert_eq!(complete["breaker"], breaker_only["breaker"]);
        assert_eq!(complete["retry"], retry_only["retry"]);
        assert_eq!(complete["rate_limiter"], limiter_only["rate_limiter"]);
    }
}
