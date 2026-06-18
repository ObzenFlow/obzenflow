// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{
    CircuitBreakerFailureMode, CircuitBreakerThresholdError, HalfOpenPolicy, OpenPolicy,
};
use super::fallback::{
    build_outcome_fallback_events, build_typed_fallback_event, build_typed_rejection_event,
};
use super::hook_adapters::{
    CircuitBreakerCompletionGate, CircuitBreakerSinkPolicy, CircuitBreakerSourcePolicy,
};
use super::state::CircuitBreakerStateViewImpl;
use super::window::{FailureWindow, FailureWindowState};
use super::{
    CircuitBreakerFamily, CircuitBreakerMiddleware, CircuitBreakerRetryPolicy,
    FailureClassification, FailureClassificationClassifier, FailureClassificationPolicy,
    FailureClassifier, FallbackFn, RetryLimits, TypedOutcomeConfig, UnknownErrorKindPolicy,
};
use crate::middleware::control::ControlMiddlewareAggregator;
use crate::middleware::{
    validate_attachment_request, ControlMiddlewareRole, Middleware, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError, MiddlewareHints,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewarePlanContribution,
    MiddlewareSafety, MiddlewareSurface, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
    SinkPolicy, SourcePolicy, SourcePollAttachment, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::control_middleware::{
    CircuitBreakerMetrics, CircuitBreakerSnapshotter, CircuitBreakerState, CircuitBreakerStateView,
    ControlMiddlewareProvider,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::CircuitBreakerRejectionReason;
use obzenflow_core::event::schema::TypedFactType;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::control_strategies::BackoffStrategy;
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

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
    pub(super) failure_mode: Option<CircuitBreakerFailureMode>,
    pub(super) open_policy: Option<OpenPolicy>,
    pub(super) half_open_policy: Option<HalfOpenPolicy>,
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
        // FLOWIP-115b: production retry is removed. A configured retry policy
        // binds successfully but is ignored with a one-time compatibility
        // warning; FLOWIP-115h reintroduces retry as boundary-owned recovery, so
        // the breaker never receives a retry policy on the production path.
        if self.retry_policy.is_some() {
            warn_circuit_breaker_retry_ignored();
        }
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
            retry_policy: None,
            retry_limits: self.retry_limits,
            failure_classification_policy: self.failure_classification_policy,
        })
    }
}

/// FLOWIP-115b: production circuit-breaker retry is removed; FLOWIP-115h
/// reintroduces it as boundary-owned recovery. A retry policy on a built factory
/// is ignored with this one-time, process-wide compatibility warning.
fn warn_circuit_breaker_retry_ignored() {
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        tracing::warn!(
            "Circuit breaker retry configuration is ignored: FLOWIP-115b removed \
             production retry and FLOWIP-115h reintroduces it as boundary-owned \
             recovery. Remove the retry policy to silence this warning."
        );
    });
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
    fn build_middleware_keyed(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<ControlMiddlewareAggregator>,
        effect_type: Option<String>,
    ) -> crate::middleware::MiddlewareFactoryResult<CircuitBreakerMiddleware> {
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
        let cb_state_view: Arc<dyn CircuitBreakerStateView> =
            Arc::new(CircuitBreakerStateViewImpl {
                state: middleware.state.clone(),
                generation: middleware.probe_generation.clone(),
            });
        let cb_state_view_for_snap = cb_state_view.clone();
        let successes_total = middleware.successes_total.clone();
        let failures_total = middleware.failures_total.clone();
        let rejections_total = middleware.rejections_total.clone();
        let opened_total = middleware.opened_total.clone();
        let time_in_closed = middleware.time_in_closed.clone();
        let time_in_open = middleware.time_in_open.clone();
        let time_in_half_open = middleware.time_in_half_open.clone();
        let last_state_change = middleware.last_state_change.clone();
        let snapshotter: std::sync::Arc<CircuitBreakerSnapshotter> = Arc::new(move || {
            let state = cb_state_view_for_snap.snapshot().state;

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

            match state {
                CircuitBreakerState::Closed => closed += elapsed_current,
                CircuitBreakerState::Open => open += elapsed_current,
                CircuitBreakerState::HalfOpen => half_open += elapsed_current,
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
                state: state.as_u8(),
            }
        });

        match effect_type {
            Some(effect_type) => control_middleware.register_circuit_breaker_for_effect(
                config.stage_id,
                effect_type,
                snapshotter,
                cb_state_view,
            ),
            None => control_middleware.register_circuit_breaker(
                config.stage_id,
                snapshotter,
                cb_state_view,
            ),
        }

        Ok(middleware)
    }

    /// Box the breaker middleware for the per-event chain (effect and handler
    /// paths). FLOWIP-115a sources build the same instance as an `Arc` via
    /// [`build_middleware_keyed`] and drive it through the runtime control ports.
    fn create_keyed(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<ControlMiddlewareAggregator>,
        effect_type: Option<String>,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(self.build_middleware_keyed(
            config,
            control_middleware,
            effect_type,
        )?))
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
        ControlMiddlewareRole::None
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
        control_middleware: std::sync::Arc<ControlMiddlewareAggregator>,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        self.create_keyed(config, control_middleware, None)
    }

    fn create_for_effect(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<ControlMiddlewareAggregator>,
        effect_type: &str,
    ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        self.create_keyed(config, control_middleware, Some(effect_type.to_string()))
    }

    fn register_source_policy(
        &self,
        config: &StageConfig,
        _stage_type: obzenflow_core::event::context::StageType,
        control_middleware: &std::sync::Arc<ControlMiddlewareAggregator>,
    ) -> crate::middleware::MiddlewareFactoryResult<()> {
        // Build one breaker instance (registering its cb_state and snapshotter,
        // which the CompletionGate and metrics path read), then register it as
        // one source-boundary policy. The breaker guards pre-poll regardless
        // of source kind, so stage type is irrelevant here.
        let middleware = self.build_middleware_keyed(config, control_middleware.clone(), None)?;
        let breaker = std::sync::Arc::new(middleware);
        control_middleware.register_source_policy(
            config.stage_id,
            std::sync::Arc::new(CircuitBreakerSourcePolicy { breaker }),
        );
        Ok(())
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        // The breaker is hook-bound control middleware. This slice materializes
        // the source-poll and effect surfaces; the sink-delivery phase adds sink
        // delivery to this surface set.
        MiddlewareDeclaration::control(
            self.label(),
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
                MiddlewareSurfaceKind::SinkDelivery,
            ],
        )
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        let declaration = self.declaration();
        let _attachment_id =
            validate_attachment_request(&declaration, &request).map_err(|err| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    err,
                )
            })?;

        match request.surface {
            MiddlewareSurface::SourcePoll(_) => {
                // Build one breaker (registering its state view and snapshotter),
                // then return its source policy plus the completion companion
                // that reads the same view, so completion and the boundary share
                // one state authority (FLOWIP-115b AC26).
                let middleware = self.build_middleware_keyed(
                    context.config,
                    context.control_middleware.clone(),
                    None,
                )?;
                let breaker = Arc::new(middleware);
                let view = context
                    .control_middleware
                    .circuit_breaker_state_view(&context.config.stage_id)
                    .expect("breaker just registered its state view");
                let completion_gate: Arc<dyn CompletionGate> =
                    Arc::new(CircuitBreakerCompletionGate::new(view));
                let policy: Arc<dyn SourcePolicy> =
                    Arc::new(CircuitBreakerSourcePolicy { breaker });
                Ok(MiddlewareSurfaceAttachment::SourcePoll(
                    SourcePollAttachment {
                        policy,
                        completion_gate: Some(completion_gate),
                    },
                ))
            }
            MiddlewareSurface::Effect(effect_surface) => {
                // One breaker instance guards one declared effect (FLOWIP-120c),
                // registering under the per-effect key for metrics. The breaker
                // is itself the EffectPolicy the binder composes into the chain.
                let middleware = self.build_middleware_keyed(
                    context.config,
                    context.control_middleware.clone(),
                    Some(effect_surface.effect_type.as_str().to_string()),
                )?;
                let policy: Arc<dyn crate::middleware::EffectPolicy> = Arc::new(middleware);
                Ok(MiddlewareSurfaceAttachment::Effect(policy))
            }
            MiddlewareSurface::SinkDelivery(_) => {
                // One breaker guards the sink stage's delivery unit, registered
                // stage-level. It fails fast when open via `CircuitBreakerSinkPolicy`.
                let middleware = self.build_middleware_keyed(
                    context.config,
                    context.control_middleware.clone(),
                    None,
                )?;
                let breaker = Arc::new(middleware);
                let policy: Arc<dyn SinkPolicy> = Arc::new(CircuitBreakerSinkPolicy { breaker });
                Ok(MiddlewareSurfaceAttachment::SinkDelivery(policy))
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!(
                    "circuit breaker materialize is not implemented for surface {:?} in this slice",
                    other.kind()
                )),
            )),
        }
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
