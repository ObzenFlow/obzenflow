// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{
    CircuitBreakerFailureMode, CircuitBreakerThresholdError, HalfOpenPolicy, OpenPolicy,
};
use super::criteria::RateCriteria;
use super::fallback::{
    build_outcome_fallback_events, build_typed_fallback_event, build_typed_rejection_event,
};
use super::hook_adapters::{
    CircuitBreakerCompletionGate, CircuitBreakerSinkPolicy, CircuitBreakerSourcePolicy,
};
use super::retry::Retry;
use super::state::CircuitBreakerStateViewImpl;
use super::window::{FailureWindow, FailureWindowState};
use super::{
    CircuitBreakerFamily, CircuitBreakerMiddleware, CircuitBreakerRetryPolicy,
    FailureClassification, FailureClassificationClassifier, FailureClassificationPolicy,
    FallbackFn, RetryLimits, TypedOutcomeConfig, UnknownErrorKindPolicy,
};
use crate::middleware::control::ControlMiddlewareAggregator;
use crate::middleware::{
    validate_attachment_request, EffectTypeKey, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareHints, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSafety, MiddlewareSurface, MiddlewareSurfaceAttachment,
    MiddlewareSurfaceKind, SinkPolicy, SourcePolicy, SourcePollAttachment,
    TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::observability_payload::CircuitBreakerRejectionReason;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::control_plane::{
    CircuitBreakerMetrics, CircuitBreakerSnapshotter, CircuitBreakerState, CircuitBreakerStateView,
    ControlPlaneProvider,
};
use obzenflow_runtime::effects::EffectSafety;
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

/// Typestate markers for the breaker builder's fallback shape. Users never
/// name these; they appear only in intermediate builder types.
pub mod shape {
    use super::super::{FallbackFn, TypedOutcomeConfig};
    use obzenflow_core::event::schema::TypedFactType;
    use std::marker::PhantomData;

    /// No fallback shape configured; `build()` yields a plain factory.
    pub struct Plain;

    /// A branch fallback producer is configured; a rejection producer for the
    /// same input type completes the branch shape.
    pub struct BranchFallback<In, Fb> {
        pub(super) fallback: FallbackFn,
        pub(super) marker: PhantomData<fn(&In) -> Fb>,
    }

    /// A branch rejection producer is configured; a fallback producer for the
    /// same input type completes the branch shape.
    pub struct BranchRejection<In, R> {
        pub(super) typed_outcome: TypedOutcomeConfig,
        pub(super) marker: PhantomData<fn(&In) -> R>,
    }

    /// Both branch producers are configured; `build()` yields the typed
    /// branch carrier validated against the stage contract.
    pub struct Branch<Fb, R> {
        pub(super) fallback: FallbackFn,
        pub(super) typed_outcome: TypedOutcomeConfig,
        pub(super) marker: PhantomData<fn() -> (Fb, R)>,
    }

    /// An outcome-shaped fallback is configured; `build()` yields the outcome
    /// carrier validated against the effect's fact set.
    pub struct Outcome {
        pub(super) fallback: FallbackFn,
        pub(super) effect_type: &'static str,
        pub(super) fact_types: Vec<TypedFactType>,
    }
}

/// Entry points for building a circuit breaker. Each returns the typestate
/// builder in its plain state; fallback-shaping methods transition the state,
/// and one `build()` per terminal state makes a mismatched build impossible
/// to compile rather than a deferred flow-build error.
pub struct CircuitBreaker;

impl CircuitBreaker {
    /// Open after `failures` consecutive failures. The FLOWIP-010 key
    /// `effects.circuit_breaker.threshold` may override the count at build
    /// time.
    pub fn opens_after(failures: u32) -> CircuitBreakerBuilder<shape::Plain> {
        assert!(
            failures > 0,
            "circuit-breaker opens_after requires at least one failure"
        );
        CircuitBreakerBuilder {
            core: BuilderCore::new(failures as usize, None),
            shape: shape::Plain,
        }
    }

    /// Open on rate-based criteria; see [`failure_rate`](super::failure_rate).
    pub fn opens_when(criteria: RateCriteria) -> CircuitBreakerBuilder<shape::Plain> {
        let threshold = criteria.reporting_threshold();
        CircuitBreakerBuilder {
            core: BuilderCore::new(threshold, Some(criteria.into_failure_mode())),
            shape: shape::Plain,
        }
    }
}

/// Shared configuration behind every builder state.
struct BuilderCore {
    threshold: usize,
    cooldown: Duration,
    failure_mode: Option<CircuitBreakerFailureMode>,
    open_policy: Option<OpenPolicy>,
    probes: Option<NonZeroU32>,
    on_probe_rejected: Option<OpenPolicy>,
    failure_classification_classifier: Option<FailureClassificationClassifier>,
    unknown_error_kind_policy: UnknownErrorKindPolicy,
    retry: Option<Retry>,
    failure_classification_policy: FailureClassificationPolicy,
    transitional_fallback: Option<FallbackFn>,
}

impl BuilderCore {
    fn new(threshold: usize, failure_mode: Option<CircuitBreakerFailureMode>) -> Self {
        Self {
            threshold,
            cooldown: Duration::from_secs(60),
            failure_mode,
            open_policy: None,
            probes: None,
            on_probe_rejected: None,
            failure_classification_classifier: None,
            unknown_error_kind_policy: UnknownErrorKindPolicy::TreatAsInfraFailure,
            retry: None,
            failure_classification_policy: FailureClassificationPolicy::default(),
            transitional_fallback: None,
        }
    }

    fn into_factory(
        self,
        fallback: Option<FallbackFn>,
        typed_outcome: Option<TypedOutcomeConfig>,
    ) -> Box<dyn MiddlewareFactory> {
        let half_open_policy = match (self.probes, self.on_probe_rejected) {
            (None, None) => None,
            (probes, on_rejected) => {
                let default = HalfOpenPolicy::default();
                Some(HalfOpenPolicy::new(
                    probes.unwrap_or(default.permitted_probes),
                    on_rejected.unwrap_or(default.on_rejected),
                ))
            }
        };
        let (retry_policy, retry_limits) = match self.retry {
            Some(retry) => (Some(retry.policy), retry.limits),
            None => (None, RetryLimits::default()),
        };
        Box::new(CircuitBreakerFactory {
            threshold: self.threshold,
            cooldown: self.cooldown,
            fallback,
            typed_outcome,
            failure_classification_classifier: self.failure_classification_classifier,
            failure_mode: self.failure_mode,
            open_policy: self.open_policy,
            half_open_policy,
            unknown_error_kind_policy: self.unknown_error_kind_policy,
            retry_policy,
            retry_limits,
            failure_classification_policy: self.failure_classification_policy,
        })
    }
}

/// The circuit-breaker builder, typestate over its fallback shape.
pub struct CircuitBreakerBuilder<Shape> {
    core: BuilderCore,
    shape: Shape,
}

impl<Shape> CircuitBreakerBuilder<Shape> {
    /// Set the cooldown duration before attempting to close the circuit.
    pub fn cooldown(mut self, duration: Duration) -> Self {
        self.core.cooldown = duration;
        self
    }

    /// Maximum concurrent probe calls while half-open. Zero is invalid; the
    /// default is one probe.
    pub fn probes(mut self, probes: u32) -> Self {
        self.core.probes = Some(
            NonZeroU32::new(probes).expect("circuit-breaker probes must be greater than zero"),
        );
        self
    }

    /// Behaviour while the circuit is open. Defaults to
    /// [`OpenPolicy::EmitFallback`].
    pub fn when_open(mut self, policy: OpenPolicy) -> Self {
        self.core.open_policy = Some(policy);
        self
    }

    /// Behaviour for non-probe calls arriving while half-open probe slots are
    /// in use. Defaults to [`OpenPolicy::EmitFallback`].
    pub fn when_probe_rejected(mut self, policy: OpenPolicy) -> Self {
        self.core.on_probe_rejected = Some(policy);
        self
    }

    /// Configure the failure classification callback.
    ///
    /// The classification is final for breaker health. For effect recovery it
    /// may veto a retry that the raw typed failure permits, but it cannot
    /// promote an ineligible raw failure or cause a successful physical call
    /// to be executed again.
    pub fn with_failure_classification<F>(mut self, f: F) -> Self
    where
        F: Fn(&ChainEvent, &[ChainEvent]) -> FailureClassification + Send + Sync + 'static,
    {
        self.core.failure_classification_classifier = Some(Arc::new(f));
        self
    }

    /// Configure how delivery/error classifications affect breaker accounting.
    pub fn with_failure_classification_policy(
        mut self,
        policy: FailureClassificationPolicy,
    ) -> Self {
        self.core.failure_classification_policy = policy;
        self
    }

    /// Configure effect-bound recovery; see [`Retry`]. Eligibility stays
    /// gated by effect safety and the typed failure allowlist (FLOWIP-115h).
    pub fn retry(mut self, retry: Retry) -> Self {
        self.core.retry = Some(retry);
        self
    }
}

impl CircuitBreakerBuilder<shape::Plain> {
    /// Configure the branch-shaped fallback fact (FLOWIP-120h/120m): while
    /// the breaker prevents the call, synthesize this distinct branch fact
    /// from the protected input payload. Completing the branch shape requires
    /// a rejection producer for the same input type.
    pub fn fallback_fact<In, Fb, F>(
        self,
        f: F,
    ) -> CircuitBreakerBuilder<shape::BranchFallback<In, Fb>>
    where
        In: TypedPayload + DeserializeOwned,
        Fb: TypedPayload + Serialize + 'static,
        F: Fn(&In) -> Fb + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent,
                            lineage: obzenflow_core::config::LineagePolicy|
              -> Vec<ChainEvent> {
            build_typed_fallback_event::<In, Fb, F>(&f, event, lineage)
        };
        CircuitBreakerBuilder {
            core: self.core,
            shape: shape::BranchFallback {
                fallback: Arc::new(adapter),
                marker: std::marker::PhantomData,
            },
        }
    }

    /// Configure the branch-shaped rejection fact (FLOWIP-120h/120m): when
    /// the breaker rejects without fallback data, synthesize this fact from
    /// the guarded input and the rejection reason. The reason must end up in
    /// the fact payload; strict replay reconstructs branches from facts
    /// alone. Completing the branch shape requires a fallback producer for
    /// the same input type.
    pub fn rejection_fact<In, R, F>(
        self,
        f: F,
    ) -> CircuitBreakerBuilder<shape::BranchRejection<In, R>>
    where
        In: TypedPayload + DeserializeOwned,
        R: TypedPayload + Serialize + 'static,
        F: Fn(&In, CircuitBreakerRejectionReason) -> R + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent,
                            reason: CircuitBreakerRejectionReason,
                            lineage: obzenflow_core::config::LineagePolicy|
              -> Vec<ChainEvent> {
            build_typed_rejection_event::<In, R, F>(&f, event, reason, lineage)
        };
        CircuitBreakerBuilder {
            core: self.core,
            shape: shape::BranchRejection {
                typed_outcome: TypedOutcomeConfig {
                    build_rejection: Arc::new(adapter),
                },
                marker: std::marker::PhantomData,
            },
        }
    }

    /// Configure the outcome-shaped fallback (FLOWIP-120m): while the breaker
    /// prevents the call, synthesize the protected effect's own outcome
    /// carrier instead of a distinct branch fact. This is for fallbacks that
    /// are genuine alternative outcomes (a cached decision, a stubbed
    /// authorization in a test profile); a fallback that records
    /// non-performance must stay branch-shaped. Spelled
    /// `.outcome_fallback::<TheEffect, _, _>(...)`.
    pub fn outcome_fallback<E, In, F>(self, f: F) -> CircuitBreakerBuilder<shape::Outcome>
    where
        E: obzenflow_runtime::effects::Effect,
        In: TypedPayload + DeserializeOwned,
        F: Fn(&In) -> E::Outcome + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent,
                            lineage: obzenflow_core::config::LineagePolicy|
              -> Vec<ChainEvent> {
            build_outcome_fallback_events::<E, In, F>(&f, event, lineage)
        };
        CircuitBreakerBuilder {
            core: self.core,
            shape: shape::Outcome {
                fallback: Arc::new(adapter),
                effect_type: E::EFFECT_TYPE,
                fact_types: <E::Outcome as obzenflow_core::event::schema::TypedFactSet>::fact_types(
                ),
            },
        }
    }

    /// Transitional-lane fallback for the single-effect `middleware:` route
    /// that FLOWIP-115g retires: the typed fallback producer without a branch
    /// registration, so the builder stays plain. The `effects:` lane requires
    /// the full branch pair via [`fallback_fact`](Self::fallback_fact).
    pub fn transitional_fallback_fact<In, Fb, F>(mut self, f: F) -> Self
    where
        In: TypedPayload + DeserializeOwned,
        Fb: TypedPayload + Serialize + 'static,
        F: Fn(&In) -> Fb + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent,
                            lineage: obzenflow_core::config::LineagePolicy|
              -> Vec<ChainEvent> {
            build_typed_fallback_event::<In, Fb, F>(&f, event, lineage)
        };
        self.core.transitional_fallback = Some(Arc::new(adapter));
        self
    }

    /// Build a plain breaker factory: health-only, or carrying a
    /// transitional-lane fallback.
    pub fn build(mut self) -> Box<dyn MiddlewareFactory> {
        let fallback = self.core.transitional_fallback.take();
        self.core.into_factory(fallback, None)
    }
}

impl<In, Fb> CircuitBreakerBuilder<shape::BranchFallback<In, Fb>> {
    /// Complete the branch shape with the rejection producer; see
    /// [`CircuitBreakerBuilder::rejection_fact`].
    pub fn rejection_fact<R, F>(self, f: F) -> CircuitBreakerBuilder<shape::Branch<Fb, R>>
    where
        In: TypedPayload + DeserializeOwned,
        R: TypedPayload + Serialize + 'static,
        F: Fn(&In, CircuitBreakerRejectionReason) -> R + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent,
                            reason: CircuitBreakerRejectionReason,
                            lineage: obzenflow_core::config::LineagePolicy|
              -> Vec<ChainEvent> {
            build_typed_rejection_event::<In, R, F>(&f, event, reason, lineage)
        };
        CircuitBreakerBuilder {
            core: self.core,
            shape: shape::Branch {
                fallback: self.shape.fallback,
                typed_outcome: TypedOutcomeConfig {
                    build_rejection: Arc::new(adapter),
                },
                marker: std::marker::PhantomData,
            },
        }
    }
}

impl<In, R> CircuitBreakerBuilder<shape::BranchRejection<In, R>> {
    /// Complete the branch shape with the fallback producer; see
    /// [`CircuitBreakerBuilder::fallback_fact`].
    pub fn fallback_fact<Fb, F>(self, f: F) -> CircuitBreakerBuilder<shape::Branch<Fb, R>>
    where
        In: TypedPayload + DeserializeOwned,
        Fb: TypedPayload + Serialize + 'static,
        F: Fn(&In) -> Fb + Send + Sync + 'static,
    {
        let adapter = move |event: &ChainEvent,
                            lineage: obzenflow_core::config::LineagePolicy|
              -> Vec<ChainEvent> {
            build_typed_fallback_event::<In, Fb, F>(&f, event, lineage)
        };
        CircuitBreakerBuilder {
            core: self.core,
            shape: shape::Branch {
                fallback: Arc::new(adapter),
                typed_outcome: self.shape.typed_outcome,
                marker: std::marker::PhantomData,
            },
        }
    }
}

impl<Fb, R> CircuitBreakerBuilder<shape::Branch<Fb, R>>
where
    Fb: TypedPayload + 'static,
    R: TypedPayload + 'static,
{
    /// Build the branch-shaped carrier for the `effects:` lane. The fact
    /// types were captured by the producers, so there is nothing to restate;
    /// contract membership is validated at flow build (FLOWIP-120h).
    pub fn build(self) -> crate::middleware::TypeShapingMiddleware<Fb, R> {
        let shape::Branch {
            fallback,
            typed_outcome,
            ..
        } = self.shape;
        let factory = self.core.into_factory(Some(fallback), Some(typed_outcome));
        crate::middleware::TypeShapingMiddleware::new(factory, "circuit_breaker", None)
    }
}

impl CircuitBreakerBuilder<shape::Outcome> {
    /// Build the outcome-shaped carrier for the `effects:` lane; the effect's
    /// fact set was captured by the producer.
    pub fn build(self) -> crate::middleware::OutcomeShapingMiddleware {
        let shape::Outcome {
            fallback,
            effect_type,
            fact_types,
        } = self.shape;
        let registration = obzenflow_runtime::effects::SynthesizedOutcomeRegistration {
            effect_type: Some(effect_type.to_string()),
            fact_types,
            source_label: "circuit_breaker".to_string(),
            kind: obzenflow_runtime::effects::SynthesizedOutcomeKind::OutcomeShaped,
        };
        let factory = self.core.into_factory(Some(fallback), None);
        crate::middleware::OutcomeShapingMiddleware::new(factory, registration, None)
    }
}

/// Factory for creating circuit breaker middleware. Constructed only through
/// [`CircuitBreakerBuilder`]; the former duplicated setter surface (TODO
/// G4/051c) is gone.
pub struct CircuitBreakerFactory {
    threshold: usize,
    cooldown: Duration,
    fallback: Option<FallbackFn>,
    typed_outcome: Option<TypedOutcomeConfig>,
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
    /// Shared materialization for stage-level and per-effect instances
    /// (FLOWIP-120c): the key decides where the snapshotter registers.
    fn build_middleware_keyed(
        &self,
        config: &StageConfig,
        control_middleware: std::sync::Arc<ControlMiddlewareAggregator>,
        effect_type: Option<EffectTypeKey>,
    ) -> crate::middleware::MiddlewareFactoryResult<CircuitBreakerMiddleware> {
        let effective_threshold = self.threshold;
        let validated_threshold = u32::try_from(effective_threshold)
            .ok()
            .and_then(std::num::NonZeroU32::new)
            .ok_or(CircuitBreakerThresholdError::InvalidThreshold {
                threshold: effective_threshold,
            })
            .map_err(|err| {
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
            effective_threshold,
            self.cooldown,
            self.fallback.clone(),
            Some(config.stage_id),
            None,
        );
        middleware.failure_mode = failure_mode;
        middleware.rate_window = rate_window;
        middleware.typed_outcome = self.typed_outcome.clone();
        middleware.lineage = config.lineage;
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
                // FLOWIP-115b AC28: publish the typed state; numeric/label
                // projection stays at the metric edges.
                state,
            }
        });

        let registration = match effect_type {
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
        };
        registration.map_err(|message| {
            MiddlewareFactoryError::invalid_configuration(
                self.label(),
                &config.name,
                std::io::Error::other(message),
            )
        })?;

        Ok(middleware)
    }
}

impl MiddlewareFactory for CircuitBreakerFactory {
    fn label(&self) -> &'static str {
        "circuit_breaker"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<CircuitBreakerFamily>("circuit_breaker")
    }

    fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> {
        vec![obzenflow_runtime::runtime_config::DslConfigDefault {
            key_path: obzenflow_runtime::runtime_config::CIRCUIT_BREAKER_THRESHOLD_KEY,
            value: obzenflow_runtime::runtime_config::ConfigValue::U64(self.threshold as u64),
        }]
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(TopologyMiddlewareConfigSlot::CircuitBreaker)
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        // The breaker is hook-bound control middleware. This slice materializes
        // the source-poll and effect surfaces; the sink-delivery phase adds sink
        // delivery to this surface set.
        MiddlewareDeclaration::control_with_family(
            self.label(),
            self.override_key().family_label(),
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

        let config_view = context.config_view(request.protected_unit);
        let effective_threshold = config_view
            .get(obzenflow_runtime::runtime_config::CIRCUIT_BREAKER_THRESHOLD_KEY)
            .and_then(|resolved| resolved.value.as_u64())
            .ok_or_else(|| {
                MiddlewareFactoryError::invalid_configuration(
                    self.label(),
                    &context.config.name,
                    std::io::Error::other(
                        "resolved circuit-breaker threshold is missing at the protected unit",
                    ),
                )
            })?;

        if self.retry_policy.is_some() {
            match request.surface {
                MiddlewareSurface::Effect(surface)
                    if matches!(
                        surface.safety,
                        EffectSafety::Idempotent | EffectSafety::NonIdempotentRequiresKey
                    ) => {}
                MiddlewareSurface::Effect(surface) => {
                    return Err(MiddlewareFactoryError::invalid_configuration(
                        self.label(),
                        &context.config.name,
                        std::io::Error::other(format!(
                            "circuit-breaker retry is not eligible for effect '{}' with safety {:?}",
                            surface.effect_type.as_str(),
                            surface.safety
                        )),
                    ));
                }
                _ => {
                    return Err(MiddlewareFactoryError::invalid_configuration(
                        self.label(),
                        &context.config.name,
                        std::io::Error::other(
                            "circuit-breaker retry is supported only on declared effects",
                        ),
                    ));
                }
            }
        }

        let effective_threshold = usize::try_from(effective_threshold).map_err(|_| {
            MiddlewareFactoryError::invalid_configuration(
                self.label(),
                &context.config.name,
                std::io::Error::other(
                    "resolved circuit-breaker threshold exceeds this platform's range",
                ),
            )
        })?;

        let materialized = CircuitBreakerFactory {
            threshold: effective_threshold,
            cooldown: self.cooldown,
            fallback: self.fallback.clone(),
            typed_outcome: self.typed_outcome.clone(),
            failure_classification_classifier: self.failure_classification_classifier.clone(),
            failure_mode: self.failure_mode.clone(),
            open_policy: self.open_policy.clone(),
            half_open_policy: self.half_open_policy.clone(),
            unknown_error_kind_policy: self.unknown_error_kind_policy,
            retry_policy: self.retry_policy.clone(),
            retry_limits: self.retry_limits.clone(),
            failure_classification_policy: self.failure_classification_policy.clone(),
        };

        match request.surface {
            MiddlewareSurface::SourcePoll(_) => {
                // Build one breaker (registering its state view and snapshotter),
                // then return its source policy plus the completion companion
                // that reads the same view, so completion and the boundary share
                // one state authority (FLOWIP-115b AC26).
                let middleware = materialized.build_middleware_keyed(
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
                // is an event-aware effect policy because breaker fallback and
                // classification derive facts from the parent event.
                let middleware = materialized.build_middleware_keyed(
                    context.config,
                    context.control_middleware.clone(),
                    Some(effect_surface.effect_type.clone()),
                )?;
                Ok(MiddlewareSurfaceAttachment::Effect(
                    crate::middleware::EffectPolicyAttachment::circuit_breaker(Arc::new(
                        middleware,
                    )),
                ))
            }
            MiddlewareSurface::SinkDelivery(_) => {
                // One breaker guards the sink stage's delivery unit, registered
                // stage-level. It fails fast when open via `CircuitBreakerSinkPolicy`.
                let middleware = materialized.build_middleware_keyed(
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
                "max_attempt_start_window_ms": self.retry_limits.max_attempt_start_window.as_millis() as u64,
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
    let failures = u32::try_from(threshold).expect("circuit-breaker threshold must fit in u32");
    CircuitBreaker::opens_after(failures).build()
}

/// Create a circuit breaker factory with defaults tuned for AI provider calls.
pub fn ai_circuit_breaker() -> Box<dyn MiddlewareFactory> {
    CircuitBreaker::opens_after(5).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::{
        EffectPolicyAttachment, EffectSurface, EffectUnitId, MiddlewareAttachmentRequest,
        MiddlewareDeclarationIndex, MiddlewareOrigin, ProtectedUnit, ProtectedUnitId,
    };
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::EffectType;
    use obzenflow_core::{StageId, StageKey};
    use obzenflow_runtime::effects::EffectSafety;
    use obzenflow_runtime::runtime_config::{
        materialize_flow_config, DslCandidates, FlowResolutionContext, ResolvedRuntimeConfig,
    };
    use std::collections::{BTreeMap, BTreeSet};

    fn test_stage_config(factory: &dyn MiddlewareFactory) -> StageConfig {
        let stage = StageKey::from("breaker_factory_test");
        let effect_type = EffectType::from("effect.retry");
        let mut dsl = DslCandidates::default();
        for default in factory.dsl_config_defaults() {
            dsl.declare_for_effect(
                default.key_path,
                stage.clone(),
                effect_type.clone(),
                default.value,
            );
        }
        let effective_config = materialize_flow_config(
            &ResolvedRuntimeConfig::builtin_defaults(),
            FlowResolutionContext {
                flow_name: "breaker_factory_test_flow".to_string(),
                stages: BTreeSet::from([stage.clone()]),
                edges: BTreeSet::new(),
                declared_effects: BTreeMap::from([(stage, BTreeSet::from([effect_type]))]),
                dsl,
            },
        )
        .expect("breaker defaults should resolve for the test effect");

        StageConfig {
            stage_id: StageId::new(),
            name: "breaker_factory_test".to_string(),
            flow_name: "breaker_factory_test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: Arc::new(effective_config),
        }
    }

    fn materialize_effect_attachment(
        factory: &dyn MiddlewareFactory,
        config: &StageConfig,
        control: &Arc<ControlMiddlewareAggregator>,
        declaration_index: usize,
        safety: EffectSafety,
    ) -> Result<EffectPolicyAttachment, String> {
        let surface = MiddlewareSurface::Effect(EffectSurface {
            stage_id: config.stage_id,
            effect_type: EffectTypeKey::from("effect.retry"),
            safety,
        });
        let protected_unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::Effect(EffectUnitId {
                effect_type: EffectTypeKey::from("effect.retry"),
            }),
        };
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &protected_unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::effect_policy(declaration_index),
        };
        let context = MiddlewareMaterializationContext {
            config,
            control_middleware: control,
            stage_type: StageType::Transform,
        };
        match factory
            .materialize(request, &context)
            .map_err(|error| error.to_string())?
        {
            MiddlewareSurfaceAttachment::Effect(attachment) => Ok(attachment),
            _ => panic!("circuit breaker should materialize an effect attachment"),
        }
    }

    #[test]
    #[should_panic(expected = "max_attempts must be greater than zero")]
    fn zero_attempt_configuration_is_rejected() {
        let _ = Retry::fixed(Duration::ZERO).attempts(0);
    }

    #[test]
    fn retry_materialization_is_limited_to_safe_declared_effects() {
        let factory = CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(2))
            .build();
        let config = test_stage_config(factory.as_ref());
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let error = match materialize_effect_attachment(
            factory.as_ref(),
            &config,
            &control,
            0,
            EffectSafety::Transactional,
        ) {
            Err(error) => error,
            Ok(_) => panic!("unsafe recovery attachment must fail"),
        };
        assert!(error.contains("retry is not eligible"), "{error}");

        let factory = CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(2))
            .build();
        let config = test_stage_config(factory.as_ref());
        let control = Arc::new(ControlMiddlewareAggregator::new());
        materialize_effect_attachment(
            factory.as_ref(),
            &config,
            &control,
            0,
            EffectSafety::NonIdempotentRequiresKey,
        )
        .expect("a keyed non-idempotent effect is recovery-eligible");
    }

    #[test]
    fn a_second_retrying_breaker_for_one_effect_fails_materialization() {
        let first = CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(2))
            .build();
        let second = CircuitBreaker::opens_after(3)
            .retry(Retry::fixed(Duration::ZERO).attempts(2))
            .build();
        let config = test_stage_config(first.as_ref());
        let control = Arc::new(ControlMiddlewareAggregator::new());

        materialize_effect_attachment(
            first.as_ref(),
            &config,
            &control,
            0,
            EffectSafety::Idempotent,
        )
        .expect("first breaker should materialize");
        let error = match materialize_effect_attachment(
            second.as_ref(),
            &config,
            &control,
            1,
            EffectSafety::Idempotent,
        ) {
            Err(error) => error,
            Ok(_) => panic!("duplicate breaker must be rejected, not replace the first"),
        };
        assert!(error.contains("already registered"), "{error}");
    }

    #[test]
    fn ai_breaker_helper_is_health_only() {
        let snapshot = ai_circuit_breaker()
            .config_snapshot()
            .expect("AI breaker should expose its configuration");
        assert!(snapshot["retry"].is_null());
    }

    #[test]
    fn control_factories_are_classified_by_typed_declarations() {
        use crate::middleware::control::rate_limiter::RateLimiterBuilder;

        let factories: Vec<Box<dyn MiddlewareFactory>> =
            vec![circuit_breaker(3), RateLimiterBuilder::new(5.0).build()];

        for factory in factories {
            let declaration = factory.declaration();
            assert!(declaration.is_control(), "{}", factory.label());
            assert!(!declaration.surfaces.is_empty(), "{}", factory.label());
            assert!(!declaration.is_flowip_128g_legacy_shell());
        }
    }
}
