// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::config::{
    CircuitBreakerConfigError, CircuitBreakerFailureMode, EffectCircuitBreakerConfig,
    HalfOpenPolicy,
};
use super::hook_adapters::{
    CircuitBreakerCompletionGate, CircuitBreakerSinkPolicy, CircuitBreakerSourcePolicy,
};
use super::state::CircuitBreakerStateViewImpl;
use super::window::{FailureWindow, FailureWindowState};
use super::{
    CircuitBreakerFamily, CircuitBreakerMiddleware, FailureClassificationClassifier, FailureHealth,
};
use crate::middleware::control::ControlMiddlewareAggregator;
use crate::middleware::{
    validate_attachment_request, Flowip128gLegacyShellAttachment, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError, MiddlewareHints,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, SinkPolicy, SourcePolicy,
    SourcePollAttachment, TopologyMiddlewareConfigSlot,
};
use obzenflow_runtime::control_plane::{
    CircuitBreakerMetrics, CircuitBreakerSnapshotter, CircuitBreakerState, CircuitBreakerStateView,
    ControlPlaneProvider,
};
use obzenflow_runtime::effects::EffectError;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::source::strategies::CompletionGate;
use serde_json::json;
use std::num::NonZeroU32;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Checked circuit-breaker configuration. This is the only public breaker
/// authoring value; exact effects compose it through `EffectResilience`, while
/// source and sink stages attach it directly.
#[derive(Clone)]
pub struct CircuitBreaker {
    pub(in crate::middleware::control) config: EffectCircuitBreakerConfig,
}

pub struct CheckedCircuitBreakerBuilder {
    consecutive_failures: Option<u32>,
    count_window: Option<u32>,
    minimum_calls: Option<u32>,
    failure_rate_threshold: Option<f64>,
    slow_call_duration: Option<Duration>,
    slow_call_rate_threshold: Option<f64>,
    open_for: Duration,
    probes: u32,
    rate_limited_counts_as_failure: bool,
    classifier: Option<FailureClassificationClassifier>,
}

impl CircuitBreaker {
    pub fn builder() -> CheckedCircuitBreakerBuilder {
        CheckedCircuitBreakerBuilder {
            consecutive_failures: None,
            count_window: None,
            minimum_calls: None,
            failure_rate_threshold: None,
            slow_call_duration: None,
            slow_call_rate_threshold: None,
            open_for: Duration::from_secs(60),
            probes: 1,
            rate_limited_counts_as_failure: false,
            classifier: None,
        }
    }

    pub(in crate::middleware::control) fn inherit_classifier_from(
        mut self,
        authored: &CircuitBreaker,
    ) -> Self {
        self.config.classifier = authored.config.classifier.clone();
        self
    }

    fn threshold_default(&self) -> u64 {
        match &self.config.failure_mode {
            CircuitBreakerFailureMode::Consecutive { max_failures } => max_failures.get() as u64,
            CircuitBreakerFailureMode::RateBased { window, .. } => match window {
                FailureWindow::Count { size } => *size as u64,
            },
        }
    }

    fn resolved_for_threshold(&self, threshold: u64) -> Result<Self, String> {
        let threshold = u32::try_from(threshold)
            .ok()
            .and_then(NonZeroU32::new)
            .ok_or_else(|| {
                format!("circuit-breaker threshold must be in 1..=u32::MAX, got {threshold}")
            })?;
        let mut resolved = self.clone();
        match &mut resolved.config.failure_mode {
            CircuitBreakerFailureMode::Consecutive { max_failures } => {
                *max_failures = threshold;
            }
            CircuitBreakerFailureMode::RateBased {
                window,
                minimum_calls,
                ..
            } => {
                if minimum_calls.get() > threshold.get() {
                    return Err(format!(
                        "minimum_calls ({}) must be <= count_window ({})",
                        minimum_calls.get(),
                        threshold.get()
                    ));
                }
                *window = FailureWindow::Count {
                    size: threshold.get(),
                };
            }
        }
        Ok(resolved)
    }

    fn resolved_from_context(
        &self,
        request: &MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> Result<Self, MiddlewareFactoryError> {
        let threshold = context
            .config_view(request.protected_unit)
            .get(obzenflow_runtime::runtime_config::CIRCUIT_BREAKER_THRESHOLD_KEY)
            .and_then(|resolved| resolved.value.as_u64())
            .ok_or_else(|| {
                MiddlewareFactoryError::invalid_configuration(
                    "circuit_breaker",
                    &context.config.name,
                    std::io::Error::other(
                        "resolved circuit-breaker threshold is missing at the protected unit",
                    ),
                )
            })?;
        self.resolved_for_threshold(threshold).map_err(|message| {
            MiddlewareFactoryError::invalid_configuration(
                "circuit_breaker",
                &context.config.name,
                std::io::Error::other(message),
            )
        })
    }
}

impl CheckedCircuitBreakerBuilder {
    pub fn consecutive_failures(mut self, failures: u32) -> Self {
        self.consecutive_failures = Some(failures);
        self
    }

    pub fn count_window(mut self, calls: u32) -> Self {
        self.count_window = Some(calls);
        self
    }

    pub fn minimum_calls(mut self, calls: u32) -> Self {
        self.minimum_calls = Some(calls);
        self
    }

    pub fn failure_rate_threshold(mut self, threshold: f64) -> Self {
        self.failure_rate_threshold = Some(threshold);
        self
    }

    pub fn slow_call_duration(mut self, duration: Duration) -> Self {
        self.slow_call_duration = Some(duration);
        self
    }

    pub fn slow_call_rate_threshold(mut self, threshold: f64) -> Self {
        self.slow_call_rate_threshold = Some(threshold);
        self
    }

    pub fn open_for(mut self, duration: Duration) -> Self {
        self.open_for = duration;
        self
    }

    pub fn probes(mut self, probes: u32) -> Self {
        self.probes = probes;
        self
    }

    pub fn rate_limited_counts_as_failure(mut self, enabled: bool) -> Self {
        self.rate_limited_counts_as_failure = enabled;
        self
    }

    pub fn classify_health_with<F>(mut self, classifier: F) -> Self
    where
        F: Fn(&EffectError) -> FailureHealth + Send + Sync + 'static,
    {
        self.classifier = Some(Arc::new(classifier));
        self
    }

    pub fn build(self) -> Result<CircuitBreaker, CircuitBreakerConfigError> {
        fn non_zero(
            value: u32,
            field: &'static str,
        ) -> Result<NonZeroU32, CircuitBreakerConfigError> {
            NonZeroU32::new(value).ok_or(CircuitBreakerConfigError::Zero { field })
        }

        fn rate(value: f64, field: &'static str) -> Result<f32, CircuitBreakerConfigError> {
            if !(value.is_finite() && 0.0 < value && value <= 1.0) {
                return Err(CircuitBreakerConfigError::InvalidRate { field, value });
            }
            Ok(value as f32)
        }

        if self.open_for.is_zero() {
            return Err(CircuitBreakerConfigError::Zero { field: "open_for" });
        }
        let probes = non_zero(self.probes, "probes")?;
        let has_rate_fields = self.minimum_calls.is_some()
            || self.failure_rate_threshold.is_some()
            || self.slow_call_duration.is_some()
            || self.slow_call_rate_threshold.is_some();

        let failure_mode = match (self.consecutive_failures, self.count_window) {
            (Some(_), Some(_)) => return Err(CircuitBreakerConfigError::MixedModes),
            (Some(_), None) if has_rate_fields => {
                return Err(CircuitBreakerConfigError::MixedModes)
            }
            (Some(failures), None) => CircuitBreakerFailureMode::Consecutive {
                max_failures: non_zero(failures, "consecutive_failures")?,
            },
            (None, Some(count_window)) => {
                let count_window = non_zero(count_window, "count_window")?;
                let minimum_calls = self
                    .minimum_calls
                    .ok_or(CircuitBreakerConfigError::MissingMinimumCalls)
                    .and_then(|value| non_zero(value, "minimum_calls"))?;
                if minimum_calls.get() > count_window.get() {
                    return Err(CircuitBreakerConfigError::MinimumCallsExceedsWindow {
                        minimum_calls: minimum_calls.get(),
                        count_window: count_window.get(),
                    });
                }
                let slow_pair = match (self.slow_call_duration, self.slow_call_rate_threshold) {
                    (None, None) => None,
                    (Some(duration), Some(threshold)) => {
                        if duration.is_zero() {
                            return Err(CircuitBreakerConfigError::Zero {
                                field: "slow_call_duration",
                            });
                        }
                        Some((duration, rate(threshold, "slow_call_rate_threshold")?))
                    }
                    _ => return Err(CircuitBreakerConfigError::IncompleteSlowCallTrigger),
                };
                if self.failure_rate_threshold.is_none() && slow_pair.is_none() {
                    return Err(CircuitBreakerConfigError::MissingRateTrigger);
                }
                CircuitBreakerFailureMode::RateBased {
                    window: FailureWindow::Count {
                        size: count_window.get(),
                    },
                    // A value outside the admitted domain disables the failure
                    // trigger for a slow-call-only rate breaker.
                    failure_rate_threshold: self
                        .failure_rate_threshold
                        .map(|value| rate(value, "failure_rate_threshold"))
                        .transpose()?
                        .unwrap_or(2.0),
                    slow_call_rate_threshold: slow_pair.map(|(_, threshold)| threshold),
                    slow_call_duration_threshold: slow_pair.map(|(duration, _)| duration),
                    minimum_calls,
                }
            }
            (None, None) if has_rate_fields => {
                return Err(CircuitBreakerConfigError::MissingCountWindow)
            }
            (None, None) => return Err(CircuitBreakerConfigError::MissingMode),
        };

        Ok(CircuitBreaker {
            config: EffectCircuitBreakerConfig {
                failure_mode,
                open_for: self.open_for,
                probes,
                classifier: self.classifier,
                rate_limited_counts_as_failure: self.rate_limited_counts_as_failure,
            },
        })
    }
}

/// Internal state-core materializer shared by the three final boundary
/// adapters. It is deliberately not a public factory.
pub(in crate::middleware::control) struct CircuitBreakerFactory {
    config: EffectCircuitBreakerConfig,
}

impl CircuitBreakerFactory {
    pub(in crate::middleware::control) fn from_effect_breaker(breaker: &CircuitBreaker) -> Self {
        Self {
            config: breaker.config.clone(),
        }
    }

    pub(in crate::middleware::control) fn build_middleware_keyed(
        &self,
        config: &StageConfig,
        control_middleware: Arc<ControlMiddlewareAggregator>,
        effect_type: Option<crate::middleware::EffectTypeKey>,
    ) -> crate::middleware::MiddlewareFactoryResult<CircuitBreakerMiddleware> {
        let threshold = match &self.config.failure_mode {
            CircuitBreakerFailureMode::Consecutive { max_failures } => max_failures.get() as usize,
            CircuitBreakerFailureMode::RateBased { window, .. } => match window {
                FailureWindow::Count { size } => *size as usize,
            },
        };
        let rate_window = match &self.config.failure_mode {
            CircuitBreakerFailureMode::RateBased {
                window: FailureWindow::Count { size },
                ..
            } => Some(Arc::new(Mutex::new(FailureWindowState::new(
                *size as usize,
            )))),
            CircuitBreakerFailureMode::Consecutive { .. } => None,
        };

        let mut middleware = CircuitBreakerMiddleware::with_cooldown_for_stage(
            threshold,
            self.config.open_for,
            config.stage_id,
        );
        middleware.failure_mode = self.config.failure_mode.clone();
        middleware.rate_window = rate_window;
        middleware.half_open_policy = HalfOpenPolicy::new(self.config.probes);
        middleware.failure_classification_classifier = self.config.classifier.clone();
        middleware.rate_limited_counts_as_failure = self.config.rate_limited_counts_as_failure;

        let state_view: Arc<dyn CircuitBreakerStateView> = Arc::new(CircuitBreakerStateViewImpl {
            state: middleware.state.clone(),
            generation: middleware.probe_generation.clone(),
        });
        let state_view_for_snapshot = state_view.clone();
        let requests_total = middleware.requests_total.clone();
        let successes_total = middleware.successes_total.clone();
        let failures_total = middleware.failures_total.clone();
        let slow_total = middleware.slow_total.clone();
        let rejections_total = middleware.rejections_total.clone();
        let opened_total = middleware.opened_total.clone();
        let time_in_closed = middleware.time_in_closed.clone();
        let time_in_open = middleware.time_in_open.clone();
        let time_in_half_open = middleware.time_in_half_open.clone();
        let last_state_change = middleware.last_state_change.clone();
        let snapshotter: Arc<CircuitBreakerSnapshotter> = Arc::new(move || {
            let state = state_view_for_snapshot.snapshot().state;
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
                requests_total: requests_total.load(Ordering::Relaxed),
                successes_total: successes_total.load(Ordering::Relaxed),
                failures_total: failures_total.load(Ordering::Relaxed),
                slow_total: slow_total.load(Ordering::Relaxed),
                rejections_total: rejections_total.load(Ordering::Relaxed),
                opened_total: opened_total.load(Ordering::Relaxed),
                time_closed_seconds: closed.as_secs_f64(),
                time_open_seconds: open.as_secs_f64(),
                time_half_open_seconds: half_open.as_secs_f64(),
                state,
            }
        });

        let registration = match effect_type {
            Some(effect_type) => control_middleware.register_circuit_breaker_for_effect(
                config.stage_id,
                effect_type,
                snapshotter,
                state_view,
            ),
            None => control_middleware.register_circuit_breaker(
                config.stage_id,
                snapshotter,
                state_view,
            ),
        };
        registration.map_err(|message| {
            MiddlewareFactoryError::invalid_configuration(
                "circuit_breaker",
                &config.name,
                std::io::Error::other(message),
            )
        })?;
        Ok(middleware)
    }
}

impl MiddlewareFactory for CircuitBreaker {
    fn label(&self) -> &'static str {
        "circuit_breaker"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<CircuitBreakerFamily>("circuit_breaker")
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::control_with_family(
            self.label(),
            self.override_key().family_label(),
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::SinkDelivery,
            ],
        )
    }

    fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> {
        vec![obzenflow_runtime::runtime_config::DslConfigDefault {
            key_path: obzenflow_runtime::runtime_config::CIRCUIT_BREAKER_THRESHOLD_KEY,
            value: obzenflow_runtime::runtime_config::ConfigValue::U64(self.threshold_default()),
        }]
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(TopologyMiddlewareConfigSlot::CircuitBreaker)
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        let resolved = self.resolved_from_context(&request, context)?;
        let materializer = CircuitBreakerFactory::from_effect_breaker(&resolved);

        match request.surface {
            MiddlewareSurface::SourcePoll(_) => {
                let breaker = Arc::new(materializer.build_middleware_keyed(
                    context.config,
                    context.control_middleware.clone(),
                    None,
                )?);
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
            MiddlewareSurface::SinkDelivery(_) => {
                let breaker = Arc::new(materializer.build_middleware_keyed(
                    context.config,
                    context.control_middleware.clone(),
                    None,
                )?);
                let policy: Arc<dyn SinkPolicy> = Arc::new(CircuitBreakerSinkPolicy { breaker });
                Ok(MiddlewareSurfaceAttachment::SinkDelivery(policy))
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!(
                    "standalone circuit breaker cannot attach to {:?}; declared effects use EffectResilience",
                    other.kind()
                )),
            )),
        }
    }

    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Advanced
    }

    fn hints(&self) -> MiddlewareHints {
        MiddlewareHints::default()
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        let mode = match &self.config.failure_mode {
            CircuitBreakerFailureMode::Consecutive { max_failures } => json!({
                "kind": "consecutive",
                "consecutive_failures": max_failures.get(),
            }),
            CircuitBreakerFailureMode::RateBased {
                window: FailureWindow::Count { size },
                failure_rate_threshold,
                slow_call_rate_threshold,
                slow_call_duration_threshold,
                minimum_calls,
            } => json!({
                "kind": "rate_based",
                "count_window": size,
                "minimum_calls": minimum_calls.get(),
                "failure_rate_threshold": failure_rate_threshold,
                "slow_call_rate_threshold": slow_call_rate_threshold,
                "slow_call_duration_ms": slow_call_duration_threshold.map(|d| d.as_millis() as u64),
            }),
        };
        Some(json!({
            "mode": mode,
            "open_for_ms": self.config.open_for.as_millis() as u64,
            "probes": self.config.probes.get(),
        }))
    }
}

/// Sealed compatibility factory for the two AI map-reduce shell consumers
/// explicitly assigned to FLOWIP-128g.
struct AiCircuitBreakerFactory {
    breaker: CircuitBreaker,
}

impl MiddlewareFactory for AiCircuitBreakerFactory {
    fn label(&self) -> &'static str {
        "ai_circuit_breaker"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<CircuitBreakerFamily>("circuit_breaker")
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::flowip_128g_legacy_shell(
            self.label(),
            self.override_key().family_label(),
        )
    }

    fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> {
        self.breaker.dsl_config_defaults()
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(TopologyMiddlewareConfigSlot::CircuitBreaker)
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> crate::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|error| {
            MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                error,
            )
        })?;
        let resolved = self.breaker.resolved_from_context(&request, context)?;
        match request.surface {
            MiddlewareSurface::Handler { .. } => {
                let middleware = CircuitBreakerFactory::from_effect_breaker(&resolved)
                    .build_middleware_keyed(
                        context.config,
                        context.control_middleware.clone(),
                        None,
                    )?;
                Ok(MiddlewareSurfaceAttachment::Flowip128gLegacyShell(
                    Flowip128gLegacyShellAttachment::new(Box::new(middleware)),
                ))
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!(
                    "FLOWIP-128g migration shell cannot attach to {:?}",
                    other.kind()
                )),
            )),
        }
    }

    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Advanced
    }
}

pub fn ai_circuit_breaker() -> Box<dyn MiddlewareFactory> {
    let breaker = CircuitBreaker::builder()
        .consecutive_failures(5)
        .build()
        .expect("constant AI breaker configuration is valid");
    Box::new(AiCircuitBreakerFactory { breaker })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_builder_rejects_partial_and_mixed_modes() {
        assert!(matches!(
            CircuitBreaker::builder().build(),
            Err(CircuitBreakerConfigError::MissingMode)
        ));
        assert!(matches!(
            CircuitBreaker::builder()
                .consecutive_failures(2)
                .count_window(5)
                .build(),
            Err(CircuitBreakerConfigError::MixedModes)
        ));
        assert!(matches!(
            CircuitBreaker::builder()
                .count_window(5)
                .minimum_calls(6)
                .failure_rate_threshold(0.5)
                .build(),
            Err(CircuitBreakerConfigError::MinimumCallsExceedsWindow { .. })
        ));
        assert!(matches!(
            CircuitBreaker::builder()
                .failure_rate_threshold(0.5)
                .build(),
            Err(CircuitBreakerConfigError::MissingCountWindow)
        ));
    }

    #[test]
    fn checked_builder_rejects_zero_invalid_rates_and_partial_slow_triggers() {
        for result in [
            CircuitBreaker::builder().consecutive_failures(0).build(),
            CircuitBreaker::builder()
                .count_window(0)
                .minimum_calls(1)
                .failure_rate_threshold(0.5)
                .build(),
            CircuitBreaker::builder()
                .count_window(5)
                .minimum_calls(0)
                .failure_rate_threshold(0.5)
                .build(),
            CircuitBreaker::builder()
                .consecutive_failures(1)
                .probes(0)
                .build(),
            CircuitBreaker::builder()
                .consecutive_failures(1)
                .open_for(Duration::ZERO)
                .build(),
        ] {
            assert!(matches!(
                result,
                Err(CircuitBreakerConfigError::Zero { .. })
            ));
        }

        for threshold in [0.0, 1.1, f64::NAN, f64::INFINITY] {
            assert!(matches!(
                CircuitBreaker::builder()
                    .count_window(5)
                    .minimum_calls(1)
                    .failure_rate_threshold(threshold)
                    .build(),
                Err(CircuitBreakerConfigError::InvalidRate { .. })
            ));
        }

        assert!(matches!(
            CircuitBreaker::builder()
                .count_window(5)
                .minimum_calls(1)
                .slow_call_duration(Duration::from_millis(1))
                .build(),
            Err(CircuitBreakerConfigError::IncompleteSlowCallTrigger)
        ));
        assert!(matches!(
            CircuitBreaker::builder()
                .count_window(5)
                .minimum_calls(1)
                .slow_call_duration(Duration::ZERO)
                .slow_call_rate_threshold(0.5)
                .build(),
            Err(CircuitBreakerConfigError::Zero {
                field: "slow_call_duration"
            })
        ));
    }

    #[test]
    fn standalone_breaker_has_only_structural_source_and_sink_surfaces() {
        let breaker = CircuitBreaker::builder()
            .consecutive_failures(3)
            .build()
            .unwrap();
        let declaration = breaker.declaration();
        assert!(declaration.supports(MiddlewareSurfaceKind::SourcePoll));
        assert!(declaration.supports(MiddlewareSurfaceKind::SinkDelivery));
        assert!(!declaration.supports(MiddlewareSurfaceKind::Effect));
        assert!(!declaration.is_flowip_128g_legacy_shell());
    }

    #[test]
    fn only_ai_helper_uses_the_sealed_legacy_shell() {
        assert!(ai_circuit_breaker()
            .declaration()
            .is_flowip_128g_legacy_shell());
    }
}
