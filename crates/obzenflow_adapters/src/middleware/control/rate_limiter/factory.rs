// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Inert rate-limiter values and checked boundary materialisation.
//!
//! [`RateLimiter::declaration`] plus [`RateLimiter::materialize`]
//! are the sole production placement authority for the hook-bound rate limiter:
//! the binder picks the concrete live-I/O surface per call site and routes it
//! through `materialize`, which builds the matching [`super::hook_adapters`]
//! policy. No generic middleware-chain creation route remains.

use super::config::{
    validated_rate_limiter_config, RateLimiterConfigError, ValidatedRateLimiterConfig,
    DEFAULT_COST_PER_EVENT,
};
use super::hook_adapters::{
    RateLimiterIngressPolicy, RateLimiterSinkPolicy, RateLimiterSourcePolicy,
    SourceRateLimitPosition,
};
use super::{RateLimiterFamily, RateLimiterMiddleware};
use crate::middleware::{
    validate_attachment_request, EffectPolicyAttachment, MaterializationClaim,
    MiddlewareAttachmentRequest, MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceAttachmentKind, MiddlewareSurfaceKind,
    SinkPolicy, SourcePolicy, SourcePollAttachment, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::IngressBoundaryMiddleware;
use std::sync::Arc;

/// Inert token-bucket configuration. Every attachment materialises its own bucket.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    pub(in crate::middleware::control) events_per_second: f64,
    pub(in crate::middleware::control) burst_capacity: Option<f64>,
    pub(in crate::middleware::control) cost_per_attempt: f64,
}

impl RateLimiter {
    pub fn burst_capacity(mut self, capacity: f64) -> Self {
        self.burst_capacity = Some(capacity);
        self
    }

    pub fn cost(mut self, cost: f64) -> Self {
        self.cost_per_attempt = cost;
        self
    }

    pub(in crate::middleware::control) fn validate(
        &self,
    ) -> Result<ValidatedRateLimiterConfig, RateLimiterConfigError> {
        validated_rate_limiter_config(
            self.events_per_second,
            self.burst_capacity,
            self.cost_per_attempt,
        )
    }

    fn resolved_config(
        &self,
        view: &obzenflow_runtime::runtime_config::ExactConfigView<'_>,
    ) -> Result<ValidatedRateLimiterConfig, RateLimiterConfigError> {
        use obzenflow_runtime::runtime_config::{
            RATE_LIMITER_BURST_CAPACITY_KEY, RATE_LIMITER_COST_PER_ATTEMPT_KEY,
            RATE_LIMITER_EVENTS_PER_SECOND_KEY,
        };
        self.validate()?;
        let rate = view
            .get(RATE_LIMITER_EVENTS_PER_SECOND_KEY)
            .and_then(|v| v.value.as_f64())
            .unwrap_or(self.events_per_second);
        let burst = view
            .get(RATE_LIMITER_BURST_CAPACITY_KEY)
            .and_then(|v| v.value.as_f64())
            .or(self.burst_capacity);
        let cost = view
            .get(RATE_LIMITER_COST_PER_ATTEMPT_KEY)
            .and_then(|v| v.value.as_f64())
            .unwrap_or(self.cost_per_attempt);
        validated_rate_limiter_config(rate, burst, cost)
    }
}

/// Declare an inert limiter; invalid settings are reported while building the flow.
pub fn rate_limit(events_per_second: f64) -> RateLimiter {
    RateLimiter {
        events_per_second,
        burst_capacity: None,
        cost_per_attempt: DEFAULT_COST_PER_EVENT,
    }
}

impl MiddlewareFactory for RateLimiter {
    fn builtin_control(&self) -> Option<super::super::composition::BuiltinControlContribution> {
        Some(super::super::composition::BuiltinControlContribution::limiter(self.clone()))
    }

    fn validate_configuration(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        config: &obzenflow_runtime::pipeline::config::StageConfig,
        stage_type: StageType,
    ) -> crate::middleware::MiddlewareFactoryResult<()> {
        let declaration = self.declaration();
        let context =
            MiddlewareMaterializationContext::new(config, stage_type, &declaration, &request);
        self.resolved_config(&context.config_view())
            .map(|_| ())
            .map_err(|error| {
                MiddlewareFactoryError::invalid_configuration(self.label(), &config.name, error)
            })
    }

    fn label(&self) -> &'static str {
        "rate_limiter"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<RateLimiterFamily>("rate_limiter")
    }

    fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> {
        use obzenflow_runtime::runtime_config::{
            ConfigValue, DslConfigDefault, RATE_LIMITER_BURST_CAPACITY_KEY,
            RATE_LIMITER_COST_PER_ATTEMPT_KEY, RATE_LIMITER_EVENTS_PER_SECOND_KEY,
        };
        let mut defaults = vec![
            DslConfigDefault {
                key_path: RATE_LIMITER_EVENTS_PER_SECOND_KEY,
                value: ConfigValue::F64(self.events_per_second),
            },
            DslConfigDefault {
                key_path: RATE_LIMITER_COST_PER_ATTEMPT_KEY,
                value: ConfigValue::F64(self.cost_per_attempt),
            },
        ];
        if let Some(burst_capacity) = self.burst_capacity {
            defaults.push(DslConfigDefault {
                key_path: RATE_LIMITER_BURST_CAPACITY_KEY,
                value: ConfigValue::F64(burst_capacity),
            });
        }
        defaults
    }

    fn consumed_config_keys(&self) -> Vec<&'static str> {
        vec![
            obzenflow_runtime::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY,
            obzenflow_runtime::runtime_config::RATE_LIMITER_BURST_CAPACITY_KEY,
            obzenflow_runtime::runtime_config::RATE_LIMITER_COST_PER_ATTEMPT_KEY,
        ]
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        Some(TopologyMiddlewareConfigSlot::RateLimiter)
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        // FLOWIP-115d: the rate limiter is hook-bound control middleware that
        // attaches to the live-I/O boundary surfaces. The binder picks the
        // concrete surface per call site and routes it through `materialize`.
        MiddlewareDeclaration::rate_limiter(
            self.label(),
            self.override_key().family_label(),
            vec![
                MiddlewareSurfaceKind::SourcePoll,
                MiddlewareSurfaceKind::Effect,
                MiddlewareSurfaceKind::SinkDelivery,
                MiddlewareSurfaceKind::Ingress,
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
        context
            .authorize_materialization(MaterializationClaim::RateLimiter, &declaration, &request)
            .map_err(|error| {
                MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    error,
                )
            })?;

        let validated = self
            .resolved_config(&context.config_view())
            .map_err(|err| {
                MiddlewareFactoryError::invalid_configuration(
                    self.label(),
                    &context.config.name,
                    err,
                )
            })?;

        match request.surface {
            MiddlewareSurface::SourcePoll(_) => {
                // FLOWIP-114m: an infinite source paces pre-poll; a finite source
                // charges after a clean non-empty delivery.
                let charge_at = match context.stage_type {
                    StageType::InfiniteSource => SourceRateLimitPosition::PrePoll,
                    _ => SourceRateLimitPosition::AfterPoll,
                };
                let middleware = Arc::new(
                    RateLimiterMiddleware::new(
                        context.config.stage_id,
                        validated,
                        context,
                        MaterializationClaim::RateLimiter,
                    )
                    .map_err(|message| {
                        MiddlewareFactoryError::invalid_configuration(
                            self.label(),
                            &context.config.name,
                            std::io::Error::other(message),
                        )
                    })?,
                );
                let policy: Arc<dyn SourcePolicy> =
                    Arc::new(RateLimiterSourcePolicy::new(middleware, charge_at));
                MiddlewareSurfaceAttachment::claimed(
                    MiddlewareSurfaceAttachmentKind::SourcePoll(SourcePollAttachment {
                        policy,
                        completion_gate: None,
                    }),
                    MaterializationClaim::RateLimiter,
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
            MiddlewareSurface::Effect(effect_surface) => {
                // FLOWIP-120c: one limiter instance guards one declared effect,
                // registered under the per-effect key for metrics.
                let middleware = RateLimiterMiddleware::new_keyed(
                    context.config.stage_id,
                    validated,
                    context,
                    MaterializationClaim::RateLimiter,
                    Some(effect_surface.effect_type.clone()),
                )
                .map_err(|message| {
                    MiddlewareFactoryError::invalid_configuration(
                        self.label(),
                        &context.config.name,
                        std::io::Error::other(message),
                    )
                })?;
                MiddlewareSurfaceAttachment::claimed(
                    MiddlewareSurfaceAttachmentKind::Effect(EffectPolicyAttachment::neutral(
                        Arc::new(middleware),
                    )),
                    MaterializationClaim::RateLimiter,
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
            MiddlewareSurface::SinkDelivery(_) => {
                let middleware = Arc::new(
                    RateLimiterMiddleware::new(
                        context.config.stage_id,
                        validated,
                        context,
                        MaterializationClaim::RateLimiter,
                    )
                    .map_err(|message| {
                        MiddlewareFactoryError::invalid_configuration(
                            self.label(),
                            &context.config.name,
                            std::io::Error::other(message),
                        )
                    })?,
                );
                let policy: Arc<dyn SinkPolicy> = Arc::new(RateLimiterSinkPolicy::new(middleware));
                MiddlewareSurfaceAttachment::claimed(
                    MiddlewareSurfaceAttachmentKind::SinkDelivery(policy),
                    MaterializationClaim::RateLimiter,
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
            MiddlewareSurface::Ingress(_) => {
                // FLOWIP-115d: source-backed hosted ingress. One core per hosted
                // protected unit; the adapter is fail-fast at the listener edge.
                let middleware = Arc::new(
                    RateLimiterMiddleware::new(
                        context.config.stage_id,
                        validated,
                        context,
                        MaterializationClaim::RateLimiter,
                    )
                    .map_err(|message| {
                        MiddlewareFactoryError::invalid_configuration(
                            self.label(),
                            &context.config.name,
                            std::io::Error::other(message),
                        )
                    })?,
                );
                let policy: Arc<dyn IngressBoundaryMiddleware> =
                    Arc::new(RateLimiterIngressPolicy::new(middleware));
                MiddlewareSurfaceAttachment::claimed(
                    MiddlewareSurfaceAttachmentKind::Ingress(policy),
                    MaterializationClaim::RateLimiter,
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
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!(
                    "rate limiter materialize is not implemented for surface {:?}",
                    other.kind()
                )),
            )),
        }
    }

    fn supported_stage_types(&self) -> &[StageType] {
        // Rate limiting makes sense for all stage types, including joins where the
        // single stage-local bucket is shared across both join inputs (FLOWIP-114m).
        &[
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
            StageType::Join,
        ]
    }

    fn safety_level(&self) -> MiddlewareSafety {
        // Rate limiting on sinks can cause backpressure
        MiddlewareSafety::Advanced
    }

    fn hints(&self) -> crate::middleware::MiddlewareHints {
        crate::middleware::MiddlewareHints {
            rate_limits: true,
            ..Default::default()
        }
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        let validated = self.validate().ok()?;
        let mut snapshot = serde_json::json!({
            "tokens_per_sec": validated.events_per_second,
            "burst_capacity": validated.burst_capacity,
            "cost_per_event": validated.cost_per_event,
            "limit_rate": validated.limit_rate(),
        });
        if let Some(configured_burst_capacity) = validated.configured_burst_capacity {
            snapshot["configured_burst_capacity"] = serde_json::json!(configured_burst_capacity);
        }
        Some(snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
    use crate::middleware::materialize_factory_checked;
    use obzenflow_core::{StageId, StageKey};
    use obzenflow_runtime::pipeline::config::StageConfig;
    use obzenflow_runtime::runtime_config::{
        materialize_flow_config, DslCandidates, FlowResolutionContext, ResolvedRuntimeConfig,
    };
    use serde_json::json;
    use std::collections::BTreeSet;

    fn test_stage_config(name: &str, factory: &dyn MiddlewareFactory) -> StageConfig {
        let stage = StageKey::from(name);
        let mut dsl = DslCandidates::default();
        for key_path in factory.consumed_config_keys() {
            dsl.declare_stage_consumption(key_path, stage.clone());
        }
        for default in factory.dsl_config_defaults() {
            dsl.declare(
                default.key_path,
                obzenflow_core::config::ConfigScope::stage(stage.clone()),
                default.value,
            );
        }
        let effective_config = materialize_flow_config(
            &ResolvedRuntimeConfig::builtin_defaults(),
            FlowResolutionContext {
                flow_name: "test_flow".to_string(),
                stages: BTreeSet::from([stage]),
                edges: BTreeSet::new(),
                declared_effects: Default::default(),
                dsl,
            },
        )
        .expect("rate-limiter defaults should resolve for the test stage");

        StageConfig {
            stage_id: StageId::new(),
            name: name.to_string(),
            flow_name: "test_flow".to_string(),
            cycle_guard: None,
            lineage: obzenflow_core::config::LineagePolicy::default(),
            effective_config: Arc::new(effective_config),
        }
    }

    #[test]
    fn rate_limiter_is_exclusively_hook_bound() {
        let factory = crate::middleware::control::rate_limit(10.0);
        let declaration = factory.declaration();
        assert!(declaration.is_control());
        assert!(!declaration.surfaces.is_empty());
    }

    #[test]
    fn test_rate_limiter_supported_stage_types_includes_join() {
        let factory = crate::middleware::control::rate_limit(10.0);
        let supported = factory.supported_stage_types();
        assert!(
            supported.contains(&StageType::Join),
            "FLOWIP-114m: Join must be a supported stage type for rate_limiter"
        );
        for expected in [
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
            StageType::Join,
        ] {
            assert!(
                supported.contains(&expected),
                "missing supported stage type: {expected:?}"
            );
        }
    }

    #[test]
    fn test_rate_limiter_declares_only_live_io_surfaces() {
        let factory = crate::middleware::control::rate_limit(100.0).burst_capacity(500.0);
        let declaration = factory.declaration();
        assert!(declaration.is_control());
        assert!(!declaration
            .surfaces
            .contains(&MiddlewareSurfaceKind::Handler));
    }

    #[test]
    fn test_rate_limiter_builder_preserves_config() {
        let factory = rate_limit(100.0).burst_capacity(500.0).cost(2.0);

        assert_eq!(factory.events_per_second, 100.0);
        assert_eq!(factory.burst_capacity, Some(500.0));
        assert_eq!(factory.cost_per_attempt, 2.0);
    }

    #[test]
    fn dsl_defaults_and_consumption_preserve_optional_burst_semantics() {
        use obzenflow_runtime::runtime_config::{
            RATE_LIMITER_BURST_CAPACITY_KEY, RATE_LIMITER_COST_PER_ATTEMPT_KEY,
            RATE_LIMITER_EVENTS_PER_SECOND_KEY,
        };

        let implicit_factory = crate::middleware::control::rate_limit(10.0);
        let implicit_defaults = implicit_factory.dsl_config_defaults();
        let implicit_consumed: BTreeSet<_> = implicit_factory
            .consumed_config_keys()
            .into_iter()
            .collect();
        assert_eq!(implicit_defaults.len(), 2);
        assert_eq!(
            implicit_defaults[0].key_path,
            RATE_LIMITER_EVENTS_PER_SECOND_KEY
        );
        assert_eq!(
            implicit_consumed,
            BTreeSet::from([
                RATE_LIMITER_EVENTS_PER_SECOND_KEY,
                RATE_LIMITER_BURST_CAPACITY_KEY,
                RATE_LIMITER_COST_PER_ATTEMPT_KEY,
            ])
        );
        assert!(implicit_defaults
            .iter()
            .all(|default| implicit_consumed.contains(default.key_path)));

        let explicit_factory = crate::middleware::control::rate_limit(10.0).burst_capacity(25.0);
        let explicit_defaults = explicit_factory.dsl_config_defaults();
        let explicit_consumed: BTreeSet<_> = explicit_factory
            .consumed_config_keys()
            .into_iter()
            .collect();
        assert_eq!(explicit_defaults.len(), 3);
        assert!(explicit_defaults.iter().any(|default| {
            default.key_path == RATE_LIMITER_BURST_CAPACITY_KEY
                && default.value.as_f64() == Some(25.0)
        }));
        assert!(explicit_defaults
            .iter()
            .all(|default| explicit_consumed.contains(default.key_path)));

        let boxed: Box<dyn MiddlewareFactory> = Box::new(implicit_factory);
        assert_eq!(
            boxed
                .consumed_config_keys()
                .into_iter()
                .collect::<BTreeSet<_>>(),
            implicit_consumed,
            "boxed forwarding must preserve the concrete factory's optional-key override"
        );
    }

    #[test]
    fn test_rate_limit_helpers_use_builder_defaults() {
        assert_eq!(
            rate_limit(25.0).config_snapshot(),
            Some(json!({
                "tokens_per_sec": 25.0,
                "burst_capacity": 25.0,
                "cost_per_event": 1.0,
                "limit_rate": 25.0,
            }))
        );
        assert_eq!(
            rate_limit(25.0).burst_capacity(50.0).config_snapshot(),
            Some(json!({
                "tokens_per_sec": 25.0,
                "burst_capacity": 50.0,
                "configured_burst_capacity": 50.0,
                "cost_per_event": 1.0,
                "limit_rate": 25.0,
            }))
        );
    }

    #[test]
    fn test_rate_limiter_rejects_zero_rate() {
        let err = crate::middleware::control::rate_limit(0.0)
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("events_per_second"));
    }

    #[test]
    fn test_rate_limiter_rejects_negative_rate() {
        let err = crate::middleware::control::rate_limit(-1.0)
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("events_per_second"));
    }

    #[test]
    fn test_rate_limiter_rejects_zero_cost() {
        let err = crate::middleware::control::rate_limit(10.0)
            .cost(0.0)
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("cost_per_attempt"));
    }

    #[test]
    fn test_rate_limiter_rejects_non_finite_values() {
        let inf_err = crate::middleware::control::rate_limit(f64::INFINITY)
            .validate()
            .unwrap_err();
        assert!(inf_err.to_string().contains("events_per_second"));

        let nan_err = crate::middleware::control::rate_limit(10.0)
            .cost(f64::NAN)
            .validate()
            .unwrap_err();
        assert!(nan_err.to_string().contains("cost_per_attempt"));
    }

    #[test]
    fn test_rate_limiter_rejects_explicit_burst_smaller_than_cost() {
        let err = crate::middleware::control::rate_limit(10.0)
            .burst_capacity(2.0)
            .cost(5.0)
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("burst_capacity"));
        assert!(err.to_string().contains("cost_per_attempt"));
    }

    #[test]
    fn test_rate_limiter_config_snapshot_uses_effective_capacity_for_low_rates() {
        let snapshot = crate::middleware::control::rate_limit(0.5)
            .config_snapshot()
            .expect("valid limiter snapshot");
        assert_eq!(snapshot["burst_capacity"], json!(1.0));
        assert_eq!(snapshot["cost_per_event"], json!(1.0));
        assert_eq!(snapshot["limit_rate"], json!(0.5));
        assert!(snapshot.get("configured_burst_capacity").is_none());
    }

    #[test]
    fn test_rate_limiter_config_snapshot_exposes_weighted_effective_fields() {
        let snapshot = crate::middleware::control::rate_limit(2.0)
            .cost(5.0)
            .config_snapshot()
            .expect("valid limiter snapshot");
        assert_eq!(snapshot["tokens_per_sec"], json!(2.0));
        assert_eq!(snapshot["burst_capacity"], json!(5.0));
        assert_eq!(snapshot["cost_per_event"], json!(5.0));
        assert_eq!(snapshot["limit_rate"], json!(0.4));
    }

    /// FLOWIP-115d: the rate limiter materialized onto the `Ingress` surface
    /// admits while the bucket has tokens and then fails fast with a
    /// `RateLimited` reject once the bucket is exhausted, never waiting.
    #[test]
    fn rate_limiter_ingress_admits_then_rejects_fail_fast() {
        use crate::middleware::{
            HostedIngressTargetKey, IngressRouteScope, IngressSurface, IngressUnitId,
            MiddlewareAttachmentRequest, MiddlewareAttachmentSite, ProtectedUnit, ProtectedUnitId,
            SourceStageIngressOwner,
        };
        use obzenflow_core::ingress::{
            IngressAdmissionDecision, IngressAttemptContext, IngressAttemptSeq, IngressKey,
        };
        use obzenflow_core::StageKey;

        let factory = crate::middleware::control::rate_limit(1.0);
        let control = Arc::new(ControlMiddlewareAggregator::new());
        let config = test_stage_config("accounts", &factory);
        let stage_key = StageKey("accounts".to_string());
        let target = HostedIngressTargetKey {
            surface: IngressKey("/api/bank/accounts".to_string()),
            scope: IngressRouteScope::Admission,
        };
        let surface = MiddlewareSurface::Ingress(IngressSurface {
            owner: SourceStageIngressOwner {
                stage_id: config.stage_id,
                stage_key: stage_key.clone(),
            },
            target: target.clone(),
        });
        let unit = ProtectedUnitId {
            stage_id: config.stage_id,
            unit: ProtectedUnit::Ingress(IngressUnitId {
                source_stage_key: stage_key,
                target,
            }),
        };
        let request = MiddlewareAttachmentRequest {
            stage_key: &config.name,
            surface: &surface,
            protected_unit: &unit,
            authored_site: MiddlewareAttachmentSite::Implementation,
        };
        // Burst capacity 1 (events_per_second defaults the burst), 1 event/sec.
        let boundary = materialize_factory_checked(
            &factory,
            request,
            &config,
            StageType::InfiniteSource,
            &control,
        )
        .expect("ingress materialize")
        .into_ingress()
        .expect("expected an Ingress attachment");

        let attempt = IngressAttemptContext {
            attempt_seq: IngressAttemptSeq(0),
            request_count: 1,
            event_count: 1,
            batch_count: 0,
        };
        assert!(
            matches!(
                boundary.on_ingress(&attempt),
                IngressAdmissionDecision::Accept
            ),
            "the burst token admits the first attempt"
        );
        assert!(
            matches!(
                boundary.on_ingress(&attempt),
                IngressAdmissionDecision::Reject { .. }
            ),
            "an exhausted bucket fails fast with a rate-limited reject, never waiting"
        );
    }
}
