// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115d: rate-limiter builder, factory, and public constructors.
//!
//! [`RateLimiterFactory::declaration`] plus [`RateLimiterFactory::materialize`]
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
    validate_attachment_request, EffectPolicyAttachment, MiddlewareAttachmentRequest,
    MiddlewareDeclaration, MiddlewareFactory, MiddlewareFactoryError,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety, MiddlewareSurface,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind, SinkPolicy, SourcePolicy,
    SourcePollAttachment, TopologyMiddlewareConfigSlot,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::ingress::IngressBoundaryMiddleware;
use std::sync::Arc;

/// Inert per-attempt limiter configuration owned by `EffectResilience`.
#[derive(Debug, Clone)]
pub struct RateLimiter {
    pub(in crate::middleware::control) events_per_second: f64,
    pub(in crate::middleware::control) burst_capacity: Option<f64>,
    pub(in crate::middleware::control) cost_per_attempt: f64,
}

impl RateLimiter {
    pub fn per_second(events_per_second: f64) -> Result<Self, RateLimiterConfigError> {
        let value = Self {
            events_per_second,
            burst_capacity: None,
            cost_per_attempt: DEFAULT_COST_PER_EVENT,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn with_burst(mut self, capacity: f64) -> Result<Self, RateLimiterConfigError> {
        self.burst_capacity = Some(capacity);
        self.validate()?;
        Ok(self)
    }

    pub fn with_cost_per_attempt(mut self, cost: f64) -> Result<Self, RateLimiterConfigError> {
        self.cost_per_attempt = cost;
        self.validate()?;
        Ok(self)
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
}

/// Builder for constructing rate limiter middleware factories.
///
/// Every factory built here produces independent per-attachment buckets; see the
/// module docs for the no-global-bucket decision (FLOWIP-114o Q6), the
/// async-await vs blocking wait by placement, and the admission accounting
/// contract.
#[derive(Clone)]
pub struct RateLimiterBuilder {
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
}

impl RateLimiterBuilder {
    /// Create a basic rate limiter builder.
    pub fn new(events_per_second: f64) -> Self {
        Self {
            events_per_second,
            burst_capacity: None,
            cost_per_event: DEFAULT_COST_PER_EVENT,
        }
    }

    /// Set burst capacity (defaults to `events_per_second`).
    pub fn with_burst(mut self, capacity: f64) -> Self {
        self.burst_capacity = Some(capacity);
        self
    }

    /// Set cost per event (for weighted rate limiting).
    pub fn with_cost_per_event(mut self, cost: f64) -> Self {
        self.cost_per_event = cost;
        self
    }

    /// Backward-compatible alias for `with_cost_per_event`.
    pub fn with_cost(self, cost: f64) -> Self {
        self.with_cost_per_event(cost)
    }

    /// Build the boxed middleware factory.
    pub fn build(self) -> Box<dyn MiddlewareFactory> {
        Box::new(RateLimiterFactory::from(self))
    }
}

/// Factory for creating rate limiter middleware.
///
/// `RateLimiterFactory` remains available for direct construction and testing,
/// but caller-facing configuration should prefer `RateLimiterBuilder`.
#[derive(Clone)]
pub struct RateLimiterFactory {
    events_per_second: f64,
    burst_capacity: Option<f64>,
    cost_per_event: f64,
}

impl RateLimiterFactory {
    /// Create a basic rate limiter
    pub fn new(events_per_second: f64) -> Self {
        Self {
            events_per_second,
            burst_capacity: None,
            cost_per_event: DEFAULT_COST_PER_EVENT,
        }
    }

    /// Set burst capacity (defaults to events_per_second)
    pub fn with_burst(mut self, capacity: f64) -> Self {
        self.burst_capacity = Some(capacity);
        self
    }

    /// Set cost per event (for weighted rate limiting).
    pub fn with_cost_per_event(mut self, cost: f64) -> Self {
        self.cost_per_event = cost;
        self
    }

    /// Backward-compatible alias for `with_cost_per_event`.
    pub fn with_cost(self, cost: f64) -> Self {
        self.with_cost_per_event(cost)
    }

    fn validated_config(&self) -> Result<ValidatedRateLimiterConfig, RateLimiterConfigError> {
        validated_rate_limiter_config(
            self.events_per_second,
            self.burst_capacity,
            self.cost_per_event,
        )
    }
}

impl From<RateLimiterBuilder> for RateLimiterFactory {
    fn from(builder: RateLimiterBuilder) -> Self {
        Self {
            events_per_second: builder.events_per_second,
            burst_capacity: builder.burst_capacity,
            cost_per_event: builder.cost_per_event,
        }
    }
}

impl MiddlewareFactory for RateLimiterFactory {
    fn label(&self) -> &'static str {
        "rate_limiter"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<RateLimiterFamily>("rate_limiter")
    }

    fn dsl_config_defaults(&self) -> Vec<obzenflow_runtime::runtime_config::DslConfigDefault> {
        use obzenflow_runtime::runtime_config::{
            ConfigValue, DslConfigDefault, RATE_LIMITER_BURST_CAPACITY_KEY,
            RATE_LIMITER_EVENTS_PER_SECOND_KEY,
        };
        let mut defaults = vec![DslConfigDefault {
            key_path: RATE_LIMITER_EVENTS_PER_SECOND_KEY,
            value: ConfigValue::F64(self.events_per_second),
        }];
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

        let config_view = context.config_view(request.protected_unit);
        let events_per_second = config_view
            .get(obzenflow_runtime::runtime_config::RATE_LIMITER_EVENTS_PER_SECOND_KEY)
            .and_then(|resolved| resolved.value.as_f64())
            .ok_or_else(|| {
                MiddlewareFactoryError::invalid_configuration(
                    self.label(),
                    &context.config.name,
                    std::io::Error::other(
                        "resolved rate-limiter events_per_second is missing at the protected unit",
                    ),
                )
            })?;
        let burst_capacity = config_view
            .get(obzenflow_runtime::runtime_config::RATE_LIMITER_BURST_CAPACITY_KEY)
            .and_then(|resolved| resolved.value.as_f64());
        let validated =
            validated_rate_limiter_config(events_per_second, burst_capacity, self.cost_per_event)
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
                        context.control_middleware().clone(),
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
                Ok(MiddlewareSurfaceAttachment::SourcePoll(
                    SourcePollAttachment {
                        policy,
                        completion_gate: None,
                    },
                ))
            }
            MiddlewareSurface::Effect(effect_surface) => {
                // FLOWIP-120c: one limiter instance guards one declared effect,
                // registered under the per-effect key for metrics.
                let middleware = RateLimiterMiddleware::new_keyed(
                    context.config.stage_id,
                    validated,
                    context.control_middleware().clone(),
                    Some(effect_surface.effect_type.clone()),
                )
                .map_err(|message| {
                    MiddlewareFactoryError::invalid_configuration(
                        self.label(),
                        &context.config.name,
                        std::io::Error::other(message),
                    )
                })?;
                Ok(MiddlewareSurfaceAttachment::Effect(
                    EffectPolicyAttachment::neutral(Arc::new(middleware)),
                ))
            }
            MiddlewareSurface::SinkDelivery(_) => {
                let middleware = Arc::new(
                    RateLimiterMiddleware::new(
                        context.config.stage_id,
                        validated,
                        context.control_middleware().clone(),
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
                Ok(MiddlewareSurfaceAttachment::SinkDelivery(policy))
            }
            MiddlewareSurface::Ingress(_) => {
                // FLOWIP-115d: source-backed hosted ingress. One core per hosted
                // protected unit; the adapter is fail-fast at the listener edge.
                let middleware = Arc::new(
                    RateLimiterMiddleware::new(
                        context.config.stage_id,
                        validated,
                        context.control_middleware().clone(),
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
                Ok(MiddlewareSurfaceAttachment::Ingress(policy))
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
        let validated = self.validated_config().ok()?;
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

/// Attach a token-bucket rate limiter admitting `events_per_second`.
///
/// Each attachment owns its own bucket: flow-level `rate_limit(N)` materialises
/// one instance per stage (FLOWIP-050d), and there is no process-wide shared
/// bucket (FLOWIP-114o Q6). On async sources and the effect boundary the limiter
/// awaits its permit as a cancellable future; on sync sources and handler chains
/// it blocks. Counters increment at admission with no refund on a downstream
/// `Skip`/`Abort` (FLOWIP-114m, FLOWIP-114o). See the module docs for the full
/// wait, bucket, and accounting contract.
pub fn rate_limit(events_per_second: f64) -> Box<dyn MiddlewareFactory> {
    RateLimiterBuilder::new(events_per_second).build()
}

/// Attach a rate limiter with an explicit burst capacity. Same per-instance
/// bucket, wait, and accounting contract as [`rate_limit`].
pub fn rate_limit_with_burst(events_per_second: f64, burst: f64) -> Box<dyn MiddlewareFactory> {
    RateLimiterBuilder::new(events_per_second)
        .with_burst(burst)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::control::ControlMiddlewareAggregator;
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
        let factory = RateLimiterFactory::new(10.0);
        let declaration = factory.declaration();
        assert!(declaration.is_control());
        assert!(!declaration.surfaces.is_empty());
        assert!(!declaration.is_flowip_128g_legacy_shell());
    }

    #[test]
    fn test_rate_limiter_supported_stage_types_includes_join() {
        let factory = RateLimiterFactory::new(10.0);
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
        let factory = RateLimiterFactory::new(100.0).with_burst(500.0);
        let declaration = factory.declaration();
        assert!(declaration.is_control());
        assert!(!declaration
            .surfaces
            .contains(&MiddlewareSurfaceKind::Handler));
    }

    #[test]
    fn test_rate_limiter_builder_preserves_config() {
        let factory = RateLimiterFactory::from(
            RateLimiterBuilder::new(100.0)
                .with_burst(500.0)
                .with_cost_per_event(2.0),
        );

        assert_eq!(factory.events_per_second, 100.0);
        assert_eq!(factory.burst_capacity, Some(500.0));
        assert_eq!(factory.cost_per_event, 2.0);
    }

    #[test]
    fn dsl_defaults_preserve_optional_burst_semantics() {
        use obzenflow_runtime::runtime_config::{
            RATE_LIMITER_BURST_CAPACITY_KEY, RATE_LIMITER_EVENTS_PER_SECOND_KEY,
        };

        let implicit = RateLimiterFactory::new(10.0).dsl_config_defaults();
        assert_eq!(implicit.len(), 1);
        assert_eq!(implicit[0].key_path, RATE_LIMITER_EVENTS_PER_SECOND_KEY);

        let explicit = RateLimiterFactory::new(10.0)
            .with_burst(25.0)
            .dsl_config_defaults();
        assert_eq!(explicit.len(), 2);
        assert!(explicit.iter().any(|default| {
            default.key_path == RATE_LIMITER_BURST_CAPACITY_KEY
                && default.value.as_f64() == Some(25.0)
        }));
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
            rate_limit_with_burst(25.0, 50.0).config_snapshot(),
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
        let err = RateLimiterFactory::new(0.0).validated_config().unwrap_err();
        assert!(err.to_string().contains("events_per_second"));
    }

    #[test]
    fn test_rate_limiter_rejects_negative_rate() {
        let err = RateLimiterFactory::new(-1.0)
            .validated_config()
            .unwrap_err();
        assert!(err.to_string().contains("events_per_second"));
    }

    #[test]
    fn test_rate_limiter_rejects_zero_cost() {
        let err = RateLimiterFactory::new(10.0)
            .with_cost_per_event(0.0)
            .validated_config()
            .unwrap_err();
        assert!(err.to_string().contains("cost_per_event"));
    }

    #[test]
    fn test_rate_limiter_rejects_non_finite_values() {
        let inf_err = RateLimiterFactory::new(f64::INFINITY)
            .validated_config()
            .unwrap_err();
        assert!(inf_err.to_string().contains("events_per_second"));

        let nan_err = RateLimiterFactory::new(10.0)
            .with_cost_per_event(f64::NAN)
            .validated_config()
            .unwrap_err();
        assert!(nan_err.to_string().contains("cost_per_event"));
    }

    #[test]
    fn test_rate_limiter_rejects_explicit_burst_smaller_than_cost() {
        let err = RateLimiterFactory::new(10.0)
            .with_burst(2.0)
            .with_cost_per_event(5.0)
            .validated_config()
            .unwrap_err();
        assert!(err.to_string().contains("burst_capacity"));
        assert!(err.to_string().contains("cost_per_event"));
    }

    #[test]
    fn test_rate_limiter_config_snapshot_uses_effective_capacity_for_low_rates() {
        let snapshot = RateLimiterFactory::new(0.5)
            .config_snapshot()
            .expect("valid low-rate config should expose a snapshot");
        assert_eq!(snapshot["burst_capacity"], json!(1.0));
        assert_eq!(snapshot["cost_per_event"], json!(1.0));
        assert_eq!(snapshot["limit_rate"], json!(0.5));
        assert!(snapshot.get("configured_burst_capacity").is_none());
    }

    #[test]
    fn test_rate_limiter_config_snapshot_exposes_weighted_effective_fields() {
        let snapshot = RateLimiterFactory::new(2.0)
            .with_cost_per_event(5.0)
            .config_snapshot()
            .expect("valid weighted config should expose a snapshot");
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
            MiddlewareAttachmentRequest, MiddlewareDeclarationIndex,
            MiddlewareMaterializationContext, MiddlewareOrigin, ProtectedUnit, ProtectedUnitId,
            SourceStageIngressOwner,
        };
        use obzenflow_core::ingress::{
            IngressAdmissionDecision, IngressAttemptContext, IngressAttemptSeq, IngressKey,
        };
        use obzenflow_core::StageKey;

        let factory = RateLimiterFactory::new(1.0);
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
        let origin = MiddlewareOrigin::Stage;
        let request = MiddlewareAttachmentRequest {
            surface: &surface,
            protected_unit: &unit,
            origin: &origin,
            declaration_index: MiddlewareDeclarationIndex::resolved(0),
        };
        let ctx =
            MiddlewareMaterializationContext::new(&config, &control, StageType::InfiniteSource);

        // Burst capacity 1 (events_per_second defaults the burst), 1 event/sec.
        let boundary = match factory
            .materialize(request, &ctx)
            .expect("ingress materialize")
        {
            MiddlewareSurfaceAttachment::Ingress(boundary) => boundary,
            _ => panic!("expected an Ingress attachment"),
        };

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
