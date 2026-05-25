// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware resolution with full tracking for observability
//!
//! This module implements the SRE-first approach to middleware inheritance,
//! tracking every decision for debugging and operational visibility.

use indexmap::IndexMap;
use obzenflow_adapters::middleware::{
    ControlMiddlewareRole, MiddlewareFactory, MiddlewareOverrideKey,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Tracks middleware origin for debugging and audit
pub struct MiddlewareSpec {
    pub factory: Box<dyn MiddlewareFactory>,
    pub source: MiddlewareSource,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MiddlewareSource {
    /// Middleware came from flow-level configuration
    Flow,
    /// Middleware came from stage-level configuration
    Stage,
    /// Stage middleware that overrides flow middleware
    StageOverride {
        family_label: String,
        flow_label: String,
        stage_label: String,
        overrode_config: String,
    },
}

/// Middleware resolution result with full audit trail
pub struct ResolvedMiddleware {
    /// Final middleware list in execution order
    pub middleware: Vec<MiddlewareSpec>,
    /// What was overridden
    pub overrides: Vec<OverrideRecord>,
    /// Configuration warnings
    pub warnings: Vec<ConfigWarning>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OverrideRecord {
    pub middleware_type: String,
    pub flow_config: String,
    pub stage_config: String,
    pub reason: OverrideReason,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OverrideReason {
    /// Stage explicitly specified same middleware type
    ExplicitOverride,
    /// Stage used disable() to remove flow middleware
    ExplicitDisable,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigWarning {
    pub level: WarnLevel,
    pub message: String,
    pub suggestion: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WarnLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareSourceScope {
    Flow,
    Stage,
}

#[derive(Debug, Error)]
pub enum MiddlewareResolutionError {
    #[error(
        "Duplicate middleware override family '{family_label}' in {scope:?} scope for stage '{stage_name}': '{first_label}' and '{second_label}'"
    )]
    DuplicateOverrideKey {
        stage_name: String,
        scope: MiddlewareSourceScope,
        family_label: &'static str,
        first_label: &'static str,
        second_label: &'static str,
    },
}

/// Resolve a stage's effective middleware list without taking ownership.
///
/// This is used by topology/plan extraction code paths that need the resolved
/// view but do not (and cannot) move the factories out of their owning
/// stage descriptors.
pub fn resolve_middleware_view<'a>(
    flow_middleware: &'a [Box<dyn MiddlewareFactory>],
    stage_middleware: &'a [Box<dyn MiddlewareFactory>],
    stage_name: &str,
) -> Result<Vec<&'a dyn MiddlewareFactory>, MiddlewareResolutionError> {
    let mut resolved: IndexMap<
        MiddlewareOverrideKey,
        (MiddlewareSourceScope, &'a dyn MiddlewareFactory),
    > = IndexMap::new();

    for factory in flow_middleware {
        let key = factory.override_key();
        if let Some((_, existing)) = resolved.get(&key) {
            return Err(MiddlewareResolutionError::DuplicateOverrideKey {
                stage_name: stage_name.to_string(),
                scope: MiddlewareSourceScope::Flow,
                family_label: key.family_label(),
                first_label: existing.label(),
                second_label: factory.label(),
            });
        }
        resolved.insert(key, (MiddlewareSourceScope::Flow, factory.as_ref()));
    }

    for factory in stage_middleware {
        let key = factory.override_key();
        if let Some((scope, existing)) = resolved.get(&key) {
            if *scope == MiddlewareSourceScope::Stage {
                return Err(MiddlewareResolutionError::DuplicateOverrideKey {
                    stage_name: stage_name.to_string(),
                    scope: MiddlewareSourceScope::Stage,
                    family_label: key.family_label(),
                    first_label: existing.label(),
                    second_label: factory.label(),
                });
            }
        }

        resolved.insert(key, (MiddlewareSourceScope::Stage, factory.as_ref()));
    }

    Ok(resolved.into_values().map(|(_, factory)| factory).collect())
}

/// Pure function that resolves middleware with full tracking
pub fn resolve_middleware(
    flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
    stage_middleware: Vec<Box<dyn MiddlewareFactory>>,
    stage_name: &str,
) -> Result<ResolvedMiddleware, MiddlewareResolutionError> {
    let mut resolved: IndexMap<MiddlewareOverrideKey, MiddlewareSpec> = IndexMap::new();
    let mut overrides = Vec::new();
    let mut warnings = Vec::new();

    // Phase 1: Add flow middleware
    for factory in flow_middleware {
        let key = factory.override_key();
        if let Some(existing) = resolved.get(&key) {
            return Err(MiddlewareResolutionError::DuplicateOverrideKey {
                stage_name: stage_name.to_string(),
                scope: MiddlewareSourceScope::Flow,
                family_label: key.family_label(),
                first_label: existing.factory.label(),
                second_label: factory.label(),
            });
        }
        let spec = MiddlewareSpec {
            source: MiddlewareSource::Flow,
            factory,
        };
        resolved.insert(key, spec);
    }

    // Phase 2: Apply stage middleware
    for factory in stage_middleware {
        let key = factory.override_key();

        // Same-scope duplicates (stage list) are configuration errors.
        if let Some(existing) = resolved.get(&key) {
            if matches!(
                existing.source,
                MiddlewareSource::Stage | MiddlewareSource::StageOverride { .. }
            ) {
                return Err(MiddlewareResolutionError::DuplicateOverrideKey {
                    stage_name: stage_name.to_string(),
                    scope: MiddlewareSourceScope::Stage,
                    family_label: key.family_label(),
                    first_label: existing.factory.label(),
                    second_label: factory.label(),
                });
            }
        }

        let source = if let Some(existing) = resolved.get(&key) {
            // Existing is flow-level: record override audit and check override safety.
            overrides.push(OverrideRecord {
                middleware_type: key.family_label().to_string(),
                flow_config: format_factory_config(&existing.factory),
                stage_config: format_factory_config(&factory),
                reason: OverrideReason::ExplicitOverride,
            });

            if let Some(warning) = check_override_safety(&existing.factory, &factory) {
                warnings.push(warning);
            }

            MiddlewareSource::StageOverride {
                family_label: key.family_label().to_string(),
                flow_label: existing.factory.label().to_string(),
                stage_label: factory.label().to_string(),
                overrode_config: format_factory_config(&existing.factory),
            }
        } else {
            MiddlewareSource::Stage
        };

        resolved.insert(key, MiddlewareSpec { source, factory });
    }

    // Phase 3: Validate final configuration
    warnings.extend(validate_middleware_combination(&resolved, stage_name));

    Ok(ResolvedMiddleware {
        middleware: resolved.into_values().collect(),
        overrides,
        warnings,
    })
}

#[cfg(test)]
mod view_tests {
    use super::*;
    use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
    use obzenflow_adapters::middleware::{
        ControlMiddlewareRole, Middleware, MiddlewareAction, MiddlewareContext,
        MiddlewareFactoryResult, MiddlewareHints, MiddlewarePlanContribution, MiddlewareSafety,
        SourceMiddlewarePhase, TopologyMiddlewareConfigSlot,
    };
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::ChainEvent;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use std::num::NonZeroU64;
    use std::sync::Arc;

    struct DummyMiddleware;
    impl Middleware for DummyMiddleware {
        fn label(&self) -> &'static str {
            "dummy"
        }

        fn source_phase(&self) -> SourceMiddlewarePhase {
            SourceMiddlewarePhase::Ordinary
        }

        fn pre_handle(
            &self,
            _event: &ChainEvent,
            _ctx: &mut MiddlewareContext,
        ) -> MiddlewareAction {
            MiddlewareAction::Continue
        }
    }

    struct FamilyA;
    struct FamilyB;
    struct BackpressureFamily;

    struct MockFactory {
        label: &'static str,
        key: MiddlewareOverrideKey,
        control_role: ControlMiddlewareRole,
        plan: MiddlewarePlanContribution,
    }

    impl MiddlewareFactory for MockFactory {
        fn label(&self) -> &'static str {
            self.label
        }

        fn override_key(&self) -> MiddlewareOverrideKey {
            self.key
        }

        fn control_role(&self) -> ControlMiddlewareRole {
            self.control_role
        }

        fn plan_contribution(&self) -> MiddlewarePlanContribution {
            self.plan.clone()
        }

        fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
            None
        }

        fn create(
            &self,
            _config: &StageConfig,
            _control_middleware: Arc<ControlMiddlewareAggregator>,
        ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
            Ok(Box::new(DummyMiddleware))
        }

        fn supported_stage_types(&self) -> &[StageType] {
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
            MiddlewareSafety::Safe
        }

        fn hints(&self) -> MiddlewareHints {
            MiddlewareHints::default()
        }
    }

    fn backpressure_factory(label: &'static str, window: u64) -> Box<dyn MiddlewareFactory> {
        Box::new(MockFactory {
            label,
            key: MiddlewareOverrideKey::of::<BackpressureFamily>("backpressure"),
            control_role: ControlMiddlewareRole::None,
            plan: MiddlewarePlanContribution::Backpressure {
                window: NonZeroU64::new(window).expect("window > 0"),
            },
        })
    }

    #[test]
    fn resolve_middleware_view_errors_on_same_scope_duplicates() {
        let flow = vec![
            Box::new(MockFactory {
                label: "flow.a1",
                key: MiddlewareOverrideKey::of::<FamilyA>("a"),
                control_role: ControlMiddlewareRole::None,
                plan: MiddlewarePlanContribution::None,
            }) as Box<dyn MiddlewareFactory>,
            Box::new(MockFactory {
                label: "flow.a2",
                key: MiddlewareOverrideKey::of::<FamilyA>("a"),
                control_role: ControlMiddlewareRole::None,
                plan: MiddlewarePlanContribution::None,
            }) as Box<dyn MiddlewareFactory>,
        ];

        let err = resolve_middleware_view(&flow, &[], "stage")
            .err()
            .expect("expected duplicate override key error");
        match err {
            MiddlewareResolutionError::DuplicateOverrideKey { scope, .. } => {
                assert_eq!(scope, MiddlewareSourceScope::Flow);
            }
        }

        let stage = vec![
            Box::new(MockFactory {
                label: "stage.b1",
                key: MiddlewareOverrideKey::of::<FamilyB>("b"),
                control_role: ControlMiddlewareRole::None,
                plan: MiddlewarePlanContribution::None,
            }) as Box<dyn MiddlewareFactory>,
            Box::new(MockFactory {
                label: "stage.b2",
                key: MiddlewareOverrideKey::of::<FamilyB>("b"),
                control_role: ControlMiddlewareRole::None,
                plan: MiddlewarePlanContribution::None,
            }) as Box<dyn MiddlewareFactory>,
        ];

        let err = resolve_middleware_view(&[], &stage, "stage")
            .err()
            .expect("expected duplicate override key error");
        match err {
            MiddlewareResolutionError::DuplicateOverrideKey { scope, .. } => {
                assert_eq!(scope, MiddlewareSourceScope::Stage);
            }
        }
    }

    #[test]
    fn backpressure_plan_contribution_is_extracted_from_resolved_view() {
        // Label intentionally does NOT contain "backpressure" so this would fail under string dispatch.
        let flow = vec![backpressure_factory("opaque.bp", 10)];
        let resolved = resolve_middleware_view(&flow, &[], "stage").expect("resolve view");

        let windows: Vec<_> = resolved
            .iter()
            .filter_map(|f| match f.plan_contribution() {
                MiddlewarePlanContribution::Backpressure { window } => Some(window.get()),
                _ => None,
            })
            .collect();

        assert_eq!(windows, vec![10]);
    }

    #[test]
    fn backpressure_stage_override_wins_over_flow_level() {
        let flow = vec![backpressure_factory("flow.bp", 10)];
        let stage = vec![backpressure_factory("stage.renamed", 5)];

        let resolved = resolve_middleware_view(&flow, &stage, "stage").expect("resolve view");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].label(), "stage.renamed");

        match resolved[0].plan_contribution() {
            MiddlewarePlanContribution::Backpressure { window } => assert_eq!(window.get(), 5),
            other => panic!("expected Backpressure plan contribution, got {other:?}"),
        }
    }

    #[test]
    fn stage_override_affects_only_its_own_stage() {
        // A flow-level backpressure family resolves onto every stage independently.
        let flow = vec![backpressure_factory("flow.bp", 10)];

        // Stage A overrides the same typed family; stage B declares no stage middleware.
        let stage_a = vec![backpressure_factory("stage_a.bp", 5)];

        let resolved_a = resolve_middleware_view(&flow, &stage_a, "stage_a").expect("resolve A");
        let resolved_b = resolve_middleware_view(&flow, &[], "stage_b").expect("resolve B");

        // Stage A sees its override.
        assert_eq!(resolved_a.len(), 1);
        assert_eq!(resolved_a[0].label(), "stage_a.bp");
        match resolved_a[0].plan_contribution() {
            MiddlewarePlanContribution::Backpressure { window } => assert_eq!(window.get(), 5),
            other => panic!("expected Backpressure plan contribution, got {other:?}"),
        }

        // Stage B is untouched: the flow-level contributor resolves independently for it.
        assert_eq!(resolved_b.len(), 1);
        assert_eq!(resolved_b[0].label(), "flow.bp");
        match resolved_b[0].plan_contribution() {
            MiddlewarePlanContribution::Backpressure { window } => assert_eq!(window.get(), 10),
            other => panic!("expected Backpressure plan contribution, got {other:?}"),
        }
    }

    #[test]
    fn typed_override_key_ignores_diagnostic_family_label() {
        let flow = vec![Box::new(MockFactory {
            label: "flow.a",
            key: MiddlewareOverrideKey::of::<FamilyA>("flow.family"),
            control_role: ControlMiddlewareRole::None,
            plan: MiddlewarePlanContribution::None,
        }) as Box<dyn MiddlewareFactory>];
        let stage = vec![Box::new(MockFactory {
            label: "stage.a",
            key: MiddlewareOverrideKey::of::<FamilyA>("stage.family"),
            control_role: ControlMiddlewareRole::None,
            plan: MiddlewarePlanContribution::None,
        }) as Box<dyn MiddlewareFactory>];

        let resolved = resolve_middleware_view(&flow, &stage, "stage").expect("resolve view");
        assert_eq!(resolved.len(), 1);
        assert_eq!(resolved[0].label(), "stage.a");
    }
}

/// Format factory configuration for display
fn format_factory_config(factory: &dyn MiddlewareFactory) -> String {
    match factory.config_snapshot() {
        Some(snapshot) => format!("{}({snapshot})", factory.label()),
        None => format!("{}(...)", factory.label()),
    }
}

/// Check if an override might cause operational issues
fn check_override_safety(
    flow_mw: &dyn MiddlewareFactory,
    stage_mw: &dyn MiddlewareFactory,
) -> Option<ConfigWarning> {
    let family_label = stage_mw.override_key().family_label();
    let flow_label = flow_mw.label();
    let stage_label = stage_mw.label();

    let role = stage_mw.control_role();
    let stage_hints = stage_mw.hints();

    if role == ControlMiddlewareRole::RateLimiter || stage_hints.rate_limits {
        return Some(ConfigWarning {
            level: WarnLevel::Medium,
            message: format!(
                "Stage overrides flow rate limiting (override family '{family_label}'): flow='{flow_label}', stage='{stage_label}'"
            ),
            suggestion: Some("Verify this stage needs different rate limiting".to_string()),
        });
    }

    if role == ControlMiddlewareRole::CircuitBreaker {
        return Some(ConfigWarning {
            level: WarnLevel::Medium,
            message: format!(
                "Stage overrides flow circuit breaker (override family '{family_label}'): flow='{flow_label}', stage='{stage_label}'"
            ),
            suggestion: Some("Verify this stage needs different circuit breaker settings".to_string()),
        });
    }

    None
}

/// Validate the final middleware combination for potential issues
fn validate_middleware_combination(
    resolved: &IndexMap<MiddlewareOverrideKey, MiddlewareSpec>,
    stage_name: &str,
) -> Vec<ConfigWarning> {
    let mut warnings = Vec::new();

    // Check for potentially conflicting middleware combinations based on typed hints/roles.
    let has_retry = resolved
        .values()
        .any(|spec| spec.factory.hints().retry.is_some());
    let has_circuit_breaker = resolved
        .values()
        .any(|spec| spec.factory.control_role() == ControlMiddlewareRole::CircuitBreaker);

    if has_retry && !has_circuit_breaker {
        warnings.push(ConfigWarning {
            level: WarnLevel::Low,
            message: format!(
                "Stage '{stage_name}' has retry behaviour without a circuit breaker; this can cause cascading failures"
            ),
            suggestion: Some("Consider adding circuit breaker middleware".to_string()),
        });
    }

    warnings
}

/// Log resolved middleware configuration for debugging
pub fn log_resolved_middleware(stage_name: &str, resolved: &ResolvedMiddleware) {
    tracing::info!(
        stage = stage_name,
        middleware_count = resolved.middleware.len(),
        override_count = resolved.overrides.len(),
        warning_count = resolved.warnings.len(),
        "Resolved middleware configuration"
    );

    // Log each middleware with its source
    for spec in &resolved.middleware {
        tracing::debug!(
            stage = stage_name,
            middleware_type = spec.factory.label(),
            source = ?spec.source,
            "Middleware configured"
        );
    }

    // Log overrides
    for override_record in &resolved.overrides {
        tracing::info!(
            stage = stage_name,
            middleware_type = &override_record.middleware_type,
            flow_config = &override_record.flow_config,
            stage_config = &override_record.stage_config,
            "Middleware override detected"
        );
    }

    // Log warnings
    for warning in &resolved.warnings {
        match warning.level {
            WarnLevel::High => tracing::warn!(
                stage = stage_name,
                message = &warning.message,
                suggestion = ?warning.suggestion,
                "High priority configuration warning"
            ),
            WarnLevel::Medium => tracing::info!(
                stage = stage_name,
                message = &warning.message,
                suggestion = ?warning.suggestion,
                "Medium priority configuration warning"
            ),
            WarnLevel::Low => tracing::debug!(
                stage = stage_name,
                message = &warning.message,
                suggestion = ?warning.suggestion,
                "Low priority configuration warning"
            ),
        }
    }
}

#[cfg(test)]
mod tests {

    // TODO: Add tests once we have mock MiddlewareFactory implementations
}
