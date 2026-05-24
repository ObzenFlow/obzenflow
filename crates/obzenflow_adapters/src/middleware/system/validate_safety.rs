// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Safety validation for middleware combinations
//!
//! This module provides utilities for validating middleware safety when
//! constructing pipelines, issuing warnings for dangerous combinations.

use crate::middleware::{MiddlewareFactory, MiddlewareSafety};
use obzenflow_core::event::context::StageType;
use tracing::{error, warn};

/// Validates middleware safety for a given stage type
pub fn validate_middleware_safety(
    factory: &dyn MiddlewareFactory,
    stage_type: StageType,
    stage_name: &str,
) -> ValidationResult {
    let mut result = ValidationResult::default();

    // Check if middleware supports this stage type
    if !factory.supported_stage_types().contains(&stage_type) {
        let message = format!(
            "⚠️  Middleware '{}' does not officially support {} stages. \
             Applying to stage '{}' may have unexpected behavior!",
            factory.label(),
            format_stage_type(stage_type),
            stage_name
        );
        warn!("{}", message);
        result.warnings.push(message);
    }

    // Warn about dangerous middleware
    match factory.safety_level() {
        MiddlewareSafety::Dangerous => {
            let message = format!(
                "⚠️  DANGEROUS: Applying '{}' middleware to {} stage '{}'. \
                 This can cause data loss or pipeline hangs if not configured correctly!",
                factory.label(),
                format_stage_type(stage_type),
                stage_name
            );
            warn!("{}", message);
            result.warnings.push(message);
            result.has_dangerous = true;
        }
        MiddlewareSafety::Advanced => {
            let message = format!(
                "⚠️  ADVANCED: Middleware '{}' on stage '{}' requires careful configuration",
                factory.label(),
                stage_name
            );
            warn!("{}", message);
            result.warnings.push(message);
        }
        MiddlewareSafety::Safe => {
            // No warning needed
        }
    }

    // Check for specific dangerous patterns
    check_dangerous_patterns(factory, stage_type, stage_name, &mut result);

    result
}

/// Check for known dangerous middleware patterns
fn check_dangerous_patterns(
    factory: &dyn MiddlewareFactory,
    stage_type: StageType,
    stage_name: &str,
    result: &mut ValidationResult,
) {
    let hints = factory.hints();

    // Pattern 1: Skip control events on sinks
    if hints.drops_control_events && stage_type == StageType::Sink {
        let message = format!(
            "❌ CRITICAL: Never skip control events on sink '{stage_name}'! \
                 This will prevent proper draining and cause data loss!"
        );
        error!("{}", message);
        result.errors.push(message);
    }

    // Pattern 2: Infinite retry on sources
    if let Some(retry) = &hints.retry {
        use crate::middleware::Attempts;
        if matches!(retry.max_attempts, Attempts::Infinite) {
            match stage_type {
                StageType::FiniteSource | StageType::InfiniteSource => {
                    let message = format!(
                        "❌ CRITICAL: Infinite retry on source '{stage_name}' makes no sense! \
                         Sources generate data, they don't retry receiving it!"
                    );
                    error!("{}", message);
                    result.errors.push(message);
                }
                _ => {}
            }
        }
    }

    // Pattern 3: Unbounded batching on sinks
    if let Some(batch) = &hints.batching {
        if !batch.bounded && stage_type == StageType::Sink {
            let message = format!(
                "⚠️  WARNING: Unbounded batching on sink '{stage_name}' can cause data to be stuck! \
                 Make sure you have a timeout configured!"
            );
            warn!("{}", message);
            result.warnings.push(message);
        }
    }

    // Pattern 4: Rate limiting on sinks
    if hints.rate_limits && stage_type == StageType::Sink {
        let message = format!(
            "⚠️  WARNING: Rate limiting on sink '{stage_name}' can cause severe backpressure! \
             Make sure upstream stages can handle this!"
        );
        warn!("{}", message);
        result.warnings.push(message);
    }
}

/// Format stage type for user-friendly messages
fn format_stage_type(stage_type: StageType) -> &'static str {
    match stage_type {
        StageType::FiniteSource => "finite source",
        StageType::InfiniteSource => "infinite source",
        StageType::Transform => "transform",
        StageType::Sink => "sink",
        StageType::Stateful => "stateful",
        StageType::Join => "join",
    }
}

/// Result of middleware validation
#[derive(Debug, Default)]
pub struct ValidationResult {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub has_dangerous: bool,
}

impl ValidationResult {
    /// Check if validation passed (no errors)
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    /// Check if there are any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Get a summary of all issues
    pub fn summary(&self) -> String {
        let mut parts = Vec::new();

        if !self.errors.is_empty() {
            parts.push(format!("{} errors", self.errors.len()));
        }

        if !self.warnings.is_empty() {
            parts.push(format!("{} warnings", self.warnings.len()));
        }

        if parts.is_empty() {
            "No issues found".to_string()
        } else {
            parts.join(", ")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::middleware::{
        Attempts, BackoffKind, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory,
        BatchingHint, MiddlewareHints, RetryHint,
    };
    use obzenflow_core::ChainEvent;
    use obzenflow_runtime::pipeline::config::StageConfig;
    use std::time::Duration;

    // Mock middleware for testing dangerous patterns
    struct MockSkipControlMiddleware;
    impl Middleware for MockSkipControlMiddleware {
        fn label(&self) -> &'static str {
            "mock.skip_control_middleware"
        }

        fn source_phase(&self) -> crate::middleware::SourceMiddlewarePhase {
            crate::middleware::SourceMiddlewarePhase::Ordinary
        }

        fn pre_handle(
            &self,
            _event: &ChainEvent,
            _ctx: &mut MiddlewareContext,
        ) -> MiddlewareAction {
            MiddlewareAction::Continue
        }
    }

    struct MockSkipControlFamily;
    struct MockSkipControlFactory;
    impl MiddlewareFactory for MockSkipControlFactory {
        fn label(&self) -> &'static str {
            "opaque.drop_control"
        }

        fn override_key(&self) -> crate::middleware::MiddlewareOverrideKey {
            crate::middleware::MiddlewareOverrideKey::of::<MockSkipControlFamily>(
                "skip_control_events",
            )
        }

        fn control_role(&self) -> crate::middleware::ControlMiddlewareRole {
            crate::middleware::ControlMiddlewareRole::None
        }

        fn plan_contribution(&self) -> crate::middleware::MiddlewarePlanContribution {
            crate::middleware::MiddlewarePlanContribution::None
        }

        fn topology_config_slot(&self) -> Option<crate::middleware::TopologyMiddlewareConfigSlot> {
            None
        }

        fn create(
            &self,
            _config: &StageConfig,
            _control_middleware: std::sync::Arc<
                crate::middleware::control::ControlMiddlewareAggregator,
            >,
        ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
            Ok(Box::new(MockSkipControlMiddleware))
        }
        fn supported_stage_types(&self) -> &[StageType] {
            &[StageType::Sink, StageType::Transform]
        }
        fn safety_level(&self) -> MiddlewareSafety {
            MiddlewareSafety::Dangerous
        }
        fn hints(&self) -> MiddlewareHints {
            MiddlewareHints {
                drops_control_events: true,
                ..Default::default()
            }
        }
    }

    struct MockInfiniteRetryFamily;
    struct MockInfiniteRetryFactory;
    impl MiddlewareFactory for MockInfiniteRetryFactory {
        fn label(&self) -> &'static str {
            "opaque.infinite_attempts"
        }

        fn override_key(&self) -> crate::middleware::MiddlewareOverrideKey {
            crate::middleware::MiddlewareOverrideKey::of::<MockInfiniteRetryFamily>(
                "infinite_retry",
            )
        }

        fn control_role(&self) -> crate::middleware::ControlMiddlewareRole {
            crate::middleware::ControlMiddlewareRole::None
        }

        fn plan_contribution(&self) -> crate::middleware::MiddlewarePlanContribution {
            crate::middleware::MiddlewarePlanContribution::None
        }

        fn topology_config_slot(&self) -> Option<crate::middleware::TopologyMiddlewareConfigSlot> {
            None
        }

        fn create(
            &self,
            _config: &StageConfig,
            _control_middleware: std::sync::Arc<
                crate::middleware::control::ControlMiddlewareAggregator,
            >,
        ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
            Ok(Box::new(MockSkipControlMiddleware)) // Reuse the middleware impl
        }
        fn supported_stage_types(&self) -> &[StageType] {
            &[StageType::FiniteSource, StageType::InfiniteSource]
        }
        fn safety_level(&self) -> MiddlewareSafety {
            MiddlewareSafety::Dangerous
        }
        fn hints(&self) -> MiddlewareHints {
            MiddlewareHints {
                retry: Some(RetryHint {
                    max_attempts: Attempts::Infinite,
                    backoff: BackoffKind::Fixed {
                        delay: Duration::from_millis(0),
                    },
                }),
                ..Default::default()
            }
        }
    }

    #[test]
    fn test_skip_control_on_sink_is_error() {
        let factory = MockSkipControlFactory;
        let result = validate_middleware_safety(&factory, StageType::Sink, "test_sink");

        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("CRITICAL")));
        assert!(result.has_dangerous);
    }

    #[test]
    fn test_skip_control_on_transform_is_warning() {
        let factory = MockSkipControlFactory;
        let result = validate_middleware_safety(&factory, StageType::Transform, "test_transform");

        assert!(result.is_ok()); // No errors
        assert!(result.has_warnings());
        assert!(result.has_dangerous);
    }

    #[test]
    fn test_infinite_retry_on_source_is_error() {
        let factory = MockInfiniteRetryFactory;
        let result = validate_middleware_safety(&factory, StageType::FiniteSource, "test_source");

        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("makes no sense")));
    }

    #[test]
    fn test_safe_middleware_no_warnings() {
        // Create a mock safe middleware factory
        struct SafeFamily;
        struct SafeFactory;
        impl MiddlewareFactory for SafeFactory {
            fn label(&self) -> &'static str {
                "safe_middleware"
            }

            fn override_key(&self) -> crate::middleware::MiddlewareOverrideKey {
                crate::middleware::MiddlewareOverrideKey::of::<SafeFamily>("safe_middleware")
            }

            fn control_role(&self) -> crate::middleware::ControlMiddlewareRole {
                crate::middleware::ControlMiddlewareRole::None
            }

            fn plan_contribution(&self) -> crate::middleware::MiddlewarePlanContribution {
                crate::middleware::MiddlewarePlanContribution::None
            }

            fn topology_config_slot(
                &self,
            ) -> Option<crate::middleware::TopologyMiddlewareConfigSlot> {
                None
            }

            fn create(
                &self,
                _: &StageConfig,
                _control_middleware: std::sync::Arc<
                    crate::middleware::control::ControlMiddlewareAggregator,
                >,
            ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
                unimplemented!()
            }
        }

        let factory = SafeFactory;
        let result = validate_middleware_safety(&factory, StageType::Transform, "test_transform");

        assert!(result.is_ok());
        assert!(!result.has_warnings());
        assert!(!result.has_dangerous);
    }

    #[test]
    fn test_rate_limit_on_sink_warning_is_hint_driven() {
        struct RateLimitFamily;
        struct RateLimitFactory;
        impl MiddlewareFactory for RateLimitFactory {
            fn label(&self) -> &'static str {
                "opaque.rl"
            }

            fn override_key(&self) -> crate::middleware::MiddlewareOverrideKey {
                crate::middleware::MiddlewareOverrideKey::of::<RateLimitFamily>("rate_limit")
            }

            fn control_role(&self) -> crate::middleware::ControlMiddlewareRole {
                crate::middleware::ControlMiddlewareRole::None
            }

            fn plan_contribution(&self) -> crate::middleware::MiddlewarePlanContribution {
                crate::middleware::MiddlewarePlanContribution::None
            }

            fn topology_config_slot(
                &self,
            ) -> Option<crate::middleware::TopologyMiddlewareConfigSlot> {
                None
            }

            fn create(
                &self,
                _: &StageConfig,
                _control_middleware: std::sync::Arc<
                    crate::middleware::control::ControlMiddlewareAggregator,
                >,
            ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
                unimplemented!()
            }

            fn hints(&self) -> MiddlewareHints {
                MiddlewareHints {
                    rate_limits: true,
                    ..Default::default()
                }
            }
        }

        let factory = RateLimitFactory;
        let result = validate_middleware_safety(&factory, StageType::Sink, "snk");
        assert!(result.is_ok());
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.contains("Rate limiting on sink")),
            "expected Sink+RateLimit warning from typed hint"
        );
    }

    #[test]
    fn test_unbounded_batching_on_sink_warning_is_hint_driven() {
        struct BatchingFamily;
        struct UnboundedBatchingFactory;
        impl MiddlewareFactory for UnboundedBatchingFactory {
            fn label(&self) -> &'static str {
                "opaque.batching"
            }

            fn override_key(&self) -> crate::middleware::MiddlewareOverrideKey {
                crate::middleware::MiddlewareOverrideKey::of::<BatchingFamily>("batching")
            }

            fn control_role(&self) -> crate::middleware::ControlMiddlewareRole {
                crate::middleware::ControlMiddlewareRole::None
            }

            fn plan_contribution(&self) -> crate::middleware::MiddlewarePlanContribution {
                crate::middleware::MiddlewarePlanContribution::None
            }

            fn topology_config_slot(
                &self,
            ) -> Option<crate::middleware::TopologyMiddlewareConfigSlot> {
                None
            }

            fn create(
                &self,
                _: &StageConfig,
                _control_middleware: std::sync::Arc<
                    crate::middleware::control::ControlMiddlewareAggregator,
                >,
            ) -> crate::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
                unimplemented!()
            }

            fn hints(&self) -> MiddlewareHints {
                MiddlewareHints {
                    batching: Some(BatchingHint {
                        bounded: false,
                        timeout_ms: None,
                    }),
                    ..Default::default()
                }
            }
        }

        let factory = UnboundedBatchingFactory;
        let result = validate_middleware_safety(&factory, StageType::Sink, "snk");
        assert!(result.is_ok());
        assert!(
            result
                .warnings
                .iter()
                .any(|w| w.contains("Unbounded batching")),
            "expected Sink+UnboundedBatching warning from typed hint"
        );
    }
}
