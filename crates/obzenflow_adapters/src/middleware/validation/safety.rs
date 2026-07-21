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
            // FLOWIP-120i: this warning keeps firing under strict replay,
            // because topology validation is real in both modes, but it must
            // say that the configured policy is inert for data-path
            // accounting rather than implying live enforcement.
            let strict_replay = crate::middleware::strict_replay_active();
            let message = if strict_replay {
                format!(
                    "⚠️  ADVANCED: Middleware '{}' on stage '{}' requires careful configuration (configured for topology validation; live accounting suppressed)",
                    factory.label(),
                    stage_name
                )
            } else {
                format!(
                    "⚠️  ADVANCED: Middleware '{}' on stage '{}' requires careful configuration",
                    factory.label(),
                    stage_name
                )
            };
            warn!(
                run_mode = if strict_replay { "replay" } else { "live" },
                strict_replay, "{}", message
            );
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
        Attempts, BackoffKind, BatchingHint, MiddlewareAttachmentRequest, MiddlewareDeclaration,
        MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult, MiddlewareHints,
        MiddlewareMaterializationContext, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
        RetryHint,
    };
    use std::time::Duration;

    macro_rules! safety_only_factory {
        () => {
            fn declaration(&self) -> MiddlewareDeclaration {
                MiddlewareDeclaration::observer(self.label(), vec![MiddlewareSurfaceKind::Handler])
            }

            fn materialize(
                &self,
                _request: MiddlewareAttachmentRequest<'_>,
                context: &MiddlewareMaterializationContext<'_>,
            ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
                Err(MiddlewareFactoryError::materialization_failed(
                    self.label(),
                    &context.config.name,
                    std::io::Error::other("safety-only test factory"),
                ))
            }
        };
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

        safety_only_factory!();
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

        safety_only_factory!();
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

            safety_only_factory!();
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

            safety_only_factory!();

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

            safety_only_factory!();

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
