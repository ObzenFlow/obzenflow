//! Safety validation for middleware combinations
//!
//! This module provides utilities for validating middleware safety when
//! constructing pipelines, issuing warnings for dangerous combinations.

use crate::middleware::{MiddlewareFactory, MiddlewareSafety};
use obzenflow_runtime_services::control_plane::stages::supervisors::stage_handle::StageType;
use tracing::{warn, error};

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
            factory.name(),
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
                factory.name(),
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
                factory.name(),
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
    // Pattern 1: Skip control events on sinks
    if factory.name().contains("skip") && factory.name().contains("control") {
        if stage_type == StageType::Sink {
            let message = format!(
                "❌ CRITICAL: Never skip control events on sink '{}'! \
                 This will prevent proper draining and cause data loss!",
                stage_name
            );
            error!("{}", message);
            result.errors.push(message);
        }
    }
    
    // Pattern 2: Infinite retry on sources
    if let Some(requirement) = factory.required_control_strategy() {
        use crate::middleware::ControlStrategyRequirement;
        if let ControlStrategyRequirement::Retry { max_attempts, .. } = requirement {
            if max_attempts == usize::MAX {
                match stage_type {
                    StageType::FiniteSource | StageType::InfiniteSource => {
                        let message = format!(
                            "❌ CRITICAL: Infinite retry on source '{}' makes no sense! \
                             Sources generate data, they don't retry receiving it!",
                            stage_name
                        );
                        error!("{}", message);
                        result.errors.push(message);
                    }
                    _ => {}
                }
            }
        }
    }
    
    // Pattern 3: Unbounded batching on sinks
    if factory.name().contains("batch") && factory.name().contains("unbounded") {
        if stage_type == StageType::Sink {
            let message = format!(
                "⚠️  WARNING: Unbounded batching on sink '{}' can cause data to be stuck! \
                 Make sure you have a timeout configured!",
                stage_name
            );
            warn!("{}", message);
            result.warnings.push(message);
        }
    }
    
    // Pattern 4: Rate limiting on sinks
    if factory.name().contains("rate_limit") {
        if stage_type == StageType::Sink {
            let message = format!(
                "⚠️  WARNING: Rate limiting on sink '{}' can cause severe backpressure! \
                 Make sure upstream stages can handle this!",
                stage_name
            );
            warn!("{}", message);
            result.warnings.push(message);
        }
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
    use crate::middleware::{dangerous_examples::*, Middleware, MiddlewareFactory};
    use obzenflow_runtime_services::control_plane::stages::supervisors::StageConfig;
    
    #[test]
    fn test_skip_control_on_sink_is_error() {
        let factory = SkipControlEventsFactory;
        let result = validate_middleware_safety(
            &factory,
            StageType::Sink,
            "test_sink"
        );
        
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("CRITICAL")));
        assert!(result.has_dangerous);
    }
    
    #[test]
    fn test_skip_control_on_transform_is_warning() {
        let factory = SkipControlEventsFactory;
        let result = validate_middleware_safety(
            &factory,
            StageType::Transform,
            "test_transform"
        );
        
        assert!(result.is_ok()); // No errors
        assert!(result.has_warnings());
        assert!(result.has_dangerous);
    }
    
    #[test]
    fn test_infinite_retry_on_source_is_error() {
        let factory = InfiniteRetryFactory;
        let result = validate_middleware_safety(
            &factory,
            StageType::FiniteSource,
            "test_source"
        );
        
        assert!(!result.is_ok());
        assert!(result.errors.iter().any(|e| e.contains("makes no sense")));
    }
    
    #[test]
    fn test_safe_middleware_no_warnings() {
        // Create a mock safe middleware factory
        struct SafeFactory;
        impl MiddlewareFactory for SafeFactory {
            fn create(&self, _: &StageConfig) -> Box<dyn Middleware> {
                unimplemented!()
            }
            fn name(&self) -> &str { "safe_middleware" }
        }
        
        let factory = SafeFactory;
        let result = validate_middleware_safety(
            &factory,
            StageType::Transform,
            "test_transform"
        );
        
        assert!(result.is_ok());
        assert!(!result.has_warnings());
        assert!(!result.has_dangerous);
    }
}