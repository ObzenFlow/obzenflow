//! Middleware resolution with full tracking for observability
//!
//! This module implements the SRE-first approach to middleware inheritance,
//! tracking every decision for debugging and operational visibility.

use indexmap::IndexMap;
use obzenflow_adapters::middleware::MiddlewareFactory;
use serde::{Deserialize, Serialize};

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
        overrode_type: String,
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

/// Pure function that resolves middleware with full tracking
pub fn resolve_middleware(
    flow_middleware: Vec<Box<dyn MiddlewareFactory>>,
    stage_middleware: Vec<Box<dyn MiddlewareFactory>>,
    stage_name: &str,
) -> ResolvedMiddleware {
    let mut resolved: IndexMap<String, MiddlewareSpec> = IndexMap::new();
    let mut overrides = Vec::new();
    let mut warnings = Vec::new();

    // Phase 1: Add flow middleware
    for factory in flow_middleware {
        let name = factory.name().to_string();
        let spec = MiddlewareSpec {
            source: MiddlewareSource::Flow,
            factory,
        };
        resolved.insert(name, spec);
    }

    // Phase 2: Apply stage middleware
    for factory in stage_middleware {
        let name = factory.name().to_string();

        // Check for override
        if let Some(existing) = resolved.get(&name) {
            // Track the override
            overrides.push(OverrideRecord {
                middleware_type: name.clone(),
                flow_config: format_factory_config(&existing.factory),
                stage_config: format_factory_config(&factory),
                reason: OverrideReason::ExplicitOverride,
            });

            // Check for suspicious overrides
            if let Some(warning) = check_override_safety(&existing.factory, &factory) {
                warnings.push(warning);
            }
        }

        // Determine source
        let source = if resolved.contains_key(&name) {
            MiddlewareSource::StageOverride {
                overrode_type: name.clone(),
                overrode_config: format_factory_config(&resolved[&name].factory),
            }
        } else {
            MiddlewareSource::Stage
        };

        // Insert or override
        let spec = MiddlewareSpec { source, factory };
        resolved.insert(name, spec);
    }

    // Phase 3: Validate final configuration
    warnings.extend(validate_middleware_combination(&resolved, stage_name));

    ResolvedMiddleware {
        middleware: resolved.into_values().collect(),
        overrides,
        warnings,
    }
}

/// Format factory configuration for display
fn format_factory_config(factory: &dyn MiddlewareFactory) -> String {
    // This will need to be implemented based on the actual MiddlewareFactory trait
    // For now, we'll use a placeholder
    match factory.name() {
        "rate_limiter" => {
            // TODO: Extract actual rate from factory
            "rate_limit(...)".to_string()
        }
        "timeout" => {
            // TODO: Extract actual timeout from factory
            "timeout(...)".to_string()
        }
        _ => format!("{}(...)", factory.name()),
    }
}

/// Check if an override might cause operational issues
fn check_override_safety(
    flow_mw: &dyn MiddlewareFactory,
    stage_mw: &dyn MiddlewareFactory,
) -> Option<ConfigWarning> {
    match (flow_mw.name(), stage_mw.name()) {
        ("rate_limiter", "rate_limiter") => {
            // TODO: Once we can extract config from factories, implement actual checking
            // For now, return a placeholder warning for demonstration
            Some(ConfigWarning {
                level: WarnLevel::Medium,
                message: format!(
                    "Stage '{}' overrides flow rate limiting - verify this is intentional",
                    stage_mw.name()
                ),
                suggestion: Some(
                    "Consider if this stage really needs different rate limiting".to_string(),
                ),
            })
        }
        ("timeout", "timeout") => {
            // Check for significantly different timeouts
            Some(ConfigWarning {
                level: WarnLevel::Low,
                message: "Stage overrides flow timeout".to_string(),
                suggestion: None,
            })
        }
        _ => None,
    }
}

/// Validate the final middleware combination for potential issues
fn validate_middleware_combination(
    resolved: &IndexMap<String, MiddlewareSpec>,
    stage_name: &str,
) -> Vec<ConfigWarning> {
    let mut warnings = Vec::new();

    // Check for missing critical middleware
    let has_timeout = resolved.contains_key("timeout");
    let _has_rate_limit = resolved.contains_key("rate_limiter");

    if !has_timeout {
        warnings.push(ConfigWarning {
            level: WarnLevel::Medium,
            message: format!("Stage '{stage_name}' has no timeout middleware"),
            suggestion: Some("Consider adding timeout middleware to prevent hanging".to_string()),
        });
    }

    // Check for potentially conflicting middleware
    if resolved.contains_key("retry") && !resolved.contains_key("circuit_breaker") {
        warnings.push(ConfigWarning {
            level: WarnLevel::Low,
            message: "Retry middleware without circuit breaker can cause cascading failures"
                .to_string(),
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
            middleware_type = spec.factory.name(),
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
