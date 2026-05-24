// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Middleware factory trait and related types
//!
//! This module defines the factory pattern for creating middleware instances
//! with stage context, solving the problem of injecting stage-specific
//! configuration into middleware at construction time.

use super::{Middleware, MiddlewareHints, MiddlewareSafety};
use obzenflow_core::event::context::StageType;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::control_strategies::ControlEventStrategy;
use std::any::TypeId;
use std::error::Error as StdError;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use thiserror::Error;

use super::control::ControlMiddlewareAggregator;

pub type MiddlewareFactorySource = Box<dyn StdError + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum MiddlewareFactoryError {
    #[error(
        "Invalid configuration for middleware '{middleware}' on stage '{stage_name}': {source}"
    )]
    InvalidConfiguration {
        middleware: String,
        stage_name: String,
        #[source]
        source: MiddlewareFactorySource,
    },

    #[error("Middleware '{middleware}' does not support stage '{stage_name}' ({stage_type})")]
    UnsupportedStageType {
        middleware: String,
        stage_type: StageType,
        stage_name: String,
    },

    #[error("Failed to materialize middleware '{middleware}' for stage '{stage_name}': {source}")]
    MaterializationFailed {
        middleware: String,
        stage_name: String,
        #[source]
        source: MiddlewareFactorySource,
    },
}

pub type MiddlewareFactoryResult<T> = Result<T, MiddlewareFactoryError>;

impl MiddlewareFactoryError {
    pub fn invalid_configuration(
        middleware: impl Into<String>,
        stage_name: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::InvalidConfiguration {
            middleware: middleware.into(),
            stage_name: stage_name.into(),
            source: Box::new(source),
        }
    }

    pub fn unsupported_stage_type(
        middleware: impl Into<String>,
        stage_type: StageType,
        stage_name: impl Into<String>,
    ) -> Self {
        Self::UnsupportedStageType {
            middleware: middleware.into(),
            stage_type,
            stage_name: stage_name.into(),
        }
    }

    pub fn materialization_failed(
        middleware: impl Into<String>,
        stage_name: impl Into<String>,
        source: impl StdError + Send + Sync + 'static,
    ) -> Self {
        Self::MaterializationFailed {
            middleware: middleware.into(),
            stage_name: stage_name.into(),
            source: Box::new(source),
        }
    }
}

/// Factory that creates middleware with stage context.
///
/// This trait solves the stage context injection problem by deferring middleware
/// creation until the supervisor is built with full context available.
///
/// ## Example Implementation
///
/// ```rust
/// use obzenflow_adapters::middleware::{
///     ControlMiddlewareRole, Middleware, MiddlewareAction, MiddlewareContext, MiddlewareFactory,
///     MiddlewareFactoryResult, MiddlewareOverrideKey, MiddlewarePlanContribution,
///     SourceMiddlewarePhase, TopologyMiddlewareConfigSlot,
/// };
/// use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
/// use obzenflow_runtime::pipeline::config::StageConfig;
/// use obzenflow_core::event::chain_event::ChainEvent;
/// use std::sync::Arc;
///
/// struct LoggingFamily;
///
/// struct LoggingMiddleware;
///
/// impl Middleware for LoggingMiddleware {
///     fn label(&self) -> &'static str {
///         "logging"
///     }
///
///     fn source_phase(&self) -> SourceMiddlewarePhase {
///         SourceMiddlewarePhase::Ordinary
///     }
///
///     fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
///         MiddlewareAction::Continue
///     }
/// }
///
/// struct LoggingFactory;
///
/// impl MiddlewareFactory for LoggingFactory {
///     fn label(&self) -> &'static str {
///         "logging"
///     }
///
///     fn override_key(&self) -> MiddlewareOverrideKey {
///         MiddlewareOverrideKey::of::<LoggingFamily>("logging")
///     }
///
///     fn control_role(&self) -> ControlMiddlewareRole {
///         ControlMiddlewareRole::None
///     }
///
///     fn plan_contribution(&self) -> MiddlewarePlanContribution {
///         MiddlewarePlanContribution::None
///     }
///
///     fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
///         None
///     }
///
///     fn create(
///         &self,
///         _config: &StageConfig,
///         _control_middleware: Arc<ControlMiddlewareAggregator>,
///     ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
///         Ok(Box::new(LoggingMiddleware))
///     }
/// }
/// ```
pub trait MiddlewareFactory: Send + Sync {
    /// Display label for logs, topology output, metrics labels, and diagnostics.
    fn label(&self) -> &'static str;

    /// Typed override family key used by the resolver.
    fn override_key(&self) -> MiddlewareOverrideKey;

    /// Typed control role used by the DSL for control binding.
    fn control_role(&self) -> ControlMiddlewareRole;

    /// Typed plan contribution used by the DSL for runtime planning.
    fn plan_contribution(&self) -> MiddlewarePlanContribution;

    /// Typed topology slot for config snapshot placement.
    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot>;

    /// Create middleware instance with full stage context
    fn create(
        &self,
        config: &StageConfig,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> MiddlewareFactoryResult<Box<dyn Middleware>>;

    /// Create a control event strategy if this middleware needs one
    ///
    /// Most middleware don't need special control event handling and can
    /// return None. Middleware that needs delay or barrier logic (like
    /// windowing) should return their strategy.
    fn create_control_strategy(&self) -> Option<Box<dyn ControlEventStrategy>> {
        None
    }

    /// Which stage types this middleware supports
    ///
    /// Default implementation supports all stage types. Override this
    /// to restrict middleware to specific stage types.
    fn supported_stage_types(&self) -> &[StageType] {
        &[
            StageType::FiniteSource,
            StageType::InfiniteSource,
            StageType::Transform,
            StageType::Sink,
            StageType::Stateful,
        ]
    }

    /// Safety level of this middleware
    ///
    /// Default is Safe. Override for middleware that can cause
    /// data loss or pipeline hangs if misused.
    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Safe
    }

    /// Static hints about this middleware's behavior
    ///
    /// Default implementation returns no hints. Override to provide
    /// information about retry behavior, control event handling, etc.
    fn hints(&self) -> MiddlewareHints {
        MiddlewareHints::default()
    }

    /// Return static configuration for topology observability (FLOWIP-059).
    ///
    /// Middleware factories override this to expose their structural configuration
    /// (thresholds, policies, capacities, etc.) for the `/api/topology` endpoint.
    /// This is purely structural data - no runtime state.
    ///
    /// Default returns None for middleware that doesn't need config exposure.
    fn config_snapshot(&self) -> Option<serde_json::Value> {
        None
    }
}

// Implementation for Box<dyn MiddlewareFactory> to allow boxed factories
impl<F: MiddlewareFactory + ?Sized> MiddlewareFactory for Box<F> {
    fn label(&self) -> &'static str {
        (**self).label()
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        (**self).override_key()
    }

    fn control_role(&self) -> ControlMiddlewareRole {
        (**self).control_role()
    }

    fn plan_contribution(&self) -> MiddlewarePlanContribution {
        (**self).plan_contribution()
    }

    fn topology_config_slot(&self) -> Option<TopologyMiddlewareConfigSlot> {
        (**self).topology_config_slot()
    }

    fn create(
        &self,
        config: &StageConfig,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
        (**self).create(config, control_middleware)
    }

    fn create_control_strategy(&self) -> Option<Box<dyn ControlEventStrategy>> {
        (**self).create_control_strategy()
    }

    fn supported_stage_types(&self) -> &[StageType] {
        (**self).supported_stage_types()
    }

    fn safety_level(&self) -> MiddlewareSafety {
        (**self).safety_level()
    }

    fn hints(&self) -> MiddlewareHints {
        (**self).hints()
    }

    fn config_snapshot(&self) -> Option<serde_json::Value> {
        (**self).config_snapshot()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct MiddlewareOverrideKey {
    type_id: TypeId,
    family_label: &'static str,
}

impl MiddlewareOverrideKey {
    pub fn of<T: 'static>(family_label: &'static str) -> Self {
        Self {
            type_id: TypeId::of::<T>(),
            family_label,
        }
    }

    pub fn family_label(&self) -> &'static str {
        self.family_label
    }
}

impl PartialEq for MiddlewareOverrideKey {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Eq for MiddlewareOverrideKey {}

impl Hash for MiddlewareOverrideKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id.hash(state);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlMiddlewareRole {
    None,
    CircuitBreaker,
    RateLimiter,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MiddlewarePlanContribution {
    None,
    Backpressure { window: std::num::NonZeroU64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyMiddlewareConfigSlot {
    CircuitBreaker,
    RateLimiter,
    Retry,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::hash_map::DefaultHasher;

    struct TestFamily;

    fn hash_key(key: MiddlewareOverrideKey) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn middleware_override_key_identity_ignores_family_label() {
        let flow_key = MiddlewareOverrideKey::of::<TestFamily>("flow.label");
        let stage_key = MiddlewareOverrideKey::of::<TestFamily>("stage.label");

        assert_eq!(flow_key, stage_key);
        assert_eq!(hash_key(flow_key), hash_key(stage_key));
        assert_eq!(flow_key.family_label(), "flow.label");
        assert_eq!(stage_key.family_label(), "stage.label");
    }
}
