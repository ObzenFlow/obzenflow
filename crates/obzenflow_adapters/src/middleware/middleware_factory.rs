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
use std::error::Error as StdError;
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
/// use obzenflow_adapters::middleware::{MiddlewareFactory, Middleware, LoggingMiddleware};
/// use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
/// use obzenflow_runtime::pipeline::config::StageConfig;
/// use std::sync::Arc;
///
/// struct LoggingFactory;
///
/// impl MiddlewareFactory for LoggingFactory {
///     fn create(
///         &self,
///         _config: &StageConfig,
///         _control_middleware: Arc<ControlMiddlewareAggregator>,
///     ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
///         // LoggingMiddleware::new() takes no arguments
///         Ok(Box::new(LoggingMiddleware::new()))
///     }
///     
///     fn name(&self) -> &str {
///         "logging"
///     }
/// }
/// ```
pub trait MiddlewareFactory: Send + Sync {
    /// Create middleware instance with full stage context
    fn create(
        &self,
        config: &StageConfig,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> MiddlewareFactoryResult<Box<dyn Middleware>>;

    /// Get a descriptive name for this middleware type
    fn name(&self) -> &str;

    /// Create a control event strategy if this middleware needs one
    ///
    /// Most middleware don't need special control event handling and can
    /// return None. Middleware that needs retry logic (like circuit breakers)
    /// or delay logic (like windowing) should return their strategy.
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
    fn create(
        &self,
        config: &StageConfig,
        control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> MiddlewareFactoryResult<Box<dyn Middleware>> {
        (**self).create(config, control_middleware)
    }

    fn name(&self) -> &str {
        (**self).name()
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
