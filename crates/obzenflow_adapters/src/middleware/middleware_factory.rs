//! Middleware factory trait and related types
//!
//! This module defines the factory pattern for creating middleware instances
//! with stage context, solving the problem of injecting stage-specific
//! configuration into middleware at construction time.

use super::{Middleware, MiddlewareSafety, MiddlewareHints};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_core::event::context::StageType;
use obzenflow_runtime_services::stages::common::control_strategies::ControlEventStrategy;

/// Factory that creates middleware with stage context.
/// 
/// This trait solves the stage context injection problem by deferring middleware
/// creation until the supervisor is built with full context available.
/// 
/// ## Example Implementation
/// 
/// ```rust
/// use obzenflow_adapters::middleware::{MiddlewareFactory, Middleware, LoggingMiddleware};
/// use obzenflow_runtime_services::pipeline::config::StageConfig;
/// 
/// struct LoggingFactory;
/// 
/// impl MiddlewareFactory for LoggingFactory {
///     fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
///         // LoggingMiddleware::new() takes no arguments
///         Box::new(LoggingMiddleware::new())
///     }
///     
///     fn name(&self) -> &str {
///         "logging"
///     }
/// }
/// ```
pub trait MiddlewareFactory: Send + Sync {
    /// Create middleware instance with full stage context
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware>;
    
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
            StageType::Stateful
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
}

// Implementation for Box<dyn MiddlewareFactory> to allow boxed factories
impl<F: MiddlewareFactory + ?Sized> MiddlewareFactory for Box<F> {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        (**self).create(config)
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
}