//! # Middleware System for FlowState
//!
//! This module provides a composable middleware system for adding cross-cutting
//! concerns like monitoring, logging, rate limiting, and retries to your FlowState
//! pipeline stages without modifying their core logic.
//!
//! ## Middleware Overview
//!
//! Middleware is composable - you can stack multiple middleware on a handler.
//! When used in flows (via the DSL layer), middleware is specified as an array
//! of middleware instances for each stage.
//!
//! The middleware execution order is:
//! 1. First middleware's `pre_handle`
//! 2. Second middleware's `pre_handle`
//! 3. Handler processes the event
//! 4. Second middleware's `post_handle`
//! 5. First middleware's `post_handle`
//!
//! ## Monitoring with FLOWIP-056-666
//!
//! With FLOWIP-056-666 Wide Events, monitoring is no longer implemented as middleware.
//! Instead, metrics are automatically derived from the event journal by MetricsAggregator.
//!
//! The monitoring taxonomies (RED, USE, Golden Signals, SAAFE) are now documentation-only
//! and provide Prometheus queries and Grafana dashboards for viewing metrics.
//!
//! ```rust
//! // OLD: Monitoring middleware (no longer available)
//! // let red_middleware = RED::monitoring();
//!
//! // NEW: Metrics are automatically collected from the journal
//! // See obzenflow_adapters::monitoring::aggregator::MetricsAggregator
//! ```
//!
//! ## Available Monitoring Views
//!
//! FlowState provides several monitoring taxonomies as view definitions:
//!
//! ### RED (Rate, Errors, Duration)
//! Best for request/response systems and sources. Tracks:
//! - **Rate**: Events processed per second
//! - **Errors**: Error count and rate
//! - **Duration**: Processing time distribution
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::red::RED;
//! 
//! // Get Prometheus queries for RED metrics
//! let queries = RED::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ### USE (Utilization, Saturation, Errors)
//! Ideal for resource-focused stages like transforms. Tracks:
//! - **Utilization**: Resource usage percentage
//! - **Saturation**: Queue depth and backpressure
//! - **Errors**: Processing errors
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::use_taxonomy::USE;
//! 
//! // Get Prometheus queries for USE metrics
//! let queries = USE::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ### GoldenSignals (Latency, Traffic, Errors, Saturation)
//! Comprehensive monitoring for critical stages. Tracks:
//! - **Latency**: End-to-end processing time
//! - **Traffic**: Request volume
//! - **Errors**: Error rate and types
//! - **Saturation**: Resource saturation
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::golden_signals::GoldenSignals;
//! 
//! // Get Prometheus queries for Golden Signals metrics
//! let queries = GoldenSignals::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ### SAAFE (Saturation, Anomalies, Amendments, Failures, Errors)
//! Advanced monitoring for sinks and data quality. Tracks:
//! - **Saturation**: Backpressure and queue depth
//! - **Anomalies**: Unusual patterns in data
//! - **Amendments**: Data corrections/updates
//! - **Failures**: Persistent failures
//! - **Errors**: Transient errors
//!
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::saafe::SAAFE;
//! 
//! // Get Prometheus queries for SAAFE metrics
//! let queries = SAAFE::prometheus_queries("my_flow", "my_stage");
//! ```
//!
//! ## Applying Middleware to Handlers
//!
//! Use the handler extension traits to apply middleware:
//!
//! ```rust
//! use obzenflow_adapters::middleware::{TransformHandlerExt, LoggingMiddleware};
//! use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
//! use obzenflow_core::event::chain_event::ChainEvent;
//!
//! struct MyTransform;
//!
//! impl TransformHandler for MyTransform {
//!     fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
//!         vec![event]
//!     }
//! }
//!
//! // Apply middleware directly (monitoring middleware is created via factories in descriptors)
//! let handler_with_middleware = MyTransform
//!     .middleware()
//!     .with(LoggingMiddleware::new())
//!     .build();
//! ```
//!
//! ## Common Middleware Utilities
//!
//! The `common` module provides pre-built middleware for common patterns:
//!
//! ```rust
//! use obzenflow_adapters::middleware::common;
//! use std::time::Duration;
//!
//! // Rate limiting - limits events per second
//! let rate_limiter = common::rate_limit(100.0);
//!
//! // Retry logic - uses exponential backoff
//! let retry_middleware = common::retry::exponential(3)
//!     .initial_delay(Duration::from_millis(100))
//!     .build();
//!
//! // Timeout - fails if processing takes too long  
//! let timeout_middleware = common::timeout(Duration::from_secs(1));
//!
//! // Logging - logs events and results
//! let logging_middleware = common::logging(tracing::Level::INFO);
//! ```
//!
//! ## Custom Middleware
//!
//! You can also create custom middleware by implementing the `Middleware` trait:
//!
//! ```rust
//! use obzenflow_adapters::middleware::{Middleware, MiddlewareAction, MiddlewareContext};
//! use obzenflow_core::event::chain_event::ChainEvent;
//! 
//! struct MyCustomMiddleware;
//! 
//! impl Middleware for MyCustomMiddleware {
//!     fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
//!         println!("Processing event: {:?}", event.id);
//!         MiddlewareAction::Continue
//!     }
//!     
//!     fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
//!         println!("Produced {} results", results.len());
//!     }
//! }
//! ```

// Handler-specific middleware adapters
mod transform_middleware;
mod source_middleware;
mod sink_middleware;

// Common middleware utilities
mod function;
pub mod common;
pub mod context;
pub mod circuit_breaker;
pub mod retry;
pub mod flow_boundary;
pub mod flow_metrics_registry;
pub mod sli;
pub mod windowing;
pub mod rate_limiter;
mod logging_middleware;
mod control_requirements;
mod safety_validation;
mod timing;
mod system_enrichment;
mod outcome_enrichment;

// Dangerous middleware examples moved to examples/dangerous_examples.rs
// Factory tests moved to tests/factory_tests.rs
// Note: With FLOWIP-056-666, monitoring is no longer implemented as middleware.
// Metrics are automatically derived from the event journal by MetricsAggregator.
// Taxonomies now provide Prometheus queries and Grafana dashboards:
// - RED::prometheus_queries()
// - USE::prometheus_queries()
// - GoldenSignals::prometheus_queries()
// - SAAFE::prometheus_queries()

// Handler-specific exports
pub use transform_middleware::{MiddlewareTransform, TransformHandlerExt, TransformMiddlewareBuilder};
pub use source_middleware::{
    MiddlewareFiniteSource, MiddlewareInfiniteSource,
    FiniteSourceHandlerExt, InfiniteSourceHandlerExt,
    FiniteSourceMiddlewareBuilder, InfiniteSourceMiddlewareBuilder
};
pub use sink_middleware::{MiddlewareSink, SinkHandlerExt, SinkMiddlewareBuilder};

// Common utilities
pub use function::{FnMiddleware, middleware_fn};
pub use common::{rate_limit, timeout, logging};
pub use context::{MiddlewareContext, MiddlewareEvent};
pub use circuit_breaker::{CircuitBreakerMiddleware, CircuitBreakerBuilder, circuit_breaker};
pub use retry::{RetryMiddleware, RetryBuilder, RetryStrategy};
pub use flow_boundary::{FlowBoundaryTracker, BoundaryTrackingMiddleware, BoundaryConfig, FlowMetrics};
pub use logging_middleware::LoggingMiddleware;
pub use sli::{CircuitBreakerSLI, RetrySLI, LatencySLI, SLOTracker, SLODefinition, AlertConfig};
pub use control_requirements::{ControlStrategyRequirement, BackoffConfig};
pub use self::safety_validation::{validate_middleware_safety, ValidationResult};
pub use windowing::{WindowingMiddleware, WindowingMiddlewareFactory};
pub use rate_limiter::{RateLimiterMiddleware, RateLimiterFactory};
pub use timing::TimingMiddleware;
pub use system_enrichment::SystemEnrichmentMiddleware;
pub use outcome_enrichment::OutcomeEnrichmentMiddleware;
// Monitoring is provided via taxonomy-specific methods

use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_runtime_services::stages::common::stage_handle::StageType;

/// Safety level of middleware
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MiddlewareSafety {
    /// Safe for all uses
    Safe,
    /// Requires understanding of implications
    Advanced,
    /// Can cause data loss or pipeline hangs if misused
    Dangerous,
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
    
    /// Declare what control event strategy this middleware requires
    /// 
    /// Most middleware don't need special control event handling and can
    /// return None. Middleware that needs retry logic (like circuit breakers)
    /// or delay logic (like windowing) should return their requirements.
    fn required_control_strategy(&self) -> Option<ControlStrategyRequirement> {
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
}

// Implementation for Box<dyn MiddlewareFactory> to allow boxed factories
impl<F: MiddlewareFactory + ?Sized> MiddlewareFactory for Box<F> {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        (**self).create(config)
    }
    
    fn name(&self) -> &str {
        (**self).name()
    }
    
    fn required_control_strategy(&self) -> Option<ControlStrategyRequirement> {
        (**self).required_control_strategy()
    }
    
    fn supported_stage_types(&self) -> &[StageType] {
        (**self).supported_stage_types()
    }
    
    fn safety_level(&self) -> MiddlewareSafety {
        (**self).safety_level()
    }
}

/// Trait for composable middleware that wraps Step behavior.
/// 
/// Middleware follows a three-phase interaction model:
/// 1. **Pre-processing** (`pre_handle`): Before the step processes the event
/// 2. **Post-processing** (`post_handle`): After successful processing  
/// 3. **Error handling** (`on_error`): When something goes wrong
/// 
/// ## Example Implementation
/// 
/// ```rust
/// use obzenflow_adapters::middleware::{Middleware, MiddlewareAction, MiddlewareContext};
/// use obzenflow_core::ChainEvent;
/// 
/// struct LoggingMiddleware;
/// 
/// impl Middleware for LoggingMiddleware {
///     fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
///         println!("Processing event: {:?}", event.id);
///         MiddlewareAction::Continue
///     }
///     
///     fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
///         println!("Produced {} results", results.len());
///     }
/// }
/// ```
pub trait Middleware: Send + Sync {
    /// Called before the inner step processes the event.
    /// 
    /// Use this to:
    /// - Implement rate limiting (return `Skip` or `Abort`)
    /// - Add caching (return `Skip` with cached results)
    /// - Validate events (return `Abort` for invalid)
    /// - Record metrics (return `Continue` after recording)
    /// - Emit middleware events for observability
    fn pre_handle(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        MiddlewareAction::Continue
    }

    /// Called after the inner step successfully processes the event.
    /// 
    /// Use this to:
    /// - Observe results (but not modify them)
    /// - Record success metrics
    /// - Perform side effects like logging
    /// - Emit middleware events based on outcomes
    fn post_handle(&self, _event: &ChainEvent, _results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
        // Default: no-op
    }

    /// Called when an error occurs during processing.
    /// 
    /// Use this to:
    /// - Implement retry logic (return `Retry`)
    /// - Convert errors to events (return `Recover`)
    /// - Let errors bubble up (return `Propagate`)
    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        ErrorAction::Propagate
    }

    /// Called before each result event is written to the journal.
    /// 
    /// This hook enables event enrichment with timing data, flow context,
    /// and other metadata needed for observability. The event is mutable,
    /// allowing middleware to add fields following the "wide events" pattern.
    /// 
    /// Use this to:
    /// - Add processing time to events
    /// - Ensure flow context is populated
    /// - Enrich events with deployment/environment info
    /// - Add correlation IDs for tracing
    /// 
    /// Default implementation is a no-op for backward compatibility.
    fn pre_write(&self, _event: &mut ChainEvent, _ctx: &MiddlewareContext) {
        // Default: no-op
    }
}

/// Actions that middleware can take during pre-processing
#[derive(Debug, Clone)]
pub enum MiddlewareAction {
    /// Continue with normal processing
    Continue,
    /// Skip the inner step and return these results instead
    Skip(Vec<ChainEvent>),
    /// Abort processing entirely (returns empty results)
    Abort,
}

/// Actions that middleware can take when handling errors
#[derive(Debug, Clone)]
pub enum ErrorAction {
    /// Propagate the error up to the caller
    Propagate,
    /// Recover by converting the error into events
    Recover(Vec<ChainEvent>),
    /// Retry the operation (be careful of infinite loops!)
    Retry,
}

// Implementation for Box<dyn Middleware> to allow boxed middleware
impl<M: Middleware + ?Sized> Middleware for Box<M> {
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        (**self).pre_handle(event, ctx)
    }

    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        (**self).post_handle(event, results, ctx)
    }

    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction {
        (**self).on_error(event, ctx)
    }

    fn pre_write(&self, event: &mut ChainEvent, ctx: &MiddlewareContext) {
        (**self).pre_write(event, ctx)
    }
}

