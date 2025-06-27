//! # Middleware Architecture for FlowState
//! 
//! This module provides a composable middleware system for wrapping handler behavior
//! with cross-cutting concerns like monitoring, logging, retry logic, and circuit breaking.
//! 
//! ## Example with New Handler Types
//! 
//! ```rust
//! use flowstate::middleware::{TransformHandlerExt, LoggingMiddleware, logging, retry};
//! 
//! // Apply middleware to transform handler - using concrete LoggingMiddleware
//! let transform = MyTransformHandler::new()
//!     .middleware()
//!     .with(LoggingMiddleware::new())
//!     .with(retry::fixed_delay(3, Duration::from_millis(100)))
//!     .build();
//! 
//! // Apply to source handler (needs writer_id) - using builder pattern
//! let source = MySourceHandler::new()
//!     .middleware(writer_id)
//!     .with(logging::debug().with_metadata().build())
//!     .build();
//! 
//! // Apply to sink handler - with custom prefix
//! let sink = MySinkHandler::new()
//!     .middleware()
//!     .with(LoggingMiddleware::with_prefix("SINK WAZ HERE!"))
//!     .with(retry::exponential(5).with_jitter().build())
//!     .build();
//! ```

// Handler-specific middleware adapters
mod transform_middleware;
mod source_middleware;
mod sink_middleware;

// Common middleware utilities
mod function;
pub mod monitoring;
pub mod common;
pub mod flow_boundary;
mod logging_middleware;
#[macro_use]
mod macros;

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
pub use monitoring::{MetricRecorder, MonitoringMiddleware};
pub use common::{rate_limit, timeout, logging, retry};
pub use flow_boundary::{FlowBoundaryTracker, BoundaryTrackingMiddleware, BoundaryConfig, FlowMetrics};
pub use logging_middleware::LoggingMiddleware;
// monitoring! macro is exported at crate root

use obzenflow_core::event::chain_event::ChainEvent;
use std::error::Error;

/// Type alias for errors returned by steps
pub type StepError = Box<dyn Error + Send + Sync>;

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
/// struct LoggingMiddleware;
/// 
/// impl Middleware for LoggingMiddleware {
///     fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
///         println!("Processing event: {}", event.id);
///         MiddlewareAction::Continue
///     }
///     
///     fn post_handle(&self, event: &ChainEvent, results: &mut Vec<ChainEvent>) {
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
    fn pre_handle(&self, _event: &ChainEvent) -> MiddlewareAction {
        MiddlewareAction::Continue
    }

    /// Called after the inner step successfully processes the event.
    /// 
    /// Use this to:
    /// - Transform or enrich results
    /// - Record success metrics
    /// - Perform side effects like logging
    fn post_handle(&self, _event: &ChainEvent, _results: &mut Vec<ChainEvent>) {
        // Default: no-op
    }

    /// Called when an error occurs during processing.
    /// 
    /// Use this to:
    /// - Implement retry logic (return `Retry`)
    /// - Convert errors to events (return `Recover`)
    /// - Let errors bubble up (return `Propagate`)
    fn on_error(&self, _event: &ChainEvent, _error: &StepError) -> ErrorAction {
        ErrorAction::Propagate
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
    fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
        (**self).pre_handle(event)
    }

    fn post_handle(&self, event: &ChainEvent, results: &mut Vec<ChainEvent>) {
        (**self).post_handle(event, results)
    }

    fn on_error(&self, event: &ChainEvent, error: &StepError) -> ErrorAction {
        (**self).on_error(event, error)
    }
}

