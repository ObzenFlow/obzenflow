//! # Middleware Architecture for FlowState
//! 
//! This module provides a composable middleware system for wrapping Step and EventHandler behavior
//! with cross-cutting concerns like monitoring, logging, retry logic, and circuit breaking.
//! 
//! ## Example for Steps
//! 
//! ```rust
//! use flowstate::middleware::{Middleware, StepExt};
//! 
//! // Apply middleware to any step
//! let step = MyProcessor::new()
//!     .middleware()
//!     .with(LoggingMiddleware::new())
//!     .with(RetryMiddleware::new(3))
//!     .build();
//! ```
//! 
//! ## Example for EventHandlers
//! 
//! ```rust
//! use flowstate::middleware::{Middleware, EventHandlerExt};
//! 
//! // Apply middleware to any event handler
//! let handler = MyEventHandler::new()
//!     .middleware()
//!     .with(LoggingMiddleware::new())
//!     .with(RetryMiddleware::new(3))
//!     .build();
//! ```

mod middleware_step;
mod middleware_event_handler;
mod builder;
mod event_handler_builder;
mod function;
pub mod monitoring;
mod helpers;
mod helpers_event_handler;
pub mod common;
pub mod flow_boundary;
pub mod flow_observer;
#[macro_use]
mod macros;

pub use middleware_step::MiddlewareStep;
pub use middleware_event_handler::MiddlewareEventHandler;
pub use builder::MiddlewareBuilder;
pub use event_handler_builder::{EventHandlerMiddlewareBuilder, EventHandlerExt};
pub use function::{FnMiddleware, middleware_fn};
pub use monitoring::{MetricRecorder, MonitoringMiddleware};
pub use common::{rate_limit, timeout, logging, retry};
pub use flow_boundary::{FlowBoundaryTracker, BoundaryTrackingMiddleware, BoundaryConfig, FlowMetrics};
pub use flow_observer::FlowObserver;
pub use helpers_event_handler::apply_middleware_vec;
// monitoring! macro is exported at crate root

use crate::chain_event::ChainEvent;
use crate::step::Step;
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

/// Extension trait that adds the `.middleware()` method to all Steps.
/// 
/// This enables the fluent builder pattern for applying middleware:
/// 
/// ```rust
/// let step = MyStep::new()
///     .middleware()
///     .with(middleware1)
///     .with(middleware2)
///     .build();
/// ```
pub trait StepExt: Step + Sized {
    /// Start building a middleware stack for this step
    fn middleware(self) -> MiddlewareBuilder<Self> {
        MiddlewareBuilder::new(self)
    }
}

// Blanket implementation for all Steps
impl<S: Step> StepExt for S {}
