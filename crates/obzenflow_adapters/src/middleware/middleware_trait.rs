//! Core middleware trait and related types
//!
//! This module defines the fundamental middleware abstraction and the
//! actions that middleware can take during event processing.

use super::MiddlewareContext;
use obzenflow_core::event::chain_event::ChainEvent;

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
    fn post_handle(
        &self,
        _event: &ChainEvent,
        _results: &[ChainEvent],
        _ctx: &mut MiddlewareContext,
    ) {
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
