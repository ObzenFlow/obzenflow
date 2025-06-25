use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_runtime_services::control_plane::stage_supervisor::event_handler::{EventHandler, ProcessingMode};
use obzenflow_core::Result;
use super::{Middleware, MiddlewareAction, ErrorAction, StepError};

/// MiddlewareEventHandler as a decorator that wraps an EventHandler with additional behaviour.
///
/// The middleware intercepts the event processing at three points:
/// - Before processing (pre_handle)
/// - After successful processing (post_handle)
/// - On error (on_error)
///
/// This follows the same pattern as MiddlewareStep but for EventHandlers.
///
/// ## Example
///
/// ```rust
/// let handler = MiddlewareEventHandler::new(
///     MyEventProcessor::new(),
///     LoggingMiddleware::new()
/// );
/// ```
///
/// ## Nesting
///
/// Multiple middleware can be nested, creating an "onion" like pattern:
///
/// ```
/// MiddlewareEventHandler {
///     inner: MiddlewareEventHandler {
///         inner: MyProcessor,
///         middleware: LoggingMiddleware,
///     },
///     middleware: RetryMiddleware,
/// }
/// ```
#[derive(Debug, Clone)]
pub struct MiddlewareEventHandler<H, M>
where
    H: EventHandler,
    M: Middleware,
{
    pub(super) inner: H,
    pub(super) middleware: M,
}

impl<H, M> MiddlewareEventHandler<H, M>
where
    H: EventHandler,
    M: Middleware,
{
    /// Create a new MiddlewareEventHandler wrapping the given handler with middleware
    pub fn new(inner: H, middleware: M) -> Self {
        Self { inner, middleware }
    }
}

impl<H, M> EventHandler for MiddlewareEventHandler<H, M>
where
    H: EventHandler,
    M: Middleware,
{
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Pre-processing phase
        match self.middleware.pre_handle(&event) {
            MiddlewareAction::Continue => {
                // Process with inner handler
                let mut results = self.inner.transform(event.clone());
                // Post-processing phase
                self.middleware.post_handle(&event, &mut results);
                results
            }
            MiddlewareAction::Skip(results) => results,
            MiddlewareAction::Abort => vec![],
        }
    }

    fn observe(&self, event: &ChainEvent) -> Result<()> {
        // Pre-processing phase
        match self.middleware.pre_handle(event) {
            MiddlewareAction::Continue => {
                // Process with inner handler
                match self.inner.observe(event) {
                    Ok(()) => {
                        // For observe, we don't have results to pass to post_handle
                        // But we still call it for consistency (e.g., for metrics)
                        let mut empty_results = vec![];
                        self.middleware.post_handle(event, &mut empty_results);
                        Ok(())
                    }
                    Err(e) => {
                        // Convert to StepError for middleware
                        let step_error: StepError = e.into();
                        match self.middleware.on_error(event, &step_error) {
                            ErrorAction::Propagate => Err(step_error.into()),
                            ErrorAction::Recover(_) => Ok(()), // For observe, we ignore recovery events
                            ErrorAction::Retry => self.inner.observe(event), // Simple retry once
                        }
                    }
                }
            }
            MiddlewareAction::Skip(_) => Ok(()), // For observe, skip means success
            MiddlewareAction::Abort => Ok(()), // For observe, abort means no-op
        }
    }

    fn aggregate(&mut self, event: ChainEvent) -> Option<Vec<ChainEvent>> {
        // Pre-processing phase
        match self.middleware.pre_handle(&event) {
            MiddlewareAction::Continue => {
                // Process with inner handler
                match self.inner.aggregate(event.clone()) {
                    Some(mut results) => {
                        // Post-processing phase
                        self.middleware.post_handle(&event, &mut results);
                        Some(results)
                    }
                    None => None,
                }
            }
            MiddlewareAction::Skip(results) => Some(results),
            MiddlewareAction::Abort => Some(vec![]), // Abort returns empty results
        }
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        self.inner.processing_mode()
    }
}