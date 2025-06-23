//! Helper functions for applying middleware to EventHandlers

use crate::lifecycle::EventHandler;
use crate::middleware::Middleware;

/// Apply a vector of middleware to an EventHandler.
/// 
/// This is useful when you have a dynamic list of middleware to apply,
/// such as in the DSL macro expansion.
/// 
/// ## Example
/// 
/// ```rust
/// let handler = MyEventHandler::new();
/// let middleware: Vec<Box<dyn Middleware>> = vec![
///     Box::new(LoggingMiddleware::new()),
///     Box::new(RetryMiddleware::new(3)),
/// ];
/// 
/// let wrapped = apply_middleware_vec(handler, middleware);
/// ```
pub fn apply_middleware_vec<H: EventHandler + 'static>(
    handler: H,
    middleware: Vec<Box<dyn Middleware>>,
) -> Box<dyn EventHandler> {
    if middleware.is_empty() {
        return Box::new(handler);
    }
    
    // Apply middleware in order - each wraps the previous
    let mut result: Box<dyn EventHandler> = Box::new(handler);
    
    for mw in middleware {
        result = Box::new(MiddlewareEventHandlerDyn {
            inner: result,
            middleware: mw,
        });
    }
    
    result
}

/// Dynamic version of MiddlewareEventHandler for use with trait objects
struct MiddlewareEventHandlerDyn {
    inner: Box<dyn EventHandler>,
    middleware: Box<dyn Middleware>,
}

impl EventHandler for MiddlewareEventHandlerDyn {
    fn transform(&self, event: crate::chain_event::ChainEvent) -> Vec<crate::chain_event::ChainEvent> {
        use crate::middleware::MiddlewareAction;
        
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

    fn observe(&self, event: &crate::chain_event::ChainEvent) -> crate::step::Result<()> {
        use crate::middleware::{MiddlewareAction, ErrorAction, StepError};
        
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

    fn aggregate(&mut self, event: crate::chain_event::ChainEvent) -> Option<Vec<crate::chain_event::ChainEvent>> {
        use crate::middleware::MiddlewareAction;
        
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
    
    fn processing_mode(&self) -> crate::lifecycle::ProcessingMode {
        self.inner.processing_mode()
    }
    
}