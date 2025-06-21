use crate::chain_event::ChainEvent;
use super::{Middleware, MiddlewareAction, ErrorAction, StepError};

/// Middleware implementation that wraps functions/closures.
/// 
/// This allows creating middleware from functions instead of implementing
/// the full trait on a struct. Perfect for simple, one-off behaviors.
/// 
/// ## Example
/// 
/// ```rust
/// let middleware = FnMiddleware {
///     pre: |event| {
///         println!("Processing: {:?}", event);
///         MiddlewareAction::Continue
///     },
///     post: |event, results| {
///         println!("Produced {} results", results.len());
///     },
///     error: |event, error| {
///         println!("Error processing {:?}: {}", event, error);
///         ErrorAction::Propagate
///     }
/// };
/// ```
pub struct FnMiddleware<F, G, H>
where
    F: Fn(&ChainEvent) -> MiddlewareAction,
    G: Fn(&ChainEvent, &mut Vec<ChainEvent>),
    H: Fn(&ChainEvent, &StepError) -> ErrorAction,
{
    pub pre: F,
    pub post: G,
    pub error: H,
}

impl<F, G, H> Middleware for FnMiddleware<F, G, H>
where
    F: Fn(&ChainEvent) -> MiddlewareAction + Send + Sync,
    G: Fn(&ChainEvent, &mut Vec<ChainEvent>) + Send + Sync,
    H: Fn(&ChainEvent, &StepError) -> ErrorAction + Send + Sync,
{
    fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
        // Call the stored pre-processing function
        (self.pre)(event)
    }

    fn post_handle(&self, event: &ChainEvent, results: &mut Vec<ChainEvent>) {
        // Call the stored post-processing function
        (self.post)(event, results)
    }

    fn on_error(&self, event: &ChainEvent, error: &StepError) -> ErrorAction {
        // Call the stored error handling function
        (self.error)(event, error)
    }
}

/// Convenience function for creating middleware from a single pre-processing function.
/// 
/// Most function-based middleware only needs pre-processing logic. This helper
/// makes it easy to create middleware that only cares about the pre-handle phase.
/// 
/// ## Example
/// 
/// ```rust
/// // Filter events by type
/// let filter = middleware_fn(|event| {
///     if event.event_type == "important" {
///         MiddlewareAction::Continue
///     } else {
///         MiddlewareAction::Skip(vec![])
///     }
/// });
/// 
/// // Apply to a step
/// let step = MyStep::new()
///     .middleware()
///     .with(filter)
///     .build();
/// ```
pub fn middleware_fn<F>(pre: F) -> impl Middleware
where
    F: Fn(&ChainEvent) -> MiddlewareAction + Send + Sync + 'static,
{
    FnMiddleware {
        pre,
        post: |_, _| {},  // No-op for post-processing
        error: |_, _| ErrorAction::Propagate,  // Default error handling
    }
}
