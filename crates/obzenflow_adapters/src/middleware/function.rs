use obzenflow_core::event::chain_event::ChainEvent;
use super::{Middleware, MiddlewareAction, ErrorAction, MiddlewareContext};

/// Middleware implementation that wraps functions/closures.
/// 
/// This allows creating middleware from functions instead of implementing
/// the full trait on a struct. Perfect for simple, one-off behaviors.
/// 
/// ## Example
/// 
/// ```rust
/// use obzenflow_adapters::middleware::{FnMiddleware, MiddlewareAction, ErrorAction};
/// 
/// let middleware = FnMiddleware {
///     pre: |event, ctx| {
///         println!("Processing: {:?}", event);
///         MiddlewareAction::Continue
///     },
///     post: |event, results, ctx| {
///         println!("Produced {} results", results.len());
///     },
///     error: |event, ctx| {
///         println!("Error processing {:?}", event);
///         ErrorAction::Propagate
///     }
/// };
/// ```
pub struct FnMiddleware<F, G, H>
where
    F: Fn(&ChainEvent, &mut MiddlewareContext) -> MiddlewareAction,
    G: Fn(&ChainEvent, &[ChainEvent], &mut MiddlewareContext),
    H: Fn(&ChainEvent, &mut MiddlewareContext) -> ErrorAction,
{
    pub pre: F,
    pub post: G,
    pub error: H,
}

impl<F, G, H> Middleware for FnMiddleware<F, G, H>
where
    F: Fn(&ChainEvent, &mut MiddlewareContext) -> MiddlewareAction + Send + Sync,
    G: Fn(&ChainEvent, &[ChainEvent], &mut MiddlewareContext) + Send + Sync,
    H: Fn(&ChainEvent, &mut MiddlewareContext) -> ErrorAction + Send + Sync,
{
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Call the stored pre-processing function
        (self.pre)(event, ctx)
    }

    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        // Call the stored post-processing function
        (self.post)(event, results, ctx)
    }

    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> ErrorAction {
        // Call the stored error handling function
        (self.error)(event, ctx)
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
/// # use obzenflow_adapters::middleware::{middleware_fn, MiddlewareAction, TransformHandlerExt};
/// # use obzenflow_runtime_services::stages::common::handlers::TransformHandler;
/// # use obzenflow_core::ChainEvent;
/// # use async_trait::async_trait;
/// #
/// # struct MyStep;
/// # 
/// # #[async_trait]
/// # impl TransformHandler for MyStep {
/// #     fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
/// #         vec![event]
/// #     }
/// # }
/// #
/// # impl MyStep {
/// #     fn new() -> Self { Self }
/// # }
/// #
/// // Filter events by type
/// let filter = middleware_fn(|event, ctx| {
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
    F: Fn(&ChainEvent, &mut MiddlewareContext) -> MiddlewareAction + Send + Sync + 'static,
{
    FnMiddleware {
        pre,
        post: |_, _, _| {},  // No-op for post-processing
        error: |_, _| ErrorAction::Propagate,  // Default error handling
    }
}
