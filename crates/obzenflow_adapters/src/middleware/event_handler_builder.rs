use obzenflow_runtime_services::control_plane::stage_supervisor::event_handler::EventHandler;
use super::{Middleware, MiddlewareEventHandler};

/// Builder for composing multiple middleware with EventHandlers.
/// 
/// This follows the same pattern as MiddlewareBuilder but for EventHandlers,
/// providing a fluent API for stacking middleware layers with compile-time safety.
/// 
/// ## Example
/// 
/// ```rust
/// let handler = MyEventProcessor::new()
///     .middleware()
///     .with(LoggingMiddleware::new())
///     .with(RetryMiddleware::new(3))
///     .with(MonitoringMiddleware::new())
///     .build();
/// 
/// // Execution order (LIFO):
/// // Event → Monitoring → Retry → Logging → MyEventProcessor
/// ```
pub struct EventHandlerMiddlewareBuilder<H> {
    handler: H,
    /// Track middleware stack for debugging (applied LIFO)
    middleware_stack: Vec<String>,
}

impl<H: EventHandler> EventHandlerMiddlewareBuilder<H> {
    /// Create a new builder wrapping the given handler
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            middleware_stack: vec![],
        }
    }

    /// Add a middleware to the stack.
    /// 
    /// IMPORTANT: Middleware is applied LIFO - last added is outermost!
    /// 
    /// ## Type Transformation
    /// 
    /// Each call to `with()` wraps the current handler in a new MiddlewareEventHandler,
    /// creating a nested structure in the type system:
    /// 
    /// ```rust
    /// // Start: EventHandlerMiddlewareBuilder<MyHandler>
    /// let builder1 = MyHandler::new().middleware();
    /// 
    /// // After .with(LoggingMiddleware):
    /// // Type becomes: EventHandlerMiddlewareBuilder<MiddlewareEventHandler<MyHandler, LoggingMiddleware>>
    /// let builder2 = builder1.with(LoggingMiddleware::new());
    /// 
    /// // After .with(RetryMiddleware):  
    /// // Type becomes: EventHandlerMiddlewareBuilder<MiddlewareEventHandler<MiddlewareEventHandler<MyHandler, LoggingMiddleware>, RetryMiddleware>>
    /// let builder3 = builder2.with(RetryMiddleware::new(3));
    /// ```
    pub fn with<M: Middleware>(mut self, middleware: M) -> EventHandlerMiddlewareBuilder<MiddlewareEventHandler<H, M>> {
        // Track the middleware name for debugging
        self.middleware_stack.push(std::any::type_name::<M>().to_string());

        // Create a new builder with a wrapped handler
        EventHandlerMiddlewareBuilder {
            handler: MiddlewareEventHandler {
                inner: self.handler,  // Current handler becomes the inner
                middleware,          // New middleware wraps it
            },
            middleware_stack: self.middleware_stack,
        }
    }

    /// Build the final handler with all middleware applied
    pub fn build(self) -> H {
        self.handler
    }

    /// Debug method to understand middleware execution order
    /// 
    /// ## Example
    /// 
    /// ```rust
    /// let builder = handler.middleware()
    ///     .with(A)
    ///     .with(B)
    ///     .with(C);
    /// 
    /// println!("{}", builder.explain_stack());
    /// // Output: "Event → C → B → A → EventHandler"
    /// ```
    pub fn explain_stack(&self) -> String {
        if self.middleware_stack.is_empty() {
            "Event → EventHandler".to_string()
        } else {
            format!("Event → {} → EventHandler",
                self.middleware_stack.iter().rev().cloned().collect::<Vec<_>>().join(" → "))
        }
    }
}

/// Extension trait that adds the `.middleware()` method to all EventHandlers.
/// 
/// This enables the fluent builder pattern for applying middleware:
/// 
/// ```rust
/// let handler = MyEventHandler::new()
///     .middleware()
///     .with(middleware1)
///     .with(middleware2)
///     .build();
/// ```
pub trait EventHandlerExt: EventHandler + Sized {
    /// Start building a middleware stack for this handler
    fn middleware(self) -> EventHandlerMiddlewareBuilder<Self> {
        EventHandlerMiddlewareBuilder::new(self)
    }
}

// Blanket implementation for all EventHandlers
impl<H: EventHandler> EventHandlerExt for H {}