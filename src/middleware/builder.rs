use crate::step::Step;
use super::{Middleware, MiddlewareStep};

/// Builder for composing multiple middleware with explicit ordering.
/// 
/// This builder provides a fluent API for stacking middleware layers. The key insight
/// is that each call to `with()` returns a NEW type that encodes the middleware stack
/// in the type system, providing compile-time safety and zero runtime overhead.
/// 
/// ## Design Pattern
/// 
/// This implements the [Builder Pattern](https://rust-unofficial.github.io/patterns/patterns/creational/builder.html)
/// with a twist - it uses Rust's type system to encode the middleware stack at compile time.
/// For more on this technique, see:
/// - [Type-State Builder Pattern](https://cliffle.com/blog/rust-typestate/)
/// - [Builder Pattern in Rust](https://doc.rust-lang.org/1.0.0/style/ownership/builders.html)
/// 
/// ## Example
/// 
/// ```rust
/// let step = MyProcessor::new()
///     .middleware()
///     .with(LoggingMiddleware::new())
///     .with(RetryMiddleware::new(3))
///     .with(MonitoringMiddleware::new())
///     .build();
/// 
/// // Execution order (LIFO):
/// // Event → Monitoring → Retry → Logging → MyProcessor
/// ```
pub struct MiddlewareBuilder<S> {
    step: S,
    /// Track middleware stack for debugging (applied LIFO)
    middleware_stack: Vec<String>,
}

impl<S: Step> MiddlewareBuilder<S> {
    /// Create a new builder wrapping the given step
    pub fn new(step: S) -> Self {
        Self {
            step,
            middleware_stack: vec![],
        }
    }

    /// Add a middleware to the stack.
    /// 
    /// IMPORTANT: Middleware is applied LIFO - last added is outermost!
    /// 
    /// ## Type Magic Explained
    /// 
    /// This method signature might look confusing at first:
    /// ```rust
    /// pub fn with<M: Middleware>(self, middleware: M) 
    ///     -> MiddlewareBuilder<MiddlewareStep<S, M>>
    /// ```
    /// 
    /// Let's break it down:
    /// - `self` (not &self) - Consumes the current builder
    /// - `M: Middleware` - Generic parameter M must implement Middleware
    /// - Returns a NEW builder with a DIFFERENT type
    /// 
    /// ## Example Type Transformation
    /// 
    /// ```rust
    /// // Start: MiddlewareBuilder<MyProcessor>
    /// let builder1 = MyProcessor::new().middleware();
    /// 
    /// // After .with(LoggingMiddleware):
    /// // Type becomes: MiddlewareBuilder<MiddlewareStep<MyProcessor, LoggingMiddleware>>
    /// let builder2 = builder1.with(LoggingMiddleware::new());
    /// 
    /// // After .with(RetryMiddleware):  
    /// // Type becomes: MiddlewareBuilder<MiddlewareStep<MiddlewareStep<MyProcessor, LoggingMiddleware>, RetryMiddleware>>
    /// let builder3 = builder2.with(RetryMiddleware::new(3));
    /// ```
    pub fn with<M: Middleware>(mut self, middleware: M) -> MiddlewareBuilder<MiddlewareStep<S, M>> {
        // Track the middleware name for debugging
        self.middleware_stack.push(std::any::type_name::<M>().to_string());

        // Create a new builder with a wrapped step
        MiddlewareBuilder {
            step: MiddlewareStep {
                inner: self.step,  // Current step becomes the inner
                middleware,        // New middleware wraps it
            },
            middleware_stack: self.middleware_stack,
        }
    }

    /// Build the final step with all middleware applied
    pub fn build(self) -> S {
        self.step
    }

    /// Debug method to understand middleware execution order
    /// 
    /// ## Example
    /// 
    /// ```rust
    /// let builder = step.middleware()
    ///     .with(A)
    ///     .with(B)
    ///     .with(C);
    /// 
    /// println!("{}", builder.explain_stack());
    /// // Output: "Event → C → B → A → Step"
    /// ```
    pub fn explain_stack(&self) -> String {
        if self.middleware_stack.is_empty() {
            "Event → Step".to_string()
        } else {
            format!("Event → {} → Step",
                self.middleware_stack.iter().rev().cloned().collect::<Vec<_>>().join(" → "))
        }
    }
}
