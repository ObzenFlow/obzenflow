use crate::chain_event::ChainEvent;
use crate::step::Step;
use super::{Middleware, MiddlewareAction};

/// MiddlewareStep as a decorator that wraps a Step with additional behaviour.
///
/// The middleware intercepts the event processing at three points:
/// - Before processing (pre_handle)
/// - After successful processing (post_handle)
/// - On error (on_error)
///
/// ## Example
///
/// ```rust
/// let step = MiddlewareStep::new(
///     MyProcessor::new(),
///     LoggingMiddleware::new()
/// );
/// ```
///
/// ## Nesting
///
/// Multiple middleware can be nested, creating an "onion" like pattern:
///
/// ```
/// MiddlewareStep {
///     inner: MiddlewareStep {
///         inner: MyProcessor,
///         middleware: LoggingMiddleware,
///     },
///     middleware: RetryMiddleware,
/// }
/// ```
#[derive(Debug, Clone)]
pub struct MiddlewareStep<S, M>
where
    S: Step,
    M: Middleware,
{
    pub(super) inner: S,
    pub(super) middleware: M,
}

impl<S, M> MiddlewareStep<S, M>
where
    S: Step,
    M: Middleware,
{
    /// Create a new MiddlewareStep wrapping the given step with middleware
    pub fn new(inner: S, middleware: M) -> Self {
        Self { inner, middleware }
    }
}

impl<S, M> Step for MiddlewareStep<S, M>
where
    S: Step,
    M: Middleware,
{
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        // Pre-processing phase
        match self.middleware.pre_handle(&event) {
            MiddlewareAction::Continue => {
                // Process with inner step - Step::handle doesn't return Result
                let mut results = self.inner.handle(event.clone());
                // Post-processing phase
                self.middleware.post_handle(&event, &mut results);
                results
            }
            MiddlewareAction::Skip(results) => results,
            MiddlewareAction::Abort => vec![],
        }
    }
    
    fn step_type(&self) -> crate::step::StepType {
        self.inner.step_type()
    }
}
