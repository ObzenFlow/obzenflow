//! Builder pattern for composing EventHandlers with behaviors

use crate::lifecycle::{EventHandler, BehaviorEventHandler, CompletableBehavior};

/// Builder for composing EventHandlers with behaviors
pub struct BehaviorBuilder<H: EventHandler> {
    handler: H,
}

impl<H: EventHandler> BehaviorBuilder<H> {
    /// Create a new BehaviorBuilder with the given handler
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
    
    /// Add completion tracking
    pub fn with_completion<F>(self, is_complete: F) -> BehaviorEventHandler<H, CompletableBehavior<F>>
    where
        F: Fn() -> bool + Send + Sync,
    {
        BehaviorEventHandler::new(
            self.handler,
            CompletableBehavior::new(is_complete),
        )
    }
    
    /// Build the handler without any behaviors
    pub fn build(self) -> H {
        self.handler
    }
}