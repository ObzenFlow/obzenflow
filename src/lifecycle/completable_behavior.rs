//! Completable behavior implementation for source completion detection

use super::behavior::{HandlerBehavior, BehaviorCondition};

/// Behavior that tracks completion for sources
pub struct CompletableBehavior<F>
where
    F: Fn() -> bool + Send + Sync,
{
    is_complete: F,
}

impl<F> CompletableBehavior<F>
where
    F: Fn() -> bool + Send + Sync,
{
    /// Create a new CompletableBehavior with the given completion check
    pub fn new(is_complete: F) -> Self {
        Self { is_complete }
    }
}

impl<F> HandlerBehavior for CompletableBehavior<F>
where
    F: Fn() -> bool + Send + Sync,
{
    type State = bool; // Have we signaled completion?
    
    fn update_state(&mut self, state: &mut Self::State) {
        if !*state && (self.is_complete)() {
            *state = true;
        }
    }
    
    fn check_condition(&self, state: &Self::State) -> BehaviorCondition {
        if *state {
            BehaviorCondition::SourceComplete { natural: true }
        } else {
            BehaviorCondition::Normal
        }
    }
}