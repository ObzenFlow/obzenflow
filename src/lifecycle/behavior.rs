//! Handler behaviors - composable decorators for EventHandlers
//!
//! This module provides the BehaviorEventHandler pattern, allowing EventHandlers
//! to be augmented with lifecycle behaviors like completion detection, state tracking,
//! and resource management.

use crate::chain_event::ChainEvent;
use crate::lifecycle::{EventHandler, ProcessingMode};
use crate::step::Result;
use serde_json::Value;

/// Trait for behaviors that augment EventHandlers
pub trait HandlerBehavior: Send + Sync {
    /// Configuration/state for this behavior
    type State: Send + Sync + Default;
    
    /// Called periodically to update behavior state
    fn update_state(&mut self, state: &mut Self::State);
    
    /// Check any conditions this behavior tracks
    fn check_condition(&self, state: &Self::State) -> BehaviorCondition;
}

/// Conditions that behaviors can signal
#[derive(Debug, Clone)]
pub enum BehaviorCondition {
    /// Normal operation
    Normal,
    /// Source has completed naturally
    SourceComplete { natural: bool },
    /// Handler state changed
    StateChanged { new_state: HandlerState },
    /// Custom lifecycle condition
    Custom { name: String, data: Value },
}

/// Handler lifecycle states
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandlerState {
    /// Handler is initializing
    Initializing,
    /// Handler is ready to process
    Ready,
    /// Handler is in a degraded state
    Degraded,
    /// Handler is shutting down
    ShuttingDown,
}

/// A decorator that adds behavior to any EventHandler
pub struct BehaviorEventHandler<H, B>
where
    H: EventHandler,
    B: HandlerBehavior,
{
    pub(crate) inner: H,
    pub(crate) behavior: B,
    pub(crate) behavior_state: B::State,
}

impl<H, B> BehaviorEventHandler<H, B>
where
    H: EventHandler,
    B: HandlerBehavior,
{
    /// Create a new BehaviorEventHandler
    pub fn new(inner: H, behavior: B) -> Self {
        Self {
            inner,
            behavior,
            behavior_state: B::State::default(),
        }
    }
    
    /// Get a reference to the inner handler
    pub fn inner(&self) -> &H {
        &self.inner
    }
    
    /// Get a mutable reference to the inner handler
    pub fn inner_mut(&mut self) -> &mut H {
        &mut self.inner
    }
    
    /// Update behavior state and check conditions
    pub fn update_behavior(&mut self) -> BehaviorCondition {
        self.behavior.update_state(&mut self.behavior_state);
        self.behavior.check_condition(&self.behavior_state)
    }
}

impl<H, B> EventHandler for BehaviorEventHandler<H, B>
where
    H: EventHandler,
    B: HandlerBehavior,
{
    fn transform(&self, event: ChainEvent) -> Vec<ChainEvent> {
        self.inner.transform(event)
    }
    
    fn observe(&self, event: &ChainEvent) -> Result<()> {
        self.inner.observe(event)
    }
    
    fn aggregate(&mut self, event: ChainEvent) -> Option<Vec<ChainEvent>> {
        self.inner.aggregate(event)
    }
    
    fn processing_mode(&self) -> ProcessingMode {
        self.inner.processing_mode()
    }
    
    fn check_behaviors(&mut self) -> Option<BehaviorCondition> {
        self.behavior.update_state(&mut self.behavior_state);
        Some(self.behavior.check_condition(&self.behavior_state))
    }
}