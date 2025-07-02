//! Composite strategy that combines multiple control event strategies

use crate::control_plane::stages::control_strategy::{ControlEventStrategy, ControlEventAction, ProcessingContext};
use obzenflow_core::event::event_envelope::EventEnvelope;

/// Composite strategy that stacks multiple strategies
/// 
/// When multiple middleware have control event requirements, this strategy
/// combines them using precedence rules: most restrictive action wins.
pub struct CompositeStrategy {
    strategies: Vec<Box<dyn ControlEventStrategy>>,
}

impl CompositeStrategy {
    pub fn new(strategies: Vec<Box<dyn ControlEventStrategy>>) -> Self {
        Self { strategies }
    }
}

impl ControlEventStrategy for CompositeStrategy {
    fn handle_eof(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        // Start with the least restrictive action
        let mut result = ControlEventAction::Forward;
        
        // Let each strategy vote, keeping the most restrictive action
        for strategy in &self.strategies {
            let action = strategy.handle_eof(envelope, ctx);
            result = combine_actions(result, action);
        }
        
        result
    }
    
    fn handle_checkpoint(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        let mut result = ControlEventAction::Forward;
        
        for strategy in &self.strategies {
            let action = strategy.handle_checkpoint(envelope, ctx);
            result = combine_actions(result, action);
        }
        
        result
    }
    
    fn handle_watermark(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        let mut result = ControlEventAction::Forward;
        
        for strategy in &self.strategies {
            let action = strategy.handle_watermark(envelope, ctx);
            result = combine_actions(result, action);
        }
        
        result
    }
    
    fn handle_drain(&self, envelope: &EventEnvelope, ctx: &mut ProcessingContext) -> ControlEventAction {
        let mut result = ControlEventAction::Forward;
        
        for strategy in &self.strategies {
            let action = strategy.handle_drain(envelope, ctx);
            result = combine_actions(result, action);
        }
        
        result
    }
}

/// Combine two actions using precedence rules
/// 
/// Precedence (most to least restrictive):
/// 1. Delay - Must wait for time-based conditions
/// 2. Retry - Can retry but shouldn't exit yet  
/// 3. Skip - Dangerous, only if explicitly needed
/// 4. Forward - Default when all strategies agree
fn combine_actions(current: ControlEventAction, new: ControlEventAction) -> ControlEventAction {
    use ControlEventAction::*;
    
    match (current, new) {
        // Delay always wins - can't do anything while waiting
        (Delay(d1), Delay(d2)) => Delay(d1.max(d2)), // Take longer delay
        (Delay(d), _) | (_, Delay(d)) => Delay(d),
        
        // Retry wins over Forward/Skip
        (Retry, _) | (_, Retry) => Retry,
        
        // Skip only wins over Forward
        (Skip, Forward) | (Forward, Skip) => Skip,
        
        // Both Forward or both Skip
        (Forward, Forward) => Forward,
        (Skip, Skip) => Skip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    #[test]
    fn test_action_precedence() {
        use ControlEventAction::*;
        
        // Delay beats everything
        assert_eq!(combine_actions(Delay(Duration::from_secs(5)), Forward), Delay(Duration::from_secs(5)));
        assert_eq!(combine_actions(Retry, Delay(Duration::from_secs(3))), Delay(Duration::from_secs(3)));
        
        // Longer delay wins
        assert_eq!(
            combine_actions(Delay(Duration::from_secs(5)), Delay(Duration::from_secs(10))),
            Delay(Duration::from_secs(10))
        );
        
        // Retry beats Forward and Skip
        assert_eq!(combine_actions(Retry, Forward), Retry);
        assert_eq!(combine_actions(Skip, Retry), Retry);
        
        // Skip only beats Forward
        assert_eq!(combine_actions(Skip, Forward), Skip);
    }
}