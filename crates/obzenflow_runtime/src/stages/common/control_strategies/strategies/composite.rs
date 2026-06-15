// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Composite strategy that combines multiple control event strategies

use super::super::{ProcessingContext, SignalDecision, SignalGate};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;

/// Composite strategy that stacks multiple strategies
///
/// When multiple middleware have control event requirements, this strategy
/// combines them using precedence rules: most restrictive action wins.
pub struct CompositeStrategy {
    strategies: Vec<Box<dyn SignalGate>>,
}

impl CompositeStrategy {
    pub fn new(strategies: Vec<Box<dyn SignalGate>>) -> Self {
        Self { strategies }
    }
}

impl SignalGate for CompositeStrategy {
    fn handle_eof(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        // Start with the least restrictive action
        let mut result = SignalDecision::Continue;

        // Let each strategy vote, keeping the most restrictive action
        for strategy in &self.strategies {
            let action = strategy.handle_eof(envelope, ctx);
            result = combine_actions(result, action);
        }

        result
    }

    fn handle_checkpoint(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        let mut result = SignalDecision::Continue;

        for strategy in &self.strategies {
            let action = strategy.handle_checkpoint(envelope, ctx);
            result = combine_actions(result, action);
        }

        result
    }

    fn handle_watermark(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        let mut result = SignalDecision::Continue;

        for strategy in &self.strategies {
            let action = strategy.handle_watermark(envelope, ctx);
            result = combine_actions(result, action);
        }

        result
    }

    fn handle_drain(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        let mut result = SignalDecision::Continue;

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
/// 1. Pause - Must wait for time-based conditions
/// 2. SuppressSignal - Dangerous, only if explicitly needed
/// 3. Continue - Default when all strategies agree
fn combine_actions(current: SignalDecision, new: SignalDecision) -> SignalDecision {
    use SignalDecision::*;

    match (current, new) {
        // Pause always wins - can't do anything while waiting
        (Pause(d1), Pause(d2)) => Pause(d1.max(d2)), // Take longer delay
        (Pause(d), _) | (_, Pause(d)) => Pause(d),

        // SuppressSignal only wins over Continue
        (SuppressSignal, Continue) | (Continue, SuppressSignal) => SuppressSignal,

        // Both Continue or both SuppressSignal
        (Continue, Continue) => Continue,
        (SuppressSignal, SuppressSignal) => SuppressSignal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_action_precedence() {
        use SignalDecision::*;

        // Pause beats everything
        assert_eq!(
            combine_actions(Pause(Duration::from_secs(5)), Continue),
            Pause(Duration::from_secs(5))
        );
        assert_eq!(
            combine_actions(SuppressSignal, Pause(Duration::from_secs(3))),
            Pause(Duration::from_secs(3))
        );

        // Longer delay wins
        assert_eq!(
            combine_actions(
                Pause(Duration::from_secs(5)),
                Pause(Duration::from_secs(10))
            ),
            Pause(Duration::from_secs(10))
        );

        // SuppressSignal only beats Continue
        assert_eq!(combine_actions(SuppressSignal, Continue), SuppressSignal);
    }
}
