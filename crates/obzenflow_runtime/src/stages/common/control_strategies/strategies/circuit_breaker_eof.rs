// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Circuit-breaker-aware EOF/Drain coordination.
//!
//! This strategy consults an `Arc<AtomicU8>` shared with the circuit breaker middleware to
//! delay accepting EOF/Drain while the breaker is actively probing (HalfOpen) or cooling down
//! (Open). This improves graceful shutdown semantics when the breaker is mid-recovery.

use super::super::{ControlEventAction, ControlEventStrategy, ProcessingContext};
use obzenflow_core::control_middleware::cb_state;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub struct CircuitBreakerEofStrategy {
    breaker_state: Arc<AtomicU8>,
    open_delay: Duration,
    half_open_delay: Duration,
}

impl CircuitBreakerEofStrategy {
    pub fn new(
        breaker_state: Arc<AtomicU8>,
        open_delay: Duration,
        half_open_delay: Duration,
    ) -> Self {
        Self {
            breaker_state,
            open_delay,
            half_open_delay,
        }
    }

    fn action_for(&self, ctx: &mut ProcessingContext) -> ControlEventAction {
        let state = self.breaker_state.load(Ordering::Acquire);
        match state {
            cb_state::HALF_OPEN => {
                if ctx.eof_attempts == 0 && !self.half_open_delay.is_zero() {
                    ctx.eof_attempts += 1;
                    return ControlEventAction::Delay(self.half_open_delay);
                }
            }
            cb_state::OPEN => {
                if ctx.eof_attempts == 0 && !self.open_delay.is_zero() {
                    ctx.eof_attempts += 1;
                    return ControlEventAction::Delay(self.open_delay);
                }
            }
            _ => {}
        }

        ControlEventAction::Forward
    }
}

impl ControlEventStrategy for CircuitBreakerEofStrategy {
    fn handle_eof(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
        self.action_for(ctx)
    }

    fn handle_drain(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
        self.action_for(ctx)
    }
}
