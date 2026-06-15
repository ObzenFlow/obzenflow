// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core types and traits for control event handling strategies
//!
//! This module contains the fundamental abstractions for implementing
//! control event handling strategies in stage supervisors.

use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;
use std::time::Duration;

/// Strategy for handling control events in stage supervisors
pub trait SignalGate: Send + Sync {
    /// Handle an EOF event
    fn handle_eof(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> SignalDecision;

    /// Handle a watermark event
    fn handle_watermark(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        // Default: always forward watermarks
        SignalDecision::Continue
    }

    /// Handle a checkpoint event (when implemented)
    fn handle_checkpoint(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        // Default: always forward checkpoints
        SignalDecision::Continue
    }

    /// Handle a drain signal (when implemented)
    fn handle_drain(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        // Default: always forward drain signals
        SignalDecision::Continue
    }
}

/// What a signal gate decides for an inbound control signal (FLOWIP-115c).
#[derive(Debug, Clone, PartialEq)]
pub enum SignalDecision {
    /// Let runtime run its normal rule for this signal: forward, forward and
    /// drain, buffer at a cycle entry, or suppress a non-terminal signal.
    Continue,

    /// Hold the signal for the given duration, then re-resolve. Time is
    /// relative; the supervisor turns it into a deadline when it suspends.
    Pause(Duration),

    /// Suppress this signal entirely (dangerous; the hook-local stop result).
    SuppressSignal,
}

/// Mutable context passed to control event strategies
pub struct ProcessingContext {
    /// Number of times EOF handling has been deferred by the current strategy.
    pub eof_attempts: usize,

    /// Whether we're currently in a delay period
    pub in_delay: bool,

    /// Custom state that strategies can use
    pub custom_state: std::collections::HashMap<String, String>,

    /// Buffered EOF event for control-flow coordination scenarios.
    pub buffered_eof: Option<EventEnvelope<ChainEvent>>,
}

impl ProcessingContext {
    pub fn new() -> Self {
        Self {
            eof_attempts: 0,
            in_delay: false,
            custom_state: std::collections::HashMap::new(),
            buffered_eof: None,
        }
    }
}

impl Default for ProcessingContext {
    fn default() -> Self {
        Self::new()
    }
}
