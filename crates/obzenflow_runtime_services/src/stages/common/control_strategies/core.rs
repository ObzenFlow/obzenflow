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
pub trait ControlEventStrategy: Send + Sync {
    /// Handle an EOF event
    fn handle_eof(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
        ctx: &mut ProcessingContext,
    ) -> ControlEventAction;

    /// Handle a watermark event
    fn handle_watermark(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
        // Default: always forward watermarks
        ControlEventAction::Forward
    }

    /// Handle a checkpoint event (when implemented)
    fn handle_checkpoint(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
        // Default: always forward checkpoints
        ControlEventAction::Forward
    }

    /// Handle a drain signal (when implemented)
    fn handle_drain(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
        // Default: always forward drain signals
        ControlEventAction::Forward
    }
}

/// Actions that a control event strategy can return
#[derive(Debug, Clone, PartialEq)]
pub enum ControlEventAction {
    /// Forward the control event downstream immediately
    Forward,

    /// Delay forwarding the control event
    Delay(Duration),

    /// Don't accept the control event yet, retry processing
    Retry,

    /// Skip this control event (dangerous! use with extreme caution)
    Skip,
}

/// Mutable context passed to control event strategies
pub struct ProcessingContext {
    /// Number of times EOF has been attempted (for retry strategies)
    pub eof_attempts: usize,

    /// Whether we're currently in a delay period
    pub in_delay: bool,

    /// Custom state that strategies can use
    pub custom_state: std::collections::HashMap<String, String>,

    /// Buffered EOF event for retry scenarios
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
