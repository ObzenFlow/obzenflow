// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Control signal dispatch helper
//!
//! Replaces the duplicated match block that every supervisor uses to map a
//! `FlowControlPayload` variant to the appropriate `ControlEventStrategy`
//! method call.

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::{ChainEvent, EventEnvelope};

use super::core::{ControlEventAction, ControlEventStrategy, ProcessingContext};

/// Dispatch a control signal to the appropriate strategy method and return the
/// resulting action.
///
/// This eliminates the per-supervisor match block that maps
/// `FlowControlPayload` variants to `ControlEventStrategy` method calls.
pub(crate) fn dispatch_control_signal(
    strategy: &dyn ControlEventStrategy,
    signal: &FlowControlPayload,
    envelope: &EventEnvelope<ChainEvent>,
    processing_ctx: &mut ProcessingContext,
) -> ControlEventAction {
    match signal {
        FlowControlPayload::Eof { .. } => strategy.handle_eof(envelope, processing_ctx),
        FlowControlPayload::Watermark { .. } => strategy.handle_watermark(envelope, processing_ctx),
        FlowControlPayload::Checkpoint { .. } => {
            strategy.handle_checkpoint(envelope, processing_ctx)
        }
        FlowControlPayload::Drain => strategy.handle_drain(envelope, processing_ctx),
        _ => ControlEventAction::Forward,
    }
}
