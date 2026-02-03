// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Default Jonestown strategy - forward EOF and terminate immediately

use super::super::{ControlEventAction, ControlEventStrategy, ProcessingContext};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;

/// The default "Jonestown Protocol" strategy
///
/// Named after the infamous event where everyone "drank the Kool-Aid" together,
/// this strategy ensures coordinated shutdown across the entire pipeline.
/// When an EOF is received, it is immediately forwarded downstream and the
/// stage terminates its processing loop.
pub struct JonestownStrategy;

impl ControlEventStrategy for JonestownStrategy {
    fn handle_eof(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> ControlEventAction {
        // Simple and direct: forward EOF immediately
        ControlEventAction::Forward
    }
}
