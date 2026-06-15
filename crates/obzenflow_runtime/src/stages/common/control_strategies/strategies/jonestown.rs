// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Default Jonestown strategy - continue to the runtime's poison-pill rule.

use super::super::{ProcessingContext, SignalDecision, SignalGate};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::ChainEvent;

/// The default "Jonestown Protocol" strategy.
///
/// The strategy itself does not decide the concrete lifecycle transition. It
/// returns [`SignalDecision::Continue`], and the runtime resolves EOF, Drain,
/// cycle buffering, and terminal state through the normal poison-pill rule.
pub struct JonestownSignalStrategy;

impl SignalGate for JonestownSignalStrategy {
    fn handle_eof(
        &self,
        _envelope: &EventEnvelope<ChainEvent>,
        _ctx: &mut ProcessingContext,
    ) -> SignalDecision {
        SignalDecision::Continue
    }
}
