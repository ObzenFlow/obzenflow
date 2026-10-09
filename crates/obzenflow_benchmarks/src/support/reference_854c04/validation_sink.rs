// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public inline-sink adapter for reference 854c04bab261477ad4bec813369244e84f2ffb20.
//! Equivalent successful Noop delivery; fixture execution remains untimed.
use super::{DiscardTicks, Tick};
use obzenflow_runtime::stages::sink::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    SinkWriteResult,
};

#[async_trait::async_trait]
impl InlineSink for DiscardTicks {
    type Input = Tick;
    fn describe(&self) -> SinkDescription {
        SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, _input: Tick, _context: SinkWriteContext) -> SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
            None,
        )))
    }
}
