// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Current public inline-sink API for the untimed archive fixture.
use super::{DiscardTicks, Tick};
use obzenflow_runtime::stages::sink::{InlineSink, SinkDescription, SinkWriteFailure};

#[async_trait::async_trait]
impl InlineSink for DiscardTicks {
    type Input = Tick;
    fn describe(&self) -> SinkDescription {
        SinkDescription::method(
            obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
        )
        .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }
    async fn write(&mut self, _input: Tick) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}
