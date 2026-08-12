// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::sink::traits::SinkHandler;
use obzenflow_runtime::stages::sink::{
    SinkDescription, SinkWriteContext, SinkTerminalOutcome, SinkWriteReport,
    InlineSink,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Input;

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "compile_fail.sink.input";
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OtherInput;

impl TypedPayload for OtherInput {
    const EVENT_TYPE: &'static str = "compile_fail.sink.other_input";
}

#[derive(Clone, Debug)]
pub struct RawHandler;

#[async_trait]
impl SinkHandler for RawHandler {
    async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
    }
}

#[derive(Clone, Debug)]
pub struct WrongInputHandler;

#[async_trait]
impl InlineSink for WrongInputHandler {
    type Input = OtherInput;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: Self::Input,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success_via(DeliveryMethod::Noop, None),
        ))
    }
}
