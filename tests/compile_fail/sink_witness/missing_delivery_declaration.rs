// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/typed_sink.rs"]
mod support;
use async_trait::async_trait;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::sink::{
    SinkInputContext, SinkTerminalOutcome, TypedSinkConsumeReport, TypedSinkHandler,
};
use support::Input;

#[derive(Clone, Debug)]
struct MissingDeclaration;

#[async_trait]
impl TypedSinkHandler for MissingDeclaration {
    type Input = Input;

    async fn consume(
        &mut self,
        _input: Self::Input,
        _context: SinkInputContext,
    ) -> Result<TypedSinkConsumeReport, HandlerError> {
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        ))
    }
}

fn main() {}
