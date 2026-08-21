// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/typed_sink.rs"]
mod support;
use async_trait::async_trait;
use obzenflow_runtime::stages::sink::{
    SinkConnector, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport, SinkWriter,
    SinkWriterInitContext,
};
use support::Input;

#[derive(Debug)]
struct MissingDescription;

#[derive(Debug)]
struct MissingDescriptionWriter;

#[async_trait]
impl SinkWriter for MissingDescriptionWriter {
    type Input = Input;

    async fn write(
        &mut self,
        _input: Self::Input,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(
            SinkTerminalOutcome::success_via(
                obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                None,
            ),
        ))
    }
}

#[async_trait]
impl SinkConnector for MissingDescription {
    type Input = Input;
    type Writer = MissingDescriptionWriter;

    async fn open(
        &self,
        _context: SinkWriterInitContext,
    ) -> obzenflow_runtime::stages::sink::SinkOperationResult<Self::Writer> {
        Ok(MissingDescriptionWriter)
    }
}

fn main() {}
