// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::sink::{
    SinkConnector, SinkDescription, SinkOperationResult, SinkWriteContext, SinkWriteResult,
    SinkWriter, SinkWriterInitContext,
};
use obzenflow_core::event::{StageFatalCode, StageFatalReason};

#[path = "../support/typed_sink.rs"]
mod support;
use support::Input;

struct Connector;
struct Writer;

#[async_trait]
impl SinkWriter for Writer {
    type Input = Input;

    async fn write(
        &mut self,
        _input: Self::Input,
        _context: SinkWriteContext,
    ) -> SinkWriteResult {
        unreachable!()
    }
}

#[async_trait]
impl SinkConnector for Connector {
    type Input = Input;
    type Writer = Writer;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn open(
        &self,
        _context: SinkWriterInitContext,
    ) -> SinkOperationResult<Self::Writer> {
        Err(HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            "connector-forged fatal",
        )))
    }
}

fn main() {}
