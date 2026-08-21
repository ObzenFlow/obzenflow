// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::sink::{
    SinkOperationError, SinkOperationResult, SinkWriteContext, SinkWriteResult, SinkWriter,
    SinkWriterLifecycleReport,
};

#[path = "../support/typed_sink.rs"]
mod support;
use support::Input;

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

    async fn flush(&mut self) -> SinkOperationResult<SinkWriterLifecycleReport> {
        let error: SinkOperationError = HandlerError::Fatal(StageFatal::new(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            "connector-forged fatal",
        ));
        Err(error)
    }
}

fn main() {}
