// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::sink::{SinkWriteContext, SinkWriteResult, SinkWriter};

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
        Err(HandlerError::ContractViolation(
            "connector-forged contract violation".into(),
        ))
    }
}

fn main() {}
