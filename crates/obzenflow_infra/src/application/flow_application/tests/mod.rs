// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::disk_journals;
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;

use obzenflow_dsl::{flow, infinite_source, sink, source};
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use std::net::TcpListener;
use std::sync::Mutex;
use tokio::sync::oneshot;

mod lifecycle;
mod startup;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct IdlePayload;

impl TypedPayload for IdlePayload {
    const EVENT_TYPE: &'static str = "flow_application.idle";
}

#[derive(Clone, Debug)]
struct IdleInfiniteSource;

impl TypedInfiniteSourceHandler for IdleInfiniteSource {
    type Output = IdlePayload;

    fn next(&mut self) -> Result<Vec<Self::Output>, SourceError> {
        Ok(Vec::new())
    }
}

#[derive(Clone, Debug)]
struct OneShotSource {
    emitted: bool,
}

impl OneShotSource {
    fn new() -> Self {
        Self { emitted: false }
    }
}

impl TypedFiniteSourceHandler for OneShotSource {
    type Output = IdlePayload;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.emitted {
            Ok(None)
        } else {
            self.emitted = true;
            Ok(Some(vec![IdlePayload]))
        }
    }
}

#[derive(Clone, Debug)]
struct NoopSink;

#[async_trait]
impl InlineSink for NoopSink {
    type Input = IdlePayload;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: IdlePayload,
        _context: SinkWriteContext,
    ) -> obzenflow_runtime::stages::sink::SinkWriteResult {
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("test".to_string()),
            None,
        )))
    }
}

fn available_local_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral port");
    listener.local_addr().expect("local addr").port()
}
