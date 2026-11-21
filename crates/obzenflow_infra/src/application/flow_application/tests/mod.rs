// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::disk_journals;
use async_trait::async_trait;
use obzenflow_dsl::{flow, infinite_source, sink, source};
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use std::net::TcpListener;
use std::sync::Mutex;
use tokio::sync::oneshot;

#[cfg(all(feature = "tokio-console", tokio_unstable))]
mod console_telemetry;
mod lifecycle;
#[cfg(feature = "prometheus")]
mod prometheus;
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

fn noop_sink() -> obzenflow_adapters::sinks::DiscardSink<IdlePayload> {
    obzenflow_adapters::sinks::DiscardSink::new()
}
