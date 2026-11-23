// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::disk_journals;
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_adapters::sources::ValuesSource;
use obzenflow_dsl::{flow, sink, source};
use obzenflow_runtime::effects::{EffectContext, EffectError, Port};
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handlers::source::{
    Continuous, ContinuousRead, FiniteRead, InputReadiness, SourceRead,
};
use obzenflow_runtime::stages::common::handlers::{
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
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

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct NextInput;

#[obzenflow_runtime::effects::port]
#[async_trait]
trait TestInput {
    #[query(type = "flow_application.input.read")]
    async fn read(
        &self,
        from: &NextInput,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<IdlePayload, NextInput>, obzenflow_runtime::queries::QueryError>;
}

struct ScriptedInput {
    ends: bool,
    reads: Arc<AtomicUsize>,
}

#[obzenflow_runtime::effects::system("scripted-input")]
#[async_trait]
impl TestInput for ScriptedInput {
    async fn read(
        &self,
        _from: &NextInput,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<IdlePayload, NextInput>, obzenflow_runtime::queries::QueryError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.ends {
            Ok(FiniteRead::EndOfInput)
        } else {
            std::future::pending().await
        }
    }
}

struct FiniteInput(obzenflow_runtime::queries::QueryPort<dyn TestInput>);

impl FiniteInput {
    fn new(ends: bool, reads: Arc<AtomicUsize>) -> Self {
        Self(obzenflow_runtime::queries::QueryPort::to(ScriptedInput { ends, reads }))
    }

    fn waiting() -> Self {
        Self::new(false, Arc::default())
    }
}

impl TypedFiniteSourceHandler for FiniteInput {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload> {
        SourceRead::finite(self.0.clone(), NextInput, Ok)
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct NextLiveInput;

#[obzenflow_runtime::effects::port]
#[async_trait]
trait TestLiveInput: InputReadiness {
    #[query(type = "flow_application.live_input.take")]
    async fn take(
        &self,
        from: &NextLiveInput,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<IdlePayload, NextLiveInput>, obzenflow_runtime::queries::QueryError>;
}

type Readiness = Arc<dyn Fn() -> BoxFuture<'static, Result<(), SourceError>> + Send + Sync>;

struct NoLiveInput {
    ready: Readiness,
}

#[async_trait]
impl InputReadiness for NoLiveInput {
    async fn ready(&self) -> Result<(), SourceError> {
        (self.ready)().await
    }
}

#[obzenflow_runtime::effects::system("no-live-input")]
#[async_trait]
impl TestLiveInput for NoLiveInput {
    async fn take(
        &self,
        _from: &NextLiveInput,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<IdlePayload, NextLiveInput>, obzenflow_runtime::queries::QueryError> {
        Ok(ContinuousRead {
            records: Vec::new(),
            next: NextLiveInput,
        })
    }
}

struct IdleInfiniteSource {
    port: obzenflow_runtime::queries::QueryPort<dyn TestLiveInput>,
}

impl IdleInfiniteSource {
    fn new() -> Self {
        Self::waiting(|| Box::pin(std::future::pending()))
    }

    fn waiting(
        ready: impl Fn() -> BoxFuture<'static, Result<(), SourceError>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            port: obzenflow_runtime::queries::QueryPort::to(NoLiveInput {
                ready: Arc::new(ready),
            })
            ,
        }
    }
}

impl TypedInfiniteSourceHandler for IdleInfiniteSource {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload, Continuous> {
        SourceRead::continuous(self.port.clone(), NextLiveInput, Ok)
    }
}

fn counting_source(waits: Arc<AtomicUsize>) -> IdleInfiniteSource {
    IdleInfiniteSource::waiting(move || {
        waits.fetch_add(1, Ordering::SeqCst);
        Box::pin(std::future::pending())
    })
}

fn one_shot() -> ValuesSource<[IdlePayload; 1]> {
    ValuesSource::new([IdlePayload])
}

fn noop_sink() -> obzenflow_adapters::sinks::DiscardSink<IdlePayload> {
    obzenflow_adapters::sinks::DiscardSink::new()
}
