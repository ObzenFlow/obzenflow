// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::journal::disk_journals;
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_adapters::sources::ValuesSource;
use obzenflow_core::event::payloads::execution_payload::ReattachGuarantee;
use obzenflow_dsl::{flow, sink, source};
use obzenflow_runtime::effects::{EffectContext, EffectError, Port};
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handlers::source::{ContinuousRead, FiniteRead, SourceRead};
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
    #[effect(type = "flow_application.input.read", exactly_once, records = reply)]
    async fn read(
        &self,
        from: &NextInput,
        ctx: &EffectContext,
    ) -> Result<FiniteRead<IdlePayload, NextInput>, EffectError>;
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
        _ctx: &EffectContext,
    ) -> Result<FiniteRead<IdlePayload, NextInput>, EffectError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if self.ends {
            Ok(FiniteRead::EndOfInput)
        } else {
            std::future::pending().await
        }
    }
}

struct FiniteInput(Port<dyn TestInput>);

impl FiniteInput {
    fn new(ends: bool, reads: Arc<AtomicUsize>) -> Self {
        Self(Port::to(ScriptedInput { ends, reads }).own_instance())
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
trait TestLiveInput {
    #[effect(type = "flow_application.live_input.take", at_most_once, records = reply)]
    async fn take(
        &self,
        from: &NextLiveInput,
        ctx: &EffectContext,
    ) -> Result<ContinuousRead<IdlePayload, NextLiveInput>, EffectError>;
}

struct NoLiveInput;

#[obzenflow_runtime::effects::system("no-live-input")]
#[async_trait]
impl TestLiveInput for NoLiveInput {
    async fn take(
        &self,
        _from: &NextLiveInput,
        _ctx: &EffectContext,
    ) -> Result<ContinuousRead<IdlePayload, NextLiveInput>, EffectError> {
        Ok(ContinuousRead {
            records: Vec::new(),
            next: NextLiveInput,
        })
    }
}

type Readiness = Arc<dyn Fn() -> BoxFuture<'static, Result<(), SourceError>> + Send + Sync>;

struct IdleInfiniteSource {
    port: Port<dyn TestLiveInput>,
    ready: Readiness,
}

impl IdleInfiniteSource {
    fn new() -> Self {
        Self::waiting(|| Box::pin(std::future::pending()))
    }

    fn waiting(
        ready: impl Fn() -> BoxFuture<'static, Result<(), SourceError>> + Send + Sync + 'static,
    ) -> Self {
        Self {
            port: Port::to(NoLiveInput).own_instance(),
            ready: Arc::new(ready),
        }
    }
}

impl TypedInfiniteSourceHandler for IdleInfiniteSource {
    type Output = IdlePayload;

    fn read(&self) -> SourceRead<IdlePayload> {
        let ready = self.ready.clone();
        SourceRead::continuous(
            self.port.clone(),
            NextLiveInput,
            move || ready(),
            ReattachGuarantee::AtMostOnce,
            Ok,
        )
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
