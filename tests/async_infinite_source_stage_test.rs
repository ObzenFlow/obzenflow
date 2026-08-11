// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::{StageId, TypedPayload, WriterId};
use obzenflow_dsl::{async_infinite_source, flow, sink, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    SinkDeliveryDeclaration, SinkInputContext, SinkTerminalOutcome,
    TypedAsyncInfiniteSourceHandler, TypedSinkConsumeReport, TypedSinkHandler,
};
use obzenflow_runtime::stages::observer::{
    ObserverCommitResult, ObserverReport, OutputCommitObserver, OutputCommitObserverContext,
};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde::{Deserialize, Serialize};

/// File-local payload for the async-infinite source stage test. The JSON
/// shape matches what `TestAsyncInfiniteSource` emits; the type
/// fingerprints the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AsyncInfiniteEvent {
    n: u64,
}

impl TypedPayload for AsyncInfiniteEvent {
    const EVENT_TYPE: &'static str = "async_infinite_source.event";
}
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Mutex as TokioMutex, Notify};

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

async fn wait_for_running(handle: &FlowHandle) -> Result<()> {
    let mut rx = handle.state_receiver();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if matches!(*rx.borrow(), PipelineState::Running) {
                return Ok(());
            }
            rx.changed()
                .await
                .map_err(|_| anyhow!("pipeline state channel closed"))?;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for pipeline to reach Running"))?
}

#[derive(Clone, Debug)]
struct TestAsyncInfiniteSource {
    rx: Arc<TokioMutex<mpsc::UnboundedReceiver<u64>>>,
    drain_calls: Arc<AtomicU64>,
    max_batch_size: usize,
}

impl TestAsyncInfiniteSource {
    fn new(
        rx: mpsc::UnboundedReceiver<u64>,
        drain_calls: Arc<AtomicU64>,
        max_batch_size: usize,
    ) -> Self {
        Self {
            rx: Arc::new(TokioMutex::new(rx)),
            drain_calls,
            max_batch_size,
        }
    }
}

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for TestAsyncInfiniteSource {
    type Output = AsyncInfiniteEvent;

    async fn next(&mut self) -> std::result::Result<Vec<Self::Output>, SourceError> {
        let mut rx = self.rx.lock().await;

        let first = rx
            .recv()
            .await
            .ok_or_else(|| SourceError::Transport("test channel closed".to_string()))?;

        let mut out = vec![AsyncInfiniteEvent { n: first }];

        while out.len() < self.max_batch_size {
            match rx.try_recv() {
                Ok(n) => out.push(AsyncInfiniteEvent { n }),
                Err(_) => break,
            }
        }

        Ok(out)
    }

    async fn drain(&mut self) -> std::result::Result<(), SourceError> {
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
    event_ready: Arc<Notify>,
}

impl CollectSink {
    fn new(events: Arc<Mutex<Vec<ChainEvent>>>, event_ready: Arc<Notify>) -> Self {
        Self {
            events,
            event_ready,
        }
    }
}

#[async_trait]
impl TypedSinkHandler for CollectSink {
    type Input = AsyncInfiniteEvent;

    fn delivery_declaration(&self) -> SinkDeliveryDeclaration {
        SinkDeliveryDeclaration::undeclared()
    }

    async fn consume(
        &mut self,
        event: AsyncInfiniteEvent,
        _context: SinkInputContext,
    ) -> std::result::Result<TypedSinkConsumeReport, HandlerError> {
        self.events
            .lock()
            .unwrap()
            .push(event.to_event(WriterId::from(StageId::new())));
        self.event_ready.notify_waiters();
        Ok(TypedSinkConsumeReport::terminal(
            SinkTerminalOutcome::success(DeliveryMethod::Custom("Collect".to_string()), None),
        ))
    }
}

#[derive(Clone, Debug)]
struct CountDataCommitObserver {
    calls: Arc<AtomicU64>,
}

impl OutputCommitObserver for CountDataCommitObserver {
    fn label(&self) -> &'static str {
        "count_data_commit"
    }

    fn before_output_commit(
        &self,
        _ctx: &OutputCommitObserverContext<'_>,
        event: &mut ChainEvent,
    ) -> ObserverCommitResult {
        if event.is_data() {
            self.calls.fetch_add(1, Ordering::Relaxed);
        }
        Ok(ObserverReport::empty())
    }
}

#[derive(Clone, Debug)]
struct CountDataCommitFactory {
    calls: Arc<AtomicU64>,
}

impl MiddlewareFactory for CountDataCommitFactory {
    fn label(&self) -> &'static str {
        "count_data_commit"
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<CountDataCommitFactory>("count_data_commit")
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(self.label(), vec![MiddlewareSurfaceKind::OutputCommit])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|err| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, err)
        })?;
        match request.surface.kind() {
            MiddlewareSurfaceKind::OutputCommit => {
                Ok(MiddlewareSurfaceAttachment::output_commit_observer(
                    Arc::new(CountDataCommitObserver {
                        calls: self.calls.clone(),
                    }),
                ))
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!("unsupported observer surface {other:?}")),
            )),
        }
    }
}

async fn wait_for_data_event_count(
    events: &Arc<Mutex<Vec<ChainEvent>>>,
    event_ready: &Notify,
    expected: usize,
) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let observed = events
                .lock()
                .unwrap()
                .iter()
                .filter(|event| event.is_data())
                .count();
            if observed >= expected {
                return Ok(());
            }
            event_ready.notified().await;
        }
    })
    .await
    .map_err(|_| anyhow!("timeout waiting for {expected} data events"))?
}

#[tokio::test]
async fn async_infinite_source_stop_interrupts_blocked_next_and_calls_drain() -> Result<()> {
    let (_tx, rx) = mpsc::unbounded_channel();
    let drain_calls = Arc::new(AtomicU64::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let event_ready = Arc::new(Notify::new());
    let journal_root = unique_journal_dir("async_infinite_source_stop");
    let drain_calls_for_flow = Arc::clone(&drain_calls);
    let events_for_flow = Arc::clone(&events);
    let event_ready_for_flow = Arc::clone(&event_ready);

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestAsyncInfiniteSource::new(rx, drain_calls_for_flow, 32);
        let sink = CollectSink::new(events_for_flow, event_ready_for_flow);

        Ok(flow! {
            name: "async_infinite_source_stop_test",
            journals: disk_journals(journal_root),

            stages: {
                source = async_infinite_source!(AsyncInfiniteEvent => source);
                sink = sink!(AsyncInfiniteEvent => sink);
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("Failed to create flow: {e:?}"))?;

    handle.start().await?;
    wait_for_running(&handle).await?;

    handle.stop().await?;

    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;

    assert_eq!(
        drain_calls.load(Ordering::Relaxed),
        1,
        "expected async infinite source drain() to be called once"
    );

    let data_events: Vec<ChainEvent> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.is_data())
        .cloned()
        .collect();
    assert!(
        data_events.is_empty(),
        "expected no data events when the source never receives submissions"
    );

    Ok(())
}

#[tokio::test]
async fn async_infinite_source_emits_events_and_applies_stage_middleware() -> Result<()> {
    let (tx, rx) = mpsc::unbounded_channel();
    let drain_calls = Arc::new(AtomicU64::new(0));
    let events = Arc::new(Mutex::new(Vec::new()));
    let event_ready = Arc::new(Notify::new());
    let observer_calls = Arc::new(AtomicU64::new(0));
    let journal_root = unique_journal_dir("async_infinite_source_middleware");
    let drain_calls_for_flow = Arc::clone(&drain_calls);
    let events_for_flow = Arc::clone(&events);
    let event_ready_for_flow = Arc::clone(&event_ready);
    let observer_calls_for_flow = Arc::clone(&observer_calls);

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source = TestAsyncInfiniteSource::new(rx, drain_calls_for_flow, 32);
        let sink = CollectSink::new(events_for_flow, event_ready_for_flow);

        Ok(flow! {
            name: "async_infinite_source_middleware_test",
            journals: disk_journals(journal_root),

            stages: {
                source = async_infinite_source!(AsyncInfiniteEvent => source, observers: [
                    CountDataCommitFactory { calls: observer_calls_for_flow }
                ]);
                sink = sink!(AsyncInfiniteEvent => sink);
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow!("Failed to create flow: {e:?}"))?;

    handle.start().await?;
    wait_for_running(&handle).await?;

    tx.send(1)
        .map_err(|_| anyhow!("failed to send to source channel"))?;
    tx.send(2)
        .map_err(|_| anyhow!("failed to send to source channel"))?;

    wait_for_data_event_count(&events, &event_ready, 2).await?;

    handle.stop().await?;

    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .map_err(|_| anyhow!("timeout waiting for pipeline to terminate after stop"))??;

    let data_events: Vec<ChainEvent> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.is_data())
        .cloned()
        .collect();
    assert!(
        data_events.len() >= 2,
        "expected at least two data events to reach the sink"
    );

    assert_eq!(
        observer_calls.load(Ordering::Relaxed),
        data_events.len() as u64,
        "the typed output-commit observer sees every data event without mutating it"
    );

    assert_eq!(
        drain_calls.load(Ordering::Relaxed),
        1,
        "expected async infinite source drain() to be called once"
    );

    Ok(())
}
