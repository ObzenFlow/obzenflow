// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{Middleware, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{async_infinite_source, flow, sink};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{AsyncInfiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::SourceError;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};
use tokio::sync::{mpsc, Mutex as TokioMutex};

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
    writer_id: WriterId,
    drain_calls: Arc<AtomicU64>,
    max_batch_size: usize,
}

impl TestAsyncInfiniteSource {
    fn new(max_batch_size: usize) -> (Self, mpsc::UnboundedSender<u64>, Arc<AtomicU64>) {
        let (tx, rx) = mpsc::unbounded_channel();
        let drain_calls = Arc::new(AtomicU64::new(0));
        (
            Self {
                rx: Arc::new(TokioMutex::new(rx)),
                writer_id: WriterId::from(StageId::new()),
                drain_calls: drain_calls.clone(),
                max_batch_size,
            },
            tx,
            drain_calls,
        )
    }
}

#[async_trait]
impl AsyncInfiniteSourceHandler for TestAsyncInfiniteSource {
    async fn next(&mut self) -> std::result::Result<Vec<ChainEvent>, SourceError> {
        let mut rx = self.rx.lock().await;

        let first = rx
            .recv()
            .await
            .ok_or_else(|| SourceError::Transport("test channel closed".to_string()))?;

        let mut out = vec![ChainEventFactory::data_event(
            self.writer_id,
            "TestEvent",
            json!({ "n": first }),
        )];

        while out.len() < self.max_batch_size {
            match rx.try_recv() {
                Ok(n) => out.push(ChainEventFactory::data_event(
                    self.writer_id,
                    "TestEvent",
                    json!({ "n": n }),
                )),
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
}

impl CollectSink {
    fn new() -> (Self, Arc<Mutex<Vec<ChainEvent>>>) {
        let events = Arc::new(Mutex::new(Vec::new()));
        (
            Self {
                events: events.clone(),
            },
            events,
        )
    }
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        self.events.lock().unwrap().push(event);
        Ok(DeliveryPayload::success(
            "collect_sink",
            DeliveryMethod::Custom("Collect".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct InjectFieldMiddleware;

impl Middleware for InjectFieldMiddleware {
    fn middleware_name(&self) -> &'static str {
        "inject_field"
    }

    fn pre_write(&self, event: &mut ChainEvent, _ctx: &MiddlewareContext) {
        if let ChainEventContent::Data { payload, .. } = &mut event.content {
            if payload.is_object() {
                payload["mw"] = json!(true);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct InjectFieldFactory;

impl MiddlewareFactory for InjectFieldFactory {
    fn create(
        &self,
        _config: &StageConfig,
        _control_middleware: Arc<ControlMiddlewareAggregator>,
    ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<Box<dyn Middleware>> {
        Ok(Box::new(InjectFieldMiddleware))
    }

    fn name(&self) -> &str {
        "inject_field"
    }
}

#[tokio::test]
async fn async_infinite_source_stop_interrupts_blocked_next_and_calls_drain() -> Result<()> {
    let (source, _tx, drain_calls) = TestAsyncInfiniteSource::new(32);
    let (sink, events) = CollectSink::new();

    let journal_root = unique_journal_dir("async_infinite_source_stop");

    let handle = flow! {
        name: "async_infinite_source_stop_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = async_infinite_source!(source);
            sink = sink!(sink);
        },

        topology: {
            source |> sink;
        }
    }
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
    let (source, tx, drain_calls) = TestAsyncInfiniteSource::new(32);
    let (sink, events) = CollectSink::new();

    let journal_root = unique_journal_dir("async_infinite_source_middleware");

    let handle = flow! {
        name: "async_infinite_source_middleware_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = async_infinite_source!(source, [
                InjectFieldFactory
            ]);
            sink = sink!(sink);
        },

        topology: {
            source |> sink;
        }
    }
    .await
    .map_err(|e| anyhow!("Failed to create flow: {e:?}"))?;

    handle.start().await?;
    wait_for_running(&handle).await?;

    tx.send(1)
        .map_err(|_| anyhow!("failed to send to source channel"))?;
    tx.send(2)
        .map_err(|_| anyhow!("failed to send to source channel"))?;

    tokio::time::sleep(Duration::from_millis(25)).await;

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

    for event in data_events {
        assert_eq!(
            event.payload().get("mw").and_then(|v| v.as_bool()),
            Some(true),
            "expected middleware to set payload.mw=true"
        );
    }

    assert_eq!(
        drain_calls.load(Ordering::Relaxed),
        1,
        "expected async infinite source drain() to be called once"
    );

    Ok(())
}
