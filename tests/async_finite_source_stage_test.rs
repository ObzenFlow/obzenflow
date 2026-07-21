// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::TypedPayload;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{async_source, flow, sink};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{AsyncFiniteSourceHandler, SinkHandler};
use obzenflow_runtime::stages::observer::{
    ObserverCommitResult, ObserverReport, OutputCommitObserver, OutputCommitObserverContext,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use serde_json::json;

/// File-local payload for the async-finite source stage test. The JSON
/// shape matches what `TestAsyncEventSource` emits; the type fingerprints
/// the stage contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AsyncTestEvent {
    index: u64,
}

impl TypedPayload for AsyncTestEvent {
    const EVENT_TYPE: &'static str = "async_finite_source.event";
}
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

#[derive(Clone, Debug)]
struct TestAsyncEventSource {
    emitted: usize,
    writer_id: WriterId,
    drain_calls: Arc<AtomicU64>,
}

impl TestAsyncEventSource {
    fn new() -> (Self, Arc<AtomicU64>) {
        let drain_calls = Arc::new(AtomicU64::new(0));
        (
            Self {
                emitted: 0,
                writer_id: WriterId::from(StageId::new()),
                drain_calls: drain_calls.clone(),
            },
            drain_calls,
        )
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for TestAsyncEventSource {
    async fn next(&mut self) -> std::result::Result<Option<Vec<ChainEvent>>, SourceError> {
        if self.emitted < 2 {
            let index = self.emitted;
            self.emitted += 1;
            tokio::task::yield_now().await;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                <AsyncTestEvent as TypedPayload>::EVENT_TYPE,
                json!({ "index": index }),
            )]))
        } else {
            Ok(None)
        }
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
            DeliveryMethod::Custom("Collect".to_string()),
            None,
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
                Ok(MiddlewareSurfaceAttachment::OutputCommitObserver(Arc::new(
                    CountDataCommitObserver {
                        calls: self.calls.clone(),
                    },
                )))
            }
            other => Err(MiddlewareFactoryError::materialization_failed(
                self.label(),
                &context.config.name,
                std::io::Error::other(format!("unsupported observer surface {other:?}")),
            )),
        }
    }
}

#[tokio::test]
async fn async_finite_source_emits_events_and_calls_drain() -> Result<()> {
    let (source, drain_calls) = TestAsyncEventSource::new();
    let (sink, events) = CollectSink::new();

    let journal_root = unique_journal_dir("async_finite_source_basic");

    let handle = flow! {
        name: "async_finite_source_basic_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = async_source!(AsyncTestEvent => source);
            sink = sink!(AsyncTestEvent => sink);
        },

        topology: {
            source |> sink;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    let data_events: Vec<ChainEvent> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.is_data())
        .cloned()
        .collect();
    assert_eq!(
        data_events.len(),
        2,
        "expected two data events to reach the sink"
    );
    assert_eq!(
        drain_calls.load(Ordering::Relaxed),
        1,
        "expected async source drain() to be called once"
    );

    Ok(())
}

#[tokio::test]
async fn async_finite_source_applies_stage_middleware() -> Result<()> {
    let (source, _drain_calls) = TestAsyncEventSource::new();
    let (sink, events) = CollectSink::new();
    let observer_calls = Arc::new(AtomicU64::new(0));
    let observer_calls_for_flow = observer_calls.clone();

    let journal_root = unique_journal_dir("async_finite_source_middleware");

    let handle = flow! {
        name: "async_finite_source_middleware_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = async_source!(AsyncTestEvent => source, [
                CountDataCommitFactory { calls: observer_calls_for_flow.clone() }
            ]);
            sink = sink!(AsyncTestEvent => sink);
        },

        topology: {
            source |> sink;
        }
    }
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    let data_events: Vec<ChainEvent> = events
        .lock()
        .unwrap()
        .iter()
        .filter(|event| event.is_data())
        .cloned()
        .collect();

    assert_eq!(
        data_events.len(),
        2,
        "expected two data events to reach the sink"
    );
    assert_eq!(
        observer_calls.load(Ordering::Relaxed),
        data_events.len() as u64,
        "the typed output-commit observer sees every data event without mutating it"
    );

    Ok(())
}
