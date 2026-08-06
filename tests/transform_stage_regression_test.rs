// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform stage integration regression tests.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareMaterializationContext,
    MiddlewareOverrideKey, MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::TypedPayload;
use obzenflow_core::WriterId;
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    FiniteSourceHandler, SinkHandler, TransformHandler,
};
use obzenflow_runtime::stages::observer::{
    ObserverCommitResult, ObserverReport, OutputCommitObserver, OutputCommitObserverContext,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// File-local payload for the transform stage regression test. The JSON shape
/// matches what `TestEventSource` emits; the type fingerprints the stage
/// contract per FLOWIP-114c.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct TransformStageEvent {
    index: u64,
}

impl TypedPayload for TransformStageEvent {
    const EVENT_TYPE: &'static str = "transform.stage.event";
}
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

#[derive(Clone, Debug)]
struct TestEventSource {
    emitted: usize,
    writer_id: WriterId,
}

impl TestEventSource {
    fn new() -> Self {
        Self {
            emitted: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TestEventSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted < 2 {
            let index = self.emitted;
            self.emitted += 1;
            Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                <TransformStageEvent as TypedPayload>::EVENT_TYPE,
                json!({ "index": index }),
            )]))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone, Debug)]
struct EventCounterSink {
    count: Arc<AtomicU64>,
}

impl EventCounterSink {
    fn new(count: Arc<AtomicU64>) -> Self {
        Self { count }
    }
}

#[async_trait]
impl SinkHandler for EventCounterSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> std::result::Result<DeliveryPayload, HandlerError> {
        if event.is_data() {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Count".to_string()),
            None,
        ))
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    events: Arc<Mutex<Vec<ChainEvent>>>,
}

impl CollectSink {
    fn new(events: Arc<Mutex<Vec<ChainEvent>>>) -> Self {
        Self { events }
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
struct ErrorTransform {
    drain_calls: Arc<AtomicU64>,
}

impl ErrorTransform {
    fn new(drain_calls: Arc<AtomicU64>) -> Self {
        Self { drain_calls }
    }
}

#[async_trait]
impl TransformHandler for ErrorTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        let index = event
            .payload()
            .get("index")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        match index {
            0 => Err(HandlerError::Timeout("simulated timeout".to_string())),
            1 => Err(HandlerError::Domain("simulated domain error".to_string())),
            _ => Ok(vec![event]),
        }
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
        Ok(())
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

#[derive(Clone, Debug)]
struct PassThroughTransform;

#[async_trait]
impl TransformHandler for PassThroughTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct DrainFailTransform {
    drain_calls: Arc<AtomicU64>,
}

impl DrainFailTransform {
    fn new(drain_calls: Arc<AtomicU64>) -> Self {
        Self { drain_calls }
    }
}

#[async_trait]
impl TransformHandler for DrainFailTransform {
    fn process(&self, event: ChainEvent) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
        Err(HandlerError::Other("simulated drain failure".to_string()))
    }
}

#[tokio::test]
async fn transform_routes_error_kinds_to_correct_journal() -> Result<()> {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_flow = counter.clone();
    let drain_calls = Arc::new(AtomicU64::new(0));
    let drain_calls_for_flow = drain_calls.clone();

    let journal_root = unique_journal_dir("transform_routing");
    let journal_root_for_flow = journal_root.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new();
        let transform_handler = ErrorTransform::new(drain_calls_for_flow);
        let sink_handler = EventCounterSink::new(counter_for_flow);

        Ok(flow! {
            name: "transform_routing_test",
            journals: disk_journals(journal_root_for_flow.clone()),
            middleware: [],

            stages: {
                source = source!(TransformStageEvent => source_handler);
                errors = transform!(TransformStageEvent -> TransformStageEvent => transform_handler);
                sink = sink!(TransformStageEvent => sink_handler);
            },

            topology: {
                source |> errors;
                errors |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    // Only the Domain error should be written to the transform data journal (and reach the sink).
    assert_eq!(counter.load(Ordering::Relaxed), 1);

    // The handler should be drained exactly once after the subscription queue is empty.
    assert_eq!(drain_calls.load(Ordering::Relaxed), 1);

    // Find the transform stage data/error journals on disk and validate routing.
    let flows_dir = journal_root.join("flows");
    let mut data_journals = Vec::new();
    let mut error_journals = Vec::new();

    for flow_dir in std::fs::read_dir(&flows_dir)? {
        let flow_dir = flow_dir?.path();
        if !flow_dir.is_dir() {
            continue;
        }

        for file in std::fs::read_dir(&flow_dir)? {
            let file_path = file?.path();
            let Some(name) = file_path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };

            if name.starts_with("Transform_errors_error_") && name.ends_with(".log") {
                error_journals.push(file_path);
            } else if name.starts_with("Transform_errors_") && name.ends_with(".log") {
                data_journals.push(file_path);
            }
        }
    }

    assert_eq!(
        data_journals.len(),
        1,
        "expected exactly one transform data journal"
    );
    assert_eq!(
        error_journals.len(),
        1,
        "expected exactly one transform error journal"
    );

    async fn read_chain_journal(
        path: std::path::PathBuf,
    ) -> Result<Vec<obzenflow_core::EventEnvelope<ChainEvent>>> {
        let journal: obzenflow_infra::journal::DiskJournal<ChainEvent> =
            obzenflow_infra::journal::DiskJournal::with_owner(
                path,
                JournalOwner::stage(StageId::new()),
            )?;
        journal
            .read_causally_ordered()
            .await
            .map_err(|e| anyhow::anyhow!("{e:?}"))
    }

    let error_events: Vec<ChainEvent> = read_chain_journal(error_journals[0].clone())
        .await?
        .into_iter()
        .map(|env| env.event)
        .filter(|e| e.is_data())
        .collect();

    assert_eq!(
        error_events.len(),
        1,
        "expected 1 data event in error journal"
    );
    assert_eq!(
        error_events[0]
            .payload()
            .get("index")
            .and_then(|v| v.as_u64()),
        Some(0)
    );
    assert!(matches!(
        error_events[0].processing_info.status,
        ProcessingStatus::Error {
            kind: Some(ErrorKind::Timeout),
            ..
        }
    ));

    let data_events: Vec<ChainEvent> = read_chain_journal(data_journals[0].clone())
        .await?
        .into_iter()
        .map(|env| env.event)
        .filter(|e| matches!(e.content, ChainEventContent::Data { .. }))
        .collect();

    // Transform data journal should contain the Domain error event (index=1) and no Timeout event (index=0).
    let mut saw_domain = false;
    for event in data_events {
        if event.payload().get("index").and_then(|v| v.as_u64()) == Some(1) {
            assert!(matches!(
                event.processing_info.status,
                ProcessingStatus::Error {
                    kind: Some(ErrorKind::Domain),
                    ..
                }
            ));
            saw_domain = true;
        }
        if event.payload().get("index").and_then(|v| v.as_u64()) == Some(0) {
            anyhow::bail!("did not expect index=0 event in transform data journal");
        }
    }

    assert!(
        saw_domain,
        "expected Domain error event in transform data journal"
    );

    // Sanity: ensure EOF exists somewhere in the transform data journal.
    let has_eof = read_chain_journal(data_journals[0].clone())
        .await?
        .into_iter()
        .any(|env| {
            matches!(
                env.event.content,
                ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
            )
        });
    assert!(has_eof, "expected EOF in transform data journal");

    Ok(())
}

#[tokio::test]
async fn transform_applies_stage_middleware() -> Result<()> {
    let events = Arc::new(Mutex::new(Vec::new()));
    let events_for_flow = events.clone();
    let observer_calls = Arc::new(AtomicU64::new(0));
    let observer_calls_for_flow = observer_calls.clone();

    let journal_root = unique_journal_dir("transform_middleware");

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new();
        let transform_handler = PassThroughTransform;
        let sink_handler = CollectSink::new(events_for_flow);

        Ok(flow! {
            name: "transform_middleware_test",
            journals: disk_journals(journal_root.clone()),
            middleware: [],

            stages: {
                source = source!(TransformStageEvent => source_handler);
                mw_transform = transform!(TransformStageEvent -> TransformStageEvent => transform_handler, [
                    CountDataCommitFactory { calls: observer_calls_for_flow.clone() }
                ]);
                sink = sink!(TransformStageEvent => sink_handler);
            },

            topology: {
                source |> mw_transform;
                mw_transform |> sink;
            }
        })
    })
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

#[tokio::test]
async fn transform_drain_failure_is_stage_level_failure() -> Result<()> {
    let counter = Arc::new(AtomicU64::new(0));
    let drain_calls = Arc::new(AtomicU64::new(0));
    let drain_calls_for_flow = drain_calls.clone();

    let journal_root = unique_journal_dir("transform_drain_failure");

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new();
        let transform_handler = DrainFailTransform::new(drain_calls_for_flow);
        let sink_handler = EventCounterSink::new(counter);

        Ok(flow! {
            name: "transform_drain_failure_test",
            journals: disk_journals(journal_root.clone()),
            middleware: [],

            stages: {
                source = source!(TransformStageEvent => source_handler);
                drain_fail_transform = transform!(TransformStageEvent -> TransformStageEvent => transform_handler);
                sink = sink!(TransformStageEvent => sink_handler);
            },

            topology: {
                source |> drain_fail_transform;
                drain_fail_transform |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    let run_result = tokio::time::timeout(Duration::from_secs(10), handle.run()).await;
    let err = match run_result {
        Ok(Ok(())) => anyhow::bail!("expected flow to fail due to transform drain failure"),
        Ok(Err(e)) => e,
        Err(_) => anyhow::bail!("flow did not complete within timeout"),
    };

    assert_eq!(drain_calls.load(Ordering::Relaxed), 1);
    assert!(
        format!("{err:?}").contains("Failed to drain transform handler"),
        "expected stage-level drain failure to surface; got: {err:?}"
    );

    Ok(())
}
