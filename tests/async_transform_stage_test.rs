// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::control::ControlMiddlewareAggregator;
use obzenflow_adapters::middleware::{Middleware, MiddlewareContext, MiddlewareFactory};
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::WriterId;
use obzenflow_dsl::{async_transform, flow, sink, source};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::pipeline::config::StageConfig;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    AsyncTransformHandler, FiniteSourceHandler, SinkHandler,
};
use serde_json::json;
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
                "TestEvent",
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
    fn new() -> (Self, Arc<AtomicU64>) {
        let count = Arc::new(AtomicU64::new(0));
        (
            Self {
                count: count.clone(),
            },
            count,
        )
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
            "counter_sink",
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
struct AsyncErrorTransform {
    drain_calls: Arc<AtomicU64>,
}

impl AsyncErrorTransform {
    fn new() -> (Self, Arc<AtomicU64>) {
        let drain_calls = Arc::new(AtomicU64::new(0));
        (
            Self {
                drain_calls: drain_calls.clone(),
            },
            drain_calls,
        )
    }
}

#[async_trait]
impl AsyncTransformHandler for AsyncErrorTransform {
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        tokio::time::sleep(Duration::from_millis(5)).await;

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
    ) -> Box<dyn Middleware> {
        Box::new(InjectFieldMiddleware)
    }

    fn name(&self) -> &str {
        "inject_field"
    }
}

#[derive(Clone, Debug)]
struct AsyncPassThroughTransform;

#[async_trait]
impl AsyncTransformHandler for AsyncPassThroughTransform {
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct AsyncDrainFailTransform {
    drain_calls: Arc<AtomicU64>,
}

impl AsyncDrainFailTransform {
    fn new() -> (Self, Arc<AtomicU64>) {
        let drain_calls = Arc::new(AtomicU64::new(0));
        (
            Self {
                drain_calls: drain_calls.clone(),
            },
            drain_calls,
        )
    }
}

#[async_trait]
impl AsyncTransformHandler for AsyncDrainFailTransform {
    async fn process(
        &self,
        event: ChainEvent,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        tokio::time::sleep(Duration::from_millis(5)).await;
        Ok(vec![event])
    }

    async fn drain(&mut self) -> std::result::Result<(), HandlerError> {
        self.drain_calls.fetch_add(1, Ordering::Relaxed);
        Err(HandlerError::Other("simulated drain failure".to_string()))
    }
}

#[tokio::test]
async fn async_transform_routes_error_kinds_to_correct_journal() -> Result<()> {
    let (counter_sink, counter) = EventCounterSink::new();
    let (transform, drain_calls) = AsyncErrorTransform::new();

    let journal_root = unique_journal_dir("async_transform_routing");
    let journal_root_for_flow = journal_root.clone();

    let handle = flow! {
        name: "async_transform_routing_test",
        journals: disk_journals(journal_root_for_flow.clone()),
        middleware: [],

        stages: {
            source = source!(TestEventSource::new());
            async_errors = async_transform!(transform);
            sink = sink!(counter_sink);
        },

        topology: {
            source |> async_errors;
            async_errors |> sink;
        }
    }
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

            if name.starts_with("Transform_async_errors_error_") && name.ends_with(".log") {
                error_journals.push(file_path);
            } else if name.starts_with("Transform_async_errors_") && name.ends_with(".log") {
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
async fn async_transform_applies_stage_middleware() -> Result<()> {
    let (sink, events) = CollectSink::new();

    let journal_root = unique_journal_dir("async_transform_middleware");

    let handle = flow! {
        name: "async_transform_middleware_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = source!(TestEventSource::new());
            mw_transform = async_transform!(AsyncPassThroughTransform, [
                InjectFieldFactory
            ]);
            sink = sink!(sink);
        },

        topology: {
            source |> mw_transform;
            mw_transform |> sink;
        }
    }
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
    for event in data_events {
        assert_eq!(
            event.payload().get("mw").and_then(|v| v.as_bool()),
            Some(true),
            "expected middleware to set payload.mw=true"
        );
    }

    Ok(())
}

#[tokio::test]
async fn async_transform_drain_failure_is_stage_level_failure() -> Result<()> {
    let (sink, _counter) = EventCounterSink::new();
    let (transform, drain_calls) = AsyncDrainFailTransform::new();

    let journal_root = unique_journal_dir("async_transform_drain_failure");

    let handle = flow! {
        name: "async_transform_drain_failure_test",
        journals: disk_journals(journal_root.clone()),
        middleware: [],

        stages: {
            source = source!(TestEventSource::new());
            drain_fail_transform = async_transform!(transform);
            sink = sink!(sink);
        },

        topology: {
            source |> drain_fail_transform;
            drain_fail_transform |> sink;
        }
    }
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
