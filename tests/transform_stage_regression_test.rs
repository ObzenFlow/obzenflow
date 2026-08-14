// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform stage integration regression tests.

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::handler_observer;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::TypedPayload;
use obzenflow_core::{StageId, WriterId};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler, TypedTransformHandler,
};
use obzenflow_runtime::stages::observer::{HandlerObserver, HandlerObserverContext};
use obzenflow_runtime::stages::transform::TryMapTyped;
use serde::{Deserialize, Serialize};

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
}

impl TestEventSource {
    fn new() -> Self {
        Self { emitted: 0 }
    }
}

impl TypedFiniteSourceHandler for TestEventSource {
    type Output = TransformStageEvent;

    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<Self::Output>>,
        obzenflow_runtime::stages::common::handlers::source::traits::SourceError,
    > {
        if self.emitted < 2 {
            let index = self.emitted;
            self.emitted += 1;
            Ok(Some(vec![TransformStageEvent {
                index: index as u64,
            }]))
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
impl InlineSink for EventCounterSink {
    type Input = TransformStageEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _event: TransformStageEvent,
        _context: SinkWriteContext,
    ) -> std::result::Result<SinkWriteReport, HandlerError> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Count".to_string()),
            None,
        )))
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
impl InlineSink for CollectSink {
    type Input = TransformStageEvent;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        event: TransformStageEvent,
        _context: SinkWriteContext,
    ) -> std::result::Result<SinkWriteReport, HandlerError> {
        self.events
            .lock()
            .unwrap()
            .push(event.to_event(WriterId::from(StageId::new())));
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("Collect".to_string()),
            None,
        )))
    }
}

#[derive(Clone, Debug)]
struct ErrorTransform;

impl ErrorTransform {
    fn new() -> Self {
        Self
    }
}

impl TypedTransformHandler for ErrorTransform {
    type Input = TransformStageEvent;
    type Output = TransformStageEvent;

    fn process(
        &self,
        event: TransformStageEvent,
    ) -> std::result::Result<TransformStageEvent, HandlerError> {
        match event.index {
            0 => Err(HandlerError::Timeout("simulated timeout".to_string())),
            1 => Err(HandlerError::Domain("simulated domain error".to_string())),
            _ => Ok(event),
        }
    }
}

#[derive(Clone, Debug)]
struct CountHandlerOutputObserver {
    calls: Arc<AtomicU64>,
}

impl HandlerObserver for CountHandlerOutputObserver {
    fn after_handle(&self, _ctx: &HandlerObserverContext<'_>, outputs: &[ChainEvent]) {
        self.calls.fetch_add(
            outputs.iter().filter(|event| event.is_data()).count() as u64,
            Ordering::Relaxed,
        );
    }
}

#[derive(Clone, Debug)]
struct PassThroughTransform;

impl TypedTransformHandler for PassThroughTransform {
    type Input = TransformStageEvent;
    type Output = TransformStageEvent;

    fn process(
        &self,
        event: TransformStageEvent,
    ) -> std::result::Result<TransformStageEvent, HandlerError> {
        Ok(event)
    }
}

#[tokio::test]
async fn transform_routes_error_kinds_to_correct_journal() -> Result<()> {
    let counter = Arc::new(AtomicU64::new(0));
    let counter_for_flow = counter.clone();

    let journal_root = unique_journal_dir("transform_routing");
    let journal_root_for_flow = journal_root.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new();
        let transform_handler = ErrorTransform::new();
        let sink_handler = EventCounterSink::new(counter_for_flow);

        Ok(flow! {
            name: "transform_routing_test",
            journals: disk_journals(journal_root_for_flow.clone()),

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
async fn typed_try_map_success_and_failure_use_the_supervisor_journal_contract() -> Result<()> {
    let delivered = Arc::new(AtomicU64::new(0));
    let delivered_for_flow = delivered.clone();
    let journal_root = unique_journal_dir("typed_try_map_journal");
    let journal_root_for_flow = journal_root.clone();

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let source_handler = TestEventSource::new();
        let transform_handler = TryMapTyped::new(|event: TransformStageEvent| {
            if event.index == 0 {
                Err("index zero is invalid")
            } else {
                Ok(event)
            }
        });
        let sink_handler = EventCounterSink::new(delivered_for_flow);

        Ok(flow! {
            name: "typed_try_map_journal_test",
            journals: disk_journals(journal_root_for_flow.clone()),

            stages: {
                source = source!(TransformStageEvent => source_handler);
                try_map = transform!(TransformStageEvent -> TransformStageEvent => transform_handler);
                sink = sink!(TransformStageEvent => sink_handler);
            },

            topology: {
                source |> try_map;
                try_map |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow::anyhow!("Failed to create typed try-map flow: {error:?}"))?;

    handle.run().await?;
    assert_eq!(
        delivered.load(Ordering::Relaxed),
        1,
        "the successful conversion reaches the ordinary data lane"
    );

    let mut data_journal = None;
    let mut error_journal = None;
    for flow_dir in std::fs::read_dir(journal_root.join("flows"))? {
        let flow_dir = flow_dir?.path();
        if !flow_dir.is_dir() {
            continue;
        }
        for file in std::fs::read_dir(flow_dir)? {
            let path = file?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with("Transform_try_map_error_") && name.ends_with(".log") {
                error_journal = Some(path);
            } else if name.starts_with("Transform_try_map_") && name.ends_with(".log") {
                data_journal = Some(path);
            }
        }
    }

    async fn read_journal(
        path: std::path::PathBuf,
    ) -> Result<Vec<obzenflow_core::EventEnvelope<ChainEvent>>> {
        let journal = obzenflow_infra::journal::DiskJournal::<ChainEvent>::with_owner(
            path,
            JournalOwner::stage(StageId::new()),
        )?;
        journal
            .read_causally_ordered()
            .await
            .map_err(|error| anyhow::anyhow!("{error:?}"))
    }

    let error_events = read_journal(error_journal.expect("try-map error journal exists"))
        .await?
        .into_iter()
        .map(|envelope| envelope.event)
        .filter(|event| event.is_data())
        .collect::<Vec<_>>();
    assert_eq!(error_events.len(), 1);
    assert_eq!(error_events[0].payload()["index"], serde_json::json!(0));
    assert!(matches!(
        &error_events[0].processing_info.status,
        ProcessingStatus::Error {
            kind: Some(ErrorKind::Unknown),
            message,
            ..
        } if message.contains("typed try-map failed: index zero is invalid")
    ));

    let successful_events = read_journal(data_journal.expect("try-map data journal exists"))
        .await?
        .into_iter()
        .map(|envelope| envelope.event)
        .filter(|event| TransformStageEvent::event_type_matches(&event.event_type()))
        .collect::<Vec<_>>();
    assert_eq!(successful_events.len(), 1);
    assert_eq!(
        successful_events[0].payload()["index"],
        serde_json::json!(1)
    );
    assert!(matches!(
        successful_events[0].processing_info.status,
        ProcessingStatus::Success
    ));

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

            stages: {
                source = source!(TransformStageEvent => source_handler);
                mw_transform = transform!(TransformStageEvent -> TransformStageEvent => transform_handler, observers: [
                    handler_observer(
                        "count_handler_outputs",
                        CountHandlerOutputObserver { calls: observer_calls_for_flow.clone() }
                    )
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
        "the handler observer sees every live data output"
    );

    Ok(())
}
