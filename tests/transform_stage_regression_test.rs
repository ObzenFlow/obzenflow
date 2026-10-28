// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform stage integration regression tests.

mod replay_testkit;

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_adapters::middleware::handler_observer;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::payloads::stage_processing_payload::StageProcessingFailed;
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::event::ChainPayload;
use obzenflow_core::{JournalRecord, StageId, TypedPayload, WriterId};
use obzenflow_dsl::{flow, sink, source, transform, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkWriteFailure, TypedFiniteSourceHandler, TypedTransformHandler,
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
        SinkDescription::method(DeliveryMethod::Custom("Count".to_string()))
    }

    async fn write(&mut self, _event: TransformStageEvent) -> Result<(), SinkWriteFailure> {
        self.count.fetch_add(1, Ordering::Relaxed);
        Ok(())
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
        SinkDescription::method(DeliveryMethod::Custom("Collect".to_string()))
    }

    async fn write(&mut self, event: TransformStageEvent) -> Result<(), SinkWriteFailure> {
        self.events
            .lock()
            .unwrap()
            .push(event.to_event(WriterId::from(StageId::new())));
        Ok(())
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
            outputs
                .iter()
                .filter(|event| event.consumes_data_credit())
                .count() as u64,
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

    assert_eq!(counter.load(Ordering::Relaxed), 0);

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
    ) -> Result<Vec<JournalRecord<ChainPayload>>> {
        Ok(replay_testkit::read_journal_envelopes::<ChainEvent>(&path).await)
    }

    let failures: Vec<StageProcessingFailed> = read_chain_journal(error_journals[0].clone())
        .await?
        .into_iter()
        .filter_map(|env| match env.into_parts().1 {
            ChainPayload::Execution(ExecutionPayload::StageProcessingFailed(failure)) => {
                Some(failure)
            }
            _ => None,
        })
        .collect();
    let recorded: Vec<_> = failures
        .iter()
        .map(|failure| {
            (
                failure.input.payload["index"].as_u64(),
                failure.kind.clone(),
                failure.detail.as_str(),
            )
        })
        .collect();
    assert_eq!(
        recorded,
        [
            (Some(0), ErrorKind::Timeout, "simulated timeout"),
            (Some(1), ErrorKind::Domain, "simulated domain error"),
        ],
        "every kind of handler error is recorded in the error journal"
    );

    let data_events: Vec<ChainEvent> = read_chain_journal(data_journals[0].clone())
        .await?
        .into_iter()
        .map(|env| env.authored())
        .filter(|e| e.consumes_data_credit())
        .collect();
    assert!(
        data_events.is_empty(),
        "no rejected input enters the transform data journal: {data_events:?}"
    );

    // Sanity: ensure EOF exists somewhere in the transform data journal.
    let has_eof = read_chain_journal(data_journals[0].clone())
        .await?
        .into_iter()
        .any(|env| {
            matches!(
                env.payload,
                ChainPayload::FlowControl(FlowControlPayload::Eof { .. })
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

    async fn read_journal(path: std::path::PathBuf) -> Result<Vec<JournalRecord<ChainPayload>>> {
        Ok(replay_testkit::read_journal_envelopes::<ChainEvent>(&path).await)
    }

    let error_events = read_journal(error_journal.expect("try-map error journal exists"))
        .await?
        .into_iter()
        .map(|envelope| envelope.authored())
        .filter(|event| event.consumes_data_credit())
        .collect::<Vec<_>>();
    assert_eq!(error_events.len(), 1);
    assert_eq!(
        error_events[0].event_type(),
        "runtime.stage.processing_failed"
    );
    assert_eq!(
        error_events[0].payload()["input"]["payload"]["index"],
        serde_json::json!(0)
    );
    assert_eq!(
        error_events[0].payload()["detail"],
        serde_json::json!("index zero is invalid")
    );
    assert!(matches!(
        &error_events[0].processing.status,
        ProcessingStatus::Error {
            kind: Some(ErrorKind::Unknown),
            message,
            ..
        } if message == "index zero is invalid"
    ));

    let successful_events = read_journal(data_journal.expect("try-map data journal exists"))
        .await?
        .into_iter()
        .map(|envelope| envelope.authored())
        .filter(|event| TransformStageEvent::matches_event_type(&event.event_type()))
        .collect::<Vec<_>>();
    assert_eq!(successful_events.len(), 1);
    assert_eq!(
        successful_events[0].payload()["index"],
        serde_json::json!(1)
    );
    assert!(matches!(
        successful_events[0].processing.status,
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
                mw_transform = transform!(TransformStageEvent -> TransformStageEvent with { handler_observer(
                        "count_handler_outputs",
                        CountHandlerOutputObserver { calls: observer_calls_for_flow.clone() }
                    ) } => transform_handler);
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
        .filter(|event| event.consumes_data_credit())
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CheckedEvent {
    index: u64,
}

impl TypedPayload for CheckedEvent {
    const EVENT_TYPE: &'static str = "transform.checked.event";
}

#[derive(Clone, Debug)]
struct RejectFirst;

impl TypedTransformHandler for RejectFirst {
    type Input = TransformStageEvent;
    type Output = TransformStageEvent;

    fn process(
        &self,
        event: TransformStageEvent,
    ) -> std::result::Result<TransformStageEvent, HandlerError> {
        match event.index {
            0 => Err(HandlerError::Validation("event 0 is invalid".to_string())),
            _ => Ok(event),
        }
    }
}

#[derive(Clone, Debug)]
struct CheckFirst;

impl TypedTransformHandler for CheckFirst {
    type Input = TransformStageEvent;
    type Output = CheckedEvent;

    fn process(
        &self,
        event: TransformStageEvent,
    ) -> std::result::Result<CheckedEvent, HandlerError> {
        match event.index {
            0 => Err(HandlerError::Validation("event 0 is invalid".to_string())),
            index => Ok(CheckedEvent { index }),
        }
    }
}

#[derive(Clone, Debug)]
struct IndexSink<T> {
    written: Arc<Mutex<Vec<u64>>>,
    index: fn(&T) -> u64,
}

#[async_trait]
impl<T: TypedPayload + Clone + Send + Sync + std::fmt::Debug + 'static> InlineSink
    for IndexSink<T>
{
    type Input = T;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("Index".to_string()))
    }

    async fn write(&mut self, event: T) -> Result<(), SinkWriteFailure> {
        self.written.lock().unwrap().push((self.index)(&event));
        Ok(())
    }
}

fn index_sink<T>(index: fn(&T) -> u64) -> (IndexSink<T>, Arc<Mutex<Vec<u64>>>) {
    let written = Arc::new(Mutex::new(Vec::new()));
    (
        IndexSink {
            written: written.clone(),
            index,
        },
        written,
    )
}

async fn the_one_rejection(run: &std::path::Path, stage: &str) -> StageProcessingFailed {
    let data = replay_testkit::read_stage_envelopes_appended(run, stage).await;
    assert!(
        !data.iter().any(|record| matches!(
            record.envelope.provenance.event.processing.status,
            ProcessingStatus::Error { .. }
        )),
        "{stage}'s data journal holds no error-marked row"
    );
    let failures = replay_testkit::processing_failures(run, stage).await;
    let [(record, failure)] = failures.as_slice() else {
        panic!("{stage} records each rejection once: {failures:?}");
    };
    assert_eq!(
        record.envelope.provenance.event.writer_id,
        replay_testkit::stage_writer(run, stage)
    );
    assert_eq!(failure.kind, ErrorKind::Validation);
    assert_eq!(failure.detail, "event 0 is invalid");
    assert_eq!(failure.input.event_type.0, TransformStageEvent::EVENT_TYPE);
    assert_eq!(failure.input.payload["index"], serde_json::json!(0));
    failure.clone()
}

#[tokio::test]
async fn a_sink_behind_a_same_type_validator_writes_only_valid_records() -> Result<()> {
    let journal_root = unique_journal_dir("same_type_validator");
    let journals = journal_root.clone();
    let (written_sink, written) = index_sink(|event: &TransformStageEvent| event.index);
    let handle = FlowDefinition::materialize(move |_| {
        let source_handler = TestEventSource::new();
        let written_sink = written_sink.clone();
        Ok(flow! {
            name: "same_type_validator",
            journals: disk_journals(journals.clone()),
            stages: {
                source = source!(TransformStageEvent => source_handler);
                check = transform!(TransformStageEvent -> TransformStageEvent => RejectFirst);
                written = sink!(TransformStageEvent => written_sink);
            },
            topology: {
                source |> check;
                check |> written;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow::anyhow!("Failed to create flow: {error:?}"))?;
    handle.run().await?;

    assert_eq!(*written.lock().unwrap(), [1]);
    let run = replay_testkit::latest_run_dir(&journal_root);
    the_one_rejection(&run, "check").await;
    assert!(replay_testkit::read_stage_error_envelopes(&run, "written")
        .await
        .is_empty());
    Ok(())
}

#[tokio::test]
async fn a_type_changing_validator_keeps_its_rejection_out_of_its_data() -> Result<()> {
    let journal_root = unique_journal_dir("type_changing_validator");
    let journals = journal_root.clone();
    let (written_sink, written) = index_sink(|event: &CheckedEvent| event.index);
    let handle = FlowDefinition::materialize(move |_| {
        let source_handler = TestEventSource::new();
        let written_sink = written_sink.clone();
        Ok(flow! {
            name: "type_changing_validator",
            journals: disk_journals(journals.clone()),
            stages: {
                source = source!(TransformStageEvent => source_handler);
                check = transform!(TransformStageEvent -> CheckedEvent => CheckFirst);
                written = sink!(CheckedEvent => written_sink);
            },
            topology: {
                source |> check;
                check |> written;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow::anyhow!("Failed to create flow: {error:?}"))?;
    handle.run().await?;

    assert_eq!(*written.lock().unwrap(), [1]);
    let run = replay_testkit::latest_run_dir(&journal_root);
    the_one_rejection(&run, "check").await;
    let data_types: Vec<_> = replay_testkit::read_stage_envelopes_appended(&run, "check")
        .await
        .into_iter()
        .filter(|record| matches!(record.payload, ChainPayload::Fact(_)))
        .map(|record| record.event_type())
        .collect();
    assert_eq!(data_types, [CheckedEvent::EVENT_TYPE]);
    Ok(())
}

#[tokio::test]
async fn a_validator_feeding_two_sinks_records_each_rejection_once() -> Result<()> {
    let journal_root = unique_journal_dir("validator_fan_out");
    let journals = journal_root.clone();
    let (ledger_sink, ledger) = index_sink(|event: &TransformStageEvent| event.index);
    let (audit_sink, audit) = index_sink(|event: &TransformStageEvent| event.index);
    let handle = FlowDefinition::materialize(move |_| {
        let source_handler = TestEventSource::new();
        let ledger_sink = ledger_sink.clone();
        let audit_sink = audit_sink.clone();
        Ok(flow! {
            name: "validator_fan_out",
            journals: disk_journals(journals.clone()),
            stages: {
                source = source!(TransformStageEvent => source_handler);
                check = transform!(TransformStageEvent -> TransformStageEvent => RejectFirst);
                ledger = sink!(TransformStageEvent => ledger_sink);
                audit = sink!(TransformStageEvent => audit_sink);
            },
            topology: {
                source |> check;
                check |> ledger;
                check |> audit;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .map_err(|error| anyhow::anyhow!("Failed to create flow: {error:?}"))?;
    handle.run().await?;

    assert_eq!(*ledger.lock().unwrap(), [1]);
    assert_eq!(*audit.lock().unwrap(), [1]);
    let run = replay_testkit::latest_run_dir(&journal_root);
    the_one_rejection(&run, "check").await;
    for sink in ["ledger", "audit"] {
        assert!(
            replay_testkit::read_stage_error_envelopes(&run, sink)
                .await
                .is_empty(),
            "{sink} records nothing it never received"
        );
    }
    Ok(())
}
