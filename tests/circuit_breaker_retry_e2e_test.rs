// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115h end-to-end proof for source-poll retry recovery.
//!
//! This deliberately crosses the public DSL binder, a live circuit-breaker
//! attachment, the async source supervisor, and the durable stage journal. The
//! first physical poll fails transiently, the second succeeds, and the next
//! logical poll reaches EOF. Only terminal data is exposed, followed in the
//! journal by the buffered `AttemptFailed` and `SucceededAfterRetry` facts.

use async_trait::async_trait;
use obzenflow_adapters::middleware::CircuitBreakerBuilder;
use obzenflow_core::{
    event::{
        chain_event::{ChainEvent, ChainEventFactory},
        payloads::{
            delivery_payload::{DeliveryMethod, DeliveryPayload},
            flow_control_payload::FlowControlPayload,
            observability_payload::{
                MiddlewareLifecycle, ObservabilityPayload, RetryEvent, RetryInvocation,
                RetryProtectedUnit,
            },
        },
        status::processing_status::ErrorKind,
        ChainEventContent,
    },
    id::StageId,
    journal::{journal_owner::JournalOwner, Journal},
    TypedPayload, WriterId,
};
use obzenflow_dsl::{async_source, flow, sink, FlowDefinition};
use obzenflow_infra::{application::FlowApplication, journal::disk_journals};
use obzenflow_runtime::effects::SinkDeliverySafety;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::{
    common::handlers::{
        AsyncFiniteSourceHandler, SinkHandler, SourcePollRetryOwnership, SourcePollRetrySafety,
    },
    SourceError,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct RetryProbe {
    value: u64,
}

impl TypedPayload for RetryProbe {
    const EVENT_TYPE: &'static str = "circuit_breaker_retry.probe";
}

#[derive(Clone, Debug)]
struct FailOnceSource {
    calls: Arc<AtomicUsize>,
    writer_id: WriterId,
}

impl FailOnceSource {
    fn new(calls: Arc<AtomicUsize>) -> Self {
        Self {
            calls,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for FailOnceSource {
    fn bind_writer_id(&mut self, id: WriterId) {
        self.writer_id = id;
    }

    fn poll_retry_safety(&self) -> Option<SourcePollRetrySafety> {
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation)
    }

    fn poll_retry_ownership(&self) -> SourcePollRetryOwnership {
        SourcePollRetryOwnership::NoNestedRetry
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Err(SourceError::Timeout("transient first poll".to_string())),
            1 => Ok(Some(vec![ChainEventFactory::data_event(
                self.writer_id,
                RetryProbe::EVENT_TYPE,
                json!(RetryProbe { value: 115 }),
            )])),
            _ => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
struct FailThenEofSource {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl AsyncFiniteSourceHandler for FailThenEofSource {
    fn poll_retry_safety(&self) -> Option<SourcePollRetrySafety> {
        Some(SourcePollRetrySafety::RetrySafeAfterErrorOrCancellation)
    }

    fn poll_retry_ownership(&self) -> SourcePollRetryOwnership {
        SourcePollRetryOwnership::NoNestedRetry
    }

    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        match self.calls.fetch_add(1, Ordering::SeqCst) {
            0 => Err(SourceError::Transport("transient before EOF".to_string())),
            _ => Ok(None),
        }
    }
}

#[derive(Clone, Debug)]
struct CollectSink {
    values: Arc<Mutex<Vec<RetryProbe>>>,
}

#[derive(Clone, Debug)]
struct RetryOnceSink {
    attempts: Arc<AtomicUsize>,
    values: Arc<Mutex<Vec<RetryProbe>>>,
}

#[async_trait]
impl SinkHandler for RetryOnceSink {
    fn delivery_safety(&self) -> Option<SinkDeliverySafety> {
        Some(SinkDeliverySafety::IdempotentProjection)
    }

    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let Some(value) = RetryProbe::from_event(&event) {
            if self.attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                return Err(HandlerError::Remote("transient sink failure".to_string()));
            }
            self.values
                .lock()
                .expect("values lock poisoned")
                .push(value);
        }
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }
}

#[async_trait]
impl SinkHandler for CollectSink {
    async fn consume(&mut self, event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
        if let Some(value) = RetryProbe::from_event(&event) {
            self.values
                .lock()
                .expect("values lock poisoned")
                .push(value);
        }
        Ok(DeliveryPayload::success(
            DeliveryMethod::Custom("Memory".to_string()),
            None,
        ))
    }
}

fn retry_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    sink_attempts: Arc<AtomicUsize>,
    values: Arc<Mutex<Vec<RetryProbe>>>,
) -> FlowDefinition {
    flow! {
        name: "circuit_breaker_retry_e2e",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            feed = async_source!(RetryProbe => FailOnceSource::new(calls), [
                CircuitBreakerBuilder::new(3)
                    .with_retry_fixed(Duration::ZERO, 2)
                    .build()
            ]);
            collector = sink!(RetryProbe => RetryOnceSink { attempts: sink_attempts, values }, middleware: [
                CircuitBreakerBuilder::new(3)
                    .with_retry_fixed(Duration::ZERO, 2)
                    .build()
            ]);
        },

        topology: {
            feed |> collector;
        }
    }
}

fn retry_to_eof_flow(
    journal_base: PathBuf,
    calls: Arc<AtomicUsize>,
    values: Arc<Mutex<Vec<RetryProbe>>>,
) -> FlowDefinition {
    flow! {
        name: "circuit_breaker_retry_to_eof_e2e",
        journals: disk_journals(journal_base),
        middleware: [],

        stages: {
            feed = async_source!(RetryProbe => FailThenEofSource { calls }, [
                CircuitBreakerBuilder::new(3)
                    .with_retry_fixed(Duration::ZERO, 2)
                    .build()
            ]);
            collector = sink!(RetryProbe => CollectSink { values });
        },

        topology: {
            feed |> collector;
        }
    }
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let mut entries: Vec<PathBuf> = std::fs::read_dir(base.join("flows"))
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries.pop().expect("flow should produce an archive")
}

async fn read_stage_events(run_dir: &Path, stage_key: &str) -> Vec<ChainEvent> {
    let manifest: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(run_dir.join("run_manifest.json"))
            .expect("run manifest should be readable"),
    )
    .expect("run manifest should parse");
    let journal_file = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain stage '{stage_key}'"));
    let journal: obzenflow_infra::journal::DiskJournal<ChainEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            run_dir.join(journal_file),
            JournalOwner::stage(StageId::new()),
        )
        .expect("stage journal should open");

    let mut reader = journal
        .reader_from(0)
        .await
        .expect("stage journal reader should open");
    let mut events = Vec::new();
    while let Some(envelope) = reader.next().await.expect("stage journal should read") {
        events.push(envelope.event);
    }
    events
}

#[tokio::test]
async fn retry_safe_source_recovers_once_and_journals_terminal_truth_before_lifecycle() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let calls = Arc::new(AtomicUsize::new(0));
    let sink_attempts = Arc::new(AtomicUsize::new(0));
    let values = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(vec![OsString::from("obzenflow")])
        .run_async(retry_flow(
            journal_base.clone(),
            calls.clone(),
            sink_attempts.clone(),
            values.clone(),
        ))
        .await
        .expect("retry-enabled flow should bind and complete live");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        3,
        "one failed physical poll, its retry, and one EOF poll should execute"
    );
    assert_eq!(
        *values.lock().expect("values lock poisoned"),
        vec![RetryProbe { value: 115 }],
        "the transient attempt must not leak an error event or duplicate data"
    );
    assert_eq!(
        sink_attempts.load(Ordering::SeqCst),
        2,
        "one logical sink input should make two physical attempts"
    );

    let events = read_stage_events(&latest_run_dir(&journal_base), "feed").await;
    let data_positions: Vec<usize> = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| RetryProbe::from_event(event).map(|_| index))
        .collect();
    assert_eq!(
        data_positions.len(),
        1,
        "only terminal data should be journalled"
    );

    let retry_rows: Vec<(usize, RetryEvent)> = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| match &event.content {
            ChainEventContent::Observability(ObservabilityPayload::Middleware(
                MiddlewareLifecycle::Retry(retry),
            )) => Some((index, retry.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        retry_rows.len(),
        2,
        "one recovered invocation should emit exactly two durable retry rows"
    );

    let (failed_position, failed_context) = match &retry_rows[0] {
        (
            position,
            RetryEvent::AttemptFailed {
                context,
                attempt_number,
                max_attempts,
                error_kind,
                delay_ms,
                ..
            },
        ) => {
            assert_eq!(*attempt_number, 1);
            assert_eq!(*max_attempts, 2);
            assert_eq!(*error_kind, Some(ErrorKind::Timeout));
            assert_eq!(*delay_ms, Some(0));
            (
                *position,
                context
                    .as_ref()
                    .expect("new retry row should carry context"),
            )
        }
        other => panic!("first retry row should be AttemptFailed, got {other:?}"),
    };

    let (recovered_position, recovered_context) = match &retry_rows[1] {
        (
            position,
            RetryEvent::SucceededAfterRetry {
                context,
                total_attempts,
                ..
            },
        ) => {
            assert_eq!(*total_attempts, 2);
            (
                *position,
                context
                    .as_ref()
                    .expect("new retry row should carry context"),
            )
        }
        other => panic!("second retry row should be SucceededAfterRetry, got {other:?}"),
    };

    assert_eq!(failed_context, recovered_context);
    assert!(matches!(
        failed_context.protected_unit,
        RetryProtectedUnit::SourcePoll
    ));
    assert!(matches!(
        failed_context.invocation,
        RetryInvocation::SourcePoll { .. }
    ));
    assert!(
        data_positions[0] < failed_position && failed_position < recovered_position,
        "terminal data must be committed before ordered retry lifecycle facts"
    );

    let sink_events = read_stage_events(&latest_run_dir(&journal_base), "collector").await;
    let receipt_positions: Vec<usize> = sink_events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| event.is_delivery().then_some(index))
        .collect();
    let sink_retry_positions: Vec<usize> = sink_events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(
                &event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::Retry(_)
                ))
            )
            .then_some(index)
        })
        .collect();
    assert_eq!(
        receipt_positions.len(),
        1,
        "intermediate sink attempts must not journal receipts or advance receipt progress"
    );
    assert_eq!(sink_retry_positions.len(), 2);
    assert!(
        sink_retry_positions
            .iter()
            .all(|position| receipt_positions[0] < *position),
        "the one terminal delivery receipt must precede retry evidence"
    );
}

#[tokio::test]
async fn retry_safe_source_journals_eof_before_its_buffered_lifecycle() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");
    let calls = Arc::new(AtomicUsize::new(0));
    let values = Arc::new(Mutex::new(Vec::new()));

    FlowApplication::builder()
        .with_cli_args(vec![OsString::from("obzenflow")])
        .run_async(retry_to_eof_flow(
            journal_base.clone(),
            calls.clone(),
            values.clone(),
        ))
        .await
        .expect("retry-to-EOF flow should bind and complete live");

    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(values.lock().expect("values lock poisoned").is_empty());

    let events = read_stage_events(&latest_run_dir(&journal_base), "feed").await;
    let eof_position = events
        .iter()
        .position(|event| {
            matches!(
                &event.content,
                ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
            )
        })
        .expect("terminal EOF should be journalled");
    let retry_positions: Vec<usize> = events
        .iter()
        .enumerate()
        .filter_map(|(index, event)| {
            matches!(
                &event.content,
                ChainEventContent::Observability(ObservabilityPayload::Middleware(
                    MiddlewareLifecycle::Retry(_)
                ))
            )
            .then_some(index)
        })
        .collect();

    assert_eq!(retry_positions.len(), 2);
    assert!(
        retry_positions
            .iter()
            .all(|position| eof_position < *position),
        "terminal EOF must be committed before its buffered retry lifecycle facts"
    );
}
