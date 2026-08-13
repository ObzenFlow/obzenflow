// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use async_trait::async_trait;
use obzenflow_adapters::middleware::observability::log_event;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryResult};
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{ChainEventContent, SystemEvent};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, FlowId, JournalId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::MemoryJournalFactory;
use obzenflow_runtime::journal::{FlowJournalFactory, RunSubstrateState};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::{
    InlineSink, SinkDescription, SinkTerminalOutcome, SinkWriteContext, SinkWriteReport,
    TypedFiniteSourceHandler,
};
use obzenflow_runtime::stages::SourceError;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const INPUT_COUNT: usize = 3;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input {
    id: usize,
}

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "logging_failure.input";
}

#[derive(Clone, Debug)]
struct InputSource {
    remaining: usize,
}

impl TypedFiniteSourceHandler for InputSource {
    type Output = Input;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.remaining == 0 {
            return Ok(None);
        }
        let id = self.remaining;
        self.remaining -= 1;
        Ok(Some(vec![Input { id }]))
    }
}

#[derive(Clone, Debug)]
struct CountingSink {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl InlineSink for CountingSink {
    type Input = Input;

    fn describe(&self) -> SinkDescription {
        SinkDescription::unspecified()
    }

    async fn write(
        &mut self,
        _input: Input,
        _context: SinkWriteContext,
    ) -> Result<SinkWriteReport, HandlerError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(SinkWriteReport::terminal(SinkTerminalOutcome::success_via(
            DeliveryMethod::Custom("test".to_string()),
            None,
        )))
    }
}

type TrackedJournals = Arc<Mutex<Vec<(JournalName, Arc<dyn Journal<ChainEvent>>)>>>;

struct FailLoggingJournal {
    inner: Arc<dyn Journal<ChainEvent>>,
    failed_logging_appends: Arc<AtomicUsize>,
}

fn is_logging_evidence(event: &ChainEvent) -> bool {
    matches!(
        &event.content,
        ChainEventContent::Observability(ObservabilityPayload::Middleware(
            MiddlewareLifecycle::Logging(_)
        ))
    )
}

#[async_trait]
impl Journal<ChainEvent> for FailLoggingJournal {
    fn id(&self) -> &JournalId {
        self.inner.id()
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }

    async fn append(
        &self,
        event: ChainEvent,
        parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<EventEnvelope<ChainEvent>, JournalError> {
        if is_logging_evidence(&event) {
            self.failed_logging_appends.fetch_add(1, Ordering::SeqCst);
            return Err(JournalError::Implementation {
                message: "injected logging evidence append failure".to_string(),
                source: Box::new(std::io::Error::other(
                    "injected logging evidence append failure",
                )),
            });
        }
        self.inner.append(event, parent).await
    }

    async fn append_group(
        &self,
        group_id: &str,
        events: Vec<ChainEvent>,
        parent: Option<&EventEnvelope<ChainEvent>>,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        if events.iter().any(is_logging_evidence) {
            self.failed_logging_appends.fetch_add(1, Ordering::SeqCst);
            return Err(JournalError::Implementation {
                message: "injected grouped logging evidence append failure".to_string(),
                source: Box::new(std::io::Error::other(
                    "injected grouped logging evidence append failure",
                )),
            });
        }
        self.inner.append_group(group_id, events, parent).await
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        self.inner.read_all_unordered().await
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<ChainEvent>>, JournalError> {
        self.inner.read_event(event_id).await
    }

    async fn reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
        self.inner.reader_from(position).await
    }

    async fn read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<EventEnvelope<ChainEvent>>, JournalError> {
        self.inner.read_last_n(count).await
    }
}

struct TrackingFactory {
    inner: MemoryJournalFactory,
    chain_journals: HashMap<JournalName, Arc<dyn Journal<ChainEvent>>>,
    tracked: TrackedJournals,
    failed_logging_appends: Arc<AtomicUsize>,
    inject_logging_failure: bool,
}

impl TrackingFactory {
    fn new(
        flow_id: FlowId,
        tracked: TrackedJournals,
        failed_logging_appends: Arc<AtomicUsize>,
        inject_logging_failure: bool,
    ) -> Self {
        Self {
            inner: MemoryJournalFactory::new(flow_id),
            chain_journals: HashMap::new(),
            tracked,
            failed_logging_appends,
            inject_logging_failure,
        }
    }
}

impl FlowJournalFactory for TrackingFactory {
    fn run_state(&self) -> RunSubstrateState {
        RunSubstrateState::Ephemeral
    }

    fn create_chain_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<ChainEvent>>, JournalError> {
        if let Some(journal) = self.chain_journals.get(&name) {
            return Ok(journal.clone());
        }
        let inner = self.inner.create_chain_journal(name.clone(), owner)?;
        let journal: Arc<dyn Journal<ChainEvent>> = if self.inject_logging_failure {
            Arc::new(FailLoggingJournal {
                inner,
                failed_logging_appends: self.failed_logging_appends.clone(),
            })
        } else {
            inner
        };
        self.chain_journals.insert(name.clone(), journal.clone());
        self.tracked.lock().unwrap().push((name, journal.clone()));
        Ok(journal)
    }

    fn create_system_journal(
        &mut self,
        name: JournalName,
        owner: JournalOwner,
    ) -> Result<Arc<dyn Journal<SystemEvent>>, JournalError> {
        self.inner.create_system_journal(name, owner)
    }
}

struct CaseResult {
    calls: usize,
    successful_receipts: usize,
    logging_rows: usize,
    failed_logging_appends: usize,
    max_drop_counter_on_receipts: u64,
}

async fn run_case(with_logging: bool, inject_logging_failure: bool) -> CaseResult {
    let calls = Arc::new(AtomicUsize::new(0));
    let tracked: TrackedJournals = Arc::new(Mutex::new(Vec::new()));
    let failed_logging_appends = Arc::new(AtomicUsize::new(0));
    let factory_tracked = tracked.clone();
    let factory_failures = failed_logging_appends.clone();
    let sink_calls = calls.clone();
    let flow_definition = FlowDefinition::materialize(move |_runtime_config| {
        let source = InputSource {
            remaining: INPUT_COUNT,
        };
        let sink = CountingSink { calls: sink_calls };
        let journals = move |flow_id| {
            Ok(TrackingFactory::new(
                flow_id,
                factory_tracked,
                factory_failures,
                inject_logging_failure,
            ))
        };

        if with_logging {
            Ok(flow! {
                name: "logging_sink_failure_treatment",
                journals: journals,
                stages: {
                    input = source!(Input => source);
                    sink = sink!(Input => sink, observers: [
                        log_event("test.sink.delivery")
                    ]);
                },
                topology: { input |> sink; }
            })
        } else {
            Ok(flow! {
                name: "logging_sink_failure_control",
                journals: journals,
                stages: {
                    input = source!(Input => source);
                    sink = sink!(Input => sink);
                },
                topology: { input |> sink; }
            })
        }
    });

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(flow_definition)
        .await
        .expect("flow remains successful when observer evidence append fails");

    let journals = tracked.lock().unwrap().clone();
    let mut events = Vec::new();
    for (_, journal) in journals {
        events.extend(journal.read_all_unordered().await.unwrap());
    }
    let successful_receipts = events
        .iter()
        .filter(|envelope| {
            matches!(
                &envelope.event.content,
                ChainEventContent::Delivery(payload)
                    if matches!(payload.result, DeliveryResult::Success { .. })
            )
        })
        .count();
    let logging_rows = events
        .iter()
        .filter(|envelope| is_logging_evidence(&envelope.event))
        .count();
    let max_drop_counter_on_receipts = events
        .iter()
        .filter(|envelope| envelope.event.is_delivery())
        .filter_map(|envelope| envelope.event.runtime_context.as_ref())
        .map(|runtime| runtime.observer_diagnostics_dropped_journal_append_failed_total)
        .max()
        .unwrap_or(0);

    CaseResult {
        calls: calls.load(Ordering::SeqCst),
        successful_receipts,
        logging_rows,
        failed_logging_appends: failed_logging_appends.load(Ordering::SeqCst),
        max_drop_counter_on_receipts,
    }
}

#[tokio::test]
async fn logging_append_failure_cannot_change_physical_sink_success_or_receipts() {
    let control = run_case(false, false).await;
    let treatment = run_case(true, true).await;

    assert_eq!(control.calls, INPUT_COUNT);
    assert_eq!(control.successful_receipts, INPUT_COUNT);
    assert_eq!(treatment.calls, control.calls);
    assert_eq!(treatment.successful_receipts, control.successful_receipts);
    assert_eq!(treatment.logging_rows, 0);
    assert_eq!(treatment.failed_logging_appends, INPUT_COUNT);
    assert_eq!(
        treatment.max_drop_counter_on_receipts, INPUT_COUNT as u64,
        "each failed diagnostic is counted before its successful authoritative receipt"
    );
}
