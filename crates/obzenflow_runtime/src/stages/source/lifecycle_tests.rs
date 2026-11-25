// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::finite::fsm::tests::TestJournal;
use super::*;
use crate::effects::{EffectDeclaration, EffectPortRegistry};
use crate::id_conversions::StageIdExt;
use crate::queries::FiniteRead;
use crate::stages::common::handlers::source::prepared::{AdmitSource, AdmittedSource};
use crate::stages::common::handlers::source::SourceRead;
use crate::stages::common::handlers::UnifiedAsyncFiniteSourceHandler;
use crate::stages::resources_builder::{StageResources, StageResourcesBuilder};
use crate::supervised_base::{SupervisorBuilder, SupervisorHandle};
use crate::systems::Port;
use async_trait::async_trait;
use futures::FutureExt;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::effect_payload::{
    EffectAttemptOrdinal, EffectOutcomePayload, EffectRecord,
};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, RecoveryFact, SourcePollContinuation, SourcePollErrorKind, StageLifecycleFact,
};
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    ChainPayload, JournalRecord, SourceDiagnosticReason, StageFatalCode, StageFatalReason,
};
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::journal::factory::CurrentRunLocator;
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::journal::{ArchiveStatus, StatusDerivation};
use obzenflow_core::{ChainEvent, EventId, FlowId, StageId, SystemId, TypedPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Serialize, Deserialize)]
struct Row;
impl TypedPayload for Row {
    const EVENT_TYPE: &'static str = "source.lifecycle.row";
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
enum Marker {
    Row,
    Reject,
    Terminal,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextRead(u32);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait LifecycleInput: Send + Sync {
    #[query(type = "source.lifecycle.read")]
    async fn read(
        &self,
        from: &NextRead,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextRead>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Default)]
struct Counts {
    builds: AtomicUsize,
    reads: AtomicUsize,
    reading: Notify,
}

#[derive(Clone, Copy)]
enum Reading {
    Pending,
    RowThenPending,
    RowThenTerminal,
    RejectThenPending,
}

#[derive(Clone)]
struct Fixture {
    counts: Arc<Counts>,
    reading: Reading,
}

impl Fixture {
    fn new(reading: Reading) -> Self {
        Self {
            counts: Arc::default(),
            reading,
        }
    }

    async fn records(&self) -> Vec<Marker> {
        let read = self.counts.reads.fetch_add(1, Ordering::SeqCst);
        self.counts.reading.notify_one();
        match (self.reading, read) {
            (Reading::RowThenPending | Reading::RowThenTerminal, 0) => vec![Marker::Row],
            (Reading::RowThenTerminal, _) => vec![Marker::Terminal],
            (Reading::RejectThenPending, 0) => vec![Marker::Reject],
            _ => std::future::pending().await,
        }
    }
}

#[async_trait]
impl LifecycleInput for Fixture {
    async fn read(
        &self,
        from: &NextRead,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextRead>, obzenflow_runtime::queries::QueryError> {
        Ok(FiniteRead::Batch {
            records: self.records().await,
            next: NextRead(from.0 + 1),
        })
    }
}

fn row(marker: Marker) -> Result<Row, SourceError> {
    match marker {
        Marker::Row => Ok(Row),
        Marker::Reject => Err(SourceError::Validation(
            SourceDiagnosticReason::InvalidValue.into(),
        )),
        Marker::Terminal => Err(SourceError::Terminal {
            kind: SourcePollErrorKind::Transport,
            diagnostic: SourceDiagnosticReason::InputUnavailable.into(),
        }),
    }
}

fn deferred(fixture: &Fixture) -> Port<dyn LifecycleInput> {
    let fixture = fixture.clone();
    Port::deferred(move || {
        fixture.counts.builds.fetch_add(1, Ordering::SeqCst);
        Ok(fixture.clone())
    })
}

struct FiniteRows {
    port: Port<dyn LifecycleInput>,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for FiniteRows {
    type Output = Row;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> SourceRead<Row> {
        SourceRead::finite(self.port.clone(), NextRead(0), row)
    }
}

impl FiniteRows {
    fn of(fixture: &Fixture) -> Self {
        Self {
            port: deferred(fixture),
        }
    }
}

struct Journals {
    data: Arc<dyn Journal<ChainEvent>>,
    error: Arc<dyn Journal<ChainEvent>>,
}

fn admitted<F, H>(
    handler: H,
) -> (
    AdmittedSource<H::Handler>,
    EffectPortRegistry,
    Vec<EffectDeclaration>,
)
where
    F: ?Sized,
    H: AdmitSource<F>,
{
    let admitted = handler.admit();
    let declarations = admitted
        .effect_declarations()
        .expect("the source declares its query");
    let ports = EffectPortRegistry::collect_from_declarations(declarations.iter())
        .expect("the declaration registers its port");
    let declarations = declarations
        .iter()
        .map(EffectDeclaration::runtime_projection)
        .collect();
    (admitted, ports, declarations)
}

async fn resources_with_ports(
    ports: EffectPortRegistry,
    declarations: Vec<EffectDeclaration>,
) -> (StageId, StageResources, Journals) {
    resources_with(ports, declarations, None).await
}

async fn resources_with(
    ports: EffectPortRegistry,
    declarations: Vec<EffectDeclaration>,
    archive: Option<Arc<dyn ReplayArchive>>,
) -> (StageId, StageResources, Journals) {
    let mut topology = obzenflow_topology::TopologyBuilder::new();
    let source = topology.add_stage(Some("input".into()));
    let sink = topology.add_stage(Some("output".into()));
    let stage = StageId::from_topology_id(source);
    let sink = StageId::from_topology_id(sink);
    let system = Arc::new(TestJournal::new(JournalOwner::stage(stage)));
    let journal = || {
        Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)))
            as Arc<dyn Journal<ChainEvent>>
    };
    let mut resources = StageResourcesBuilder::new(
        FlowId::new(),
        SystemId::new(),
        Arc::new(topology.build_unchecked().unwrap()),
        system.clone(),
        HashMap::from([(stage, journal()), (sink, journal())]),
        HashMap::from([(stage, journal()), (sink, journal())]),
    )
    .with_effect_ports(ports)
    .with_replay_archive(archive)
    .build()
    .await
    .unwrap();
    let mut stage_resources = resources.take_stage_resources(stage).unwrap();
    stage_resources.effect_declarations = declarations;
    let journals = Journals {
        data: stage_resources.data_journal.clone(),
        error: stage_resources.error_journal.clone(),
    };
    (stage, stage_resources, journals)
}

type Rows = [JournalRecord<ChainPayload>];

/// The lifecycle failure's causal link, if the stage recorded one.
fn failed_cause(rows: &Rows) -> Option<Option<EventId>> {
    rows.iter().find_map(|row| match &row.payload {
        ChainPayload::Execution(ExecutionPayload::StageLifecycle(StageLifecycleFact::Failed {
            causal_event_id,
            ..
        })) => Some(*causal_event_id),
        _ => None,
    })
}

fn has_eof(rows: &Rows) -> bool {
    rows.iter().any(|row| row.is_eof())
}

fn facts(rows: &Rows) -> usize {
    rows.iter()
        .filter(|row| matches!(row.payload, ChainPayload::Fact(_)))
        .count()
}

fn source_failures(rows: &Rows) -> Vec<(EventId, &ExecutionPayload)> {
    rows.iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(
                payload @ (ExecutionPayload::SourceRecordRejected(_)
                | ExecutionPayload::SourcePollFailed(_)),
            ) => Some((row.envelope.provenance.event.id, payload)),
            _ => None,
        })
        .collect()
}

async fn notified(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .expect("source progress");
}

mod finite_lifecycle {
    use super::*;
    use finite::AsyncFiniteSourceBuilder as Builder;
    use finite::FiniteSourceConfig as Config;
    use finite::FiniteSourceEvent as Event;
    use finite::FiniteSourceState as State;
    type Handler = <FiniteRows as AdmitSource<dyn UnifiedAsyncFiniteSourceHandler>>::Handler;
    type Handle = crate::supervised_base::StandardHandle<Event<Handler>, State<Handler>>;

    async fn build(fixture: &Fixture) -> (Handle, Journals) {
        let (source, ports, declarations) =
            admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(FiniteRows::of(fixture));
        let (stage, resources, journals) = resources_with_ports(ports, declarations).await;
        let config = Config::new(stage, "input", "lifecycle");
        (
            Builder::new(source, config, resources)
                .build()
                .await
                .unwrap(),
            journals,
        )
    }

    async fn start(handle: &Handle) {
        for event in [Event::Initialize, Event::Ready, Event::Start] {
            handle.send_event(event).await.unwrap();
        }
    }

    async fn finish(handle: &Handle) -> Result<(), crate::supervised_base::HandleError> {
        tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
            .await
            .expect("source terminates")
    }

    #[tokio::test]
    async fn stop_before_start_skips_resolution() {
        let fixture = Fixture::new(Reading::Pending);
        let (handle, journals) = build(&fixture).await;
        start(&handle).await;
        handle.send_event(Event::BeginDrain).await.unwrap();
        finish(&handle).await.unwrap();
        assert_eq!(fixture.counts.builds.load(Ordering::SeqCst), 0);
        assert_eq!(fixture.counts.reads.load(Ordering::SeqCst), 0);
        let data = journals.data.read_all_unordered().await.unwrap();
        assert_eq!(facts(&data), 0);
    }

    #[tokio::test]
    async fn failure_during_publication_returns_before_settling() {
        let fixture = Fixture::new(Reading::RowThenPending);
        let (source, ports, declarations) =
            admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(FiniteRows::of(&fixture));
        let (stage, mut resources, _) = resources_with_ports(ports, declarations).await;
        let journal = Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)));
        let (writing, release) =
            journal.block_matching_append(|event| matches!(event.payload, ChainPayload::Fact(_)));
        resources.data_journal = journal.clone();
        let config = Config::new(stage, "input", "lifecycle");
        let handle = Builder::new(source, config, resources)
            .build()
            .await
            .unwrap();
        start(&handle).await;
        notified(&writing).await;
        handle
            .send_event(Event::Error("failure during source publication".into()))
            .await
            .unwrap();
        let failure = tokio::time::timeout(Duration::from_secs(3), handle.wait_for_failure())
            .await
            .unwrap()
            .unwrap();
        assert!(failure
            .cause
            .to_string()
            .contains("failure during source publication"));
        assert!(matches!(handle.current_state(), State::Failing(_)));
        assert!(handle.wait_for_stage_exit().now_or_never().is_none());
        release.notify_one();
        assert!(finish(&handle).await.is_err());
        let records = journal.read_all_unordered().await.unwrap();
        assert_eq!(facts(&records), 1);
    }

    #[tokio::test]
    async fn terminal_read_failure_fails_without_eof() {
        let fixture = Fixture::new(Reading::RowThenTerminal);
        let (handle, journals) = build(&fixture).await;
        start(&handle).await;
        assert!(
            finish(&handle).await.is_err(),
            "terminal report fails the stage"
        );
        assert!(matches!(
            handle.current_state(),
            State::Failed(reason)
                if reason == "Source 'input' cannot continue: terminal source transport error: the input is unavailable"
        ));
        assert_eq!(
            fixture.counts.reads.load(Ordering::SeqCst),
            2,
            "no read after a terminal report"
        );

        let data = journals.data.read_all_unordered().await.unwrap();
        let errors = journals.error.read_all_unordered().await.unwrap();
        let failures = source_failures(&errors);
        assert_eq!(failures.len(), 1, "{failures:?}");
        let (diagnostic_id, ExecutionPayload::SourcePollFailed(poll)) = failures[0] else {
            panic!("expected runtime.source.poll_failed, got {failures:?}");
        };
        assert_eq!(poll.continuation, SourcePollContinuation::Terminal);
        assert_eq!(poll.kind, ErrorKind::Remote);
        assert_eq!(failed_cause(&data), Some(Some(diagnostic_id)));
        assert!(!has_eof(&data), "a terminal source writes no EOF");
        assert_eq!(
            facts(&data),
            1,
            "the row accepted before the failure stays committed"
        );
    }

    #[tokio::test]
    async fn rejected_record_keeps_source_reading() {
        let fixture = Fixture::new(Reading::RejectThenPending);
        let (handle, journals) = build(&fixture).await;
        start(&handle).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while fixture.counts.reads.load(Ordering::SeqCst) < 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("reading continues after a rejection");
        let errors = journals.error.read_all_unordered().await.unwrap();
        let failures = source_failures(&errors);
        assert_eq!(failures.len(), 1, "{failures:?}");
        let (_, ExecutionPayload::SourceRecordRejected(rejected)) = failures[0] else {
            panic!("expected runtime.source.record_rejected, got {failures:?}");
        };
        assert_eq!(rejected.kind, ErrorKind::Validation);
        handle.send_event(Event::BeginDrain).await.unwrap();
        finish(&handle).await.unwrap();
    }

    #[tokio::test]
    async fn failed_resolver_stops_without_read() {
        let unbuilt = FiniteRows {
            port: Port::deferred(
                || -> Result<Fixture, crate::effects::EffectPortResolutionError> {
                    Err(crate::effects::EffectPortResolutionError::ClientConstructionFailed)
                },
            ),
        };
        let (source, ports, declarations) =
            admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(unbuilt);
        let (stage, resources, journals) = resources_with_ports(ports, declarations).await;
        let config = Config::new(stage, "input", "lifecycle");
        let handle = Builder::new(source, config, resources)
            .build()
            .await
            .unwrap();
        start(&handle).await;
        assert!(finish(&handle).await.is_err(), "the stage fails");

        let mut rows = journals.data.read_all_unordered().await.unwrap();
        rows.extend(journals.error.read_all_unordered().await.unwrap());
        assert!(
            query_rows(&rows).is_empty(),
            "the source writes no read row"
        );
        let reasons: Vec<_> = rows
            .iter()
            .filter_map(|row| match &row.payload {
                ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) => {
                    Some(fatal.reason)
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            reasons,
            [obzenflow_core::event::StageFatalReason::EffectPortResolutionFailed]
        );
    }
}

async fn run_finite<H>(
    source: AdmittedSource<H>,
    ports: EffectPortRegistry,
    declarations: Vec<EffectDeclaration>,
) -> Journals
where
    H: UnifiedAsyncFiniteSourceHandler + Send + Sync + 'static,
{
    let (stage, resources, journals) = resources_with_ports(ports, declarations).await;
    let config = finite::FiniteSourceConfig::new(stage, "input", "lifecycle");
    let handle = finite::AsyncFiniteSourceBuilder::new(source, config, resources)
        .build()
        .await
        .unwrap();
    for event in [
        finite::FiniteSourceEvent::Initialize,
        finite::FiniteSourceEvent::Ready,
        finite::FiniteSourceEvent::Start,
    ] {
        handle.send_event(event).await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .expect("source completes");
    journals
}

struct QueryRow {
    record: EffectRecord,
    attempt: Option<EffectAttemptOrdinal>,
    event_type: String,
}

fn query_rows(rows: &Rows) -> Vec<QueryRow> {
    rows.iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(ExecutionPayload::EffectRecord(record))
                if record.is_query() =>
            {
                Some(QueryRow {
                    record: record.as_ref().clone(),
                    attempt: row
                        .envelope
                        .provenance
                        .event
                        .effect_provenance
                        .as_ref()
                        .and_then(|provenance| provenance.attempt),
                    event_type: row.event_type(),
                })
            }
            _ => None,
        })
        .collect()
}

fn attempt_starts(rows: &Rows) -> usize {
    rows.iter()
        .filter(|row| {
            matches!(
                row.payload,
                ChainPayload::Execution(ExecutionPayload::EffectAttemptStarted(_))
            )
        })
        .count()
}

fn reply<T: serde::de::DeserializeOwned>(row: &QueryRow) -> T {
    match &row.record.outcome {
        EffectOutcomePayload::Succeeded { output } => {
            serde_json::from_value(output.clone()).expect("the recorded reply decodes")
        }
        other => panic!("expected a recorded reply, got {other:?}"),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextScripted(u32);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait ScriptedInput: Send + Sync {
    #[query(type = "source.lifecycle.scripted_read")]
    async fn read(
        &self,
        from: &NextScripted,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextScripted>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone)]
struct Script(Arc<Vec<Vec<Marker>>>);

#[async_trait]
impl ScriptedInput for Script {
    async fn read(
        &self,
        from: &NextScripted,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextScripted>, obzenflow_runtime::queries::QueryError> {
        Ok(match self.0.get(from.0 as usize) {
            Some(records) => FiniteRead::Batch {
                records: records.clone(),
                next: NextScripted(from.0 + 1),
            },
            None => FiniteRead::EndOfInput,
        })
    }
}

struct ScriptedRows(Port<dyn ScriptedInput>);

#[async_trait]
impl TypedAsyncFiniteSourceHandler for ScriptedRows {
    type Output = Row;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> SourceRead<Row> {
        SourceRead::finite(self.0.clone(), NextScripted(0), row)
    }
}

#[tokio::test]
async fn port_read_records_each_answer() {
    let script = Script(Arc::new(vec![vec![Marker::Row], Vec::new()]));
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(ScriptedRows(Port::to(script)));
    let journals = run_finite(source, ports, declarations).await;
    let data = journals.data.read_all_unordered().await.unwrap();

    let reads = query_rows(&data);
    assert_eq!(
        reads
            .iter()
            .map(|read| read.record.cursor.input_seq.get())
            .collect::<Vec<_>>(),
        [1, 2, 3],
        "the empty answer and the end of input have their own read numbers"
    );
    assert!(reads.iter().all(|read| read.event_type == "query.answered"
        && read.record.descriptor.effect_type.as_str() == "source.lifecycle.scripted_read"));
    let replies: Vec<FiniteRead<Marker, NextScripted>> = reads.iter().map(reply).collect();
    assert!(
        matches!(&replies[0], FiniteRead::Batch { records, next } if records.len() == 1 && next.0 == 1)
    );
    assert!(
        matches!(&replies[1], FiniteRead::Batch { records, next } if records.is_empty() && next.0 == 2)
    );
    assert!(matches!(replies[2], FiniteRead::EndOfInput));
    assert_eq!(attempt_starts(&data), 0, "a query starts no attempt");
    assert_eq!(facts(&data), 1);
    assert!(has_eof(&data));
}

#[tokio::test]
async fn source_fact_parent_names_answer() {
    let script = Script(Arc::new(vec![vec![Marker::Row]]));
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(ScriptedRows(Port::to(script)));
    let journals = run_finite(source, ports, declarations).await;
    let data = journals.data.read_all_unordered().await.unwrap();
    let answer = data
        .iter()
        .find(|row| row.event_type() == "query.answered")
        .expect("the first read is answered");
    let fact = data
        .iter()
        .find(|row| matches!(row.payload, ChainPayload::Fact(_)))
        .expect("the read supplies one fact");
    assert_eq!(
        fact.envelope.provenance.event.causality.parent_ids.first(),
        Some(&answer.envelope.provenance.event.id),
    );
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextInline(u32);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextBlocking(u32);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextAwaited(u32);

#[derive(Clone, Default)]
struct Placement(Arc<std::sync::Mutex<Vec<std::thread::ThreadId>>>);

impl Placement {
    fn record(&self, thread: std::thread::ThreadId) {
        self.0.lock().unwrap().push(thread);
    }

    fn threads(&self) -> Vec<std::thread::ThreadId> {
        self.0.lock().unwrap().clone()
    }
}

fn placed<N>(at: u32, next: fn(u32) -> N) -> FiniteRead<Marker, N> {
    match at {
        0 => FiniteRead::Batch {
            records: vec![Marker::Row],
            next: next(1),
        },
        _ => FiniteRead::EndOfInput,
    }
}

#[obzenflow_runtime::effects::port]
trait InlineInput: Send + Sync {
    #[query(type = "source.lifecycle.inline_read")]
    fn read(
        &self,
        from: &NextInline,
    ) -> Result<FiniteRead<Marker, NextInline>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone, Default)]
struct Inline(Placement);

impl InlineInput for Inline {
    fn read(
        &self,
        from: &NextInline,
    ) -> Result<FiniteRead<Marker, NextInline>, obzenflow_runtime::queries::QueryError> {
        self.0.record(std::thread::current().id());
        Ok(placed(from.0, NextInline))
    }
}

struct InlineRows(Port<dyn InlineInput>);

#[async_trait]
impl TypedAsyncFiniteSourceHandler for InlineRows {
    type Output = Row;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> SourceRead<Row> {
        SourceRead::finite(self.0.clone(), NextInline(0), row)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn sync_read_runs_inline() {
    let inline = Inline::default();
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(InlineRows(Port::to(inline.clone())));
    let journals = run_finite(source, ports, declarations).await;
    let threads = inline.0.threads();
    assert_eq!(threads.len(), 2);
    assert!(threads
        .iter()
        .all(|thread| *thread == std::thread::current().id()));
    assert_eq!(facts(&journals.data.read_all_unordered().await.unwrap()), 1);
}

#[obzenflow_runtime::effects::port]
#[async_trait]
trait BlockingInput: Send + Sync {
    #[query(type = "source.lifecycle.blocking_read")]
    async fn read(
        &self,
        from: &NextBlocking,
    ) -> Result<FiniteRead<Marker, NextBlocking>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone, Default)]
struct Blocking(Placement);

#[async_trait]
impl BlockingInput for Blocking {
    async fn read(
        &self,
        from: &NextBlocking,
    ) -> Result<FiniteRead<Marker, NextBlocking>, obzenflow_runtime::queries::QueryError> {
        let thread = tokio::task::spawn_blocking(|| std::thread::current().id())
            .await
            .map_err(|_| {
                crate::queries::QueryError::terminal(
                    SourcePollErrorKind::Other,
                    SourceDiagnosticReason::InputUnavailable,
                )
            })?;
        self.0.record(thread);
        Ok(placed(from.0, NextBlocking))
    }
}

struct BlockingRows(Port<dyn BlockingInput>);

#[async_trait]
impl TypedAsyncFiniteSourceHandler for BlockingRows {
    type Output = Row;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> SourceRead<Row> {
        SourceRead::finite(self.0.clone(), NextBlocking(0), row)
    }
}

#[tokio::test(flavor = "current_thread")]
async fn blocking_system_read_uses_blocking_pool() {
    let blocking = Blocking::default();
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(
        BlockingRows(Port::to(blocking.clone())),
    );
    let journals = run_finite(source, ports, declarations).await;
    let threads = blocking.0.threads();
    assert_eq!(threads.len(), 2);
    assert!(threads
        .iter()
        .all(|thread| *thread != std::thread::current().id()));
    assert_eq!(facts(&journals.data.read_all_unordered().await.unwrap()), 1);
}

#[obzenflow_runtime::effects::port]
#[async_trait]
trait AwaitedInput: Send + Sync {
    #[query(type = "source.lifecycle.awaited_read")]
    async fn read(
        &self,
        from: &NextAwaited,
    ) -> Result<FiniteRead<Marker, NextAwaited>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone, Default)]
struct Awaited {
    release: Arc<Notify>,
    waiting: Arc<Notify>,
}

#[async_trait]
impl AwaitedInput for Awaited {
    async fn read(
        &self,
        from: &NextAwaited,
    ) -> Result<FiniteRead<Marker, NextAwaited>, obzenflow_runtime::queries::QueryError> {
        self.waiting.notify_one();
        self.release.notified().await;
        Ok(placed(from.0, NextAwaited))
    }
}

struct AwaitedRows(Port<dyn AwaitedInput>);

#[async_trait]
impl TypedAsyncFiniteSourceHandler for AwaitedRows {
    type Output = Row;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> SourceRead<Row> {
        SourceRead::finite(self.0.clone(), NextAwaited(0), row)
    }
}

#[tokio::test]
async fn async_read_awaits_system() {
    let awaited = Awaited::default();
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(AwaitedRows(Port::to(awaited.clone())));
    let (stage, resources, journals) = resources_with_ports(ports, declarations).await;
    let config = finite::FiniteSourceConfig::new(stage, "input", "lifecycle");
    let handle = finite::AsyncFiniteSourceBuilder::new(source, config, resources)
        .build()
        .await
        .unwrap();
    for event in [
        finite::FiniteSourceEvent::Initialize,
        finite::FiniteSourceEvent::Ready,
        finite::FiniteSourceEvent::Start,
    ] {
        handle.send_event(event).await.unwrap();
    }
    notified(&awaited.waiting).await;
    let before = journals.data.read_all_unordered().await.unwrap();
    assert!(query_rows(&before).is_empty(), "the read is still awaited");
    assert_eq!(facts(&before), 0);
    awaited.release.notify_one();
    notified(&awaited.waiting).await;
    awaited.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .unwrap();
    let after = journals.data.read_all_unordered().await.unwrap();
    assert_eq!(query_rows(&after).len(), 2);
    assert_eq!(facts(&after), 1);
}

#[tokio::test]
async fn placeholder_read_ends_input() {
    let source = AdmittedSource::finite_placeholder(None);
    let declarations = source.effect_declarations().expect("a placeholder query");
    let ports = EffectPortRegistry::collect_from_declarations(declarations.iter())
        .expect("the declaration registers its port");
    let declarations = declarations
        .iter()
        .map(EffectDeclaration::runtime_projection)
        .collect();
    let journals = run_finite(source, ports, declarations).await;
    let data = journals.data.read_all_unordered().await.unwrap();

    let reads = query_rows(&data);
    assert_eq!(reads.len(), 1, "one terminal read");
    assert_eq!(reads[0].record.cursor.input_seq.get(), 1);
    let answer: FiniteRead<
        serde_json::Value,
        crate::stages::common::handlers::source::placeholder::PlaceholderRead,
    > = reply(&reads[0]);
    assert!(matches!(answer, FiniteRead::EndOfInput));
    assert_eq!(facts(&data), 0);
    assert!(has_eof(&data));
}

#[tokio::test]
async fn shared_cold_port_resolves_once() {
    let builds = Arc::new(AtomicUsize::new(0));
    let script = Script(Arc::new(vec![vec![Marker::Row], vec![Marker::Row]]));
    let counted = builds.clone();
    let port: Port<dyn ScriptedInput> = Port::deferred(move || {
        counted.fetch_add(1, Ordering::SeqCst);
        Ok(script.clone())
    });
    let left =
        AdmitSource::<dyn UnifiedAsyncFiniteSourceHandler>::admit(ScriptedRows(port.clone()));
    let right = AdmitSource::<dyn UnifiedAsyncFiniteSourceHandler>::admit(ScriptedRows(port));
    let left_declarations = left.effect_declarations().unwrap();
    let right_declarations = right.effect_declarations().unwrap();
    let ports = EffectPortRegistry::collect_from_declarations(
        left_declarations.iter().chain(right_declarations.iter()),
    )
    .expect("one shared port registers once");
    let runtime = |declarations: Vec<EffectDeclaration>| {
        declarations
            .iter()
            .map(EffectDeclaration::runtime_projection)
            .collect::<Vec<_>>()
    };

    let mut topology = obzenflow_topology::TopologyBuilder::new();
    let left_stage = StageId::from_topology_id(topology.add_stage(Some("left".into())));
    let right_stage = StageId::from_topology_id(topology.add_stage(Some("right".into())));
    let journal = |stage| {
        Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)))
            as Arc<dyn Journal<ChainEvent>>
    };
    let mut resources = StageResourcesBuilder::new(
        FlowId::new(),
        SystemId::new(),
        Arc::new(topology.build_unchecked().unwrap()),
        Arc::new(TestJournal::new(JournalOwner::stage(left_stage))),
        HashMap::from([
            (left_stage, journal(left_stage)),
            (right_stage, journal(right_stage)),
        ]),
        HashMap::from([
            (left_stage, journal(left_stage)),
            (right_stage, journal(right_stage)),
        ]),
    )
    .with_effect_ports(ports)
    .build()
    .await
    .unwrap();
    let mut handles = Vec::new();
    let mut journals = Vec::new();
    for (stage, name, source, declarations) in [
        (left_stage, "left", left, runtime(left_declarations)),
        (right_stage, "right", right, runtime(right_declarations)),
    ] {
        let mut stage_resources = resources.take_stage_resources(stage).unwrap();
        stage_resources.effect_declarations = declarations;
        journals.push(stage_resources.data_journal.clone());
        let config = finite::FiniteSourceConfig::new(stage, name, "lifecycle");
        let handle = finite::AsyncFiniteSourceBuilder::new(source, config, stage_resources)
            .build()
            .await
            .unwrap();
        for event in [
            finite::FiniteSourceEvent::Initialize,
            finite::FiniteSourceEvent::Ready,
            finite::FiniteSourceEvent::Start,
        ] {
            handle.send_event(event).await.unwrap();
        }
        handles.push(handle);
    }
    for handle in &handles {
        tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
            .await
            .expect("source terminates")
            .expect("source completes");
    }
    assert_eq!(builds.load(Ordering::SeqCst), 1, "one resolution per run");
    for journal in journals {
        let data = journal.read_all_unordered().await.unwrap();
        assert_eq!(facts(&data), 2, "each stage reads its own input");
        assert_eq!(
            query_rows(&data)
                .iter()
                .map(|read| read.record.cursor.input_seq.get())
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextLimited(u32);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait LimitedInput: Send + Sync {
    #[query(type = "source.lifecycle.limited_read")]
    async fn read(
        &self,
        from: &NextLimited,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextLimited>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone, Default)]
struct Limited {
    calls: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
}

const LIMITED_DELAY: Duration = Duration::from_millis(300);

#[async_trait]
impl LimitedInput for Limited {
    async fn read(
        &self,
        from: &NextLimited,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextLimited>, obzenflow_runtime::queries::QueryError> {
        let call = {
            let mut calls = self.calls.lock().unwrap();
            calls.push(tokio::time::Instant::now());
            calls.len()
        };
        match (call, from.0) {
            (1, _) => Err(crate::queries::QueryError::recoverable(
                SourcePollErrorKind::Transport,
                SourceDiagnosticReason::RateLimited,
            )
            .with_retry_after(LIMITED_DELAY)),
            (_, 0) => Ok(FiniteRead::Batch {
                records: vec![Marker::Row],
                next: NextLimited(1),
            }),
            _ => Ok(FiniteRead::EndOfInput),
        }
    }
}

struct LimitedRows(Port<dyn LimitedInput>);

#[async_trait]
impl TypedAsyncFiniteSourceHandler for LimitedRows {
    type Output = Row;

    fn poll_timeout(&self) -> Option<Duration> {
        None
    }

    fn read(&self) -> SourceRead<Row> {
        SourceRead::finite(self.0.clone(), NextLimited(0), row)
    }
}

#[tokio::test]
async fn rate_limited_read_keeps_one_cursor() {
    let limited = Limited::default();
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(LimitedRows(Port::to(limited.clone())));
    let journals = run_finite(source, ports, declarations).await;
    let calls = limited.calls.lock().unwrap().clone();
    assert_eq!(
        calls.len(),
        3,
        "one refused read, its retry and the end of input"
    );
    assert!(
        calls[1] - calls[0] >= LIMITED_DELAY,
        "the retry waits for the delay that the system asked for"
    );
    let data = journals.data.read_all_unordered().await.unwrap();
    let rows: Vec<(u64, u32, Option<u32>, String)> = query_rows(&data)
        .iter()
        .map(|row| {
            (
                row.record.cursor.input_seq.get(),
                row.record.cursor.effect_ordinal.get(),
                row.attempt.map(EffectAttemptOrdinal::get),
                row.event_type.clone(),
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            (1, 0, Some(1), "query.failed".to_string()),
            (1, 0, Some(2), "query.answered".to_string()),
            (2, 0, Some(1), "query.answered".to_string()),
        ]
    );
    let scheduled: Vec<(u64, u32)> = data
        .iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(ExecutionPayload::Recovery(RecoveryFact::RetryScheduled {
                cursor,
                next_attempt,
                ..
            })) => Some((cursor.input_seq.get(), *next_attempt)),
            _ => None,
        })
        .collect();
    assert_eq!(scheduled, [(1, 2)], "the failed attempt schedules the next");
    assert_eq!(facts(&data), 1);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextHanging(u32);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait HangingInput: Send + Sync {
    #[query(type = "source.lifecycle.hanging_read")]
    async fn read(
        &self,
        from: &NextHanging,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextHanging>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone, Default)]
struct Hanging {
    ended: Arc<std::sync::Mutex<Vec<Duration>>>,
}

struct CallEnded {
    started: tokio::time::Instant,
    ended: Arc<std::sync::Mutex<Vec<Duration>>>,
}

impl Drop for CallEnded {
    fn drop(&mut self) {
        self.ended.lock().unwrap().push(self.started.elapsed());
    }
}

#[async_trait]
impl HangingInput for Hanging {
    async fn read(
        &self,
        _from: &NextHanging,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextHanging>, obzenflow_runtime::queries::QueryError> {
        let _call = CallEnded {
            started: tokio::time::Instant::now(),
            ended: self.ended.clone(),
        };
        std::future::pending().await
    }
}

async fn first_call_end<H>(hanging: &Hanging, line: H) -> Duration
where
    H: AdmitSource<dyn UnifiedAsyncFiniteSourceHandler>,
    H::Handler: UnifiedAsyncFiniteSourceHandler + Send + Sync + 'static,
{
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(line);
    let (stage, resources, _journals) = resources_with_ports(ports, declarations).await;
    let config = finite::FiniteSourceConfig::new(stage, "input", "lifecycle");
    let handle = finite::AsyncFiniteSourceBuilder::new(source, config, resources)
        .build()
        .await
        .unwrap();
    for event in [
        finite::FiniteSourceEvent::Initialize,
        finite::FiniteSourceEvent::Ready,
        finite::FiniteSourceEvent::Start,
    ] {
        handle.send_event(event).await.unwrap();
    }
    loop {
        if let Some(ended) = hanging.ended.lock().unwrap().first() {
            return *ended;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(start_paused = true)]
async fn query_line_default_deadline_ends_call() {
    let hanging = Hanging::default();
    let line = crate::queries::query(NextHanging(0))
        .decode(row)
        .through(Port::to(hanging.clone()));
    let ended = first_call_end(&hanging, line).await;
    assert_eq!(ended, Duration::from_secs(30));
}

#[tokio::test(start_paused = true)]
async fn query_line_poll_timeout_ends_call() {
    let hanging = Hanging::default();
    let line = crate::queries::query(NextHanging(0))
        .decode(row)
        .poll_timeout(Duration::from_secs(5))
        .through(Port::to(hanging.clone()));
    let ended = first_call_end(&hanging, line).await;
    assert_eq!(ended, Duration::from_secs(5));
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextAnswered(u32);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait AnsweredInput: Send + Sync {
    #[query(type = "source.lifecycle.answered_read")]
    async fn read(
        &self,
        from: &NextAnswered,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextAnswered>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone, Default)]
struct Answered {
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl AnsweredInput for Answered {
    async fn read(
        &self,
        from: &NextAnswered,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<FiniteRead<Marker, NextAnswered>, obzenflow_runtime::queries::QueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(match from.0 {
            0 => FiniteRead::Batch {
                records: vec![Marker::Row],
                next: NextAnswered(1),
            },
            _ => FiniteRead::EndOfInput,
        })
    }
}

fn answered_line(answered: &Answered) -> crate::queries::Query<Row> {
    crate::queries::query(NextAnswered(0))
        .decode(row)
        .poll_timeout(Duration::from_secs(1))
        .through(Port::to(answered.clone()))
}

async fn start_finite<H>(
    source: AdmittedSource<H>,
    stage: StageId,
    resources: StageResources,
) -> crate::supervised_base::StandardHandle<
    finite::FiniteSourceEvent<H>,
    finite::FiniteSourceState<H>,
>
where
    H: UnifiedAsyncFiniteSourceHandler + Send + Sync + 'static,
{
    let config = finite::FiniteSourceConfig::new(stage, "input", "lifecycle");
    let handle = finite::AsyncFiniteSourceBuilder::new(source, config, resources)
        .build()
        .await
        .unwrap();
    for event in [
        finite::FiniteSourceEvent::Initialize,
        finite::FiniteSourceEvent::Ready,
        finite::FiniteSourceEvent::Start,
    ] {
        handle.send_event(event).await.unwrap();
    }
    handle
}

async fn held_answer_run(answered: &Answered) -> (Arc<TestJournal<ChainEvent>>, StageId) {
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(answered_line(answered));
    let (stage, mut resources, _) = resources_with_ports(ports, declarations).await;
    let journal = Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)));
    let (writing, release) =
        journal.block_matching_append(|event| event.event_type() == "query.answered");
    resources.data_journal = journal.clone();
    let handle = start_finite(source, stage, resources).await;
    notified(&writing).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .expect("source completes");
    (journal, stage)
}

#[tokio::test(start_paused = true)]
async fn held_append_past_deadline_commits_once() {
    let answered = Answered::default();
    let (journal, _) = held_answer_run(&answered).await;

    assert_eq!(
        answered.calls.load(Ordering::SeqCst),
        2,
        "one supplier call for each read"
    );
    let rows = journal.read_all_unordered().await.unwrap();
    let reads: Vec<(u64, Option<u32>, String)> = query_rows(&rows)
        .iter()
        .map(|row| {
            (
                row.record.cursor.input_seq.get(),
                row.attempt.map(EffectAttemptOrdinal::get),
                row.event_type.clone(),
            )
        })
        .collect();
    assert_eq!(
        reads,
        [
            (1, Some(1), "query.answered".to_string()),
            (2, Some(1), "query.answered".to_string()),
        ]
    );
    assert_eq!(facts(&rows), 1, "the held answer advances to its record");
}

struct RecordedArchive {
    journal: Arc<TestJournal<ChainEvent>>,
    flow_id: String,
    stage: StageId,
}

impl RecordedArchive {
    async fn reader(&self) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        self.journal
            .reader()
            .await
            .map_err(|_| ReplayError::MissingJournal {
                path: self.archive_path().to_path_buf(),
            })
    }
}

#[async_trait]
impl ReplayArchive for RecordedArchive {
    async fn open_source_reader(
        &self,
        _: &str,
        _: StageType,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        self.reader().await
    }

    async fn open_effect_history(
        &self,
        _: &str,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        self.reader().await
    }

    fn source_data_journal_path(&self, _: &str) -> Result<PathBuf, ReplayError> {
        Ok(self.archive_path().join("input.log"))
    }

    fn archive_flow_id(&self) -> &str {
        &self.flow_id
    }

    fn archived_stage_id(&self, _: &str) -> Result<StageId, ReplayError> {
        Ok(self.stage)
    }

    fn archive_status(&self) -> ArchiveStatus {
        ArchiveStatus::Completed
    }

    fn status_derivation(&self) -> StatusDerivation {
        StatusDerivation {
            terminal_events_found: 1,
            chosen: ArchiveStatus::Completed,
            warning: None,
        }
    }

    fn allow_incomplete_archive(&self) -> bool {
        false
    }

    fn source_stage_keys(&self) -> Vec<String> {
        vec!["input".to_string()]
    }

    fn recorded_source_eof(&self, _: &str) -> Option<EofKind> {
        Some(EofKind::Natural)
    }

    fn archive_path(&self) -> &Path {
        Path::new("recorded")
    }

    fn claim_continuation(&self, _: FlowId, _: &CurrentRunLocator) -> Result<(), ReplayError> {
        Ok(())
    }
}

#[tokio::test(start_paused = true)]
async fn held_append_replay_calls_no_supplier() {
    let (recorded, stage) = held_answer_run(&Answered::default()).await;
    let rows = recorded.read_all_unordered().await.unwrap();
    let flow_id = query_rows(&rows)[0]
        .record
        .cursor
        .recorded_flow_id
        .to_string();
    let archive = Arc::new(RecordedArchive {
        journal: recorded,
        flow_id,
        stage,
    });

    let replayed = Answered::default();
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(answered_line(&replayed));
    let (stage, resources, journals) = resources_with(ports, declarations, Some(archive)).await;
    let handle = start_finite(source, stage, resources).await;
    tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .expect("replay terminates")
        .expect("replay completes");

    assert_eq!(
        replayed.calls.load(Ordering::SeqCst),
        0,
        "the strict replay asks no supplier"
    );
    let rows = journals.data.read_all_unordered().await.unwrap();
    assert_eq!(facts(&rows), 1, "the replay publishes the recorded record");
    assert!(has_eof(&rows));
}

#[tokio::test]
async fn failed_answer_append_records_journal_fatal() {
    let answered = Answered::default();
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(answered_line(&answered));
    let (stage, mut resources, journals) = resources_with_ports(ports, declarations).await;
    let journal = Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)));
    journal.fail_matching_append(|event| event.event_type() == "query.answered");
    resources.data_journal = journal.clone();
    let handle = start_finite(source, stage, resources).await;
    assert!(
        tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
            .await
            .expect("source terminates")
            .is_err(),
        "the stage fails"
    );

    let mut rows = journal.read_all_unordered().await.unwrap();
    rows.extend(journals.error.read_all_unordered().await.unwrap());
    let fatals: Vec<_> = rows
        .iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) => {
                Some((fatal.code, fatal.reason))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        fatals,
        [(StageFatalCode::Journal, StageFatalReason::JournalFailure)]
    );
    assert!(query_rows(&rows).is_empty(), "no answer commits");
}

#[derive(Clone)]
struct ChannelRows(Arc<crate::events::SourceReceiver<Row>>);

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for ChannelRows {
    type Output = Row;

    fn poll_timeout(&self) -> Option<Duration> {
        Some(Duration::from_secs(1))
    }

    fn read(&self) -> SourceRead<Row, crate::stages::common::handlers::source::Continuous> {
        SourceRead::channel(Arc::clone(&self.0))
    }
}

#[tokio::test(start_paused = true)]
async fn held_member_past_deadline_commits_once() {
    let (sender, receiver) = crate::events::source_channel::<Row>(1);
    let (source, ports, declarations) = admitted::<
        dyn crate::stages::common::handlers::UnifiedAsyncInfiniteSourceHandler,
        _,
    >(ChannelRows(Arc::new(receiver)));
    let (stage, mut resources, _) = resources_with_ports(ports, declarations).await;
    let journal = Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)));
    let (writing, release) = journal.block_matching_append(|event| {
        event.event_type() == obzenflow_core::source::MEMBER_RECORDED_EVENT_TYPE
    });
    resources.data_journal = journal.clone();
    let config = infinite::InfiniteSourceConfig::new(stage, "input", "lifecycle");
    let handle = infinite::AsyncInfiniteSourceBuilder::new(source, config, resources)
        .build()
        .await
        .unwrap();
    for event in [
        infinite::InfiniteSourceEvent::Initialize,
        infinite::InfiniteSourceEvent::Ready,
        infinite::InfiniteSourceEvent::Start,
    ] {
        handle.send_event(event).await.unwrap();
    }
    sender.send(Row).await.expect("the channel has space");
    notified(&writing).await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    release.notify_one();
    tokio::time::timeout(Duration::from_secs(30), async {
        while facts(&journal.read_all_unordered().await.unwrap()) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the held member publishes its record");
    handle
        .send_event(infinite::InfiniteSourceEvent::BeginDrain)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .expect("source completes");

    let rows = journal.read_all_unordered().await.unwrap();
    let members = rows
        .iter()
        .filter(|row| row.event_type() == obzenflow_core::source::MEMBER_RECORDED_EVENT_TYPE)
        .count();
    assert_eq!(members, 1, "the held member commits once");
    assert_eq!(facts(&rows), 1, "the held member advances to its record");
}
