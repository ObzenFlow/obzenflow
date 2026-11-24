// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::finite::fsm::tests::TestJournal;
use super::*;
use crate::effects::{EffectPortRegistry, SystemName};
use crate::id_conversions::StageIdExt;
use crate::stages::common::handlers::source::prepared::{AdmitSource, AdmittedSource};
use crate::stages::common::handlers::source::{
    Continuous, ContinuousRead, FiniteRead, InputReadiness, SourceRead,
};
use crate::stages::common::handlers::{
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
};
use crate::stages::resources_builder::{StageResources, StageResourcesBuilder};
use crate::supervised_base::{SupervisorBuilder, SupervisorHandle};
use async_trait::async_trait;
use futures::FutureExt;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourcePollContinuation, SourcePollErrorKind, StageLifecycleFact,
};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{ChainPayload, JournalRecord, SourceDiagnosticReason};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
use obzenflow_core::{ChainEvent, EventId, FlowId, StageId, SystemId, TypedPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextTake(u32);

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

#[obzenflow_runtime::effects::port]
#[async_trait]
trait LifecycleFeed: InputReadiness + Send + Sync {
    #[query(type = "source.lifecycle.take")]
    async fn take(
        &self,
        from: &NextTake,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<Marker, NextTake>, obzenflow_runtime::queries::QueryError>;
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
    NeverReady,
}

#[derive(Clone)]
struct Fixture {
    counts: Arc<Counts>,
    reading: Reading,
}

impl SystemName for Fixture {
    const NAME: &'static str = "lifecycle-fixture";
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

#[async_trait]
impl InputReadiness for Fixture {
    async fn ready(&self) -> Result<(), SourceError> {
        if matches!(self.reading, Reading::NeverReady) {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

#[async_trait]
impl LifecycleFeed for Fixture {
    async fn take(
        &self,
        from: &NextTake,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<Marker, NextTake>, obzenflow_runtime::queries::QueryError> {
        Ok(ContinuousRead {
            records: self.records().await,
            next: NextTake(from.0 + 1),
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

fn deferred<P>(fixture: &Fixture) -> crate::queries::QueryPort<P>
where
    P: ?Sized + Send + Sync + 'static,
    crate::effects::PortImpl<Fixture>: crate::effects::ServesPort<P, Fixture>,
{
    let fixture = fixture.clone();
    obzenflow_runtime::queries::QueryPort::deferred(move || {
        fixture.counts.builds.fetch_add(1, Ordering::SeqCst);
        Ok(fixture.clone())
    })
}

struct FiniteRows {
    port: obzenflow_runtime::queries::QueryPort<dyn LifecycleInput>,
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

struct LiveRows {
    port: obzenflow_runtime::queries::QueryPort<dyn LifecycleFeed>,
}

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for LiveRows {
    type Output = Row;

    fn read(&self) -> SourceRead<Row, Continuous> {
        SourceRead::continuous(self.port.clone(), NextTake(0), row)
    }
}

impl LiveRows {
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
    Vec<crate::effects::EffectDeclaration>,
)
where
    F: ?Sized,
    H: AdmitSource<F>,
{
    let admitted = handler.admit();
    let mut ports = EffectPortRegistry::new();
    ports.install_source_system(admitted.system());
    (admitted, ports, Vec::new())
}

async fn resources_with_ports(
    ports: EffectPortRegistry,
    declarations: Vec<crate::effects::EffectDeclaration>,
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

macro_rules! lifecycle_family {
    ($module:ident, $builder:path, $config:path, $event:path, $state:path, $family:path, $rows:ident) => {
        mod $module {
            use super::*;
            use $builder as Builder;
            use $config as Config;
            use $event as Event;
            use $state as State;
            type Handler = <$rows as AdmitSource<dyn $family>>::Handler;
            type Handle = crate::supervised_base::StandardHandle<Event<Handler>, State<Handler>>;

            async fn build(
                fixture: &Fixture,
            ) -> (Handle, Journals) {
                let (source, ports, declarations) = admitted::<dyn $family, _>($rows::of(fixture));
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
                let (source, ports, declarations) = admitted::<dyn $family, _>($rows::of(&fixture));
                let (stage, mut resources, _) = resources_with_ports(ports, declarations).await;
                let journal = Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)));
                let (writing, release) = journal.block_matching_append(|event| {
                    matches!(event.payload, ChainPayload::Fact(_))
                });
                resources.data_journal = journal.clone();
                let config = Config::new(stage, "input", "lifecycle");
                let handle = Builder::new(source, config, resources).build().await.unwrap();
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
                assert!(failure.cause.to_string().contains("failure during source publication"));
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
                assert!(finish(&handle).await.is_err(), "terminal report fails the stage");
                assert!(matches!(
                    handle.current_state(),
                    State::Failed(reason)
                        if reason == "Source 'input' cannot continue: terminal source transport error: the input is unavailable"
                ));
                assert_eq!(fixture.counts.reads.load(Ordering::SeqCst), 2, "no read after a terminal report");

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
                assert_eq!(facts(&data), 1, "the row accepted before the failure stays committed");
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
        }
    };
}

lifecycle_family!(
    finite_lifecycle,
    finite::AsyncFiniteSourceBuilder,
    finite::FiniteSourceConfig,
    finite::FiniteSourceEvent,
    finite::FiniteSourceState,
    UnifiedAsyncFiniteSourceHandler,
    FiniteRows
);
lifecycle_family!(
    infinite_lifecycle,
    infinite::AsyncInfiniteSourceBuilder,
    infinite::InfiniteSourceConfig,
    infinite::InfiniteSourceEvent,
    infinite::InfiniteSourceState,
    UnifiedAsyncInfiniteSourceHandler,
    LiveRows
);

async fn run_finite<H>(
    source: AdmittedSource<H>,
    ports: EffectPortRegistry,
    declarations: Vec<crate::effects::EffectDeclaration>,
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

fn source_inputs(rows: &Rows) -> Vec<obzenflow_core::query::SourceInputRecord> {
    rows.iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(ExecutionPayload::SourceInput(record)) => {
                Some(record.as_ref().clone())
            }
            _ => None,
        })
        .collect()
}

fn declared_binding(rows: &Rows) -> Option<obzenflow_core::effect_binding::SystemBinding> {
    rows.iter().find_map(|row| match &row.payload {
        ChainPayload::Execution(ExecutionPayload::SourceBindingDeclared(binding)) => {
            Some(binding.clone())
        }
        _ => None,
    })
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

fn reply<T: serde::de::DeserializeOwned>(record: &obzenflow_core::query::SourceInputRecord) -> T {
    match &record.input {
        obzenflow_core::query::SourceInput::QueryAnswered { answer: output, .. } => {
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

impl SystemName for Script {
    const NAME: &'static str = "lifecycle-script";
}

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

struct ScriptedRows(obzenflow_runtime::queries::QueryPort<dyn ScriptedInput>);

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
async fn port_read_records_system_binding() {
    let script = Script(Arc::new(vec![vec![Marker::Row], Vec::new()]));
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(
        ScriptedRows(obzenflow_runtime::queries::QueryPort::to(script)),
    );
    let journals = run_finite(source, ports, declarations).await;
    let data = journals.data.read_all_unordered().await.unwrap();

    let declared = declared_binding(&data).expect("the stage declares its binding");
    assert_eq!(declared.name(), "lifecycle-script");
    let reads = source_inputs(&data);
    assert_eq!(
        reads.iter().map(|read| read.number).collect::<Vec<_>>(),
        [1, 2, 3],
        "the empty answer and the end of input have their own read numbers"
    );
    assert!(reads.iter().all(|read| matches!(&read.input,obzenflow_core::query::SourceInput::QueryAnswered{query_type,..} if query_type=="source.lifecycle.scripted_read")));
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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct NextGuarded(u32);

#[obzenflow_runtime::effects::port]
#[async_trait]
trait GuardedTake: InputReadiness + Send + Sync {
    #[query(type = "source.lifecycle.guarded_take")]
    async fn take(
        &self,
        from: &NextGuarded,
        ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<Marker, NextGuarded>, obzenflow_runtime::queries::QueryError>;
}

#[derive(Clone, Default)]
struct Guarded {
    calls: Arc<AtomicUsize>,
}

impl SystemName for Guarded {
    const NAME: &'static str = "guarded-take";
}

#[async_trait]
impl InputReadiness for Guarded {
    async fn ready(&self) -> Result<(), SourceError> {
        Ok(())
    }
}

#[async_trait]
impl GuardedTake for Guarded {
    async fn take(
        &self,
        from: &NextGuarded,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<Marker, NextGuarded>, obzenflow_runtime::queries::QueryError> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        if call > 2 {
            return std::future::pending().await;
        }
        Ok(ContinuousRead {
            records: vec![Marker::Row],
            next: NextGuarded(from.0 + 1),
        })
    }
}

struct GuardedRows(obzenflow_runtime::queries::QueryPort<dyn GuardedTake>);

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for GuardedRows {
    type Output = Row;

    fn read(&self) -> SourceRead<Row, Continuous> {
        SourceRead::continuous(self.0.clone(), NextGuarded(0), row)
    }
}

#[tokio::test]
async fn pending_query_records_completed_answers() {
    let guarded = Guarded::default();
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncInfiniteSourceHandler, _>(
        GuardedRows(obzenflow_runtime::queries::QueryPort::to(guarded.clone())),
    );
    let (stage, resources, journals) = resources_with_ports(ports, declarations).await;
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
    tokio::time::timeout(Duration::from_secs(3), async {
        while guarded.calls.load(Ordering::SeqCst) < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the source takes three times");
    handle
        .send_event(infinite::InfiniteSourceEvent::BeginDrain)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .unwrap();
    let rows = journals.data.read_all_unordered().await.unwrap();
    assert_eq!(source_inputs(&rows).len(), 2);
    assert_eq!(facts(&rows), 2);
    assert_eq!(attempt_starts(&rows), 0);
}

#[tokio::test]
async fn readiness_wait_issues_no_call() {
    let fixture = Fixture::new(Reading::NeverReady);
    let (source, ports, declarations) =
        admitted::<dyn UnifiedAsyncInfiniteSourceHandler, _>(WaitingRows(deferred(&fixture)));
    let (stage, resources, journals) = resources_with_ports(ports, declarations).await;
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
    tokio::time::sleep(Duration::from_millis(50)).await;
    handle
        .send_event(infinite::InfiniteSourceEvent::BeginDrain)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .unwrap();
    assert_eq!(fixture.counts.reads.load(Ordering::SeqCst), 0);
    let data = journals.data.read_all_unordered().await.unwrap();
    assert!(source_inputs(&data).is_empty());
}

struct WaitingRows(obzenflow_runtime::queries::QueryPort<dyn LifecycleFeed>);

#[async_trait]
impl TypedAsyncInfiniteSourceHandler for WaitingRows {
    type Output = Row;

    fn read(&self) -> SourceRead<Row, Continuous> {
        SourceRead::continuous(self.0.clone(), NextTake(0), row)
    }
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

impl SystemName for Inline {
    const NAME: &'static str = "inline-input";
}

impl InlineInput for Inline {
    fn read(
        &self,
        from: &NextInline,
    ) -> Result<FiniteRead<Marker, NextInline>, obzenflow_runtime::queries::QueryError> {
        self.0.record(std::thread::current().id());
        Ok(placed(from.0, NextInline))
    }
}

struct InlineRows(obzenflow_runtime::queries::QueryPort<dyn InlineInput>);

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
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(
        InlineRows(obzenflow_runtime::queries::QueryPort::to(inline.clone())),
    );
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

impl SystemName for Blocking {
    const NAME: &'static str = "blocking-input";
}

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

struct BlockingRows(obzenflow_runtime::queries::QueryPort<dyn BlockingInput>);

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
        BlockingRows(obzenflow_runtime::queries::QueryPort::to(blocking.clone())),
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

impl SystemName for Awaited {
    const NAME: &'static str = "awaited-input";
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

struct AwaitedRows(obzenflow_runtime::queries::QueryPort<dyn AwaitedInput>);

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
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(
        AwaitedRows(obzenflow_runtime::queries::QueryPort::to(awaited.clone())),
    );
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
    assert!(
        source_inputs(&before).is_empty(),
        "the read is still awaited"
    );
    assert_eq!(facts(&before), 0);
    awaited.release.notify_one();
    notified(&awaited.waiting).await;
    awaited.release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .unwrap();
    let after = journals.data.read_all_unordered().await.unwrap();
    assert_eq!(source_inputs(&after).len(), 2);
    assert_eq!(facts(&after), 1);
}

#[tokio::test]
async fn placeholder_source_read_records_placeholder() {
    let source = AdmittedSource::finite_placeholder(None);
    let mut ports = EffectPortRegistry::new();
    ports.install_source_system(source.system());
    let journals = run_finite(source, ports, Vec::new()).await;
    let data = journals.data.read_all_unordered().await.unwrap();

    let declared = declared_binding(&data).expect("the placeholder declares its binding");
    assert_eq!(declared.name(), "placeholder");
    let reads = source_inputs(&data);
    assert_eq!(reads.len(), 1, "one terminal read");
    assert_eq!(reads[0].number, 1);
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
    let port: obzenflow_runtime::queries::QueryPort<dyn ScriptedInput> =
        obzenflow_runtime::queries::QueryPort::deferred(move || {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(script.clone())
        });
    let left =
        AdmitSource::<dyn UnifiedAsyncFiniteSourceHandler>::admit(ScriptedRows(port.clone()));
    let right = AdmitSource::<dyn UnifiedAsyncFiniteSourceHandler>::admit(ScriptedRows(port));
    let left_declarations = Vec::new();
    let right_declarations = Vec::new();
    let mut ports = EffectPortRegistry::new();
    ports.install_source_system(left.system());
    ports.install_source_system(right.system());

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
        (left_stage, "left", left, left_declarations),
        (right_stage, "right", right, right_declarations),
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
            source_inputs(&data)
                .iter()
                .map(|read| read.number)
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

impl SystemName for Limited {
    const NAME: &'static str = "limited";
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

struct LimitedRows(obzenflow_runtime::queries::QueryPort<dyn LimitedInput>);

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
async fn rate_limited_retry_keeps_read_number() {
    let limited = Limited::default();
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncFiniteSourceHandler, _>(
        LimitedRows(obzenflow_runtime::queries::QueryPort::to(limited.clone())),
    );
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
    let cursors: Vec<(u64, u32)> = source_inputs(&data)
        .iter()
        .map(|record| (record.number, record.attempt))
        .collect();
    assert_eq!(cursors, [(1, 1), (1, 2), (2, 1)]);
    assert_eq!(facts(&data), 1);
}

struct Scripted {
    rows: AtomicUsize,
}

impl SystemName for Scripted {
    const NAME: &'static str = "scripted-feed";
}

#[async_trait]
impl InputReadiness for Scripted {
    async fn ready(&self) -> Result<(), SourceError> {
        if self.rows.load(Ordering::SeqCst) == 0 {
            std::future::pending::<()>().await;
        }
        Ok(())
    }
}

#[async_trait]
impl LifecycleFeed for Scripted {
    async fn take(
        &self,
        from: &NextTake,
        _ctx: &obzenflow_runtime::queries::QueryContext,
    ) -> Result<ContinuousRead<Marker, NextTake>, obzenflow_runtime::queries::QueryError> {
        self.rows.fetch_sub(1, Ordering::SeqCst);
        Ok(ContinuousRead {
            records: vec![Marker::Row],
            next: NextTake(from.0 + 1),
        })
    }
}

#[tokio::test]
async fn continuous_read_second_system_supplies_rows() {
    let rows = LiveRows {
        port: obzenflow_runtime::queries::QueryPort::to(Scripted {
            rows: AtomicUsize::new(2),
        }),
    };
    let (source, ports, declarations) = admitted::<dyn UnifiedAsyncInfiniteSourceHandler, _>(rows);
    let (stage, resources, journals) = resources_with_ports(ports, declarations).await;
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
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let data = journals.data.read_all_unordered().await.unwrap();
            if facts(&data) == 2 {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the second system supplies both rows");
    handle
        .send_event(infinite::InfiniteSourceEvent::BeginDrain)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), handle.wait_for_completion())
        .await
        .expect("source terminates")
        .unwrap();
    let data = journals.data.read_all_unordered().await.unwrap();
    assert_eq!(
        source_inputs(&data).len(),
        2,
        "the readiness waits made no read"
    );
}
