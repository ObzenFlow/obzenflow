// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Controlled acquisition, cancellation and cleanup through the real source tasks.

use super::finite::fsm::tests::TestJournal;
use super::*;
use crate::id_conversions::StageIdExt;
use crate::stages::common::handlers::source::prepared::{
    AdmitSource, ConnectorSource, DirectSource,
};
use crate::stages::common::handlers::source::typed::SourceObservationSink;
use crate::stages::common::handlers::{
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
};
use crate::stages::resources_builder::{StageResources, StageResourcesBuilder};
use crate::supervised_base::{SupervisorBuilder, SupervisorHandle};
use async_trait::async_trait;
use futures::FutureExt;
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceOpenIntent, SourcePollContinuation, SourcePollErrorKind,
    StageLifecycleFact,
};
use obzenflow_core::event::{ChainPayload, JournalRecord};
use obzenflow_core::journal::archive::{
    ArchiveStatus, ReplayArchive, ReplayError, StatusDerivation,
};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal, JournalReader};
use obzenflow_core::{ChainEvent, EventId, FlowId, StageId, SystemId, TypedPayload};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Serialize, Deserialize)]
struct Row;
impl TypedPayload for Row {
    const EVENT_TYPE: &'static str = "source.lifecycle.row";
}

#[derive(Default)]
struct Counts {
    opens: AtomicUsize,
    installations: AtomicUsize,
    polls: AtomicUsize,
    drains: AtomicUsize,
    drops: AtomicUsize,
    block_cleanup: AtomicBool,
    cleaning: Notify,
    release_cleanup: Notify,
    opening: Notify,
    polling: Notify,
    journal_failure: Arc<AtomicBool>,
}

struct Resource(Arc<Counts>);
impl Drop for Resource {
    fn drop(&mut self) {
        self.0.drops.fetch_add(1, Ordering::SeqCst);
    }
}

#[derive(Clone, Copy)]
enum Opening {
    Ready,
    Fail,
    Pending,
}
#[derive(Clone, Copy)]
enum Reading {
    Eof,
    Pending,
    FailJournal,
    RowThenPending,
    RowThenTerminal,
    RejectThenPending,
}

#[derive(Clone)]
struct Fixture {
    counts: Arc<Counts>,
    opening: Opening,
    reading: Reading,
    drain_fails: bool,
}

impl Fixture {
    fn new(opening: Opening, reading: Reading) -> Self {
        Self {
            counts: Arc::default(),
            opening,
            reading,
            drain_fails: false,
        }
    }

    async fn open_reader(&self, context: SourceReaderInitContext) -> Result<Reader, SourceError> {
        assert_eq!(context.stage_name, "input");
        assert_eq!(context.flow_name, "lifecycle");
        self.counts.opens.fetch_add(1, Ordering::SeqCst);
        let resource = Resource(self.counts.clone());
        self.counts.opening.notify_one();
        match self.opening {
            Opening::Fail => Err(SourceError::Transport(
                obzenflow_core::event::SourceDiagnosticReason::InputUnavailable.into(),
            )),
            Opening::Pending => std::future::pending().await,
            Opening::Ready => Ok(Reader {
                resource,
                reading: self.reading,
                drain_fails: self.drain_fails,
            }),
        }
    }
}

struct Reader {
    resource: Resource,
    reading: Reading,
    drain_fails: bool,
}
impl Reader {
    async fn read(&mut self) -> Result<Option<Vec<Row>>, SourceError> {
        let counts = &self.resource.0;
        let poll = counts.polls.fetch_add(1, Ordering::SeqCst);
        counts.polling.notify_one();
        match self.reading {
            Reading::Eof => Ok(None),
            Reading::RowThenPending if poll == 0 => Ok(Some(vec![Row])),
            Reading::RowThenPending => std::future::pending().await,
            Reading::Pending => std::future::pending().await,
            Reading::FailJournal => {
                counts.journal_failure.store(true, Ordering::SeqCst);
                Err(SourceError::Validation(
                    obzenflow_core::event::SourceDiagnosticReason::InvalidRecord.into(),
                ))
            }
            Reading::RowThenTerminal if poll == 0 => Ok(Some(vec![Row])),
            Reading::RowThenTerminal => Err(SourceError::Terminal {
                kind: SourcePollErrorKind::Transport,
                diagnostic: obzenflow_core::event::SourceDiagnosticReason::InputUnavailable.into(),
            }),
            Reading::RejectThenPending if poll == 0 => Err(SourceError::Validation(
                obzenflow_core::event::SourceDiagnosticReason::InvalidValue.into(),
            )),
            Reading::RejectThenPending => std::future::pending().await,
        }
    }

    async fn cleanup(&mut self) -> Result<(), SourceError> {
        self.resource.0.drains.fetch_add(1, Ordering::SeqCst);
        self.resource.0.cleaning.notify_one();
        if self.resource.0.block_cleanup.load(Ordering::SeqCst) {
            self.resource.0.release_cleanup.notified().await;
        }
        if self.drain_fails {
            Err(SourceError::Other(
                obzenflow_core::event::SourceDiagnosticReason::Unclassified.into(),
            ))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl AsyncFiniteSourceConnector for Fixture {
    type Output = Row;
    type Reader = Reader;
    async fn open(&self, context: SourceReaderInitContext) -> Result<Reader, SourceError> {
        self.open_reader(context).await
    }
}
#[async_trait]
impl AsyncInfiniteSourceConnector for Fixture {
    type Output = Row;
    type Reader = Reader;
    async fn open(&self, context: SourceReaderInitContext) -> Result<Reader, SourceError> {
        self.open_reader(context).await
    }
}
#[async_trait]
impl TypedAsyncFiniteSourceHandler for Reader {
    type Output = Row;
    fn install_source_observation_sink(&mut self, _sink: SourceObservationSink) {
        self.resource.0.installations.fetch_add(1, Ordering::SeqCst);
    }
    fn poll_timeout(&self) -> Option<Duration> {
        None
    }
    async fn next(&mut self) -> Result<Option<Vec<Row>>, SourceError> {
        self.read().await
    }
    async fn drain(&mut self) -> Result<(), SourceError> {
        self.cleanup().await
    }
}
#[async_trait]
impl TypedAsyncInfiniteSourceHandler for Reader {
    type Output = Row;
    fn install_source_observation_sink(&mut self, _sink: SourceObservationSink) {
        self.resource.0.installations.fetch_add(1, Ordering::SeqCst);
    }
    async fn next(&mut self) -> Result<Vec<Row>, SourceError> {
        Ok(self.read().await?.unwrap_or_default())
    }
    async fn drain(&mut self) -> Result<(), SourceError> {
        self.cleanup().await
    }
}

struct Journals {
    data: Arc<dyn Journal<ChainEvent>>,
    error: Arc<dyn Journal<ChainEvent>>,
}

async fn resources(counts: &Counts) -> (StageId, StageResources, Journals) {
    let mut topology = obzenflow_topology::TopologyBuilder::new();
    let source = topology.add_stage(Some("input".into()));
    let sink = topology.add_stage(Some("output".into()));
    let stage = StageId::from_topology_id(source);
    let sink = StageId::from_topology_id(sink);
    let system = Arc::new(
        TestJournal::new(JournalOwner::stage(stage))
            .with_append_failure(counts.journal_failure.clone()),
    );
    let journal = || {
        Arc::new(
            TestJournal::<ChainEvent>::new(JournalOwner::stage(stage))
                .with_append_failure(counts.journal_failure.clone()),
        ) as Arc<dyn Journal<ChainEvent>>
    };
    let mut resources = StageResourcesBuilder::new(
        FlowId::new(),
        SystemId::new(),
        Arc::new(topology.build_unchecked().unwrap()),
        system.clone(),
        HashMap::from([(stage, journal()), (sink, journal())]),
        HashMap::from([(stage, journal()), (sink, journal())]),
    )
    .build()
    .await
    .unwrap();
    let stage_resources = resources.take_stage_resources(stage).unwrap();
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

fn source_failures(rows: &Rows) -> Vec<(EventId, &ExecutionPayload)> {
    rows.iter()
        .filter_map(|row| match &row.payload {
            ChainPayload::Execution(
                payload @ (ExecutionPayload::SourcePollError(_)
                | ExecutionPayload::SourceOpenFailed(_)),
            ) => Some((row.envelope.provenance.event.id, payload)),
            _ => None,
        })
        .collect()
}

struct ParkBeforePoll(Arc<Notify>);
impl SourceBoundary for ParkBeforePoll {
    fn around_poll<'a>(&'a self, _execute: SourcePollExecution<'a>) -> SourceBoundaryFuture<'a> {
        Box::pin(async move {
            self.0.notify_one();
            std::future::pending().await
        })
    }
}

async fn notified(notify: &Notify) {
    tokio::time::timeout(Duration::from_secs(3), notify.notified())
        .await
        .expect("lifecycle progress");
}

macro_rules! lifecycle_family {
    ($module:ident, $builder:path, $config:path, $event:path, $state:path, $family:path) => {
        mod $module {
            use super::*;
            use $builder as Builder;
            use $config as Config;
            use $event as Event;
            use $state as State;
            type Handler = <Fixture as AdmitSource<dyn $family, ConnectorSource>>::Handler;
            type Handle = crate::supervised_base::StandardHandle<Event<Handler>, State<Handler>>;

            async fn build(
                fixture: Fixture,
                boundary: Option<Arc<dyn SourceBoundary>>,
                resume: bool,
            ) -> (Handle, Journals) {
                let (stage, mut resources, journals) = resources(&fixture.counts).await;
                if resume {
                    resources.runtime_execution = crate::execution::RuntimeExecution::new(
                        crate::execution::RuntimeMode::Resume,
                        Some(Arc::new(EmptyArchive(stage, None))),
                    );
                    let control = resources.runtime_execution.resume_control().unwrap();
                    // This test supplies an already-reconstructed plan requiring
                    // live acquisition. Finite positioning remains FLOWIP-134i.
                    control.record_generation_boundary(stage, control.resume_generation());
                }
                let mut config = Config::new(stage, "input", "lifecycle");
                config.source_boundary = boundary;
                (
                    Builder::new(
                        <Fixture as AdmitSource<dyn $family, ConnectorSource>>::prepare(fixture),
                        config,
                        resources,
                    )
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
            async fn direct_and_connector_admission_share_the_adapter_and_defer_capabilities() {
                let direct_counts = Arc::new(Counts::default());
                let reader = Reader {
                    resource: Resource(direct_counts.clone()),
                    reading: Reading::Pending,
                    drain_fails: false,
                };
                // Both routes must produce the very same existing adapter type.
                let direct: Handler =
                    <Reader as AdmitSource<dyn $family, DirectSource>>::prepare(reader);
                let fixture = Fixture::new(Opening::Ready, Reading::Pending);
                let connector_counts = fixture.counts.clone();
                let connector =
                    <Fixture as AdmitSource<dyn $family, ConnectorSource>>::prepare(fixture);

                for (mut handler, counts, expected_opens) in [
                    (direct, direct_counts, 0),
                    (connector, connector_counts, 1),
                ] {
                    let stage_id = StageId::new();
                    handler.install_writer_id(obzenflow_core::WriterId::from(stage_id));
                    assert_eq!(counts.opens.load(Ordering::SeqCst), 0);
                    assert_eq!(counts.installations.load(Ordering::SeqCst), 0);

                    let context = SourceReaderInitContext {
                        stage_id,
                        stage_name: "input".into(),
                        flow_name: "lifecycle".into(),
                    };
                    handler.acquire(context.clone()).await.unwrap();
                    handler.acquire(context).await.unwrap();
                    assert_eq!(counts.opens.load(Ordering::SeqCst), expected_opens);
                    assert_eq!(counts.installations.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                    handler.drain().await.unwrap();
                    drop(handler);
                    assert_eq!(counts.drains.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                }
            }

            #[tokio::test]
            async fn cancelled_acquisition_cannot_reopen() {
                let fixture = Fixture::new(Opening::Pending, Reading::Pending);
                let counts = fixture.counts.clone();
                let mut handler =
                    <Fixture as AdmitSource<dyn $family, ConnectorSource>>::prepare(fixture);
                let context = SourceReaderInitContext {
                    stage_id: StageId::new(),
                    stage_name: "input".into(),
                    flow_name: "lifecycle".into(),
                };
                // Poll opening once, then drop its pending future and resources.
                assert!(handler.acquire(context.clone()).now_or_never().is_none());
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                assert!(matches!(
                    handler.acquire(context).now_or_never(),
                    Some(Err(_))
                ));
                assert_eq!(counts.opens.load(Ordering::SeqCst), 1);
                assert_eq!(counts.installations.load(Ordering::SeqCst), 0);
                assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
            }

            #[tokio::test]
            async fn materialisation_and_stop_before_acquisition_are_cold() {
                let fixture = Fixture::new(Opening::Ready, Reading::Pending);
                let counts = fixture.counts.clone();
                let (handle, _) = build(fixture.clone(), None, false).await;
                assert_eq!(counts.opens.load(Ordering::SeqCst), 0);
                // All commands are queued without yielding; the running select
                // must honour the stop before polling the opening future.
                start(&handle).await;
                handle.send_event(Event::BeginDrain).await.unwrap();
                finish(&handle).await.unwrap();
                assert_eq!(counts.opens.load(Ordering::SeqCst), 0);
                assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                assert_eq!(counts.drains.load(Ordering::SeqCst), 0);
            }

            #[tokio::test]
            async fn replay_opening_is_owned_until_settled_without_claiming_startup() {
                use crate::stages::common::stage_lifecycle::StageMilestone;
                for fail in [false, true] {
                    let fixture = Fixture::new(Opening::Ready, Reading::Pending);
                    let counts = fixture.counts.clone();
                    let (stage, mut resources, _) = resources(&counts).await;
                    let opening = Arc::new(Notify::new());
                    let release = Arc::new(Notify::new());
                    resources.runtime_execution = crate::execution::RuntimeExecution::new(
                        crate::execution::RuntimeMode::Replay,
                        Some(Arc::new(EmptyArchive(stage, Some((opening.clone(), release.clone()))))),
                    );
                    let handle = Builder::new(
                        <Fixture as AdmitSource<dyn $family, ConnectorSource>>::prepare(fixture),
                        Config::new(stage, "input", "lifecycle"), resources,
                    ).build().await.unwrap();
                    start(&handle).await;
                    notified(&opening).await;
                    assert!(matches!(handle.current_state(), State::AcquiringInput));
                    assert!(handle.wait_for_milestone(StageMilestone::Started).now_or_never().is_none());
                    handle.send_event(if fail { Event::Error("failure while opening replay".into()) } else { Event::BeginDrain }).await.unwrap();
                    if fail {
                        let failure = tokio::time::timeout(Duration::from_secs(3), handle.wait_for_failure()).await.unwrap().unwrap();
                        assert!(failure.cause.to_string().contains("failure while opening replay"));
                    } else {
                        tokio::time::timeout(Duration::from_secs(3), async {
                            while !matches!(handle.current_state(), State::Draining) { tokio::task::yield_now().await; }
                        }).await.unwrap();
                    }
                    assert!(handle.wait_for_stage_exit().now_or_never().is_none());
                    release.notify_one();
                    assert_eq!(finish(&handle).await.is_err(), fail);
                    assert!(handle.wait_for_milestone(StageMilestone::Started).await.is_err());
                    assert_eq!(counts.opens.load(Ordering::SeqCst), 0);
                }
            }

            #[tokio::test]
            async fn source_failure_is_visible_while_accepted_output_publication_settles() {
                let fixture = Fixture::new(Opening::Ready, Reading::RowThenPending);
                let counts = fixture.counts.clone();
                let (stage, mut resources, _) = resources(&counts).await;
                let journal = Arc::new(TestJournal::<ChainEvent>::new(JournalOwner::stage(stage)));
                let (writing, release) = journal.block_matching_append(|event| matches!(event.payload, obzenflow_core::event::ChainPayload::Fact(_)));
                resources.data_journal = journal.clone();
                let handle = Builder::new(
                    <Fixture as AdmitSource<dyn $family, ConnectorSource>>::prepare(fixture),
                    Config::new(stage, "input", "lifecycle"), resources,
                ).build().await.unwrap();
                start(&handle).await;
                notified(&writing).await;
                handle.send_event(Event::Error("failure during source publication".into())).await.unwrap();
                let failure = tokio::time::timeout(Duration::from_secs(3), handle.wait_for_failure()).await.unwrap().unwrap();
                assert!(failure.cause.to_string().contains("failure during source publication"));
                assert!(matches!(handle.current_state(), State::Failing(_)));
                assert!(handle.wait_for_stage_exit().now_or_never().is_none());
                release.notify_one();
                assert!(finish(&handle).await.is_err());
                let records = journal.read_all_unordered().await.unwrap();
                assert_eq!(records.iter().filter(|row| matches!(row.payload, obzenflow_core::event::ChainPayload::Fact(_))).count(), 1);
                assert_eq!(counts.drains.load(Ordering::SeqCst), 1);
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
            }

            #[tokio::test]
            async fn failure_is_acknowledged_before_owned_reader_cleanup_finishes() {
                use crate::stages::common::stage_handle::FORCE_SHUTDOWN_MESSAGE;
                let fixture = Fixture::new(Opening::Ready, Reading::Pending);
                let counts = fixture.counts.clone();
                counts.block_cleanup.store(true, Ordering::SeqCst);
                let (handle, _) = build(fixture, None, false).await;
                start(&handle).await;
                notified(&counts.polling).await;
                handle.send_event(Event::Error("original failure".into())).await.unwrap();
                let failure = tokio::time::timeout(Duration::from_secs(3), handle.wait_for_failure()).await.unwrap().unwrap();
                assert!(failure.cause.to_string().contains("original failure"));
                notified(&counts.cleaning).await;
                assert!(matches!(handle.current_state(), State::Failing(cause) if cause == "original failure"));
                handle.send_event(Event::Error(FORCE_SHUTDOWN_MESSAGE.into())).await.unwrap();
                assert!(handle.wait_for_stage_exit().now_or_never().is_none());
                assert_eq!(counts.drops.load(Ordering::SeqCst), 0);
                counts.release_cleanup.notify_one();
                assert!(finish(&handle).await.is_err());
                assert!(matches!(handle.current_state(), State::Failed(cause) if cause == "original failure"));
                assert_eq!(counts.drains.load(Ordering::SeqCst), 1);
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
            }

            #[tokio::test]
            async fn failure_during_cancellation_remains_a_failure_after_cleanup() {
                use crate::stages::common::stage_handle::FORCE_SHUTDOWN_MESSAGE;
                let fixture = Fixture::new(Opening::Ready, Reading::Pending);
                let counts = fixture.counts.clone();
                counts.block_cleanup.store(true, Ordering::SeqCst);
                let (handle, _) = build(fixture, None, false).await;
                start(&handle).await;
                notified(&counts.polling).await;
                handle.send_event(Event::Error(FORCE_SHUTDOWN_MESSAGE.into())).await.unwrap();
                notified(&counts.cleaning).await;
                assert!(matches!(handle.current_state(), State::Cancelling(_)));
                assert!(handle.wait_for_failure().now_or_never().is_none());
                handle.send_event(Event::Error("failure during cancellation".into())).await.unwrap();
                let failure = tokio::time::timeout(Duration::from_secs(3), handle.wait_for_failure()).await.unwrap().unwrap();
                assert!(failure.cause.to_string().contains("failure during cancellation"));
                assert!(matches!(handle.current_state(), State::Failing(_)));
                counts.release_cleanup.notify_one();
                assert!(finish(&handle).await.is_err());
                assert!(matches!(handle.current_state(), State::Failed(cause) if cause == "failure during cancellation"));
                assert_eq!(counts.drains.load(Ordering::SeqCst), 1);
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
            }

            #[tokio::test]
            async fn partial_open_failure_is_terminal_and_source_attributed() {
                for resume in [false, true] {
                    let fixture = Fixture::new(Opening::Fail, Reading::Pending);
                    let counts = fixture.counts.clone();
                    let (handle, journals) = build(fixture, None, resume).await;
                    start(&handle).await;
                    assert!(finish(&handle).await.is_err(), "failed FSM must fail its handle");
                    assert!(matches!(handle.current_state(), State::Failed(_)));
                    assert_eq!(counts.opens.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                    assert_eq!(counts.drains.load(Ordering::SeqCst), 0);
                    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                    let data = journals.data.read_all_unordered().await.unwrap();
                    let evidence = format!("{data:?}");
                    assert!(
                        evidence.contains(if resume {
                            "Cannot resume source 'input': the input is unavailable."
                        } else {
                            "Cannot start source 'input': the input is unavailable."
                        }),
                        "{evidence}"
                    );

                    // Opening evidence is committed once, distinct from any poll, and
                    // the lifecycle failure links to it without an EOF.
                    let errors = journals.error.read_all_unordered().await.unwrap();
                    let failures = source_failures(&errors);
                    assert_eq!(failures.len(), 1, "{failures:?}");
                    let (opened_id, ExecutionPayload::SourceOpenFailed(opened)) = failures[0] else {
                        panic!("expected source.open_failed, got {failures:?}");
                    };
                    assert_eq!(opened.error_type, SourcePollErrorKind::Transport);
                    assert_eq!(
                        opened.intent,
                        if resume { SourceOpenIntent::Resume } else { SourceOpenIntent::Start }
                    );
                    assert_eq!(failed_cause(&data), Some(Some(opened_id)));
                    assert!(!has_eof(&data));
                }
            }

            #[tokio::test]
            async fn terminal_poll_failure_commits_its_diagnostic_and_fails_without_eof() {
                let fixture = Fixture::new(Opening::Ready, Reading::RowThenTerminal);
                let counts = fixture.counts.clone();
                let (handle, journals) = build(fixture, None, false).await;
                start(&handle).await;
                assert!(finish(&handle).await.is_err(), "terminal report fails the stage");
                assert!(matches!(
                    handle.current_state(),
                    State::Failed(reason)
                        if reason == "Source 'input' cannot continue: terminal source transport error: the input is unavailable"
                ));
                assert_eq!(counts.polls.load(Ordering::SeqCst), 2, "no poll after a terminal report");
                assert_eq!(counts.drains.load(Ordering::SeqCst), 1, "cleanup runs once");

                let data = journals.data.read_all_unordered().await.unwrap();
                let errors = journals.error.read_all_unordered().await.unwrap();
                let failures = source_failures(&errors);
                assert_eq!(failures.len(), 1, "{failures:?}");
                let (diagnostic_id, ExecutionPayload::SourcePollError(poll)) = failures[0] else {
                    panic!("expected source.poll_error, got {failures:?}");
                };
                assert_eq!(poll.continuation, SourcePollContinuation::Terminal);
                assert_eq!(failed_cause(&data), Some(Some(diagnostic_id)));
                assert!(!has_eof(&data), "a terminal source writes no EOF");
                assert_eq!(
                    data.iter().filter(|row| row.payload.consumes_data_credit()).count(),
                    1,
                    "the row accepted before the failure stays committed"
                );
            }

            #[tokio::test]
            async fn record_rejection_is_journalled_and_reading_continues() {
                let fixture = Fixture::new(Opening::Ready, Reading::RejectThenPending);
                let counts = fixture.counts.clone();
                let (handle, journals) = build(fixture, None, false).await;
                start(&handle).await;
                // A second poll starts only after the first poll's rejection committed.
                tokio::time::timeout(Duration::from_secs(3), async {
                    while counts.polls.load(Ordering::SeqCst) < 2 {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .expect("reading continues after a rejection");
                let errors = journals.error.read_all_unordered().await.unwrap();
                let failures = source_failures(&errors);
                assert_eq!(failures.len(), 1, "{failures:?}");
                let (_, ExecutionPayload::SourcePollError(poll)) = failures[0] else {
                    panic!("expected source.poll_error, got {failures:?}");
                };
                assert_eq!(poll.continuation, SourcePollContinuation::Recoverable);
                assert_eq!(poll.error_type, SourcePollErrorKind::Validation);
                handle.send_event(Event::BeginDrain).await.unwrap();
                finish(&handle).await.unwrap();
            }

            #[tokio::test]
            async fn cancelling_partial_open_releases_resources_without_a_reader() {
                for abort in [false, true] {
                    let fixture = Fixture::new(Opening::Pending, Reading::Pending);
                    let counts = fixture.counts.clone();
                    let (handle, _) = build(fixture, None, false).await;
                    start(&handle).await;
                    notified(&counts.opening).await;
                    assert!(matches!(handle.current_state(), State::AcquiringInput));
                    assert!(handle.wait_for_milestone(crate::stages::common::stage_lifecycle::StageMilestone::Started).now_or_never().is_none());
                    if abort {
                        handle.abort_and_wait().await.unwrap();
                    } else {
                        handle.send_event(Event::BeginDrain).await.unwrap();
                        finish(&handle).await.unwrap();
                    }
                    assert_eq!(counts.opens.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                    assert_eq!(counts.drains.load(Ordering::SeqCst), 0);
                }
            }

            #[tokio::test]
            async fn acquired_reader_is_cleaned_before_any_poll_on_stop_or_failure() {
                for fail in [false, true] {
                    let mut fixture = Fixture::new(Opening::Ready, Reading::Pending);
                    fixture.drain_fails = fail;
                    let counts = fixture.counts.clone();
                    let entered = Arc::new(Notify::new());
                    let (handle, journals) = build(
                        fixture,
                        Some(Arc::new(ParkBeforePoll(entered.clone()))),
                        false,
                    )
                    .await;
                    start(&handle).await;
                    notified(&entered).await;
                    handle
                        .send_event(if fail {
                            Event::Error("primary failure before first poll".into())
                        } else {
                            Event::BeginDrain
                        })
                        .await
                        .unwrap();
                    let result = finish(&handle).await;
                    assert_eq!(result.is_err(), fail, "semantic failure survives cleanup: {result:?}");
                    assert_eq!(counts.opens.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                    assert_eq!(counts.drains.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                    if fail {
                        assert!(matches!(
                            handle.current_state(),
                            State::Failed(reason) if reason == "primary failure before first poll"
                        ));
                    }
                    let evidence = journals.data.read_all_unordered().await.unwrap();
                    assert_eq!(
                        evidence.iter().filter(|row| matches!(
                            row.payload,
                            obzenflow_core::event::ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SourceCleanupFailed { .. })
                        )).count(),
                        usize::from(fail)
                    );
                }
            }

            #[tokio::test]
            async fn forced_abort_drops_acquired_reader_without_awaited_cleanup() {
                let fixture = Fixture::new(Opening::Ready, Reading::Pending);
                let counts = fixture.counts.clone();
                let (handle, _) = build(fixture, None, false).await;
                start(&handle).await;
                notified(&counts.polling).await;
                handle.abort_and_wait().await.unwrap();
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                assert_eq!(counts.drains.load(Ordering::SeqCst), 0);
            }

            #[tokio::test]
            async fn journal_failure_cannot_skip_or_repeat_reader_cleanup() {
                let mut fixture = Fixture::new(Opening::Ready, Reading::FailJournal);
                fixture.drain_fails = true;
                let counts = fixture.counts.clone();
                let (handle, _) = build(fixture, None, false).await;
                start(&handle).await;
                let failure = finish(&handle)
                    .await
                    .expect_err("broken journal cannot certify success");
                assert!(failure.to_string().contains("Journal is full"), "{failure}");
                assert_eq!(counts.opens.load(Ordering::SeqCst), 1);
                assert_eq!(counts.drains.load(Ordering::SeqCst), 1);
                assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
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
    UnifiedAsyncFiniteSourceHandler
);
lifecycle_family!(
    infinite_lifecycle,
    infinite::AsyncInfiniteSourceBuilder,
    infinite::InfiniteSourceConfig,
    infinite::InfiniteSourceEvent,
    infinite::InfiniteSourceState,
    UnifiedAsyncInfiniteSourceHandler
);

#[tokio::test]
async fn cleanup_failure_after_natural_exhaustion_is_secondary_evidence() {
    let mut fixture = Fixture::new(Opening::Ready, Reading::Eof);
    fixture.drain_fails = true;
    let counts = fixture.counts.clone();
    let (stage, resources, journals) = resources(&counts).await;
    let data = resources.data_journal.clone();
    let handle = finite::AsyncFiniteSourceBuilder::new(
        <Fixture as AdmitSource<dyn UnifiedAsyncFiniteSourceHandler, ConnectorSource>>::prepare(
            fixture,
        ),
        finite::FiniteSourceConfig::new(stage, "input", "lifecycle"),
        resources,
    )
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
        .unwrap()
        .unwrap();
    assert!(matches!(
        handle.current_state(),
        finite::FiniteSourceState::Drained
    ));
    assert_eq!(counts.polls.load(Ordering::SeqCst), 1);
    assert_eq!(counts.drains.load(Ordering::SeqCst), 1);
    let evidence = journals.data.read_all_unordered().await.unwrap();
    assert_eq!(
        evidence
            .iter()
            .filter(|row| matches!(
                row.payload,
                obzenflow_core::event::ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SourceCleanupFailed { .. })
            ))
            .count(),
        1
    );
    assert_eq!(
        data.read_all_unordered()
            .await
            .unwrap()
            .iter()
            .filter(|row| row.is_eof())
            .count(),
        1
    );
}

struct EmptyArchive(StageId, Option<(Arc<Notify>, Arc<Notify>)>);
#[async_trait]
impl ReplayArchive for EmptyArchive {
    async fn open_source_reader(
        &self,
        _: &str,
        _: obzenflow_core::event::context::StageType,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        if let Some((opening, release)) = &self.1 {
            opening.notify_one();
            release.notified().await;
        }
        Ok(TestJournal::<ChainEvent>::new(JournalOwner::stage(self.0))
            .reader()
            .await
            .unwrap())
    }
    async fn open_effect_history(
        &self,
        _: &str,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        unreachable!()
    }
    fn source_data_journal_path(&self, _: &str) -> Result<PathBuf, ReplayError> {
        Ok(PathBuf::from("memory"))
    }
    fn archive_flow_id(&self) -> &str {
        "lifecycle"
    }
    fn archived_stage_id(&self, _: &str) -> Result<StageId, ReplayError> {
        Ok(self.0)
    }
    fn archive_status(&self) -> ArchiveStatus {
        ArchiveStatus::Failed
    }
    fn status_derivation(&self) -> StatusDerivation {
        StatusDerivation {
            terminal_events_found: 1,
            chosen: ArchiveStatus::Failed,
            warning: None,
        }
    }
    fn allow_incomplete_archive(&self) -> bool {
        true
    }
    fn source_stage_keys(&self) -> Vec<String> {
        vec!["input".into()]
    }
    fn archive_path(&self) -> &Path {
        Path::new("memory")
    }
}
