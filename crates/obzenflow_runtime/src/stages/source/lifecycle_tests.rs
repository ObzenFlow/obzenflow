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
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::archive::{
    ArchiveStatus, ReplayArchive, ReplayError, StatusDerivation,
};
use obzenflow_core::journal::{journal_owner::JournalOwner, Journal, JournalReader};
use obzenflow_core::{ChainEvent, FlowId, StageId, SystemId, TypedPayload};
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
            Opening::Fail => Err(SourceError::Transport("credential=DO_NOT_PERSIST".into())),
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
        counts.polls.fetch_add(1, Ordering::SeqCst);
        counts.polling.notify_one();
        match self.reading {
            Reading::Eof => Ok(None),
            Reading::Pending => std::future::pending().await,
            Reading::FailJournal => {
                counts.journal_failure.store(true, Ordering::SeqCst);
                Err(SourceError::Validation("primary read failure".into()))
            }
        }
    }

    async fn cleanup(&mut self) -> Result<(), SourceError> {
        self.resource.0.drains.fetch_add(1, Ordering::SeqCst);
        if self.drain_fails {
            Err(SourceError::Other(
                "secondary cleanup failure credential=DO_NOT_PERSIST".into(),
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

async fn resources(counts: &Counts) -> (StageId, StageResources, Arc<TestJournal<SystemEvent>>) {
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
    (
        stage,
        resources.take_stage_resources(stage).unwrap(),
        system,
    )
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
            ) -> (Handle, Arc<TestJournal<SystemEvent>>) {
                let (stage, mut resources, journal) = resources(&fixture.counts).await;
                if resume {
                    resources.runtime_execution = crate::execution::RuntimeExecution::new(
                        crate::execution::RuntimeMode::Resume,
                        Some(Arc::new(EmptyArchive(stage))),
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
                    journal,
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
            async fn partial_open_failure_is_terminal_and_source_attributed() {
                for resume in [false, true] {
                    let fixture = Fixture::new(Opening::Fail, Reading::Pending);
                    let counts = fixture.counts.clone();
                    let (handle, journal) = build(fixture, None, resume).await;
                    start(&handle).await;
                    finish(&handle).await.unwrap();
                    assert!(matches!(handle.current_state(), State::Failed(_)));
                    assert_eq!(counts.opens.load(Ordering::SeqCst), 1);
                    assert_eq!(counts.polls.load(Ordering::SeqCst), 0);
                    assert_eq!(counts.drains.load(Ordering::SeqCst), 0);
                    assert_eq!(counts.drops.load(Ordering::SeqCst), 1);
                    let evidence = format!("{:?}", journal.read_all_unordered().await.unwrap());
                    assert!(
                        evidence.contains(if resume {
                            "Cannot resume source 'input'"
                        } else {
                            "Cannot start source 'input'"
                        }),
                        "{evidence}"
                    );
                    assert!(!evidence.contains("DO_NOT_PERSIST"));
                }
            }

            #[tokio::test]
            async fn cancelling_partial_open_releases_resources_without_a_reader() {
                for abort in [false, true] {
                    let fixture = Fixture::new(Opening::Pending, Reading::Pending);
                    let counts = fixture.counts.clone();
                    let (handle, _) = build(fixture, None, false).await;
                    start(&handle).await;
                    notified(&counts.opening).await;
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
                    let (handle, journal) = build(
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
                    finish(&handle).await.unwrap();
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
                    let evidence = journal.read_all_unordered().await.unwrap();
                    assert!(!serde_json::to_string(&evidence).unwrap().contains("DO_NOT_PERSIST"));
                    assert_eq!(
                        evidence.iter().filter(|row| matches!(
                            row.payload,
                            obzenflow_core::event::SystemPayload::SourceCleanupFailed { .. }
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
    let (stage, resources, journal) = resources(&counts).await;
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
    let evidence = journal.read_all_unordered().await.unwrap();
    assert!(!serde_json::to_string(&evidence)
        .unwrap()
        .contains("DO_NOT_PERSIST"));
    assert_eq!(
        evidence
            .iter()
            .filter(|row| matches!(
                row.payload,
                obzenflow_core::event::SystemPayload::SourceCleanupFailed { .. }
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

struct EmptyArchive(StageId);
#[async_trait]
impl ReplayArchive for EmptyArchive {
    async fn open_source_reader(
        &self,
        _: &str,
        _: obzenflow_core::event::context::StageType,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
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
