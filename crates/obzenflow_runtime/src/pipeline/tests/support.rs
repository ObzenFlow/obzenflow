// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared pipeline contexts, controlled journals, stages and runner fixtures.

use crate::id_conversions::StageIdExt;
use crate::metrics::observations::ObservationRegistry;
use crate::pipeline::fsm::{PipelineContext, PipelineFsmEvent, PipelineFsmState};
use crate::pipeline::supervisor::PipelineSupervisor;
use crate::pipeline::PipelineState;
use crate::stages::common::stage_handle::{StageError, StageEvent, StageHandle};
use crate::supervised_base::{ChannelBuilder, StateWatcher, SupervisorHandle};
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{ChainEvent, JournalEvent, SystemEvent};
use obzenflow_core::id::{FlowId, JournalId, SystemId};
use obzenflow_core::journal::factory::FlowJournalFactory;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::MetricsSnapshotExporter;
use obzenflow_core::{JournalRecord, StageId};
use obzenflow_topology::TopologyBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Adds only the failure and pause points needed by lifecycle scenarios.
/// Storage, envelope construction and readers belong to the supplied journal.
pub(in crate::pipeline) struct ControlledJournal<T: JournalEvent> {
    inner: Arc<dyn Journal<T>>,
    pub(in crate::pipeline) gate_event: Option<fn(&T) -> bool>,
    pub(in crate::pipeline) gate: Option<Arc<TerminalAppendGate>>,
    pub(in crate::pipeline) fail_reader: Option<usize>,
    pub(in crate::pipeline) reader_calls: AtomicUsize,
}

impl<T: JournalEvent> ControlledJournal<T> {
    pub(in crate::pipeline) fn new(inner: Arc<dyn Journal<T>>) -> Self {
        Self {
            inner,
            fail_reader: None,
            gate_event: None,
            gate: None,
            reader_calls: AtomicUsize::new(0),
        }
    }
}

pub(in crate::pipeline) struct TerminalAppendGate {
    pub(in crate::pipeline) entered: tokio::sync::Notify,
    pub(in crate::pipeline) release: tokio::sync::Notify,
    pub(in crate::pipeline) fail: bool,
}

#[async_trait]
impl<T> obzenflow_core::journal::JournalStorage<T> for ControlledJournal<T>
where
    T: JournalEvent + 'static,
{
    fn storage_id(&self) -> &JournalId {
        self.inner.id()
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }

    async fn storage_append(
        &self,
        event: T,
        options: AppendOptions<T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        if self.gate_event.is_some_and(|matches| matches(&event)) {
            if let Some(gate) = &self.gate {
                gate.entered.notify_one();
                gate.release.notified().await;
                if gate.fail {
                    return Err(JournalError::Full);
                }
            }
        }
        self.inner.append(event, options).await
    }

    async fn storage_append_group(
        &self,
        group_id: &str,
        events: Vec<T>,
        options: AppendOptions<T>,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.inner.append_group(group_id, events, options).await
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.reader_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.read_all_unordered().await
    }

    async fn storage_read_event(
        &self,
        event_id: &obzenflow_core::EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        self.reader_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.read_event(event_id).await
    }

    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        if self.fail_reader == Some(self.reader_calls.fetch_add(1, Ordering::Relaxed) + 1) {
            return Err(JournalError::Full);
        }
        self.inner.reader_from(position).await
    }

    async fn storage_read_metrics_tail(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.reader_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.read_metrics_tail().await
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.reader_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.read_last_n(count).await
    }
}

pub(in crate::pipeline) fn new_system_journal(
    journals: &mut dyn FlowJournalFactory,
    system_id: SystemId,
) -> Arc<dyn Journal<SystemEvent>> {
    journals
        .create_system_journal(JournalName::System, JournalOwner::system(system_id))
        .expect("create system journal")
}

pub(in crate::pipeline) fn new_metrics_journals(
    journals: &mut dyn FlowJournalFactory,
) -> crate::metrics::builder::MetricsJournals {
    let system_id = SystemId::new();
    crate::metrics::builder::MetricsJournals {
        system_id,
        coordination: journals
            .create_system_journal(
                JournalName::MetricsCoordination,
                JournalOwner::system(system_id),
            )
            .unwrap(),
        export: journals
            .create_system_journal(JournalName::MetricsExport, JournalOwner::system(system_id))
            .unwrap(),
    }
}

pub(in crate::pipeline) fn new_stage_journal(
    journals: &mut dyn FlowJournalFactory,
    stage_id: StageId,
    name: &str,
) -> Arc<dyn Journal<ChainEvent>> {
    journals
        .create_chain_journal(
            JournalName::Stage {
                id: stage_id,
                stage_type: StageType::Transform,
                name: name.into(),
            },
            JournalOwner::stage(stage_id),
        )
        .expect("create stage journal")
}

pub(in crate::pipeline) fn source_sink_topology_with_source(
) -> (Arc<obzenflow_topology::Topology>, StageId, StageId) {
    let mut builder = TopologyBuilder::new();
    let source = builder.add_stage(Some("source".to_string()));
    let sink = builder.add_stage(Some("sink".to_string()));
    (
        Arc::new(builder.build_unchecked().expect("source/sink topology")),
        StageId::from_topology_id(source),
        StageId::from_topology_id(sink),
    )
}

pub(in crate::pipeline) fn test_context(
    topology: Arc<obzenflow_topology::Topology>,
    system_id: SystemId,
    system_journal: Arc<dyn Journal<SystemEvent>>,
) -> PipelineContext {
    PipelineContext {
        observations: Arc::new(ObservationRegistry::default()),
        runtime_execution: None,
        observation_export_interval: std::time::Duration::from_millis(250),
        system_id,
        topology,
        flow_name: "test_flow".to_string(),
        flow_id: FlowId::new(),
        system_journal: system_journal.clone(),
        stage_supervisors: HashMap::new(),
        source_supervisors: HashMap::new(),
        completed_stages: HashSet::new(),
        outstanding_milestones: HashSet::new(),
        outstanding_children: HashSet::new(),
        cleanup_deadline: None,
        metrics_deadline: None,
        metrics_journals: None,
        metrics_exporter: None,
        resources: Default::default(),
        stage_data_journals: Vec::new(),
        stage_error_journals: Vec::new(),
        backpressure_registry: None,
        stage_lifecycle_metrics: HashMap::new(),
        flow_start_time: None,
        stop_intent: Default::default(),
        termination: Default::default(),
        metrics_drain_timeout_ms: 5_000,
    }
}

#[derive(Clone, Default)]
pub(in crate::pipeline) struct StageResults {
    pub milestones: [bool; 3],
    pub failure: Option<crate::stages::common::stage_handle::StageFailure>,
    pub exit: Option<crate::stages::common::stage_lifecycle::LifecycleExit>,
}
#[derive(Clone)]
pub(in crate::pipeline) struct StageSignals(pub Arc<tokio::sync::watch::Sender<StageResults>>);
impl Default for StageSignals {
    fn default() -> Self {
        Self(Arc::new(
            tokio::sync::watch::channel(StageResults::default()).0,
        ))
    }
}
impl StageSignals {
    pub fn acknowledge(&self, milestone: crate::stages::common::stage_handle::StageMilestone) {
        self.0
            .send_modify(|result| result.milestones[milestone as usize] = true);
    }
    pub fn fail(&self, id: StageId, cause: &str) {
        self.0.send_modify(|result| {
            result
                .failure
                .get_or_insert(crate::stages::common::stage_handle::StageFailure {
                    stage_id: id,
                    cause: StageError::Other(cause.into()),
                    snapshot: Default::default(),
                });
        });
    }
    pub fn complete(&self) {
        self.0.send_modify(|result| result.exit = Some(crate::stages::common::stage_lifecycle::LifecycleExit::Completed(Default::default())));
    }
    fn cancel(&self) {
        use crate::stages::common::stage_lifecycle::{LifecycleExit, LifecycleFailure};
        self.0.send_modify(|result| {
            result.exit.get_or_insert_with(|| match &result.failure {
                Some(failure) => LifecycleExit::Failed(LifecycleFailure {
                    cause: failure.cause.clone(),
                    snapshot: failure.snapshot.clone(),
                }),
                None => LifecycleExit::Cancelled {
                    reason: "fixture cancellation".into(),
                    snapshot: Default::default(),
                },
            });
        });
    }
}

pub(in crate::pipeline) struct TestPipelineStageHandle {
    pub(in crate::pipeline) signals: StageSignals,
    pub(in crate::pipeline) acknowledge_commands: bool,

    pub(in crate::pipeline) stall_drain: bool,
    pub(in crate::pipeline) panic_on_start: bool,
    pub(in crate::pipeline) id: StageId,
    pub(in crate::pipeline) name: String,
    pub(in crate::pipeline) stage_type: StageType,
    pub(in crate::pipeline) start_gate: Option<StartGate>,
    pub(in crate::pipeline) shutdown_probe: Option<ShutdownProbe>,
}

pub(in crate::pipeline) struct StartGate {
    pub(in crate::pipeline) entered: Mutex<Option<oneshot::Sender<()>>>,
    pub(in crate::pipeline) release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    pub(in crate::pipeline) count: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
pub(in crate::pipeline) struct ShutdownProbe {
    pub(in crate::pipeline) completed: Arc<std::sync::atomic::AtomicBool>,
    pub(in crate::pipeline) completed_notify: Arc<tokio::sync::Notify>,
    pub(in crate::pipeline) request_abort_count: Arc<AtomicUsize>,
    pub(in crate::pipeline) force_shutdown_count: Arc<AtomicUsize>,
    pub(in crate::pipeline) wait_for_completion_count: Arc<AtomicUsize>,
    pub(in crate::pipeline) abort_and_join_count: Arc<AtomicUsize>,
}

impl TestPipelineStageHandle {
    pub(in crate::pipeline) fn boxed(
        id: StageId,
        name: impl Into<String>,
        stage_type: StageType,
    ) -> Arc<dyn StageHandle> {
        Arc::new(Self {
            signals: Default::default(),
            acknowledge_commands: true,
            stall_drain: false,
            panic_on_start: false,
            id,
            name: name.into(),
            stage_type,
            start_gate: None,
            shutdown_probe: None,
        })
    }
}

#[async_trait]
impl StageHandle for TestPipelineStageHandle {
    fn stage_id(&self) -> StageId {
        self.id
    }

    fn stage_name(&self) -> &str {
        &self.name
    }

    fn stage_type(&self) -> StageType {
        self.stage_type
    }

    async fn initialize(&self) -> Result<(), StageError> {
        if self.acknowledge_commands {
            self.signals
                .acknowledge(crate::stages::common::stage_handle::StageMilestone::Initialized);
        }
        Ok(())
    }

    async fn ready(&self) -> Result<(), StageError> {
        if self.acknowledge_commands {
            self.signals
                .acknowledge(crate::stages::common::stage_handle::StageMilestone::Ready);
        }
        if !matches!(
            self.stage_type,
            StageType::FiniteSource | StageType::InfiniteSource
        ) {
            self.start().await?;
        }
        Ok(())
    }

    async fn start(&self) -> Result<(), StageError> {
        if let Some(gate) = &self.start_gate {
            gate.count.fetch_add(1, Ordering::Relaxed);
            if let Some(entered) = gate
                .entered
                .lock()
                .expect("start gate lock poisoned")
                .take()
            {
                let _ = entered.send(());
            }
            if let Some(release) = gate.release.lock().await.take() {
                let _ = release.await;
            }
        }
        assert!(
            !self.panic_on_start,
            "pipeline command panic after metrics start"
        );
        if self.acknowledge_commands {
            self.signals
                .acknowledge(crate::stages::common::stage_handle::StageMilestone::Started);
        }
        Ok(())
    }

    async fn send_event(&self, _event: StageEvent) -> Result<(), StageError> {
        Ok(())
    }

    async fn begin_drain(&self) -> Result<(), StageError> {
        if self.stall_drain {
            std::future::pending::<()>().await;
        }
        self.signals.complete();
        Ok(())
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn is_drained(&self) -> bool {
        self.signals.0.borrow().exit.is_some()
    }

    async fn force_shutdown(&self) -> Result<(), StageError> {
        if let Some(probe) = &self.shutdown_probe {
            probe.force_shutdown_count.fetch_add(1, Ordering::Relaxed);
        } else {
            self.signals.cancel();
        }
        Ok(())
    }

    async fn wait_for_milestone(
        &self,
        milestone: crate::stages::common::stage_handle::StageMilestone,
    ) -> Result<crate::stages::common::stage_handle::StageAck, StageError> {
        let mut results = self.signals.0.subscribe();
        loop {
            let snapshot = results.borrow().clone();
            if snapshot.milestones[milestone as usize] {
                return Ok(crate::stages::common::stage_handle::StageAck {
                    stage_id: self.id,
                    milestone,
                    snapshot: Default::default(),
                });
            }
            if let Some(failure) = snapshot.failure {
                return Err(failure.cause);
            }
            if snapshot.exit.is_some() {
                return Err(StageError::Aborted);
            }
            results.changed().await.unwrap();
        }
    }
    async fn wait_for_failure(&self) -> Option<crate::stages::common::stage_handle::StageFailure> {
        let mut results = self.signals.0.subscribe();
        loop {
            let snapshot = results.borrow().clone();
            if snapshot.failure.is_some() {
                return snapshot.failure;
            }
            if snapshot.exit.is_some() {
                return None;
            }
            results.changed().await.unwrap();
        }
    }
    async fn wait_for_completion(&self) -> crate::stages::common::stage_handle::StageExit {
        let mut results = self.signals.0.subscribe();
        if let Some(probe) = &self.shutdown_probe {
            probe
                .wait_for_completion_count
                .fetch_add(1, Ordering::Relaxed);
        }
        let outcome = loop {
            if let Some(exit) = results.borrow().exit.clone() {
                break exit;
            }
            if let Some(probe) = &self.shutdown_probe {
                let notified = probe.completed_notify.notified();
                if probe.completed.load(Ordering::Relaxed) {
                    break crate::stages::common::stage_lifecycle::LifecycleExit::Completed(
                        Default::default(),
                    );
                }
                tokio::select! { _ = results.changed() => {}, _ = notified => {} }
            } else {
                results.changed().await.unwrap();
            }
        };
        crate::stages::common::stage_handle::StageExit {
            stage_id: self.id,
            outcome,
        }
    }

    async fn abort_and_join(&self) -> Result<(), StageError> {
        if let Some(probe) = &self.shutdown_probe {
            probe.abort_and_join_count.fetch_add(1, Ordering::Relaxed);
        }
        match &self.signals.0.borrow().failure {
            Some(failure) => Err(failure.cause.clone()),
            None => Ok(()),
        }
    }

    fn request_abort(&self) {
        self.signals.cancel();
        if let Some(probe) = &self.shutdown_probe {
            if probe
                .request_abort_count
                .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_err()
            {
                return;
            }
            probe.completed.store(true, Ordering::Relaxed);
            probe.completed_notify.notify_waiters();
        }
    }
}

pub(in crate::pipeline) async fn wait_for_state(
    rx: &mut tokio::sync::watch::Receiver<PipelineState>,
    label: &str,
    predicate: impl Fn(&PipelineState) -> bool,
) {
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            {
                let state = rx.borrow();
                if predicate(&state) {
                    return;
                }
            }
            rx.changed().await.expect("state channel should stay open");
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timeout waiting for {label}"));
}

pub(in crate::pipeline) fn spawn_supervisor_loop(
    initial_state: PipelineState,
    system_id: SystemId,
    context: PipelineContext,
    receiver: crate::supervised_base::EventReceiver<PipelineFsmEvent>,
    watcher: StateWatcher<PipelineState>,
) -> JoinHandle<Result<(), BoxError>> {
    assert_eq!(
        initial_state,
        PipelineState::Created,
        "lifecycle scenarios run through initialization"
    );
    let scope = context.resources.publications.clone();
    let supervisor = PipelineSupervisor::new(
        system_id,
        receiver,
        watcher.clone(),
        context.resources.failure.clone(),
    );
    let task = crate::supervised_base::SupervisorTaskBuilder::new("test_pipeline")
        .with_publications(scope)
        .spawn_self_supervised(supervisor, PipelineFsmState::Created, context);
    let (sender, _receiver, _) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(initial_state);
    let handle = crate::supervised_base::HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap();
    tokio::spawn(async move {
        handle
            .wait_for_completion()
            .await
            .map_err(|error| Box::new(error) as BoxError)
    })
}

pub(in crate::pipeline) struct DiscardSnapshots;
impl obzenflow_core::metrics::MetricsSnapshotExporter for DiscardSnapshots {
    fn publish_app_snapshot(&self, _: obzenflow_core::metrics::AppMetricsSnapshot) {}
    fn publish_infra_snapshot(&self, _: obzenflow_core::metrics::InfraMetricsSnapshot) {}
}

pub(in crate::pipeline) fn owned_test_stage(
    id: StageId,
    stage_type: StageType,
    probe: Option<ShutdownProbe>,
) -> TestPipelineStageHandle {
    TestPipelineStageHandle {
        id,
        name: "builder stage".into(),
        stage_type,
        signals: Default::default(),
        acknowledge_commands: true,
        stall_drain: false,
        panic_on_start: false,
        start_gate: None,
        shutdown_probe: probe,
    }
}

fn make_topology() -> Arc<obzenflow_topology::Topology> {
    let mut builder = TopologyBuilder::new();
    builder.add_stage(Some("stage1".to_string()));
    builder.add_stage(Some("stage2".to_string()));
    Arc::new(builder.build_unchecked().expect("build topology"))
}

pub(in crate::pipeline) fn make_context(
    system_id: SystemId,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    metrics_exporter: Option<Arc<dyn MetricsSnapshotExporter>>,
) -> PipelineContext {
    PipelineContext {
        observations: Arc::new(ObservationRegistry::default()),
        runtime_execution: None,
        observation_export_interval: std::time::Duration::from_millis(250),
        system_id,
        topology: make_topology(),
        flow_name: "test_flow".to_string(),
        flow_id: FlowId::new(),
        system_journal: system_journal.clone(),
        stage_supervisors: HashMap::new(),
        source_supervisors: HashMap::new(),
        completed_stages: HashSet::new(),
        outstanding_milestones: HashSet::new(),
        outstanding_children: HashSet::new(),
        cleanup_deadline: None,
        metrics_deadline: None,
        metrics_exporter,
        metrics_journals: None,
        resources: Default::default(),
        stage_data_journals,
        stage_error_journals: Vec::new(),
        backpressure_registry: None,
        stage_lifecycle_metrics: HashMap::new(),
        flow_start_time: None,
        stop_intent: Default::default(),
        termination: Default::default(),
        metrics_drain_timeout_ms: 5_000,
    }
}

pub(in crate::pipeline) fn make_fsm_context(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) -> PipelineContext {
    let mut journals = make_journals();
    let system_id = SystemId::new();
    let system_journal = new_system_journal(&mut *journals, system_id);
    let mut context = make_context(system_id, system_journal, Vec::new(), None);
    context.metrics_journals = Some(new_metrics_journals(&mut *journals));
    context
}
