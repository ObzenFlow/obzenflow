// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared pipeline contexts, controlled journals, stages and runner fixtures.

use crate::id_conversions::StageIdExt;
use crate::messaging::SystemSubscription;
use crate::pipeline::fsm::{PipelineContext, PipelineFsmEvent, PipelineFsmState};
use crate::pipeline::supervisor::PipelineSupervisor;
use crate::pipeline::{FlowStopMode, PipelineControl, PipelineState};
use crate::stages::common::stage_handle::{StageError, StageEvent, StageHandle};
use crate::supervised_base::{ChannelBuilder, EventSender, StateWatcher, SupervisorHandle};
use crate::testing::memory_journal::MemoryJournal as LiveJournal;
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{ChainEvent, JournalEvent, JournalWriterId, SystemEvent};
use obzenflow_core::id::{FlowId, JournalId, SystemId};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::MetricsSnapshotExporter;
use obzenflow_core::{EventEnvelope, StageId};
use obzenflow_topology::TopologyBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

pub(in crate::pipeline) struct MemoryJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    pub(in crate::pipeline) terminal_append: Option<Arc<TerminalAppendGate>>,
    pub(in crate::pipeline) metrics_ready_append: Option<Arc<TerminalAppendGate>>,
    pub(in crate::pipeline) fail_reader: Option<usize>,
    reader_calls: AtomicUsize,
}

impl<T: JournalEvent> MemoryJournal<T> {
    pub(in crate::pipeline) fn with_owner(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
            terminal_append: None,
            metrics_ready_append: None,
            fail_reader: None,
            reader_calls: AtomicUsize::new(0),
        }
    }
}

pub(in crate::pipeline) struct TerminalAppendGate {
    pub(in crate::pipeline) entered: tokio::sync::Notify,
    pub(in crate::pipeline) release: tokio::sync::Notify,
    pub(in crate::pipeline) fail: bool,
}

struct MemoryJournalReader<T: JournalEvent> {
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    pos: usize,
}

#[async_trait]
impl<T> JournalReader<T> for MemoryJournalReader<T>
where
    T: JournalEvent,
{
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self
            .events
            .lock()
            .expect("MemoryJournalReader: poisoned lock");
        if self.pos >= guard.len() {
            return Ok(None);
        }
        let envelope = guard[self.pos].clone();
        drop(guard);
        self.pos += 1;
        Ok(Some(envelope))
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }
}

#[async_trait]
impl<T> Journal<T> for MemoryJournal<T>
where
    T: JournalEvent + 'static,
{
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        if event.event_type_name() == "system.metrics.ready" {
            if let Some(gate) = &self.metrics_ready_append {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        if matches!(
            event.event_type_name(),
            "system.pipeline.completed" | "system.pipeline.cancelled" | "system.pipeline.failed"
        ) {
            if let Some(gate) = &self.terminal_append {
                gate.entered.notify_one();
                gate.release.notified().await;
                if gate.fail {
                    return Err(JournalError::Full);
                }
            }
        }
        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        let mut guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        guard.push(envelope.clone());
        Ok(envelope)
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.clone())
    }

    async fn read_event(
        &self,
        event_id: &obzenflow_core::EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.iter().find(|e| e.event.id() == event_id).cloned())
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        if self.fail_reader == Some(self.reader_calls.fetch_add(1, Ordering::Relaxed) + 1) {
            return Err(JournalError::Full);
        }
        Ok(Box::new(MemoryJournalReader {
            events: Arc::clone(&self.events),
            pos: position as usize,
        }))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        let len = guard.len();
        let start = len.saturating_sub(count);
        Ok(guard[start..].iter().rev().cloned().collect())
    }
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

pub(in crate::pipeline) fn source_sink_topology() -> (Arc<obzenflow_topology::Topology>, StageId) {
    let (topology, _source, sink) = source_sink_topology_with_source();
    (topology, sink)
}

pub(in crate::pipeline) fn empty_topology() -> Arc<obzenflow_topology::Topology> {
    Arc::new(
        TopologyBuilder::new()
            .build_unchecked()
            .expect("empty topology"),
    )
}

pub(in crate::pipeline) async fn system_subscription_with(
    journal: &Arc<MemoryJournal<SystemEvent>>,
    events: impl IntoIterator<Item = SystemEvent>,
) -> SystemSubscription<SystemEvent> {
    for event in events {
        journal.append(event, None).await.expect("append event");
    }
    SystemSubscription::new(journal.reader().await.expect("reader"), "test".to_string())
}

pub(in crate::pipeline) async fn empty_system_subscription(
    journal: &Arc<MemoryJournal<SystemEvent>>,
) -> SystemSubscription<SystemEvent> {
    system_subscription_with(journal, std::iter::empty()).await
}

pub(in crate::pipeline) fn test_context(
    topology: Arc<obzenflow_topology::Topology>,
    system_id: SystemId,
    system_journal: Arc<MemoryJournal<SystemEvent>>,
    completion_subscription: Option<SystemSubscription<SystemEvent>>,
) -> PipelineContext {
    let system_journal: Arc<dyn Journal<SystemEvent>> = system_journal;
    PipelineContext {
        system_id,
        topology,
        flow_name: "test_flow".to_string(),
        flow_id: FlowId::new(),
        system_journal,
        stage_supervisors: HashMap::new(),
        source_supervisors: HashMap::new(),
        completed_stages: Vec::new(),
        running_stages: HashSet::new(),
        completion_subscription,
        metrics_exporter: None,
        resources: Default::default(),
        progress: Default::default(),
        stage_data_journals: Vec::new(),
        stage_error_journals: Vec::new(),
        backpressure_registry: None,
        contract_status: HashMap::new(),
        contract_pairs: HashMap::new(),
        expected_contract_pairs: HashSet::new(),
        expected_sources: Vec::new(),
        stage_lifecycle_metrics: HashMap::new(),
        flow_start_time: None,
        last_system_event_id_seen: None,
        stop_intent: Default::default(),
        termination: Default::default(),
        source_contract_strict: Default::default(),
        metrics_drain_timeout_ms: 5_000,
    }
}

pub(in crate::pipeline) struct TestPipelineStageHandle {
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
            stall_drain: false,
            panic_on_start: false,
            id,
            name: name.into(),
            stage_type,
            start_gate: None,
            shutdown_probe: None,
        })
    }

    pub(in crate::pipeline) fn with_start_gate(
        id: StageId,
        name: impl Into<String>,
        stage_type: StageType,
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
        count: Arc<AtomicUsize>,
    ) -> Arc<dyn StageHandle> {
        Arc::new(Self {
            stall_drain: false,
            panic_on_start: false,
            id,
            name: name.into(),
            stage_type,
            start_gate: Some(StartGate {
                entered: Mutex::new(Some(entered)),
                release: tokio::sync::Mutex::new(Some(release)),
                count,
            }),
            shutdown_probe: None,
        })
    }

    pub(in crate::pipeline) fn with_stalled_completion(
        id: StageId,
        name: impl Into<String>,
        stage_type: StageType,
        shutdown_probe: ShutdownProbe,
    ) -> Arc<dyn StageHandle> {
        Arc::new(Self {
            stall_drain: false,
            panic_on_start: false,
            id,
            name: name.into(),
            stage_type,
            start_gate: None,
            shutdown_probe: Some(shutdown_probe),
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
        Ok(())
    }

    async fn ready(&self) -> Result<(), StageError> {
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
        Ok(())
    }

    async fn send_event(&self, _event: StageEvent) -> Result<(), StageError> {
        Ok(())
    }

    async fn begin_drain(&self) -> Result<(), StageError> {
        if self.stall_drain {
            std::future::pending::<()>().await;
        }
        Ok(())
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn is_drained(&self) -> bool {
        false
    }

    async fn force_shutdown(&self) -> Result<(), StageError> {
        if let Some(probe) = &self.shutdown_probe {
            probe.force_shutdown_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn wait_for_completion(&self) -> Result<(), StageError> {
        if let Some(probe) = &self.shutdown_probe {
            probe
                .wait_for_completion_count
                .fetch_add(1, Ordering::Relaxed);
            let notified = probe.completed_notify.notified();
            if !probe.completed.load(Ordering::Relaxed) {
                notified.await;
            }
            if probe.request_abort_count.load(Ordering::Relaxed) > 0 {
                return Err(StageError::Aborted);
            }
        }
        Ok(())
    }

    async fn abort_and_join(&self) -> Result<(), StageError> {
        if let Some(probe) = &self.shutdown_probe {
            probe.abort_and_join_count.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn request_abort(&self) {
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

pub(in crate::pipeline) fn test_supervisor(
    system_id: SystemId,
    _journal: Arc<MemoryJournal<SystemEvent>>,
) -> SystemId {
    system_id
}

pub(super) fn initial_fsm_state(state: &PipelineState) -> PipelineFsmState {
    match state {
        PipelineState::Created | PipelineState::Materializing => PipelineFsmState::Created,
        PipelineState::Materialized => PipelineFsmState::AwaitingStageReadiness,
        PipelineState::ReadyForRun => PipelineFsmState::ReadyForRun,
        PipelineState::Running => PipelineFsmState::Running,
        PipelineState::SourceCompleted => PipelineFsmState::SourceCompleted,
        PipelineState::Draining => PipelineFsmState::Draining,
        _ => PipelineFsmState::SettlingStages,
    }
}

pub(in crate::pipeline) async fn ready_stage(ctx: &mut PipelineContext, id: StageId) {
    ctx.stage_supervisors.insert(
        id,
        TestPipelineStageHandle::boxed(id, "sink", StageType::Sink),
    );
    ctx.system_journal
        .append(SystemEvent::stage_running(id), None)
        .await
        .unwrap();
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
    mut context: PipelineContext,
    receiver: crate::supervised_base::EventReceiver<PipelineFsmEvent>,
    watcher: StateWatcher<PipelineState>,
) -> JoinHandle<Result<(), BoxError>> {
    tokio::spawn(async move {
        if context.completion_subscription.is_none() {
            context.completion_subscription = Some(SystemSubscription::new(
                context.system_journal.reader().await?,
                "test_pipeline".into(),
            ));
        }
        context.expected_sources = context.source_supervisors.keys().copied().collect();
        if matches!(
            initial_state,
            PipelineState::Running | PipelineState::SourceCompleted | PipelineState::Draining
        ) {
            context
                .flow_start_time
                .get_or_insert_with(std::time::Instant::now);
        }
        let scope = context.resources.publications.clone();
        let supervisor = PipelineSupervisor::new(
            system_id,
            receiver,
            watcher.clone(),
            context.resources.failure.clone(),
        );
        let (sender, _receiver, _) =
            ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(initial_state.clone());
        let task = crate::supervised_base::SupervisorTaskBuilder::new("test_pipeline")
            .with_publications(scope)
            .spawn_self_supervised(supervisor, initial_fsm_state(&initial_state), context);
        let handle = crate::supervised_base::HandleBuilder::new()
            .with_event_sender(sender)
            .with_state_watcher(watcher)
            .with_supervisor_task(task)
            .build_standard()
            .unwrap();
        handle
            .wait_for_completion()
            .await
            .map_err(|error| Box::new(error) as BoxError)
    })
}

pub(in crate::pipeline) async fn stop_and_join(
    sender: &EventSender<PipelineFsmEvent>,
    task: JoinHandle<Result<(), BoxError>>,
) {
    sender
        .send(PipelineFsmEvent::from(PipelineControl::Stop {
            mode: FlowStopMode::Cancel,
        }))
        .await
        .expect("stop should send");

    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("supervisor should stop")
        .expect("supervisor task should join")
        .expect("supervisor should return ok");
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
        system_id,
        topology: make_topology(),
        flow_name: "test_flow".to_string(),
        flow_id: FlowId::new(),
        system_journal,
        stage_supervisors: HashMap::new(),
        source_supervisors: HashMap::new(),
        completed_stages: Vec::new(),
        running_stages: HashSet::new(),
        completion_subscription: None,
        metrics_exporter,
        resources: Default::default(),
        progress: Default::default(),
        stage_data_journals,
        stage_error_journals: Vec::new(),
        backpressure_registry: None,
        contract_status: HashMap::new(),
        contract_pairs: HashMap::new(),
        expected_contract_pairs: HashSet::new(),
        expected_sources: Vec::new(),
        stage_lifecycle_metrics: HashMap::new(),
        flow_start_time: None,
        last_system_event_id_seen: None,
        stop_intent: Default::default(),
        termination: Default::default(),
        source_contract_strict: Default::default(),
        metrics_drain_timeout_ms: 5_000,
    }
}

pub(in crate::pipeline) fn make_fsm_context() -> PipelineContext {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(LiveJournal::with_owner(JournalOwner::system(system_id)));
    make_context(system_id, system_journal, Vec::new(), None)
}
