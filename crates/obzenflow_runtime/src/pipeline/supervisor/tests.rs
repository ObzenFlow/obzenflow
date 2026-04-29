// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::bootstrap::{install_bootstrap_config, BootstrapConfig, StartupMode};
use crate::id_conversions::StageIdExt;
use crate::message_bus::FsmMessageBus;
use crate::messaging::SystemSubscription;
use crate::pipeline::fsm::PipelineContext;
use crate::stages::common::stage_handle::{BoxedStageHandle, StageError, StageEvent, StageHandle};
use crate::supervised_base::{
    ChannelBuilder, EventSender, SelfSupervisedExt, SelfSupervisedWithExternalEvents, StateWatcher,
};
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{JournalEvent, JournalWriterId, SystemEvent};
use obzenflow_core::id::{FlowId, JournalId, SystemId};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::EventEnvelope;
use obzenflow_topology::TopologyBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

struct MemoryJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
}

impl<T: JournalEvent> MemoryJournal<T> {
    fn with_owner(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
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

    async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
        let guard = self
            .events
            .lock()
            .expect("MemoryJournalReader: poisoned lock");
        let len = guard.len();
        drop(guard);
        let before = self.pos;
        self.pos = std::cmp::min(len, self.pos.saturating_add(n as usize));
        Ok((self.pos - before) as u64)
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
        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        let mut guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        guard.push(envelope.clone());
        Ok(envelope)
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.clone())
    }

    async fn read_causally_after(
        &self,
        after_event_id: &obzenflow_core::EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        if let Some(idx) = guard.iter().position(|e| e.event.id() == after_event_id) {
            return Ok(guard.iter().skip(idx + 1).cloned().collect());
        }
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        event_id: &obzenflow_core::EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.iter().find(|e| e.event.id() == event_id).cloned())
    }

    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader {
            events: Arc::clone(&self.events),
            pos: 0,
        }))
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
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

fn source_sink_topology_with_source() -> (Arc<obzenflow_topology::Topology>, StageId, StageId) {
    let mut builder = TopologyBuilder::new();
    let source = builder.add_stage(Some("source".to_string()));
    let sink = builder.add_stage(Some("sink".to_string()));
    (
        Arc::new(builder.build_unchecked().expect("source/sink topology")),
        StageId::from_topology_id(source),
        StageId::from_topology_id(sink),
    )
}

fn source_sink_topology() -> (Arc<obzenflow_topology::Topology>, StageId) {
    let (topology, _source, sink) = source_sink_topology_with_source();
    (topology, sink)
}

fn empty_topology() -> Arc<obzenflow_topology::Topology> {
    Arc::new(
        TopologyBuilder::new()
            .build_unchecked()
            .expect("empty topology"),
    )
}

async fn system_subscription_with(
    journal: &Arc<MemoryJournal<SystemEvent>>,
    events: impl IntoIterator<Item = SystemEvent>,
) -> SystemSubscription<SystemEvent> {
    for event in events {
        journal.append(event, None).await.expect("append event");
    }
    SystemSubscription::new(journal.reader().await.expect("reader"), "test".to_string())
}

async fn empty_system_subscription(
    journal: &Arc<MemoryJournal<SystemEvent>>,
) -> SystemSubscription<SystemEvent> {
    system_subscription_with(journal, std::iter::empty()).await
}

fn test_context(
    topology: Arc<obzenflow_topology::Topology>,
    system_id: SystemId,
    system_journal: Arc<MemoryJournal<SystemEvent>>,
    completion_subscription: Option<SystemSubscription<SystemEvent>>,
) -> PipelineContext {
    let system_journal: Arc<dyn Journal<SystemEvent>> = system_journal;
    PipelineContext {
        system_id,
        bus: Arc::new(FsmMessageBus::new()),
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
        metrics_handle: None,
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
    }
}

struct TestPipelineStageHandle {
    id: StageId,
    name: String,
    stage_type: StageType,
    start_gate: Option<StartGate>,
}

struct StartGate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    count: Arc<AtomicUsize>,
}

impl TestPipelineStageHandle {
    fn new(id: StageId, name: impl Into<String>, stage_type: StageType) -> BoxedStageHandle {
        Box::new(Self {
            id,
            name: name.into(),
            stage_type,
            start_gate: None,
        })
    }

    fn with_start_gate(
        id: StageId,
        name: impl Into<String>,
        stage_type: StageType,
        entered: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
        count: Arc<AtomicUsize>,
    ) -> BoxedStageHandle {
        Box::new(Self {
            id,
            name: name.into(),
            stage_type,
            start_gate: Some(StartGate {
                entered: Mutex::new(Some(entered)),
                release: tokio::sync::Mutex::new(Some(release)),
                count,
            }),
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
        Ok(())
    }

    async fn send_event(&self, _event: StageEvent) -> Result<(), StageError> {
        Ok(())
    }

    async fn begin_drain(&self) -> Result<(), StageError> {
        Ok(())
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn is_drained(&self) -> bool {
        false
    }

    async fn force_shutdown(&self) -> Result<(), StageError> {
        Ok(())
    }

    async fn wait_for_completion(&self) -> Result<(), StageError> {
        Ok(())
    }
}

fn test_supervisor(
    system_id: SystemId,
    system_journal: Arc<MemoryJournal<SystemEvent>>,
) -> PipelineSupervisor {
    let system_journal: Arc<dyn Journal<SystemEvent>> = system_journal;
    PipelineSupervisor {
        name: "test_pipeline_supervisor".to_string(),
        system_id,
        system_journal,
        last_barrier_log: None,
        last_manual_wait_log: None,
        drain_idle_iters: 0,
    }
}

async fn wait_for_state(
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

fn spawn_supervisor_loop(
    initial_state: PipelineState,
    supervisor: PipelineSupervisor,
    context: PipelineContext,
    receiver: crate::supervised_base::EventReceiver<PipelineEvent>,
    watcher: StateWatcher<PipelineState>,
) -> JoinHandle<Result<(), BoxError>> {
    tokio::spawn(async move {
        let supervisor = SelfSupervisedWithExternalEvents::new(supervisor, receiver, watcher);
        SelfSupervisedExt::run(supervisor, initial_state, context).await
    })
}

async fn stop_and_join(
    sender: &EventSender<PipelineEvent>,
    task: JoinHandle<Result<(), BoxError>>,
) {
    sender
        .send(PipelineEvent::StopRequested {
            mode: FlowStopMode::Cancel,
            reason: Some("test_stop".to_string()),
        })
        .await
        .expect("stop should send");

    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("supervisor should stop")
        .expect("supervisor task should join")
        .expect("supervisor should return ok");
}

#[test]
fn is_gating_edge_for_contract_behaves_as_expected() {
    // Non-source edges are always gating, regardless of mode.
    assert!(is_gating_edge_for_contract(
        false,
        SourceContractStrictMode::Abort
    ));
    assert!(is_gating_edge_for_contract(
        false,
        SourceContractStrictMode::Warn
    ));

    // Source edges are gating only when strict mode is Abort.
    assert!(is_gating_edge_for_contract(
        true,
        SourceContractStrictMode::Abort
    ));
    assert!(
        !is_gating_edge_for_contract(true, SourceContractStrictMode::Warn),
        "source edges should be non-gating when strict mode is Warn"
    );
}

#[tokio::test]
async fn manual_ready_for_run_publishes_state_and_waits_for_external_run() {
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink_stage_id) = source_sink_topology();
    let subscription = empty_system_subscription(&system_journal).await;
    let mut context = test_context(
        topology,
        system_id,
        system_journal.clone(),
        Some(subscription),
    );
    context.running_stages.insert(sink_stage_id);

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Materialized);
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal),
        context,
        receiver,
        watcher,
    );

    wait_for_state(&mut state_rx, "ReadyForRun", |state| {
        matches!(state, PipelineState::ReadyForRun)
    })
    .await;

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        matches!(*state_rx.borrow(), PipelineState::ReadyForRun),
        "manual startup should wait in ReadyForRun until Play/Run arrives"
    );

    stop_and_join(&sender, task).await;
}

#[tokio::test]
async fn auto_ready_for_run_emits_run_and_reaches_running() {
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink_stage_id) = source_sink_topology();
    let subscription = empty_system_subscription(&system_journal).await;
    let mut context = test_context(
        topology,
        system_id,
        system_journal.clone(),
        Some(subscription),
    );
    context.running_stages.insert(sink_stage_id);

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Materialized);
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal),
        context,
        receiver,
        watcher,
    );

    wait_for_state(&mut state_rx, "Running", |state| {
        matches!(state, PipelineState::Running)
    })
    .await;

    stop_and_join(&sender, task).await;
}

#[tokio::test]
async fn materializing_stage_count_mismatch_transitions_to_failed_without_panic() {
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink_stage_id) = source_sink_topology();
    let mut context = test_context(topology, system_id, system_journal.clone(), None);
    context.stage_supervisors.insert(
        sink_stage_id,
        TestPipelineStageHandle::new(sink_stage_id, "sink", StageType::Sink),
    );

    let (_sender, receiver, watcher) =
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Materializing);
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materializing,
        test_supervisor(system_id, system_journal),
        context,
        receiver,
        watcher,
    );

    wait_for_state(&mut state_rx, "Failed", |state| {
        matches!(
            state,
            PipelineState::Failed { reason, .. } if reason.contains("Stage count mismatch")
        )
    })
    .await;

    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .expect("supervisor should terminate after materialization failure")
        .expect("supervisor task should join")
        .expect("supervisor should return ok after failure transition");
}

#[tokio::test]
async fn materialized_to_ready_for_run_publishes_post_transition_state() {
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink_stage_id) = source_sink_topology();
    let subscription = empty_system_subscription(&system_journal).await;
    let mut context = test_context(
        topology,
        system_id,
        system_journal.clone(),
        Some(subscription),
    );
    context.running_stages.insert(sink_stage_id);

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Materialized);
    let watcher_for_assertion = watcher.clone();
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal),
        context,
        receiver,
        watcher,
    );

    wait_for_state(&mut state_rx, "ReadyForRun", |state| {
        matches!(state, PipelineState::ReadyForRun)
    })
    .await;

    assert!(
        matches!(watcher_for_assertion.current(), PipelineState::ReadyForRun),
        "observer state should publish ReadyForRun immediately after the readiness transition"
    );

    stop_and_join(&sender, task).await;
}

#[tokio::test]
async fn running_state_is_published_after_source_start_actions_complete() {
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, source_stage_id, sink_stage_id) = source_sink_topology_with_source();
    let subscription = empty_system_subscription(&system_journal).await;
    let mut context = test_context(
        topology,
        system_id,
        system_journal.clone(),
        Some(subscription),
    );
    context.running_stages.insert(sink_stage_id);
    context.stage_supervisors.insert(
        sink_stage_id,
        TestPipelineStageHandle::new(sink_stage_id, "sink", StageType::Sink),
    );

    let (entered_tx, entered_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let source_start_count = Arc::new(AtomicUsize::new(0));
    context.source_supervisors.insert(
        source_stage_id,
        TestPipelineStageHandle::with_start_gate(
            source_stage_id,
            "source",
            StageType::FiniteSource,
            entered_tx,
            release_rx,
            source_start_count.clone(),
        ),
    );

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::ReadyForRun);
    let watcher_for_assertion = watcher.clone();
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::ReadyForRun,
        test_supervisor(system_id, system_journal),
        context,
        receiver,
        watcher,
    );

    sender
        .send(PipelineEvent::Run)
        .await
        .expect("Run should send");
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .expect("source start action should begin")
        .expect("source start gate should be signalled");

    assert!(
        matches!(watcher_for_assertion.current(), PipelineState::ReadyForRun),
        "Running must not be published until NotifySourceStart completes"
    );

    release_tx
        .send(())
        .expect("source start action should still be waiting");
    wait_for_state(&mut state_rx, "Running", |state| {
        matches!(state, PipelineState::Running)
    })
    .await;
    assert_eq!(source_start_count.load(Ordering::Relaxed), 1);

    stop_and_join(&sender, task).await;
}

#[tokio::test]
async fn early_run_queued_in_materialized_is_consumed_before_ready_for_run() {
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink_stage_id) = source_sink_topology();
    let subscription = empty_system_subscription(&system_journal).await;
    let mut context = test_context(
        topology,
        system_id,
        system_journal.clone(),
        Some(subscription),
    );
    context.running_stages.insert(sink_stage_id);

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Materialized);
    sender
        .send(PipelineEvent::Run)
        .await
        .expect("early Run should queue");

    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal),
        context,
        receiver,
        watcher,
    );

    wait_for_state(&mut state_rx, "ReadyForRun", |state| {
        matches!(state, PipelineState::ReadyForRun)
    })
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    assert!(
        matches!(*state_rx.borrow(), PipelineState::ReadyForRun),
        "queued pre-ready Run must not be deferred and replayed after readiness"
    );

    stop_and_join(&sender, task).await;
}

#[tokio::test]
async fn materialized_without_non_source_stages_transitions_to_error() {
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let topology = empty_topology();
    let mut context = test_context(topology, system_id, system_journal.clone(), None);
    let mut supervisor = test_supervisor(system_id, system_journal);

    let directive = materialized::dispatch_materialized(&mut supervisor, &mut context)
        .await
        .expect("dispatch should succeed");

    assert!(matches!(
        directive,
        EventLoopDirective::Transition(PipelineEvent::Error { ref message })
            if message.contains("source-only")
    ));
}

#[tokio::test]
async fn materialized_stage_failed_or_cancelled_before_readiness_transitions_to_error() {
    let system_id = SystemId::new();

    for event in [
        SystemEvent::stage_failed(StageId::new(), "boom".to_string(), false),
        SystemEvent::stage_cancelled(StageId::new(), "cancelled".to_string()),
    ] {
        let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
        let (topology, _sink_stage_id) = source_sink_topology();
        let subscription = system_subscription_with(&system_journal, [event]).await;
        let mut context = test_context(
            topology,
            system_id,
            system_journal.clone(),
            Some(subscription),
        );
        let mut supervisor = test_supervisor(system_id, system_journal);

        let directive = materialized::dispatch_materialized(&mut supervisor, &mut context)
            .await
            .expect("dispatch should succeed");

        assert!(matches!(
            directive,
            EventLoopDirective::Transition(PipelineEvent::Error { .. })
        ));
    }
}

#[tokio::test]
async fn ready_for_run_stage_failure_transitions_to_error_before_run() {
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, _sink_stage_id) = source_sink_topology();
    let failed = SystemEvent::stage_failed(StageId::new(), "ready fault".to_string(), false);
    let subscription = system_subscription_with(&system_journal, [failed]).await;
    let mut context = test_context(
        topology,
        system_id,
        system_journal.clone(),
        Some(subscription),
    );
    let mut supervisor = test_supervisor(system_id, system_journal);

    let directive = ready_for_run::dispatch_ready_for_run(&mut supervisor, &mut context)
        .await
        .expect("dispatch should succeed");

    assert!(matches!(
        directive,
        EventLoopDirective::Transition(PipelineEvent::Error { ref message })
            if message.contains("ready fault")
    ));
}
