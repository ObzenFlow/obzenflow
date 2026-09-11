// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::*;
use crate::bootstrap::{
    bootstrap_test_lock_async, install_bootstrap_config, BootstrapConfig, StartupMode,
};
use crate::feed_plan::{FeedKey, FeedRole};
use crate::id_conversions::StageIdExt;
use crate::messaging::SystemSubscription;
use crate::pipeline::fsm::PipelineContext;
use crate::stages::common::stage_handle::{StageError, StageEvent, StageHandle};
use crate::supervised_base::{ChannelBuilder, EventSender, StateWatcher};
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{JournalEvent, JournalWriterId, SystemEvent};
use obzenflow_core::id::{FlowId, JournalId, SystemId};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::{EventEnvelope, StageId};
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
    terminal_append: Option<Arc<TerminalAppendGate>>,
    metrics_ready_append: Option<Arc<TerminalAppendGate>>,
    fail_reader: Option<usize>,
    reader_calls: AtomicUsize,
}

impl<T: JournalEvent> MemoryJournal<T> {
    fn with_owner(owner: JournalOwner) -> Self {
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

struct TerminalAppendGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    fail: bool,
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

#[tokio::test]
async fn dropping_pipeline_context_cancels_its_metrics_supervisor() {
    use crate::metrics::fsm::{MetricsAggregatorEvent, MetricsAggregatorState};
    use crate::supervised_base::HandleBuilder;

    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, _, _) = source_sink_topology_with_source();
    let context = test_context(topology, system_id, journal, None);
    let (sender, _receiver, watcher) =
        ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
            .build(MetricsAggregatorState::Running);
    let (started_tx, started_rx) = oneshot::channel();
    let (terminated_tx, terminated_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _termination = terminated_tx;
        started_tx.send(()).unwrap();
        std::future::pending::<Result<(), Box<dyn std::error::Error + Send + Sync>>>().await
    });
    context.resources.metrics.install_for_test(
        HandleBuilder::new()
            .with_event_sender(sender)
            .with_state_watcher(watcher)
            .with_supervisor_task(task)
            .build_standard()
            .unwrap(),
    );
    started_rx.await.unwrap();
    drop(context);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), terminated_rx)
            .await
            .expect("metrics task must be cancelled when its pipeline disappears")
            .is_err()
    );
}

#[test]
fn contract_keys_for_stage_pair_returns_all_matching_logical_feeds() {
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, upstream, downstream) = source_sink_topology_with_source();
    let first_key = FeedKey::new(upstream, downstream, "test.first", FeedRole::Reference);
    let second_key = FeedKey::new(upstream, downstream, "test.second", FeedRole::Stream);
    let mut context = test_context(topology, system_id, system_journal, None);
    context.expected_contract_pairs.insert(first_key.clone());
    context.expected_contract_pairs.insert(second_key.clone());

    let keys = context.contract_keys_for_stage_pair(upstream, downstream);

    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&first_key));
    assert!(keys.contains(&second_key));
}

#[test]
fn contract_keys_for_contract_event_returns_matching_logical_feed() {
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, upstream, downstream) = source_sink_topology_with_source();
    let first_key = FeedKey::new(upstream, downstream, "test.first", FeedRole::Reference);
    let second_key = FeedKey::new(upstream, downstream, "test.second", FeedRole::Stream);
    let mut context = test_context(topology, system_id, system_journal, None);
    context.expected_contract_pairs.insert(first_key.clone());
    context.expected_contract_pairs.insert(second_key.clone());

    let keys = context.contract_keys_for_contract_event(
        upstream,
        downstream,
        Some("test.first"),
        Some("reference"),
    );

    assert_eq!(keys, vec![first_key]);
}

#[test]
fn contract_keys_for_stage_pair_falls_back_for_legacy_stage_pair_status() {
    let system_id = SystemId::new();
    let system_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, upstream, downstream) = source_sink_topology_with_source();
    let context = test_context(topology, system_id, system_journal, None);

    assert_eq!(
        context.contract_keys_for_stage_pair(upstream, downstream),
        vec![FeedKey::legacy_stage_pair(upstream, downstream)]
    );
}

struct TestPipelineStageHandle {
    stall_drain: bool,
    panic_on_start: bool,
    id: StageId,
    name: String,
    stage_type: StageType,
    start_gate: Option<StartGate>,
    shutdown_probe: Option<ShutdownProbe>,
}

struct StartGate {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: tokio::sync::Mutex<Option<oneshot::Receiver<()>>>,
    count: Arc<AtomicUsize>,
}

#[derive(Clone, Default)]
struct ShutdownProbe {
    completed: Arc<std::sync::atomic::AtomicBool>,
    completed_notify: Arc<tokio::sync::Notify>,
    request_abort_count: Arc<AtomicUsize>,
    force_shutdown_count: Arc<AtomicUsize>,
    wait_for_completion_count: Arc<AtomicUsize>,
    abort_and_join_count: Arc<AtomicUsize>,
}

impl TestPipelineStageHandle {
    fn boxed(id: StageId, name: impl Into<String>, stage_type: StageType) -> Arc<dyn StageHandle> {
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

    fn with_start_gate(
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

    fn with_stalled_completion(
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

fn test_supervisor(system_id: SystemId, _journal: Arc<MemoryJournal<SystemEvent>>) -> SystemId {
    system_id
}

fn initial_fsm_state(state: &PipelineState) -> PipelineFsmState {
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

async fn ready_stage(ctx: &mut PipelineContext, id: StageId) {
    ctx.stage_supervisors.insert(
        id,
        TestPipelineStageHandle::boxed(id, "sink", StageType::Sink),
    );
    ctx.system_journal
        .append(SystemEvent::stage_running(id), None)
        .await
        .unwrap();
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

async fn stop_and_join(
    sender: &EventSender<PipelineFsmEvent>,
    task: JoinHandle<Result<(), BoxError>>,
) {
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Stop {
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

#[tokio::test]
async fn graceful_deadline_bounds_a_stalled_source_control_send() {
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, _) = source_sink_topology();
    let subscription = empty_system_subscription(&journal).await;
    let mut context = test_context(topology, system_id, journal.clone(), Some(subscription));
    let stage_id = StageId::new();
    context.source_supervisors.insert(
        stage_id,
        Arc::new(TestPipelineStageHandle {
            id: stage_id,
            name: "stalled_source_control".into(),
            stage_type: StageType::FiniteSource,
            start_gate: None,
            shutdown_probe: None,
            stall_drain: true,
            panic_on_start: false,
        }),
    );
    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Running);
    let task = spawn_supervisor_loop(
        PipelineState::Running,
        test_supervisor(system_id, journal.clone()),
        context,
        receiver,
        watcher,
    );
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Stop {
            mode: FlowStopMode::Graceful {
                timeout: std::time::Duration::from_millis(20),
            },
        }))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_millis(500), task)
        .await
        .expect("a full source control queue cannot hold the pipeline beyond its graceful deadline")
        .unwrap()
        .unwrap();
    let facts = journal.read_all_unordered().await.unwrap();
    let admissions: Vec<_> = facts
        .iter()
        .filter_map(|envelope| match &envelope.event.event {
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::StopAdmitted { admission },
            ) => Some(admission.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        admissions,
        [
            obzenflow_core::event::PipelineStopAdmission::Graceful {
                timeout_ms: obzenflow_core::event::types::DurationMs(20)
            },
            obzenflow_core::event::PipelineStopAdmission::Cancel {
                cause: obzenflow_core::event::PipelineCancellationCause::GracefulTimeout
            },
        ]
    );
}

#[tokio::test]
async fn expired_graceful_stop_aborts_and_joins_stalled_stage_without_fresh_cleanup_budget() {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        // A timeout escalation must not fall back to this much longer budget.
        shutdown_timeout: std::time::Duration::from_secs(5),
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
    let shutdown_probe = ShutdownProbe::default();
    context.stage_supervisors.insert(
        sink_stage_id,
        TestPipelineStageHandle::with_stalled_completion(
            sink_stage_id,
            "stalled_sink",
            StageType::Sink,
            shutdown_probe.clone(),
        ),
    );
    context.stop_intent.apply_request(
        FlowStopMode::Graceful {
            timeout: std::time::Duration::from_millis(20),
        },
        Some("test_graceful_stop".to_string()),
    );

    let (_sender, receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Draining);
    let started = std::time::Instant::now();
    let task = spawn_supervisor_loop(
        PipelineState::Draining,
        test_supervisor(system_id, system_journal.clone()),
        context,
        receiver,
        watcher,
    );

    tokio::time::timeout(std::time::Duration::from_millis(500), task)
        .await
        .expect("expired graceful stop must not start the five-second cleanup budget")
        .expect("pipeline supervisor task should join")
        .expect("timeout cleanup should complete without an action error");

    assert!(
        started.elapsed() < std::time::Duration::from_millis(500),
        "cleanup must remain bounded by the original graceful-stop deadline"
    );
    assert_eq!(
        shutdown_probe.force_shutdown_count.load(Ordering::Relaxed),
        0,
        "an overdue supervisor should bypass cooperative force-shutdown"
    );
    assert_eq!(
        shutdown_probe
            .wait_for_completion_count
            .load(Ordering::Relaxed),
        1,
        "the existing typed completion must still be observed after abort"
    );
    assert_eq!(
        shutdown_probe.request_abort_count.load(Ordering::Relaxed),
        1,
        "the overdue supervisor receives one synchronous abort before joining"
    );
}

#[tokio::test]
async fn manual_ready_for_run_publishes_state_and_waits_for_external_run() {
    let _lock = bootstrap_test_lock_async().await;
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
    ready_stage(&mut context, sink_stage_id).await;

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Materialized);
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal.clone()),
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
    let _lock = bootstrap_test_lock_async().await;
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
    ready_stage(&mut context, sink_stage_id).await;

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Materialized);
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal.clone()),
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
        TestPipelineStageHandle::boxed(sink_stage_id, "sink", StageType::Sink),
    );

    let (_sender, receiver, watcher) = ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
        .build(PipelineState::Materializing);
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materializing,
        test_supervisor(system_id, system_journal.clone()),
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
    let _lock = bootstrap_test_lock_async().await;
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
    ready_stage(&mut context, sink_stage_id).await;

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Materialized);
    let watcher_for_assertion = watcher.clone();
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal.clone()),
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
async fn running_state_requires_committed_source_running_after_start() {
    let _lock = bootstrap_test_lock_async().await;
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
    ready_stage(&mut context, sink_stage_id).await;
    context.stage_supervisors.insert(
        sink_stage_id,
        TestPipelineStageHandle::boxed(sink_stage_id, "sink", StageType::Sink),
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
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::ReadyForRun);
    let watcher_for_assertion = watcher.clone();
    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::ReadyForRun,
        test_supervisor(system_id, system_journal.clone()),
        context,
        receiver,
        watcher,
    );

    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Start))
        .await
        .expect("Run should send");
    tokio::time::timeout(std::time::Duration::from_secs(2), entered_rx)
        .await
        .expect("source start action should begin")
        .expect("source start gate should be signalled");

    assert!(
        matches!(watcher_for_assertion.current(), PipelineState::ReadyForRun),
        "a pending source command is not running evidence"
    );

    release_tx
        .send(())
        .expect("source start action should still be waiting");
    system_journal
        .append(SystemEvent::stage_running(source_stage_id), None)
        .await
        .unwrap();
    wait_for_state(&mut state_rx, "Running", |state| {
        matches!(state, PipelineState::Running)
    })
    .await;
    assert_eq!(source_start_count.load(Ordering::Relaxed), 1);

    stop_and_join(&sender, task).await;
}

#[tokio::test]
async fn early_run_queued_in_materialized_is_consumed_before_ready_for_run() {
    let _lock = bootstrap_test_lock_async().await;
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
    ready_stage(&mut context, sink_stage_id).await;

    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Materialized);
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Start))
        .await
        .expect("early Run should queue");

    let mut state_rx = watcher.subscribe();
    let task = spawn_supervisor_loop(
        PipelineState::Materialized,
        test_supervisor(system_id, system_journal.clone()),
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
async fn empty_topology_fails_through_the_canonical_fsm() {
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let mut context = test_context(empty_topology(), system_id, journal, None);
    let mut machine =
        crate::pipeline::fsm::build_pipeline_fsm_with_initial(PipelineFsmState::Created);
    machine
        .handle(PipelineFsmEvent::Bootstrap, &mut context)
        .await
        .unwrap();
    assert!(matches!(machine.state(), PipelineFsmState::SettlingStages));
    assert!(context
        .termination
        .failure
        .as_ref()
        .unwrap()
        .reason
        .contains("Stage count mismatch"));
}

#[tokio::test]
async fn stage_failures_and_cancellations_before_readiness_use_journal_evidence() {
    for state in [
        PipelineFsmState::AwaitingStageReadiness,
        PipelineFsmState::ReadyForRun,
    ] {
        for cancelled in [false, true] {
            let system_id = SystemId::new();
            let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
            let (topology, sink) = source_sink_topology();
            let event = if cancelled {
                SystemEvent::stage_cancelled(sink, "cancelled".into())
            } else {
                SystemEvent::stage_failed(sink, "ready fault".into(), false)
            };
            let envelope = journal.append(event, None).await.unwrap();
            let mut context = test_context(topology, system_id, journal, None);
            let mut machine = crate::pipeline::fsm::build_pipeline_fsm_with_initial(state.clone());
            machine
                .handle(PipelineFsmEvent::Journal(Box::new(envelope)), &mut context)
                .await
                .unwrap();
            assert!(matches!(machine.state(), PipelineFsmState::SettlingStages));
            assert!(context.termination.failure.is_some());
        }
    }
}

#[tokio::test]
async fn materialisation_reconsiders_readiness_facts_already_consumed() {
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink) = source_sink_topology();
    let mut context = test_context(topology, system_id, journal.clone(), None);
    context.stage_supervisors.insert(
        sink,
        TestPipelineStageHandle::boxed(sink, "sink", StageType::Sink),
    );
    let mut machine =
        crate::pipeline::fsm::build_pipeline_fsm_with_initial(PipelineFsmState::Materializing);
    let envelope = journal
        .append(SystemEvent::stage_running(sink), None)
        .await
        .unwrap();
    assert!(machine
        .handle(PipelineFsmEvent::Journal(Box::new(envelope)), &mut context)
        .await
        .unwrap()
        .is_empty());
    let actions = machine
        .handle(PipelineFsmEvent::PhysicalSettlementSatisfied, &mut context)
        .await
        .unwrap();
    assert!(matches!(
        machine.state(),
        PipelineFsmState::AwaitingStageReadiness
    ));
    let readiness = actions
        .into_iter()
        .find_map(|action| match action {
            PipelineAction::Publish { event, .. } => Some(*event),
            _ => None,
        })
        .expect("previously consumed Running fact must authorise readiness publication");
    let envelope = journal.append(readiness, None).await.unwrap();
    machine
        .handle(PipelineFsmEvent::Journal(Box::new(envelope)), &mut context)
        .await
        .unwrap();
    assert!(matches!(machine.state(), PipelineFsmState::ReadyForRun));
}

#[tokio::test]
async fn persistent_controls_cannot_starve_command_delivery_or_stage_joins() {
    use obzenflow_fsm::FsmAction;
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, journal, None);
    ctx.source_supervisors.insert(
        source,
        TestPipelineStageHandle::boxed(source, "source", StageType::FiniteSource),
    );
    ctx.stage_supervisors.insert(
        sink,
        TestPipelineStageHandle::boxed(sink, "sink", StageType::Sink),
    );
    PipelineAction::StartSources
        .execute(&mut ctx)
        .await
        .unwrap();
    PipelineAction::ObserveStages
        .execute(&mut ctx)
        .await
        .unwrap();
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Running);
    for _ in 0..32 {
        sender
            .send(PipelineFsmEvent::Control(PipelineControl::Start))
            .await
            .unwrap();
    }
    let mut supervisor =
        PipelineSupervisor::new(system_id, receiver, watcher, ctx.resources.failure.clone());
    let mut controls_observed = 0;
    for _ in 0..16 {
        if ctx.resources.delivery.is_empty() && ctx.resources.stages_joined {
            break;
        }
        if matches!(
            supervisor
                .dispatch_state(&PipelineFsmState::Running, &mut ctx)
                .await
                .unwrap(),
            EventLoopDirective::Transition(PipelineFsmEvent::Control(_))
        ) {
            controls_observed += 1;
        }
    }
    assert!(
        ctx.resources.delivery.is_empty(),
        "authorised commands need bounded service"
    );
    assert!(
        ctx.resources.stages_joined,
        "every stage join needs bounded service"
    );
    assert!(
        controls_observed > 0 && controls_observed < 32,
        "controls must share dispatch with owned work"
    );
}

#[tokio::test]
async fn completed_action_failure_gateway_does_not_report_the_original_error_again() {
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, _) = source_sink_topology();
    let mut ctx = test_context(topology, system_id, journal, None);
    ctx.resources
        .retain_failure(Box::new(std::io::Error::other("handoff failed")));
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Draining);
    let mut supervisor =
        PipelineSupervisor::new(system_id, receiver, watcher, ctx.resources.failure.clone());
    // This hook follows successful execution of the shared runner's failure
    // actions. Dispatch must retain the error for completion without routing it
    // through that gateway a second time.
    supervisor
        .after_transition(&PipelineFsmState::SettlingStages, &ctx)
        .await
        .unwrap();
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Start))
        .await
        .unwrap();
    assert!(matches!(
        supervisor
            .dispatch_state(&PipelineFsmState::SettlingStages, &mut ctx)
            .await
            .unwrap(),
        EventLoopDirective::Transition(PipelineFsmEvent::Control(_))
    ));
    assert_eq!(
        ctx.resources.failure.get().unwrap().to_string(),
        "handoff failed"
    );
}

#[tokio::test]
async fn abort_publication_rejection_cannot_skip_siblings_or_resume_commands() {
    use obzenflow_fsm::FsmAction;
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut context = test_context(topology, system_id, journal, None);
    let probes = [ShutdownProbe::default(), ShutdownProbe::default()];
    let handles: Vec<Arc<dyn StageHandle>> = vec![
        Arc::new(owned_test_stage(
            source,
            StageType::FiniteSource,
            Some(probes[0].clone()),
        )),
        Arc::new(owned_test_stage(
            sink,
            StageType::Sink,
            Some(probes[1].clone()),
        )),
    ];
    context
        .source_supervisors
        .insert(source, handles[0].clone());
    context.stage_supervisors.insert(sink, handles[1].clone());
    for id in [source, sink] {
        context.stage_data_journals.push((
            id,
            Arc::new(MemoryJournal::with_owner(JournalOwner::stage(id))),
        ));
    }
    context
        .resources
        .delivery
        .enqueue(
            handles,
            &[crate::pipeline::resources::StageCommand::Start],
            2,
        )
        .unwrap();
    context.progress.abort_cause =
        Some((ViolationCause::Other("contract fault".into()), Some(source)));
    // These fixtures reject the control-publication capability. Every child
    // must still be aborted before the original admission error is returned.
    assert!(PipelineAction::CancelStages {
        contract_abort: true
    }
    .execute(&mut context)
    .await
    .is_err());
    assert!(context.resources.delivery.is_empty());
    for probe in probes {
        assert_eq!(probe.request_abort_count.load(Ordering::Relaxed), 1);
    }
    let original = context.resources.failure.get().unwrap().to_string();
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Draining);
    let mut supervisor = PipelineSupervisor::new(
        system_id,
        receiver,
        watcher,
        context.resources.failure.clone(),
    );
    let state = PipelineFsmState::SettlingStages;
    let first = supervisor
        .dispatch_state(&state, &mut context)
        .await
        .unwrap();
    assert!(
        matches!(first, EventLoopDirective::Transition(PipelineFsmEvent::OperationalFailure { message }) if message == original)
    );
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Start))
        .await
        .unwrap();
    assert!(matches!(
        supervisor
            .dispatch_state(&state, &mut context)
            .await
            .unwrap(),
        EventLoopDirective::Transition(PipelineFsmEvent::Control(_))
    ));
    assert_eq!(
        context.resources.failure.get().unwrap().to_string(),
        original
    );
}

#[tokio::test]
async fn terminal_publication_retains_its_outcome_while_servicing_graceful_expiry() {
    use obzenflow_core::event::{
        PipelineCancellationCause, PipelineLifecycleEvent, PipelineStopAdmission, SystemEventType,
    };
    let system_id = SystemId::new();
    let gate = Arc::new(TerminalAppendGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fail: false,
    });
    let mut journal = MemoryJournal::with_owner(JournalOwner::system(system_id));
    journal.terminal_append = Some(gate.clone());
    let journal = Arc::new(journal);
    let mut context = test_context(empty_topology(), system_id, journal.clone(), None);
    context.flow_start_time = Some(std::time::Instant::now());
    let probe = ShutdownProbe::default();
    probe.completed.store(true, Ordering::Relaxed);
    let stage = StageId::new();
    context.stage_supervisors.insert(
        stage,
        TestPipelineStageHandle::with_stalled_completion(
            stage,
            "already settled",
            StageType::Sink,
            probe.clone(),
        ),
    );
    // The fixture completes ordinary cleanup before the terminal write. Its
    // abort probe then observes whether Runtime services the new deadline.
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Draining);
    journal
        .append(
            obzenflow_core::event::SystemEventFactory::new(system_id)
                .pipeline_all_stages_completed(),
            None,
        )
        .await
        .unwrap();
    let task = spawn_supervisor_loop(
        PipelineState::Draining,
        test_supervisor(system_id, journal.clone()),
        context,
        receiver,
        watcher,
    );
    tokio::time::timeout(std::time::Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Stop {
            mode: FlowStopMode::Graceful {
                timeout: std::time::Duration::ZERO,
            },
        }))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while probe.request_abort_count.load(Ordering::Relaxed) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("terminal publication must not suspend graceful expiry");
    assert!(!task.is_finished());
    gate.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let events = journal.read_all_unordered().await.unwrap();
    let facts: Vec<_> = events
        .iter()
        .filter_map(|row| match &row.event.event {
            SystemEventType::PipelineLifecycle(event) => Some(event),
            _ => None,
        })
        .collect();
    assert!(facts
        .iter()
        .any(|fact| matches!(fact, PipelineLifecycleEvent::Completed { .. })));
    assert_eq!(
        facts
            .iter()
            .filter(|event| matches!(
                event,
                PipelineLifecycleEvent::Completed { .. }
                    | PipelineLifecycleEvent::Cancelled { .. }
                    | PipelineLifecycleEvent::Failed { .. }
                    | PipelineLifecycleEvent::NotStarted
            ))
            .count(),
        1
    );
    assert!(facts.iter().any(|event| matches!(
        event,
        PipelineLifecycleEvent::StopAdmitted {
            admission: PipelineStopAdmission::Cancel {
                cause: PipelineCancellationCause::GracefulTimeout
            }
        }
    )));
}

#[tokio::test]
async fn supervisor_join_waits_for_terminal_publication_and_propagates_append_failure() {
    for terminal in ["completed", "cancelled", "failed"] {
        for fail in [false, true] {
            let system_id = SystemId::new();
            let gate = Arc::new(TerminalAppendGate {
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
                fail,
            });
            let mut journal = MemoryJournal::with_owner(JournalOwner::system(system_id));
            journal.terminal_append = Some(gate.clone());
            let journal = Arc::new(journal);
            let mut context = test_context(empty_topology(), system_id, journal.clone(), None);
            context.flow_start_time = Some(std::time::Instant::now());
            if terminal == "cancelled" {
                context
                    .stop_intent
                    .apply_request(FlowStopMode::Cancel, Some("test_stop".into()));
            }
            let published = context.termination.published.clone();
            let state = PipelineState::Draining;
            let (sender, receiver, watcher) =
                ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(state.clone());
            let event = if terminal == "failed" {
                PipelineFsmEvent::OperationalFailure {
                    message: "test_failure".into(),
                }
            } else {
                journal
                    .append(
                        obzenflow_core::event::SystemEventFactory::new(system_id)
                            .pipeline_all_stages_completed(),
                        None,
                    )
                    .await
                    .unwrap();
                PipelineFsmEvent::Control(PipelineControl::Start)
            };
            sender.send(event).await.unwrap();
            let task = spawn_supervisor_loop(
                state,
                test_supervisor(system_id, journal.clone()),
                context,
                receiver,
                watcher,
            );
            tokio::time::timeout(std::time::Duration::from_secs(2), gate.entered.notified())
                .await
                .unwrap();
            assert!(
                !task.is_finished(),
                "terminal state alone must not complete the supervisor join"
            );
            assert!(
                published.get().is_none(),
                "a blocked append is not published"
            );
            let event_type = format!("system.pipeline.{terminal}");
            assert!(!journal
                .read_all_unordered()
                .await
                .unwrap()
                .iter()
                .any(|event| event.event.event_type_name() == event_type));
            gate.release.notify_one();
            let result = tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                result.is_err(),
                fail,
                "{terminal} publication error must reach the joining caller"
            );
            assert_eq!(published.get().is_some(), !fail);
            let events = journal.read_all_unordered().await.unwrap();
            if let Some(retained) = published.get() {
                assert!(events
                    .iter()
                    .any(|event| Some(event.event.id) == retained.event_id));
            }
            assert_eq!(
                events
                    .iter()
                    .filter(|event| event.event.event_type_name() == event_type)
                    .count(),
                usize::from(!fail)
            );
        }
    }
}

#[tokio::test]
async fn unexpected_errors_preserve_failed_outcomes_before_and_during_stop() {
    use crate::pipeline::termination::ExecutionOutcome;
    use obzenflow_fsm::FsmAction;
    for (state, stopping) in [
        (PipelineState::Materializing, false),
        (PipelineState::Materialized, false),
        (PipelineState::ReadyForRun, false),
        (PipelineState::Running, false),
        (PipelineState::SourceCompleted, false),
        (PipelineState::Draining, false),
        (PipelineState::Draining, true),
    ] {
        let system_id = SystemId::new();
        let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
        let mut context = test_context(empty_topology(), system_id, journal.clone(), None);
        context.flow_start_time = Some(std::time::Instant::now());
        if stopping {
            context.stop_intent.apply_request(
                FlowStopMode::Graceful {
                    timeout: std::time::Duration::from_secs(60),
                },
                None,
            );
        }
        let original_admission = (
            context.stop_intent.deadline,
            context.stop_intent.reason.clone(),
        );
        let published = context.termination.published.clone();
        // Enter the precise handler under test before dispatch. Materializing
        // and SourceCompleted dispatch can otherwise produce an earlier event.
        let mut fsm =
            crate::pipeline::fsm::build_pipeline_fsm_with_initial(initial_fsm_state(&state));
        let actions = fsm
            .handle(
                PipelineFsmEvent::OperationalFailure {
                    message: "unexpected pipeline failure".into(),
                },
                &mut context,
            )
            .await
            .unwrap();
        for action in actions {
            action.execute(&mut context).await.unwrap();
        }
        assert_eq!(
            (
                context.stop_intent.deadline,
                context.stop_intent.reason.clone()
            ),
            original_admission,
            "failure must not manufacture or renew a stop"
        );
        let (sender, receiver, watcher) = ChannelBuilder::new().build(state.clone());
        sender
            .send(PipelineFsmEvent::OperationalFailure {
                message: "unexpected pipeline failure".into(),
            })
            .await
            .unwrap();
        let task = spawn_supervisor_loop(
            state.clone(),
            test_supervisor(system_id, journal.clone()),
            context,
            receiver,
            watcher,
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(
            matches!(&published.get().unwrap().outcome, ExecutionOutcome::Failed(failure)
            if failure.reason == "unexpected pipeline failure"),
            "{state:?}, stopping={stopping}: {:?}",
            published.get()
        );
        let events = journal.read_all_unordered().await.unwrap();
        let terminal: Vec<_> = events
            .iter()
            .map(|event| event.event.event_type_name())
            .filter(|name| {
                matches!(
                    *name,
                    "system.pipeline.completed"
                        | "system.pipeline.cancelled"
                        | "system.pipeline.failed"
                )
            })
            .collect();
        assert_eq!(terminal, ["system.pipeline.failed"]);
    }
}

#[tokio::test]
async fn pre_execution_teardown_is_explicit_and_failures_stay_selected() {
    use crate::pipeline::termination::{execution_result, ExecutionOutcome};
    for fail in [false, true] {
        let system_id = SystemId::new();
        let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
        let mut context = test_context(empty_topology(), system_id, journal.clone(), None);
        let published = context.termination.published.clone();
        assert!(
            execution_result(&published).is_err(),
            "absent evidence cannot mean success"
        );
        if fail {
            context.termination.fail("first failure".into(), None);
            context.termination.fail("cleanup failure".into(), None);
        }
        let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Created);
        sender
            .send(PipelineFsmEvent::Control(PipelineControl::Stop {
                mode: FlowStopMode::Cancel,
            }))
            .await
            .unwrap();
        let task = spawn_supervisor_loop(
            PipelineState::Created,
            test_supervisor(system_id, journal.clone()),
            context,
            receiver,
            watcher,
        );
        tokio::time::timeout(std::time::Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if fail {
            assert!(matches!(&published.get().unwrap().outcome,
            ExecutionOutcome::Failed(failure) if failure.reason == "first failure"));
        } else {
            assert!(matches!(
                published.get().unwrap().outcome,
                ExecutionOutcome::NotStarted
            ));
        }
        assert_eq!(execution_result(&published).is_err(), fail);
        let facts = journal.read_all_unordered().await.unwrap();
        let terminal = if fail {
            "system.pipeline.failed"
        } else {
            "system.pipeline.not_started"
        };
        assert!(facts
            .iter()
            .any(|fact| fact.event.event_type_name() == terminal));
        assert_eq!(
            facts.last().unwrap().event.event_type_name(),
            "system.pipeline.drained"
        );
    }
}

#[tokio::test]
async fn cancellation_catches_up_late_producer_failure_before_selecting_terminal() {
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink) = source_sink_topology();
    let subscription = empty_system_subscription(&journal).await;
    let mut context = test_context(topology, system_id, journal.clone(), Some(subscription));
    context.stage_supervisors.insert(
        sink,
        TestPipelineStageHandle::boxed(sink, "sink", StageType::Sink),
    );
    for _ in 0..64 {
        journal
            .append(SystemEvent::stage_running(sink), None)
            .await
            .unwrap();
    }
    journal
        .append(
            SystemEvent::stage_failed(sink, "late producer failure".into(), false),
            None,
        )
        .await
        .unwrap();
    let published = context.termination.published.clone();
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Running);
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Stop {
            mode: FlowStopMode::Cancel,
        }))
        .await
        .unwrap();
    let task = spawn_supervisor_loop(
        PipelineState::Running,
        system_id,
        context,
        receiver,
        watcher,
    );
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(
        matches!(&published.get().unwrap().outcome, crate::pipeline::termination::ExecutionOutcome::Failed(failure) if failure.reason.contains("late producer failure"))
    );
    let rows = journal.read_all_unordered().await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| row.event.event_type_name() == "system.pipeline.failed")
            .count(),
        1
    );
    assert!(!rows
        .iter()
        .any(|row| row.event.event_type_name() == "system.pipeline.cancelled"));
}

struct PausedReader {
    row: Option<EventEnvelope<SystemEvent>>,
    calls: Arc<AtomicUsize>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl JournalReader<SystemEvent> for PausedReader {
    async fn next(&mut self) -> Result<Option<EventEnvelope<SystemEvent>>, JournalError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        // Moving the cursor before suspension intentionally makes cancellation
        // unsafe. A recreated read would skip this committed envelope.
        let row = self.row.take();
        if row.is_some() {
            self.entered.notify_one();
            self.release.notified().await;
        }
        Ok(row)
    }
    fn position(&self) -> u64 {
        u64::from(self.row.is_none())
    }
}

#[tokio::test]
async fn pending_journal_read_survives_controls_and_gets_bounded_service() {
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, sink) = source_sink_topology();
    let row = journal
        .append(SystemEvent::stage_running(sink), None)
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let subscription = SystemSubscription::new(
        Box::new(PausedReader {
            row: Some(row.clone()),
            calls: calls.clone(),
            entered: entered.clone(),
            release: release.clone(),
        }),
        "paused reader".into(),
    );
    let mut context = test_context(topology, system_id, journal, Some(subscription));
    let (sender, receiver, watcher) = ChannelBuilder::new()
        .with_event_buffer(32)
        .build(PipelineState::Running);
    let mut supervisor = PipelineSupervisor::new(
        system_id,
        receiver,
        watcher,
        context.resources.failure.clone(),
    );
    let mut first = Box::pin(supervisor.dispatch_state(&PipelineFsmState::Running, &mut context));
    assert!(futures::poll!(&mut first).is_pending());
    tokio::time::timeout(Duration::from_secs(2), async {
        tokio::select! {
            _ = entered.notified() => {},
            result = &mut first => panic!("read unexpectedly completed: {result:?}"),
        }
    })
    .await
    .unwrap();
    sender
        .send(PipelineFsmEvent::Control(PipelineControl::Start))
        .await
        .unwrap();
    assert!(matches!(
        first.await.unwrap(),
        EventLoopDirective::Transition(PipelineFsmEvent::Control(_))
    ));
    for _ in 0..32 {
        sender
            .send(PipelineFsmEvent::Control(PipelineControl::Start))
            .await
            .unwrap();
    }
    for _ in 0..8 {
        assert!(matches!(
            supervisor
                .dispatch_state(&PipelineFsmState::Running, &mut context)
                .await
                .unwrap(),
            EventLoopDirective::Transition(PipelineFsmEvent::Control(_))
        ));
    }
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    release.notify_one();
    let mut delivered = false;
    for _ in 0..4 {
        if let EventLoopDirective::Transition(PipelineFsmEvent::Journal(envelope)) = supervisor
            .dispatch_state(&PipelineFsmState::Running, &mut context)
            .await
            .unwrap()
        {
            assert_eq!(envelope.event.id, row.event.id);
            delivered = true;
            break;
        }
    }
    assert!(
        delivered,
        "ready journal input must be served despite the full control queue"
    );
    assert_eq!(calls.load(Ordering::Relaxed), 1);
}

struct DiscardSnapshots;
impl obzenflow_core::metrics::MetricsSnapshotExporter for DiscardSnapshots {
    fn publish_app_snapshot(&self, _: obzenflow_core::metrics::AppMetricsSnapshot) {}
    fn publish_infra_snapshot(&self, _: obzenflow_core::metrics::InfraMetricsSnapshot) {}
}

fn owned_test_stage(
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

#[tokio::test]
async fn subscription_or_metrics_preparation_failure_joins_every_supplied_stage() {
    for fail_reader in [1, 2] {
        let system_id = SystemId::new();
        let mut journal = MemoryJournal::with_owner(JournalOwner::system(system_id));
        journal.fail_reader = Some(fail_reader);
        let (topology, source, sink) = source_sink_topology_with_source();
        let probes = [ShutdownProbe::default(), ShutdownProbe::default()];
        let result =
            crate::pipeline::PipelineBuilder::new(topology, Arc::new(journal), FlowId::new())
                .with_sources(vec![Box::new(owned_test_stage(
                    source,
                    StageType::FiniteSource,
                    Some(probes[0].clone()),
                ))])
                .with_stages(vec![Box::new(owned_test_stage(
                    sink,
                    StageType::Sink,
                    Some(probes[1].clone()),
                ))])
                .with_metrics_exporter(Arc::new(DiscardSnapshots))
                .build()
                .await;
        assert!(
            result.is_err(),
            "reader {fail_reader} must fail construction"
        );
        for probe in probes {
            assert_eq!(probe.request_abort_count.load(Ordering::Relaxed), 1);
            assert_eq!(probe.abort_and_join_count.load(Ordering::Relaxed), 1);
        }
    }
}

#[tokio::test]
async fn parent_panic_retains_metrics_publication_until_repeated_flow_joins_finish() {
    use crate::__private::lifecycle;
    let system_id = SystemId::new();
    let metrics_gate = Arc::new(TerminalAppendGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fail: false,
    });
    let mut journal = MemoryJournal::with_owner(JournalOwner::system(system_id));
    journal.metrics_ready_append = Some(metrics_gate.clone());
    let journal = Arc::new(journal);
    let (topology, source, sink) = source_sink_topology_with_source();
    let (entered, start_entered) = oneshot::channel();
    let (release, start_release) = oneshot::channel();
    let mut stage = owned_test_stage(sink, StageType::Sink, None);
    stage.panic_on_start = true;
    stage.start_gate = Some(StartGate {
        entered: Mutex::new(Some(entered)),
        release: tokio::sync::Mutex::new(Some(start_release)),
        count: Arc::new(AtomicUsize::new(0)),
    });
    let flow = crate::pipeline::PipelineBuilder::new(topology, journal.clone(), FlowId::new())
        .with_sources(vec![Box::new(owned_test_stage(
            source,
            StageType::FiniteSource,
            None,
        ))])
        .with_stages(vec![Box::new(stage)])
        .with_metrics_exporter(Arc::new(DiscardSnapshots))
        .build()
        .await
        .unwrap();
    let guard = lifecycle::guard_execution(&flow);
    tokio::time::timeout(Duration::from_secs(2), async {
        start_entered.await.unwrap();
        metrics_gate.entered.notified().await;
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while flow.is_running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut abandoned = Box::pin(lifecycle::wait(&flow));
    assert!(
        futures::poll!(&mut abandoned).is_pending(),
        "accepted metrics publication still owns its join"
    );
    drop(abandoned);
    metrics_gate.release.notify_one();
    for _ in 0..2 {
        let error = tokio::time::timeout(Duration::from_secs(2), lifecycle::wait(&flow))
            .await
            .unwrap()
            .unwrap_err();
        assert!(std::error::Error::source(&error)
            .unwrap()
            .to_string()
            .contains("panicked"));
    }
    guard.disarm();
    let rows = journal.read_all_unordered().await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| row.event.event_type_name() == "system.metrics.ready")
            .count(),
        1
    );
    assert!(!rows
        .iter()
        .any(|row| row.event.event_type_name() == "system.pipeline.drained"));
}
