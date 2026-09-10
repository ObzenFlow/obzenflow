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
    terminal_append: Option<Arc<TerminalAppendGate>>,
}

impl<T: JournalEvent> MemoryJournal<T> {
    fn with_owner(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
            terminal_append: None,
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
    let mut context = test_context(topology, system_id, journal, None);
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
    context.metrics_handle = Some(
        HandleBuilder::new()
            .with_event_sender(sender)
            .with_state_watcher(watcher)
            .with_supervisor_task(task)
            .build_standard()
            .unwrap()
            .into(),
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
    request_abort_count: Arc<AtomicUsize>,
    force_shutdown_count: Arc<AtomicUsize>,
    wait_for_completion_count: Arc<AtomicUsize>,
    abort_and_join_count: Arc<AtomicUsize>,
}

impl TestPipelineStageHandle {
    fn boxed(id: StageId, name: impl Into<String>, stage_type: StageType) -> Arc<dyn StageHandle> {
        Arc::new(Self {
            stall_drain: false,
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
            if !probe.completed.load(Ordering::Relaxed) {
                std::future::pending::<()>().await;
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
            probe.request_abort_count.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn test_supervisor(
    system_id: SystemId,
    _system_journal: Arc<MemoryJournal<SystemEvent>>,
) -> PipelineSupervisor {
    PipelineSupervisor {
        name: "test_pipeline_supervisor".to_string(),
        system_id,
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
        let scope = crate::supervised_base::publication::PublicationScope::concurrent();
        let result = scope
            .enter(crate::pipeline::driver::run(
                supervisor,
                receiver,
                watcher,
                context,
                initial_state,
            ))
            .await;
        scope.join().await?;
        result
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
        }),
    );
    let (sender, receiver, watcher) =
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Running);
    let task = spawn_supervisor_loop(
        PipelineState::Running,
        test_supervisor(system_id, journal.clone()),
        context,
        receiver,
        watcher,
    );
    sender
        .send(PipelineEvent::StopRequested {
            mode: FlowStopMode::Graceful {
                timeout: std::time::Duration::from_millis(20),
            },
            reason: None,
        })
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
        ChannelBuilder::<PipelineEvent, PipelineState>::new().build(PipelineState::Draining);
    let started = std::time::Instant::now();
    let task = spawn_supervisor_loop(
        PipelineState::Draining,
        test_supervisor(system_id, system_journal),
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
        0,
        "an overdue supervisor should not receive a fresh completion wait"
    );
    assert_eq!(
        shutdown_probe.abort_and_join_count.load(Ordering::Relaxed),
        1,
        "the overdue supervisor must be aborted and joined exactly once"
    );
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
        TestPipelineStageHandle::boxed(sink_stage_id, "sink", StageType::Sink),
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
    context.running_stages.insert(sink_stage_id);
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
    let _lock = bootstrap_test_lock_async().await;
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
    sender
        .send(PipelineEvent::AllStagesCompleted)
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
        .send(PipelineEvent::StopRequested {
            mode: FlowStopMode::Graceful {
                timeout: std::time::Duration::ZERO,
            },
            reason: None,
        })
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
    assert!(matches!(facts[0], PipelineLifecycleEvent::Completed { .. }));
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
                ChannelBuilder::<PipelineEvent, PipelineState>::new().build(state.clone());
            let event = if terminal == "failed" {
                PipelineEvent::Error {
                    message: "test_failure".into(),
                }
            } else {
                PipelineEvent::AllStagesCompleted
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
        let mut fsm = crate::pipeline::fsm::build_pipeline_fsm_with_initial(state.clone());
        let actions = fsm
            .handle(
                PipelineEvent::Error {
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
            .send(PipelineEvent::Error {
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
            .send(PipelineEvent::StopRequested {
                mode: FlowStopMode::Cancel,
                reason: None,
            })
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
