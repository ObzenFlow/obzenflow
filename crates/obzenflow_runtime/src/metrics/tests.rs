// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Collector conformance scenarios supplied with real memory/disk journals by Infra.

use super::fsm::{
    MetricsAggregatorAction as Action, MetricsAggregatorContext as Context,
    MetricsAggregatorEvent as Event, MetricsAggregatorState as State, MetricsJournalKind as Rail,
};
use super::snapshot::{LookupOutcome, SnapshotObservation};
use super::subscription::{MetricsSubscription, IDLE_BACKOFF};
use super::supervisor::MetricsAggregatorSupervisor;
use crate::journal::FlowJournalFactory;
use crate::supervised_base::{ChannelBuilder, EventLoopDirective, SelfSupervised};
use async_trait::async_trait;
use obzenflow_core::event::context::{RuntimeContext, StageType};
use obzenflow_core::event::{ChainEventFactory, JournalEvent, SystemEvent, SystemEventFactory};
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::{JournalError, JournalReader};
use obzenflow_core::metrics::{AppMetricsSnapshot, InfraMetricsSnapshot, MetricsSnapshotExporter};
use obzenflow_core::{
    ChainEvent, EventEnvelope, EventId, Journal, JournalId, JournalOwner, StageId, SystemId,
    WriterId,
};
use obzenflow_fsm::FsmAction;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[derive(Default)]
struct Probe {
    tail_requests: Mutex<Vec<usize>>,
    materialised: AtomicUsize,
    fail_tail: AtomicBool,
    tail_gate: Mutex<Option<(usize, Arc<Gate>)>>,
    next_calls: AtomicUsize,
    fail_next_at: AtomicUsize,
    unknown_end: AtomicBool,
    end_checks: AtomicUsize,
    position: AtomicU64,
    read_gate: Mutex<Option<Arc<Gate>>>,
    export_gate: Mutex<Option<Arc<Gate>>>,
}

struct ObservedJournal<T: JournalEvent> {
    inner: Arc<dyn Journal<T>>,
    probe: Arc<Probe>,
}

impl<T: JournalEvent> ObservedJournal<T> {
    fn new(inner: Arc<dyn Journal<T>>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            probe: Arc::new(Probe::default()),
        })
    }
}

struct ObservedReader<T: JournalEvent> {
    inner: Box<dyn JournalReader<T>>,
    probe: Arc<Probe>,
}

#[async_trait]
impl<T: JournalEvent + 'static> JournalReader<T> for ObservedReader<T> {
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let call = self.probe.next_calls.fetch_add(1, Ordering::SeqCst) + 1;
        let gate = self.probe.read_gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        if self.probe.fail_next_at.load(Ordering::SeqCst) == call {
            return Err(JournalError::Full);
        }
        let result = self.inner.next().await;
        self.probe
            .position
            .store(self.inner.position(), Ordering::SeqCst);
        result
    }
    fn position(&self) -> u64 {
        self.inner.position()
    }
    fn is_at_end(&self) -> bool {
        self.probe.end_checks.fetch_add(1, Ordering::SeqCst);
        !self.probe.unknown_end.load(Ordering::SeqCst) && self.inner.is_at_end()
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for ObservedJournal<T> {
    fn id(&self) -> &JournalId {
        self.inner.id()
    }
    fn owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }
    async fn append(
        &self,
        event: T,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        if event.event_type_name() == "system.metrics.exported" {
            let gate = self.probe.export_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
        }
        self.inner.append(event, parent).await
    }
    async fn append_group(
        &self,
        id: &str,
        events: Vec<T>,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        self.inner.append_group(id, events, parent).await
    }
    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        self.inner.read_all_unordered().await
    }
    async fn read_event(&self, id: &EventId) -> Result<Option<EventEnvelope<T>>, JournalError> {
        self.inner.read_event(id).await
    }
    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(ObservedReader {
            inner: self.inner.reader_from(position).await?,
            probe: self.probe.clone(),
        }))
    }
    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        self.probe.tail_requests.lock().unwrap().push(count);
        if self.probe.fail_tail.load(Ordering::SeqCst) {
            return Err(JournalError::Full);
        }
        let rows = self.inner.read_last_n(count).await?;
        self.probe
            .materialised
            .fetch_add(rows.len(), Ordering::SeqCst);
        let gate = {
            let mut gate = self.probe.tail_gate.lock().unwrap();
            if gate.as_ref().is_some_and(|(size, _)| *size == count) {
                gate.take().map(|(_, gate)| gate)
            } else {
                None
            }
        };
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        Ok(rows)
    }
}

#[derive(Default)]
struct Exports(Mutex<Vec<AppMetricsSnapshot>>);
impl MetricsSnapshotExporter for Exports {
    fn publish_app_snapshot(&self, snapshot: AppMetricsSnapshot) {
        self.0.lock().unwrap().push(snapshot);
    }
    fn publish_infra_snapshot(&self, _: InfraMetricsSnapshot) {}
}

fn stage_journal(
    factory: &mut dyn FlowJournalFactory,
    stage: StageId,
    name: &str,
) -> Arc<ObservedJournal<ChainEvent>> {
    ObservedJournal::new(
        factory
            .create_chain_journal(
                JournalName::Stage {
                    id: stage,
                    stage_type: StageType::Transform,
                    name: name.into(),
                },
                JournalOwner::stage(stage),
            )
            .unwrap(),
    )
}

fn fact(stage: StageId, writer: WriterId, total: u64, gauge: u32) -> ChainEvent {
    let mut event =
        ChainEventFactory::data_event(writer, "metrics.fact", serde_json::json!({"total": total}));
    event.flow_context.stage_id = stage;
    event.runtime_context = Some(RuntimeContext {
        in_flight: gauge,
        recent_p50_ms: gauge.into(),
        recent_p90_ms: gauge.into(),
        recent_p95_ms: gauge.into(),
        recent_p99_ms: gauge.into(),
        recent_p999_ms: gauge.into(),
        processing_time_sum_nanos: total * 10,
        failures_total: total,
        events_processed_total: total,
        cb_requests_total: total,
        cb_state: f64::from(gauge % 3),
        rl_events_total: total,
        rl_bucket_tokens: f64::from(gauge),
        rl_bucket_capacity: f64::from(gauge + 1),
        ..super::instrumentation::StageInstrumentation::new().snapshot_with_control()
    });
    event
}

fn noise(stage: StageId) -> ChainEvent {
    ChainEventFactory::data_event(stage.into(), "metrics.no_snapshot", serde_json::json!({}))
}

async fn context(
    factory: &mut dyn FlowJournalFactory,
    data: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    errors: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
) -> (
    Context,
    super::fsm::MetricsAggregatorIo,
    Arc<ObservedJournal<SystemEvent>>,
    Arc<Exports>,
) {
    let system_id = SystemId::new();
    let system = ObservedJournal::new(
        factory
            .create_system_journal(JournalName::System, JournalOwner::system(system_id))
            .unwrap(),
    );
    let exports = Arc::new(Exports::default());
    let (ctx, io) = Context::new(
        super::MetricsInputs::new(data, errors),
        system.clone(),
        exports.clone(),
        1,
        system_id,
        HashMap::new(),
        vec![],
    )
    .await
    .unwrap();
    (ctx, io, system, exports)
}

async fn fold(ctx: &mut Context, stage: StageId, rail: Rail, row: EventEnvelope<ChainEvent>) {
    Action::UpdateMetrics {
        envelope: Box::new(row),
        journal_kind: rail,
        journal_stage: stage,
    }
    .execute(ctx)
    .await
    .unwrap();
}

fn assert_values(ctx: &Context, stage: StageId, total: u64, gauge: u32) {
    let store = &ctx.metrics_store;
    let metrics = &store.stage_metrics[&stage];
    assert_eq!(metrics.last_in_flight, Some(gauge));
    assert_eq!(metrics.snapshot_p50_ms, Some(gauge.into()));
    assert_eq!(metrics.snapshot_p999_ms, Some(gauge.into()));
    assert_eq!(metrics.latest_events_processed_total, Some(total));
    assert_eq!(metrics.last_failures_total, Some(total));
    assert_eq!(metrics.processing_time_sum_nanos, Some(total * 10));
    assert_eq!(store.circuit_breaker_state[&stage], f64::from(gauge % 3));
    assert_eq!(store.rate_limiter_bucket_tokens[&stage], f64::from(gauge));
    assert_eq!(
        store.rate_limiter_bucket_capacity[&stage],
        f64::from(gauge + 1)
    );
    assert!(store.circuit_breaker_state_transitions_total.is_empty());
}

pub async fn metrics_cache_bounds_negative_search_and_reuses_examined_heads(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let journal = stage_journal(&mut *factory, stage, "negative");
    let mut observation = SnapshotObservation::default();
    assert_eq!(observation.lookup_outcome(), None);
    observation.refresh(journal.as_ref(), stage).await.unwrap();
    assert_eq!(observation.lookup_outcome(), Some(LookupOutcome::Absent));
    assert_eq!(*journal.probe.tail_requests.lock().unwrap(), [1]);
    let foreign = StageId::new();
    for _ in 0..1_000 {
        journal.append(noise(foreign), None).await.unwrap();
    }
    journal.probe.tail_requests.lock().unwrap().clear();
    journal.probe.materialised.store(0, Ordering::SeqCst);
    observation.refresh(journal.as_ref(), stage).await.unwrap();
    assert_eq!(
        *journal.probe.tail_requests.lock().unwrap(),
        [1, 5, 20, 100, 500, 2_000]
    );
    assert_eq!(journal.probe.materialised.load(Ordering::SeqCst), 1_626);
    journal.probe.tail_requests.lock().unwrap().clear();
    observation.refresh(journal.as_ref(), stage).await.unwrap();
    assert_eq!(*journal.probe.tail_requests.lock().unwrap(), [1]);
    for _ in 0..9 {
        journal.append(noise(foreign), None).await.unwrap();
    }
    journal.probe.tail_requests.lock().unwrap().clear();
    journal.probe.materialised.store(0, Ordering::SeqCst);
    observation.refresh(journal.as_ref(), stage).await.unwrap();
    assert_eq!(*journal.probe.tail_requests.lock().unwrap(), [1, 5, 20]);
    assert_eq!(journal.probe.materialised.load(Ordering::SeqCst), 26);
    journal
        .append(fact(stage, stage.into(), 1, 7), None)
        .await
        .unwrap();
    observation.refresh(journal.as_ref(), stage).await.unwrap();
    assert_eq!(observation.selected().unwrap().in_flight, 7);

    // Shared lifecycle helpers also stop at a successfully examined beginning.
    let empty = stage_journal(&mut *factory, stage, "empty");
    let erased: Arc<dyn Journal<ChainEvent>> = empty.clone();
    assert!(super::tail_read::read_latest_runtime_context(&erased)
        .await
        .is_none());
    assert!(
        super::tail_read::read_latest_runtime_context_for_stage(&erased, stage)
            .await
            .is_none()
    );
    assert_eq!(*empty.probe.tail_requests.lock().unwrap(), [1, 1]);
}

pub async fn metrics_snapshot_selection_survives_both_refresh_failure_orders(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "data");
    let older = data
        .append(fact(stage, stage.into(), 1, 9), None)
        .await
        .unwrap();
    let newer = data
        .append(fact(stage, stage.into(), 2, 4), None)
        .await
        .unwrap();
    let (mut ctx, mut io, _, exports) =
        context(&mut *factory, vec![(stage, data.clone())], vec![]).await;
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    assert_values(&ctx, stage, 2, 4);
    assert_eq!(ctx.metrics_store.total_events_processed, 0);
    let mut prefetched = Vec::new();
    while prefetched.len() < 2 {
        prefetched.extend(
            io.data_subscription
                .poll_batch()
                .await
                .unwrap()
                .expect("committed snapshot records")
                .events,
        );
    }
    assert_eq!(prefetched[1].event.id, newer.event.id);
    assert!(ctx.metrics_store.ensure_snapshots_reconciled().is_err());
    assert_eq!(ctx.metrics_store.total_events_processed, 0);
    fold(&mut ctx, stage, Rail::Data, older).await;
    data.probe.fail_tail.store(true, Ordering::SeqCst);
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    assert_values(&ctx, stage, 2, 4);
    assert!(ctx.metrics_store.ensure_snapshots_reconciled().is_err());
    fold(&mut ctx, stage, Rail::Data, newer).await;
    ctx.metrics_store.ensure_snapshots_reconciled().unwrap();
    let latest = data
        .append(fact(stage, stage.into(), 3, 1), None)
        .await
        .unwrap();
    fold(&mut ctx, stage, Rail::Data, latest).await;
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    assert_values(&ctx, stage, 3, 1);
    assert_eq!(ctx.metrics_store.total_events_processed, 3);
    assert_eq!(
        exports.0.lock().unwrap().last().unwrap().event_counts[&stage],
        3
    );
    data.probe.fail_tail.store(false, Ordering::SeqCst);
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    ctx.metrics_store.ensure_snapshots_reconciled().unwrap();
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    assert_values(&ctx, stage, 3, 1);
}

pub async fn metrics_capped_search_keeps_sequential_selection_and_search_uncertainty(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "sparse");
    let first = data
        .append(fact(stage, stage.into(), 1, 6), None)
        .await
        .unwrap();
    let mut warm = SnapshotObservation::default();
    warm.refresh(data.as_ref(), stage).await.unwrap();
    // Real atomic frames make the large sparse history economical to create.
    for group in 0..51 {
        data.append_group(
            &format!("sparse-{group}"),
            (0..1_000).map(|_| noise(stage)).collect(),
            None,
        )
        .await
        .unwrap();
    }
    let mut cold = SnapshotObservation::default();
    for observation in [&mut cold, &mut warm] {
        observation.refresh(data.as_ref(), stage).await.unwrap();
        assert_eq!(observation.lookup_outcome(), Some(LookupOutcome::Capped));
        observation.fold(&first, stage).unwrap();
        assert_eq!(observation.selected().unwrap().in_flight, 6);
        observation.refresh(data.as_ref(), stage).await.unwrap();
        assert_eq!(observation.lookup_outcome(), Some(LookupOutcome::Capped));
        assert!(!observation.is_ahead_of_fold());
    }
    let next = data
        .append(fact(stage, stage.into(), 2, 2), None)
        .await
        .unwrap();
    cold.fold(&next, stage).unwrap();
    data.probe.fail_tail.store(true, Ordering::SeqCst);
    assert!(cold.refresh(data.as_ref(), stage).await.is_err());
    assert_eq!(cold.lookup_outcome(), Some(LookupOutcome::Capped));
    assert_eq!(cold.selected().unwrap().in_flight, 2);
}

pub async fn metrics_tail_results_bind_to_the_window_actually_examined(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "moving_head");
    data.append(fact(stage, stage.into(), 1, 8), None)
        .await
        .unwrap();
    data.append(noise(stage), None).await.unwrap();
    let gate = Arc::new(Gate::default());
    *data.probe.tail_gate.lock().unwrap() = Some((5, gate.clone()));
    let mut observation = SnapshotObservation::default();
    let (result, ()) = tokio::join!(observation.refresh(data.as_ref(), stage), async {
        gate.entered.notified().await;
        data.append(fact(stage, stage.into(), 2, 3), None)
            .await
            .unwrap();
        gate.release.notify_one();
    });
    result.unwrap();
    assert_eq!(observation.selected().unwrap().in_flight, 8);
    observation.refresh(data.as_ref(), stage).await.unwrap();
    assert_eq!(observation.selected().unwrap().in_flight, 3);
}

pub async fn metrics_snapshot_identity_handles_mixed_writers_groups_and_rail_precedence(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let archived = StageId::new();
    let data = stage_journal(&mut *factory, stage, "data");
    let errors = stage_journal(&mut *factory, stage, "errors");
    for _ in 0..4 {
        data.append(noise(archived), None).await.unwrap();
    }
    let a = data
        .append(fact(stage, archived.into(), 1, 8), None)
        .await
        .unwrap();
    let mut b = fact(stage, stage.into(), 2, 5);
    b.id = a.event.id;
    let mut c = fact(stage, stage.into(), 3, 2);
    c.id = a.event.id;
    let group = data
        .append_group("repeated-event-id", vec![b, c], None)
        .await
        .unwrap();
    let (mut ctx, _, _, _) = context(
        &mut *factory,
        vec![(stage, data.clone())],
        vec![(stage, errors.clone())],
    )
    .await;
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    for row in [a, group[0].clone()] {
        fold(&mut ctx, stage, Rail::Data, row).await;
        Action::ExportMetrics.execute(&mut ctx).await.unwrap();
        assert_values(&ctx, stage, 3, 2);
        assert!(ctx.metrics_store.ensure_snapshots_reconciled().is_err());
    }
    fold(&mut ctx, stage, Rail::Data, group[1].clone()).await;
    ctx.metrics_store.ensure_snapshots_reconciled().unwrap();
    let error = errors
        .append(fact(stage, archived.into(), 2, 7), None)
        .await
        .unwrap();
    errors
        .append(fact(archived, archived.into(), 99, 99), None)
        .await
        .unwrap();
    fold(&mut ctx, stage, Rail::Error, error).await;
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    assert_values(&ctx, stage, 3, 7);
    assert_eq!(ctx.metrics_store.stage_vector_clocks.get(&archived), None);
    assert_eq!(ctx.metrics_store.stage_vector_clocks[&stage], 2);
    ctx.metrics_store.ensure_snapshots_reconciled().unwrap();
}

pub async fn metrics_batches_preserve_prefix_errors_and_require_fresh_positive_ends(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "batch");
    let mut sub = MetricsSubscription::new(&[(stage, data.clone())])
        .await
        .unwrap();
    assert!(sub.poll_batch().await.unwrap().is_none());
    let calls = data.probe.next_calls.load(Ordering::SeqCst);
    assert!(sub.poll_batch().await.unwrap().is_none());
    assert_eq!(data.probe.next_calls.load(Ordering::SeqCst), calls);
    for _ in 0..3 {
        data.append(noise(stage), None).await.unwrap();
    }
    let (mut ctx, _, _, _) = context(&mut *factory, vec![(stage, data.clone())], vec![]).await;
    // Terminal invalidates the live-empty cooldown before the next read.
    data.probe.fail_next_at.store(calls + 3, Ordering::SeqCst);
    sub.observe_terminal();
    let mut folded = 0;
    loop {
        match sub.poll_batch().await {
            Ok(Some(batch)) => {
                assert_eq!(batch.stage, stage);
                folded += batch.events.len();
                assert!(!sub.is_complete());
                for row in batch.events {
                    fold(&mut ctx, stage, Rail::Data, row).await;
                }
            }
            Err(JournalError::Full) => break,
            _ => panic!("prefix must be delivered before its retained failure"),
        }
    }
    assert_eq!(folded, 2);
    assert_eq!(ctx.metrics_store.total_events_processed, 2);
    assert_eq!(data.probe.next_calls.load(Ordering::SeqCst), calls + 3);
    assert_eq!(data.probe.position.load(Ordering::SeqCst), 2);

    let empty = stage_journal(&mut *factory, stage, "unknown_end");
    empty.probe.unknown_end.store(true, Ordering::SeqCst);
    let mut sub = MetricsSubscription::new(&[(stage, empty.clone())])
        .await
        .unwrap();
    sub.observe_terminal();
    assert!(sub.poll_batch().await.unwrap().is_none());
    assert!(!sub.is_complete());
    empty.probe.unknown_end.store(false, Ordering::SeqCst);
    // This timer tests the specified retry delay, not a synchronisation barrier.
    tokio::time::sleep(IDLE_BACKOFF).await;
    assert!(sub.poll_batch().await.unwrap().is_none());
    assert!(sub.is_complete());
    assert_eq!(empty.probe.end_checks.load(Ordering::SeqCst), 2);
}

fn supervisor(ctx: &Context, io: super::fsm::MetricsAggregatorIo) -> MetricsAggregatorSupervisor {
    let (_, _, watcher) = ChannelBuilder::<Event, State>::new().build(State::Running);
    MetricsAggregatorSupervisor {
        name: "metrics_conformance".into(),
        system_journal: ctx.system_journal.clone(),
        system_id: ctx.system_id,
        data_subscription: Some(io.data_subscription),
        error_subscription: io.error_subscription,
        system_subscription: Some(io.system_subscription),
        system_retry_at: None,
        next_input: 0,
        state_watcher: watcher,
        last_state: None,
    }
}

pub async fn metrics_rotation_coalesces_exports_and_spaces_from_acknowledged_publication(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "data");
    let errors = stage_journal(&mut *factory, stage, "errors");
    for _ in 0..200 {
        data.append(noise(stage), None).await.unwrap();
        errors.append(noise(stage), None).await.unwrap();
    }
    let (mut ctx, io, system, _) =
        context(&mut *factory, vec![(stage, data)], vec![(stage, errors)]).await;
    let events = SystemEventFactory::new(ctx.system_id);
    system
        .append(events.pipeline_draining(), None)
        .await
        .unwrap();
    system
        .append(events.pipeline_all_stages_completed(), None)
        .await
        .unwrap();
    let mut supervisor = supervisor(&ctx, io);
    let mut machine = super::fsm::build_metrics_aggregator_fsm();
    for action in machine.handle(Event::StartRunning, &mut ctx).await.unwrap() {
        action.execute(&mut ctx).await.unwrap();
    }
    for expected in [
        "system", "data", "error", "export", "system", "data", "error",
    ] {
        let EventLoopDirective::Transition(event) = supervisor
            .dispatch_state(machine.state(), &mut ctx)
            .await
            .unwrap()
        else {
            panic!("ready input turn")
        };
        match (&event, expected) {
            (Event::ProcessSystemEvent { .. }, "system") | (Event::ExportMetrics, "export") => {}
            (
                Event::ProcessBatch {
                    events,
                    journal_kind,
                    ..
                },
                kind,
            ) => {
                assert!(!events.is_empty() && events.len() <= 64);
                assert_eq!(
                    *journal_kind,
                    if kind == "data" {
                        Rail::Data
                    } else {
                        Rail::Error
                    }
                );
            }
            _ => panic!("unexpected turn {event:?}, expected {expected}"),
        }
        let actions = machine.handle(event, &mut ctx).await.unwrap();
        if expected == "system" {
            assert!(actions
                .iter()
                .all(|action| !matches!(action, Action::ExportMetrics)));
        }
        for action in actions {
            if matches!(action, Action::ExportMetrics) {
                let gate = Arc::new(Gate::default());
                *system.probe.export_gate.lock().unwrap() = Some(gate.clone());
                let (result, released) = tokio::join!(action.execute(&mut ctx), async {
                    gate.entered.notified().await;
                    // A slow acknowledged publication spans several intervals.
                    tokio::time::sleep(Duration::from_millis(1_100)).await;
                    let released = tokio::time::Instant::now();
                    gate.release.notify_one();
                    released
                });
                result.unwrap();
                assert!(ctx.metrics_store.last_export_completed.unwrap() >= released);
            } else {
                action.execute(&mut ctx).await.unwrap();
            }
        }
    }
    // Eligibility returns to input collection while the next export is spaced.
    let event = supervisor
        .dispatch_state(machine.state(), &mut ctx)
        .await
        .unwrap();
    assert!(!matches!(
        event,
        EventLoopDirective::Transition(Event::ExportMetrics)
    ));
}

pub async fn metrics_physical_completion_folds_all_rails_through_the_current_terminal(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let mut data = Vec::new();
    let mut errors = Vec::new();
    let mut probes = Vec::new();
    for index in 0..5 {
        let stage = StageId::new();
        let journal = stage_journal(&mut *factory, stage, "data");
        let error = stage_journal(&mut *factory, stage, "errors");
        journal
            .append(fact(stage, stage.into(), 1, 0), None)
            .await
            .unwrap();
        if index == 0 {
            error.append(noise(stage), None).await.unwrap();
        }
        probes.push((journal.probe.clone(), 1));
        probes.push((error.probe.clone(), u64::from(index == 0)));
        data.push((stage, journal as Arc<dyn Journal<ChainEvent>>));
        errors.push((stage, error as Arc<dyn Journal<ChainEvent>>));
    }
    let (mut ctx, io, system, exports) = context(&mut *factory, data, errors).await;
    let pipeline = SystemId::new();
    ctx.pipeline_writer = Some(pipeline.into());
    let old = SystemEventFactory::new(SystemId::new());
    let current = SystemEventFactory::new(pipeline);
    system
        .append(old.pipeline_not_started(), None)
        .await
        .unwrap();
    system
        .append(current.pipeline_all_stages_completed(), None)
        .await
        .unwrap();
    system
        .append(current.pipeline_not_started(), None)
        .await
        .unwrap();
    system
        .append(current.pipeline_drained(), None)
        .await
        .unwrap();
    let supervisor = supervisor(&ctx, io);
    crate::supervised_base::SelfSupervisedExt::run(supervisor, State::Initializing, ctx)
        .await
        .unwrap();
    for (probe, records) in probes {
        assert_eq!(probe.position.load(Ordering::SeqCst), records);
        assert!(probe.end_checks.load(Ordering::SeqCst) > 0);
    }
    assert_eq!(
        system.probe.position.load(Ordering::SeqCst),
        3,
        "system reader stops at the current terminal"
    );
    let rows = system.read_all_unordered().await.unwrap();
    let names: Vec<_> = rows.iter().map(|row| row.event.event_type_name()).collect();
    let drained = names
        .iter()
        .position(|name| *name == "system.metrics.drained")
        .unwrap();
    assert_eq!(names[drained - 1], "system.metrics.exported");
    assert_eq!(names[drained + 1], "system.metrics.shutdown");
    assert_eq!(
        exports.0.lock().unwrap().last().unwrap().pipeline_state,
        "not_started"
    );
}

pub async fn metrics_final_refresh_inconsistency_fails_without_successful_drained(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "data");
    let (mut ctx, _, system, _) = context(&mut *factory, vec![(stage, data.clone())], vec![]).await;
    ctx.metrics_store.inputs_covered = true;
    data.append(fact(stage, stage.into(), 1, 1), None)
        .await
        .unwrap();
    let mut machine = super::fsm::build_metrics_aggregator_fsm();
    machine.handle(Event::StartRunning, &mut ctx).await.unwrap();
    machine
        .handle(Event::StartDraining, &mut ctx)
        .await
        .unwrap();
    let actions = machine.handle(Event::FlowTerminal, &mut ctx).await.unwrap();
    let error = actions[0].execute(&mut ctx).await.unwrap_err();
    assert!(matches!(machine.state(), State::Drained { .. }));
    machine
        .handle(Event::Error(error.to_string()), &mut ctx)
        .await
        .unwrap();
    assert!(matches!(machine.state(), State::Failed { .. }));
    assert!(Action::PublishDrainComplete {
        last_event_id: None
    }
    .execute(&mut ctx)
    .await
    .is_err());
    assert!(system.read_all_unordered().await.unwrap().is_empty());
}

pub async fn metrics_batch_quantum_keeps_pending_reads_and_finalisation_does_not_wait_for_export(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "pending_batch");
    data.append(noise(stage), None).await.unwrap();
    let mut sub = MetricsSubscription::new(&[(stage, data.clone())])
        .await
        .unwrap();
    let gate = Arc::new(Gate::default());
    *data.probe.read_gate.lock().unwrap() = Some(gate.clone());
    let (result, ()) = tokio::join!(sub.poll_batch(), async {
        gate.entered.notified().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            data.probe.next_calls.load(Ordering::SeqCst),
            1,
            "the quantum must not cancel or restart a pending read"
        );
        gate.release.notify_one();
    });
    assert_eq!(result.unwrap().unwrap().events.len(), 1);
    assert_eq!(data.probe.next_calls.load(Ordering::SeqCst), 1);

    // Start at eligibility with terminal already folded, then discover the
    // final empty input later in this rotation. No periodic timer is needed.
    let empty = stage_journal(&mut *factory, stage, "last_end");
    let (mut ctx, io, _, _) = context(&mut *factory, vec![(stage, empty)], vec![]).await;
    ctx.metrics_store.pipeline_state = "completed".into();
    ctx.metrics_store.last_export_completed = Some(tokio::time::Instant::now());
    let mut supervisor = supervisor(&ctx, io);
    supervisor.next_input = 3;
    let event = supervisor
        .dispatch_state(&State::Draining, &mut ctx)
        .await
        .unwrap();
    assert!(
        matches!(event, EventLoopDirective::Transition(Event::FlowTerminal)),
        "the completed rotation must finalise directly"
    );
}

pub async fn metrics_pending_read_cancellation_never_publishes_drained(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "data");
    let gate = Arc::new(Gate::default());
    *data.probe.read_gate.lock().unwrap() = Some(gate.clone());
    let (ctx, io, system, _) = context(&mut *factory, vec![(stage, data.clone())], vec![]).await;
    let supervisor = supervisor(&ctx, io);
    let task = tokio::spawn(crate::supervised_base::SelfSupervisedExt::run(
        supervisor,
        State::Initializing,
        ctx,
    ));
    gate.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(data.probe.position.load(Ordering::SeqCst), 0);
    assert!(!system
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .any(|row| row.event.event_type_name() == "system.metrics.drained"));
}

pub async fn metrics_watermarks_exclude_each_forwarded_control_and_error_witness(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::types::{Count, JournalIndex, JournalPath, SeqNo};
    use obzenflow_core::event::SourceContractEventParams;
    let source = StageId::new();
    let counter = StageId::new();
    let summary = StageId::new();
    let transform = StageId::new();
    let counter_data = stage_journal(&mut *factory, counter, "counter");
    let summary_data = stage_journal(&mut *factory, summary, "summary");
    let transform_data = stage_journal(&mut *factory, transform, "transform");
    let transform_errors = stage_journal(&mut *factory, transform, "errors");
    let (mut ctx, _, _, _) = context(
        &mut *factory,
        vec![
            (counter, counter_data.clone()),
            (summary, summary_data.clone()),
            (transform, transform_data.clone()),
        ],
        vec![(transform, transform_errors.clone())],
    )
    .await;
    for (stage, journal) in [
        (counter, &counter_data),
        (summary, &summary_data),
        (transform, &transform_data),
    ] {
        let row = journal
            .append(fact(stage, stage.into(), 1, 0), None)
            .await
            .unwrap();
        fold(&mut ctx, stage, Rail::Data, row).await;
    }
    // Forwarded source EOF has local counter context but retains its source writer.
    for _ in 0..3 {
        let mut eof = ChainEventFactory::eof_event(source.into(), true);
        eof.flow_context.stage_id = counter;
        let row = counter_data.append(eof, None).await.unwrap();
        fold(&mut ctx, counter, Rail::Data, row).await;
    }
    assert_eq!(ctx.metrics_store.stage_vector_clocks[&counter], 1);
    assert!(!ctx.metrics_store.stage_vector_clocks.contains_key(&source));

    // Source contract is a separate witness, not merely another EOF assertion.
    for _ in 0..4 {
        let mut contract = ChainEventFactory::source_contract_event(
            source.into(),
            SourceContractEventParams {
                expected_count: Some(Count(3)),
                source_id: source,
                route: None,
                journal_path: JournalPath("source".into()),
                journal_index: JournalIndex(0),
                writer_seq: Some(SeqNo(3)),
                vector_clock: None,
            },
        );
        contract.flow_context.stage_id = summary;
        let row = summary_data.append(contract, None).await.unwrap();
        fold(&mut ctx, summary, Rail::Data, row).await;
    }
    assert_eq!(ctx.metrics_store.stage_vector_clocks[&summary], 1);
    assert!(!ctx.metrics_store.stage_vector_clocks.contains_key(&source));

    // Error copies retain both source writer and source runtime context. Their
    // cumulative metrics still project, while no source data was collected.
    for total in 1..=5 {
        let row = transform_errors
            .append(
                fact(source, source.into(), total, 0).mark_as_error("expected", ErrorKind::Unknown),
                None,
            )
            .await
            .unwrap();
        fold(&mut ctx, transform, Rail::Error, row).await;
    }
    assert_eq!(
        ctx.metrics_store.stage_metrics[&source].latest_events_processed_total,
        Some(5)
    );
    assert!(!ctx.metrics_store.stage_vector_clocks.contains_key(&source));
    for total in 1..=3 {
        let row = transform_errors
            .append(fact(transform, transform.into(), total, 0), None)
            .await
            .unwrap();
        fold(&mut ctx, transform, Rail::Error, row).await;
    }
    assert_eq!(
        ctx.metrics_store.stage_vector_clocks[&transform], 1,
        "same writer on the error rail cannot advance data coverage"
    );
    Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    assert_eq!(
        ctx.metrics_store.stage_vector_clocks,
        HashMap::from([(counter, 1), (summary, 1), (transform, 1)])
    );
}
