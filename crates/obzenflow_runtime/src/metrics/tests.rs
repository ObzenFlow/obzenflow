// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Latest-value conformance scenarios exercised against real memory and disk journals.
use super::buffer::TailReaders;
use super::fsm::{
    MetricsAggregatorAction as Action, MetricsAggregatorContext as Context,
    MetricsAggregatorEvent as Event, MetricsAggregatorState as State,
};
use super::supervisor::MetricsAggregatorSupervisor;
use crate::supervised_base::{ChannelBuilder, SelfSupervisedExt};
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::observability::ObservationSource;
use obzenflow_core::event::{ChainEventFactory, JournalEvent, SystemEvent, SystemEventFactory};
use obzenflow_core::journal::factory::FlowJournalFactory;
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::journal::{AppendOptions, JournalError, JournalReader};
use obzenflow_core::metrics::{AppMetricsSnapshot, InfraMetricsSnapshot, MetricsSnapshotExporter};
use obzenflow_core::{
    ChainEvent, EventId, Journal, JournalId, JournalOwner, JournalRecord, StageId, SystemId,
    WriterId,
};
use obzenflow_fsm::FsmAction;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Default)]
struct Gate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
#[derive(Default)]
struct Probe {
    calls: AtomicUsize,
    fail: AtomicBool,
    gate: Mutex<Option<Arc<Gate>>>,
    active: AtomicUsize,
}
struct Reading(Arc<Probe>);
impl Drop for Reading {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
    }
}
struct ObservedJournal<T: JournalEvent> {
    inner: Arc<dyn Journal<T>>,
    probe: Arc<Probe>,
}
impl<T: JournalEvent> ObservedJournal<T> {
    fn new(inner: Arc<dyn Journal<T>>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            probe: Arc::default(),
        })
    }
}
#[async_trait]
impl<T: JournalEvent> Journal<T> for ObservedJournal<T> {
    fn id(&self) -> &JournalId {
        self.inner.id()
    }
    fn owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }
    async fn append(
        &self,
        event: T,
        options: AppendOptions<'_, T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        self.inner.append(event, options).await
    }
    async fn append_group(
        &self,
        id: &str,
        events: Vec<T>,
        options: AppendOptions<'_, T>,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.inner.append_group(id, events, options).await
    }
    async fn read_all_unordered(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.inner.read_all_unordered().await
    }
    async fn read_event(
        &self,
        id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        self.inner.read_event(id).await
    }
    async fn reader_from(&self, _: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        panic!("metrics must never create a sequential reader")
    }
    async fn read_last_n(&self, _: usize) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        panic!("metrics must never expand a backwards history search")
    }
    async fn read_metrics_tail(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.probe.calls.fetch_add(1, Ordering::SeqCst);
        self.probe.active.fetch_add(1, Ordering::SeqCst);
        let _reading = Reading(self.probe.clone());
        let gate = self.probe.gate.lock().unwrap().take();
        if let Some(gate) = gate {
            gate.entered.notify_one();
            gate.release.notified().await;
        }
        if self.probe.fail.load(Ordering::SeqCst) {
            return Err(JournalError::Full);
        }
        self.inner.read_metrics_tail().await
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
    event.runtime = Some(RuntimeProvenance {
        accounting: ExecutionAccounting {
            failures_total: total,
            events_processed_total: total,
            ..Default::default()
        },
    });
    use obzenflow_core::event::observability::*;
    use obzenflow_core::event::payloads::execution_payload::CircuitState;
    use obzenflow_core::event::provenance::{ExecutionAccounting, RuntimeProvenance};
    let mut packet = ObservabilityContext::new(CaptureStamp {
        capture_scope: CaptureScope {
            flow_id: "00000000000000000000000001".parse().unwrap(),
            resume_generation: Default::default(),
        },
        observer: stage.into(),
        capture_seq: CaptureSeq(total),
        capture_reason: CaptureReason::Record,
        observed_at_ms: total,
    });
    packet.runtime = Some(RuntimeObservability {
        in_flight: Some(gauge),
        timing: Some(TimingMeasurements {
            processing_time_count: total,
            processing_time_sum_nanos: total * 10,
            recent_p50_ms: Some(gauge.into()),
            recent_p90_ms: Some(gauge.into()),
            recent_p95_ms: Some(gauge.into()),
            recent_p99_ms: Some(gauge.into()),
            recent_p999_ms: Some(gauge.into()),
            window: MeasurementWindow {
                started_at_ms: 0,
                ended_at_ms: total,
            },
        }),
        circuit_breaker: Some(CircuitBreakerMeasurements {
            observed_state: CircuitState::Open,
            requests_total: total,
            successes_total: 0,
            failures_total: 0,
            slow_total: 0,
            rejections_total: 0,
            opened_total: 0,
            time_closed_seconds: 0.0,
            time_open_seconds: 0.0,
            time_half_open_seconds: 0.0,
        }),
        rate_limiter: Some(RateLimiterMeasurements {
            events_total: total,
            delayed_total: 0,
            tokens_consumed_total: 0.0,
            delay_seconds_total: 0.0,
            bucket_tokens: f64::from(gauge),
            bucket_capacity: f64::from(gauge + 1),
        }),
        ..Default::default()
    });
    event.envelope.observability = Some(packet);
    event
}

fn noise(stage: StageId) -> ChainEvent {
    ChainEventFactory::data_event(stage.into(), "metrics.no_snapshot", serde_json::json!({}))
}

async fn context(
    factory: &mut dyn FlowJournalFactory,
    data: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    errors: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
) -> (Context, Arc<ObservedJournal<SystemEvent>>, Arc<Exports>) {
    let system_id = SystemId::new();
    let system = ObservedJournal::new(
        factory
            .create_system_journal(JournalName::System, JournalOwner::system(system_id))
            .unwrap(),
    );
    let exports = Arc::new(Exports::default());
    let ctx = Context::new(
        super::MetricsInputs::new(data, errors),
        system.clone(),
        exports.clone(),
        Duration::from_millis(20),
        system_id,
        HashMap::new(),
        vec![],
    )
    .await
    .unwrap();
    (ctx, system, exports)
}
async fn until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("latest values should become available promptly");
}
async fn refresh(ctx: &mut Context) {
    let since = tokio::time::Instant::now();
    let mut readers = TailReaders::start(ctx);
    until(|| {
        ctx.metrics_store
            .buffer
            .refreshed_since(since, readers.len())
    })
    .await;
    readers.stop().await;
    Action::ExportMetrics.execute(ctx).await.unwrap();
}
fn run(
    ctx: Context,
) -> (
    tokio::task::JoinHandle<()>,
    crate::supervised_base::builder::EventSender<Event>,
) {
    let (control, receiver, watcher) = ChannelBuilder::new().build(State::Initializing);
    let supervisor = MetricsAggregatorSupervisor {
        name: "metrics-test".into(),
        system_journal: ctx.system_journal.clone(),
        system_id: ctx.system_id,
        control: receiver,
        readers: None,
        final_refresh: None,
        state_watcher: watcher,
        last_state: None,
    };
    (
        tokio::spawn(async move {
            supervisor.run(State::Initializing, ctx).await.unwrap();
        }),
        control,
    )
}

pub async fn metrics_tail_overwrites_and_preserves_sparse_families(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "latest");
    let mut first = fact(stage, stage.into(), 1, 9);
    first
        .envelope
        .observability
        .as_mut()
        .unwrap()
        .runtime
        .as_mut()
        .unwrap()
        .event_loops_total = Some(42);
    data.append(first, Default::default()).await.unwrap();
    for seq in 2..=100 {
        data.append(fact(stage, stage.into(), seq, 3), Default::default())
            .await
            .unwrap();
    }
    // History length and attachment-free suffixes cannot alter lookup work.
    for group in 0..51 {
        data.append_group(
            &format!("noise-{group}"),
            (0..1000).map(|_| noise(stage)).collect(),
            Default::default(),
        )
        .await
        .unwrap();
    }
    let selected = data.read_metrics_tail().await.unwrap();
    assert_eq!(
        selected.len(),
        2,
        "one latest packet plus the independently retained sparse family"
    );
    let (mut ctx, _, _) = context(&mut *factory, vec![(stage, data)], vec![]).await;
    refresh(&mut ctx).await;
    let stage_metrics = &ctx.metrics_store.stage_metrics[&stage];
    assert_eq!(stage_metrics.latest_events_processed_total, Some(100));
    assert_eq!(stage_metrics.last_in_flight, Some(3));
    assert_eq!(stage_metrics.event_loops_total, Some(42));
    assert!(ctx
        .metrics_store
        .circuit_breaker_state_transitions_total
        .is_empty());
    for _ in 0..3 {
        Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    }
    assert_eq!(
        ctx.metrics_store.stage_metrics[&stage].latest_events_processed_total,
        Some(100)
    );
}

pub async fn metrics_refresh_failures_retain_values_and_exports_do_no_reads(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "retention");
    data.append(fact(stage, stage.into(), 1, 9), Default::default())
        .await
        .unwrap();
    let (mut ctx, _, _) = context(&mut *factory, vec![(stage, data.clone())], vec![]).await;
    refresh(&mut ctx).await;
    data.probe.fail.store(true, Ordering::SeqCst);
    data.append(fact(stage, stage.into(), 2, 4), Default::default())
        .await
        .unwrap();
    refresh(&mut ctx).await;
    assert_eq!(
        ctx.metrics_store.stage_metrics[&stage].latest_events_processed_total,
        Some(1)
    );
    let calls = data.probe.calls.load(Ordering::SeqCst);
    for _ in 0..4 {
        Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    }
    assert_eq!(data.probe.calls.load(Ordering::SeqCst), calls);
    assert_eq!(
        ctx.metrics_store.stage_metrics[&stage].last_in_flight,
        Some(9)
    );
    data.probe.fail.store(false, Ordering::SeqCst);
    refresh(&mut ctx).await;
    assert_eq!(
        ctx.metrics_store.stage_metrics[&stage].latest_events_processed_total,
        Some(2)
    );
    assert_eq!(
        ctx.metrics_store.stage_metrics[&stage].last_in_flight,
        Some(4)
    );
}

pub async fn metrics_pending_refresh_does_not_block_publication_or_other_journals(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let a = StageId::new();
    let b = StageId::new();
    let slow = stage_journal(&mut *factory, a, "slow");
    let fast = stage_journal(&mut *factory, b, "fast");
    slow.append(fact(a, a.into(), 90, 3), Default::default())
        .await
        .unwrap();
    fast.append(fact(b, b.into(), 200, 1), Default::default())
        .await
        .unwrap();
    let gate = Arc::new(Gate::default());
    *slow.probe.gate.lock().unwrap() = Some(gate.clone());
    let (ctx, system, exports) = context(
        &mut *factory,
        vec![(a, slow.clone()), (b, fast.clone())],
        vec![],
    )
    .await;
    let (task, _control) = run(ctx);
    gate.entered.notified().await;
    until(|| exports.0.lock().unwrap().len() >= 4).await;
    {
        let snapshots = exports.0.lock().unwrap();
        let last = snapshots.last().unwrap();
        assert_eq!(last.event_counts.get(&b), Some(&200));
        assert!(!last.event_counts.contains_key(&a));
    }
    fast.append(fact(b, b.into(), 500, 2), Default::default())
        .await
        .unwrap();
    until(|| {
        exports
            .0
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .event_counts
            .get(&b)
            == Some(&500)
    })
    .await;
    // Even finalisation uses the available buffer, without waiting for this read.
    system
        .append(
            SystemEventFactory::new(SystemId::new()).pipeline_not_started(),
            Default::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(slow.probe.active.load(Ordering::SeqCst), 0);
    let rows = system.read_all_unordered().await.unwrap();
    let names: Vec<_> = rows.iter().map(|row| row.event_type_name()).collect();
    let drained = names
        .iter()
        .position(|name| *name == "system.metrics.drained")
        .unwrap();
    assert_eq!(names[drained - 1], "system.metrics.exported");
    assert_eq!(names[drained + 1], "system.metrics.shutdown");
    assert_eq!(
        exports.0.lock().unwrap().last().unwrap().event_counts[&b],
        500
    );
}

pub async fn metrics_cancellation_stops_owned_readers_without_drained(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "cancel");
    let gate = Arc::new(Gate::default());
    *data.probe.gate.lock().unwrap() = Some(gate.clone());
    let (ctx, system, _) = context(&mut *factory, vec![(stage, data.clone())], vec![]).await;
    let (task, _control) = run(ctx);
    gate.entered.notified().await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    until(|| data.probe.active.load(Ordering::SeqCst) == 0).await;
    assert!(!system
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .any(|row| row.event_type_name() == "system.metrics.drained"));
}

pub async fn metrics_tail_identity_and_accounting_are_idempotent(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let foreign = StageId::new();
    let data = stage_journal(&mut *factory, stage, "data");
    let errors = stage_journal(&mut *factory, stage, "errors");
    let first = data
        .append(fact(stage, foreign.into(), 1, 9), Default::default())
        .await
        .unwrap();
    let mut second = fact(stage, stage.into(), 2, 5);
    second.id = *first.id();
    let mut third = fact(stage, stage.into(), 3, 2);
    third.id = *first.id();
    data.append_group("shared-id", vec![second, third], Default::default())
        .await
        .unwrap();
    // Error accounting can be newer while its optional capture is older.
    let mut error = fact(stage, stage.into(), 4, 7);
    error
        .envelope
        .observability
        .as_mut()
        .unwrap()
        .capture
        .capture_seq = obzenflow_core::event::observability::CaptureSeq(2);
    errors.append(error, Default::default()).await.unwrap();
    let (mut ctx, _, _) = context(&mut *factory, vec![(stage, data)], vec![(stage, errors)]).await;
    refresh(&mut ctx).await;
    for _ in 0..3 {
        Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    }
    assert_eq!(
        ctx.metrics_store.stage_metrics[&stage].latest_events_processed_total,
        Some(4)
    );
    assert_eq!(
        ctx.metrics_store.stage_metrics[&stage].last_in_flight,
        Some(2)
    );
    assert_eq!(ctx.metrics_store.stage_vector_clocks[&stage], 2);
    assert!(!ctx.metrics_store.stage_vector_clocks.contains_key(&foreign));
}

pub async fn metrics_terminal_accounting_survives_without_optional_packets(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    use obzenflow_core::event::provenance::ExecutionAccounting;
    use obzenflow_core::event::{StageLifecycleEvent, SystemPayload};
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "filtered");
    let (mut ctx, system, exports) = context(&mut *factory, vec![(stage, data)], vec![]).await;
    let writer = SystemId::new();
    ctx.pipeline_writer = Some(writer.into());
    system
        .append(
            SystemEvent::new(
                stage.into(),
                SystemPayload::StageLifecycle {
                    stage_id: stage,
                    event: StageLifecycleEvent::Completed {
                        accounting: Some(ExecutionAccounting {
                            events_processed_total: 1000,
                            events_emitted_total: 0,
                            ..Default::default()
                        }),
                    },
                },
            ),
            Default::default(),
        )
        .await
        .unwrap();
    // A later lifecycle without counters cannot discard the accounting slot.
    system
        .append(
            SystemEvent::new(
                stage.into(),
                SystemPayload::StageLifecycle {
                    stage_id: stage,
                    event: StageLifecycleEvent::Drained,
                },
            ),
            Default::default(),
        )
        .await
        .unwrap();
    system
        .append(
            SystemEventFactory::new(writer).pipeline_not_started(),
            Default::default(),
        )
        .await
        .unwrap();
    system
        .append(
            SystemEventFactory::new(writer).pipeline_drained(),
            Default::default(),
        )
        .await
        .unwrap();
    let (task, _control) = run(ctx);
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    let snapshots = exports.0.lock().unwrap();
    let final_snapshot = snapshots.last().unwrap();
    assert_eq!(final_snapshot.event_counts[&stage], 1000);
    assert_eq!(final_snapshot.pipeline_state, "not_started");
}

pub async fn metrics_exports_do_not_create_observations(mut factory: Box<dyn FlowJournalFactory>) {
    let (mut ctx, _, exports) = context(&mut *factory, vec![], vec![]).await;
    let before = serde_json::to_value(ctx.metrics_store.observations.snapshot()).unwrap();
    for _ in 0..3 {
        Action::ExportMetrics.execute(&mut ctx).await.unwrap();
    }
    assert_eq!(
        before,
        serde_json::to_value(ctx.metrics_store.observations.snapshot()).unwrap()
    );
    assert_eq!(exports.0.lock().unwrap().len(), 3);
}

pub async fn metrics_optional_measurements_do_not_invent_missing_accounting(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let stage = StageId::new();
    let data = stage_journal(&mut *factory, stage, "measurements-only");
    let mut event = fact(stage, stage.into(), 1, 7);
    event.runtime = None;
    data.append(event, Default::default()).await.unwrap();
    let (mut ctx, _, exports) = context(&mut *factory, vec![(stage, data.clone())], vec![]).await;
    refresh(&mut ctx).await;
    {
        let snapshots = exports.0.lock().unwrap();
        let snapshot = snapshots.last().unwrap();
        assert_eq!(snapshot.in_flight[&stage], 7.0);
        assert!(!snapshot.event_counts.contains_key(&stage));
        assert!(!snapshot.error_counts.contains_key(&stage));
        assert!(!snapshot.events_emitted_total.contains_key(&stage));
    }
    data.append(fact(stage, stage.into(), 2, 5), Default::default())
        .await
        .unwrap();
    refresh(&mut ctx).await;
    let snapshots = exports.0.lock().unwrap();
    let snapshot = snapshots.last().unwrap();
    assert_eq!(snapshot.event_counts[&stage], 2);
    assert_eq!(
        snapshot
            .flow_metrics
            .as_ref()
            .unwrap()
            .total_events_processed,
        2
    );
}

pub async fn metrics_manual_export_uses_the_live_control_receiver(
    mut factory: Box<dyn FlowJournalFactory>,
) {
    let (mut ctx, _, exports) = context(&mut *factory, vec![], vec![]).await;
    ctx.export_interval = Duration::from_secs(30);
    let (task, control) = run(ctx);
    until(|| exports.0.lock().unwrap().len() == 1).await;
    control.send(Event::ExportMetrics).await.unwrap();
    until(|| exports.0.lock().unwrap().len() == 2).await;
    task.abort();
    let _ = task.await;
}
