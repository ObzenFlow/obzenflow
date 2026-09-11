// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    build_pipeline_fsm_with_initial, FlowStopMode, PipelineAction, PipelineContext,
    PipelineControl, PipelineFsmEvent, PipelineFsmState, PipelineState,
};
use async_trait::async_trait;
use obzenflow_core::event::{
    ChainEvent, JournalEvent, JournalWriterId, MetricsCoordinationEvent, SystemEvent,
    SystemEventFactory, SystemEventType,
};
use obzenflow_core::id::{FlowId, JournalId, SystemId};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::{AppMetricsSnapshot, InfraMetricsSnapshot, MetricsSnapshotExporter};
#[derive(Default)]
struct RecordingSnapshots(std::sync::Mutex<Vec<AppMetricsSnapshot>>);
impl MetricsSnapshotExporter for RecordingSnapshots {
    fn publish_app_snapshot(&self, value: AppMetricsSnapshot) {
        self.0.lock().unwrap().push(value);
    }
    fn publish_infra_snapshot(&self, _value: InfraMetricsSnapshot) {}
}
use crate::supervised_base::SupervisorHandle;
use obzenflow_core::{EventEnvelope, StageId};
use obzenflow_fsm::FsmAction;
use obzenflow_topology::TopologyBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn repeated_graceful_controls_have_no_actions_and_cancel_folds_once() {
    use std::time::Duration;
    for (first, second) in [(1, 60), (60, 1)] {
        let mut context = make_fsm_context();
        context.flow_start_time = Some(std::time::Instant::now());
        let mut fsm = build_pipeline_fsm_with_initial(PipelineFsmState::Running);
        let event = |seconds| {
            PipelineFsmEvent::Control(PipelineControl::Stop {
                mode: FlowStopMode::Graceful {
                    timeout: Duration::from_secs(seconds),
                },
            })
        };
        let initial = fsm.handle(event(first), &mut context).await.unwrap();
        assert!(matches!(
            initial.as_slice(),
            [PipelineAction::Publish { control: true, .. }]
        ));
        let deadline = context.stop_intent.deadline;
        for _ in 0..20 {
            assert!(fsm
                .handle(event(second), &mut context)
                .await
                .unwrap()
                .is_empty());
            assert_eq!(context.stop_intent.deadline, deadline);
        }
        let cancel = PipelineFsmEvent::Control(PipelineControl::Stop {
            mode: FlowStopMode::Cancel,
        });
        let admitted = fsm.handle(cancel.clone(), &mut context).await.unwrap();
        assert!(matches!(
            admitted.first(),
            Some(PipelineAction::CancelStages { .. })
        ));
        assert_eq!(
            admitted
                .iter()
                .filter(|a| matches!(a, PipelineAction::Publish { control: true, .. }))
                .count(),
            1
        );
        assert!(fsm.handle(cancel, &mut context).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn expired_stop_is_dispatched_before_a_full_external_control_queue() {
    use crate::pipeline::supervisor::PipelineSupervisor;
    use crate::supervised_base::{ChannelBuilder, SelfSupervised};
    let mut context = make_fsm_context();
    context.stop_intent.apply_request(
        FlowStopMode::Graceful {
            timeout: std::time::Duration::ZERO,
        },
        None,
    );
    let (sender, receiver, watcher) = ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
        .with_event_buffer(32)
        .build(PipelineState::Draining);
    for _ in 0..32 {
        sender
            .send(PipelineFsmEvent::Control(PipelineControl::Start))
            .await
            .unwrap();
    }
    let mut supervisor = PipelineSupervisor::new(
        context.system_id,
        receiver,
        watcher,
        context.resources.failure.clone(),
    );
    let directive = supervisor
        .dispatch_state(&PipelineFsmState::Draining, &mut context)
        .await
        .unwrap();
    assert!(matches!(
        directive,
        crate::supervised_base::EventLoopDirective::Transition(PipelineFsmEvent::Deadline(
            super::PipelineDeadline::GracefulStop
        ))
    ));
}

#[tokio::test]
async fn repeated_abort_controls_preserve_the_first_failure_without_new_work() {
    let mut ctx = make_fsm_context();
    let mut machine = build_pipeline_fsm_with_initial(PipelineFsmState::Running);
    let first = machine
        .handle(
            PipelineFsmEvent::Control(PipelineControl::Abort {
                reason: "first failure".into(),
            }),
            &mut ctx,
        )
        .await
        .unwrap();
    assert!(!first.is_empty());
    for _ in 0..128 {
        assert!(machine
            .handle(
                PipelineFsmEvent::Control(PipelineControl::Abort {
                    reason: "later request".into(),
                }),
                &mut ctx
            )
            .await
            .unwrap()
            .is_empty());
    }
    assert_eq!(
        ctx.termination.failure.as_ref().unwrap().reason,
        "Force abort: first failure"
    );
    assert!(matches!(machine.state(), PipelineFsmState::SettlingStages));
}

#[tokio::test]
async fn final_marker_coalesces_late_controls_without_restarting_finalisation() {
    use crate::pipeline::supervisor::PipelineSupervisor;
    use crate::supervised_base::{ChannelBuilder, EventLoopDirective, SelfSupervised};
    let mut ctx = make_fsm_context();
    ctx.stop_intent.apply_request(
        FlowStopMode::Graceful {
            timeout: std::time::Duration::ZERO,
        },
        None,
    );
    let original_deadline = ctx.stop_intent.deadline;
    let mut machine = build_pipeline_fsm_with_initial(PipelineFsmState::PublishingFinalMarker);
    let (sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Draining);
    let mut supervisor = PipelineSupervisor::new(
        ctx.system_id,
        receiver,
        watcher,
        ctx.resources.failure.clone(),
    );
    for control in [
        PipelineControl::Start,
        PipelineControl::Stop {
            mode: FlowStopMode::Graceful {
                timeout: std::time::Duration::from_secs(1),
            },
        },
        PipelineControl::Stop {
            mode: FlowStopMode::Cancel,
        },
        PipelineControl::Abort {
            reason: "too late to change execution".into(),
        },
    ] {
        sender
            .send(PipelineFsmEvent::Control(control))
            .await
            .unwrap();
        let directive = supervisor
            .dispatch_state(machine.state(), &mut ctx)
            .await
            .unwrap();
        let EventLoopDirective::Transition(event @ PipelineFsmEvent::Control(_)) = directive else {
            panic!("a settled execution's old stop deadline must not pre-empt its final append");
        };
        assert!(machine.handle(event, &mut ctx).await.unwrap().is_empty());
        assert!(matches!(
            machine.state(),
            PipelineFsmState::PublishingFinalMarker
        ));
    }
    assert_eq!(ctx.stop_intent.deadline, original_deadline);
    assert!(ctx.termination.failure.is_none());
    let marker = SystemEventFactory::new(ctx.system_id).pipeline_drained();
    ctx.progress.final_marker = Some(marker.id);
    let envelope = ctx.system_journal.append(marker, None).await.unwrap();
    machine
        .handle(PipelineFsmEvent::Journal(Box::new(envelope)), &mut ctx)
        .await
        .unwrap();
    let EventLoopDirective::Transition(event @ PipelineFsmEvent::PhysicalSettlementSatisfied) =
        supervisor
            .dispatch_state(machine.state(), &mut ctx)
            .await
            .unwrap()
    else {
        panic!("the original final marker should now permit completion");
    };
    assert!(machine.handle(event, &mut ctx).await.unwrap().is_empty());
    assert!(matches!(machine.state(), PipelineFsmState::Finished { .. }));
    assert_eq!(
        ctx.system_journal.read_all_unordered().await.unwrap().len(),
        1
    );
}

/// Minimal in-memory journal with a live reader (sees newly appended events).
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

fn make_topology() -> Arc<obzenflow_topology::Topology> {
    let mut builder = TopologyBuilder::new();
    builder.add_stage(Some("stage1".to_string()));
    builder.add_stage(Some("stage2".to_string()));
    Arc::new(builder.build_unchecked().expect("build topology"))
}

fn make_context(
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

fn make_fsm_context() -> PipelineContext {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    make_context(system_id, system_journal, Vec::new(), None)
}

#[tokio::test]
async fn readiness_and_start_consume_committed_pipeline_facts() {
    let mut ctx = make_fsm_context();
    let mut fsm = build_pipeline_fsm_with_initial(PipelineFsmState::AwaitingStageReadiness);
    assert!(fsm
        .handle(PipelineFsmEvent::PhysicalSettlementSatisfied, &mut ctx)
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        fsm.state(),
        PipelineFsmState::AwaitingStageReadiness
    ));
    ctx.progress.ready_announced = true;
    let event = SystemEventFactory::new(ctx.system_id).pipeline_ready_for_run(None);
    let envelope = ctx.system_journal.append(event, None).await.unwrap();
    fsm.handle(PipelineFsmEvent::Journal(Box::new(envelope)), &mut ctx)
        .await
        .unwrap();
    assert!(matches!(fsm.state(), PipelineFsmState::ReadyForRun));
    let actions = fsm
        .handle(PipelineFsmEvent::Control(PipelineControl::Start), &mut ctx)
        .await
        .unwrap();
    assert!(matches!(fsm.state(), PipelineFsmState::StartingSources));
    assert!(actions
        .iter()
        .all(|action| matches!(action, PipelineAction::Publish { .. })));
    assert!(!ctx.progress.sources_authorised);
    let running = SystemEventFactory::new(ctx.system_id).pipeline_running();
    let envelope = ctx.system_journal.append(running, None).await.unwrap();
    let actions = fsm
        .handle(PipelineFsmEvent::Journal(Box::new(envelope)), &mut ctx)
        .await
        .unwrap();
    assert!(matches!(actions.as_slice(), [PipelineAction::StartSources]));
}

#[tokio::test]
async fn pre_ready_and_duplicate_start_controls_do_not_authorise_sources() {
    for initial in [
        PipelineFsmState::Created,
        PipelineFsmState::Materializing,
        PipelineFsmState::AwaitingStageReadiness,
        PipelineFsmState::StartingSources,
        PipelineFsmState::Running,
        PipelineFsmState::SourceCompleted,
        PipelineFsmState::Draining,
        PipelineFsmState::SettlingStages,
        PipelineFsmState::CatchingUpProducers,
        PipelineFsmState::PublishingTerminal,
        PipelineFsmState::FinalisingMetrics,
        PipelineFsmState::PublishingFinalMarker,
    ] {
        let mut ctx = make_fsm_context();
        let mut fsm = build_pipeline_fsm_with_initial(initial.clone());
        assert!(fsm
            .handle(PipelineFsmEvent::Control(PipelineControl::Start), &mut ctx)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(fsm.state(), &initial);
    }
}

#[tokio::test]
async fn readiness_failure_and_cancel_stay_pending_until_settlement() {
    for failure in [false, true] {
        let mut ctx = make_fsm_context();
        let mut fsm = build_pipeline_fsm_with_initial(PipelineFsmState::ReadyForRun);
        let event = if failure {
            PipelineFsmEvent::OperationalFailure {
                message: "readiness fault".into(),
            }
        } else {
            PipelineFsmEvent::Control(PipelineControl::Stop {
                mode: FlowStopMode::Cancel,
            })
        };
        let actions = fsm.handle(event, &mut ctx).await.unwrap();
        assert!(matches!(fsm.state(), PipelineFsmState::SettlingStages));
        assert_eq!(fsm.state().public_state(&ctx), PipelineState::Draining);
        assert!(actions
            .iter()
            .any(|action| matches!(action, PipelineAction::ObserveStages)));
        assert!(matches!(
            actions.first(),
            Some(PipelineAction::CancelStages { .. })
        ));
        assert_eq!(ctx.termination.failure.is_some(), failure);
    }
}

#[tokio::test]
async fn every_private_phase_has_a_truthful_public_projection() {
    use crate::pipeline::termination::{ExecutionFailure, ExecutionOutcome};
    let mut ctx = make_fsm_context();
    let cases = [
        (PipelineFsmState::Created, PipelineState::Created),
        (
            PipelineFsmState::Materializing,
            PipelineState::Materializing,
        ),
        (
            PipelineFsmState::AwaitingStageReadiness,
            PipelineState::Materialized,
        ),
        (PipelineFsmState::ReadyForRun, PipelineState::ReadyForRun),
        (
            PipelineFsmState::StartingSources,
            PipelineState::ReadyForRun,
        ),
        (PipelineFsmState::Running, PipelineState::Running),
        (
            PipelineFsmState::SourceCompleted,
            PipelineState::SourceCompleted,
        ),
    ];
    for (state, projection) in cases {
        assert_eq!(state.public_state(&ctx), projection);
    }
    for state in [
        PipelineFsmState::Draining,
        PipelineFsmState::SettlingStages,
        PipelineFsmState::CatchingUpProducers,
        PipelineFsmState::PublishingTerminal,
        PipelineFsmState::FinalisingMetrics,
        PipelineFsmState::PublishingFinalMarker,
    ] {
        ctx.progress.abort_cause = None;
        assert_eq!(state.public_state(&ctx), PipelineState::Draining);
        ctx.progress.abort_cause = Some((
            obzenflow_core::event::types::ViolationCause::Other("contract".into()),
            None,
        ));
        assert!(matches!(
            state.public_state(&ctx),
            PipelineState::AbortRequested { .. }
        ));
        assert!(!state.public_state(&ctx).is_terminal());
    }
    for outcome in [
        ExecutionOutcome::Completed,
        ExecutionOutcome::Cancelled {
            reason: "user_stop".into(),
        },
        ExecutionOutcome::NotStarted,
    ] {
        assert_eq!(
            PipelineFsmState::Finished { outcome }.public_state(&ctx),
            PipelineState::Drained
        );
    }
    let failed = PipelineFsmState::Finished {
        outcome: ExecutionOutcome::Failed(ExecutionFailure {
            reason: "failed".into(),
            cause: None,
        }),
    };
    assert!(matches!(
        failed.public_state(&ctx),
        PipelineState::Failed { .. }
    ));
}

#[tokio::test]
async fn metrics_preparation_is_passive_and_cancellation_prevents_late_installation() {
    let mut ctx = make_fsm_context();
    ctx.metrics_exporter = Some(Arc::new(RecordingSnapshots::default()));
    let prepared = crate::pipeline::builder::prepare_metrics(&ctx)
        .await
        .unwrap()
        .unwrap();
    assert!(ctx
        .system_journal
        .read_all_unordered()
        .await
        .unwrap()
        .is_empty());
    ctx.resources.metrics.request_abort();
    assert!(ctx.resources.metrics.start(prepared).is_err());
    assert!(ctx.resources.metrics.handle().is_none());
    assert!(ctx
        .system_journal
        .read_all_unordered()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn original_terminal_acknowledgement_expires_metrics_before_delayed_journal_consumption() {
    use crate::metrics::{MetricsAggregatorEvent, MetricsAggregatorState};
    use crate::pipeline::supervisor::PipelineSupervisor;
    use crate::supervised_base::{ChannelBuilder, HandleBuilder, SelfSupervised};
    let mut ctx = make_fsm_context();
    ctx.metrics_drain_timeout_ms = 1;
    let (sender, _receiver, watcher) =
        ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
            .build(MetricsAggregatorState::Running);
    let task = tokio::spawn(std::future::pending::<
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    >());
    let metrics = HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap();
    ctx.resources.metrics.install_for_test(metrics);
    let ack = std::time::Instant::now() - std::time::Duration::from_secs(1);
    ctx.resources.terminal_ack.set(ack).unwrap();
    let (_sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Draining);
    let mut supervisor = PipelineSupervisor::new(
        ctx.system_id,
        receiver,
        watcher,
        ctx.resources.failure.clone(),
    );
    assert!(matches!(
        supervisor
            .dispatch_state(&PipelineFsmState::PublishingTerminal, &mut ctx)
            .await
            .unwrap(),
        crate::supervised_base::EventLoopDirective::Transition(PipelineFsmEvent::Deadline(
            super::PipelineDeadline::Metrics
        ))
    ));
    assert_eq!(ctx.resources.terminal_ack.get(), Some(&ack));
    ctx.resources.metrics.abort_and_join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_metrics_skips_when_metrics_not_started() {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));

    let mut ctx = make_context(
        system_id,
        system_journal.clone(),
        Vec::new(),
        Some(Arc::new(RecordingSnapshots::default())),
    );

    PipelineAction::DrainMetrics
        .execute(&mut ctx)
        .await
        .unwrap();

    let events = system_journal.read_causally_ordered().await.unwrap();
    assert!(
        events.is_empty(),
        "expected no system events when DrainMetrics is gated off"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn metrics_tail_refresh_keeps_counts_current_without_advancing_input_coverage() {
    use crate::metrics::fsm::{
        MetricsAggregatorAction, MetricsAggregatorContext, MetricsJournalKind,
    };
    use crate::metrics::MetricsInputs;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{context::RuntimeContext, ChainEventFactory};

    let system_id = SystemId::new();
    let system: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let stage = StageId::new();
    let data: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage)));
    let errors: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage)));
    let mut rows = Vec::new();
    for (count, target, kind) in [
        (1, &data, MetricsJournalKind::Data),
        (2, &errors, MetricsJournalKind::Error),
    ] {
        let mut event =
            ChainEventFactory::data_event(stage.into(), "test.fact", serde_json::json!({}));
        event.flow_context.stage_id = stage;
        event = event
            .with_runtime_context(RuntimeContext {
                events_processed_total: count,
                errors_total: count,
                errors_by_kind: HashMap::from([(ErrorKind::Unknown, count)]),
                ..crate::metrics::instrumentation::StageInstrumentation::new()
                    .snapshot_with_control()
            })
            .mark_as_error("expected", ErrorKind::Unknown);
        rows.push((kind, target.append(event, None).await.unwrap()));
    }
    let exporter = Arc::new(RecordingSnapshots::default());
    let (mut context, mut io) = MetricsAggregatorContext::new(
        MetricsInputs::new(vec![(stage, data)], vec![(stage, errors)]),
        system.clone(),
        exporter.clone(),
        1,
        system_id,
        HashMap::new(),
        Vec::new(),
    )
    .await
    .unwrap();

    MetricsAggregatorAction::ExportMetrics
        .execute(&mut context)
        .await
        .unwrap();
    {
        let snapshots = exporter.0.lock().unwrap();
        let snapshot = snapshots.last().unwrap();
        assert_eq!(snapshot.event_counts[&stage], 2);
        assert_eq!(snapshot.error_counts[&stage], 2);
        assert_eq!(
            snapshot.error_counts_by_kind[&stage][&ErrorKind::Unknown],
            2
        );
        assert!(snapshot.stage_vector_clocks.is_empty());
    }
    assert!(context.metrics_store.last_event_id.is_none());
    assert!(matches!(
        io.data_subscription.poll_next().await,
        crate::messaging::PollResult::Event(row) if row.event.id == rows[0].1.event.id
    ));
    assert!(!system
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .any(|row| {
            matches!(
                row.event.event,
                SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
            )
        }));

    // Catch-up can revisit older cumulative snapshots and error-marked rows.
    // Neither observation order nor repeated folding may inflate the counts.
    for _ in 0..2 {
        for (kind, row) in &rows {
            MetricsAggregatorAction::UpdateMetrics {
                envelope: Box::new(row.clone()),
                journal_kind: *kind,
                journal_stage: stage,
            }
            .execute(&mut context)
            .await
            .unwrap();
        }
    }
    let metrics = &context.metrics_store.stage_metrics[&stage];
    assert_eq!(metrics.latest_errors_total, Some(2));
    assert_eq!(metrics.errors_by_kind[&ErrorKind::Unknown], 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn late_metrics_bootstrap_reads_all_physical_inputs_without_stage_eof() {
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{context::RuntimeContext, ChainEventFactory};
    let system_id = SystemId::new();
    let journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let data_stage = StageId::new();
    let error_stage = StageId::new();
    let data: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(data_stage)));
    let errors: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(error_stage)));
    for (stage, rows, target, failed) in [
        (data_stage, 50, &data, false),
        (error_stage, 7, &errors, true),
    ] {
        for count in 1..=rows {
            let mut event = ChainEventFactory::data_event(
                stage.into(),
                "test.fact",
                serde_json::json!({"n":count}),
            );
            event.flow_context.stage_id = stage;
            event = event.with_runtime_context(RuntimeContext {
                events_emitted_total: count,
                errors_total: if failed { count } else { 0 },
                errors_by_kind: if failed {
                    HashMap::from([(ErrorKind::Unknown, count)])
                } else {
                    HashMap::new()
                },
                ..crate::metrics::instrumentation::StageInstrumentation::new()
                    .snapshot_with_control()
            });
            if failed {
                event = event.mark_as_error("expected", ErrorKind::Unknown);
            }
            target.append(event, None).await.unwrap();
        }
    }
    // The observer starts after publication, with no stage terminal or EOF.
    // Another pipeline writer and a later same-writer fact cannot extend its
    // system endpoint beyond the selected current-writer terminal.
    journal
        .append(
            SystemEventFactory::new(SystemId::new()).pipeline_not_started(),
            None,
        )
        .await
        .unwrap();
    let terminal = SystemEventFactory::new(system_id).pipeline_cancelled(
        "test".into(),
        obzenflow_core::event::types::DurationMs(0),
        None,
        None,
    );
    journal.append(terminal, None).await.unwrap();
    journal
        .append(
            SystemEventFactory::new(system_id).pipeline_failed(
                "outside fixed endpoint".into(),
                obzenflow_core::event::types::DurationMs(0),
                None,
                None,
            ),
            None,
        )
        .await
        .unwrap();
    let exporter = Arc::new(RecordingSnapshots::default());
    let mut ctx = make_context(
        system_id,
        journal.clone(),
        vec![(data_stage, data)],
        Some(exporter.clone()),
    );
    ctx.stage_error_journals.push((error_stage, errors));
    ctx.resources.prepared_metrics = crate::pipeline::builder::prepare_metrics(&ctx)
        .await
        .unwrap();
    PipelineAction::StartMetricsAggregator
        .execute(&mut ctx)
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .unwrap()
            .wait_for_completion(),
    )
    .await
    .unwrap()
    .unwrap();
    {
        let snapshots = exporter.0.lock().unwrap();
        let snapshot = snapshots.last().unwrap();
        assert_eq!(snapshot.pipeline_state, "cancelled");
        assert_eq!(snapshot.events_emitted_total[&data_stage], 50);
        assert_eq!(snapshot.events_emitted_total[&error_stage], 7);
        assert_eq!(snapshot.error_counts[&error_stage], 7);
        assert_eq!(
            snapshot.error_counts_by_kind[&error_stage][&ErrorKind::Unknown],
            7
        );
    }
    assert!(journal
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .any(|row| row.event.event_type_name() == "system.metrics.drained"));
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_cleanup_keeps_metrics_alive_until_the_terminal_fact() {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));

    let stage_id = StageId::new();
    let stage_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage_id)));

    let mut ctx = make_context(
        system_id,
        system_journal.clone(),
        vec![(stage_id, stage_journal)],
        Some(Arc::new(RecordingSnapshots::default())),
    );

    ctx.resources.prepared_metrics = crate::pipeline::builder::prepare_metrics(&ctx)
        .await
        .unwrap();
    PipelineAction::StartMetricsAggregator
        .execute(&mut ctx)
        .await
        .unwrap();
    assert!(
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .map(|h| h.is_running())
            .unwrap_or(false),
        "expected metrics handle to be stored and running"
    );

    PipelineAction::CancelStages {
        contract_abort: false,
    }
    .execute(&mut ctx)
    .await
    .unwrap();

    for _ in 0..128 {
        PipelineAction::DrainMetrics
            .execute(&mut ctx)
            .await
            .unwrap();
    }
    ctx.resources.publications.observe_accepted().await.unwrap();

    assert!(
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .unwrap()
            .is_running(),
        "stage cleanup must retain metrics for terminal catch-up"
    );

    let events = system_journal.read_causally_ordered().await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|envelope| matches!(
                &envelope.event.event,
                SystemEventType::MetricsCoordination(MetricsCoordinationEvent::DrainRequested)
            ))
            .count(),
        1,
        "repeated failure cleanup must retain one drain admission"
    );
    assert!(!events.iter().any(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
        )
    }));
    system_journal
        .append(
            SystemEventFactory::new(system_id).pipeline_not_started(),
            None,
        )
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .unwrap()
            .wait_for_completion(),
    )
    .await
    .unwrap()
    .unwrap();
    let events = system_journal.read_causally_ordered().await.unwrap();
    assert!(events.iter().any(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
        )
    }));
}
