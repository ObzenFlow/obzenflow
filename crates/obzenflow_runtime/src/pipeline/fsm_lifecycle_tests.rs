// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{
    build_pipeline_fsm_with_initial, FlowStopMode, PipelineAction, PipelineContext, PipelineEvent,
    PipelineState,
};
use async_trait::async_trait;
use obzenflow_core::event::types::ViolationCause;
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
async fn repeated_raw_graceful_controls_have_no_actions_and_cancel_folds_once() {
    use std::time::Duration;
    for (first, second) in [(1, 60), (60, 1)] {
        let mut context = make_fsm_context();
        let mut fsm = build_pipeline_fsm_with_initial(PipelineState::Running);
        let event = |seconds| PipelineEvent::StopRequested {
            mode: FlowStopMode::Graceful {
                timeout: Duration::from_secs(seconds),
            },
            reason: None,
        };
        let initial = fsm.handle(event(first), &mut context).await.unwrap();
        assert_eq!(
            initial
                .iter()
                .filter(|a| matches!(a, PipelineAction::StopSources))
                .count(),
            1
        );
        let deadline = context.stop_intent.deadline;
        for _ in 0..20 {
            assert!(fsm
                .handle(event(second), &mut context)
                .await
                .unwrap()
                .is_empty());
            assert_eq!(context.stop_intent.deadline, deadline);
        }
        let cancel = PipelineEvent::StopRequested {
            mode: FlowStopMode::Cancel,
            reason: None,
        };
        let admitted = fsm.handle(cancel.clone(), &mut context).await.unwrap();
        assert_eq!(
            admitted
                .iter()
                .filter(|a| matches!(a, PipelineAction::WritePipelineStopRequested { .. }))
                .count(),
            1
        );
        assert!(fsm.handle(cancel, &mut context).await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn expired_stop_is_dispatched_before_a_full_external_control_queue() {
    use crate::pipeline::supervisor::PipelineSupervisor;
    use crate::supervised_base::{publication::PublicationScope, ChannelBuilder};
    let mut context = make_fsm_context();
    context.stop_intent.apply_request(
        FlowStopMode::Graceful {
            timeout: std::time::Duration::ZERO,
        },
        None,
    );
    let supervisor = PipelineSupervisor {
        name: "deadline_priority".into(),
        system_id: context.system_id,
        last_barrier_log: None,
        last_manual_wait_log: None,
        drain_idle_iters: 0,
    };
    let (sender, receiver, watcher) = ChannelBuilder::<PipelineEvent, PipelineState>::new()
        .with_event_buffer(32)
        .build(PipelineState::Draining);
    for _ in 0..32 {
        sender
            .send(PipelineEvent::StopRequested {
                mode: FlowStopMode::Graceful {
                    timeout: std::time::Duration::from_secs(60),
                },
                reason: None,
            })
            .await
            .unwrap();
    }
    let journal = context.system_journal.clone();
    let scope = PublicationScope::concurrent();
    scope
        .enter(crate::pipeline::driver::run(
            supervisor,
            receiver,
            watcher,
            context,
            PipelineState::Draining,
        ))
        .await
        .unwrap();
    scope.join().await.unwrap();
    use obzenflow_core::event::{
        PipelineCancellationCause, PipelineLifecycleEvent, PipelineStopAdmission,
    };
    let facts = journal.read_all_unordered().await.unwrap();
    let admissions: Vec<_> = facts
        .iter()
        .filter_map(|row| match &row.event.event {
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::StopAdmitted {
                admission,
            }) => Some(admission),
            _ => None,
        })
        .collect();
    assert_eq!(
        admissions,
        [&PipelineStopAdmission::Cancel {
            cause: PipelineCancellationCause::GracefulTimeout,
        }]
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
        metrics_handle: None,
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

#[tokio::test(flavor = "multi_thread")]
async fn materialized_readiness_complete_moves_to_ready_for_run() {
    let mut ctx = make_fsm_context();
    let mut fsm = build_pipeline_fsm_with_initial(PipelineState::Materialized);

    let actions = fsm
        .handle(PipelineEvent::StageReadinessComplete, &mut ctx)
        .await
        .expect("readiness transition should succeed");

    assert!(
        matches!(
            actions.as_slice(),
            [PipelineAction::WritePipelineReadyForRun]
        ),
        "readiness transition must publish the ReadyForRun lifecycle fact"
    );
    assert!(matches!(fsm.state(), PipelineState::ReadyForRun));
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_for_run_run_starts_sources() {
    let mut ctx = make_fsm_context();
    let mut fsm = build_pipeline_fsm_with_initial(PipelineState::ReadyForRun);

    let actions = fsm
        .handle(PipelineEvent::Run, &mut ctx)
        .await
        .expect("run transition should succeed from ReadyForRun");

    assert!(matches!(fsm.state(), PipelineState::Running));
    assert!(
        matches!(actions.as_slice(), [PipelineAction::NotifySourceStart]),
        "ReadyForRun + Run must be the only transition that starts sources"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn pre_ready_run_self_transitions_without_actions() {
    for initial_state in [
        PipelineState::Created,
        PipelineState::Materializing,
        PipelineState::Materialized,
    ] {
        let mut ctx = make_fsm_context();
        let mut fsm = build_pipeline_fsm_with_initial(initial_state.clone());

        let actions = fsm
            .handle(PipelineEvent::Run, &mut ctx)
            .await
            .expect("pre-ready Run should not panic or become unhandled");

        assert!(actions.is_empty());
        assert_eq!(fsm.state(), &initial_state);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn duplicate_run_is_idempotent_after_sources_started() {
    for initial_state in [
        PipelineState::Running,
        PipelineState::SourceCompleted,
        PipelineState::Draining,
    ] {
        let mut ctx = make_fsm_context();
        let mut fsm = build_pipeline_fsm_with_initial(initial_state.clone());

        let actions = fsm
            .handle(PipelineEvent::Run, &mut ctx)
            .await
            .expect("post-start duplicate Run should be idempotent");

        assert!(actions.is_empty());
        assert_eq!(fsm.state(), &initial_state);
    }

    let reason = ViolationCause::Other("abort_reason".to_string());
    let upstream = Some(StageId::new());
    let initial_state = PipelineState::AbortRequested {
        reason: reason.clone(),
        upstream,
    };
    let mut ctx = make_fsm_context();
    let mut fsm = build_pipeline_fsm_with_initial(initial_state.clone());

    let actions = fsm
        .handle(PipelineEvent::Run, &mut ctx)
        .await
        .expect("AbortRequested duplicate Run should be idempotent");

    assert!(actions.is_empty());
    assert_eq!(fsm.state(), &initial_state);
}

#[tokio::test(flavor = "multi_thread")]
async fn ready_for_run_error_and_stop_transition_to_failed() {
    let mut ctx = make_fsm_context();
    let mut fsm = build_pipeline_fsm_with_initial(PipelineState::ReadyForRun);

    let actions = fsm
        .handle(
            PipelineEvent::Error {
                message: "readiness fault".to_string(),
            },
            &mut ctx,
        )
        .await
        .expect("ReadyForRun + Error should transition through failure path");

    assert!(matches!(
        fsm.state(),
        PipelineState::Failed { reason, .. } if reason == "readiness fault"
    ));
    assert!(matches!(actions.as_slice(), [PipelineAction::Cleanup]));

    let mut ctx = make_fsm_context();
    let mut fsm = build_pipeline_fsm_with_initial(PipelineState::ReadyForRun);
    let actions = fsm
        .handle(
            PipelineEvent::StopRequested {
                mode: FlowStopMode::Cancel,
                reason: Some("operator_stop".to_string()),
            },
            &mut ctx,
        )
        .await
        .expect("ReadyForRun + StopRequested should transition through stop path");

    assert!(matches!(
        fsm.state(),
        PipelineState::Failed { reason, .. } if reason == "operator_stop"
    ));
    assert!(matches!(
        actions.as_slice(),
        [
            PipelineAction::WritePipelineStopRequested { .. },
            PipelineAction::DrainMetrics,
            PipelineAction::Cleanup
        ]
    ));
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
    PipelineAction::StartMetricsAggregator
        .execute(&mut ctx)
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ctx.metrics_handle.as_ref().unwrap().wait_for_completion(),
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

    PipelineAction::StartMetricsAggregator
        .execute(&mut ctx)
        .await
        .unwrap();
    assert!(
        ctx.metrics_handle
            .as_ref()
            .map(|h| h.is_running())
            .unwrap_or(false),
        "expected metrics handle to be stored and running"
    );

    PipelineAction::WritePipelineStopRequested {
        mode: FlowStopMode::Cancel,
    }
    .execute(&mut ctx)
    .await
    .unwrap();

    PipelineAction::DrainMetrics
        .execute(&mut ctx)
        .await
        .unwrap();
    PipelineAction::Cleanup.execute(&mut ctx).await.unwrap();

    assert!(
        ctx.metrics_handle.as_ref().unwrap().is_running(),
        "stage cleanup must retain metrics for terminal catch-up"
    );

    let events = system_journal.read_causally_ordered().await.unwrap();
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
        ctx.metrics_handle.as_ref().unwrap().wait_for_completion(),
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
