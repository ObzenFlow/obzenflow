// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The approved state/input contract, including invalid internal inputs and
//! journal observations that must remain valid across lifecycle boundaries.

use super::support::{
    empty_topology, make_fsm_context, source_sink_topology_with_source, test_context,
    MemoryJournal, TestPipelineStageHandle,
};
use crate::feed_plan::{FeedKey, FeedRole};
use crate::pipeline::fsm::{
    build_pipeline_fsm_with_initial, PipelineAction as A, PipelineDeadline, PipelineFsmEvent as E,
    PipelineFsmState as S,
};
use crate::pipeline::resources::{ProducerTail, StageCommand};
use crate::pipeline::termination::{ExecutionOutcome, PublishedTermination};
use crate::pipeline::FlowStopMode;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::system_event::SystemFeedRole;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{
    PipelineLifecycleEvent, SystemEvent, SystemEventFactory, SystemEventType,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::{StageId, SystemId};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn live_phases() -> [S; 13] {
    [
        S::Created,
        S::Materializing,
        S::AwaitingStageReadiness,
        S::ReadyForRun,
        S::StartingSources,
        S::Running,
        S::SourceCompleted,
        S::Draining,
        S::SettlingStages,
        S::CatchingUpProducers,
        S::PublishingTerminal,
        S::FinalisingMetrics,
        S::PublishingFinalMarker,
    ]
}

#[tokio::test]
async fn internal_input_admission_is_phase_specific_even_with_satisfied_guards() {
    for phase in live_phases() {
        for event in [
            E::Bootstrap,
            E::PhysicalSettlementSatisfied,
            E::GracefulStopExpired,
            E::StageCleanupExpired,
        ] {
            let mut ctx = make_fsm_context();
            ctx.stop_intent.apply_request(
                FlowStopMode::Graceful {
                    timeout: Duration::ZERO,
                },
                None,
            );
            ctx.progress.cleanup_deadline = Some(Instant::now());
            ctx.resources.stages_joined = true;
            ctx.resources.producer_tail = ProducerTail::Reached;
            ctx.resources.metrics_joined = true;
            ctx.progress.metrics_cancelled = true;
            ctx.progress.final_marker_seen = true;
            let permitted = match event {
                E::Bootstrap => matches!(phase, S::Created),
                E::PhysicalSettlementSatisfied => matches!(
                    phase,
                    S::Materializing
                        | S::SettlingStages
                        | S::CatchingUpProducers
                        | S::FinalisingMetrics
                        | S::PublishingFinalMarker
                ),
                E::GracefulStopExpired => matches!(
                    phase,
                    S::Draining
                        | S::SettlingStages
                        | S::CatchingUpProducers
                        | S::PublishingTerminal
                        | S::FinalisingMetrics
                ),
                E::StageCleanupExpired => matches!(phase, S::SettlingStages),
                _ => unreachable!(),
            };
            let mut machine = build_pipeline_fsm_with_initial(phase.clone());
            let result = machine.handle(event.clone(), &mut ctx).await;
            assert_eq!(
                result.is_ok(),
                permitted,
                "{phase:?} / {event:?}: {result:?}"
            );
            if !permitted {
                assert_eq!(machine.state(), &phase);
            }
        }
    }
}

#[tokio::test]
async fn settlement_in_an_eligible_phase_still_requires_its_resource_predicate() {
    for phase in [
        S::Materializing,
        S::SettlingStages,
        S::CatchingUpProducers,
        S::FinalisingMetrics,
        S::PublishingFinalMarker,
    ] {
        let mut ctx = make_fsm_context();
        if matches!(phase, S::Materializing) {
            ctx.resources
                .delivery
                .enqueue(
                    vec![TestPipelineStageHandle::boxed(
                        StageId::new(),
                        "pending",
                        StageType::Sink,
                    )],
                    &[StageCommand::Initialize],
                    1,
                )
                .unwrap();
        }
        let mut machine = build_pipeline_fsm_with_initial(phase.clone());
        assert!(machine
            .handle(E::PhysicalSettlementSatisfied, &mut ctx)
            .await
            .is_err());
        assert_eq!(machine.state(), &phase);
    }
}

#[tokio::test]
async fn execution_deadlines_require_admission_and_expiry_without_mutating_on_rejection() {
    for future in [false, true] {
        let mut ctx = make_fsm_context();
        if future {
            ctx.stop_intent.apply_request(
                FlowStopMode::Graceful {
                    timeout: Duration::from_secs(60),
                },
                None,
            );
            ctx.progress.cleanup_deadline = Some(Instant::now() + Duration::from_secs(60));
        }
        let stop_deadline = ctx.stop_intent.deadline;
        let cleanup_deadline = ctx.progress.cleanup_deadline;
        let mut machine = build_pipeline_fsm_with_initial(S::SettlingStages);
        for event in [E::GracefulStopExpired, E::StageCleanupExpired] {
            assert!(machine.handle(event, &mut ctx).await.is_err());
            assert_eq!(ctx.stop_intent.deadline, stop_deadline);
            assert_eq!(ctx.progress.cleanup_deadline, cleanup_deadline);
            assert!(!ctx.progress.stages_cancelled);
        }
    }
}

#[tokio::test]
async fn metrics_deadline_admission_and_guards_use_the_actual_acknowledgement() {
    use crate::metrics::{MetricsAggregatorEvent, MetricsAggregatorState};
    use crate::supervised_base::{ChannelBuilder, HandleBuilder};
    let mut ctx = make_fsm_context();
    for phase in [S::PublishingTerminal, S::FinalisingMetrics] {
        let mut machine = build_pipeline_fsm_with_initial(phase);
        assert!(machine.handle(E::MetricsExpired, &mut ctx).await.is_err());
    }
    let (sender, _receiver, watcher) =
        ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
            .build(MetricsAggregatorState::Running);
    let task = tokio::spawn(std::future::pending::<
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    >());
    let handle = HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap();
    ctx.resources.metrics.install_for_test(handle);
    let ack = Instant::now();
    ctx.resources.terminal_ack.set(ack).unwrap();
    ctx.metrics_drain_timeout_ms = 60_000;
    for phase in [S::PublishingTerminal, S::FinalisingMetrics] {
        let mut machine = build_pipeline_fsm_with_initial(phase);
        assert!(machine.handle(E::MetricsExpired, &mut ctx).await.is_err());
    }
    ctx.metrics_drain_timeout_ms = 0;
    for phase in live_phases() {
        let permitted = matches!(phase, S::PublishingTerminal | S::FinalisingMetrics);
        assert_eq!(
            phase.deadline_at(&ctx, PipelineDeadline::Metrics).is_some(),
            permitted
        );
        let mut machine = build_pipeline_fsm_with_initial(phase.clone());
        let result = machine.handle(E::MetricsExpired, &mut ctx).await;
        assert_eq!(result.is_ok(), permitted, "{phase:?}");
        if permitted {
            assert!(matches!(
                result.unwrap().as_slice(),
                [A::CancelMetrics, A::ObserveMetrics]
            ));
        }
        assert_eq!(ctx.resources.terminal_ack.get(), Some(&ack));
    }
    ctx.resources.metrics.abort_and_join().await.unwrap();
}

#[tokio::test]
async fn finished_has_no_outgoing_inputs_including_controls_and_journal_rows() {
    let mut ctx = make_fsm_context();
    let mut machine = build_pipeline_fsm_with_initial(S::PublishingFinalMarker);
    ctx.progress.final_marker_seen = true;
    machine
        .handle(E::PhysicalSettlementSatisfied, &mut ctx)
        .await
        .unwrap();
    let finished = machine.state().clone();
    assert!(matches!(finished, S::Finished { .. }));
    let row = ctx
        .system_journal
        .append(SystemEvent::stage_running(StageId::new()), None)
        .await
        .unwrap();
    for event in [
        E::Bootstrap,
        E::Start,
        E::GracefulStop {
            timeout: Duration::ZERO,
        },
        E::Cancel,
        E::Abort {
            reason: "late".into(),
        },
        E::GracefulStopExpired,
        E::StageCleanupExpired,
        E::MetricsExpired,
        E::PhysicalSettlementSatisfied,
        E::OperationalFailure {
            message: "late".into(),
        },
        E::Journal(Box::new(row)),
    ] {
        assert!(
            machine.handle(event.clone(), &mut ctx).await.is_err(),
            "{event:?}"
        );
        assert_eq!(machine.state(), &finished);
    }
}

#[tokio::test]
async fn controls_and_operational_failure_follow_the_approved_successor_matrix() {
    for phase in live_phases() {
        for event in [
            E::Start,
            E::GracefulStop {
                timeout: Duration::from_secs(60),
            },
            E::Cancel,
            E::Abort {
                reason: "abort".into(),
            },
            E::OperationalFailure {
                message: "failure".into(),
            },
        ] {
            let mut ctx = make_fsm_context();
            if !matches!(
                phase,
                S::Created | S::Materializing | S::AwaitingStageReadiness | S::ReadyForRun
            ) {
                ctx.flow_start_time = Some(Instant::now());
            }
            let expected = match event {
                E::Start if phase == S::ReadyForRun => S::StartingSources,
                E::Start => phase.clone(),
                E::OperationalFailure { .. } | E::Abort { .. } => match phase {
                    S::PublishingFinalMarker if matches!(event, E::Abort { .. }) => phase.clone(),
                    S::CatchingUpProducers => S::CatchingUpProducers,
                    S::PublishingTerminal | S::FinalisingMetrics | S::PublishingFinalMarker => {
                        S::FinalisingMetrics
                    }
                    _ => S::SettlingStages,
                },
                _ if matches!(
                    phase,
                    S::SettlingStages
                        | S::CatchingUpProducers
                        | S::PublishingTerminal
                        | S::FinalisingMetrics
                        | S::PublishingFinalMarker
                ) =>
                {
                    phase.clone()
                }
                E::GracefulStop { .. } if ctx.flow_start_time.is_some() => S::Draining,
                _ => S::SettlingStages,
            };
            let mut machine = build_pipeline_fsm_with_initial(phase.clone());
            machine.handle(event.clone(), &mut ctx).await.unwrap();
            assert_eq!(machine.state(), &expected, "{phase:?} / {event:?}");
        }
    }
}

fn contract_row(
    upstream: StageId,
    reader: StageId,
    selected: Option<&str>,
    role: Option<SystemFeedRole>,
    pass: bool,
) -> SystemEvent {
    SystemEvent::new(
        reader.into(),
        SystemEventType::ContractStatus {
            upstream,
            reader,
            selected_event_type: selected.map(Into::into),
            feed_role: role,
            pass,
            reader_seq: Some(SeqNo(1)),
            advertised_writer_seq: Some(SeqNo(1)),
            reason: None,
        },
    )
}

#[tokio::test]
async fn unrelated_rows_only_advance_observation_in_every_live_phase() {
    for phase in live_phases() {
        let mut ctx = make_fsm_context();
        let upstream = StageId::new();
        let reader = StageId::new();
        let mut machine = build_pipeline_fsm_with_initial(phase.clone());
        for event in [
            SystemEvent::stage_running(upstream),
            SystemEvent::stage_completed(upstream),
            SystemEvent::stage_failed(upstream, "unrelated".into(), false),
            contract_row(upstream, reader, None, None, false),
            SystemEventFactory::new(SystemId::new()).pipeline_all_stages_completed(),
        ] {
            let id = event.id;
            ctx.resources.producer_tail = ProducerTail::Through(id);
            let row = ctx.system_journal.append(event, None).await.unwrap();
            assert!(machine
                .handle(E::Journal(Box::new(row)), &mut ctx)
                .await
                .unwrap()
                .is_empty());
            assert_eq!(machine.state(), &phase);
            assert_eq!(ctx.last_system_event_id_seen, Some(id));
            assert!(matches!(ctx.resources.producer_tail, ProducerTail::Reached));
            assert!(ctx.running_stages.is_empty() && ctx.completed_stages.is_empty());
            assert!(ctx.contract_pairs.is_empty() && ctx.termination.failure.is_none());
        }
    }
}

#[tokio::test]
async fn declared_contract_feeds_do_not_fall_back_on_unknown_payload_or_role() {
    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, upstream, reader) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, journal, None);
    let feed = FeedKey::new(upstream, reader, "payment", FeedRole::Input);
    ctx.expected_contract_pairs.insert(feed.clone());
    let mut machine = build_pipeline_fsm_with_initial(S::Running);
    for row in [
        contract_row(
            upstream,
            reader,
            Some("unrelated"),
            Some(SystemFeedRole::Input),
            false,
        ),
        contract_row(
            upstream,
            reader,
            Some("payment"),
            Some(SystemFeedRole::Reference),
            false,
        ),
        contract_row(upstream, reader, None, Some(SystemFeedRole::Stream), false),
        contract_row(reader, upstream, None, None, false),
    ] {
        let row = ctx.system_journal.append(row, None).await.unwrap();
        assert!(machine
            .handle(E::Journal(Box::new(row)), &mut ctx)
            .await
            .unwrap()
            .is_empty());
        assert!(ctx.contract_pairs.is_empty());
        assert!(ctx.termination.failure.is_none());
        assert_eq!(machine.state(), &S::Running);
    }
    let row = ctx
        .system_journal
        .append(
            contract_row(
                upstream,
                reader,
                Some("payment"),
                Some(SystemFeedRole::Input),
                false,
            ),
            None,
        )
        .await
        .unwrap();
    let actions = machine
        .handle(E::Journal(Box::new(row)), &mut ctx)
        .await
        .unwrap();
    assert_eq!(machine.state(), &S::SettlingStages);
    assert!(ctx.contract_pairs.contains_key(&feed));
    assert!(ctx.termination.failure.is_some());
    assert!(matches!(
        actions.as_slice(),
        [
            A::CancelStages {
                contract_abort: true
            },
            A::ObserveStages
        ]
    ));
}

#[tokio::test]
async fn empty_topology_cannot_announce_or_consume_all_stage_completion() {
    for phase in [
        S::Created,
        S::Materializing,
        S::AwaitingStageReadiness,
        S::Running,
    ] {
        let id = SystemId::new();
        let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(id)));
        let mut ctx = test_context(empty_topology(), id, journal, None);
        let mut machine = build_pipeline_fsm_with_initial(phase.clone());
        for event in [
            SystemEvent::stage_running(StageId::new()),
            SystemEventFactory::new(id).pipeline_all_stages_completed(),
        ] {
            let row = ctx.system_journal.append(event, None).await.unwrap();
            assert!(machine
                .handle(E::Journal(Box::new(row)), &mut ctx)
                .await
                .unwrap()
                .is_empty());
            assert_eq!(machine.state(), &phase);
            assert!(!ctx.progress.all_stages_announced);
        }
    }
}

#[tokio::test]
async fn genuine_early_stage_completion_can_settle_without_start_admission() {
    for phase in [S::Materializing, S::AwaitingStageReadiness, S::ReadyForRun] {
        let id = SystemId::new();
        let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(id)));
        let (topology, upstream, stage) = source_sink_topology_with_source();
        let mut ctx = test_context(topology, id, journal, None);
        ctx.stage_supervisors.insert(
            stage,
            TestPipelineStageHandle::boxed(stage, "sink", StageType::Sink),
        );
        ctx.stage_supervisors.insert(
            upstream,
            TestPipelineStageHandle::boxed(upstream, "completed upstream", StageType::Transform),
        );
        let mut machine = build_pipeline_fsm_with_initial(phase.clone());
        let upstream_row = ctx
            .system_journal
            .append(SystemEvent::stage_completed(upstream), None)
            .await
            .unwrap();
        assert!(machine
            .handle(E::Journal(Box::new(upstream_row)), &mut ctx)
            .await
            .unwrap()
            .is_empty());
        let row = ctx
            .system_journal
            .append(SystemEvent::stage_completed(stage), None)
            .await
            .unwrap();
        let actions = machine
            .handle(E::Journal(Box::new(row)), &mut ctx)
            .await
            .unwrap();
        let [A::Publish { event, .. }] = actions.as_slice() else {
            panic!("one completion announcement");
        };
        assert!(matches!(
            event.event,
            SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::AllStagesCompleted { .. })
        ));
        assert_eq!(machine.state(), &phase);
        assert!(ctx.flow_start_time.is_none());
        let row = ctx
            .system_journal
            .append(event.as_ref().clone(), None)
            .await
            .unwrap();
        let actions = machine
            .handle(E::Journal(Box::new(row)), &mut ctx)
            .await
            .unwrap();
        assert_eq!(machine.state(), &S::SettlingStages);
        assert!(matches!(
            actions.as_slice(),
            [A::ObserveStages, A::DrainMetrics]
        ));
    }
}

#[tokio::test]
async fn graceful_stop_during_startup_preserves_running_then_drain_authority() {
    for cancel in [false, true] {
        let mut ctx = make_fsm_context();
        let mut machine = build_pipeline_fsm_with_initial(S::ReadyForRun);
        let start = machine.handle(E::Start, &mut ctx).await.unwrap();
        let A::Publish { event: running, .. } = &start[1] else {
            panic!("running publication");
        };
        let stop = machine
            .handle(
                if cancel {
                    E::Cancel
                } else {
                    E::GracefulStop {
                        timeout: Duration::from_secs(60),
                    }
                },
                &mut ctx,
            )
            .await
            .unwrap();
        let admission = stop
            .iter()
            .find_map(|action| match action {
                A::Publish {
                    event,
                    control: true,
                } => Some(event.as_ref().clone()),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            machine.state(),
            if cancel {
                &S::SettlingStages
            } else {
                &S::Draining
            }
        );
        let row = ctx
            .system_journal
            .append(running.as_ref().clone(), None)
            .await
            .unwrap();
        let actions = machine
            .handle(E::Journal(Box::new(row)), &mut ctx)
            .await
            .unwrap();
        assert_eq!(
            actions.iter().any(|a| matches!(a, A::StartSources)),
            !cancel
        );
        let row = ctx.system_journal.append(admission, None).await.unwrap();
        let actions = machine
            .handle(E::Journal(Box::new(row)), &mut ctx)
            .await
            .unwrap();
        assert_eq!(actions.iter().any(|a| matches!(a, A::StopSources)), !cancel);
    }
}

#[tokio::test]
async fn terminal_and_final_marker_require_the_authorised_writer_and_identity() {
    let mut ctx = make_fsm_context();
    let selected = SystemEventFactory::new(ctx.system_id).pipeline_not_started();
    ctx.progress.selected_terminal = Some((selected.clone(), ExecutionOutcome::NotStarted));
    let mut machine = build_pipeline_fsm_with_initial(S::PublishingTerminal);
    let mut wrong_writer = selected.clone();
    wrong_writer.writer_id = SystemId::new().into();
    for event in [
        SystemEventFactory::new(ctx.system_id).pipeline_not_started(),
        wrong_writer,
    ] {
        let row = ctx.system_journal.append(event, None).await.unwrap();
        assert!(machine
            .handle(E::Journal(Box::new(row)), &mut ctx)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(machine.state(), &S::PublishingTerminal);
    }
    let row = ctx.system_journal.append(selected, None).await.unwrap();
    assert!(matches!(
        machine
            .handle(E::Journal(Box::new(row)), &mut ctx)
            .await
            .unwrap()
            .as_slice(),
        [A::ObserveMetrics]
    ));
    assert_eq!(machine.state(), &S::FinalisingMetrics);
    let marker = SystemEventFactory::new(ctx.system_id).pipeline_drained();
    ctx.progress.final_marker = Some(marker.id);
    let mut machine = build_pipeline_fsm_with_initial(S::PublishingFinalMarker);
    let row = ctx
        .system_journal
        .append(
            SystemEventFactory::new(ctx.system_id).pipeline_drained(),
            None,
        )
        .await
        .unwrap();
    machine
        .handle(E::Journal(Box::new(row)), &mut ctx)
        .await
        .unwrap();
    assert!(!ctx.progress.final_marker_seen);
    assert!(machine
        .handle(E::PhysicalSettlementSatisfied, &mut ctx)
        .await
        .is_err());
    let row = ctx.system_journal.append(marker, None).await.unwrap();
    machine
        .handle(E::Journal(Box::new(row)), &mut ctx)
        .await
        .unwrap();
    machine
        .handle(E::PhysicalSettlementSatisfied, &mut ctx)
        .await
        .unwrap();
    assert!(matches!(
        machine.state(),
        S::Finished {
            outcome: ExecutionOutcome::NotStarted
        }
    ));
}

#[tokio::test]
async fn final_marker_failure_finishes_with_retained_error_without_another_marker() {
    let mut ctx = make_fsm_context();
    let selected = SystemEventFactory::new(ctx.system_id).pipeline_not_started();
    ctx.progress.selected_terminal = Some((selected.clone(), ExecutionOutcome::NotStarted));
    ctx.termination
        .published
        .set(PublishedTermination {
            outcome: ExecutionOutcome::NotStarted,
            event_id: Some(selected.id),
        })
        .unwrap();
    ctx.resources.metrics_joined = true;
    ctx.resources.stages_joined = true;
    ctx.resources
        .retain_failure(Box::new(std::io::Error::other("marker append failed")));
    let mut machine = build_pipeline_fsm_with_initial(S::PublishingFinalMarker);
    machine
        .handle(
            E::OperationalFailure {
                message: "marker append failed".into(),
            },
            &mut ctx,
        )
        .await
        .unwrap();
    assert_eq!(machine.state(), &S::FinalisingMetrics);
    let actions = machine
        .handle(E::PhysicalSettlementSatisfied, &mut ctx)
        .await
        .unwrap();
    assert!(actions.is_empty());
    assert!(matches!(
        machine.state(),
        S::Finished {
            outcome: ExecutionOutcome::NotStarted
        }
    ));
    assert_eq!(
        ctx.progress.selected_terminal.as_ref().unwrap().0.id,
        selected.id
    );
    assert!(ctx.resources.failure.get().is_some());
}
