// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FSM admission rules, state projection, stop intent and contract matching.

use crate::feed_plan::{FeedKey, FeedRole};
use crate::pipeline::fsm::context::{record_stage_completion, StopIntent, StopRequestOutcome};
use crate::pipeline::fsm::{
    build_pipeline_fsm_with_initial, PipelineAction, PipelineFsmEvent, PipelineFsmState,
};
use crate::pipeline::tests::support::{
    make_fsm_context, source_sink_topology_with_source, test_context, MemoryJournal,
};
use crate::pipeline::{FlowStopMode, PipelineControl, PipelineState};
use crate::stages::common::stage_handle::{STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP};
use obzenflow_core::event::SystemEventFactory;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::{StageId, SystemId};
use std::sync::Arc;
use std::time::Duration;

#[test]
fn terminal_states_are_exactly_the_locked_set() {
    assert!(PipelineState::Drained.is_terminal());
    assert!(PipelineState::Failed {
        reason: "x".to_string(),
        failure_cause: None
    }
    .is_terminal());
    assert!(!PipelineState::Created.is_terminal());
    assert!(!PipelineState::Running.is_terminal());
    assert!(!PipelineState::Draining.is_terminal());
    assert!(!PipelineState::SourceCompleted.is_terminal());
}

#[test]
fn stop_intent_cancel_sets_defaults() {
    let mut intent = StopIntent::default();
    let outcome = intent.apply_request(FlowStopMode::Cancel, None);

    assert!(intent.requested);
    assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
    assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_USER_STOP));
    assert!(intent.deadline.is_none());

    match outcome {
        StopRequestOutcome::Applied { reason_label, .. } => {
            assert_eq!(reason_label, STOP_REASON_USER_STOP);
        }
        StopRequestOutcome::Ignored => {
            panic!("cancel request should never be ignored");
        }
    }
}

#[test]
fn stop_intent_graceful_sets_deadline() {
    let mut intent = StopIntent::default();
    let timeout = Duration::from_secs(3);
    let before = std::time::Instant::now();

    let _ = intent.apply_request(FlowStopMode::Graceful { timeout }, None);
    let after = std::time::Instant::now();

    assert!(intent.requested);
    assert!(matches!(
        intent.mode,
        Some(FlowStopMode::Graceful { timeout: t }) if t == timeout
    ));
    assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_USER_STOP));

    let deadline = intent
        .deadline
        .expect("graceful stop should set a deadline");
    assert!(deadline >= before + timeout);
    assert!(deadline <= after + timeout);
}

#[test]
fn stop_intent_cancel_overrides_graceful() {
    let mut intent = StopIntent::default();
    let _ = intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::from_secs(5),
        },
        None,
    );
    assert!(intent.deadline.is_some());

    let _ = intent.apply_request(FlowStopMode::Cancel, None);
    assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
    assert!(intent.deadline.is_none());
}

#[test]
fn stop_intent_graceful_is_ignored_after_cancel() {
    let mut intent = StopIntent::default();
    let _ = intent.apply_request(FlowStopMode::Cancel, None);
    let outcome = intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::from_secs(1),
        },
        None,
    );

    assert!(matches!(outcome, StopRequestOutcome::Ignored));
    assert!(matches!(intent.mode, Some(FlowStopMode::Cancel)));
    assert!(intent.deadline.is_none());
}

#[test]
fn stop_intent_expired_timeout_preserves_deadline() {
    let mut intent = StopIntent::default();
    let _ = intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::ZERO,
        },
        Some("first_reason".to_string()),
    );
    assert_eq!(intent.reason.as_deref(), Some("first_reason"));
    let original_deadline = intent.deadline;

    let _ = intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.to_string()));
    assert_eq!(intent.reason.as_deref(), Some(STOP_REASON_TIMEOUT));
    assert_eq!(
        intent.deadline, original_deadline,
        "timeout escalation must preserve the graceful-stop deadline"
    );
}

#[test]
fn first_graceful_deadline_wins_in_both_duration_orders() {
    for (first, second) in [(1, 60), (60, 1)] {
        let mut intent = StopIntent::default();
        intent.apply_request(
            FlowStopMode::Graceful {
                timeout: Duration::from_secs(first),
            },
            Some("first".into()),
        );
        let first_deadline = intent.deadline;
        assert!(matches!(
            intent.apply_request(
                FlowStopMode::Graceful {
                    timeout: Duration::from_secs(second)
                },
                Some("second".into()),
            ),
            StopRequestOutcome::Ignored
        ));
        assert_eq!(intent.deadline, first_deadline);
        assert_eq!(intent.reason.as_deref(), Some("first"));
    }
}

#[test]
fn cancel_is_absorbing_including_reason_and_admission_time() {
    let mut intent = StopIntent::default();
    intent.apply_request(FlowStopMode::Cancel, Some("explicit_cancel".into()));
    for (mode, reason) in [
        (FlowStopMode::Cancel, "duplicate"),
        (FlowStopMode::Cancel, STOP_REASON_TIMEOUT),
        (
            FlowStopMode::Graceful {
                timeout: Duration::ZERO,
            },
            "late_grace",
        ),
    ] {
        assert!(matches!(
            intent.apply_request(mode, Some(reason.into())),
            StopRequestOutcome::Ignored
        ));
        assert_eq!(intent.reason.as_deref(), Some("explicit_cancel"));
    }
}

#[test]
fn timeout_cancel_requires_an_expired_graceful_stop() {
    let mut intent = StopIntent::default();
    assert!(matches!(
        intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.into())),
        StopRequestOutcome::Ignored
    ));
    assert!(!intent.requested);
    intent.apply_request(
        FlowStopMode::Graceful {
            timeout: Duration::from_secs(60),
        },
        None,
    );
    let deadline = intent.deadline;
    assert!(matches!(
        intent.apply_request(FlowStopMode::Cancel, Some(STOP_REASON_TIMEOUT.into())),
        StopRequestOutcome::Ignored
    ));
    assert_eq!(intent.deadline, deadline);
    assert!(matches!(intent.mode, Some(FlowStopMode::Graceful { .. })));
}

#[test]
fn record_stage_completion_is_idempotent_for_duplicate_terminal_events() {
    let stage_a = StageId::new();
    let stage_b = StageId::new();
    let mut completed = vec![stage_a];

    let (is_new, all_completed_now) = record_stage_completion(&mut completed, stage_b, 2);
    assert!(is_new);
    assert!(all_completed_now);
    assert_eq!(completed, vec![stage_a, stage_b]);

    let (is_new, all_completed_now) = record_stage_completion(&mut completed, stage_b, 2);
    assert!(!is_new);
    assert!(!all_completed_now);
    assert_eq!(completed, vec![stage_a, stage_b]);
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

#[tokio::test]
async fn repeated_graceful_controls_have_no_actions_and_cancel_folds_once() {
    use std::time::Duration;
    for (first, second) in [(1, 60), (60, 1)] {
        let mut context = make_fsm_context();
        context.flow_start_time = Some(std::time::Instant::now());
        let mut fsm = build_pipeline_fsm_with_initial(PipelineFsmState::Running);
        let event = |seconds| {
            PipelineFsmEvent::from(PipelineControl::Stop {
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
        let cancel = PipelineFsmEvent::from(PipelineControl::Stop {
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
async fn repeated_abort_controls_preserve_the_first_failure_without_new_work() {
    let mut ctx = make_fsm_context();
    let mut machine = build_pipeline_fsm_with_initial(PipelineFsmState::Running);
    let first = machine
        .handle(
            PipelineFsmEvent::from(PipelineControl::Abort {
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
                PipelineFsmEvent::from(PipelineControl::Abort {
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
async fn readiness_and_start_consume_committed_pipeline_facts() {
    let mut ctx = make_fsm_context();
    let mut fsm = build_pipeline_fsm_with_initial(PipelineFsmState::AwaitingStageReadiness);
    assert!(fsm
        .handle(PipelineFsmEvent::PhysicalSettlementSatisfied, &mut ctx)
        .await
        .is_err());
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
    let actions = fsm.handle(PipelineFsmEvent::Start, &mut ctx).await.unwrap();
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
            .handle(PipelineFsmEvent::Start, &mut ctx)
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
            PipelineFsmEvent::from(PipelineControl::Stop {
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
