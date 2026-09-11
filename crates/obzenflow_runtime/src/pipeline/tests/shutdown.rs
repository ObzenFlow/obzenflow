// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Cancellation, resource settlement and acknowledged terminal publication.

use crate::bootstrap::{bootstrap_test_lock_async, install_bootstrap_config, BootstrapConfig};
use crate::pipeline::fsm::{
    build_pipeline_fsm_with_initial, PipelineAction, PipelineFsmEvent, PipelineFsmState,
};
use crate::pipeline::supervisor::PipelineSupervisor;
use crate::pipeline::tests::support::{
    empty_system_subscription, empty_topology, initial_fsm_state, make_fsm_context,
    owned_test_stage, source_sink_topology, source_sink_topology_with_source,
    spawn_supervisor_loop, test_context, test_supervisor, MemoryJournal, ShutdownProbe,
    TerminalAppendGate, TestPipelineStageHandle,
};
use crate::pipeline::{FlowStopMode, PipelineControl, PipelineState};
use crate::stages::common::stage_handle::StageHandle;
use crate::supervised_base::{ChannelBuilder, EventLoopDirective, SelfSupervised};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::types::ViolationCause;
use obzenflow_core::event::{JournalEvent, SystemEvent, SystemEventFactory};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{StageId, SystemId};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

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
