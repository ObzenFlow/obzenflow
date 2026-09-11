// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Materialisation, committed readiness evidence and authorised source startup.

use crate::bootstrap::{
    bootstrap_test_lock_async, install_bootstrap_config, BootstrapConfig, StartupMode,
};
use crate::journal::FlowJournalFactory;
use crate::pipeline::fsm::{PipelineAction, PipelineFsmEvent, PipelineFsmState};
use crate::pipeline::tests::support::new_system_journal;
use crate::pipeline::tests::support::{
    empty_system_subscription, empty_topology, ready_stage, source_sink_topology,
    source_sink_topology_with_source, spawn_supervisor_loop, stop_and_join, test_context,
    test_supervisor, wait_for_state, TestPipelineStageHandle,
};
use crate::pipeline::PipelineState;
use crate::supervised_base::ChannelBuilder;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::SystemId;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::oneshot;

pub async fn manual_ready_for_run_publishes_state_and_waits_for_external_run(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let system_journal = new_system_journal(&mut *journals, system_id);
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

pub async fn auto_ready_for_run_emits_run_and_reaches_running(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let system_journal = new_system_journal(&mut *journals, system_id);
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

pub async fn materializing_stage_count_mismatch_transitions_to_failed_without_panic(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let system_journal = new_system_journal(&mut *journals, system_id);
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

pub async fn materialized_to_ready_for_run_publishes_post_transition_state(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let system_journal = new_system_journal(&mut *journals, system_id);
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

pub async fn running_state_requires_committed_source_running_after_start(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let system_journal = new_system_journal(&mut *journals, system_id);
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
        .send(PipelineFsmEvent::Start)
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

pub async fn early_run_queued_in_materialized_is_consumed_before_ready_for_run(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..BootstrapConfig::default()
    });
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let system_journal = new_system_journal(&mut *journals, system_id);
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
        .send(PipelineFsmEvent::Start)
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

pub async fn empty_topology_fails_through_the_canonical_fsm(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let journal = new_system_journal(&mut *journals, system_id);
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

pub async fn stage_failures_and_cancellations_before_readiness_use_journal_evidence(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    for state in [
        PipelineFsmState::AwaitingStageReadiness,
        PipelineFsmState::ReadyForRun,
    ] {
        for cancelled in [false, true] {
            let system_id = SystemId::new();
            let mut journals = make_journals();
            let journal = new_system_journal(&mut *journals, system_id);
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

pub async fn materialisation_reconsiders_readiness_facts_already_consumed(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let system_id = SystemId::new();
    let mut journals = make_journals();
    let journal = new_system_journal(&mut *journals, system_id);
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
