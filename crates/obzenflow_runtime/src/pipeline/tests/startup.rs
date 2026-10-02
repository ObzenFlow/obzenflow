// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline startup waits for achieved child transitions and its own appends.
use super::support::*;
use crate::bootstrap::{
    bootstrap_test_lock_async, install_bootstrap_config, BootstrapConfig, StartupMode,
};
use crate::pipeline::fsm::{PipelineFsmEvent as E, PipelineFsmState};
use crate::pipeline::supervisor::PipelineSupervisor;
use crate::pipeline::PipelineState as S;
use crate::stages::common::stage_handle::StageMilestone;
use crate::supervised_base::{ChannelBuilder, EventLoopDirective, SelfSupervised};
use futures::FutureExt;
use obzenflow_core::event::context::StageType;
use obzenflow_core::journal::factory::FlowJournalFactory;
use obzenflow_core::{Journal, SystemId};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

pub async fn constructed_pipeline_keeps_startup_mode_when_bootstrap_changes(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    for startup_mode in [StartupMode::Manual, StartupMode::Auto] {
        let construction = install_bootstrap_config(BootstrapConfig {
            startup_mode,
            ..Default::default()
        });
        let mut journals = make_journals();
        let system_id = SystemId::new();
        let journal = new_system_journal(&mut *journals, system_id);
        let (topology, _, _) = source_sink_topology_with_source();
        let mut ctx = test_context(topology, system_id, journal);
        let (sender, receiver, watcher) = ChannelBuilder::new().build(S::ReadyForRun);
        // PipelineBuilder constructs this supervisor before handing the flow
        // back to its host. Later bootstrap installs cannot authorise or defer
        // input for the already-built flow.
        let mut supervisor =
            PipelineSupervisor::new(system_id, receiver, watcher, ctx.resources.failure.clone());
        drop(construction);
        let _next_host = install_bootstrap_config(BootstrapConfig {
            startup_mode: match startup_mode {
                StartupMode::Manual => StartupMode::Auto,
                StartupMode::Auto => StartupMode::Manual,
            },
            ..Default::default()
        });

        // Poll the real dispatch once. A pending manual start is observed
        // directly, without a scheduling delay or timeout as a negative oracle.
        let dispatch = supervisor
            .dispatch_state(&PipelineFsmState::ReadyForRun, &mut ctx)
            .now_or_never();
        match startup_mode {
            StartupMode::Manual => {
                assert!(
                    dispatch.is_none(),
                    "a manual pipeline must remain parked after ambient bootstrap changes: {dispatch:?}"
                );
                sender.send(E::Start).await.unwrap();
                assert!(matches!(
                    supervisor
                        .dispatch_state(&PipelineFsmState::ReadyForRun, &mut ctx)
                        .now_or_never(),
                    Some(Ok(EventLoopDirective::Transition(E::Start)))
                ));
            }
            StartupMode::Auto => assert!(
                matches!(dispatch, Some(Ok(EventLoopDirective::Transition(E::Start)))),
                "an automatic pipeline must still start after ambient bootstrap changes: {dispatch:?}"
            ),
        }
    }
}

pub async fn startup_waits_for_achieved_transitions_with_zero_child_journal_reads(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..Default::default()
    });
    let mut journals = make_journals();
    let system_id = SystemId::new();
    let pipeline_journal = new_system_journal(&mut *journals, system_id);
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, pipeline_journal.clone());
    let source_journal = Arc::new(ControlledJournal::new(new_stage_journal(
        &mut *journals,
        source,
        "source",
    )));
    let sink_journal = Arc::new(ControlledJournal::new(new_stage_journal(
        &mut *journals,
        sink,
        "sink",
    )));
    ctx.stage_data_journals = vec![
        (source, source_journal.clone()),
        (sink, sink_journal.clone()),
    ];
    let mut source_handle = owned_test_stage(source, StageType::FiniteSource, None);
    let mut sink_handle = owned_test_stage(sink, StageType::Sink, None);
    source_handle.acknowledge_commands = false;
    sink_handle.acknowledge_commands = false;
    let source_results = source_handle.signals.clone();
    let sink_results = sink_handle.signals.clone();
    ctx.source_supervisors
        .insert(source, Arc::new(source_handle));
    ctx.stage_supervisors.insert(sink, Arc::new(sink_handle));
    let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
    let mut states = watcher.subscribe();
    let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
    wait_for_state(&mut states, "InitializingStages", |s| {
        matches!(s, S::InitializingStages)
    })
    .await;
    sender.send(E::Start).await.unwrap(); // early control cannot authorize input
    source_results.acknowledge(StageMilestone::Initialized);
    tokio::task::yield_now().await;
    assert_eq!(*states.borrow(), S::InitializingStages);
    sink_results.acknowledge(StageMilestone::Initialized);
    wait_for_state(&mut states, "StartingConsumers", |s| {
        matches!(s, S::StartingConsumers)
    })
    .await;
    sink_results.acknowledge(StageMilestone::Started);
    wait_for_state(&mut states, "ReadyForRun", |s| matches!(s, S::ReadyForRun)).await;
    sender.send(E::Start).await.unwrap();
    wait_for_state(&mut states, "StartingSources", |s| {
        matches!(s, S::StartingSources)
    })
    .await;
    assert!(!pipeline_journal
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .any(|r| r.event_type_name()
            == "supervisor.runtime.pipeline_supervisor.milestone.sources_started"));
    // Sources may complete before the parent observes their retained startup acknowledgement.
    source_results.acknowledge(StageMilestone::Started);
    wait_for_state(&mut states, "Running", |s| matches!(s, S::Running)).await;
    source_results.complete();
    sink_results.complete();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(*states.borrow(), S::Drained);
    assert_eq!(source_journal.reader_calls.load(Ordering::Relaxed), 0);
    assert_eq!(sink_journal.reader_calls.load(Ordering::Relaxed), 0);
    let rows = pipeline_journal.read_all_unordered().await.unwrap();
    let types: Vec<_> = rows.iter().map(|row| row.event_type_name()).collect();
    assert!(
        types
            .iter()
            .position(|t| *t == "supervisor.runtime.pipeline_supervisor.command.start.admitted")
            .unwrap()
            < types
                .iter()
                .position(
                    |t| *t == "supervisor.runtime.pipeline_supervisor.milestone.sources_started"
                )
                .unwrap()
    );
}

pub async fn blocked_registration_preserves_cancellation_and_cleanup(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let cleanup_timeout = Duration::from_secs(30);
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        shutdown_timeout: cleanup_timeout,
        ..Default::default()
    });
    for child_fails in [false, true] {
        let mut journals = make_journals();
        let system_id = SystemId::new();
        let gate = Arc::new(TerminalAppendGate {
            entered: Default::default(),
            release: Default::default(),
            fail: false,
        });
        let mut journal = ControlledJournal::new(new_system_journal(&mut *journals, system_id));
        journal.gate_event = Some("supervisor.runtime.pipeline_supervisor.registered");
        journal.gate = Some(gate.clone());
        let journal = Arc::new(journal);
        let (topology, source, sink) = source_sink_topology_with_source();
        let mut ctx = test_context(topology, system_id, journal.clone());
        let probes = [ShutdownProbe::default(), ShutdownProbe::default()];
        let source_handle =
            owned_test_stage(source, StageType::FiniteSource, Some(probes[0].clone()));
        let sink_handle = owned_test_stage(sink, StageType::Sink, Some(probes[1].clone()));
        let source_results = source_handle.signals.clone();
        let sink_results = sink_handle.signals.clone();
        ctx.source_supervisors
            .insert(source, Arc::new(source_handle));
        ctx.stage_supervisors.insert(sink, Arc::new(sink_handle));
        let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
        let mut states = watcher.subscribe();
        let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        assert_eq!(*states.borrow(), S::Registering);
        if child_fails {
            source_results.fail(source, "failure during registration");
        } else {
            sender.send(E::Cancel).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while probes
                .iter()
                .any(|probe| probe.force_shutdown_count.load(Ordering::Relaxed) != 1)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("both children must receive cancellation before registration settles");
        assert_eq!(source_results.0.borrow().milestones, [false; 3]);
        assert_eq!(sink_results.0.borrow().milestones, [false; 3]);
        assert!(!task.is_finished());

        // Cleanup expiry must also execute while the accepted append is blocked.
        tokio::time::pause();
        tokio::time::advance(cleanup_timeout).await;
        wait_for_state(&mut states, "PublishingTerminal", |s| {
            matches!(s, S::PublishingTerminal)
        })
        .await;
        tokio::time::resume();
        for probe in &probes {
            assert_eq!(probe.request_abort_count.load(Ordering::Relaxed), 1);
        }
        assert!(!task.is_finished());
        gate.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result.is_err(), child_fails);
        let rows = journal.read_all_unordered().await.unwrap();
        assert_eq!(
            rows.iter()
                .filter(
                    |r| r.event_type_name() == "supervisor.runtime.pipeline_supervisor.registered"
                )
                .count(),
            1
        );
        assert!(!rows.iter().any(|r| matches!(
            r.event_type_name(),
            "supervisor.runtime.pipeline_supervisor.milestone.ready_for_run"
                | "supervisor.runtime.pipeline_supervisor.milestone.sources_started"
        )));
    }
}

pub async fn blocked_ready_publication_exposes_pending_state_and_preserves_cancellation(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..Default::default()
    });
    let mut journals = make_journals();
    let system_id = SystemId::new();
    let gate = Arc::new(TerminalAppendGate {
        entered: Default::default(),
        release: Default::default(),
        fail: false,
    });
    let mut journal = ControlledJournal::new(new_system_journal(&mut *journals, system_id));
    journal.gate_event = Some("supervisor.runtime.pipeline_supervisor.milestone.ready_for_run");
    journal.gate = Some(gate.clone());
    let journal = Arc::new(journal);
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, journal.clone());
    ctx.source_supervisors.insert(
        source,
        TestPipelineStageHandle::boxed(source, "source", StageType::FiniteSource),
    );
    ctx.stage_supervisors.insert(
        sink,
        TestPipelineStageHandle::boxed(sink, "sink", StageType::Sink),
    );
    let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
    let mut states = watcher.subscribe();
    let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
    tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
        .await
        .unwrap();
    assert_eq!(*states.borrow(), S::PublishingReady);
    sender.send(E::Cancel).await.unwrap();
    wait_for_state(
        &mut states,
        "cancellation while publication is pending",
        |s| matches!(s, S::CancellingChildren | S::PublishingTerminal),
    )
    .await;
    assert!(!task.is_finished());
    gate.release.notify_one();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let rows = journal.read_all_unordered().await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|r| r.event_type_name()
                == "supervisor.runtime.pipeline_supervisor.milestone.ready_for_run")
            .count(),
        1
    );
    assert!(!rows.iter().any(|r| r.event_type_name()
        == "supervisor.runtime.pipeline_supervisor.milestone.sources_started"));
}
