// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Failure is observable before child settlement; publication settlement stays owned.
use super::support::*;
use crate::bootstrap::{
    bootstrap_test_lock_async, install_bootstrap_config, BootstrapConfig, StartupMode,
};
use crate::pipeline::fsm::PipelineFsmEvent as E;
use crate::pipeline::PipelineState as S;
use crate::supervised_base::ChannelBuilder;
use obzenflow_core::event::context::StageType;
use obzenflow_core::journal::factory::FlowJournalFactory;
use obzenflow_core::{Journal, SystemId};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

pub async fn failure_remains_observable_while_child_cleanup_is_blocked(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..Default::default()
    });
    let mut journals = make_journals();
    let system_id = SystemId::new();
    let journal = new_system_journal(&mut *journals, system_id);
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, journal.clone());
    let probe = ShutdownProbe::default();
    let sink_handle = owned_test_stage(sink, StageType::Sink, Some(probe.clone()));
    let signals = sink_handle.signals.clone();
    ctx.stage_supervisors.insert(sink, Arc::new(sink_handle));
    ctx.source_supervisors.insert(
        source,
        TestPipelineStageHandle::boxed(source, "source", StageType::FiniteSource),
    );
    let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
    let mut states = watcher.subscribe();
    let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
    wait_for_state(&mut states, "Running", |s| matches!(s, S::Running)).await;
    signals.fail(sink, "original handler failure");
    wait_for_state(&mut states, "FailingChildren", |s| {
        matches!(s, S::FailingChildren { .. })
    })
    .await;
    sender.send(E::Cancel).await.unwrap();
    sender
        .send(E::Abort {
            reason: "later cancellation".into(),
        })
        .await
        .unwrap();
    tokio::task::yield_now().await;
    assert!(
        matches!(&*states.borrow(), S::FailingChildren { cause } if cause.contains("original handler failure"))
    );
    assert!(!task.is_finished());
    probe.completed.store(true, Ordering::Relaxed);
    probe.completed_notify.notify_waiters();
    let error = tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("original handler failure"));
    let rows = journal.read_all_unordered().await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|r| r.event_type_name() == "system.pipeline.failed")
            .count(),
        1
    );
    assert!(!rows
        .iter()
        .any(|r| r.event_type_name() == "system.pipeline.cancelled"));
}

pub async fn contract_failure_cause_survives_child_observation_order(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    use crate::messaging::upstream_subscription::ContractFailure;
    use crate::stages::common::stage_handle::{StageError, StageFailure};
    use crate::stages::common::stage_lifecycle::{LifecycleExit, LifecycleFailure};
    use obzenflow_core::event::payloads::system_payload::PipelineLifecycleEvent;
    use obzenflow_core::event::types::{SeqNo, ViolationCause};
    use obzenflow_core::event::SystemPayload;

    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..Default::default()
    });
    for exit_first in [false, true] {
        let mut journals = make_journals();
        let system_id = SystemId::new();
        let journal = new_system_journal(&mut *journals, system_id);
        let (topology, source, sink) = source_sink_topology_with_source();
        let mut ctx = test_context(topology, system_id, journal.clone());
        let sink_handle = owned_test_stage(sink, StageType::Sink, Some(ShutdownProbe::default()));
        let signals = sink_handle.signals.clone();
        ctx.stage_supervisors.insert(sink, Arc::new(sink_handle));
        ctx.source_supervisors.insert(
            source,
            TestPipelineStageHandle::boxed(source, "source", StageType::FiniteSource),
        );
        let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
        let mut states = watcher.subscribe();
        let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
        wait_for_state(&mut states, "Running", |s| matches!(s, S::Running)).await;
        let expected = ViolationCause::SeqDivergence {
            advertised: Some(SeqNo(3)),
            reader: SeqNo(2),
        };
        let cause = StageError::Execution(Arc::new(ContractFailure {
            upstream: source,
            cause: expected.clone(),
        }));
        let exit = LifecycleExit::Failed(LifecycleFailure {
            cause: cause.clone(),
            snapshot: Default::default(),
        });
        if !exit_first {
            signals.0.send_modify(|result| {
                result.failure = Some(StageFailure {
                    stage_id: sink,
                    cause,
                    snapshot: Default::default(),
                })
            });
            wait_for_state(&mut states, "FailingChildren", |s| {
                matches!(s, S::FailingChildren { .. })
            })
            .await;
            sender.send(E::Cancel).await.unwrap();
            sender
                .send(E::Abort {
                    reason: "later cancellation".into(),
                })
                .await
                .unwrap();
            assert!(!task.is_finished());
        }
        // In the exit-first case no prompt failure is exposed by the child.
        signals.0.send_modify(|result| result.exit = Some(exit));
        tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(&*states.borrow(), S::Failed { failure_cause: Some(cause), .. } if cause == &expected)
        );
        let rows = journal.read_all_unordered().await.unwrap();
        let failures: Vec<_> = rows
            .iter()
            .filter_map(|row| match &row.payload {
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Failed {
                    reason,
                    failure_cause,
                    ..
                }) => Some((reason, failure_cause)),
                _ => None,
            })
            .collect();
        assert_eq!(failures.len(), 1);
        assert_eq!(failures[0].1.as_ref(), Some(&expected));
        assert!(failures[0].0.contains(&source.to_string()));
        assert!(failures[0].0.contains(&sink.to_string()));
    }
}

pub async fn terminal_publication_is_owned_until_settlement_and_failure_is_retained(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..Default::default()
    });
    for fail in [false, true] {
        let mut journals = make_journals();
        let system_id = SystemId::new();
        let gate = Arc::new(TerminalAppendGate {
            entered: Default::default(),
            release: Default::default(),
            fail,
        });
        let mut journal = ControlledJournal::new(new_system_journal(&mut *journals, system_id));
        journal.terminal_append = Some(gate.clone());
        let journal = Arc::new(journal);
        let (topology, source, sink) = source_sink_topology_with_source();
        let mut ctx = test_context(topology, system_id, journal.clone());
        let source_handle = owned_test_stage(source, StageType::FiniteSource, None);
        let sink_handle = owned_test_stage(sink, StageType::Sink, None);
        let source_results = source_handle.signals.clone();
        let sink_results = sink_handle.signals.clone();
        ctx.source_supervisors
            .insert(source, Arc::new(source_handle));
        ctx.stage_supervisors.insert(sink, Arc::new(sink_handle));
        let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
        let mut states = watcher.subscribe();
        let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
        wait_for_state(&mut states, "Running", |s| matches!(s, S::Running)).await;
        source_results.complete();
        sink_results.complete();
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        assert_eq!(*states.borrow(), S::PublishingTerminal);
        sender.send(E::Cancel).await.unwrap();
        assert!(!task.is_finished());
        gate.release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(2), task)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            result.is_err(),
            fail,
            "terminal append determines the retained result"
        );
        assert!(states.borrow().is_terminal());
        let rows = journal.read_all_unordered().await.unwrap();
        assert_eq!(
            rows.iter()
                .filter(|r| r.event_type_name() == "system.pipeline.completed")
                .count(),
            usize::from(!fail)
        );
    }
}

pub async fn expired_graceful_stop_aborts_and_joins_without_a_fresh_cleanup_budget(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    let _lock = bootstrap_test_lock_async().await;
    let _guard = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Auto,
        ..Default::default()
    });
    let mut journals = make_journals();
    let system_id = SystemId::new();
    let journal = new_system_journal(&mut *journals, system_id);
    let (topology, source, sink) = source_sink_topology_with_source();
    let mut ctx = test_context(topology, system_id, journal.clone());
    let probes = [ShutdownProbe::default(), ShutdownProbe::default()];
    let mut source_handle =
        owned_test_stage(source, StageType::InfiniteSource, Some(probes[0].clone()));
    source_handle.stall_drain = true;
    ctx.source_supervisors
        .insert(source, Arc::new(source_handle));
    ctx.stage_supervisors.insert(
        sink,
        Arc::new(owned_test_stage(
            sink,
            StageType::Sink,
            Some(probes[1].clone()),
        )),
    );
    let (sender, receiver, watcher) = ChannelBuilder::new().build(S::Created);
    let mut states = watcher.subscribe();
    let task = spawn_supervisor_loop(S::Created, system_id, ctx, receiver, watcher);
    wait_for_state(&mut states, "Running", |s| matches!(s, S::Running)).await;
    sender
        .send(E::GracefulStop {
            timeout: Duration::from_millis(5),
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(matches!(*states.borrow(), S::Cancelled { .. }));
    for probe in probes {
        assert!(probe.request_abort_count.load(Ordering::Relaxed) > 0);
        assert!(probe.wait_for_completion_count.load(Ordering::Relaxed) > 0);
    }
}
