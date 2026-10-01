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

pub async fn application_abort_does_not_turn_owned_child_cancellation_into_failure(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    use crate::__private::lifecycle;
    use crate::id_conversions::StageIdExt;
    use crate::pipeline::fsm::PipelineFsmState;
    use crate::pipeline::handle::{FlowHandle, FlowHandleExtras};
    use crate::pipeline::supervisor::PipelineSupervisor;
    use crate::stages::common::stage_handle::{StageExit, StageHandle};
    use crate::supervised_base::{HandleBuilder, HandleError, SupervisorTaskBuilder};
    use futures::FutureExt;
    use obzenflow_core::event::observability::NoObservations;
    use std::task::Poll;

    let _lock = bootstrap_test_lock_async().await;
    let _bootstrap = install_bootstrap_config(BootstrapConfig {
        startup_mode: StartupMode::Manual,
        ..Default::default()
    });
    for cause in [
        "owner-publication",
        "publication-child-failure",
        "closed-publication",
        "owner",
        "independent-child",
        "child-failure",
    ] {
        let publish_after_abort = cause.contains("publication");
        let mut journals = make_journals();
        let system_id = SystemId::new();
        let journal = new_system_journal(&mut *journals, system_id);
        let mut topology = obzenflow_topology::TopologyBuilder::new();
        let source = topology.add_stage(Some("source".into()));
        let left = topology.add_stage(Some("left".into()));
        topology.set_current(source);
        let right = topology.add_stage(Some("right".into()));
        let sinks = [left, right];
        let topology = Arc::new(topology.build_unchecked().unwrap());
        let source = obzenflow_core::StageId::from_topology_id(source);
        let sinks = sinks.map(obzenflow_core::StageId::from_topology_id);
        let mut context = test_context(topology, system_id, journal.clone());
        let probes: [_; 3] = std::array::from_fn(|_| ShutdownProbe::default());
        let source_handle = Arc::new(owned_test_stage(
            source,
            StageType::InfiniteSource,
            Some(probes[0].clone()),
        ));
        let sink_handles: [_; 2] = std::array::from_fn(|index| {
            Arc::new(owned_test_stage(
                sinks[index],
                StageType::Sink,
                Some(probes[index + 1].clone()),
            ))
        });
        context
            .source_supervisors
            .insert(source, source_handle.clone());
        for handle in &sink_handles {
            context
                .stage_supervisors
                .insert(handle.stage_id(), handle.clone());
        }
        context.outstanding_children.insert(source);
        context.outstanding_children.extend(sinks);
        let publications = context.resources.publications.clone();
        let operational_failure = context.resources.failure.clone();

        // Hold an actual pipeline poll on one worker. Tokio's abort request
        // cannot destroy a future while that poll is still executing. The
        // other worker requests cancellation, then explicitly releases it.
        // Disconnect also releases the gate if the controller unwinds.
        let (entered, polling) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel::<()>();
        let mut entered = Some(entered);
        let signals = source_handle.signals.clone();
        context.resources.exits.get_mut().unwrap().push(
            std::future::poll_fn(move |_| {
                if let Some(entered) = entered.take() {
                    let _ = entered.send(());
                    assert!(
                        !matches!(
                            held.recv_timeout(Duration::from_secs(2)),
                            Err(std::sync::mpsc::RecvTimeoutError::Timeout)
                        ),
                        "controller did not release the held poll"
                    );
                }
                if publish_after_abort {
                    // Continue the same real supervisor poll after the owner
                    // has closed admission. The Start transition then attempts
                    // its actual pipeline publication before Tokio can destroy
                    // this still-executing future.
                    return Poll::Ready(E::Start);
                }
                match signals.0.borrow().exit.clone() {
                    Some(outcome) => Poll::Ready(E::ChildExited(StageExit {
                        stage_id: source,
                        outcome,
                    })),
                    None => Poll::Pending,
                }
            })
            .boxed(),
        );
        for sibling in &sink_handles {
            let sibling = sibling.clone();
            context
                .resources
                .exits
                .get_mut()
                .unwrap()
                .push(async move { E::ChildExited(sibling.wait_for_completion().await) }.boxed());
        }
        let extras = FlowHandleExtras {
            stage_cleanup: vec![
                source_handle.clone(),
                sink_handles[0].clone(),
                sink_handles[1].clone(),
            ],
            published_outcome: context.termination.published.clone(),
            metrics: context.resources.metrics.clone(),
            operational_failure: context.resources.failure.clone(),
            topology: None,
            flow_name: "held_parent_poll".into(),
            contract_attachments: None,
            ingress_refusals: None,
            metrics_journals: None,
            stage_journals: vec![],
            system_journals: vec![journal.clone()],
            system_journal: Some(journal.clone()),
            pipeline_writer_id: system_id.into(),
            observations: context.observations.clone(),
            host_observations: Arc::new(NoObservations),
            liveness_snapshots: None,
            run_substrate: obzenflow_core::journal::factory::RunSubstrateState::Ephemeral,
            flow_effective_config: None,
        };
        let initial = if publish_after_abort {
            S::ReadyForRun
        } else {
            S::Running
        };
        let initial_fsm = if publish_after_abort {
            PipelineFsmState::ReadyForRun
        } else {
            PipelineFsmState::Running
        };
        let (sender, receiver, watcher) = ChannelBuilder::new().build(initial);
        let supervisor = PipelineSupervisor::new(
            system_id,
            receiver,
            watcher.clone(),
            context.resources.failure.clone(),
        );
        let task = SupervisorTaskBuilder::new("held_pipeline_poll")
            .with_publications(context.resources.publications.clone())
            .spawn_self_supervised(supervisor, initial_fsm, context);
        let flow = FlowHandle::new(
            HandleBuilder::new()
                .with_event_sender(sender)
                .with_state_watcher(watcher)
                .with_supervisor_task(task)
                .build_standard()
                .unwrap(),
            extras,
        );
        let guard = lifecycle::guard_execution(&flow);
        tokio::time::timeout(Duration::from_secs(2), polling)
            .await
            .unwrap()
            .unwrap();
        if cause == "closed-publication" {
            guard.disarm();
            publications.close();
        } else if cause == "independent-child" {
            guard.disarm();
            assert!(flow.is_running());
            assert!(probes
                .iter()
                .all(|probe| probe.request_abort_count.load(Ordering::Relaxed) == 0));
            source_handle.request_abort();
            for handle in &sink_handles {
                handle.request_abort();
            }
        } else {
            if matches!(cause, "child-failure" | "publication-child-failure") {
                source_handle.signals.fail(source, "original child failure");
            }
            drop(guard);
        }
        let requests_during_poll = probes
            .each_ref()
            .map(|probe| probe.request_abort_count.load(Ordering::Relaxed));
        release.send(()).unwrap();
        if cause == "closed-publication" {
            tokio::time::timeout(Duration::from_secs(2), async {
                while operational_failure.get().is_none() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("ordinary closed admission must remain an operational failure");
            // A later owner abort must not relabel the original closed-admission
            // failure as a consequence of cancellation.
            drop(lifecycle::guard_execution(&flow));
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            while flow.is_running() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let requests_before_join = probes
            .each_ref()
            .map(|probe| probe.request_abort_count.load(Ordering::Relaxed));
        let assisting_joins = probes
            .each_ref()
            .map(|probe| probe.abort_and_join_count.load(Ordering::Relaxed));
        for _ in 0..2 {
            let result = tokio::time::timeout(Duration::from_secs(2), lifecycle::wait(&flow))
                .await
                .unwrap();
            let error = result.unwrap_err();
            eprintln!("held parent poll cause={cause}: requests_during_poll={requests_during_poll:?}; requests_before_join={requests_before_join:?}; result={error:?}");
            let source = std::error::Error::source(&error).unwrap();
            if matches!(cause, "owner" | "owner-publication") {
                assert!(
                    matches!(
                        source.downcast_ref::<HandleError>(),
                        Some(HandleError::SupervisorAborted)
                    ),
                    "owner cancellation manufactured a failure: {error:?}"
                );
            } else if cause == "independent-child" {
                assert!(
                    matches!(
                        source.downcast_ref::<HandleError>(),
                        Some(HandleError::SupervisorFailed(_))
                    ),
                    "independent child cancellation must still fail: {error:?}"
                );
            } else if cause == "closed-publication" {
                let retained = operational_failure.get().unwrap();
                assert!(std::error::Error::source(retained).is_some_and(|cause| {
                    cause.is::<crate::supervised_base::publication::AdmissionClosed>()
                }));
                assert!(!publications.is_cancelled_admission(retained));
                let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(&error);
                let mut retained_cause = false;
                while let Some(current) = cause {
                    retained_cause |= current
                        .is::<crate::supervised_base::publication::AdmissionClosed>()
                        || matches!(current.downcast_ref::<obzenflow_fsm::FsmError>(),
                            Some(obzenflow_fsm::FsmError::HandlerError(message))
                                if message == "publication admission is closed");
                    cause = current.source();
                }
                assert!(
                    retained_cause,
                    "original admission failure must survive later owner cancellation: {error:?}"
                );
            } else {
                assert!(
                    matches!(source.downcast_ref::<crate::stages::common::stage_handle::StageError>(), Some(crate::stages::common::stage_handle::StageError::Other(message)) if message == "original child failure"),
                    "{error:?}"
                );
            }
        }
        assert_eq!(
            requests_before_join, [1; 3],
            "context destruction must cancel the source and both siblings before assisting joins"
        );
        assert_eq!(
            assisting_joins, [0; 3],
            "observe the owner before assistance"
        );
        if cause != "independent-child" {
            assert_eq!(
                requests_during_poll, [0; 3],
                "the live parent must not race its own child's cancellation"
            );
            assert!(!journal
                .read_all_unordered()
                .await
                .unwrap()
                .iter()
                .any(|row| matches!(
                    row.event_type_name(),
                    "system.pipeline.failed"
                        | "system.pipeline.cancelled"
                        | "system.pipeline.completed"
                )));
        }
    }
}

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
