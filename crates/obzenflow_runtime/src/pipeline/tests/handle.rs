// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Public flow controls, execution guards and repeatable completion waits.

use super::*;
use crate::__private::lifecycle;
use crate::metrics::observations::ObservationRegistry;
use crate::stages::common::stage_handle::STOP_REASON_TIMEOUT;
use crate::stages::common::stage_lifecycle::{LifecyclePhase, LifecycleResults};
use crate::supervised_base::{ChannelBuilder, EventReceiver, HandleBuilder};
use obzenflow_core::event::observability::NoObservations;
use std::error::Error;
use tokio::sync::mpsc::error::TryRecvError;

fn empty_extras() -> FlowHandleExtras {
    FlowHandleExtras {
        metrics_journals: None,
        stage_journals: vec![],
        system_journals: vec![],
        ingress_refusals: None,
        observations: Arc::new(ObservationRegistry::default()),
        host_observations: Arc::new(NoObservations),
        stage_cleanup: Vec::new(),
        published_outcome: Default::default(),
        metrics: Default::default(),
        operational_failure: Default::default(),
        topology: None,
        flow_name: "test_flow".to_string(),
        contract_attachments: None,
        system_journal: None,
        pipeline_writer_id: WriterId::from(obzenflow_core::id::SystemId::new()),
        flow_effective_config: None,
        liveness_snapshots: None,
        run_substrate: RunSubstrateState::Ephemeral,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn execution_guard_is_independent_of_completion_observers_and_handle_ownership() {
    use crate::pipeline::tests::support::{owned_test_stage, ShutdownProbe};
    use obzenflow_core::event::context::StageType;
    use std::sync::atomic::Ordering;

    for disarm in [false, true] {
        let (sender, _receiver, watcher) =
            ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Created);
        let task = tokio::spawn(std::future::pending::<
            Result<(), Box<dyn std::error::Error + Send + Sync>>,
        >());
        let probes = [
            ShutdownProbe::default(),
            ShutdownProbe::default(),
            ShutdownProbe::default(),
        ];
        let mut extras = empty_extras();
        // One source and both independent fan-out siblings must receive the
        // owner's request before the resource join can assist cancellation.
        for (kind, probe) in [StageType::InfiniteSource, StageType::Sink, StageType::Sink]
            .into_iter()
            .zip(&probes)
        {
            extras.stage_cleanup.push(Arc::new(owned_test_stage(
                obzenflow_core::StageId::new(),
                kind,
                Some(probe.clone()),
            )));
        }
        let flow = Arc::new(FlowHandle::new(
            HandleBuilder::new()
                .with_event_sender(sender)
                .with_state_watcher(watcher)
                .with_supervisor_task(task)
                .build_standard()
                .unwrap(),
            extras,
        ));
        let guard = lifecycle::guard_execution(&flow);
        assert_eq!(
            Arc::strong_count(&flow),
            1,
            "guard must not retain the FlowHandle"
        );
        let mut observer = Box::pin(lifecycle::wait(&flow));
        assert!(futures::poll!(&mut observer).is_pending());
        drop(observer);
        assert!(
            flow.is_running(),
            "dropping an observer must not cancel execution"
        );
        if disarm {
            guard.disarm();
            assert!(
                probes
                    .iter()
                    .all(|probe| probe.request_abort_count.load(Ordering::Relaxed) == 0),
                "disarmed ownership supplies no cancellation witness"
            );
            assert!(
                flow.is_running(),
                "releasing the fallback must not request cancellation"
            );
            drop(lifecycle::guard_execution(&flow));
        } else {
            drop(guard);
        }
        let owned_requests = probes
            .each_ref()
            .map(|probe| probe.request_abort_count.load(Ordering::Relaxed));
        let assisting_joins = probes
            .each_ref()
            .map(|probe| probe.abort_and_join_count.load(Ordering::Relaxed));
        for _ in 0..2 {
            let error = tokio::time::timeout(Duration::from_secs(1), lifecycle::wait(&flow))
                .await
                .unwrap()
                .unwrap_err();
            assert!(
                matches!(
                    error.source().and_then(
                        |source| source.downcast_ref::<crate::supervised_base::HandleError>()
                    ),
                    Some(crate::supervised_base::HandleError::SupervisorAborted)
                ),
                "{error:?}"
            );
        }
        assert_eq!(
            owned_requests, [1; 3],
            "later cleanup cannot repair missing original witnesses"
        );
        assert_eq!(
            assisting_joins, [0; 3],
            "observe the owner before assistance"
        );
        assert!(!flow.is_running());
        assert_eq!(flow.current_state(), PipelineState::Created);
    }
}

fn flow_handle_that_finishes_in(final_state: PipelineState) -> FlowHandle {
    let (event_sender, mut event_receiver, state_watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
            .with_event_buffer(4)
            .build(PipelineState::ReadyForRun);

    let state_watcher_for_task = state_watcher.clone();
    let extras = empty_extras();
    let published = extras.published_outcome.clone();
    let task = crate::supervised_base::SupervisorTaskBuilder::<()>::new("flow-test")
        .spawn_for_test(move || async move {
            LifecycleResults::observe(&LifecyclePhase::Ready, Default::default(), None);
            match event_receiver.recv().await {
                Some(PipelineFsmEvent::Start) => {
                    use super::super::termination::{
                        ExecutionFailure, ExecutionOutcome, PublishedTermination,
                    };
                    let outcome = match &final_state {
                        PipelineState::Failed {
                            reason,
                            failure_cause,
                        } => ExecutionOutcome::Failed(ExecutionFailure {
                            reason: reason.clone(),
                            cause: failure_cause.clone(),
                        }),
                        PipelineState::FailingChildren { cause } => {
                            ExecutionOutcome::Failed(ExecutionFailure {
                                reason: cause.clone(),
                                cause: None,
                            })
                        }
                        _ => ExecutionOutcome::Completed,
                    };
                    published
                        .set(PublishedTermination {
                            outcome,
                            event_id: Some(obzenflow_core::EventId::new()),
                        })
                        .unwrap();
                    state_watcher_for_task
                        .update(final_state)
                        .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> { Box::new(e) })?;
                    Ok(())
                }
                Some(event) => Err(format!("unexpected event: {event:?}").into()),
                None => Err("event channel closed before Run".into()),
            }
        });

    let handle = HandleBuilder::new()
        .with_event_sender(event_sender)
        .with_state_watcher(state_watcher)
        .with_supervisor_task(task)
        .build_standard()
        .expect("standard handle should build");

    FlowHandle::new(handle, extras)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_pipeline_preserves_acknowledged_resource_failures() {
    use crate::metrics::fsm::MetricsAggregatorState;
    use crate::pipeline::tests::support::{owned_test_stage, ShutdownProbe};
    use crate::supervised_base::SupervisorHandle;
    use obzenflow_core::event::context::StageType;
    use std::sync::atomic::Ordering;

    for owner in ["stage", "metrics", "publication"] {
        for primary_failure in [false, true] {
            let mut extras = empty_extras();
            let probes = [ShutdownProbe::default(), ShutdownProbe::default()];
            for (index, probe) in probes.iter().enumerate() {
                let child = owned_test_stage(
                    obzenflow_core::StageId::new(),
                    StageType::Sink,
                    Some(probe.clone()),
                );
                if owner == "stage" && index == 0 {
                    child.signals.fail(child.id, "acknowledged child failure");
                }
                extras.stage_cleanup.push(Arc::new(child));
            }
            if owner == "metrics" {
                let (sender, _receiver, watcher) =
                    ChannelBuilder::new().build(MetricsAggregatorState::Created);
                let task = tokio::spawn(async {
                    Err::<(), Box<dyn Error + Send + Sync>>(
                        std::io::Error::other("acknowledged child failure").into(),
                    )
                });
                let metrics = HandleBuilder::new()
                    .with_event_sender(sender)
                    .with_state_watcher(watcher)
                    .with_supervisor_task(task)
                    .build_standard()
                    .unwrap();
                metrics.wait_for_completion().await.unwrap_err();
                extras.metrics.install_for_test(metrics);
            } else if owner == "publication" {
                extras
                    .operational_failure
                    .set(crate::supervised_base::publication::SharedError::from(
                        Box::new(std::io::Error::other("acknowledged child failure"))
                            as Box<dyn Error + Send + Sync>,
                    ))
                    .unwrap();
            }
            let (sender, _receiver, watcher) = ChannelBuilder::new().build(PipelineState::Created);
            let task = tokio::spawn(async move {
                if primary_failure {
                    Err(std::io::Error::other("primary pipeline failure").into())
                } else {
                    std::future::pending::<Result<(), Box<dyn Error + Send + Sync>>>().await
                }
            });
            let flow = FlowHandle::new(
                HandleBuilder::new()
                    .with_event_sender(sender)
                    .with_state_watcher(watcher)
                    .with_supervisor_task(task)
                    .build_standard()
                    .unwrap(),
                extras,
            );
            if primary_failure {
                flow.handle.join().await.unwrap_err();
            }
            drop(lifecycle::guard_execution(&flow));
            for _ in 0..2 {
                let error = lifecycle::wait(&flow).await.unwrap_err();
                let expected = if primary_failure {
                    "primary pipeline failure"
                } else {
                    "acknowledged child failure"
                };
                let mut cause: &(dyn Error + 'static) = &error;
                while let Some(source) = cause.source() {
                    cause = source;
                }
                if let Some(cause) = cause.downcast_ref::<std::io::Error>() {
                    assert_eq!(cause.to_string(), expected, "{owner}: {error:?}");
                } else {
                    assert!(
                        matches!(cause.downcast_ref::<crate::stages::common::stage_handle::StageError>(), Some(crate::stages::common::stage_handle::StageError::Other(message)) if message == expected),
                        "{owner}: {error:?}"
                    );
                }
            }
            assert!(
                probes
                    .iter()
                    .all(|probe| probe.abort_and_join_count.load(Ordering::Relaxed) == 2),
                "every sibling joins even after failure"
            );
        }
    }
}

fn flow_handle_for_start_admission(
    initial_state: PipelineState,
) -> (FlowHandle, EventReceiver<PipelineFsmEvent>) {
    let (event_sender, event_receiver, state_watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new()
            .with_event_buffer(4)
            .build(initial_state.clone());

    let task = crate::supervised_base::SupervisorTaskBuilder::<()>::new("flow-admission-test")
        .spawn_for_test(move || async move {
            match initial_state {
                PipelineState::ReadyForRun => {
                    LifecycleResults::observe(&LifecyclePhase::Ready, Default::default(), None)
                }
                PipelineState::Running => {
                    LifecycleResults::observe(&LifecyclePhase::Active, Default::default(), None)
                }
                _ => {}
            }
            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        });

    let handle = HandleBuilder::new()
        .with_event_sender(event_sender)
        .with_state_watcher(state_watcher)
        .with_supervisor_task(task)
        .build_standard()
        .expect("standard handle should build");

    (FlowHandle::new(handle, empty_extras()), event_receiver)
}

#[tokio::test]
async fn start_if_ready_now_dispatches_run_in_ready_for_run() {
    let (handle, mut event_receiver) = flow_handle_for_start_admission(PipelineState::ReadyForRun);

    let outcome = handle
        .start_if_ready_now()
        .await
        .expect("ReadyForRun admission should succeed");

    assert_eq!(
        outcome,
        FlowStartControlOutcome::Submitted {
            observed_state: PipelineState::ReadyForRun
        }
    );
    assert!(
        matches!(event_receiver.try_recv(), Ok(PipelineFsmEvent::Start)),
        "ReadyForRun admission should dispatch Run"
    );
}

#[tokio::test]
async fn start_if_ready_now_accepts_running_without_dispatch() {
    let (handle, mut event_receiver) = flow_handle_for_start_admission(PipelineState::Running);

    let outcome = handle
        .start_if_ready_now()
        .await
        .expect("Running admission should succeed");

    assert_eq!(
        outcome,
        FlowStartControlOutcome::AlreadyRunning {
            state: PipelineState::Running
        }
    );
    assert!(
        matches!(event_receiver.try_recv(), Err(TryRecvError::Empty)),
        "Running admission must not dispatch duplicate Run"
    );
}

#[tokio::test]
async fn start_if_ready_now_rejects_non_ready_states_without_dispatch() {
    let cases = [
        PipelineState::Created,
        PipelineState::InitializingStages,
        PipelineState::StartingConsumers,
        PipelineState::CancellingChildren,
        PipelineState::FailingChildren {
            cause: "abort".to_string(),
        },
        PipelineState::Draining,
        PipelineState::Drained,
        PipelineState::Failed {
            reason: "failed".to_string(),
            failure_cause: None,
        },
    ];

    for state in cases {
        let (handle, mut event_receiver) = flow_handle_for_start_admission(state.clone());

        let outcome = handle
            .start_if_ready_now()
            .await
            .expect("rejection should be reported as a control outcome");

        assert_eq!(
            outcome,
            FlowStartControlOutcome::Rejected {
                state,
                reason: "pipeline is not ready for run"
            }
        );
        assert!(
            matches!(event_receiver.try_recv(), Err(TryRecvError::Empty)),
            "rejected admission must not dispatch Run"
        );
    }
}

#[tokio::test]
async fn wait_for_ready_returns_error_for_terminal_or_aborting_states() {
    let cases = [
        PipelineState::CancellingChildren,
        PipelineState::FailingChildren {
            cause: "abort".to_string(),
        },
        PipelineState::Draining,
        PipelineState::Drained,
        PipelineState::Failed {
            reason: "failed".to_string(),
            failure_cause: None,
        },
    ];

    for state in cases {
        let (handle, _event_receiver) = flow_handle_for_start_admission(state);

        let result = handle.wait_for_ready().await;

        assert!(result.is_err(), "terminal state must not satisfy readiness");
    }
}

#[tokio::test]
async fn start_accepts_coalesced_running_without_dispatch() {
    let (handle, mut event_receiver) = flow_handle_for_start_admission(PipelineState::Running);

    handle
        .start()
        .await
        .expect("Running should satisfy start without dispatching Run");

    assert!(
        matches!(event_receiver.try_recv(), Err(TryRecvError::Empty)),
        "start must not dispatch duplicate Run after observing Running"
    );
}

#[tokio::test]
async fn run_returns_failed_terminal_state_as_error() {
    let handle = flow_handle_that_finishes_in(PipelineState::Failed {
        reason: "terminal failure".to_string(),
        failure_cause: None,
    });

    let result = handle.run().await;
    assert!(
        result.is_err(),
        "Failed terminal state must surface as an error"
    );
    let err = result.expect_err("error should be present");
    let source = err.source().expect("source error should be present");

    assert!(
        source.to_string().contains("terminal failure"),
        "unexpected source error: {source}"
    );
}

#[tokio::test]
async fn run_returns_abort_terminal_state_as_error() {
    let handle = flow_handle_that_finishes_in(PipelineState::FailingChildren {
        cause: "abort requested".to_string(),
    });

    let result = handle.run().await;
    assert!(
        result.is_err(),
        "AbortRequested terminal state must surface as an error"
    );
    let err = result.expect_err("error should be present");
    let source = err.source().expect("source error should be present");

    assert!(
        source.to_string().contains("abort requested"),
        "unexpected source error: {source}"
    );
}

#[tokio::test]
async fn all_flow_completion_paths_report_execution_and_task_failures_consistently() {
    use super::super::termination::{ExecutionFailure, ExecutionOutcome, PublishedTermination};

    #[derive(Clone, Copy)]
    enum Exit {
        Returned,
        Failed,
        Panicked,
        Aborted,
    }
    let failed = PipelineState::Failed {
        reason: "FSM cleanup state is not the execution result".into(),
        failure_cause: None,
    };
    let cases = [
        (
            PipelineState::Drained,
            Some(ExecutionOutcome::Completed),
            Exit::Returned,
            None,
        ),
        (
            failed.clone(),
            Some(ExecutionOutcome::Cancelled {
                reason: "operator stop".into(),
            }),
            Exit::Returned,
            None,
        ),
        (
            failed.clone(),
            Some(ExecutionOutcome::Cancelled {
                reason: STOP_REASON_TIMEOUT.into(),
            }),
            Exit::Returned,
            None,
        ),
        (
            PipelineState::ReadyForRun,
            Some(ExecutionOutcome::NotStarted),
            Exit::Returned,
            None,
        ),
        (
            failed,
            Some(ExecutionOutcome::Failed(ExecutionFailure {
                reason: "acknowledged failure".into(),
                cause: None,
            })),
            Exit::Returned,
            Some("acknowledged failure"),
        ),
        (
            PipelineState::Drained,
            None,
            Exit::Returned,
            Some("without an acknowledged terminal outcome"),
        ),
        (
            PipelineState::Created,
            None,
            Exit::Failed,
            Some("task failed before readiness"),
        ),
        (
            PipelineState::Drained,
            Some(ExecutionOutcome::Completed),
            Exit::Panicked,
            Some("failure after publication"),
        ),
        (
            PipelineState::Created,
            None,
            Exit::Aborted,
            Some("Supervisor task was aborted"),
        ),
    ];
    for (state, outcome, exit, expected) in cases {
        for use_run in [false, true] {
            let (sender, _receiver, watcher) =
                ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(state.clone());
            let extras = empty_extras();
            let published = extras.published_outcome.clone();
            let outcome = outcome.clone();
            let task = tokio::spawn(async move {
                if let Some(outcome) = outcome {
                    let event_id = (!matches!(outcome, ExecutionOutcome::NotStarted))
                        .then(obzenflow_core::EventId::new);
                    published
                        .set(PublishedTermination { outcome, event_id })
                        .unwrap();
                }
                match exit {
                    Exit::Returned => Ok(()),
                    Exit::Failed => Err("task failed before readiness".into()),
                    Exit::Panicked => panic!("failure after publication"),
                    Exit::Aborted => std::future::pending().await,
                }
            });
            if matches!(exit, Exit::Aborted) {
                task.abort();
            }
            let handle = FlowHandle::new(
                HandleBuilder::new()
                    .with_event_sender(sender)
                    .with_state_watcher(watcher)
                    .with_supervisor_task(task)
                    .build_standard()
                    .unwrap(),
                extras,
            );
            let first = lifecycle::wait(&handle).await;
            let repeated = lifecycle::wait(&handle).await;
            let consumed = if use_run {
                tokio::time::timeout(Duration::from_secs(1), handle.run())
                    .await
                    .expect("completion must not hang waiting for readiness")
            } else {
                handle.wait_for_completion().await
            };
            let diagnostic = |result: Result<(), FlowError>| {
                result.map_err(|error| error.source().unwrap().to_string())
            };
            let first = diagnostic(first);
            assert_eq!(first, diagnostic(repeated));
            let consumed = diagnostic(consumed);
            if matches!(exit, Exit::Returned) {
                assert!(
                    first.is_ok(),
                    "resource observation does not classify journal facts"
                );
            } else {
                assert_eq!(first, consumed);
            }
            match expected {
                Some(message) => assert!(consumed.unwrap_err().contains(message)),
                None => consumed.unwrap(),
            }
        }
    }
}

#[tokio::test]
async fn framework_waits_do_not_consume_completion_or_skip_the_physical_join() {
    use super::super::termination::{ExecutionOutcome, PublishedTermination};
    let (sender, _receiver, watcher) =
        ChannelBuilder::<PipelineFsmEvent, PipelineState>::new().build(PipelineState::Drained);
    let extras = empty_extras();
    extras
        .published_outcome
        .set(PublishedTermination {
            outcome: ExecutionOutcome::Completed,
            event_id: Some(obzenflow_core::EventId::new()),
        })
        .unwrap();
    let (release, gate) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        gate.await.unwrap();
        Ok(())
    });
    let handle = FlowHandle::new(
        HandleBuilder::new()
            .with_event_sender(sender)
            .with_state_watcher(watcher)
            .with_supervisor_task(task)
            .build_standard()
            .unwrap(),
        extras,
    );
    let mut parked = Box::pin(lifecycle::wait(&handle));
    assert!(
        futures::poll!(&mut parked).is_pending(),
        "publication alone is not completion"
    );
    let mut dropped = Box::pin(lifecycle::wait(&handle));
    assert!(futures::poll!(&mut dropped).is_pending());
    drop(dropped);
    release.send(()).unwrap();
    lifecycle::wait(&handle).await.unwrap();
    parked.await.unwrap();
    lifecycle::wait(&handle).await.unwrap();
    handle.wait_for_completion().await.unwrap();
}

#[tokio::test]
async fn run_allows_successful_terminal_state() {
    let handle = flow_handle_that_finishes_in(PipelineState::Drained);

    handle
        .run()
        .await
        .expect("Drained terminal state should remain successful");
}
