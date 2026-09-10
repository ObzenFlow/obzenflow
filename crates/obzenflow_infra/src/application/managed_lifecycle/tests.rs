// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::config::StartupMode;
use super::*;
use machine::{completion_deadline, JoinBudget};
use obzenflow_runtime::__private::lifecycle::{FlowCancelCause, FlowStopStatus};
use tokio::sync::oneshot;

struct Cancelled(Option<oneshot::Sender<()>>);
impl Drop for Cancelled {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

async fn pending_task() -> (ApplicationTask, oneshot::Receiver<()>) {
    let (started_tx, started_rx) = oneshot::channel();
    let (cancelled_tx, cancelled_rx) = oneshot::channel();
    let task = ApplicationTask(tokio::spawn(async move {
        let _cancelled = Cancelled(Some(cancelled_tx));
        started_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    }));
    started_rx.await.unwrap();
    (task, cancelled_rx)
}

#[tokio::test]
async fn stopping_application_task_joins_its_cleanup() {
    let (task, mut cancelled) = pending_task().await;
    assert!(abort_and_join(vec![task]).await.is_empty());
    assert_eq!(cancelled.try_recv(), Ok(()));
}

#[tokio::test]
async fn dropping_application_task_cancels_pending_work() {
    let (task, cancelled) = pending_task().await;
    drop(task);
    cancelled.await.unwrap();
}

#[test]
fn observer_deadlines_come_from_admission_even_when_observed_late() {
    let now = Instant::now();
    let grace = Duration::from_secs(5);
    let deadline = now + Duration::from_secs(3);
    let late_observer = now + Duration::from_secs(100);
    for (status, expected) in [
        (FlowStopStatus::Graceful { deadline }, deadline + grace),
        (
            FlowStopStatus::Cancelling {
                admitted_at: deadline + Duration::from_secs(2),
                cause: FlowCancelCause::GracefulTimeout,
                graceful_deadline: Some(deadline),
            },
            deadline + grace,
        ),
        (
            FlowStopStatus::Cancelling {
                admitted_at: now,
                cause: FlowCancelCause::Requested,
                graceful_deadline: None,
            },
            now + grace,
        ),
    ] {
        assert_eq!(completion_deadline(&status, late_observer, grace), expected);
    }
}

fn context() -> Context {
    Context {
        grace: Duration::from_secs(5),
        outcome: Outcome::Success,
        on_terminal: OnTerminalArg::Exit,
        commands: Vec::new(),
    }
}

#[tokio::test]
async fn host_error_precedence_survives_cleanup_and_duplicate_failure_observations() {
    let at = TokioInstant::now();
    let hosted = vec![
        (Event::HostBound(StartupMode::Auto), "Starting"),
        (Event::Started, "Active"),
        (
            Event::Stop(
                StopReason::Graceful,
                StopInput {
                    activity: FlowActivity::Executing,
                    admitted: FlowStopStatus::NotRequested,
                    at: Instant::now(),
                },
            ),
            "SettlingFlow",
        ),
        (Event::PublicationObserved, "AbortingFlow"),
        (Event::FlowAborted, "StoppingMetrics"),
        (Event::MetricsStopped, "ClosingHost"),
        (Event::HostClosed { at }, "Deregistering"),
        (Event::DeregistrationExpired, "JoiningDeregisteredHeartbeat"),
        (Event::HeartbeatJoined { at }, "FlushingMetrics"),
        (Event::MetricsFlushed { at }, "JoiningLeftoverHeartbeat"),
        (Event::LeftoverHeartbeatJoined { at }, "JoiningTasks"),
        (Event::TasksJoined, "Finished"),
    ];
    let standalone = vec![
        (Event::Standalone, "RunningStandalone"),
        (Event::StandaloneReturned, "StoppingMetrics"),
    ];
    for trace in [hosted, standalone] {
        for origins in [
            [FailureOrigin::Application, FailureOrigin::Host],
            [FailureOrigin::Host, FailureOrigin::Application],
        ] {
            let mut machine = machine::new();
            let mut context = context();
            for (event, phase) in &trace {
                let before = machine.state().clone();
                // Exercise precedence afresh at every live phase, so a missing Failure
                // registration cannot hide behind an outcome selected at an earlier phase.
                context.outcome = Outcome::Success;
                for origin in origins.into_iter().chain(origins) {
                    assert!(machine
                        .handle(Event::Failure(origin), &mut context)
                        .await
                        .unwrap()
                        .is_empty());
                    assert_eq!(machine.state(), &before, "failure is not completion");
                    assert_ne!(context.outcome, Outcome::Success);
                    if origin == FailureOrigin::Host {
                        assert_eq!(context.outcome, Outcome::HostFailure);
                    }
                }
                assert_eq!(context.outcome, Outcome::HostFailure);
                machine.handle(event.clone(), &mut context).await.unwrap();
                assert_eq!(machine.state().variant_name(), *phase);
                assert_eq!(context.outcome, Outcome::HostFailure);
            }
        }
    }
}

#[tokio::test]
async fn transition_matrix_preserves_startup_admission_and_escalation() {
    for startup in [StartupMode::Auto, StartupMode::Manual] {
        for activity in [
            FlowActivity::BeforeRun,
            FlowActivity::Executing,
            FlowActivity::Terminal,
        ] {
            for reason in [StopReason::Graceful, StopReason::Cancel] {
                let mut machine = machine::new();
                let mut context = context();
                let actions = machine
                    .handle(Event::HostBound(startup), &mut context)
                    .await
                    .unwrap();
                assert_eq!(
                    actions,
                    if startup == StartupMode::Auto {
                        vec![Action::StartFlow]
                    } else {
                        vec![]
                    }
                );
                let at = Instant::now();
                let actions = machine
                    .handle(
                        Event::Stop(
                            reason,
                            StopInput {
                                activity,
                                admitted: FlowStopStatus::NotRequested,
                                at,
                            },
                        ),
                        &mut context,
                    )
                    .await
                    .unwrap();
                let command = match (activity, reason) {
                    (FlowActivity::Terminal, _) => None,
                    (FlowActivity::BeforeRun, _) | (_, StopReason::Cancel) => {
                        Some(StopCommand::Cancel)
                    }
                    _ => Some(StopCommand::Graceful),
                };
                assert_eq!(actions, [Action::SettleFlow(command)]);
                let admission = at + Duration::from_secs(2);
                let awaiting_admission = machine.state().clone();
                for event in [Event::StopSent, Event::StopSent, Event::GracefulExpired] {
                    assert!(machine
                        .handle(event, &mut context)
                        .await
                        .unwrap()
                        .is_empty());
                    assert_eq!(machine.state(), &awaiting_admission);
                }
                let State::SettlingFlow(settlement) = machine.state() else {
                    panic!("must settle")
                };
                assert_eq!(
                    settlement.completion_deadline(),
                    at + context.grace,
                    "send is not admission"
                );
                machine
                    .handle(
                        Event::Admission(FlowStopStatus::Graceful {
                            deadline: admission,
                        }),
                        &mut context,
                    )
                    .await
                    .unwrap();
                let bound = admission + context.grace;
                for repetition in 0..3 {
                    let actions = machine
                        .handle(Event::RepeatedSignal, &mut context)
                        .await
                        .unwrap();
                    assert_eq!(
                        actions,
                        if repetition == 0 && command != Some(StopCommand::Cancel) {
                            vec![Action::SendStop(StopCommand::Cancel)]
                        } else {
                            vec![]
                        }
                    );
                    let cancellation_requested = machine.state().clone();
                    for event in [Event::StopSent, Event::GracefulExpired] {
                        assert!(machine
                            .handle(event, &mut context)
                            .await
                            .unwrap()
                            .is_empty());
                        assert_eq!(machine.state(), &cancellation_requested);
                    }
                    machine
                        .handle(
                            Event::Admission(FlowStopStatus::Graceful {
                                deadline: admission,
                            }),
                            &mut context,
                        )
                        .await
                        .unwrap();
                    let State::SettlingFlow(settlement) = machine.state() else {
                        panic!("must settle")
                    };
                    assert_eq!(settlement.completion_deadline(), bound);
                    assert_eq!(
                        settlement.graceful_deadline(),
                        None,
                        "cancellation request suppresses timeout repetition"
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn graceful_expiry_requests_escalation_once_and_preserves_runtime_bound() {
    let mut machine = machine::new();
    let mut context = context();
    let at = Instant::now();
    let deadline = at + Duration::from_secs(2);
    machine
        .handle(
            Event::Stop(
                StopReason::Graceful,
                StopInput {
                    activity: FlowActivity::Executing,
                    admitted: FlowStopStatus::Graceful { deadline },
                    at,
                },
            ),
            &mut context,
        )
        .await
        .unwrap();
    assert_eq!(
        machine
            .handle(Event::GracefulExpired, &mut context)
            .await
            .unwrap(),
        [Action::SendStop(StopCommand::Timeout)]
    );
    let timeout_requested = machine.state().clone();
    assert!(machine
        .handle(Event::GracefulExpired, &mut context)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(machine.state(), &timeout_requested);
    machine
        .handle(
            Event::Admission(FlowStopStatus::Cancelling {
                admitted_at: at + Duration::from_secs(20),
                cause: FlowCancelCause::GracefulTimeout,
                graceful_deadline: Some(deadline),
            }),
            &mut context,
        )
        .await
        .unwrap();
    let State::SettlingFlow(settlement) = machine.state() else {
        panic!("must settle")
    };
    assert_eq!(settlement.completion_deadline(), deadline + context.grace);
    let cancelling = machine.state().clone();
    for event in [
        Event::RepeatedSignal,
        Event::GracefulExpired,
        Event::StopSent,
    ] {
        assert!(machine
            .handle(event, &mut context)
            .await
            .unwrap()
            .is_empty());
        assert_eq!(machine.state(), &cancelling);
    }
}

#[tokio::test(start_paused = true)]
async fn both_join_budgets_diagnose_once_and_require_their_own_completion() {
    let mut machine = machine::new();
    let mut context = context();
    let at = TokioInstant::now();
    let mut cleanup_actions = Vec::new();
    for event in [
        Event::PreparationFailed,
        Event::MetricsStopped,
        Event::HostAbsent { at },
        Event::FlushExpired { at },
    ] {
        cleanup_actions.extend(machine.handle(event, &mut context).await.unwrap());
    }
    assert_eq!(
        cleanup_actions,
        [
            Action::StopMetrics,
            Action::CloseHost,
            Action::FlushMetrics,
            Action::JoinLeftoverHeartbeat,
        ]
    );

    for leftover_heartbeat in [true, false] {
        let deadline = TokioInstant::now() + context.grace;
        let within = JoinBudget::Within { deadline };
        assert_eq!(
            *machine.state(),
            if leftover_heartbeat {
                State::JoiningLeftoverHeartbeat(within)
            } else {
                State::JoiningTasks(within)
            }
        );
        tokio::time::advance(context.grace + Duration::from_secs(1)).await;
        assert_eq!(
            machine
                .handle(Event::JoinBudgetExpired, &mut context)
                .await
                .unwrap(),
            [Action::DiagnoseJoinBudget]
        );
        let exceeded = JoinBudget::Exceeded { deadline };
        let waiting = if leftover_heartbeat {
            State::JoiningLeftoverHeartbeat(exceeded)
        } else {
            State::JoiningTasks(exceeded)
        };
        assert_eq!(machine.state(), &waiting);
        let wrong_completion = if leftover_heartbeat {
            Event::TasksJoined
        } else {
            Event::LeftoverHeartbeatJoined {
                at: TokioInstant::now(),
            }
        };
        for event in [
            Event::JoinBudgetExpired,
            Event::JoinBudgetExpired,
            Event::RepeatedSignal,
            Event::PreparationFailed,
            wrong_completion,
        ] {
            assert!(machine
                .handle(event, &mut context)
                .await
                .unwrap()
                .is_empty());
            assert_eq!(machine.state(), &waiting);
        }
        assert_eq!(context.outcome, Outcome::Success);
        if leftover_heartbeat {
            assert_eq!(
                machine
                    .handle(
                        Event::LeftoverHeartbeatJoined {
                            at: TokioInstant::now(),
                        },
                        &mut context,
                    )
                    .await
                    .unwrap(),
                [Action::JoinTasks]
            );
        }
    }
    assert!(machine
        .handle(Event::TasksJoined, &mut context)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(*machine.state(), State::Finished);
}

#[tokio::test]
async fn runtime_completion_during_startup_requires_teardown_before_cleanup() {
    let mut machine = machine::new();
    let mut context = context();
    assert_eq!(
        machine
            .handle(Event::HostBound(StartupMode::Auto), &mut context)
            .await
            .unwrap(),
        [Action::StartFlow]
    );
    assert_eq!(*machine.state(), State::Starting);
    machine
        .handle(Event::Failure(FailureOrigin::Application), &mut context)
        .await
        .unwrap();
    assert_eq!(
        machine
            .handle(Event::PublicationObserved, &mut context)
            .await
            .unwrap(),
        [Action::AbortFlow]
    );
    assert_eq!(*machine.state(), State::AbortingFlow);
    assert!(machine
        .handle(Event::Started, &mut context)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        *machine.state(),
        State::AbortingFlow,
        "late readiness cannot restart execution"
    );
    assert_eq!(
        machine
            .handle(Event::FlowAborted, &mut context)
            .await
            .unwrap(),
        [Action::StopMetrics]
    );
    assert_eq!(context.outcome, Outcome::ApplicationFailure);
}

/// Drive the real action completions to a named cleanup boundary for controlled I/O tests.
async fn advance_to(driver: &mut ApplicationLifecycle, reached: impl Fn(&State) -> bool) {
    while !reached(driver.machine.state()) {
        let observed = pending(&mut driver.operation).await;
        driver.operation = None;
        let event = driver.observe(observed);
        driver.dispatch(event).await;
    }
}

#[tokio::test(start_paused = true)]
async fn join_budget_keeps_blocking_task_owned_and_cancels_the_entire_group() {
    let (started_tx, started_rx) = oneshot::channel();
    let (released_tx, released_rx) = oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let blocking = ApplicationTask(tokio::task::spawn_blocking(move || {
        let _released = Cancelled(Some(released_tx));
        started_tx.send(()).unwrap();
        let _ = release_rx.recv();
    }));
    started_rx.await.unwrap();
    let (other, other_cancelled) = pending_task().await;
    let mut driver = ApplicationLifecycle::new(Duration::from_secs(5), OnTerminalArg::Exit);
    driver.tasks = vec![blocking, other];
    driver.dispatch(Event::PreparationFailed).await;
    advance_to(&mut driver, |state| matches!(state, State::JoiningTasks(_))).await;
    assert!(futures::poll!(driver.operation.as_mut().unwrap()).is_pending());
    other_cancelled.await.unwrap();
    let State::JoiningTasks(JoinBudget::Within { deadline }) = *driver.machine.state() else {
        panic!("joining")
    };
    let started = driver.operation_started;
    let allocation =
        std::ptr::from_ref(driver.operation.as_ref().unwrap().as_ref().get_ref()).cast::<()>();
    let allocated_bytes =
        std::mem::size_of_val(driver.operation.as_ref().unwrap().as_ref().get_ref());
    tokio::time::advance(Duration::from_secs(6)).await;
    driver.dispatch(Event::JoinBudgetExpired).await;
    assert_eq!(
        *driver.machine.state(),
        State::JoiningTasks(JoinBudget::Exceeded { deadline })
    );
    for event in [
        Event::RepeatedSignal,
        Event::JoinBudgetExpired,
        Event::PreparationFailed,
    ] {
        driver.dispatch(event).await;
        assert_eq!(driver.operation_started, started);
        assert_eq!(
            std::ptr::from_ref(driver.operation.as_ref().unwrap().as_ref().get_ref()).cast::<()>(),
            allocation
        );
        assert_eq!(
            std::mem::size_of_val(driver.operation.as_ref().unwrap().as_ref().get_ref()),
            allocated_bytes
        );
        assert!(futures::poll!(driver.operation.as_mut().unwrap()).is_pending());
    }
    let mut finishing = Box::pin(driver.drive(
        Event::RepeatedSignal,
        #[cfg(feature = "warp-server")]
        None,
    ));
    assert!(
        futures::poll!(&mut finishing).is_pending(),
        "ordinary return requires a join"
    );
    release_tx.send(()).unwrap();
    finishing.await;
    assert!(
        released_rx.await.is_ok(),
        "task termination precedes return"
    );
    driver.take_result().unwrap();
}

#[tokio::test]
async fn auxiliary_panic_preserves_primary_error_and_retains_original_join_error() {
    let task = ApplicationTask(tokio::spawn(async { panic!("auxiliary panic witness") }));
    tokio::task::yield_now().await;
    let mut driver = ApplicationLifecycle::new(Duration::from_secs(5), OnTerminalArg::Exit);
    driver.tasks.push(task);
    let result = driver
        .finish(Err(ApplicationError::InvalidConfiguration(
            "primary witness".into(),
        )))
        .await;
    assert!(
        matches!(result, Err(ApplicationError::InvalidConfiguration(message)) if message == "primary witness")
    );
    assert_eq!(driver.auxiliary_errors.len(), 1);
    assert!(driver.auxiliary_errors[0].is_panic());
}

#[tokio::test(start_paused = true)]
async fn flush_timeout_cancels_only_the_attempt_and_preserves_the_selected_result() {
    let mut driver = ApplicationLifecycle::new(Duration::from_secs(5), OnTerminalArg::Exit);
    driver.failure = Some(Failure::Application(
        ApplicationError::InvalidConfiguration("primary witness".into()),
    ));
    driver.dispatch(Event::PreparationFailed).await;
    advance_to(&mut driver, |state| {
        matches!(state, State::FlushingMetrics { .. })
    })
    .await;
    let (cancelled_tx, mut cancelled_rx) = oneshot::channel();
    let witness = Cancelled(Some(cancelled_tx));
    driver.begin(Box::pin(async move {
        let _witness = witness;
        std::future::pending::<Observed>().await
    }));
    let mut finishing = Box::pin(driver.drive(
        Event::RepeatedSignal,
        #[cfg(feature = "warp-server")]
        None,
    ));
    assert!(futures::poll!(&mut finishing).is_pending());
    tokio::time::advance(Duration::from_secs(5)).await;
    finishing.await;
    assert_eq!(cancelled_rx.try_recv(), Ok(()));
    assert!(
        matches!(driver.take_result(), Err(ApplicationError::InvalidConfiguration(message)) if message == "primary witness")
    );
}

#[tokio::test]
async fn dropping_driver_cancels_resources_without_claiming_finished() {
    let (task, cancelled) = pending_task().await;
    let (metrics, metrics_cancelled) = pending_task().await;
    let mut driver = ApplicationLifecycle::new(Duration::from_secs(5), OnTerminalArg::Exit);
    driver.tasks.push(task);
    driver.metrics_collector = Some(metrics);
    driver.dispatch(Event::PreparationFailed).await;
    assert!(!matches!(driver.machine.state(), State::Finished));
    drop(driver);
    cancelled.await.unwrap();
    metrics_cancelled.await.unwrap();
}

#[cfg(feature = "studio-registration")]
#[tokio::test(start_paused = true)]
async fn deregistration_has_its_full_grace_before_generic_cancellation() {
    let (heartbeat, heartbeat_cancelled) = pending_task().await;
    let (generic, mut generic_cancelled) = pending_task().await;
    let mut driver = ApplicationLifecycle::new(Duration::from_secs(1), OnTerminalArg::Exit);
    driver.heartbeat = Some(heartbeat);
    driver.tasks.push(generic);
    driver.dispatch(Event::PreparationFailed).await;
    advance_to(&mut driver, |state| matches!(state, State::ClosingHost)).await;
    // Observe a normally closed host; its existing close owner is tested separately.
    driver.operation = None;
    driver
        .dispatch(Event::HostClosed {
            at: TokioInstant::now(),
        })
        .await;
    assert!(futures::poll!(driver.operation.as_mut().unwrap()).is_pending());
    tokio::time::advance(Duration::from_secs(4)).await;
    assert!(generic_cancelled.try_recv().is_err());
    let mut finishing = Box::pin(driver.drive(Event::RepeatedSignal, None));
    assert!(futures::poll!(&mut finishing).is_pending());
    tokio::time::advance(Duration::from_secs(1)).await;
    finishing.await;
    heartbeat_cancelled.await.unwrap();
    assert_eq!(generic_cancelled.try_recv(), Ok(()));
    driver.take_result().unwrap();
}
