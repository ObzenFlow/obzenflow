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
    for origins in [
        [FailureOrigin::Application, FailureOrigin::Host],
        [FailureOrigin::Host, FailureOrigin::Application],
    ] {
        let mut machine = machine::new();
        let mut context = context();
        machine
            .handle(Event::PreparationFailed, &mut context)
            .await
            .unwrap();
        for origin in origins {
            assert!(machine
                .handle(Event::Failure(origin), &mut context)
                .await
                .unwrap()
                .is_empty());
        }
        machine
            .handle(Event::MetricsStopped, &mut context)
            .await
            .unwrap();
        for origin in origins {
            assert!(machine
                .handle(Event::Failure(origin), &mut context)
                .await
                .unwrap()
                .is_empty());
        }
        assert_eq!(context.outcome, Outcome::HostFailure);
        assert_eq!(
            *machine.state(),
            State::ClosingHost,
            "an error is not cleanup completion"
        );
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
                machine.handle(Event::StopSent, &mut context).await.unwrap();
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
                for _ in 0..3 {
                    machine
                        .handle(Event::RepeatedSignal, &mut context)
                        .await
                        .unwrap();
                    machine.handle(Event::StopSent, &mut context).await.unwrap();
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
    assert!(machine
        .handle(Event::GracefulExpired, &mut context)
        .await
        .unwrap()
        .is_empty());
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
    assert!(machine
        .handle(Event::RepeatedSignal, &mut context)
        .await
        .unwrap()
        .is_empty());
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
