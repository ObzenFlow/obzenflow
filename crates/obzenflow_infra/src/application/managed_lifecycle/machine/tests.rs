// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::machine;
use super::*;
use crate::application::config::{OnTerminalArg, StartupMode};
use obzenflow_fsm::StateVariant;
use obzenflow_runtime::__private::lifecycle::{FlowCancelCause, FlowStopStatus};
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;

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
