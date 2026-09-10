// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::machine;
use super::settlement::Settlement;
use super::*;
use crate::application::config::{OnTerminalArg, StartupMode};
use obzenflow_core::event::types::DurationMs;
use obzenflow_core::event::{PipelineCancellationCause, PipelineStopAdmission};
use obzenflow_fsm::StateVariant;
use std::time::Duration;
use tokio::time::Instant as TokioInstant;

#[test]
fn observed_admission_suppresses_duplicate_stop_requests() {
    for admission in [
        PipelineStopAdmission::Graceful {
            timeout_ms: DurationMs(3_000),
        },
        PipelineStopAdmission::Cancel {
            cause: PipelineCancellationCause::GracefulTimeout,
        },
        PipelineStopAdmission::Cancel {
            cause: PipelineCancellationCause::Requested,
        },
    ] {
        let input = StopInput {
            activity: FlowActivity::Executing,
            admitted: Some(admission.clone()),
        };
        let (settlement, command) =
            Settlement::begin(StopReason::Graceful, &input, Duration::from_secs(5));
        assert_eq!(command, None);
        assert_eq!(settlement.admitted, Some(admission));
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
                    admitted: None,
                },
            ),
            "SettlingFlow",
        ),
        (Event::PublicationObserved, "StoppingMetrics"),
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
                let actions = machine
                    .handle(
                        Event::Stop(
                            reason,
                            StopInput {
                                activity,
                                admitted: None,
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
                let admission = Some(PipelineStopAdmission::Graceful {
                    timeout_ms: DurationMs(2_000),
                });
                let awaiting_admission = machine.state().clone();
                for event in [Event::StopSent, Event::StopSent] {
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
                assert_eq!(settlement.admitted, None, "send is not admission");
                machine
                    .handle(Event::Admission(admission.clone()), &mut context)
                    .await
                    .unwrap();
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
                    for event in [Event::StopSent, Event::StopSent] {
                        assert!(machine
                            .handle(event, &mut context)
                            .await
                            .unwrap()
                            .is_empty());
                        assert_eq!(machine.state(), &cancellation_requested);
                    }
                    machine
                        .handle(Event::Admission(admission.clone()), &mut context)
                        .await
                        .unwrap();
                    let State::SettlingFlow(settlement) = machine.state() else {
                        panic!("must settle")
                    };
                    assert_eq!(settlement.admitted, admission);
                    assert_eq!(settlement.requested, Some(StopCommand::Cancel));
                }
            }
        }
    }
}

#[tokio::test]
async fn runtime_timeout_admission_is_observed_without_an_application_timeout_command() {
    let mut machine = machine::new();
    let mut context = context();
    machine
        .handle(
            Event::Stop(
                StopReason::Graceful,
                StopInput {
                    activity: FlowActivity::Executing,
                    admitted: Some(PipelineStopAdmission::Graceful {
                        timeout_ms: DurationMs(2_000),
                    }),
                },
            ),
            &mut context,
        )
        .await
        .unwrap();
    let cancellation = Some(PipelineStopAdmission::Cancel {
        cause: PipelineCancellationCause::GracefulTimeout,
    });
    machine
        .handle(Event::Admission(cancellation.clone()), &mut context)
        .await
        .unwrap();
    let State::SettlingFlow(settlement) = machine.state() else {
        panic!("must settle")
    };
    assert_eq!(settlement.admitted, cancellation);
    let cancelling = machine.state().clone();
    for event in [Event::RepeatedSignal, Event::StopSent] {
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
async fn joined_journal_observation_during_startup_proceeds_to_application_cleanup() {
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
        [Action::StopMetrics]
    );
    assert_eq!(*machine.state(), State::StoppingMetrics);
    assert!(machine
        .handle(Event::Started, &mut context)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        *machine.state(),
        State::StoppingMetrics,
        "late readiness cannot restart execution"
    );
    assert_eq!(context.outcome, Outcome::ApplicationFailure);
}
