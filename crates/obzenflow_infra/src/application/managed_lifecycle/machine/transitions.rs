// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transition decisions only; actions enqueue commands for the application driver.

use obzenflow_fsm::Transition;
use obzenflow_runtime::__private::lifecycle::FlowStopStatus;
use std::time::Duration;

use super::model::{Action, Context, Event, FailureOrigin, JoinBudget, Outcome, State};
use super::settlement::{
    CompletionBound, Escalation, FlowActivity, Settlement, StopCommand, StopReason,
};
use crate::application::config::StartupMode;

pub(super) fn reduce<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> obzenflow_fsm::types::BoxFuture<'a, obzenflow_fsm::types::FsmResult<Transition<State, Action>>>
{
    Box::pin(async move {
        let mut next = state.clone();
        let mut actions = Vec::new();
        match (state, event) {
            (_, Event::Failure(origin)) => {
                ctx.outcome = match (ctx.outcome, origin) {
                    (_, FailureOrigin::Host) | (Outcome::HostFailure, _) => Outcome::HostFailure,
                    _ => Outcome::ApplicationFailure,
                };
            }
            (State::Preparing, Event::HostBound(StartupMode::Auto)) => {
                next = State::Starting;
                actions.push(Action::StartFlow);
            }
            (State::Preparing, Event::HostBound(StartupMode::Manual))
            | (State::Starting, Event::Started) => next = State::Active,
            (State::Preparing, Event::Standalone) => {
                next = State::RunningStandalone;
                actions.push(Action::RunStandalone);
            }
            (State::Preparing | State::Starting | State::Active, Event::Stop(reason, input)) => {
                let before_run = matches!(reason, StopReason::BeforeRun);
                let command = if matches!(input.activity, FlowActivity::Terminal) {
                    None
                } else if before_run {
                    Some(StopCommand::Cancel)
                } else {
                    match (&input.admitted, reason, input.activity) {
                        (FlowStopStatus::Graceful { .. }, StopReason::Cancel, _) => {
                            Some(StopCommand::Cancel)
                        }
                        (FlowStopStatus::NotRequested, _, FlowActivity::BeforeRun)
                        | (FlowStopStatus::NotRequested, StopReason::Cancel, _) => {
                            Some(StopCommand::Cancel)
                        }
                        (FlowStopStatus::NotRequested, _, _) => Some(StopCommand::Graceful),
                        _ => None,
                    }
                };
                let mut settlement = Settlement {
                    bound: if before_run {
                        CompletionBound::BeforeRun(input.at + ctx.grace + ctx.grace)
                    } else {
                        CompletionBound::AwaitingAdmission(input.at + ctx.grace)
                    },
                    escalation: match command {
                        Some(StopCommand::Cancel) => Escalation::Requested(StopCommand::Cancel),
                        _ => Escalation::AwaitingAdmission,
                    },
                };
                settlement.observe(&input.admitted, ctx.grace);
                next = State::SettlingFlow(settlement);
                actions.push(Action::SettleFlow(command));
            }
            (State::SettlingFlow(settlement), Event::Admission(status)) => {
                let mut settlement = settlement.clone();
                settlement.observe(status, ctx.grace);
                next = State::SettlingFlow(settlement);
            }
            (State::SettlingFlow(settlement), Event::RepeatedSignal)
                if !matches!(
                    settlement.escalation,
                    Escalation::Cancelling | Escalation::Requested(StopCommand::Cancel)
                ) =>
            {
                let mut settlement = settlement.clone();
                settlement.escalation = Escalation::Requested(StopCommand::Cancel);
                next = State::SettlingFlow(settlement);
                actions.push(Action::SendStop(StopCommand::Cancel));
            }
            (State::SettlingFlow(settlement), Event::GracefulExpired)
                if matches!(settlement.escalation, Escalation::Graceful(_)) =>
            {
                let mut settlement = settlement.clone();
                settlement.escalation = Escalation::Requested(StopCommand::Timeout);
                next = State::SettlingFlow(settlement);
                actions.push(Action::SendStop(StopCommand::Timeout));
            }
            (State::SettlingFlow(_), Event::PublicationObserved | Event::CompletionExpired) => {
                next = State::AbortingFlow;
                actions.push(Action::AbortFlow);
            }
            (State::Preparing, Event::PreparationFailed)
            | (State::AbortingFlow, Event::FlowAborted)
            | (State::RunningStandalone, Event::StandaloneReturned) => {
                next = State::StoppingMetrics;
                actions.push(Action::StopMetrics);
            }
            (State::StoppingMetrics, Event::MetricsStopped) => {
                next = State::ClosingHost;
                actions.push(Action::CloseHost);
            }
            (State::ClosingHost, Event::HostClosed { at }) => {
                next = State::Deregistering {
                    deadline: *at + Duration::from_secs(5),
                };
                actions.push(Action::AwaitDeregistration);
            }
            (State::ClosingHost, Event::HostAbsent { at })
            | (
                State::Deregistering { .. } | State::JoiningDeregisteredHeartbeat,
                Event::HeartbeatJoined { at },
            ) => {
                next = State::FlushingMetrics {
                    deadline: *at + ctx.grace,
                };
                actions.push(Action::FlushMetrics);
            }
            (State::Deregistering { .. }, Event::DeregistrationExpired) => {
                next = State::JoiningDeregisteredHeartbeat;
                actions.push(Action::AbortDeregistration);
            }
            (
                State::FlushingMetrics { .. },
                Event::MetricsFlushed { at } | Event::FlushExpired { at },
            ) => {
                next = State::JoiningLeftoverHeartbeat(JoinBudget::new(*at, ctx.grace));
                actions.push(Action::JoinLeftoverHeartbeat);
            }
            (State::JoiningLeftoverHeartbeat(_), Event::LeftoverHeartbeatJoined { at }) => {
                next = State::JoiningTasks(JoinBudget::new(*at, ctx.grace));
                actions.push(Action::JoinTasks);
            }
            (State::JoiningTasks(_), Event::TasksJoined) => next = State::Finished,
            (State::JoiningTasks(JoinBudget::Within { deadline }), Event::JoinBudgetExpired) => {
                next = State::JoiningTasks(JoinBudget::Exceeded {
                    deadline: *deadline,
                });
                actions.push(Action::DiagnoseJoinBudget);
            }
            (
                State::JoiningLeftoverHeartbeat(JoinBudget::Within { deadline }),
                Event::JoinBudgetExpired,
            ) => {
                next = State::JoiningLeftoverHeartbeat(JoinBudget::Exceeded {
                    deadline: *deadline,
                });
                actions.push(Action::DiagnoseJoinBudget);
            }
            _ => {}
        }
        Ok(Transition {
            next_state: next,
            actions,
        })
    })
}
