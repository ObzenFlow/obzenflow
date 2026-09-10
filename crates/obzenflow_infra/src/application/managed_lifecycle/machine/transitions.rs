// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transition decisions only; actions enqueue commands for the application driver.
//!
//! The FSM declaration selects each handler. Payload checks below diagnose a registration
//! mismatch; they do not select between lifecycle edges. Duplicate inputs are handled
//! explicitly within the policy for their registered edge.

use obzenflow_fsm::{
    types::{BoxFuture, FsmResult},
    Transition,
};
use std::time::Duration;
use tokio::time::Instant as TokioInstant;

use super::model::{Action, Context, Event, FailureOrigin, JoinBudget, Outcome, State};
use super::settlement::{Escalation, Settlement, StopCommand};
use crate::application::config::StartupMode;

pub(super) fn record_failure<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let Event::Failure(origin) = event else {
            return Ok(registration_mismatch("record_failure", state, event));
        };
        ctx.outcome = match (ctx.outcome, origin) {
            (_, FailureOrigin::Host) | (Outcome::HostFailure, _) => Outcome::HostFailure,
            _ => Outcome::ApplicationFailure,
        };
        Ok(stay(state))
    })
}

pub(super) fn host_bound<'a>(
    state: &'a State,
    event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let Event::HostBound(startup) = event else {
            return Ok(registration_mismatch("host_bound", state, event));
        };
        Ok(match startup {
            StartupMode::Auto => Transition {
                next_state: State::Starting,
                actions: vec![Action::StartFlow],
            },
            StartupMode::Manual => Transition {
                next_state: State::Active,
                actions: vec![],
            },
        })
    })
}

pub(super) fn started<'a>(
    _state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async {
        Ok(Transition {
            next_state: State::Active,
            actions: vec![],
        })
    })
}

pub(super) fn run_standalone<'a>(
    _state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async {
        Ok(Transition {
            next_state: State::RunningStandalone,
            actions: vec![Action::RunStandalone],
        })
    })
}

pub(super) fn begin_settlement<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let Event::Stop(reason, input) = event else {
            return Ok(registration_mismatch("begin_settlement", state, event));
        };
        let (settlement, command) = Settlement::begin(*reason, input, ctx.grace);
        Ok(Transition {
            next_state: State::SettlingFlow(settlement),
            actions: vec![Action::SettleFlow(command)],
        })
    })
}

pub(super) fn observe_admission<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let (State::SettlingFlow(settlement), Event::Admission(status)) = (state, event) else {
            return Ok(registration_mismatch("observe_admission", state, event));
        };
        let mut settlement = settlement.clone();
        settlement.observe(status, ctx.grace);
        Ok(Transition {
            next_state: State::SettlingFlow(settlement),
            actions: vec![],
        })
    })
}

pub(super) fn acknowledge_stop_send<'a>(
    state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    // Sending a request neither establishes admission nor renews its bound.
    Box::pin(async move { Ok(stay(state)) })
}

pub(super) fn request_cancellation<'a>(
    state: &'a State,
    event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let State::SettlingFlow(settlement) = state else {
            return Ok(registration_mismatch("request_cancellation", state, event));
        };
        match settlement.escalation {
            Escalation::Cancelling | Escalation::Requested(StopCommand::Cancel) => Ok(stay(state)),
            Escalation::AwaitingAdmission
            | Escalation::Graceful(_)
            | Escalation::Requested(StopCommand::Graceful | StopCommand::Timeout) => {
                let mut settlement = settlement.clone();
                settlement.escalation = Escalation::Requested(StopCommand::Cancel);
                Ok(Transition {
                    next_state: State::SettlingFlow(settlement),
                    actions: vec![Action::SendStop(StopCommand::Cancel)],
                })
            }
        }
    })
}

pub(super) fn graceful_expired<'a>(
    state: &'a State,
    event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let State::SettlingFlow(settlement) = state else {
            return Ok(registration_mismatch("graceful_expired", state, event));
        };
        match settlement.escalation {
            Escalation::Graceful(_) => {
                let mut settlement = settlement.clone();
                settlement.escalation = Escalation::Requested(StopCommand::Timeout);
                Ok(Transition {
                    next_state: State::SettlingFlow(settlement),
                    actions: vec![Action::SendStop(StopCommand::Timeout)],
                })
            }
            Escalation::AwaitingAdmission | Escalation::Requested(_) | Escalation::Cancelling => {
                Ok(stay(state))
            }
        }
    })
}

pub(super) fn abort_flow<'a>(
    _state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async {
        Ok(Transition {
            next_state: State::AbortingFlow,
            actions: vec![Action::AbortFlow],
        })
    })
}

pub(super) fn stop_metrics<'a>(
    _state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async {
        Ok(Transition {
            next_state: State::StoppingMetrics,
            actions: vec![Action::StopMetrics],
        })
    })
}

pub(super) fn close_host<'a>(
    _state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async {
        Ok(Transition {
            next_state: State::ClosingHost,
            actions: vec![Action::CloseHost],
        })
    })
}

pub(super) fn host_closed<'a>(
    state: &'a State,
    event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let Event::HostClosed { at } = event else {
            return Ok(registration_mismatch("host_closed", state, event));
        };
        Ok(Transition {
            next_state: State::Deregistering {
                deadline: *at + Duration::from_secs(5),
            },
            actions: vec![Action::AwaitDeregistration],
        })
    })
}

pub(super) fn host_absent<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let Event::HostAbsent { at } = event else {
            return Ok(registration_mismatch("host_absent", state, event));
        };
        Ok(flush_metrics(*at, ctx.grace))
    })
}

pub(super) fn heartbeat_joined<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let Event::HeartbeatJoined { at } = event else {
            return Ok(registration_mismatch("heartbeat_joined", state, event));
        };
        Ok(flush_metrics(*at, ctx.grace))
    })
}

pub(super) fn abort_deregistration<'a>(
    _state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async {
        Ok(Transition {
            next_state: State::JoiningDeregisteredHeartbeat,
            actions: vec![Action::AbortDeregistration],
        })
    })
}

pub(super) fn finish_metrics_flush<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let (Event::MetricsFlushed { at } | Event::FlushExpired { at }) = event else {
            return Ok(registration_mismatch("finish_metrics_flush", state, event));
        };
        Ok(Transition {
            next_state: State::JoiningLeftoverHeartbeat(JoinBudget::new(*at, ctx.grace)),
            actions: vec![Action::JoinLeftoverHeartbeat],
        })
    })
}

pub(super) fn join_tasks<'a>(
    state: &'a State,
    event: &'a Event,
    ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let Event::LeftoverHeartbeatJoined { at } = event else {
            return Ok(registration_mismatch("join_tasks", state, event));
        };
        Ok(Transition {
            next_state: State::JoiningTasks(JoinBudget::new(*at, ctx.grace)),
            actions: vec![Action::JoinTasks],
        })
    })
}

pub(super) fn finish<'a>(
    _state: &'a State,
    _event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async {
        Ok(Transition {
            next_state: State::Finished,
            actions: vec![],
        })
    })
}

pub(super) fn leftover_heartbeat_budget_expired<'a>(
    state: &'a State,
    event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let State::JoiningLeftoverHeartbeat(budget) = state else {
            return Ok(registration_mismatch(
                "leftover_heartbeat_budget_expired",
                state,
                event,
            ));
        };
        let (budget, actions) = expire_join_budget(*budget);
        Ok(Transition {
            next_state: State::JoiningLeftoverHeartbeat(budget),
            actions,
        })
    })
}

pub(super) fn tasks_budget_expired<'a>(
    state: &'a State,
    event: &'a Event,
    _ctx: &'a mut Context,
) -> BoxFuture<'a, FsmResult<Transition<State, Action>>> {
    Box::pin(async move {
        let State::JoiningTasks(budget) = state else {
            return Ok(registration_mismatch("tasks_budget_expired", state, event));
        };
        let (budget, actions) = expire_join_budget(*budget);
        Ok(Transition {
            next_state: State::JoiningTasks(budget),
            actions,
        })
    })
}

fn flush_metrics(at: TokioInstant, grace: Duration) -> Transition<State, Action> {
    Transition {
        next_state: State::FlushingMetrics {
            deadline: at + grace,
        },
        actions: vec![Action::FlushMetrics],
    }
}

fn expire_join_budget(budget: JoinBudget) -> (JoinBudget, Vec<Action>) {
    match budget {
        JoinBudget::Within { deadline } => (
            JoinBudget::Exceeded { deadline },
            vec![Action::DiagnoseJoinBudget],
        ),
        JoinBudget::Exceeded { .. } => (budget, vec![]),
    }
}

fn stay(state: &State) -> Transition<State, Action> {
    Transition {
        next_state: state.clone(),
        actions: vec![],
    }
}

fn registration_mismatch(handler: &str, state: &State, event: &Event) -> Transition<State, Action> {
    // The driver expects infallible handlers. Diagnose an internal wiring error without
    // panicking during cleanup, altering its outcome or pretending an operation completed.
    tracing::error!(
        handler,
        ?state,
        ?event,
        "Application lifecycle handler registration mismatch"
    );
    stay(state)
}
