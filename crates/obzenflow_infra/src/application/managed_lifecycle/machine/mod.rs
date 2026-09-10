// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-142a: application policy only. No handles, errors, I/O or relative FSM timers.
//!
//! Transition matrix (unlisted observations are diagnosed and ignored):
//! Preparing -> Starting/Active/RunningStandalone on successful preparation;
//! Preparing -> SettlingFlow on failed preparation with a materialised flow;
//! Preparing -> StoppingMetrics on failed preparation without a flow.
//! Starting -> Active on start completion; Starting/Active -> SettlingFlow on stop.
//! SettlingFlow retains Runtime admission bounds through sends and escalation.
//! Publication or expiry -> AbortingFlow -> StoppingMetrics -> ClosingHost.
//! A closed host -> Deregistering -> FlushingMetrics; an absent host skips deregistration.
//! FlushingMetrics -> JoiningLeftoverHeartbeat -> JoiningTasks -> Finished.
//! Join-budget expiry stays in the joining phase with the original deadline.
//!
//! Terminal parking enables no terminal observation. External Play remains Runtime-owned.
//! Each physical cleanup starts on a transition action, never on entry or a self-transition.
//!
//! Module layout follows the Runtime stage supervisors: keep the transition map here,
//! with cohesive policy in private submodules.
//! - `model`: lifecycle states, observations, commands and context.
//! - `settlement`: Runtime stop admission, escalation state and absolute deadlines.
//! - `transitions`: handlers for the edges registered here, with shared policy helpers.
//!
//! Resource handles, pending futures and I/O stay in the application driver.
//! Re-exported types retain visibility within `managed_lifecycle` only.

mod model;
mod settlement;
mod transitions;

use obzenflow_fsm::{fsm, StateMachine};

#[cfg(test)]
pub(super) use model::JoinBudget;
pub(super) use model::{Action, Context, Event, FailureOrigin, Outcome, State};
#[cfg(test)]
pub(super) use settlement::completion_deadline;
pub(super) use settlement::{FlowActivity, StopCommand, StopInput, StopReason};

pub(super) type Machine = StateMachine<State, Event, Context, Action>;

pub(super) fn new() -> Machine {
    fsm! {
        state: State;
        event: Event;
        context: Context;
        action: Action;
        initial: State::Preparing;

        unhandled => |state: &State, event: &Event, _ctx: &mut Context| {
            Box::pin(async move {
                tracing::debug!(?state, ?event, "Ignored application lifecycle observation");
                Ok(())
            })
        };
        state State::Preparing {
            on Event::Failure => transitions::record_failure;
            on Event::HostBound => transitions::host_bound;
            on Event::Standalone => transitions::run_standalone;
            on Event::PreparationFailed => transitions::stop_metrics;
            on Event::Stop => transitions::begin_settlement;
        }
        state State::Starting {
            on Event::Failure => transitions::record_failure;
            on Event::Started => transitions::started;
            on Event::Stop => transitions::begin_settlement;
        }
        state State::Active {
            on Event::Failure => transitions::record_failure;
            on Event::Stop => transitions::begin_settlement;
        }
        state State::RunningStandalone {
            on Event::Failure => transitions::record_failure;
            on Event::StandaloneReturned => transitions::stop_metrics;
        }
        state State::SettlingFlow {
            on Event::Failure => transitions::record_failure;
            on Event::Admission => transitions::observe_admission;
            on Event::StopSent => transitions::acknowledge_stop_send;
            on Event::RepeatedSignal => transitions::request_cancellation;
            on Event::GracefulExpired => transitions::graceful_expired;
            on Event::PublicationObserved => transitions::abort_flow;
            on Event::CompletionExpired => transitions::abort_flow;
        }
        state State::AbortingFlow {
            on Event::Failure => transitions::record_failure;
            on Event::FlowAborted => transitions::stop_metrics;
        }
        state State::StoppingMetrics {
            on Event::Failure => transitions::record_failure;
            on Event::MetricsStopped => transitions::close_host;
        }
        state State::ClosingHost {
            on Event::Failure => transitions::record_failure;
            on Event::HostClosed => transitions::host_closed;
            on Event::HostAbsent => transitions::host_absent;
        }
        state State::Deregistering {
            on Event::Failure => transitions::record_failure;
            on Event::HeartbeatJoined => transitions::heartbeat_joined;
            on Event::DeregistrationExpired => transitions::abort_deregistration;
        }
        state State::JoiningDeregisteredHeartbeat {
            on Event::Failure => transitions::record_failure;
            on Event::HeartbeatJoined => transitions::heartbeat_joined;
        }
        state State::FlushingMetrics {
            on Event::Failure => transitions::record_failure;
            on Event::MetricsFlushed => transitions::finish_metrics_flush;
            on Event::FlushExpired => transitions::finish_metrics_flush;
        }
        state State::JoiningLeftoverHeartbeat {
            on Event::Failure => transitions::record_failure;
            on Event::LeftoverHeartbeatJoined => transitions::join_tasks;
            on Event::JoinBudgetExpired => transitions::leftover_heartbeat_budget_expired;
        }
        state State::JoiningTasks {
            on Event::Failure => transitions::record_failure;
            on Event::TasksJoined => transitions::finish;
            on Event::JoinBudgetExpired => transitions::tasks_budget_expired;
        }
    }
}
