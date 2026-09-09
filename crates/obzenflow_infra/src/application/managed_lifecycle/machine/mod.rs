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
//! - `transitions`: state changes and the commands selected by each observation.
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

use transitions::reduce;

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
            on Event::Failure => reduce;
            on Event::HostBound => reduce;
            on Event::Standalone => reduce;
            on Event::PreparationFailed => reduce;
            on Event::Stop => reduce;
        }
        state State::Starting {
            on Event::Failure => reduce;
            on Event::Started => reduce;
            on Event::Stop => reduce;
        }
        state State::Active {
            on Event::Failure => reduce;
            on Event::Stop => reduce;
        }
        state State::RunningStandalone {
            on Event::Failure => reduce;
            on Event::StandaloneReturned => reduce;
        }
        state State::SettlingFlow {
            on Event::Failure => reduce;
            on Event::Admission => reduce;
            on Event::StopSent => reduce;
            on Event::RepeatedSignal => reduce;
            on Event::GracefulExpired => reduce;
            on Event::PublicationObserved => reduce;
            on Event::CompletionExpired => reduce;
        }
        state State::AbortingFlow {
            on Event::Failure => reduce;
            on Event::FlowAborted => reduce;
        }
        state State::StoppingMetrics {
            on Event::Failure => reduce;
            on Event::MetricsStopped => reduce;
        }
        state State::ClosingHost {
            on Event::Failure => reduce;
            on Event::HostClosed => reduce;
            on Event::HostAbsent => reduce;
        }
        state State::Deregistering {
            on Event::Failure => reduce;
            on Event::HeartbeatJoined => reduce;
            on Event::DeregistrationExpired => reduce;
        }
        state State::JoiningDeregisteredHeartbeat {
            on Event::Failure => reduce;
            on Event::HeartbeatJoined => reduce;
        }
        state State::FlushingMetrics {
            on Event::Failure => reduce;
            on Event::MetricsFlushed => reduce;
            on Event::FlushExpired => reduce;
        }
        state State::JoiningLeftoverHeartbeat {
            on Event::Failure => reduce;
            on Event::LeftoverHeartbeatJoined => reduce;
            on Event::JoinBudgetExpired => reduce;
        }
        state State::JoiningTasks {
            on Event::Failure => reduce;
            on Event::TasksJoined => reduce;
            on Event::JoinBudgetExpired => reduce;
        }
    }
}
