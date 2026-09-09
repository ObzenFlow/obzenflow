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

use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, StateMachine, StateVariant, Transition,
};
use obzenflow_runtime::__private::lifecycle::{FlowCancelCause, FlowStopStatus};
use std::time::{Duration, Instant};
use tokio::time::Instant as TokioInstant;

use crate::application::config::{OnTerminalArg, StartupMode};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StopCommand {
    Graceful,
    Cancel,
    Timeout,
}

#[derive(Clone, Copy, Debug)]
pub(super) enum StopReason {
    Graceful,
    Cancel,
    BeforeRun,
}

/// A boundary classification of the current Runtime observation, not execution truth.
#[derive(Clone, Copy, Debug)]
pub(super) enum FlowActivity {
    BeforeRun,
    Executing,
    Terminal,
}

#[derive(Clone, Debug)]
pub(super) struct StopInput {
    pub activity: FlowActivity,
    pub admitted: FlowStopStatus,
    pub at: Instant,
}

#[derive(Clone, Debug, PartialEq)]
enum CompletionBound {
    AwaitingAdmission(Instant),
    Admitted(Instant),
    BeforeRun(Instant),
}

#[derive(Clone, Debug, PartialEq)]
enum Escalation {
    AwaitingAdmission,
    Graceful(Instant),
    Requested(StopCommand),
    Cancelling,
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Settlement {
    bound: CompletionBound,
    escalation: Escalation,
}

impl Settlement {
    pub fn completion_deadline(&self) -> Instant {
        match self.bound {
            CompletionBound::AwaitingAdmission(at)
            | CompletionBound::Admitted(at)
            | CompletionBound::BeforeRun(at) => at,
        }
    }

    pub fn graceful_deadline(&self) -> Option<Instant> {
        match self.escalation {
            Escalation::Graceful(at) => Some(at),
            _ => None,
        }
    }

    fn observe(&mut self, status: &FlowStopStatus, grace: Duration) {
        if matches!(self.bound, CompletionBound::BeforeRun(_)) {
            return;
        }
        if !matches!(status, FlowStopStatus::NotRequested) {
            self.bound = CompletionBound::Admitted(completion_deadline(
                status,
                self.completion_deadline(),
                grace,
            ));
        }
        match status {
            FlowStopStatus::Graceful { deadline }
                if !matches!(self.escalation, Escalation::Requested(_)) =>
            {
                self.escalation = Escalation::Graceful(*deadline);
            }
            FlowStopStatus::Cancelling { .. } => self.escalation = Escalation::Cancelling,
            _ => {}
        }
    }
}

/// Derive the outer bound from Runtime admission, never from a repeated request.
pub(super) fn completion_deadline(
    status: &FlowStopStatus,
    not_admitted: Instant,
    grace: Duration,
) -> Instant {
    match status {
        FlowStopStatus::NotRequested => not_admitted,
        FlowStopStatus::Graceful { deadline } => *deadline + grace,
        FlowStopStatus::Cancelling {
            admitted_at,
            cause,
            graceful_deadline,
        } => {
            if *cause == FlowCancelCause::GracefulTimeout {
                graceful_deadline.unwrap_or(*admitted_at) + grace
            } else {
                *admitted_at + grace
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum JoinBudget {
    Within { deadline: TokioInstant },
    Exceeded { deadline: TokioInstant },
}

impl JoinBudget {
    fn new(at: TokioInstant, grace: Duration) -> Self {
        Self::Within {
            deadline: at + grace,
        }
    }

    pub fn pending_deadline(self) -> Option<TokioInstant> {
        match self {
            Self::Within { deadline } => Some(deadline),
            Self::Exceeded { .. } => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, StateVariant)]
pub(super) enum State {
    Preparing,
    Starting,
    Active,
    RunningStandalone,
    SettlingFlow(Settlement),
    AbortingFlow,
    StoppingMetrics,
    ClosingHost,
    Deregistering { deadline: TokioInstant },
    JoiningDeregisteredHeartbeat,
    FlushingMetrics { deadline: TokioInstant },
    JoiningLeftoverHeartbeat(JoinBudget),
    JoiningTasks(JoinBudget),
    Finished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailureOrigin {
    Application,
    Host,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(super) enum Outcome {
    #[default]
    Success,
    ApplicationFailure,
    HostFailure,
}

#[derive(Clone, Debug, EventVariant)]
pub(super) enum Event {
    Failure(FailureOrigin),
    HostBound(StartupMode),
    Standalone,
    PreparationFailed,
    Started,
    Stop(StopReason, StopInput),
    Admission(FlowStopStatus),
    StopSent,
    RepeatedSignal,
    GracefulExpired,
    PublicationObserved,
    CompletionExpired,
    FlowAborted,
    StandaloneReturned,
    MetricsStopped,
    HostClosed { at: TokioInstant },
    HostAbsent { at: TokioInstant },
    HeartbeatJoined { at: TokioInstant },
    DeregistrationExpired,
    MetricsFlushed { at: TokioInstant },
    FlushExpired { at: TokioInstant },
    LeftoverHeartbeatJoined { at: TokioInstant },
    TasksJoined,
    JoinBudgetExpired,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Action {
    StartFlow,
    RunStandalone,
    SettleFlow(Option<StopCommand>),
    SendStop(StopCommand),
    AbortFlow,
    StopMetrics,
    CloseHost,
    AwaitDeregistration,
    AbortDeregistration,
    FlushMetrics,
    JoinLeftoverHeartbeat,
    JoinTasks,
    DiagnoseJoinBudget,
}

pub(super) struct Context {
    pub grace: Duration,
    pub outcome: Outcome,
    pub on_terminal: OnTerminalArg,
    // Actions enqueue driver commands synchronously. Pending futures and owned results
    // stay outside the FSM context, which the foundation requires to be Sync.
    pub commands: Vec<Action>,
}

impl FsmContext for Context {}

#[async_trait::async_trait]
impl FsmAction for Action {
    type Context = Context;

    async fn execute(&self, context: &mut Context) -> obzenflow_fsm::types::FsmResult<()> {
        context.commands.push(*self);
        Ok(())
    }
}

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

fn reduce<'a>(
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
