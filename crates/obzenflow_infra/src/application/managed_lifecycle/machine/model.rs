// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Lifecycle vocabulary and the synchronous command queue consumed by the driver.

use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use obzenflow_runtime::__private::lifecycle::FlowStopStatus;
use std::time::Duration;
use tokio::time::Instant as TokioInstant;

use super::settlement::{Settlement, StopCommand, StopInput, StopReason};
use crate::application::config::{OnTerminalArg, StartupMode};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(in super::super) enum JoinBudget {
    Within { deadline: TokioInstant },
    Exceeded { deadline: TokioInstant },
}

impl JoinBudget {
    pub(super) fn new(at: TokioInstant, grace: Duration) -> Self {
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
pub(in super::super) enum State {
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
pub(in super::super) enum FailureOrigin {
    Application,
    Host,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub(in super::super) enum Outcome {
    #[default]
    Success,
    ApplicationFailure,
    HostFailure,
}

#[derive(Clone, Debug, EventVariant)]
pub(in super::super) enum Event {
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
pub(in super::super) enum Action {
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

pub(in super::super) struct Context {
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
