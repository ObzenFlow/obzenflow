// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Minimal FSM for the composite monitor-supervisor.
//!
//! The composite supervisor's lifecycle is trivial: it runs, folding member
//! `StageLifecycle` into composite lifecycle, until the fold latches a terminal
//! (or the flow ends), then it terminates. All I/O (poll, fold, journal append)
//! happens in `dispatch_state`; the FSM only carries the lifecycle states, so it
//! declares no transition actions.

use obzenflow_fsm::types::FsmResult;
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, FsmError, StateMachine, StateVariant, Transition,
};

/// FSM states for the composite monitor-supervisor.
#[derive(Clone, Debug, PartialEq)]
pub enum CompositeSupervisorState {
    /// Watching member lifecycle and authoring composite lifecycle.
    Running,
    /// Terminal: the composite reached a terminal lifecycle or the flow ended.
    Drained,
    /// Terminal: the supervisor's own dispatch errored.
    Failed { error: String },
}

impl StateVariant for CompositeSupervisorState {
    fn variant_name(&self) -> &str {
        match self {
            CompositeSupervisorState::Running => "Running",
            CompositeSupervisorState::Drained => "Drained",
            CompositeSupervisorState::Failed { .. } => "Failed",
        }
    }
}

/// Events driving the supervisor's own state transitions.
#[derive(Clone, Debug)]
pub enum CompositeSupervisorEvent {
    /// The composite reached a terminal lifecycle, or the flow ended.
    Finish,
    /// The supervisor's own dispatch errored.
    Error(String),
}

impl EventVariant for CompositeSupervisorEvent {
    fn variant_name(&self) -> &str {
        match self {
            CompositeSupervisorEvent::Finish => "Finish",
            CompositeSupervisorEvent::Error(_) => "Error",
        }
    }
}

/// The supervisor performs all work in `dispatch_state`, so it emits no
/// transition actions. This action type is therefore uninhabited.
#[derive(Clone, Debug)]
pub enum CompositeSupervisorAction {}

#[async_trait::async_trait]
impl FsmAction for CompositeSupervisorAction {
    type Context = CompositeSupervisorContext;

    async fn execute(&self, _ctx: &mut Self::Context) -> FsmResult<()> {
        match *self {}
    }
}

/// The FSM context. The supervisor holds its subscription, rollup, and journal
/// on itself, so the context carries nothing.
pub struct CompositeSupervisorContext;

impl FsmContext for CompositeSupervisorContext {}

pub type CompositeSupervisorFsm = StateMachine<
    CompositeSupervisorState,
    CompositeSupervisorEvent,
    CompositeSupervisorContext,
    CompositeSupervisorAction,
>;

/// Build the composite supervisor FSM: `Running` transitions to `Drained` on
/// `Finish` and to `Failed` on `Error`; both are terminal.
pub fn build_composite_supervisor_fsm() -> CompositeSupervisorFsm {
    fsm! {
        state:   CompositeSupervisorState;
        event:   CompositeSupervisorEvent;
        context: CompositeSupervisorContext;
        action:  CompositeSupervisorAction;
        initial: CompositeSupervisorState::Running;

        state CompositeSupervisorState::Running {
            on CompositeSupervisorEvent::Finish => |_state: &CompositeSupervisorState, _event: &CompositeSupervisorEvent, _ctx: &mut CompositeSupervisorContext| {
                Box::pin(async move {
                    Ok(Transition {
                        next_state: CompositeSupervisorState::Drained,
                        actions: vec![],
                    })
                })
            };

            on CompositeSupervisorEvent::Error => |_state: &CompositeSupervisorState, event: &CompositeSupervisorEvent, _ctx: &mut CompositeSupervisorContext| {
                let event = event.clone();
                Box::pin(async move {
                    let error = match event {
                        CompositeSupervisorEvent::Error(err) => err,
                        _ => {
                            return Err(FsmError::HandlerError(
                                "Invalid event for Error handler".to_string(),
                            ));
                        }
                    };
                    Ok(Transition {
                        next_state: CompositeSupervisorState::Failed { error },
                        actions: vec![],
                    })
                })
            };
        }
    }
}
