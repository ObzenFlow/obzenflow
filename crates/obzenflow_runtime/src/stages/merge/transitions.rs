// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::ReaderGeneration;
use obzenflow_fsm::types::{BoxFuture, FsmResult};
use obzenflow_fsm::{EventVariant, FsmError, StateVariant, Transition};

use super::fsm::{MergeAction, MergeContext, MergeDrain, MergeEvent, MergeInputs, MergeState};
use crate::messaging::upstream_subscription::ReaderFrontier;
use crate::stages::common::input_facts::{fold_worst, EofRead};
use crate::stages::common::supervision::catch_up::frontier_step;
use crate::supervised_base::handler_supervised::SupervisorAction;

type MergeTransition = Transition<MergeState, MergeAction>;
type Decision<'a> = BoxFuture<'a, FsmResult<MergeTransition>>;

fn change(next_state: MergeState, actions: Vec<MergeAction>) -> MergeTransition {
    Transition {
        next_state,
        actions,
    }
}

fn decided<'a>(transition: MergeTransition) -> Decision<'a> {
    Box::pin(async move { Ok(transition) })
}

fn failing(cause: String) -> MergeTransition {
    change(
        MergeState::failure(cause.clone()),
        vec![
            MergeAction::SendFailure { message: cause },
            MergeAction::Cleanup,
            MergeAction::Host(SupervisorAction::CloseMailbox),
            MergeAction::Host(SupervisorAction::SettlePublications),
            MergeAction::Host(SupervisorAction::Emit(MergeEvent::TerminationSettled)),
        ],
    )
}

fn decide<'a>(
    ctx: &MergeContext,
    from: &MergeState,
    decision: impl FnOnce(&MergeContext) -> Result<MergeTransition, String>,
) -> Decision<'a> {
    let transition = decision(ctx).unwrap_or_else(failing);
    if transition.next_state.variant_name() != from.variant_name() {
        match &transition.next_state {
            MergeState::LastPhase(_) => tracing::info!(
                stage_name = %ctx.stage_name,
                strategy = ?ctx.strategy,
                "Merge first phase completed; reading held producers"
            ),
            MergeState::Draining(_) => tracing::info!(
                stage_name = %ctx.stage_name,
                strategy = ?ctx.strategy,
                "Merge finished reading; draining"
            ),
            _ => {}
        }
    }
    decided(transition)
}

fn crossed(
    state: &MergeState,
    generation: ReaderGeneration,
    frontier: ReaderFrontier,
    ctx: &MergeContext,
) -> Result<(MergeInputs, Vec<MergeAction>), String> {
    let Some(inputs) = state.inputs() else {
        unreachable!()
    };
    let step = frontier_step(&inputs.catch_up, generation, frontier, &ctx.catch_up)
        .into_result()
        .map_err(|fatal| fatal.detail)?;
    let mut actions: Vec<_> = step.actions.into_iter().map(MergeAction::CatchUp).collect();
    if state.holding() {
        actions.extend(step.crossed_into.map(MergeAction::PromoteHeld));
    }
    Ok((
        MergeInputs {
            catch_up: step.next,
            ..inputs.clone()
        },
        actions,
    ))
}

fn read_eof(
    state: &MergeState,
    eof: &EofRead,
    ctx: &MergeContext,
) -> Result<(MergeInputs, Vec<MergeAction>), String> {
    let (mut inputs, mut actions) = crossed(state, eof.frontier.highest(), eof.frontier, ctx)?;
    actions.push(MergeAction::ForwardControl(eof.envelope.clone()));
    inputs.worst_eof = fold_worst(inputs.worst_eof, eof.side_worst);
    Ok((inputs, actions))
}

fn drained(inputs: &MergeInputs, eof: &EofRead) -> MergeState {
    MergeState::Draining(MergeDrain {
        worst_eof: inputs.worst_eof,
        eof_clock: eof.clock(),
    })
}

pub(super) fn initialize<'a>(
    _: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    decided(change(
        MergeState::Initializing,
        vec![
            MergeAction::Host(SupervisorAction::Register),
            MergeAction::AllocateResources,
            MergeAction::Host(SupervisorAction::Emit(MergeEvent::InitializationCompleted)),
        ],
    ))
}

pub(super) fn initialized<'a>(
    _: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    decided(change(MergeState::Initialized, Vec::new()))
}

pub(super) fn ready<'a>(
    _: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    decided(change(
        MergeState::Starting,
        vec![
            MergeAction::PublishRunning,
            MergeAction::Host(SupervisorAction::Emit(MergeEvent::ActivationCompleted)),
        ],
    ))
}

pub(super) fn activated<'a>(
    _: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    decided(change(
        MergeState::FirstPhase(MergeInputs::default()),
        Vec::new(),
    ))
}

pub(super) fn watermark<'a>(
    state: &'a MergeState,
    event: &'a MergeEvent,
    ctx: &'a mut MergeContext,
) -> Decision<'a> {
    let MergeEvent::WatermarkRead(read) = event else {
        unreachable!()
    };
    decide(ctx, state, |ctx| {
        let (inputs, actions) = crossed(state, read.generation, read.frontier, ctx)?;
        Ok(change(state.with_inputs(inputs), actions))
    })
}

pub(super) fn first_phase_eof<'a>(
    state: &'a MergeState,
    event: &'a MergeEvent,
    ctx: &'a mut MergeContext,
) -> Decision<'a> {
    let MergeEvent::EofRead(eof) = event else {
        unreachable!()
    };
    decide(ctx, state, |ctx| {
        let (inputs, actions) = read_eof(state, eof, ctx)?;
        let next_state = if !eof.ends_reads {
            MergeState::FirstPhase(inputs)
        } else if eof.side_worst.unwrap_or(EofKind::Natural) == EofKind::Natural {
            MergeState::LastPhase(inputs)
        } else {
            drained(&inputs, eof)
        };
        Ok(change(next_state, actions))
    })
}

pub(super) fn last_phase_eof<'a>(
    state: &'a MergeState,
    event: &'a MergeEvent,
    ctx: &'a mut MergeContext,
) -> Decision<'a> {
    let MergeEvent::EofRead(eof) = event else {
        unreachable!()
    };
    decide(ctx, state, |ctx| {
        let (inputs, actions) = read_eof(state, eof, ctx)?;
        let next_state = if eof.ends_reads {
            drained(&inputs, eof)
        } else {
            MergeState::LastPhase(inputs)
        };
        Ok(change(next_state, actions))
    })
}

pub(super) fn drain_complete<'a>(
    state: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    let MergeState::Draining(drain) = state else {
        unreachable!()
    };
    decided(change(
        MergeState::Finalizing,
        vec![
            MergeAction::AuthorEof {
                kind: drain.worst_eof.unwrap_or(EofKind::Natural),
                clock: drain.eof_clock.clone(),
            },
            MergeAction::SendCompletion,
            MergeAction::Cleanup,
            MergeAction::Host(SupervisorAction::CloseMailbox),
            MergeAction::Host(SupervisorAction::SettlePublications),
            MergeAction::Host(SupervisorAction::Emit(MergeEvent::FinalizationCompleted)),
        ],
    ))
}

pub(super) fn finalized<'a>(
    _: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    decided(change(MergeState::Drained, Vec::new()))
}

pub(super) fn failure_settled<'a>(
    state: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    let MergeState::Failing(cause) = state else {
        unreachable!()
    };
    decided(change(MergeState::Failed(cause.clone()), Vec::new()))
}

pub(super) fn cancellation_settled<'a>(
    state: &'a MergeState,
    _: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    let MergeState::Cancelling(cause) = state else {
        unreachable!()
    };
    decided(change(MergeState::Cancelled(cause.clone()), Vec::new()))
}

pub(super) fn failure<'a>(
    _: &'a MergeState,
    event: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> Decision<'a> {
    let MergeEvent::Error(cause) = event else {
        unreachable!()
    };
    decided(failing(cause.clone()))
}

pub(super) fn unhandled<'a>(
    state: &'a MergeState,
    event: &'a MergeEvent,
    _: &'a mut MergeContext,
) -> BoxFuture<'a, FsmResult<()>> {
    let ignored = matches!(event, MergeEvent::Initialize | MergeEvent::Ready)
        || matches!(
            state,
            MergeState::Failing(_)
                | MergeState::Cancelling(_)
                | MergeState::Failed(_)
                | MergeState::Cancelled(_)
                | MergeState::Drained
        );
    let state_name = state.variant_name().to_string();
    let event_name = event.variant_name().to_string();
    Box::pin(async move {
        if ignored {
            return Ok(());
        }
        Err(FsmError::UnhandledEvent {
            state: state_name,
            event: event_name,
        })
    })
}
