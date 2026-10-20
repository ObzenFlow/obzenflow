// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::atomic::Ordering;

use obzenflow_core::event::payloads::execution_payload::JoinPhaseFact;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::ReaderGeneration;
use obzenflow_fsm::types::{BoxFuture, FsmResult};
use obzenflow_fsm::{EventVariant, FsmError, StateVariant, Transition};

use super::fsm::{
    DrainStep, HoldCompletion, JoinAction, JoinContext, JoinDrain, JoinEnrichment, JoinEvent,
    JoinHold, JoinLive, JoinReading, JoinResources, JoinState, PlanShape, ThenLivePhases,
};
use super::phase::{check_crossing_phase, observe_hold, HoldOutcome};
use crate::messaging::upstream_subscription::ReaderFrontier;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::input_facts::{fold_worst, EofRead};
use crate::stages::common::supervision::catch_up::frontier_step;
use crate::supervised_base::handler_supervised::SupervisorAction;

type JoinTransition<H> = Transition<JoinState, JoinAction<H>>;
type Decision<'a, H> = BoxFuture<'a, FsmResult<JoinTransition<H>>>;

fn change<H>(next_state: JoinState, actions: Vec<JoinAction<H>>) -> JoinTransition<H> {
    Transition {
        next_state,
        actions,
    }
}

fn decided<'a, H: UnifiedJoinHandler + 'a>(transition: JoinTransition<H>) -> Decision<'a, H> {
    Box::pin(async move { Ok(transition) })
}

fn failing<H>(cause: String) -> JoinTransition<H> {
    change(
        JoinState::failure(cause.clone()),
        vec![
            JoinAction::SendFailure { message: cause },
            JoinAction::Cleanup,
            JoinAction::Host(SupervisorAction::CloseMailbox),
            JoinAction::Host(SupervisorAction::SettlePublications),
            JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled)),
        ],
    )
}

fn counted<H: UnifiedJoinHandler>(ctx: &JoinContext<H>, cause: String) -> JoinTransition<H> {
    ctx.instrumentation
        .failures_total
        .fetch_add(1, Ordering::Relaxed);
    failing(cause)
}

fn decide<'a, H: UnifiedJoinHandler + 'a>(
    ctx: &JoinContext<H>,
    from: &JoinState,
    decision: impl FnOnce(&JoinResources<H>) -> Result<JoinTransition<H>, String>,
) -> Decision<'a, H> {
    let decision = match ctx.resources.as_ref() {
        Some(resources) => decision(resources).inspect(|transition| {
            if transition.next_state.variant_name() != from.variant_name() {
                tracing::info!(
                    stage_name = %resources.stage_name,
                    from = from.variant_name(),
                    to = transition.next_state.variant_name(),
                    "Join changed phase"
                );
            }
        }),
        None => Err("join resources belong to a pending operation".to_string()),
    };
    decided(decision.unwrap_or_else(|cause| counted(ctx, cause)))
}

fn crossed<H: UnifiedJoinHandler>(
    state: &JoinState,
    generation: ReaderGeneration,
    frontier: ReaderFrontier,
    resources: &JoinResources<H>,
) -> Result<(JoinReading, Vec<JoinAction<H>>), String> {
    let (Some(reading), Some(phase)) = (state.reading(), state.crossing_phase()) else {
        unreachable!()
    };
    let step = frontier_step(&reading.catch_up, generation, frontier, &resources.catch_up)
        .into_result()
        .map_err(|fatal| fatal.detail)?;
    let mut actions: Vec<_> = step.actions.into_iter().map(JoinAction::CatchUp).collect();
    if let Some(crossed) = step.crossed_into {
        check_crossing_phase(
            &resources.stage_name,
            &resources.archived_phases,
            crossed,
            phase,
        )?;
        actions.extend(state.held_input().map(|side| JoinAction::PromoteHeld {
            side,
            generation: crossed,
        }));
    }
    Ok((
        JoinReading {
            catch_up: step.next,
            ..reading.clone()
        },
        actions,
    ))
}

fn read_eof<H: UnifiedJoinHandler>(
    state: &JoinState,
    eof: &EofRead,
    resources: &JoinResources<H>,
) -> Result<(JoinReading, Vec<JoinAction<H>>), String> {
    let (mut reading, mut actions) =
        crossed(state, eof.frontier.highest(), eof.frontier, resources)?;
    actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
    reading.worst_eof = fold_worst(reading.worst_eof, eof.side_worst);
    Ok((reading, actions))
}

fn drained_by(eof: &EofRead, reading: &JoinReading, step: DrainStep) -> JoinState {
    JoinState::Draining(JoinDrain {
        step,
        worst_eof: reading.worst_eof,
        clock: eof.clock(),
    })
}

fn hydrated(reading: JoinReading, completion: HoldCompletion) -> (JoinState, JoinPhaseFact) {
    match reading.plan.shape() {
        PlanShape::Live(reads) | PlanShape::ThenLive(ThenLivePhases::LiveOnly(reads)) => (
            JoinState::Live(JoinLive { reading, reads }),
            completion.live_entered(),
        ),
        PlanShape::FiniteEof => (
            JoinState::Enriching(JoinEnrichment::ToEnd(reading)),
            completion.enriching_entered(),
        ),
        PlanShape::ThenLive(ThenLivePhases::BatchThenLive(_) | ThenLivePhases::BatchOnly) => (
            JoinState::Enriching(JoinEnrichment::Holding(JoinHold::new(reading))),
            completion.enriching_entered(),
        ),
    }
}

fn batch_read<H: UnifiedJoinHandler>(
    hold: &JoinHold,
    reading: JoinReading,
    eof: &EofRead,
    resources: &JoinResources<H>,
) -> Result<(JoinState, Option<JoinPhaseFact>), String> {
    let batch = resources
        .phase_witnesses
        .as_ref()
        .map(|witnesses| witnesses.batch_stream.as_slice());
    let (witnessed, outcome) = observe_hold(&hold.witnessed, batch, &resources.witness_keys, eof);
    Ok(match outcome {
        HoldOutcome::Holding => (
            JoinState::Enriching(JoinEnrichment::Holding(JoinHold { reading, witnessed })),
            None,
        ),
        HoldOutcome::Completed(completion) => match reading.plan.live_reads() {
            Some(reads) => (
                JoinState::Live(JoinLive { reading, reads }),
                Some(completion.live_entered()),
            ),
            None if eof.ends_reads => (
                drained_by(
                    eof,
                    &reading,
                    DrainStep::Finalize {
                        final_eof: eof.envelope.clone(),
                    },
                ),
                None,
            ),
            None => {
                return Err(format!(
                    "join '{}' completed its batch on an EOF that did not end its stream",
                    resources.stage_name
                ))
            }
        },
        HoldOutcome::LeftOpen => (drained_by(eof, &reading, DrainStep::Flush), None),
    })
}

pub(super) fn initialize<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Created(plan) = state else {
        unreachable!()
    };
    decided(change(
        JoinState::Initializing(*plan),
        vec![
            JoinAction::Host(SupervisorAction::Register),
            JoinAction::AllocateResources,
            JoinAction::Host(SupervisorAction::Emit(JoinEvent::InitializationCompleted)),
        ],
    ))
}

pub(super) fn initialized<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Initializing(plan) = state else {
        unreachable!()
    };
    decided(change(JoinState::Initialized(*plan), Vec::new()))
}

pub(super) fn ready<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Initialized(plan) = state else {
        unreachable!()
    };
    decided(change(
        JoinState::Starting(*plan),
        vec![
            JoinAction::PublishRunning,
            JoinAction::Host(SupervisorAction::Emit(JoinEvent::ActivationCompleted)),
        ],
    ))
}

pub(super) fn activated<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Starting(plan) = state else {
        unreachable!()
    };
    let reading = JoinReading::new(*plan);
    let next_state = match plan.shape() {
        PlanShape::Live(reads) => JoinState::Live(JoinLive { reading, reads }),
        PlanShape::FiniteEof | PlanShape::ThenLive(_) => {
            JoinState::Hydrating(JoinHold::new(reading))
        }
    };
    decided(change(next_state, Vec::new()))
}

pub(super) fn watermark<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    event: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinEvent::WatermarkRead(read) = event else {
        unreachable!()
    };
    decide(ctx, state, |resources| {
        let (reading, actions) = crossed(state, read.generation, read.frontier, resources)?;
        Ok(change(state.with_reading(reading), actions))
    })
}

pub(super) fn hydrating_eof<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    event: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let (JoinState::Hydrating(hold), JoinEvent::ReferenceEofRead(eof)) = (state, event) else {
        unreachable!()
    };
    decide(ctx, state, |resources| {
        let (reading, mut actions) = read_eof(state, eof, resources)?;
        let catalog = resources
            .phase_witnesses
            .as_ref()
            .map(|witnesses| witnesses.catalog.as_slice());
        let (witnessed, outcome) =
            observe_hold(&hold.witnessed, catalog, &resources.witness_keys, eof);
        let next_state = match outcome {
            HoldOutcome::Holding => JoinState::Hydrating(JoinHold { reading, witnessed }),
            HoldOutcome::Completed(completion) => {
                let (next_state, entered) = hydrated(reading, completion);
                actions.push(JoinAction::EmitHydrationHeartbeat);
                actions.push(JoinAction::RecordPhase(entered));
                next_state
            }
            HoldOutcome::LeftOpen => {
                actions.push(JoinAction::EmitHydrationHeartbeat);
                JoinState::Draining(JoinDrain {
                    step: DrainStep::Flush,
                    worst_eof: reading.worst_eof,
                    clock: None,
                })
            }
        };
        Ok(change(next_state, actions))
    })
}

pub(super) fn enriching_eof<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    event: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let (JoinState::Enriching(enrichment), JoinEvent::StreamEofRead(eof)) = (state, event) else {
        unreachable!()
    };
    decide(ctx, state, |resources| {
        let (reading, mut actions) = read_eof(state, eof, resources)?;
        let next_state = match enrichment {
            JoinEnrichment::ToEnd(_) if eof.ends_reads => drained_by(
                eof,
                &reading,
                DrainStep::CheckInputs {
                    final_eof: eof.envelope.clone(),
                },
            ),
            JoinEnrichment::ToEnd(_) => state.with_reading(reading),
            JoinEnrichment::Holding(hold) => {
                let (next_state, entered) = batch_read(hold, reading, eof, resources)?;
                actions.extend(entered.map(JoinAction::RecordPhase));
                next_state
            }
        };
        Ok(change(next_state, actions))
    })
}

pub(super) fn live_reference_eof<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    event: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinEvent::ReferenceEofRead(eof) = event else {
        unreachable!()
    };
    decide(ctx, state, |resources| {
        let (reading, actions) = read_eof(state, eof, resources)?;
        Ok(change(state.with_reading(reading), actions))
    })
}

pub(super) fn live_stream_eof<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    event: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinEvent::StreamEofRead(eof) = event else {
        unreachable!()
    };
    decide(ctx, state, |resources| {
        let (reading, actions) = read_eof(state, eof, resources)?;
        let next_state = if eof.ends_reads {
            drained_by(
                eof,
                &reading,
                DrainStep::Finalize {
                    final_eof: eof.envelope.clone(),
                },
            )
        } else {
            state.with_reading(reading)
        };
        Ok(change(next_state, actions))
    })
}

pub(super) fn drain_step_ended<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Draining(drain) = state else {
        unreachable!()
    };
    decide(ctx, state, |resources| {
        let step = match &drain.step {
            DrainStep::CheckInputs { final_eof } => DrainStep::Finalize {
                final_eof: final_eof.clone(),
            },
            DrainStep::Finalize { .. } => DrainStep::Flush,
            DrainStep::Flush => {
                return Err(format!(
                    "join '{}' reported a drain step after its flush",
                    resources.stage_name
                ))
            }
        };
        Ok(change(
            JoinState::Draining(JoinDrain {
                step,
                ..drain.clone()
            }),
            Vec::new(),
        ))
    })
}

pub(super) fn drain_complete<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Draining(drain) = state else {
        unreachable!()
    };
    decide(ctx, state, |resources| {
        if drain.step != DrainStep::Flush {
            return Err(format!(
                "join '{}' completed its drain before its flush",
                resources.stage_name
            ));
        }
        Ok(change(
            JoinState::Finalizing,
            vec![
                JoinAction::AuthorEof {
                    kind: drain.worst_eof.unwrap_or(EofKind::Natural),
                    clock: drain.clock.clone(),
                },
                JoinAction::SendCompletion,
                JoinAction::Cleanup,
                JoinAction::Host(SupervisorAction::CloseMailbox),
                JoinAction::Host(SupervisorAction::SettlePublications),
                JoinAction::Host(SupervisorAction::Emit(JoinEvent::FinalizationCompleted)),
            ],
        ))
    })
}

pub(super) fn finalized<'a, H: UnifiedJoinHandler>(
    _: &'a JoinState,
    _: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    decided(change(JoinState::Drained, Vec::new()))
}

pub(super) fn failure_settled<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Failing(cause) = state else {
        unreachable!()
    };
    decided(change(JoinState::Failed(cause.clone()), Vec::new()))
}

pub(super) fn cancellation_settled<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    _: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinState::Cancelling(cause) = state else {
        unreachable!()
    };
    decided(change(JoinState::Cancelled(cause.clone()), Vec::new()))
}

pub(super) fn cancelling_error<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    event: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinEvent::Error(cause) = event else {
        unreachable!()
    };
    decided(match JoinState::failure(cause.clone()) {
        JoinState::Cancelling(_) => change(state.clone(), Vec::new()),
        _ => failing(cause.clone()),
    })
}

pub(super) fn failure<'a, H: UnifiedJoinHandler>(
    _: &'a JoinState,
    event: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinEvent::Error(cause) = event else {
        unreachable!()
    };
    decided(failing(cause.clone()))
}

pub(super) fn counted_failure<'a, H: UnifiedJoinHandler>(
    _: &'a JoinState,
    event: &'a JoinEvent,
    ctx: &'a mut JoinContext<H>,
) -> Decision<'a, H> {
    let JoinEvent::Error(cause) = event else {
        unreachable!()
    };
    decided(counted(ctx, cause.clone()))
}

pub(super) fn unhandled<'a, H: UnifiedJoinHandler>(
    state: &'a JoinState,
    event: &'a JoinEvent,
    _: &'a mut JoinContext<H>,
) -> BoxFuture<'a, FsmResult<()>> {
    let ignored = matches!(event, JoinEvent::Initialize | JoinEvent::Ready)
        || matches!(
            state,
            JoinState::Failing(_)
                | JoinState::Cancelling(_)
                | JoinState::Failed(_)
                | JoinState::Cancelled(_)
                | JoinState::Drained
        );
    let state_name = state.variant_name().to_string();
    let event_name = event.variant_name().to_string();
    Box::pin(async move {
        if ignored {
            return Ok(());
        }
        tracing::error!(
            supervisor = "JoinSupervisor",
            state = %state_name,
            event = %event_name,
            "Unhandled event in FSM - this indicates a state machine configuration error"
        );
        Err(FsmError::UnhandledEvent {
            state: state_name,
            event: event_name,
        })
    })
}
