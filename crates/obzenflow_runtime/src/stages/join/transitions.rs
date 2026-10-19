// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::HashMap;
use std::sync::Arc;

use obzenflow_core::event::payloads::execution_payload::JoinPhaseFact;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::{ReaderGeneration, StageId, StageKey};
use obzenflow_fsm::Transition;

use super::config::PhaseWitnesses;
use super::fsm::{
    DrainStep, HoldCompletion, JoinAction, JoinDrain, JoinEvent, JoinPlan, JoinReading,
    JoinResources, JoinState, JoinSubscriptionSide, PlanShape, ThenLivePhases,
};
use super::phase::{check_crossing_phase, observe_hold, CrossingPhase, HoldOutcome};
use crate::messaging::upstream_subscription::ReaderFrontier;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::input_facts::{fold_worst, EofRead, WatermarkRead};
use crate::stages::common::supervision::catch_up::{frontier_step, CatchUpContext, CatchUpState};
use crate::supervised_base::handler_supervised::SupervisorAction;

pub(crate) struct JoinInputs<'a> {
    pub(crate) stage_name: &'a str,
    pub(crate) phase_witnesses: Option<&'a PhaseWitnesses>,
    pub(crate) witness_keys: &'a HashMap<StageId, StageKey>,
    pub(crate) archived_phases: &'a [JoinPhaseFact],
    pub(crate) catch_up: &'a CatchUpContext,
}

impl<'a> JoinInputs<'a> {
    pub(crate) fn of<H: UnifiedJoinHandler>(resources: &'a JoinResources<H>) -> Self {
        Self {
            stage_name: &resources.stage_name,
            phase_witnesses: resources.phase_witnesses.as_ref(),
            witness_keys: &resources.witness_keys,
            archived_phases: &resources.archived_phases,
            catch_up: &resources.catch_up,
        }
    }
}

pub(crate) type JoinTransition<H> = Transition<JoinState, JoinAction<H>>;

pub(crate) fn failure<H>(cause: String) -> JoinTransition<H> {
    Transition {
        next_state: JoinState::failure(cause.clone()),
        actions: vec![
            JoinAction::SendFailure { message: cause },
            JoinAction::Cleanup,
            JoinAction::Host(SupervisorAction::CloseMailbox),
            JoinAction::Host(SupervisorAction::SettlePublications),
            JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled)),
        ],
    }
}

pub(crate) fn opening<H>(plan: JoinPlan) -> JoinTransition<H> {
    let reading = JoinReading::new(plan);
    let next_state = match plan.shape() {
        PlanShape::Live(_) => JoinState::Live(reading),
        PlanShape::FiniteEof | PlanShape::ThenLive(_) => JoinState::Hydrating(reading),
    };
    Transition {
        next_state,
        actions: Vec::new(),
    }
}

struct Crossing<H> {
    catch_up: CatchUpState,
    actions: Vec<JoinAction<H>>,
}

fn cross<H>(
    catch_up: &CatchUpState,
    generation: ReaderGeneration,
    frontier: ReaderFrontier,
    phase: CrossingPhase,
    held: Option<JoinSubscriptionSide>,
    inputs: &JoinInputs<'_>,
) -> Result<Crossing<H>, String> {
    let step = frontier_step(catch_up, generation, frontier, inputs.catch_up);
    if let CatchUpState::Refused { fatal } | CatchUpState::Failed { fatal } = &step.next {
        return Err(fatal.detail.clone());
    }
    if let Some(crossed) = step.crossed_into {
        check_crossing_phase(inputs.stage_name, inputs.archived_phases, crossed, phase)?;
    }
    let mut actions: Vec<JoinAction<H>> =
        step.actions.into_iter().map(JoinAction::CatchUp).collect();
    if let (Some(generation), Some(side)) = (step.crossed_into, held) {
        actions.push(JoinAction::PromoteHeld { side, generation });
    }
    Ok(Crossing {
        catch_up: step.next,
        actions,
    })
}

fn entered(completion: HoldCompletion, live: bool) -> JoinPhaseFact {
    if live {
        JoinPhaseFact::LiveEntered {
            generation: completion.generation,
            ended_by: completion.ended_by,
        }
    } else {
        JoinPhaseFact::EnrichingEntered {
            generation: completion.generation,
            ended_by: completion.ended_by,
        }
    }
}

fn ended(eof: &EofRead, worst_eof: Option<EofKind>, step: DrainStep) -> JoinDrain {
    JoinDrain {
        step,
        worst_eof,
        clock: eof.clock(),
    }
}

pub(crate) fn hydrating_watermark<H>(
    reading: &JoinReading,
    read: &WatermarkRead,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    let crossing = cross(
        &reading.catch_up,
        read.generation,
        read.frontier,
        CrossingPhase::Hydrating,
        Some(JoinSubscriptionSide::Stream),
        inputs,
    )?;
    Ok(Transition {
        next_state: JoinState::Hydrating(JoinReading {
            catch_up: crossing.catch_up,
            ..reading.clone()
        }),
        actions: crossing.actions,
    })
}

pub(crate) fn hydrating_eof<H>(
    reading: &JoinReading,
    eof: &EofRead,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    let crossing = cross(
        &reading.catch_up,
        eof.frontier.highest(),
        eof.frontier,
        CrossingPhase::Hydrating,
        Some(JoinSubscriptionSide::Stream),
        inputs,
    )?;
    let mut actions = crossing.actions;
    actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
    let worst_eof = fold_worst(reading.worst_eof, eof.side_worst);
    let witnesses = inputs
        .phase_witnesses
        .map(|witnesses| witnesses.catalog.as_slice());
    let (witnessed, hold) = observe_hold(&reading.witnessed, witnesses, inputs.witness_keys, eof);
    let held = JoinReading {
        plan: reading.plan,
        catch_up: crossing.catch_up,
        worst_eof,
        witnessed,
    };
    let next_state = match hold {
        HoldOutcome::Holding => JoinState::Hydrating(held),
        HoldOutcome::Completed(completion) => {
            actions.push(JoinAction::EmitHydrationHeartbeat);
            let next = JoinReading {
                witnessed: Arc::default(),
                ..held
            };
            match reading.plan.shape() {
                PlanShape::Live(_) | PlanShape::ThenLive(ThenLivePhases::LiveOnly(_)) => {
                    actions.push(JoinAction::RecordPhase(entered(completion, true)));
                    JoinState::Live(next)
                }
                PlanShape::FiniteEof
                | PlanShape::ThenLive(
                    ThenLivePhases::BatchThenLive(_) | ThenLivePhases::BatchOnly,
                ) => {
                    actions.push(JoinAction::RecordPhase(entered(completion, false)));
                    JoinState::Enriching(next)
                }
            }
        }
        HoldOutcome::LeftOpen => {
            actions.push(JoinAction::EmitHydrationHeartbeat);
            JoinState::Draining(JoinDrain {
                step: DrainStep::Flush,
                worst_eof,
                clock: None,
            })
        }
    };
    Ok(Transition {
        next_state,
        actions,
    })
}

fn enriching_held(plan: JoinPlan) -> Option<JoinSubscriptionSide> {
    plan.enriching_holds_catalog()
        .then_some(JoinSubscriptionSide::Reference)
}

pub(crate) fn enriching_watermark<H>(
    reading: &JoinReading,
    read: &WatermarkRead,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    let crossing = cross(
        &reading.catch_up,
        read.generation,
        read.frontier,
        CrossingPhase::Enriching,
        enriching_held(reading.plan),
        inputs,
    )?;
    Ok(Transition {
        next_state: JoinState::Enriching(JoinReading {
            catch_up: crossing.catch_up,
            ..reading.clone()
        }),
        actions: crossing.actions,
    })
}

pub(crate) fn enriching_eof<H>(
    reading: &JoinReading,
    eof: &EofRead,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    let crossing = cross(
        &reading.catch_up,
        eof.frontier.highest(),
        eof.frontier,
        CrossingPhase::Enriching,
        enriching_held(reading.plan),
        inputs,
    )?;
    let mut actions = crossing.actions;
    actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
    let worst_eof = fold_worst(reading.worst_eof, eof.side_worst);

    if !reading.plan.enriching_holds_catalog() {
        let next_state = if eof.ends_reads {
            JoinState::Draining(ended(
                eof,
                worst_eof,
                DrainStep::CheckInputs {
                    final_eof: eof.envelope.clone(),
                },
            ))
        } else {
            JoinState::Enriching(JoinReading {
                catch_up: crossing.catch_up,
                worst_eof,
                ..reading.clone()
            })
        };
        return Ok(Transition {
            next_state,
            actions,
        });
    }

    let witnesses = inputs
        .phase_witnesses
        .map(|witnesses| witnesses.batch_stream.as_slice());
    let (witnessed, hold) = observe_hold(&reading.witnessed, witnesses, inputs.witness_keys, eof);
    let held = JoinReading {
        plan: reading.plan,
        catch_up: crossing.catch_up,
        worst_eof,
        witnessed,
    };
    let batch_only = matches!(
        reading.plan.shape(),
        PlanShape::ThenLive(ThenLivePhases::BatchOnly)
    );
    let next_state = match hold {
        HoldOutcome::Holding => JoinState::Enriching(held),
        HoldOutcome::Completed(_) if batch_only && eof.ends_reads => JoinState::Draining(ended(
            eof,
            worst_eof,
            DrainStep::Finalize {
                final_eof: eof.envelope.clone(),
            },
        )),
        HoldOutcome::Completed(_) if batch_only => {
            return Err(format!(
                "join '{}' completed its batch on an EOF that did not end its stream",
                inputs.stage_name
            ))
        }
        HoldOutcome::Completed(completion) => {
            actions.push(JoinAction::RecordPhase(entered(completion, true)));
            JoinState::Live(JoinReading {
                witnessed: Arc::default(),
                ..held
            })
        }
        HoldOutcome::LeftOpen => JoinState::Draining(ended(eof, worst_eof, DrainStep::Flush)),
    };
    Ok(Transition {
        next_state,
        actions,
    })
}

pub(crate) fn live_watermark<H>(
    reading: &JoinReading,
    read: &WatermarkRead,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    let crossing = cross(
        &reading.catch_up,
        read.generation,
        read.frontier,
        CrossingPhase::Live,
        None,
        inputs,
    )?;
    Ok(Transition {
        next_state: JoinState::Live(JoinReading {
            catch_up: crossing.catch_up,
            ..reading.clone()
        }),
        actions: crossing.actions,
    })
}

pub(crate) fn live_eof<H>(
    reading: &JoinReading,
    eof: &EofRead,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    let crossing = cross(
        &reading.catch_up,
        eof.frontier.highest(),
        eof.frontier,
        CrossingPhase::Live,
        None,
        inputs,
    )?;
    let mut actions = crossing.actions;
    actions.push(JoinAction::ForwardControl(eof.envelope.clone()));
    let worst_eof = fold_worst(reading.worst_eof, eof.side_worst);
    let next_state = if eof.ends_reads {
        JoinState::Draining(ended(
            eof,
            worst_eof,
            DrainStep::Finalize {
                final_eof: eof.envelope.clone(),
            },
        ))
    } else {
        JoinState::Live(JoinReading {
            catch_up: crossing.catch_up,
            worst_eof,
            ..reading.clone()
        })
    };
    Ok(Transition {
        next_state,
        actions,
    })
}

pub(crate) fn drain_step_ended<H>(
    drain: &JoinDrain,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    let step = match &drain.step {
        DrainStep::CheckInputs { final_eof } => DrainStep::Finalize {
            final_eof: final_eof.clone(),
        },
        DrainStep::Finalize { .. } => DrainStep::Flush,
        DrainStep::Flush => {
            return Err(format!(
                "join '{}' reported a drain step after its flush",
                inputs.stage_name
            ))
        }
    };
    Ok(Transition {
        next_state: JoinState::Draining(JoinDrain {
            step,
            ..drain.clone()
        }),
        actions: Vec::new(),
    })
}

pub(crate) fn drain_complete<H>(
    drain: &JoinDrain,
    inputs: &JoinInputs<'_>,
) -> Result<JoinTransition<H>, String> {
    if drain.step != DrainStep::Flush {
        return Err(format!(
            "join '{}' completed its drain before its flush",
            inputs.stage_name
        ));
    }
    Ok(Transition {
        next_state: JoinState::Finalizing,
        actions: vec![
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
    })
}
