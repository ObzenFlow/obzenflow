// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FSM for merge stages. A merge reads its first-phase producers and polls its
//! held producers only after every first-phase producer authored natural EOF:
//! a `then_live` merge's batch producers before its live ones, and a `concat`
//! merge's first producer before its other one.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, ReaderGeneration, StageId, WriterId};
use obzenflow_fsm::{FsmAction, FsmContext, FsmError, Transition};

use crate::backpressure::{BackpressureReader, BackpressureWriter, StallEpisode};
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::{
    ContractConfig, ContractsWiring, ReaderFrontier, ReaderProgress,
};
use crate::messaging::{DeliveredRecord, UpstreamSubscription};
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::{ProcessingContext, SignalGate};
use crate::stages::common::heartbeat::HeartbeatHandle;
use crate::stages::common::input_facts::{fold_worst, EofRead, WatermarkRead};
use crate::stages::common::supervision::backpressure_drain::PendingOutput;
use crate::stages::common::supervision::catch_up::{
    frontier_step, CatchUpAction, CatchUpContext, CatchUpState,
};
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::forward_control_event::forward_control_event;
use crate::stages::common::supervision::lifecycle_actions;
use crate::stages::merges::MergeStrategyKind;
use crate::stages::resources_builder::BoundSubscriptionFactory;
use crate::supervised_base::handler_supervised::SupervisorAction;

/// FSM states for merge stages.
#[derive(Clone, Debug, Default, PartialEq, obzenflow_fsm::StateVariant)]
pub enum MergeState {
    #[default]
    Created,
    Initializing,
    Initialized,
    Starting,
    /// Reads only the first-phase producers; the held producers wait in their
    /// journals and their edges are renewed as held.
    FirstPhase(MergeInputs),
    /// Every first-phase producer authored natural EOF; reads the held producers.
    LastPhase(MergeInputs),
    Draining(MergeDrain),
    Finalizing,
    Drained,
    Failing(String),
    Failed(String),
    Cancelling(String),
    Cancelled(String),
}

/// A reading merge's catch-up state and the worst kind its inputs ended with.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct MergeInputs {
    worst_eof: Option<EofKind>,
    catch_up: CatchUpState,
}

/// What the merge's own EOF takes from the EOF that ended its reading.
#[derive(Clone, Debug, PartialEq)]
pub struct MergeDrain {
    worst_eof: Option<EofKind>,
    eof_clock: Option<VectorClock>,
}

impl MergeState {
    pub(crate) fn failure(cause: String) -> Self {
        use crate::stages::common::stage_handle::{
            FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
        };
        match cause.as_str() {
            FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP | STOP_REASON_TIMEOUT => {
                Self::Cancelling(cause)
            }
            _ => Self::Failing(cause),
        }
    }

    pub(crate) fn lifecycle_phase(&self) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        use crate::stages::common::stage_lifecycle::LifecyclePhase as Phase;
        match self {
            Self::Initializing => Phase::Initializing,
            Self::Initialized => Phase::Initialized,
            Self::FirstPhase(_) | Self::LastPhase(_) => Phase::Active,
            Self::Finalizing => Phase::Finalizing,
            Self::Failing(cause) => Phase::Failing(cause.clone()),
            Self::Cancelling(reason) => Phase::Cancelling(reason.clone()),
            Self::Drained => Phase::Completed,
            Self::Failed(cause) => Phase::Failed(cause.clone()),
            Self::Cancelled(reason) => Phase::Cancelled(reason.clone()),
            _ => Phase::Other,
        }
    }
}

/// Events that trigger merge state transitions.
#[derive(Clone, Debug, obzenflow_fsm::EventVariant)]
pub enum MergeEvent {
    Initialize,
    InitializationCompleted,
    Ready,
    ActivationCompleted,
    /// The reading state's dispatch read a catch-up watermark.
    WatermarkRead(WatermarkRead),
    /// The reading state's dispatch read an EOF.
    EofRead(EofRead),
    DrainComplete,
    FinalizationCompleted,
    TerminationSettled,
    Error(String),
}

impl crate::supervised_base::with_external_events::ExternalControlEvent for MergeEvent {
    fn discard_details(
        &self,
    ) -> (
        obzenflow_core::event::CommandDiscardDisposition,
        Option<String>,
    ) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Error(message) => Some(message.as_str()),
            _ => None,
        })
    }
}

#[derive(Clone, Debug)]
pub(crate) enum MergeAction {
    Host(SupervisorAction<MergeEvent>),
    AllocateResources,
    PublishRunning,
    CatchUp(CatchUpAction),
    PromoteHeld(ReaderGeneration),
    ForwardControl(Arc<DeliveredRecord<ChainPayload>>),
    AuthorEof {
        kind: EofKind,
        clock: Option<VectorClock>,
    },
    SendCompletion,
    SendFailure {
        message: String,
    },
    Cleanup,
}

pub(crate) struct PendingRow {
    pub(crate) parent: DeliveredRecord<ChainPayload>,
    pub(crate) upstream: Option<StageId>,
    pub(crate) outputs: VecDeque<PendingOutput>,
}

#[derive(Default)]
pub(crate) struct MergeInput {
    pub(crate) subscription: Option<UpstreamSubscription<ChainEvent>>,
    pub(crate) contract_state: Vec<ReaderProgress>,
    pub(crate) last_contract_check: Option<tokio::time::Instant>,
}

/// Everything merge actions and dispatch need.
pub struct MergeContext {
    pub(crate) stage_id: StageId,
    pub(crate) stage_name: String,
    pub(crate) flow_name: String,
    pub(crate) flow_id: FlowId,
    pub(crate) strategy: MergeStrategyKind,
    pub(crate) writer_id: Option<WriterId>,
    pub(crate) runtime_execution: crate::execution::RuntimeExecution,
    pub(crate) lineage_policy: obzenflow_core::config::LineagePolicy,
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    pub(crate) instrumentation: Arc<StageInstrumentation>,
    pub(crate) control_strategy: Arc<dyn SignalGate>,
    pub(crate) processing_context: ProcessingContext,

    pub(crate) first_phase_subscription_factory: BoundSubscriptionFactory,
    pub(crate) held_subscription_factory: BoundSubscriptionFactory,
    pub(crate) first_phase: MergeInput,
    pub(crate) held: MergeInput,
    pub(crate) held_producers: Vec<StageId>,

    pub(crate) backpressure_writer: BackpressureWriter,
    pub(crate) backpressure_readers: HashMap<StageId, BackpressureReader>,
    pub(crate) output_contract: StageOutputContract,
    pub(crate) pending: Option<PendingRow>,
    pub(crate) backpressure_pulse: BackpressureActivityPulse,
    pub(crate) backpressure_stall: StallEpisode,
    pub(crate) heartbeat: Option<HeartbeatHandle>,

    pub(crate) catch_up: CatchUpContext,
}

impl FsmContext for MergeContext {}

impl MergeContext {
    pub(crate) fn flow_context(&self) -> FlowContext {
        make_flow_context(
            &self.flow_name,
            &self.flow_id.to_string(),
            &self.stage_name,
            self.stage_id,
            StageType::Merge,
        )
    }
}

pub(super) fn failure_transition(cause: String) -> Transition<MergeState, MergeAction> {
    Transition {
        next_state: MergeState::failure(cause.clone()),
        actions: vec![
            MergeAction::SendFailure { message: cause },
            MergeAction::Cleanup,
            MergeAction::Host(SupervisorAction::CloseMailbox),
            MergeAction::Host(SupervisorAction::SettlePublications),
            MergeAction::Host(SupervisorAction::Emit(MergeEvent::TerminationSettled)),
        ],
    }
}

struct Crossing {
    catch_up: CatchUpState,
    actions: Vec<MergeAction>,
    crossed_into: Option<ReaderGeneration>,
}

fn cross(
    catch_up: &CatchUpState,
    generation: ReaderGeneration,
    frontier: ReaderFrontier,
    ctx: &CatchUpContext,
) -> Result<Crossing, String> {
    let step = frontier_step(catch_up, generation, frontier, ctx);
    if let CatchUpState::Refused { fatal } | CatchUpState::Failed { fatal } = &step.next {
        return Err(fatal.detail.clone());
    }
    Ok(Crossing {
        catch_up: step.next,
        actions: step.actions.into_iter().map(MergeAction::CatchUp).collect(),
        crossed_into: step.crossed_into,
    })
}

pub(super) fn first_phase_watermark(
    inputs: &MergeInputs,
    read: &WatermarkRead,
    ctx: &CatchUpContext,
) -> Transition<MergeState, MergeAction> {
    match cross(&inputs.catch_up, read.generation, read.frontier, ctx) {
        Err(detail) => failure_transition(detail),
        Ok(crossing) => {
            let mut actions = crossing.actions;
            actions.extend(crossing.crossed_into.map(MergeAction::PromoteHeld));
            Transition {
                next_state: MergeState::FirstPhase(MergeInputs {
                    worst_eof: inputs.worst_eof,
                    catch_up: crossing.catch_up,
                }),
                actions,
            }
        }
    }
}

pub(super) fn first_phase_eof(
    inputs: &MergeInputs,
    eof: &EofRead,
    ctx: &CatchUpContext,
) -> Transition<MergeState, MergeAction> {
    let crossing = match cross(&inputs.catch_up, eof.frontier.highest(), eof.frontier, ctx) {
        Err(detail) => return failure_transition(detail),
        Ok(crossing) => crossing,
    };
    let mut actions = crossing.actions;
    actions.extend(crossing.crossed_into.map(MergeAction::PromoteHeld));
    actions.push(MergeAction::ForwardControl(eof.envelope.clone()));
    let worst_eof = fold_worst(inputs.worst_eof, eof.side_worst);
    let next_state = if !eof.ends_reads {
        MergeState::FirstPhase(MergeInputs {
            worst_eof,
            catch_up: crossing.catch_up,
        })
    } else if eof.side_worst.unwrap_or(EofKind::Natural) == EofKind::Natural {
        MergeState::LastPhase(MergeInputs {
            worst_eof,
            catch_up: crossing.catch_up,
        })
    } else {
        MergeState::Draining(MergeDrain {
            worst_eof,
            eof_clock: eof.clock(),
        })
    };
    Transition {
        next_state,
        actions,
    }
}

pub(super) fn last_phase_watermark(
    inputs: &MergeInputs,
    read: &WatermarkRead,
    ctx: &CatchUpContext,
) -> Transition<MergeState, MergeAction> {
    match cross(&inputs.catch_up, read.generation, read.frontier, ctx) {
        Err(detail) => failure_transition(detail),
        Ok(crossing) => Transition {
            next_state: MergeState::LastPhase(MergeInputs {
                worst_eof: inputs.worst_eof,
                catch_up: crossing.catch_up,
            }),
            actions: crossing.actions,
        },
    }
}

pub(super) fn last_phase_eof(
    inputs: &MergeInputs,
    eof: &EofRead,
    ctx: &CatchUpContext,
) -> Transition<MergeState, MergeAction> {
    let crossing = match cross(&inputs.catch_up, eof.frontier.highest(), eof.frontier, ctx) {
        Err(detail) => return failure_transition(detail),
        Ok(crossing) => crossing,
    };
    let mut actions = crossing.actions;
    actions.push(MergeAction::ForwardControl(eof.envelope.clone()));
    let worst_eof = fold_worst(inputs.worst_eof, eof.side_worst);
    let next_state = if eof.ends_reads {
        MergeState::Draining(MergeDrain {
            worst_eof,
            eof_clock: eof.clock(),
        })
    } else {
        MergeState::LastPhase(MergeInputs {
            worst_eof,
            catch_up: crossing.catch_up,
        })
    };
    Transition {
        next_state,
        actions,
    }
}

pub(super) fn drained(drain: &MergeDrain) -> Transition<MergeState, MergeAction> {
    Transition {
        next_state: MergeState::Finalizing,
        actions: vec![
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
    }
}

#[async_trait::async_trait]
impl FsmAction for MergeAction {
    type Context = MergeContext;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), FsmError> {
        match self {
            MergeAction::Host(_) => Err(FsmError::HandlerError(
                "host action requires the supervised runner".into(),
            )),
            MergeAction::AllocateResources => {
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);
                ctx.first_phase.contract_state = ctx
                    .first_phase_subscription_factory
                    .upstream_stage_ids()
                    .into_iter()
                    .map(ReaderProgress::new)
                    .collect();
                ctx.held.contract_state = ctx
                    .held_subscription_factory
                    .upstream_stage_ids()
                    .into_iter()
                    .map(ReaderProgress::new)
                    .collect();
                let wiring = |ctx: &MergeContext| ContractsWiring {
                    writer_id,
                    contract_journal: ctx.data_journal.clone(),
                    config: ContractConfig::default(),
                    reader_stage: Some(ctx.stage_id),
                    control_plane: ctx.instrumentation.control_plane().clone(),
                    include_delivery_contract: false,
                    cycle_guard_config: None,
                };
                let first_phase = ctx
                    .first_phase_subscription_factory
                    .build_with_contracts(wiring(ctx))
                    .await
                    .map_err(|error| {
                        FsmError::HandlerError(format!(
                            "Failed to create the merge's first-phase subscription: {error}"
                        ))
                    })?
                    .with_contract_flow_context(ctx.flow_context());
                let held = ctx
                    .held_subscription_factory
                    .build_with_contracts(wiring(ctx))
                    .await
                    .map_err(|error| {
                        FsmError::HandlerError(format!(
                            "Failed to create the merge's held subscription: {error}"
                        ))
                    })?
                    .with_contract_flow_context(ctx.flow_context());
                ctx.first_phase.subscription = Some(first_phase);
                ctx.held.subscription = Some(held);
                Ok(())
            }
            MergeAction::PublishRunning => {
                lifecycle_actions::publish_running(&ctx.data_journal, ctx.flow_context()).await
            }
            MergeAction::CatchUp(action) => action.execute(&mut ctx.catch_up).await,
            MergeAction::PromoteHeld(generation) => {
                if let Some(held) = ctx.held.subscription.as_mut() {
                    held.promote_held_readers(*generation);
                }
                Ok(())
            }
            MergeAction::ForwardControl(envelope) => {
                forward_control_event(
                    envelope,
                    ctx.stage_id,
                    &ctx.stage_name,
                    StageType::Merge,
                    &ctx.data_journal,
                )
                .await
                .map_err(|error| FsmError::HandlerError(error.to_string()))?;
                Ok(())
            }
            MergeAction::AuthorEof { kind, clock } => {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    FsmError::HandlerError(
                        "No writer ID available to author the merge's EOF".to_string(),
                    )
                })?;
                let runtime_context = ctx.instrumentation.snapshot();
                let (authored_writer_seq, writer_seq_by_event_type, authored_last_event_id) =
                    ctx.instrumentation.authored_data_frontier();
                let mut eof_event = ChainEventFactory::eof_event_with_kind(writer_id, *kind);
                if let ChainPayload::FlowControl(FlowControlPayload::Eof {
                    writer_id: ref mut eof_writer,
                    writer_seq,
                    writer_seq_by_event_type: eof_writer_seq_by_event_type,
                    writer_seq_by_event_type_complete,
                    vector_clock,
                    last_event_id,
                    ..
                }) = &mut eof_event.payload
                {
                    *eof_writer = Some(writer_id);
                    *writer_seq = Some(authored_writer_seq);
                    *eof_writer_seq_by_event_type = writer_seq_by_event_type;
                    *writer_seq_by_event_type_complete = true;
                    if let Some(clock) = clock {
                        *vector_clock = Some(clock.clone());
                    }
                    *last_event_id = authored_last_event_id;
                }
                eof_event.flow_context = ctx.flow_context();
                eof_event.runtime = Some(runtime_context);
                crate::stages::common::supervision::output_committer::commit_control_output(
                    &ctx.data_journal,
                    &ctx.instrumentation,
                    eof_event,
                )
                .await
                .map_err(|error| {
                    FsmError::HandlerError(format!("Failed to author the merge's EOF: {error}"))
                })?;
                Ok(())
            }
            MergeAction::SendCompletion => {
                if let Some(heartbeat) = &ctx.heartbeat {
                    heartbeat.state.mark_completed();
                }
                lifecycle_actions::send_completion(
                    &ctx.data_journal,
                    ctx.flow_context(),
                    ctx.instrumentation.as_ref(),
                )
                .await
            }
            MergeAction::SendFailure { message } => {
                lifecycle_actions::send_failure(
                    &ctx.data_journal,
                    ctx.flow_context(),
                    message,
                    ctx.instrumentation.as_ref(),
                    None,
                )
                .await
            }
            MergeAction::Cleanup => {
                ctx.first_phase.subscription.take();
                ctx.held.subscription.take();
                if let Some(heartbeat) = ctx.heartbeat.take() {
                    heartbeat.cancel();
                }
                Ok(())
            }
        }
    }
}
