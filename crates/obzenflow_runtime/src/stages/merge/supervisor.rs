// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::VecDeque;
use std::error::Error;
use std::sync::atomic::Ordering;

use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, FsmError, StateMachine, StateVariant, Transition};

use super::fsm::{MergeAction, MergeContext, MergeEvent, MergeState, PendingRow, Side};
use crate::messaging::{DeliveredRecord, PollResult, UpstreamSubscription};
use crate::metrics::instrumentation::snapshot_stage_accounting;
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::stages::common::supervision::backpressure_drain::{
    drain_one_pending, DrainOutcome, PendingOutput,
};
use crate::stages::common::supervision::catch_up::CatchUpDisposition;
use crate::stages::common::supervision::control_resolution::{
    resolve_control_event_awaiting_pauses, ControlAction,
};
use crate::stages::common::supervision::forward_control_event::forward_control_event;
use crate::supervised_base::base::{self, Registration, Supervisor};
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};

type BoxError = Box<dyn Error + Send + Sync>;

pub(crate) struct MergeSupervisor {
    pub(crate) name: String,
    pub(crate) stage_id: StageId,
    pub(crate) first_phase_subscription: Option<UpstreamSubscription<ChainEvent>>,
    pub(crate) held_subscription: Option<UpstreamSubscription<ChainEvent>>,
}

impl MergeSupervisor {
    pub(crate) fn new(name: String, stage_id: StageId) -> Self {
        Self {
            name,
            stage_id,
            first_phase_subscription: None,
            held_subscription: None,
        }
    }

    fn subscription(&mut self, side: Side) -> Option<&mut UpstreamSubscription<ChainEvent>> {
        match side {
            Side::FirstPhase => self.first_phase_subscription.as_mut(),
            Side::Held => self.held_subscription.as_mut(),
        }
    }
}

fn failure_transition(cause: String) -> Transition<MergeState, MergeAction> {
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

macro_rules! merge_error_arm {
    () => {
        |_state: &MergeState, event: &MergeEvent, _ctx: &mut MergeContext| {
            let MergeEvent::Error(cause) = event else {
                unreachable!()
            };
            let cause = cause.clone();
            Box::pin(async move { Ok(failure_transition(cause)) })
        }
    };
}

impl Supervisor for MergeSupervisor {
    type State = MergeState;
    type Event = MergeEvent;
    type Context = MergeContext;
    type Action = MergeAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   MergeState;
            event:   MergeEvent;
            context: MergeContext;
            action:  MergeAction;
            initial: initial_state;

            state MergeState::Created {
                on MergeEvent::Initialize => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::Initializing,
                            actions: vec![
                                MergeAction::Host(SupervisorAction::Register),
                                MergeAction::AllocateResources,
                                MergeAction::Host(SupervisorAction::Emit(MergeEvent::InitializationCompleted)),
                            ],
                        })
                    })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::Initializing {
                on MergeEvent::InitializationCompleted => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move { Ok(Transition { next_state: MergeState::Initialized, actions: vec![] }) })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::Initialized {
                on MergeEvent::Ready => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::Starting,
                            actions: vec![
                                MergeAction::PublishRunning,
                                MergeAction::Host(SupervisorAction::Emit(MergeEvent::ActivationCompleted)),
                            ],
                        })
                    })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::Starting {
                on MergeEvent::ActivationCompleted => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move { Ok(Transition { next_state: MergeState::FirstPhase, actions: vec![] }) })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::FirstPhase {
                on MergeEvent::FirstPhaseCompleted => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move { Ok(Transition { next_state: MergeState::LastPhase, actions: vec![] }) })
                };
                on MergeEvent::FirstPhaseLeftOpen => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move { Ok(Transition { next_state: MergeState::Draining, actions: vec![] }) })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::LastPhase {
                on MergeEvent::ReceivedEOF => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move { Ok(Transition { next_state: MergeState::Draining, actions: vec![] }) })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::Draining {
                on MergeEvent::DrainComplete => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: MergeState::Finalizing,
                            actions: vec![
                                MergeAction::ForwardEOF,
                                MergeAction::SendCompletion,
                                MergeAction::Cleanup,
                                MergeAction::Host(SupervisorAction::CloseMailbox),
                                MergeAction::Host(SupervisorAction::SettlePublications),
                                MergeAction::Host(SupervisorAction::Emit(MergeEvent::FinalizationCompleted)),
                            ],
                        })
                    })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::Finalizing {
                on MergeEvent::FinalizationCompleted => |_state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    Box::pin(async move { Ok(Transition { next_state: MergeState::Drained, actions: vec![] }) })
                };
                on MergeEvent::Error => merge_error_arm!();
            }

            state MergeState::Failing {
                on MergeEvent::TerminationSettled => |state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    let MergeState::Failing(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: MergeState::Failed(cause), actions: vec![] }) })
                };
            }

            state MergeState::Cancelling {
                on MergeEvent::TerminationSettled => |state: &MergeState, _event: &MergeEvent, _ctx: &mut MergeContext| {
                    let MergeState::Cancelling(cause) = state else { unreachable!() };
                    let cause = cause.clone();
                    Box::pin(async move { Ok(Transition { next_state: MergeState::Cancelled(cause), actions: vec![] }) })
                };
            }

            state MergeState::Drained {}
            state MergeState::Failed {}
            state MergeState::Cancelled {}

            unhandled => |state: &MergeState, event: &MergeEvent, _ctx: &mut MergeContext| {
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
                let event_name = obzenflow_fsm::EventVariant::variant_name(event).to_string();
                Box::pin(async move {
                    if ignored {
                        return Ok(());
                    }
                    Err(FsmError::UnhandledEvent {
                        state: state_name,
                        event: event_name,
                    })
                })
            };
        }
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        SupervisorKind::Merge
    }

    fn registration(
        &self,
        context: &Self::Context,
        descriptor: SupervisorDescriptor,
    ) -> Registration {
        base::register_stage(
            context.data_journal.clone(),
            context.flow_context(),
            descriptor,
        )
    }
}

#[async_trait::async_trait]
impl HandlerSupervised for MergeSupervisor {
    type Handler = ();

    fn lifecycle_phase(&self, state: &Self::State) -> LifecyclePhase {
        state.lifecycle_phase()
    }

    fn accounting(&self, context: &Self::Context) -> ExecutionAccounting {
        snapshot_stage_accounting(&context.instrumentation)
    }

    fn after_transition(&mut self, state: &Self::State, context: &Self::Context) {
        context
            .instrumentation
            .transition_to_state(state.variant_name());
    }

    fn supervisor_action(&self, action: &Self::Action) -> Option<SupervisorAction<Self::Event>> {
        match action {
            MergeAction::Host(action) => Some(action.clone()),
            _ => None,
        }
    }

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn event_for_action_error(&self, msg: String) -> MergeEvent {
        MergeEvent::Error(msg)
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, BoxError> {
        match state {
            MergeState::FirstPhase => dispatch_first_phase(self, state, ctx).await,
            MergeState::LastPhase => dispatch_last_phase(self, state, ctx).await,
            MergeState::Draining => dispatch_draining(self, ctx).await,
            MergeState::Drained | MergeState::Failed(_) | MergeState::Cancelled(_) => {
                Ok(EventLoopDirective::Terminate)
            }
            _ => Ok(EventLoopDirective::Continue),
        }
    }
}

impl ExternalEventPolicy for MergeSupervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, MergeState::Created | MergeState::Initialized) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn defer_external_event(state: &Self::State, event: &Self::Event) -> bool {
        matches!(state, MergeState::Initializing) && matches!(event, MergeEvent::Ready)
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(
            state,
            MergeState::Drained | MergeState::Failed(_) | MergeState::Cancelled(_)
        ) {
            None
        } else {
            Some(MergeEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}

fn ensure_subscriptions(sup: &mut MergeSupervisor, ctx: &mut MergeContext) {
    if sup.first_phase_subscription.is_none() {
        sup.first_phase_subscription = ctx.first_phase_subscription.take();
    }
    if sup.held_subscription.is_none() {
        sup.held_subscription = ctx.held_subscription.take();
    }
    if let (Some(generation), Some(held)) =
        (ctx.pending_promotion.take(), sup.held_subscription.as_mut())
    {
        held.promote_held_readers(generation);
    }
    if let (Some(first_phase), Some(held)) = (
        sup.first_phase_subscription.as_ref(),
        sup.held_subscription.as_mut(),
    ) {
        if held.records_order() {
            held.set_selection_base(first_phase.delivered_total());
        }
    }
}

fn renew_held_edges(ctx: &MergeContext) {
    for producer in &ctx.held_producers {
        if let Some(reader) = ctx.backpressure_readers.get(producer) {
            reader.renew_hold();
        }
    }
}

enum FlushOutcome {
    Committed,
    BackedOff,
}

async fn flush_pending_outputs(
    sup: &mut MergeSupervisor,
    ctx: &mut MergeContext,
) -> Result<FlushOutcome, BoxError> {
    let Some(mut row) = ctx.pending.take() else {
        return Ok(FlushOutcome::Committed);
    };
    let flow_context = ctx.flow_context();
    while let Some(pending) = row.outputs.pop_front() {
        match drain_one_pending(
            pending,
            &flow_context,
            ctx.stage_id,
            ctx.heartbeat
                .as_ref()
                .map(|heartbeat| heartbeat.state.clone()),
            &ctx.data_journal,
            Some(&row.parent),
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_stall,
            Some(&ctx.output_contract),
            &mut row.outputs,
        )
        .await?
        {
            DrainOutcome::Committed { was_data } => {
                if was_data {
                    if let Some(subscription) = sup.subscription(row.side) {
                        subscription.track_output_event();
                    }
                }
            }
            DrainOutcome::BackedOff => {
                ctx.pending = Some(row);
                return Ok(FlushOutcome::BackedOff);
            }
        }
    }
    if let Some(reader) = row
        .upstream
        .and_then(|upstream| ctx.backpressure_readers.get(&upstream))
    {
        reader.ack_consumed(1);
    }
    Ok(FlushOutcome::Committed)
}

fn merged_output(ctx: &MergeContext, event: &ChainEvent) -> Result<ChainEvent, BoxError> {
    if matches!(event.processing.status, ProcessingStatus::Error { .. }) {
        return Ok(event.clone());
    }
    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
    Ok(match &event.payload {
        ChainPayload::Fact(value) => ChainEventFactory::derived_data_event(
            writer_id,
            event,
            event.event_type(),
            event.payload_schema_version,
            value.clone(),
            ctx.lineage_policy,
        ),
        _ => event.clone(),
    })
}

fn queue_row(
    sup: &mut MergeSupervisor,
    ctx: &mut MergeContext,
    side: Side,
    envelope: DeliveredRecord<ChainPayload>,
) -> Result<(), BoxError> {
    let subscription = sup
        .subscription(side)
        .ok_or("merge row delivered without its subscription")?;
    let upstream = subscription.last_delivered_upstream_stage();
    let scope = ctx.runtime_execution.dispatch_scope(
        ctx.stage_id,
        subscription.last_delivered_stage_input_position(),
        subscription.last_delivered_generation(),
    );
    let event = envelope.authored();
    if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream) {
        heartbeat.state.record_data_read(upstream, event.id);
        heartbeat.state.record_last_consumed(event.id);
    }
    let output = merged_output(ctx, &event)?;
    ctx.instrumentation
        .events_processed_total
        .fetch_add(1, Ordering::Relaxed);
    ctx.pending = Some(PendingRow {
        side,
        parent: envelope,
        upstream,
        outputs: VecDeque::from([PendingOutput {
            causal: crate::supervised_base::publication::capture(),
            event: output,
            scope,
        }]),
    });
    Ok(())
}

async fn flip(
    sup: &MergeSupervisor,
    ctx: &mut MergeContext,
    target: obzenflow_core::ReaderGeneration,
    side: Side,
) -> CatchUpDisposition {
    let caught_up = |subscription: Option<&UpstreamSubscription<ChainEvent>>| {
        subscription.is_some_and(|subscription| subscription.all_readers_caught_up(target))
    };
    let first_phase_caught_up = caught_up(sup.first_phase_subscription.as_ref());
    let all = match side {
        Side::FirstPhase => first_phase_caught_up,
        Side::Held => first_phase_caught_up && caught_up(sup.held_subscription.as_ref()),
    };
    let disposition = ctx.catch_up.frontier_reached(target, all).await;
    if let (Side::FirstPhase, CatchUpDisposition::Crossed(crossed)) = (side, &disposition) {
        ctx.pending_promotion = Some(*crossed);
    }
    disposition
}

fn max_generation(sup: &MergeSupervisor) -> obzenflow_core::ReaderGeneration {
    let side_max = |side: Option<&UpstreamSubscription<ChainEvent>>| {
        side.map(|subscription| subscription.max_reader_generation())
            .unwrap_or_default()
    };
    side_max(sup.first_phase_subscription.as_ref()).max(side_max(sup.held_subscription.as_ref()))
}

enum PollStep {
    Idle,
    Progressed,
    SideEnded(EofKind),
    Directive(EventLoopDirective<MergeEvent>),
}

async fn handle_control(
    sup: &mut MergeSupervisor,
    ctx: &mut MergeContext,
    side: Side,
    envelope: &DeliveredRecord<ChainPayload>,
) -> Result<PollStep, BoxError> {
    let ChainPayload::FlowControl(signal) = &envelope.payload else {
        return Ok(PollStep::Progressed);
    };
    if let FlowControlPayload::CatchUpComplete {
        generation: announced,
        ..
    } = signal
    {
        return Ok(PollStep::Directive(
            match flip(sup, ctx, *announced, side).await {
                CatchUpDisposition::Consumed | CatchUpDisposition::Crossed(_) => {
                    EventLoopDirective::Continue
                }
                CatchUpDisposition::Failed(fatal) | CatchUpDisposition::Refused(fatal) => {
                    EventLoopDirective::Transition(MergeEvent::Error(fatal.detail))
                }
            },
        ));
    }

    let subscription = sup
        .subscription(side)
        .ok_or("merge control delivered without its subscription")?;
    let upstream_stage = subscription.last_delivered_upstream_stage();
    let last_eof_outcome = subscription.last_eof_outcome().cloned();
    let reader_count = match side {
        Side::FirstPhase => ctx.first_phase_contract_state.len(),
        Side::Held => ctx.held_contract_state.len(),
    };

    if envelope.is_eof() {
        let target = max_generation(sup);
        if target != obzenflow_core::ReaderGeneration(0) {
            match flip(sup, ctx, target, side).await {
                CatchUpDisposition::Failed(fatal) | CatchUpDisposition::Refused(fatal) => {
                    return Ok(PollStep::Directive(EventLoopDirective::Transition(
                        MergeEvent::Error(fatal.detail),
                    )));
                }
                CatchUpDisposition::Consumed | CatchUpDisposition::Crossed(_) => {}
            }
        }
        if let Some(kind) = last_eof_outcome
            .as_ref()
            .and_then(|outcome| outcome.worst_kind)
        {
            ctx.terminal_eof_kind = Some(
                ctx.terminal_eof_kind
                    .map_or(kind, |current| current.worst(kind)),
            );
        }
        ctx.buffered_eof = Some(envelope.authored());
    }

    let resolution = resolve_control_event_awaiting_pauses(
        signal,
        envelope,
        ctx.control_strategy.as_ref(),
        &mut ctx.processing_context,
        None,
        None,
        last_eof_outcome.as_ref(),
        upstream_stage,
        reader_count,
        false,
        &ctx.stage_name,
    )
    .await;

    if matches!(
        resolution,
        ControlAction::Forward | ControlAction::ForwardAndDrain
    ) {
        forward_control_event(
            envelope,
            ctx.stage_id,
            &ctx.stage_name,
            obzenflow_core::event::context::StageType::Merge,
            &ctx.data_journal,
        )
        .await?;
    }
    if !envelope.is_eof() {
        return Ok(PollStep::Progressed);
    }
    if let Some(subscription) = sup.subscription(side) {
        subscription.take_last_eof_outcome();
    }
    Ok(match last_eof_outcome.filter(|outcome| outcome.is_final) {
        Some(outcome) => PollStep::SideEnded(outcome.worst_kind.unwrap_or(EofKind::Natural)),
        None => PollStep::Progressed,
    })
}

async fn contract_tick(
    sup: &mut MergeSupervisor,
    ctx: &mut MergeContext,
    side: Side,
) -> Result<(), BoxError> {
    let (subscription, state, last) = match side {
        Side::FirstPhase => (
            sup.first_phase_subscription.as_mut(),
            &mut ctx.first_phase_contract_state,
            &mut ctx.first_phase_last_contract_check,
        ),
        Side::Held => (
            sup.held_subscription.as_mut(),
            &mut ctx.held_contract_state,
            &mut ctx.held_last_contract_check,
        ),
    };
    if let Some(subscription) = subscription {
        subscription
            .maybe_check_contracts_tick(&mut state[..], last)
            .await
            .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
            .into_result()?;
    }
    Ok(())
}

async fn poll_side(
    sup: &mut MergeSupervisor,
    state: &MergeState,
    ctx: &mut MergeContext,
    side: Side,
) -> Result<PollStep, BoxError> {
    let poll = {
        let (subscription, contract_state) = match side {
            Side::FirstPhase => (
                sup.first_phase_subscription.as_mut(),
                &mut ctx.first_phase_contract_state,
            ),
            Side::Held => (sup.held_subscription.as_mut(), &mut ctx.held_contract_state),
        };
        let Some(subscription) = subscription else {
            return Ok(PollStep::Idle);
        };
        subscription
            .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
            .await
    };
    match poll {
        PollResult::Event(envelope) => {
            let upstream = sup
                .subscription(side)
                .and_then(|subscription| subscription.last_delivered_upstream_stage())
                .ok_or("delivered event must identify its upstream stage")?;
            ctx.instrumentation.record_consumed(&envelope, upstream);
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);
            let step = if envelope.is_control() {
                handle_control(sup, ctx, side, &envelope).await?
            } else if envelope.consumes_data_credit() {
                queue_row(sup, ctx, side, envelope)?;
                PollStep::Progressed
            } else {
                PollStep::Progressed
            };
            if let PollStep::Directive(_) = step {
                return Ok(step);
            }
            contract_tick(sup, ctx, side).await?;
            Ok(step)
        }
        PollResult::CursorAdvanced {
            upstream,
            completed_data_rows,
        } => {
            crate::backpressure::complete_filtered_data_rows(
                &ctx.backpressure_readers,
                upstream,
                completed_data_rows,
            );
            Ok(PollStep::Progressed)
        }
        PollResult::NoEvents => {
            contract_tick(sup, ctx, side).await?;
            Ok(PollStep::Idle)
        }
        PollResult::Error(error) => Ok(PollStep::Directive(EventLoopDirective::Transition(
            MergeEvent::Error(format!("Merge subscription error: {error}")),
        ))),
    }
}

async fn dispatch_first_phase(
    sup: &mut MergeSupervisor,
    state: &MergeState,
    ctx: &mut MergeContext,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    ensure_subscriptions(sup, ctx);
    renew_held_edges(ctx);
    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);
    if let FlushOutcome::BackedOff = flush_pending_outputs(sup, ctx).await? {
        return Ok(EventLoopDirective::Continue);
    }
    match poll_side(sup, state, ctx, Side::FirstPhase).await? {
        PollStep::Directive(directive) => Ok(directive),
        PollStep::SideEnded(EofKind::Natural) => {
            tracing::info!(
                stage_name = %ctx.stage_name,
                strategy = ?ctx.strategy,
                "Merge first phase completed; reading held producers"
            );
            Ok(EventLoopDirective::Transition(
                MergeEvent::FirstPhaseCompleted,
            ))
        }
        PollStep::SideEnded(_) => {
            tracing::info!(
                stage_name = %ctx.stage_name,
                strategy = ?ctx.strategy,
                "Merge first phase left open; draining without held producers"
            );
            Ok(EventLoopDirective::Transition(
                MergeEvent::FirstPhaseLeftOpen,
            ))
        }
        PollStep::Progressed => Ok(EventLoopDirective::Continue),
        PollStep::Idle => {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Ok(EventLoopDirective::Continue)
        }
    }
}

async fn dispatch_last_phase(
    sup: &mut MergeSupervisor,
    state: &MergeState,
    ctx: &mut MergeContext,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    ensure_subscriptions(sup, ctx);
    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);
    if let FlushOutcome::BackedOff = flush_pending_outputs(sup, ctx).await? {
        return Ok(EventLoopDirective::Continue);
    }
    match poll_side(sup, state, ctx, Side::Held).await? {
        PollStep::Directive(directive) => Ok(directive),
        PollStep::SideEnded(_) => Ok(EventLoopDirective::Transition(MergeEvent::ReceivedEOF)),
        PollStep::Progressed => Ok(EventLoopDirective::Continue),
        PollStep::Idle => {
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Ok(EventLoopDirective::Continue)
        }
    }
}

async fn dispatch_draining(
    sup: &mut MergeSupervisor,
    ctx: &mut MergeContext,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    ensure_subscriptions(sup, ctx);
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }
    if let FlushOutcome::BackedOff = flush_pending_outputs(sup, ctx).await? {
        return Ok(EventLoopDirective::Continue);
    }
    Ok(EventLoopDirective::Transition(MergeEvent::DrainComplete))
}
