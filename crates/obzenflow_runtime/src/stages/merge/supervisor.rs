// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::VecDeque;
use std::error::Error;
use std::sync::atomic::Ordering;

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::provenance::ExecutionAccounting;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, StateMachine, StateVariant};

use super::fsm::{MergeAction, MergeContext, MergeEvent, MergeInput, MergeState, PendingRow};
use super::transitions;
use crate::messaging::{DeliveredRecord, PollResult, UpstreamSubscription};
use crate::metrics::instrumentation::snapshot_stage_accounting;
use crate::stages::common::input_facts::{EofRead, WatermarkRead};
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::stages::common::supervision::backpressure_drain::{
    drain_one_pending, DrainOutcome, PendingOutput,
};
use crate::stages::common::supervision::control_resolution::await_signal_gate;
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
}

impl MergeSupervisor {
    pub(crate) fn new(name: String, stage_id: StageId) -> Self {
        Self { name, stage_id }
    }
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
                on MergeEvent::Initialize => transitions::initialize;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::Initializing {
                on MergeEvent::InitializationCompleted => transitions::initialized;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::Initialized {
                on MergeEvent::Ready => transitions::ready;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::Starting {
                on MergeEvent::ActivationCompleted => transitions::activated;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::FirstPhase {
                on MergeEvent::WatermarkRead => transitions::watermark;
                on MergeEvent::EofRead => transitions::first_phase_eof;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::LastPhase {
                on MergeEvent::WatermarkRead => transitions::watermark;
                on MergeEvent::EofRead => transitions::last_phase_eof;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::Draining {
                on MergeEvent::DrainComplete => transitions::drain_complete;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::Finalizing {
                on MergeEvent::FinalizationCompleted => transitions::finalized;
                on MergeEvent::Error => transitions::failure;
            }

            state MergeState::Failing {
                on MergeEvent::TerminationSettled => transitions::failure_settled;
            }

            state MergeState::Cancelling {
                on MergeEvent::TerminationSettled => transitions::cancellation_settled;
            }

            state MergeState::Drained {}
            state MergeState::Failed {}
            state MergeState::Cancelled {}

            unhandled => transitions::unhandled;
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
            MergeState::FirstPhase(_) => {
                align_held_selection(ctx);
                renew_held_edges(ctx);
                let mut input = std::mem::take(&mut ctx.first_phase);
                let directive = dispatch_reading(state, &mut input, ctx).await;
                ctx.first_phase = input;
                directive
            }
            MergeState::LastPhase(_) => {
                align_held_selection(ctx);
                let mut input = std::mem::take(&mut ctx.held);
                let directive = dispatch_reading(state, &mut input, ctx).await;
                ctx.held = input;
                directive
            }
            MergeState::Draining(_) => dispatch_draining(ctx).await,
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

fn align_held_selection(ctx: &mut MergeContext) {
    if let (Some(first_phase), Some(held)) = (
        ctx.first_phase.subscription.as_ref(),
        ctx.held.subscription.as_mut(),
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

#[derive(Default)]
struct Flushed {
    data_outputs: usize,
    backed_off: bool,
}

async fn flush_pending_outputs(ctx: &mut MergeContext) -> Result<Flushed, BoxError> {
    let Some(mut row) = ctx.pending.take() else {
        return Ok(Flushed::default());
    };
    let flow_context = ctx.flow_context();
    let mut data_outputs = 0;
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
                data_outputs += usize::from(was_data);
            }
            DrainOutcome::BackedOff => {
                ctx.pending = Some(row);
                return Ok(Flushed {
                    data_outputs,
                    backed_off: true,
                });
            }
        }
    }
    if let Some(reader) = row
        .upstream
        .and_then(|upstream| ctx.backpressure_readers.get(&upstream))
    {
        reader.ack_consumed(1);
    }
    Ok(Flushed {
        data_outputs,
        backed_off: false,
    })
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
    subscription: &UpstreamSubscription<ChainEvent>,
    ctx: &mut MergeContext,
    envelope: DeliveredRecord<ChainPayload>,
) -> Result<(), BoxError> {
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

async fn contract_tick(
    subscription: &mut UpstreamSubscription<ChainEvent>,
    contract_state: &mut [crate::messaging::upstream_subscription::ReaderProgress],
    last_contract_check: &mut Option<tokio::time::Instant>,
) -> Result<(), BoxError> {
    subscription
        .maybe_check_contracts_tick(contract_state, last_contract_check)
        .await
        .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
        .into_result()?;
    Ok(())
}

async fn dispatch_reading(
    state: &MergeState,
    input: &mut MergeInput,
    ctx: &mut MergeContext,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);
    let MergeInput {
        subscription,
        contract_state,
        last_contract_check,
    } = input;
    let flushed = flush_pending_outputs(ctx).await?;
    let Some(subscription) = subscription.as_mut() else {
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        return Ok(EventLoopDirective::Continue);
    };
    for _ in 0..flushed.data_outputs {
        subscription.track_output_event();
    }
    if flushed.backed_off {
        return Ok(EventLoopDirective::Continue);
    }

    let poll = subscription
        .poll_next_with_state(state.variant_name(), Some(&mut contract_state[..]))
        .await;
    match poll {
        PollResult::Event(envelope) => {
            let upstream = subscription
                .last_delivered_upstream_stage()
                .ok_or("delivered event must identify its upstream stage")?;
            ctx.instrumentation.record_consumed(&envelope, upstream);
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);
            if let ChainPayload::FlowControl(signal) = &envelope.payload {
                await_signal_gate(
                    signal,
                    &envelope,
                    ctx.control_strategy.as_ref(),
                    &mut ctx.processing_context,
                    &ctx.stage_name,
                )
                .await;
                if let FlowControlPayload::CatchUpComplete { generation, .. } = signal {
                    return Ok(EventLoopDirective::Transition(MergeEvent::WatermarkRead(
                        WatermarkRead {
                            generation: *generation,
                            frontier: subscription.reader_frontier(),
                        },
                    )));
                }
                if envelope.is_eof() {
                    let eof = EofRead::new(
                        envelope,
                        subscription.take_last_eof_outcome(),
                        subscription.last_delivered_generation(),
                        subscription.reader_frontier(),
                    );
                    contract_tick(subscription, contract_state, last_contract_check).await?;
                    return Ok(EventLoopDirective::Transition(MergeEvent::EofRead(eof)));
                }
                forward_control_event(
                    &envelope,
                    ctx.stage_id,
                    &ctx.stage_name,
                    obzenflow_core::event::context::StageType::Merge,
                    &ctx.data_journal,
                )
                .await?;
            } else if envelope.consumes_data_credit() {
                queue_row(subscription, ctx, envelope)?;
            }
            contract_tick(subscription, contract_state, last_contract_check).await?;
            Ok(EventLoopDirective::Continue)
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
            Ok(EventLoopDirective::Continue)
        }
        PollResult::NoEvents => {
            contract_tick(subscription, contract_state, last_contract_check).await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Ok(EventLoopDirective::Continue)
        }
        PollResult::Error(error) => Ok(EventLoopDirective::Transition(MergeEvent::Error(format!(
            "Merge subscription error: {error}"
        )))),
    }
}

async fn dispatch_draining(
    ctx: &mut MergeContext,
) -> Result<EventLoopDirective<MergeEvent>, BoxError> {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }
    if flush_pending_outputs(ctx).await?.backed_off {
        return Ok(EventLoopDirective::Continue);
    }
    Ok(EventLoopDirective::Transition(MergeEvent::DrainComplete))
}
