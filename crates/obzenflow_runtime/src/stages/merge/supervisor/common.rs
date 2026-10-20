// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::VecDeque;
use std::sync::atomic::Ordering;

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;

use super::BoxError;
use crate::messaging::{DeliveredRecord, PollResult, UpstreamSubscription};
use crate::stages::common::input_facts::{EofRead, WatermarkRead};
use crate::stages::common::supervision::backpressure_drain::{
    drain_one_pending, DrainOutcome, PendingOutput,
};
use crate::stages::common::supervision::control_resolution::await_signal_gate;
use crate::stages::common::supervision::forward_control_event::forward_control_event;
use crate::stages::merge::fsm::{MergeEvent, MergeInput, MergeResources, MergeState, PendingRow};
use crate::supervised_base::EventLoopDirective;

pub(super) fn align_held_selection(ctx: &mut MergeResources) {
    if let (Some(first_phase), Some(held)) = (
        ctx.first_phase.subscription.as_ref(),
        ctx.held.subscription.as_mut(),
    ) {
        if held.records_order() {
            held.set_selection_base(first_phase.delivered_total());
        }
    }
}

#[derive(Default)]
pub(super) struct Flushed {
    data_outputs: usize,
    pub(super) backed_off: bool,
}

pub(super) async fn flush_pending_outputs(ctx: &mut MergeResources) -> Result<Flushed, BoxError> {
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

fn merged_output(ctx: &MergeResources, event: &ChainEvent) -> Result<ChainEvent, BoxError> {
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
    ctx: &mut MergeResources,
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

pub(super) async fn dispatch_reading(
    state: &MergeState,
    input: &mut MergeInput,
    ctx: &mut MergeResources,
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
                    subscription
                        .check_contracts(&mut contract_state[..])
                        .await
                        .into_result()?;
                    let eof = EofRead::new(
                        envelope,
                        subscription.take_last_eof_outcome(),
                        subscription.last_delivered_generation(),
                        subscription.reader_frontier(),
                    );
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
