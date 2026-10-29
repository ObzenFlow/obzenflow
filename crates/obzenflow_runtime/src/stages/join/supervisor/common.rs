// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::upstream_subscription::StageInputPosition;
use crate::messaging::DeliveredRecord;
use crate::messaging::UpstreamSubscription;
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::control_resolution::await_signal_gate;
use crate::stages::common::supervision::error_routing::{error_row_in_scope, processing_failed};
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::forward_control_event::forward_control_event;
use crate::stages::common::supervision::stage_fatal::{record_stage_fatal, StageFatalCommit};
use crate::stages::join::fsm::{
    EofRead, JoinEvent, JoinResources, JoinSubscriptionSide, WatermarkRead,
};
use crate::stages::observer::dispatch::{
    run_join_after_output_observers, run_join_before_input_observers,
};
use crate::stages::observer::{
    JoinDeliverySnapshot, JoinObserverContext, JoinObserverOccurrence, JoinSide, JoinSignalKind,
    JoinSignalSnapshot,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::AppendOptions;

use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::{ChainEvent, StageId, WriterId};

use super::JoinSupervisor;

pub(super) fn ensure_subscriptions<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinResources<H>,
) {
    if sup.reference_subscription.is_none() {
        sup.reference_subscription = ctx.reference_subscription.take();
    }
    if sup.stream_subscription.is_none() {
        sup.stream_subscription = ctx.stream_subscription.take();
    }
    if let (Some(reference), Some(stream)) = (
        sup.reference_subscription.as_ref(),
        sup.stream_subscription.as_mut(),
    ) {
        if stream.records_order() {
            stream.set_selection_base(reference.delivered_total());
        }
    }
}

pub(super) async fn read_control<H: UnifiedJoinHandler>(
    ctx: &mut JoinResources<H>,
    subscription: &mut UpstreamSubscription<ChainEvent>,
    side: JoinSubscriptionSide,
    beside: Option<&UpstreamSubscription<ChainEvent>>,
    observed: Option<JoinSide>,
    envelope: DeliveredRecord<ChainPayload>,
) -> Result<Option<JoinEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let ChainPayload::FlowControl(signal) = &envelope.payload else {
        return Ok(None);
    };
    let watermark = match signal {
        FlowControlPayload::CatchUpComplete { generation, .. } => Some(*generation),
        _ => None,
    };
    if watermark.is_none() && observed.is_some() {
        if let Some(signal_snapshot) = signal_snapshot(observed, &envelope.authored()) {
            observe_join_input(
                ctx,
                ctx.runtime_execution.dispatch_scope(
                    ctx.stage_id,
                    None,
                    subscription.last_delivered_generation(),
                ),
                &envelope.authored(),
                None,
                Some(&signal_snapshot),
                Some(&envelope),
            )
            .await?;
        }
    }
    await_signal_gate(
        signal,
        &envelope,
        ctx.control_strategy.as_ref(),
        &mut ctx.processing_context,
        &ctx.stage_name,
    )
    .await;

    let frontier = beside.map_or(subscription.reader_frontier(), |beside| {
        subscription.reader_frontier().and(beside.reader_frontier())
    });
    if let Some(generation) = watermark {
        return Ok(Some(JoinEvent::WatermarkRead(WatermarkRead {
            generation,
            frontier,
        })));
    }
    if envelope.is_eof() {
        let contract_state = match side {
            JoinSubscriptionSide::Reference => &mut ctx.reference_contract_state,
            JoinSubscriptionSide::Stream => &mut ctx.stream_contract_state,
        };
        subscription
            .check_contracts(&mut contract_state[..])
            .await
            .into_result()?;
        let outcome = subscription.take_last_eof_outcome();
        let generation = subscription.last_delivered_generation();
        let eof = EofRead::new(envelope, outcome, generation, frontier);
        return Ok(Some(match side {
            JoinSubscriptionSide::Reference => JoinEvent::ReferenceEofRead(eof),
            JoinSubscriptionSide::Stream => JoinEvent::StreamEofRead(eof),
        }));
    }
    forward_control_to_journal(ctx, &envelope).await?;
    Ok(None)
}

pub(super) fn renew_held_inputs<H: UnifiedJoinHandler>(
    ctx: &JoinResources<H>,
    held: JoinSubscriptionSide,
) {
    let upstreams = match held {
        JoinSubscriptionSide::Reference => ctx.reference_subscription_factory.upstream_stage_ids(),
        JoinSubscriptionSide::Stream => ctx.stream_subscription_factory.upstream_stage_ids(),
    };
    for upstream in upstreams {
        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
            reader.renew_hold();
        }
    }
}

pub(super) async fn forward_control_to_journal<H: UnifiedJoinHandler>(
    ctx: &JoinResources<H>,
    envelope: &DeliveredRecord<ChainPayload>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    forward_control_event(
        envelope,
        ctx.stage_id,
        &ctx.stage_name,
        StageType::Join,
        &ctx.data_journal,
    )
    .await?;
    Ok(())
}

pub(super) async fn record_join_stage_fatal<H: UnifiedJoinHandler>(
    ctx: &JoinResources<H>,
    fatal: &StageFatal,
    parent: Option<&DeliveredRecord<ChainPayload>>,
    input_position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let writer_id = ctx
        .writer_id
        .ok_or_else(|| "fatal join input has no stage writer id".to_string())?;
    record_stage_fatal(
        fatal,
        StageFatalCommit {
            error_journal: &ctx.error_journal,
            writer_id,
            stage_id: ctx.stage_id,
            stage_key: &ctx.stage_name,
            input_position,
            parent: parent.map(DeliveredRecord::record),
            lineage: ctx.lineage_policy,
        },
    )
    .await?;
    Ok(())
}

pub(super) async fn dead_letter_reference<H: UnifiedJoinHandler>(
    ctx: &JoinResources<H>,
    scope: obzenflow_core::MiddlewareExecutionScope,
    envelope: &DeliveredRecord<ChainPayload>,
    delivery: Option<&JoinDeliverySnapshot>,
    error: &HandlerError,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let input = envelope.authored();
    let record = error_row_in_scope(
        processing_failed(
            WriterId::from(ctx.stage_id),
            &make_flow_context(
                &ctx.flow_name,
                &ctx.flow_id.to_string(),
                &ctx.stage_name,
                ctx.stage_id,
                StageType::Join,
            ),
            &input,
            error,
            ctx.lineage_policy,
        ),
        scope,
    );
    ctx.instrumentation.record_error(error.kind());
    observe_join_outputs(
        ctx,
        scope,
        Some(&input),
        delivery,
        None,
        std::slice::from_ref(&record),
        Some(envelope),
    )
    .await?;
    crate::supervised_base::publication::append(
        &ctx.error_journal,
        record,
        AppendOptions::from_record(Some(envelope))?,
    )
    .await
    .map_err(|e| format!("Failed to write join error event: {e}"))?;
    Ok(())
}

pub(super) async fn flush_pending_outputs<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinResources<H>,
) -> Result<FlushOutcome, Box<dyn std::error::Error + Send + Sync>> {
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Join,
    );

    while let Some(pending) = ctx.pending_outputs.pop_front() {
        match drain_one_pending(
            pending,
            &flow_context,
            ctx.stage_id,
            ctx.heartbeat.as_ref().map(|h| h.state.clone()),
            &ctx.data_journal,
            ctx.pending_parent.as_ref(),
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_stall,
            Some(&ctx.output_contract),
            &mut ctx.pending_outputs,
        )
        .await?
        {
            DrainOutcome::Committed { was_data } => {
                if was_data {
                    track_output_event_for_pending_source(sup, ctx);
                }
            }
            DrainOutcome::BackedOff => return Ok(FlushOutcome::Blocked),
        }
    }

    if let Some(ack) = ctx.pending_subscription_ack.take() {
        if let Some(reader) = ctx.backpressure_readers.get(&ack.upstream) {
            reader.ack_consumed(1);
        }
    }
    ctx.pending_parent = None;

    Ok(FlushOutcome::Drained)
}

pub(super) async fn observe_join_input<H: UnifiedJoinHandler>(
    ctx: &JoinResources<H>,
    scope: obzenflow_core::MiddlewareExecutionScope,
    _input: &ChainEvent,
    delivery: Option<&JoinDeliverySnapshot>,
    signal: Option<&JoinSignalSnapshot>,
    _parent: Option<&DeliveredRecord<ChainPayload>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !ctx.observers.has_join() || scope.is_deterministic_replay() {
        return Ok(());
    }
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Join,
    );
    let occurrence = match (delivery, signal) {
        (Some(delivery), None) => JoinObserverOccurrence::Delivery(delivery),
        (None, Some(signal)) => JoinObserverOccurrence::Signal(signal),
        _ => return Err("join observer occurrence must be exactly one delivery or signal".into()),
    };
    let observer_ctx = JoinObserverContext::new(ctx.flow_id, &flow_context, occurrence);
    run_join_before_input_observers(&ctx.observers, scope, &observer_ctx);
    Ok(())
}

pub(super) async fn observe_join_outputs<H: UnifiedJoinHandler>(
    ctx: &JoinResources<H>,
    scope: obzenflow_core::MiddlewareExecutionScope,
    _input: Option<&ChainEvent>,
    delivery: Option<&JoinDeliverySnapshot>,
    signal: Option<&JoinSignalSnapshot>,
    outputs: &[ChainEvent],
    _parent: Option<&DeliveredRecord<ChainPayload>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if !ctx.observers.has_join() || scope.is_deterministic_replay() {
        return Ok(());
    }
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Join,
    );
    let occurrence = match (delivery, signal) {
        (Some(delivery), None) => JoinObserverOccurrence::Delivery(delivery),
        (None, Some(signal)) => JoinObserverOccurrence::Signal(signal),
        _ => return Err("join observer occurrence must be exactly one delivery or signal".into()),
    };
    let observer_ctx = JoinObserverContext::new(ctx.flow_id, &flow_context, occurrence);
    run_join_after_output_observers(&ctx.observers, scope, &observer_ctx, outputs);
    Ok(())
}

pub(super) fn delivery_snapshot(
    side: JoinSide,
    source_stage_id: StageId,
    stage_input_position: Option<StageInputPosition>,
    envelope: &DeliveredRecord<ChainPayload>,
    reference_high_water: &obzenflow_core::event::vector_clock::VectorClock,
) -> Result<JoinDeliverySnapshot, Box<dyn std::error::Error + Send + Sync>> {
    let position =
        stage_input_position.ok_or("join delivered data input without StageInputPosition")?;
    Ok(JoinDeliverySnapshot::new(
        side,
        source_stage_id,
        position,
        envelope.clone(),
        reference_high_water.clone(),
    ))
}

pub(super) fn signal_snapshot(
    side: Option<JoinSide>,
    input: &ChainEvent,
) -> Option<JoinSignalSnapshot> {
    let signal = match &input.payload {
        ChainPayload::FlowControl(FlowControlPayload::Eof { .. }) => JoinSignalKind::Eof,
        ChainPayload::FlowControl(FlowControlPayload::Drain) => JoinSignalKind::Drain,
        ChainPayload::FlowControl(_) => JoinSignalKind::OtherControl,
        _ => return None,
    };
    Some(JoinSignalSnapshot::new(side, signal))
}

pub(super) fn observe_reference_envelope<H: UnifiedJoinHandler>(
    ctx: &mut JoinResources<H>,
    envelope: &DeliveredRecord<ChainPayload>,
) {
    // Conservative interim for FLOWIP-071h: merge all reference-side ancestry into one
    // high-water clock (component-wise max).
    CausalOrderingService::update_with_parent(
        &mut ctx.reference_high_water_clock,
        &envelope.envelope.provenance.journal.vector_clock,
    );
}

fn track_output_event_for_pending_source<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &JoinResources<H>,
) {
    match ctx.pending_subscription_ack.map(|ack| ack.side) {
        Some(JoinSubscriptionSide::Reference) => {
            if let Some(sub) = sup.reference_subscription.as_mut() {
                sub.track_output_event();
            }
        }
        Some(JoinSubscriptionSide::Stream) => {
            if let Some(sub) = sup.stream_subscription.as_mut() {
                sub.track_output_event();
            }
        }
        None => {
            if let Some(sub) = sup.reference_subscription.as_mut() {
                sub.track_output_event();
            }
            if let Some(sub) = sup.stream_subscription.as_mut() {
                sub.track_output_event();
            }
        }
    }
}

pub(super) enum FlushOutcome {
    Drained,
    /// Backpressure blocked draining; return early and retry later.
    Blocked,
}

pub(in crate::stages::join) async fn emit_join_heartbeat_if_due<
    H: UnifiedJoinHandler + Send + Sync + 'static,
>(
    ctx: &mut JoinResources<H>,
    stage_id: StageId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let interval = ctx.heartbeat_interval;
    if interval == 0 || ctx.events_since_last_heartbeat < interval {
        return Ok(());
    }

    let Some(writer_id) = ctx.writer_id else {
        // Writer not initialised yet; skip heartbeat rather than failing.
        return Ok(());
    };

    let events_since_last = ctx.events_since_last_heartbeat;
    if events_since_last == 0 {
        return Ok(());
    }

    let runtime_context = ctx.instrumentation.capture_accounting();
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        stage_id,
        StageType::Join,
    );

    let payload = ExecutionPayload::JoinReferenceProgress {
        reference_inputs_since_last_report: events_since_last,
    };

    let heartbeat =
        ChainEventFactory::execution_event(writer_id, payload).with_flow_context(flow_context);
    let heartbeat = runtime_context.attach_to(heartbeat);

    crate::supervised_base::publication::append(
        &ctx.data_journal,
        heartbeat,
        AppendOptions::default()
            .with_capture(ctx.instrumentation.journal_capture(None, vec![(0, false)])),
    )
    .await?;
    ctx.events_since_last_heartbeat = 0;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::{StageId, WriterId};

    #[test]
    fn control_rows_reach_observers_as_their_signal_kind() {
        let writer = WriterId::from(StageId::new());
        for (event, kind) in [
            (
                ChainEventFactory::eof_event(writer, true),
                JoinSignalKind::Eof,
            ),
            (
                ChainEventFactory::drain_event(writer),
                JoinSignalKind::Drain,
            ),
            (
                ChainEventFactory::watermark_event(writer, 1, None),
                JoinSignalKind::OtherControl,
            ),
        ] {
            let snapshot = signal_snapshot(Some(JoinSide::Stream), &event).expect("a signal");
            assert_eq!(snapshot.signal(), kind);
            assert_eq!(snapshot.side(), Some(JoinSide::Stream));
        }
    }
}
