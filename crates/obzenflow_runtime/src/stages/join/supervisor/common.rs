// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::upstream_subscription::StageInputPosition;
use crate::messaging::UpstreamSubscription;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::catch_up::{
    consume_catch_up_watermark, CatchUpDisposition, CatchUpStage,
};
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::forward_control_event::forward_control_event;
use crate::stages::join::fsm::{JoinContext, JoinEvent, PendingTransition};
use crate::stages::observer::dispatch::{
    run_join_after_output_observers, run_join_before_input_observers,
};
use crate::stages::observer::{
    JoinCanonicalMergeMetadata, JoinDeliverySnapshot, JoinObserverContext, JoinSide,
    JoinSignalKind, JoinSignalSnapshot,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::payloads::observability_payload::{
    MetricsLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{ChainEventFactory, EventEnvelope};
use obzenflow_core::{ChainEvent, StageId};
use serde_json::json;

use super::JoinSupervisor;

pub(super) fn ensure_subscriptions<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
) {
    if sup.reference_subscription.is_none() {
        sup.reference_subscription = ctx.reference_subscription.take();
    }
    if sup.stream_subscription.is_none() {
        sup.stream_subscription = ctx.stream_subscription.take();
    }
}

/// FLOWIP-120n: consume one side's delivered catch-up watermark. A one-sided
/// arm reacts to either side's watermark; the flip requires BOTH sides caught
/// up, so it happens when the second side crosses. A side whose subscription
/// does not exist yet counts as not caught up (no premature flip).
pub(super) async fn consume_join_catch_up_watermark<H: UnifiedJoinHandler>(
    reference: Option<&UpstreamSubscription<ChainEvent>>,
    stream: Option<&UpstreamSubscription<ChainEvent>>,
    ctx: &JoinContext<H>,
    announced: obzenflow_core::ReaderGeneration,
) -> crate::supervised_base::EventLoopDirective<JoinEvent<H>> {
    let side_caught_up = |side: Option<&UpstreamSubscription<ChainEvent>>| {
        side.is_some_and(|subscription| subscription.all_readers_caught_up(announced))
    };
    let delivered = reference
        .map(|subscription| subscription.delivered_data_count())
        .unwrap_or(0)
        + stream
            .map(|subscription| subscription.delivered_data_count())
            .unwrap_or(0);
    let flow_id = ctx.flow_id.to_string();
    let disposition = consume_catch_up_watermark(
        announced,
        side_caught_up(reference) && side_caught_up(stream),
        delivered,
        CatchUpStage {
            stage_id: ctx.stage_id,
            stage_name: &ctx.stage_name,
            flow_name: &ctx.flow_name,
            flow_id: &flow_id,
            stage_type: StageType::Join,
            writer_id: ctx.writer_id,
            data_journal: &ctx.data_journal,
            instrumentation: &ctx.instrumentation,
        },
        /* author_marker */ true,
        &ctx.runtime_execution,
    )
    .await;
    match disposition {
        CatchUpDisposition::Consumed => crate::supervised_base::EventLoopDirective::Continue,
        CatchUpDisposition::Failed(message) => {
            crate::supervised_base::EventLoopDirective::Transition(JoinEvent::Error(message))
        }
    }
}

pub(super) async fn forward_control_event_and_mirror<H: UnifiedJoinHandler>(
    ctx: &JoinContext<H>,
    envelope: &EventEnvelope<ChainEvent>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let written = forward_control_event(
        envelope,
        ctx.stage_id,
        &ctx.stage_name,
        StageType::Join,
        &ctx.data_journal,
    )
    .await?;
    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
        &written,
        &ctx.system_journal,
    )
    .await;
    Ok(())
}

pub(super) async fn flush_pending_outputs<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
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
            &ctx.system_journal,
            ctx.pending_parent.as_ref(),
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_backoff,
            Some(&ctx.output_contract),
            Some(&ctx.observers),
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

    if let Some(upstream) = ctx.pending_ack_upstream.take() {
        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
            reader.ack_consumed(1);
        }
    }
    ctx.pending_parent = None;

    if ctx.pending_outputs.is_empty()
        && matches!(
            ctx.pending_transition,
            Some(PendingTransition::DrainComplete)
        )
    {
        return Ok(FlushOutcome::DrainCompleteReady);
    }

    Ok(FlushOutcome::Drained)
}

pub(super) async fn observe_join_input<H: UnifiedJoinHandler>(
    ctx: &JoinContext<H>,
    input: &ChainEvent,
    delivery: Option<&JoinDeliverySnapshot>,
    signal: Option<&JoinSignalSnapshot>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Join,
    );
    let observer_ctx = JoinObserverContext {
        stage_id: ctx.stage_id,
        stage_name: &ctx.stage_name,
        flow_context: &flow_context,
        scope: ctx
            .runtime_execution
            .dispatch_scope(ctx.stage_id, None, None),
        input: Some(input),
        delivery,
        signal,
    };
    run_join_before_input_observers(
        &ctx.observers,
        &observer_ctx,
        &ctx.data_journal,
        &ctx.instrumentation,
        parent,
    )
    .await
}

pub(super) async fn observe_join_outputs<H: UnifiedJoinHandler>(
    ctx: &JoinContext<H>,
    input: Option<&ChainEvent>,
    delivery: Option<&JoinDeliverySnapshot>,
    signal: Option<&JoinSignalSnapshot>,
    outputs: &mut [ChainEvent],
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Join,
    );
    let observer_ctx = JoinObserverContext {
        stage_id: ctx.stage_id,
        stage_name: &ctx.stage_name,
        flow_context: &flow_context,
        scope: ctx
            .runtime_execution
            .dispatch_scope(ctx.stage_id, None, None),
        input,
        delivery,
        signal,
    };
    run_join_after_output_observers(
        &ctx.observers,
        &observer_ctx,
        outputs,
        &ctx.data_journal,
        &ctx.instrumentation,
        parent,
    )
    .await
}

pub(super) fn delivery_snapshot(
    side: JoinSide,
    source_stage_id: StageId,
    stage_input_position: Option<StageInputPosition>,
    envelope: &EventEnvelope<ChainEvent>,
    reference_high_water: &obzenflow_core::event::vector_clock::VectorClock,
    canonical_merge: Option<JoinCanonicalMergeMetadata>,
) -> Result<JoinDeliverySnapshot, Box<dyn std::error::Error + Send + Sync>> {
    let position =
        stage_input_position.ok_or("join delivered data input without StageInputPosition")?;
    Ok(JoinDeliverySnapshot {
        side,
        delivered_source_stage_id: source_stage_id,
        delivered_stage_input_position: position.0,
        input_envelope: envelope.clone(),
        reference_high_water: reference_high_water.clone(),
        canonical_merge,
    })
}

pub(super) fn signal_snapshot(
    side: Option<JoinSide>,
    input: &ChainEvent,
) -> Option<JoinSignalSnapshot> {
    let signal = match &input.content {
        obzenflow_core::event::ChainEventContent::FlowControl(FlowControlPayload::Eof {
            ..
        }) => JoinSignalKind::Eof,
        obzenflow_core::event::ChainEventContent::FlowControl(FlowControlPayload::Drain) => {
            JoinSignalKind::Drain
        }
        obzenflow_core::event::ChainEventContent::FlowControl(_) => JoinSignalKind::OtherControl,
        _ => return None,
    };
    Some(JoinSignalSnapshot { side, signal })
}

pub(super) fn observe_reference_envelope<H: UnifiedJoinHandler>(
    ctx: &mut JoinContext<H>,
    envelope: &EventEnvelope<ChainEvent>,
) {
    // Conservative interim for FLOWIP-071h: merge all reference-side ancestry into one
    // high-water clock (component-wise max).
    CausalOrderingService::update_with_parent(
        &mut ctx.reference_high_water_clock,
        &envelope.vector_clock,
    );
}

fn track_output_event_for_pending_source<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &JoinContext<H>,
) {
    match ctx.pending_ack_upstream {
        Some(upstream) if upstream == ctx.reference_stage_id => {
            if let Some(sub) = sup.reference_subscription.as_mut() {
                sub.track_output_event();
            }
        }
        Some(_) => {
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
    /// Pending outputs were drained and no terminal transition is due.
    Drained,
    /// Backpressure blocked draining; return early and retry later.
    Blocked,
    /// Pending outputs are drained and a pending drain completion transition is ready.
    DrainCompleteReady,
}

pub(super) async fn emit_join_heartbeat_if_due<H: UnifiedJoinHandler + Send + Sync + 'static>(
    ctx: &mut JoinContext<H>,
    stage_id: StageId,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let interval = crate::metrics::instrumentation::heartbeat_interval();
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

    let runtime_context = ctx.instrumentation.snapshot_with_control();
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        stage_id,
        StageType::Join,
    );

    let payload = ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
        name: "join_reference_heartbeat".to_string(),
        value: json!({
            "events_since_last_heartbeat": events_since_last,
            "events_processed_total": runtime_context.events_processed_total,
        }),
        tags: None,
    });

    let heartbeat = ChainEventFactory::observability_event(writer_id, payload)
        .with_flow_context(flow_context)
        .with_runtime_context(runtime_context);

    ctx.data_journal.append(heartbeat, None).await?;
    ctx.events_since_last_heartbeat = 0;
    Ok(())
}
