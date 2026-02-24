// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Draining state event loop for the transform supervisor (FLOWIP-051m).

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;

use super::TransformSupervisor;
use crate::stages::transform::fsm::{TransformContext, TransformEvent, TransformState};

/// Dispatch a single event-loop iteration for the Draining state.
pub(super) async fn dispatch_draining<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    state: &TransformState<H>,
    ctx: &mut TransformContext<H>,
) -> Result<EventLoopDirective<TransformEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        sup.stage_id,
        StageType::Transform,
    );

    if sup.subscription.is_none() {
        sup.subscription = ctx.subscription.take();
    }

    dispatch_draining_inner(sup, state, ctx, &flow_context).await
}

async fn dispatch_draining_inner<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    state: &TransformState<H>,
    ctx: &mut TransformContext<H>,
    flow_context: &FlowContext,
) -> Result<EventLoopDirective<TransformEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if sup.subscription.is_some() {
        while let Some(pending) = ctx.pending_outputs.pop_front() {
            match drain_one_pending(
                pending,
                flow_context,
                sup.stage_id,
                &ctx.data_journal,
                &ctx.system_journal,
                ctx.pending_parent.as_ref(),
                &ctx.instrumentation,
                &ctx.backpressure_writer,
                &mut ctx.backpressure_pulse,
                &mut ctx.backpressure_backoff,
                &mut ctx.pending_outputs,
            )
            .await?
            {
                DrainOutcome::Committed { was_data } => {
                    if was_data {
                        if let Some(subscription) = sup.subscription.as_mut() {
                            subscription.track_output_event();
                        }
                    }
                }
                DrainOutcome::BackedOff => return Ok(EventLoopDirective::Continue),
            }
        }

        if let Some(upstream) = ctx.pending_ack_upstream.take() {
            if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                reader.ack_consumed(1);
            }
        }
        ctx.pending_parent = None;
    }

    if sup.subscription.is_none() {
        return Ok(EventLoopDirective::Transition(
            TransformEvent::DrainComplete,
        ));
    };

    let poll_result = {
        let subscription = sup
            .subscription
            .as_mut()
            .expect("subscription presence checked above");
        subscription
            .poll_next_with_state(state.variant_name(), Some(&mut ctx.contract_state[..]))
            .await
    };

    match poll_result {
        PollResult::Event(mut envelope) => {
            tracing::trace!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                event_type = %envelope.event.event_type(),
                is_eof = envelope.event.is_eof(),
                "transform: draining received event from subscription"
            );

            ctx.instrumentation.record_consumed(&envelope);

            let upstream_stage = sup
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.last_delivered_upstream_stage());
            let suppress_event = sup
                .check_cycle_guard_data_event(
                    ctx,
                    &mut envelope,
                    upstream_stage,
                    "Failed to write cycle guard error event during drain",
                )
                .await?;

            if suppress_event {
                return Ok(EventLoopDirective::Continue);
            }

            if envelope.event.is_control() {
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    event_type = envelope.event.event_type(),
                    "Forwarding control event during draining"
                );

                // Do not forward EOF again during draining; it is forwarded after drain completes.
                if !envelope.event.is_eof() {
                    sup.forward_control_event_guarded(&envelope).await?;
                }

                return Ok(EventLoopDirective::Continue);
            }

            // Process data events (or pass through error-marked events).
            let envelope_clone = envelope.clone();
            let handler = &ctx.handler;

            let transformed_events =
                process_with_instrumentation(&ctx.instrumentation, || async move {
                    let event = envelope_clone.event.clone();

                    if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
                        return Ok(vec![event]);
                    }

                    match handler.process(event).await {
                        Ok(outputs) => Ok(outputs),
                        Err(err) => {
                            let reason = format!("Transform handler error during drain: {err:?}");
                            let error_event = envelope_clone
                                .event
                                .clone()
                                .mark_as_error(reason, err.kind());
                            Ok(vec![error_event])
                        }
                    }
                })
                .await?;

            // Error-journal events are written immediately; stage-journal outputs are gated by backpressure.
            let mut stage_outputs = std::collections::VecDeque::<ChainEvent>::new();
            for event in transformed_events {
                if let ProcessingStatus::Error { kind, .. } = &event.processing_info.status {
                    let k = kind.clone().unwrap_or(ErrorKind::Unknown);
                    ctx.instrumentation.record_error(k);
                }

                if route_to_error_journal(&event) {
                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        event_id = %event.id,
                        "Writing error event to error journal during drain (FLOWIP-082e)"
                    );
                    ctx.error_journal
                        .append(event, Some(&envelope))
                        .await
                        .map_err(|e| format!("Failed to write error event during drain: {e}"))?;
                } else {
                    stage_outputs.push_back(event);
                }
            }

            while let Some(event) = stage_outputs.pop_front() {
                match drain_one_pending(
                    event,
                    flow_context,
                    sup.stage_id,
                    &ctx.data_journal,
                    &ctx.system_journal,
                    Some(&envelope),
                    &ctx.instrumentation,
                    &ctx.backpressure_writer,
                    &mut ctx.backpressure_pulse,
                    &mut ctx.backpressure_backoff,
                    &mut stage_outputs,
                )
                .await?
                {
                    DrainOutcome::Committed { was_data } => {
                        if was_data {
                            if let Some(subscription) = sup.subscription.as_mut() {
                                subscription.track_output_event();
                            }
                        }
                    }
                    DrainOutcome::BackedOff => {
                        ctx.pending_parent = Some(envelope.clone());
                        ctx.pending_ack_upstream = upstream_stage;
                        ctx.pending_outputs = stage_outputs;
                        break;
                    }
                }
            }

            if ctx.pending_outputs.is_empty() {
                if let Some(upstream) = upstream_stage {
                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                        reader.ack_consumed(1);
                    }
                }
            }

            Ok(EventLoopDirective::Continue)
        }
        PollResult::NoEvents => {
            if let Some(subscription) = sup.subscription.as_mut() {
                let _ = subscription
                    .check_contracts(&mut ctx.contract_state[..])
                    .await;
            }

            tracing::info!(stage_name = %ctx.stage_name, "Transform queue drained");
            Ok(EventLoopDirective::Transition(
                TransformEvent::DrainComplete,
            ))
        }
        PollResult::Error(e) => {
            tracing::error!(
                stage_name = %ctx.stage_name,
                error = ?e,
                "Error during draining"
            );
            Ok(EventLoopDirective::Transition(TransformEvent::Error(
                format!("Drain error: {e}"),
            )))
        }
    }
}
