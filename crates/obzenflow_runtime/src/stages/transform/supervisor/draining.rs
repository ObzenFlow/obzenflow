// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Draining state event loop for the transform supervisor

use crate::effects::EffectInvocationContext;
use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::output_committer::{
    commit_framework_observability_events, is_framework_middleware_observability_event,
    FrameworkObservabilityCommit,
};
use crate::stages::observer::dispatch::{
    run_after_handler_observers, run_before_handler_observers,
};
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;

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
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }

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
                ctx.heartbeat.as_ref().map(|h| h.state.clone()),
                &ctx.data_journal,
                &ctx.system_journal,
                ctx.pending_parent.as_ref(),
                &ctx.instrumentation,
                &ctx.backpressure_writer,
                &mut ctx.backpressure_pulse,
                &mut ctx.backpressure_stall,
                Some(&ctx.output_contract),
                Some(&ctx.observers),
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
            let stage_input_position = sup
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.last_delivered_stage_input_position());

            if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream_stage) {
                if envelope.event.is_data() {
                    heartbeat
                        .state
                        .record_data_read(upstream, envelope.event.id);
                }
            }

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
            let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());
            let effect_context = stage_input_position.and_then(|input_seq| {
                ctx.writer_id.map(|writer_id| EffectInvocationContext {
                    flow_id: ctx.flow_id,
                    stage_id: ctx.stage_id,
                    stage_key: ctx.stage_name.clone(),
                    writer_id,
                    input_seq,
                    lineage: ctx.lineage_policy,
                    stage_logic_version: handler.stage_logic_version().to_string(),
                    data_journal: ctx.data_journal.clone(),
                    flow_context: Some(flow_context.clone()),
                    observers: Some(ctx.observers.clone()),
                    system_journal: Some(ctx.system_journal.clone()),
                    instrumentation: Some(ctx.instrumentation.clone()),
                    heartbeat_state: ctx.heartbeat.as_ref().map(|h| h.state.clone()),
                    parent: envelope_clone.clone(),
                    effect_history: ctx.effect_history.clone(),
                    runtime_execution: ctx.runtime_execution.clone(),
                    effect_ports: ctx.effect_ports.clone(),
                    effect_declarations: ctx.effect_declarations.clone(),
                    synthesized_outcomes: ctx.synthesized_outcomes.clone(),
                    output_contract: ctx.output_contract.clone(),
                    backpressure_writer: ctx.backpressure_writer.clone(),
                    emit_enabled: true,
                    effect_boundary: None,
                    boundary_control_events: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
                })
            });
            let boundary_control_events = effect_context
                .as_ref()
                .map(|context| context.boundary_control_events.clone());

            // FLOWIP-120c H3: per-event scope, same as the running path.
            // Generation None: drain follows the stage frontier (FLOWIP-120n).
            let scope =
                ctx.runtime_execution
                    .dispatch_scope(ctx.stage_id, stage_input_position, None);
            run_before_handler_observers(
                &ctx.observers,
                ctx.stage_id,
                &ctx.stage_name,
                flow_context,
                scope,
                &envelope.event,
                stage_input_position.map(|position| position.0),
                &ctx.data_journal,
                &ctx.instrumentation,
                &envelope,
            )
            .await?;
            let mut transformed_events =
                process_with_instrumentation(&ctx.instrumentation, || async move {
                    let event = envelope_clone.event.clone();
                    let event_id = event.id;

                    if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
                        if let Some(state) = &heartbeat_state {
                            state.record_last_consumed(event_id);
                        }
                        return Ok(vec![event]);
                    }

                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id)
                    });

                    match handler.process(event, effect_context, scope).await {
                        Ok(outputs) => {
                            if let Some(state) = &heartbeat_state {
                                state.record_last_consumed(event_id);
                            }
                            Ok(outputs)
                        }
                        Err(err) => {
                            let reason = format!("Transform handler error during drain: {err:?}");
                            let error_event = envelope_clone
                                .event
                                .clone()
                                .mark_as_error(reason, err.kind());
                            if let Some(state) = &heartbeat_state {
                                state.record_last_consumed(event_id);
                            }
                            Ok(vec![error_event])
                        }
                    }
                })
                .await?;
            run_after_handler_observers(
                &ctx.observers,
                ctx.stage_id,
                &ctx.stage_name,
                flow_context,
                scope,
                &envelope.event,
                stage_input_position.map(|position| position.0),
                transformed_events.as_mut_slice(),
                &ctx.data_journal,
                &ctx.instrumentation,
                &envelope,
            )
            .await?;
            if let Some(buffer) = boundary_control_events {
                commit_framework_observability_events(
                    EffectInvocationContext::drain_boundary_control_event_buffer(&buffer),
                    FrameworkObservabilityCommit {
                        flow_context,
                        data_journal: &ctx.data_journal,
                        system_journal: Some(&ctx.system_journal),
                        instrumentation: Some(&ctx.instrumentation),
                        heartbeat_state: ctx.heartbeat.as_ref().map(|heartbeat| &heartbeat.state),
                        backpressure_writer: &ctx.backpressure_writer,
                        parent: Some(&envelope),
                        observer_scope: scope,
                    },
                )
                .await
                .map_err(|e| {
                    format!("Failed to commit effect boundary observability events: {e}")
                })?;
            }

            // Error-journal events are written immediately; stage-journal outputs are gated by backpressure.
            let mut stage_outputs = std::collections::VecDeque::<
                crate::stages::common::supervision::backpressure_drain::PendingOutput,
            >::new();
            for event in transformed_events {
                if is_framework_middleware_observability_event(&event) {
                    commit_framework_observability_events(
                        vec![event],
                        FrameworkObservabilityCommit {
                            flow_context,
                            data_journal: &ctx.data_journal,
                            system_journal: Some(&ctx.system_journal),
                            instrumentation: Some(&ctx.instrumentation),
                            heartbeat_state: ctx
                                .heartbeat
                                .as_ref()
                                .map(|heartbeat| &heartbeat.state),
                            backpressure_writer: &ctx.backpressure_writer,
                            parent: Some(&envelope),
                            observer_scope: scope,
                        },
                    )
                    .await
                    .map_err(|e| format!("Failed to commit framework observability event: {e}"))?;
                    continue;
                }

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
                    stage_outputs.push_back(
                        crate::stages::common::supervision::backpressure_drain::PendingOutput {
                            event,
                            scope,
                        },
                    );
                }
            }

            while let Some(event) = stage_outputs.pop_front() {
                match drain_one_pending(
                    event,
                    flow_context,
                    sup.stage_id,
                    ctx.heartbeat.as_ref().map(|h| h.state.clone()),
                    &ctx.data_journal,
                    &ctx.system_journal,
                    Some(&envelope),
                    &ctx.instrumentation,
                    &ctx.backpressure_writer,
                    &mut ctx.backpressure_pulse,
                    &mut ctx.backpressure_stall,
                    Some(&ctx.output_contract),
                    Some(&ctx.observers),
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
        PollResult::CursorAdvanced {
            upstream,
            completed_data_rows,
        } => {
            crate::backpressure::complete_filtered_data_rows(
                &ctx.backpressure_readers,
                upstream,
                completed_data_rows,
            );
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);
            Ok(EventLoopDirective::Continue)
        }
        PollResult::NoEvents => {
            if let Some(subscription) = sup.subscription.as_mut() {
                drop(
                    subscription
                        .check_contracts(&mut ctx.contract_state[..])
                        .await,
                );
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
