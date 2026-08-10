// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::PollResult;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::stages::join::config::JoinReferenceMode;
use crate::stages::join::fsm::{
    JoinContext, JoinEvent, JoinState, JoinSubscriptionSide, PendingSubscriptionAck,
    PendingTransition,
};

pub(super) async fn dispatch_draining<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state: &JoinState<H>,
    ctx: &mut JoinContext<H>,
) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }

    if ctx.reference_mode == JoinReferenceMode::Live {
        return dispatch_draining_live(sup, ctx).await;
    }

    common::ensure_subscriptions(sup, ctx);
    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    match common::flush_pending_outputs(sup, ctx).await? {
        FlushOutcome::Blocked => return Ok(EventLoopDirective::Continue),
        FlushOutcome::DrainCompleteReady => {
            ctx.pending_transition = None;
            return Ok(EventLoopDirective::Transition(JoinEvent::DrainComplete));
        }
        FlushOutcome::Drained => {}
    }

    // Drain reference side first.
    if let Some(subscription) = sup.reference_subscription.as_mut() {
        match subscription
            .poll_next_with_state(
                state.variant_name(),
                Some(&mut ctx.reference_contract_state[..]),
            )
            .await
        {
            PollResult::Event(envelope) => {
                let delivered_upstream_stage = subscription
                    .last_delivered_upstream_stage()
                    .expect("delivered event must identify its upstream stage");
                ctx.instrumentation
                    .record_consumed(&envelope, delivered_upstream_stage);
                ctx.instrumentation
                    .event_loops_with_work_total
                    .fetch_add(1, Ordering::Relaxed);

                // Capture reference-side ancestry for FLOWIP-071h (conservative high-water interim).
                common::observe_reference_envelope(ctx, &envelope);

                if !envelope.event.is_control() {
                    let event = envelope.event.clone();
                    let event_id = event.id;
                    let reference_stage_id = ctx.reference_stage_id;
                    if let Some(heartbeat) = &ctx.heartbeat {
                        if event.is_data() {
                            heartbeat
                                .state
                                .record_data_read(reference_stage_id, event_id);
                        }
                    }
                    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());

                    if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
                        if let Some(state) = &heartbeat_state {
                            state.record_last_consumed(event_id);
                        }
                        let scope = ctx.runtime_execution.dispatch_scope(
                            ctx.stage_id,
                            subscription.last_delivered_stage_input_position(),
                            None,
                        );
                        ctx.pending_outputs.push_back(
                            crate::stages::common::supervision::backpressure_drain::PendingOutput {
                                event,
                                scope,
                            },
                        );
                        ctx.pending_subscription_ack = subscription
                            .last_delivered_upstream_stage()
                            .map(|upstream| PendingSubscriptionAck {
                                side: JoinSubscriptionSide::Reference,
                                upstream,
                            });
                        ctx.pending_parent = Some(envelope);
                        return Ok(EventLoopDirective::Continue);
                    }

                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(
                            state.clone(),
                            Some(reference_stage_id),
                            event_id,
                        )
                    });
                    // FLOWIP-120n: per-delivery execution scope, computed at
                    // dispatch from the delivered position. Generation None:
                    // drain follows the stage frontier.
                    let scope = ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        subscription.last_delivered_stage_input_position(),
                        None,
                    );
                    let result = ctx.handler.process_reference(
                        &mut ctx.handler_state,
                        event,
                        reference_stage_id,
                        writer_id,
                        scope,
                    );
                    if let Some(state) = &heartbeat_state {
                        state.record_last_consumed(event_id);
                    }
                    let duration = start.elapsed();
                    ctx.instrumentation
                        .in_flight_count
                        .fetch_sub(1, Ordering::Relaxed);

                    ctx.instrumentation.record_processing_time(duration);
                    if ctx.instrumentation.check_anomaly(duration) {
                        ctx.instrumentation
                            .anomalies_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    ctx.instrumentation
                        .events_processed_total
                        .fetch_add(1, Ordering::Relaxed);

                    match result {
                        Ok(results) => {
                            ctx.instrumentation
                                .events_accumulated_total
                                .fetch_add(1, Ordering::Relaxed);

                            let upstream_stage = subscription.last_delivered_upstream_stage();
                            if results.is_empty() {
                                if let Some(upstream) = upstream_stage {
                                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                        reader.ack_consumed(1);
                                    }
                                }
                            } else {
                                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                                ctx.pending_outputs.extend(results.into_iter().map(|event| {
                                    crate::stages::common::supervision::backpressure_drain::PendingOutput { event, scope }
                                }));
                                ctx.pending_subscription_ack =
                                    upstream_stage.map(|upstream| PendingSubscriptionAck {
                                        side: JoinSubscriptionSide::Reference,
                                        upstream,
                                    });
                                ctx.pending_parent = Some(envelope.clone());
                            }
                        }
                        Err(err) => {
                            if let Some(fatal) = err.as_fatal() {
                                common::record_join_stage_fatal(
                                    ctx,
                                    fatal,
                                    Some(&envelope),
                                    subscription.last_delivered_stage_input_position(),
                                )
                                .await?;
                            }
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?err,
                                "Join handler error during reference draining"
                            );
                            ctx.instrumentation.record_error(err.kind());
                            return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                                "Join handler drain-side error: {err:?}"
                            ))));
                        }
                    }
                } else if !envelope.event.is_eof() {
                    tracing::debug!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Forwarding reference control event during join draining"
                    );
                    common::forward_control_event_and_mirror(ctx, &envelope).await?;
                }

                return Ok(EventLoopDirective::Continue);
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
                return Ok(EventLoopDirective::Continue);
            }
            PollResult::NoEvents => {
                drop(
                    subscription
                        .check_contracts(&mut ctx.reference_contract_state[..])
                        .await,
                );
            }
            PollResult::Error(e) => {
                return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                    "Reference drain error: {e}"
                ))));
            }
        }
    }

    // Drain stream side next.
    if let Some(subscription) = sup.stream_subscription.as_mut() {
        match subscription
            .poll_next_with_state(
                state.variant_name(),
                Some(&mut ctx.stream_contract_state[..]),
            )
            .await
        {
            PollResult::Event(envelope) => {
                let delivered_upstream_stage = subscription
                    .last_delivered_upstream_stage()
                    .expect("delivered event must identify its upstream stage");
                ctx.instrumentation
                    .record_consumed(&envelope, delivered_upstream_stage);
                ctx.instrumentation
                    .event_loops_with_work_total
                    .fetch_add(1, Ordering::Relaxed);

                if !envelope.event.is_control() {
                    let event = envelope.event.clone();
                    let event_id = event.id;
                    // Edge identity comes from the reader slot that delivered
                    // the envelope, never from `event.writer_id`, which is
                    // preserved across stages for causal attribution. The
                    // factory and reference fallbacks cover pre-delivery
                    // states where no upstream has been recorded yet.
                    let source_id = subscription
                        .last_delivered_upstream_stage()
                        .or_else(|| {
                            ctx.stream_subscription_factory
                                .upstream_stage_ids()
                                .first()
                                .copied()
                        })
                        .unwrap_or(ctx.reference_stage_id);

                    if let Some(heartbeat) = &ctx.heartbeat {
                        if event.is_data() {
                            heartbeat.state.record_data_read(source_id, event_id);
                        }
                    }
                    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());

                    if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
                        if let Some(state) = &heartbeat_state {
                            state.record_last_consumed(event_id);
                        }
                        let scope = ctx.runtime_execution.dispatch_scope(
                            ctx.stage_id,
                            subscription.last_delivered_stage_input_position(),
                            None,
                        );
                        ctx.pending_outputs.push_back(
                            crate::stages::common::supervision::backpressure_drain::PendingOutput {
                                event,
                                scope,
                            },
                        );
                        ctx.pending_subscription_ack = subscription
                            .last_delivered_upstream_stage()
                            .map(|upstream| PendingSubscriptionAck {
                                side: JoinSubscriptionSide::Stream,
                                upstream,
                            });
                        ctx.pending_parent = Some(envelope);
                        return Ok(EventLoopDirective::Continue);
                    }

                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
                    let mut merged_parent = envelope.clone();
                    CausalOrderingService::update_with_parent(
                        &mut merged_parent.vector_clock,
                        &ctx.reference_high_water_clock,
                    );

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(state.clone(), Some(source_id), event_id)
                    });
                    // FLOWIP-120n: per-delivery execution scope, computed at
                    // dispatch from the delivered position. Generation None:
                    // drain follows the stage frontier.
                    let scope = ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        subscription.last_delivered_stage_input_position(),
                        None,
                    );
                    let result = ctx.handler.process_stream(
                        &mut ctx.handler_state,
                        event,
                        source_id,
                        writer_id,
                        scope,
                    );
                    if let Some(state) = &heartbeat_state {
                        state.record_last_consumed(event_id);
                    }
                    let duration = start.elapsed();
                    ctx.instrumentation
                        .in_flight_count
                        .fetch_sub(1, Ordering::Relaxed);

                    ctx.instrumentation.record_processing_time(duration);
                    if ctx.instrumentation.check_anomaly(duration) {
                        ctx.instrumentation
                            .anomalies_total
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    ctx.instrumentation
                        .events_processed_total
                        .fetch_add(1, Ordering::Relaxed);

                    match result {
                        Ok(invocation) => {
                            let (events, framework_eof) = invocation.into_parts();
                            ctx.instrumentation
                                .events_accumulated_total
                                .fetch_add(1, Ordering::Relaxed);

                            let upstream_stage = subscription.last_delivered_upstream_stage();
                            if events.is_empty() && framework_eof.is_none() {
                                if let Some(upstream) = upstream_stage {
                                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                        reader.ack_consumed(1);
                                    }
                                }
                            } else {
                                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                                ctx.pending_outputs.extend(events.into_iter().map(|event| {
                                    crate::stages::common::supervision::backpressure_drain::PendingOutput { event, scope }
                                }));
                                if let Some(kind) = framework_eof {
                                    ctx.pending_outputs.push_back(
                                        crate::stages::common::supervision::backpressure_drain::PendingOutput {
                                            event: ChainEventFactory::eof_event_with_kind(writer_id, kind),
                                            scope,
                                        },
                                    );
                                }
                                ctx.pending_subscription_ack =
                                    upstream_stage.map(|upstream| PendingSubscriptionAck {
                                        side: JoinSubscriptionSide::Stream,
                                        upstream,
                                    });
                                ctx.pending_parent = Some(merged_parent);
                            }
                        }
                        Err(err) => {
                            if let Some(fatal) = err.as_fatal() {
                                common::record_join_stage_fatal(
                                    ctx,
                                    fatal,
                                    Some(&envelope),
                                    subscription.last_delivered_stage_input_position(),
                                )
                                .await?;
                                if let Some(upstream) = subscription.last_delivered_upstream_stage()
                                {
                                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                        reader.ack_consumed(1);
                                    }
                                }
                                return Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                    fatal.detail.clone(),
                                )));
                            }
                            let reason = format!("Join handler error during draining: {err:?}");
                            let error_event =
                                envelope.event.clone().mark_as_error(reason, err.kind());
                            ctx.instrumentation.record_error(err.kind());

                            let upstream_stage = subscription.last_delivered_upstream_stage();
                            if route_to_error_journal(&error_event) {
                                ctx.error_journal
                                    .append(error_event, Some(&merged_parent))
                                    .await
                                    .map_err(|e| {
                                        format!("Failed to write join drain error event: {e}")
                                    })?;
                                if let Some(upstream) = upstream_stage {
                                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                        reader.ack_consumed(1);
                                    }
                                }
                            } else {
                                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                                ctx.pending_outputs.push_back(
                                    crate::stages::common::supervision::backpressure_drain::PendingOutput {
                                        event: error_event,
                                        scope,
                                    },
                                );
                                ctx.pending_subscription_ack =
                                    upstream_stage.map(|upstream| PendingSubscriptionAck {
                                        side: JoinSubscriptionSide::Stream,
                                        upstream,
                                    });
                                ctx.pending_parent = Some(merged_parent);
                            }
                        }
                    }
                } else if !envelope.event.is_eof() {
                    tracing::debug!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Forwarding stream control event during join draining"
                    );
                    common::forward_control_event_and_mirror(ctx, &envelope).await?;
                }

                return Ok(EventLoopDirective::Continue);
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
                return Ok(EventLoopDirective::Continue);
            }
            PollResult::NoEvents => {
                drop(
                    subscription
                        .check_contracts(&mut ctx.stream_contract_state[..])
                        .await,
                );
            }
            PollResult::Error(e) => {
                return Ok(EventLoopDirective::Transition(JoinEvent::Error(
                    e.to_string(),
                )));
            }
        }
    }

    // Both sides are drained this iteration. Now call handler.drain() to emit any final state.
    // FLOWIP-095k finalizer gate: Truncated terminates without the final
    // emission; Poison keeps today's behaviour (FLOWIP-075b owns the live
    // question). The transition below stays unconditional.
    match ctx.terminal_eof_kind.unwrap_or(EofKind::Natural) {
        EofKind::Natural | EofKind::Poison => {
            let handler = ctx.handler.clone();
            let empty_state = handler.initial_state();
            let mut final_state = std::mem::replace(&mut ctx.handler_state, empty_state);
            let mut events = Vec::new();
            if let Some(final_stream_eof) = ctx.final_stream_eof.take() {
                let stream_source_id = ctx
                    .stream_subscription_factory
                    .upstream_stage_ids()
                    .into_iter()
                    .next()
                    .ok_or_else(|| {
                        obzenflow_fsm::FsmError::HandlerError(
                            "No stream subscription source available".to_string(),
                        )
                    })?;
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError("No writer ID available".to_string())
                })?;
                match handler.on_stream_eof(
                    &mut final_state,
                    final_stream_eof.event.clone(),
                    stream_source_id,
                    writer_id,
                ) {
                    Ok(eof_events) => events.extend(eof_events),
                    Err(err) => {
                        ctx.handler_state = final_state;
                        ctx.instrumentation.record_error(err.kind());
                        let detail = if let Some(fatal) = err.as_fatal() {
                            common::record_join_stage_fatal(
                                ctx,
                                fatal,
                                Some(&final_stream_eof),
                                None,
                            )
                            .await?;
                            fatal.detail.clone()
                        } else {
                            err.to_string()
                        };
                        return Ok(EventLoopDirective::Transition(JoinEvent::Error(detail)));
                    }
                }
            }
            match handler
                .drain(
                    &final_state,
                    ctx.drain_parent.as_ref().map(|parent| &parent.event),
                )
                .await
            {
                Ok(drain_events) => events.extend(drain_events),
                Err(err) => {
                    ctx.handler_state = final_state;
                    ctx.instrumentation.record_error(err.kind());
                    let detail = if let Some(fatal) = err.as_fatal() {
                        common::record_join_stage_fatal(
                            ctx,
                            fatal,
                            ctx.drain_parent.as_ref(),
                            None,
                        )
                        .await?;
                        fatal.detail.clone()
                    } else {
                        err.to_string()
                    };
                    return Ok(EventLoopDirective::Transition(JoinEvent::Error(detail)));
                }
            }
            ctx.handler_state = final_state;

            let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
            ctx.pending_outputs.extend(events.into_iter().map(|event| {
                crate::stages::common::supervision::backpressure_drain::PendingOutput {
                    event,
                    scope,
                }
            }));
        }
        EofKind::Truncated => {
            tracing::info!(
                stage_name = %ctx.stage_name,
                "terminal EOF is truncated; skipping end-of-input finalization"
            );
        }
    }
    if !ctx.pending_outputs.is_empty() {
        if let Some(mut frontier) = ctx.drain_parent.clone() {
            CausalOrderingService::update_with_parent(
                &mut frontier.vector_clock,
                &ctx.reference_high_water_clock,
            );
            ctx.pending_parent = Some(frontier);
        }
    }
    ctx.pending_transition = Some(PendingTransition::DrainComplete);
    Ok(EventLoopDirective::Continue)
}

async fn dispatch_draining_live<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    common::ensure_subscriptions(sup, ctx);

    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    match common::flush_pending_outputs(sup, ctx).await? {
        FlushOutcome::Blocked => return Ok(EventLoopDirective::Continue),
        FlushOutcome::DrainCompleteReady => {
            ctx.pending_transition = None;
            return Ok(EventLoopDirective::Transition(JoinEvent::DrainComplete));
        }
        FlushOutcome::Drained => {}
    }

    if ctx.pending_transition.is_none() {
        // FLOWIP-095k finalizer gate: on_source_eof and drain are both
        // end-of-input responses the killed original never ran, so Truncated
        // suppresses both (locked 2026-07-02); Poison keeps today's behaviour
        // (FLOWIP-075b owns the live question).
        match ctx.terminal_eof_kind.unwrap_or(EofKind::Natural) {
            EofKind::Natural | EofKind::Poison => {
                let handler = ctx.handler.clone();
                let empty_state = handler.initial_state();
                let mut final_state = std::mem::replace(&mut ctx.handler_state, empty_state);

                // The final stream-side subscription outcome, not an earlier
                // EOF-shaped control record, authorizes the terminal hook.
                if let Some(final_stream_eof) = ctx.final_stream_eof.take() {
                    let stream_source_id = ctx
                        .stream_subscription_factory
                        .upstream_stage_ids()
                        .into_iter()
                        .next()
                        .ok_or_else(|| {
                            obzenflow_fsm::FsmError::HandlerError(
                                "No stream subscription source available".to_string(),
                            )
                        })?;
                    let writer_id = ctx.writer_id.ok_or_else(|| {
                        obzenflow_fsm::FsmError::HandlerError("No writer ID available".to_string())
                    })?;

                    let eof_events = match handler.on_stream_eof(
                        &mut final_state,
                        final_stream_eof.event.clone(),
                        stream_source_id,
                        writer_id,
                    ) {
                        Ok(events) => events,
                        Err(err) => {
                            ctx.handler_state = final_state;
                            ctx.instrumentation.record_error(err.kind());
                            let detail = if let Some(fatal) = err.as_fatal() {
                                common::record_join_stage_fatal(
                                    ctx,
                                    fatal,
                                    Some(&final_stream_eof),
                                    None,
                                )
                                .await?;
                                fatal.detail.clone()
                            } else {
                                err.to_string()
                            };
                            return Ok(EventLoopDirective::Transition(JoinEvent::Error(detail)));
                        }
                    };
                    let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                    ctx.pending_outputs
                        .extend(eof_events.into_iter().map(|event| {
                            crate::stages::common::supervision::backpressure_drain::PendingOutput {
                                event,
                                scope,
                            }
                        }));
                }

                let events = match handler
                    .drain(
                        &final_state,
                        ctx.drain_parent.as_ref().map(|parent| &parent.event),
                    )
                    .await
                {
                    Ok(events) => events,
                    Err(err) => {
                        ctx.handler_state = final_state;
                        ctx.instrumentation.record_error(err.kind());
                        let detail = if let Some(fatal) = err.as_fatal() {
                            common::record_join_stage_fatal(
                                ctx,
                                fatal,
                                ctx.drain_parent.as_ref(),
                                None,
                            )
                            .await?;
                            fatal.detail.clone()
                        } else {
                            err.to_string()
                        };
                        return Ok(EventLoopDirective::Transition(JoinEvent::Error(detail)));
                    }
                };
                ctx.handler_state = final_state;

                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                ctx.pending_outputs.extend(events.into_iter().map(|event| {
                    crate::stages::common::supervision::backpressure_drain::PendingOutput {
                        event,
                        scope,
                    }
                }));
                if !ctx.pending_outputs.is_empty() {
                    if let Some(mut frontier) = ctx.drain_parent.clone() {
                        CausalOrderingService::update_with_parent(
                            &mut frontier.vector_clock,
                            &ctx.reference_high_water_clock,
                        );
                        ctx.pending_parent = Some(frontier);
                    }
                }
            }
            EofKind::Truncated => {
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "terminal EOF is truncated; skipping end-of-input finalization"
                );
            }
        }
        ctx.pending_transition = Some(PendingTransition::DrainComplete);
    }

    Ok(EventLoopDirective::Continue)
}
