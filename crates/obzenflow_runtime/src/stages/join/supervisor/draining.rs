// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::PollResult;
use crate::stages::common::handlers::JoinHandler;
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::supervised_base::EventLoopDirective;
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::stages::join::config::JoinReferenceMode;
use crate::stages::join::fsm::{JoinContext, JoinEvent, JoinState, PendingTransition};

pub(super) async fn dispatch_draining<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state: &JoinState<H>,
    ctx: &mut JoinContext<H>,
) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
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
                ctx.instrumentation.record_consumed(&envelope);
                ctx.instrumentation
                    .event_loops_with_work_total
                    .fetch_add(1, Ordering::Relaxed);

                if !envelope.event.is_control() {
                    let event = envelope.event.clone();
                    let reference_stage_id = ctx.reference_stage_id;
                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let result = ctx.handler.process_event(
                        &mut ctx.handler_state,
                        event,
                        reference_stage_id,
                        writer_id,
                    );
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
                                ctx.pending_outputs.extend(results);
                                ctx.pending_ack_upstream = upstream_stage;
                            }
                        }
                        Err(err) => {
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
                ctx.instrumentation.record_consumed(&envelope);
                ctx.instrumentation
                    .event_loops_with_work_total
                    .fetch_add(1, Ordering::Relaxed);

                if !envelope.event.is_control() {
                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
                    let source_id = envelope
                        .event
                        .writer_id
                        .as_stage()
                        .copied()
                        .or_else(|| {
                            ctx.stream_subscription_factory
                                .upstream_stage_ids()
                                .first()
                                .copied()
                        })
                        .unwrap_or(ctx.reference_stage_id);

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let result = ctx.handler.process_event(
                        &mut ctx.handler_state,
                        envelope.event.clone(),
                        source_id,
                        writer_id,
                    );
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
                        Ok(events) => {
                            ctx.instrumentation
                                .events_accumulated_total
                                .fetch_add(1, Ordering::Relaxed);

                            let upstream_stage = subscription.last_delivered_upstream_stage();
                            if events.is_empty() {
                                if let Some(upstream) = upstream_stage {
                                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                        reader.ack_consumed(1);
                                    }
                                }
                            } else {
                                ctx.pending_outputs.extend(events);
                                ctx.pending_ack_upstream = upstream_stage;
                            }
                        }
                        Err(err) => {
                            let reason = format!("Join handler error during draining: {err:?}");
                            let error_event =
                                envelope.event.clone().mark_as_error(reason, err.kind());
                            ctx.instrumentation.record_error(err.kind());

                            let upstream_stage = subscription.last_delivered_upstream_stage();
                            if route_to_error_journal(&error_event) {
                                ctx.error_journal
                                    .append(error_event, None)
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
                                ctx.pending_outputs.push_back(error_event);
                                ctx.pending_ack_upstream = upstream_stage;
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
    let handler = ctx.handler.clone();
    let empty_state = handler.initial_state();
    let final_state = std::mem::replace(&mut ctx.handler_state, empty_state);
    let events = handler
        .drain(&final_state)
        .await
        .map_err(|err| obzenflow_fsm::FsmError::HandlerError(err.to_string()))?;
    ctx.handler_state = final_state;

    ctx.pending_outputs.extend(events);
    ctx.pending_transition = Some(PendingTransition::DrainComplete);
    Ok(EventLoopDirective::Continue)
}

async fn dispatch_draining_live<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
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
        let handler = ctx.handler.clone();
        let empty_state = handler.initial_state();
        let mut final_state = std::mem::replace(&mut ctx.handler_state, empty_state);

        // Live-mode semantics: stream EOF is authoritative and drives completion.
        if let Some(stream_source_id) = ctx
            .buffered_eof
            .as_ref()
            .and_then(|eof| eof.writer_id.as_stage().copied())
        {
            let writer_id = ctx.writer_id.ok_or_else(|| {
                obzenflow_fsm::FsmError::HandlerError("No writer ID available".to_string())
            })?;

            let eof_events = handler
                .on_source_eof(&mut final_state, stream_source_id, writer_id)
                .map_err(|err| obzenflow_fsm::FsmError::HandlerError(err.to_string()))?;
            ctx.pending_outputs.extend(eof_events);
        }

        let events = handler
            .drain(&final_state)
            .await
            .map_err(|err| obzenflow_fsm::FsmError::HandlerError(err.to_string()))?;
        ctx.handler_state = final_state;

        ctx.pending_outputs.extend(events);
        ctx.pending_transition = Some(PendingTransition::DrainComplete);
    }

    Ok(EventLoopDirective::Continue)
}
