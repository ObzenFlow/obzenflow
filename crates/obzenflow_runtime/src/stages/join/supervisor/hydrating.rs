// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::PollResult;
use crate::stages::common::handlers::JoinHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::control_resolution::{
    resolve_control_event, resolve_forward_control_event, ControlResolution,
};
use crate::supervised_base::EventLoopDirective;
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common;
use super::JoinSupervisor;
use crate::stages::join::fsm::{JoinContext, JoinEvent, JoinState};

pub(super) async fn dispatch_hydrating<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state: &JoinState<H>,
    ctx: &mut JoinContext<H>,
) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    common::ensure_subscriptions(sup, ctx);

    let loop_count = ctx
        .instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);
    tracing::trace!(
        stage_name = %ctx.stage_name,
        loop_iteration = loop_count + 1,
        "Hydrating - checking reference subscription"
    );

    let Some(subscription) = sup.reference_subscription.as_mut() else {
        tracing::warn!(
            stage_name = %ctx.stage_name,
            "No reference subscription available in Hydrating state"
        );
        return Ok(EventLoopDirective::Continue);
    };

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

            // Capture reference-side ancestry for FLOWIP-071h (conservative high-water interim).
            common::observe_reference_envelope(ctx, &envelope);

            let directive = match &envelope.event.content {
                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                    let contract_reader_count = ctx.reference_contract_state.len();
                    let upstream_stage = subscription.last_delivered_upstream_stage();
                    let last_eof_outcome = subscription.last_eof_outcome().cloned();

                    let mut resolution = resolve_control_event(
                        signal,
                        &envelope,
                        ctx.control_strategy.as_ref(),
                        /* cycle_config */ None,
                        /* cycle_guard */ None,
                        last_eof_outcome.as_ref(),
                        upstream_stage,
                        contract_reader_count,
                        /* drain_is_terminal */ false,
                    );

                    if let ControlResolution::Delay(duration) = resolution {
                        tracing::info!(
                            stage_name = %ctx.stage_name,
                            event_type = envelope.event.event_type(),
                            duration = ?duration,
                            "Join delaying control event during Hydrating"
                        );
                        tokio::time::sleep(duration).await;

                        resolution = resolve_forward_control_event(
                            signal,
                            &envelope,
                            /* cycle_config */ None,
                            /* cycle_guard */ None,
                            last_eof_outcome.as_ref(),
                            upstream_stage,
                            contract_reader_count,
                            /* drain_is_terminal */ false,
                        );
                    }

                    match resolution {
                        ControlResolution::Forward => {
                            common::forward_control_event_and_mirror(ctx, &envelope).await?;

                            if envelope.event.is_eof() {
                                if let Some(outcome) = subscription.take_last_eof_outcome() {
                                    tracing::info!(
                                        target: "flowip-080o",
                                        stage_name = %ctx.stage_name,
                                        upstream_stage_id = ?outcome.stage_id,
                                        upstream_stage_name = %outcome.stage_name,
                                        reader_index = outcome.reader_index,
                                        eof_count = outcome.eof_count,
                                        total_readers = outcome.total_readers,
                                        is_final = outcome.is_final,
                                        "Join (Hydrating) evaluated EOF outcome for reference side"
                                    );
                                }
                            }

                            EventLoopDirective::Continue
                        }
                        ControlResolution::ForwardAndDrain => {
                            common::forward_control_event_and_mirror(ctx, &envelope).await?;

                            if envelope.event.is_eof() {
                                let _ = subscription.take_last_eof_outcome();
                            }

                            EventLoopDirective::Transition(JoinEvent::ReceivedEOF)
                        }
                        ControlResolution::Suppress
                        | ControlResolution::BufferAtEntryPoint { .. } => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Join received cycle-only control resolution without cycle config"
                            );
                            EventLoopDirective::Continue
                        }
                        ControlResolution::Delay(_) => {
                            unreachable!("Delay is handled before executing the resolution")
                        }
                        ControlResolution::Retry => {
                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Retry requested for control event (not implemented) during Hydrating"
                            );
                            EventLoopDirective::Continue
                        }
                        ControlResolution::Skip => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Skipping control event (dangerous!) during Hydrating"
                            );
                            EventLoopDirective::Continue
                        }
                    }
                }
                obzenflow_core::event::ChainEventContent::Data { .. } => {
                    let event = envelope.event.clone();
                    let event_id = event.id;
                    let reference_stage_id = ctx.reference_stage_id;
                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
                    let upstream_stage = subscription.last_delivered_upstream_stage();

                    if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream_stage) {
                        heartbeat.state.record_data_read(upstream, event_id);
                    }
                    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id)
                    });
                    let result = ctx.handler.process_event(
                        &mut ctx.handler_state,
                        event,
                        reference_stage_id,
                        writer_id,
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
                        Ok(events_produced) => {
                            ctx.instrumentation
                                .events_accumulated_total
                                .fetch_add(1, Ordering::Relaxed);

                            tracing::debug!(
                                stage_name = %ctx.stage_name,
                                events_count = events_produced.len(),
                                "Handler produced events during hydration (should be 0)"
                            );

                            ctx.events_since_last_heartbeat =
                                ctx.events_since_last_heartbeat.saturating_add(1);
                            if let Err(e) =
                                common::emit_join_heartbeat_if_due(ctx, ctx.stage_id).await
                            {
                                tracing::warn!(
                                    stage_name = %ctx.stage_name,
                                    error = ?e,
                                    "Failed to emit join hydration heartbeat"
                                );
                            }
                        }
                        Err(err) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?err,
                                "Join handler error during reference hydration"
                            );
                            ctx.instrumentation.record_error(err.kind());
                            return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                                "Join handler hydration error: {err:?}"
                            ))));
                        }
                    }

                    if let Some(upstream) = subscription.last_delivered_upstream_stage() {
                        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                            reader.ack_consumed(1);
                        }
                    }

                    EventLoopDirective::Continue
                }
                _ => {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Join received unexpected event content type during Hydrating"
                    );
                    EventLoopDirective::Continue
                }
            };

            drop(
                subscription
                    .maybe_check_contracts_tick(
                        &mut ctx.reference_contract_state[..],
                        &mut ctx.reference_last_contract_check,
                    )
                    .await,
            );

            Ok(directive)
        }
        PollResult::NoEvents => {
            if let Some(status) = subscription
                .maybe_check_contracts_tick(
                    &mut ctx.reference_contract_state[..],
                    &mut ctx.reference_last_contract_check,
                )
                .await
            {
                match status {
                    crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream) => {
                        tracing::warn!(
                            stage_name = %ctx.stage_name,
                            upstream = ?upstream,
                            "Reference upstream stalled during join loading"
                        );
                    }
                    crate::messaging::upstream_subscription::ContractStatus::Violated {
                        upstream,
                        cause,
                    } => {
                        tracing::error!(
                            stage_name = %ctx.stage_name,
                            upstream = ?upstream,
                            cause = ?cause,
                            "Reference contract violation during join loading"
                        );
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            Ok(EventLoopDirective::Continue)
        }
        PollResult::Error(e) => Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
            "Reference subscription error: {e}"
        )))),
    }
}
