// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::PollResult;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::ChainPayload;
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::stages::join::fsm::{JoinEvent, JoinResources, JoinState, JoinSubscriptionSide};

pub(super) async fn dispatch_hydrating<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state: &JoinState,
    ctx: &mut JoinResources<H>,
) -> Result<EventLoopDirective<JoinEvent>, Box<dyn std::error::Error + Send + Sync>> {
    common::ensure_subscriptions(sup, ctx);
    if let Some(held) = state.held_input() {
        common::renew_held_inputs(ctx, held);
    }

    let loop_count = ctx
        .instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);
    tracing::trace!(
        stage_name = %ctx.stage_name,
        loop_iteration = loop_count + 1,
        "Hydrating - checking reference subscription"
    );

    if let FlushOutcome::Blocked = common::flush_pending_outputs(sup, ctx).await? {
        return Ok(EventLoopDirective::Continue);
    }

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

            let directive = match &envelope.payload {
                ChainPayload::FlowControl(_) => {
                    match common::read_control(
                        ctx,
                        subscription,
                        JoinSubscriptionSide::Reference,
                        None,
                        None,
                        envelope,
                    )
                    .await?
                    {
                        Some(fact) => EventLoopDirective::Transition(fact),
                        None => EventLoopDirective::Continue,
                    }
                }
                payload if payload.consumes_data_credit() => {
                    let event = envelope.authored();
                    let event_id = event.id;
                    let reference_stage_id = ctx.reference_stage_id;
                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
                    let upstream_stage = subscription.last_delivered_upstream_stage();
                    let scope = ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        subscription.last_delivered_stage_input_position(),
                        subscription.last_delivered_generation(),
                    );
                    let delivery_snapshot = (ctx.observers.has_join()
                        && !scope.is_deterministic_replay())
                    .then(|| {
                        common::delivery_snapshot(
                            crate::stages::observer::JoinSide::Reference,
                            reference_stage_id,
                            subscription.last_delivered_stage_input_position(),
                            &envelope,
                            &ctx.reference_high_water_clock,
                        )
                    })
                    .transpose()?;

                    if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream_stage) {
                        heartbeat.state.record_data_read(upstream, event_id);
                    }
                    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());
                    common::observe_join_input(
                        ctx,
                        scope,
                        &event,
                        delivery_snapshot.as_ref(),
                        None,
                        Some(&envelope),
                    )
                    .await?;

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id)
                    });
                    let result = ctx
                        .handler
                        .as_ref()
                        .expect("handler available before cleanup")
                        .process_reference(
                            &mut ctx.handler_state,
                            event.clone(),
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
                        Ok(mut events_produced) => {
                            ctx.instrumentation
                                .events_accumulated_total
                                .fetch_add(1, Ordering::Relaxed);

                            common::observe_join_outputs(
                                ctx,
                                scope,
                                Some(&event),
                                delivery_snapshot.as_ref(),
                                None,
                                events_produced.as_mut_slice(),
                                Some(&envelope),
                            )
                            .await?;

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
                            if let Some(fatal) = err.as_fatal() {
                                common::record_join_stage_fatal(
                                    ctx,
                                    fatal,
                                    Some(&envelope),
                                    subscription.last_delivered_stage_input_position(),
                                )
                                .await?;
                                tracing::error!(
                                    stage_name = %ctx.stage_name,
                                    error = ?err,
                                    "Join handler error during reference hydration"
                                );
                                ctx.instrumentation.record_error(err.kind());
                                return Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                    fatal.detail.clone(),
                                )));
                            }
                            common::dead_letter_reference(
                                ctx,
                                scope,
                                &envelope,
                                delivery_snapshot.as_ref(),
                                &err,
                            )
                            .await?;
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
                        event_type = envelope.event_type(),
                        "Join received unexpected event content type during Hydrating"
                    );
                    EventLoopDirective::Continue
                }
            };

            subscription
                .maybe_check_contracts_tick(
                    &mut ctx.reference_contract_state[..],
                    &mut ctx.reference_last_contract_check,
                )
                .await
                .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
                .into_result()?;

            Ok(directive)
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

                        crate::messaging::upstream_subscription::ContractStatus::Violated {
                            upstream,
                            cause,
                        }
                        .into_result()?;
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
