// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::DeliveredRecord;
use crate::messaging::PollResult;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::stages::join::fsm::{
    JoinEvent, JoinResources, JoinState, JoinSubscriptionSide, PendingSubscriptionAck,
};

pub(super) async fn dispatch_enriching<
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

    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    if let FlushOutcome::Blocked = common::flush_pending_outputs(sup, ctx).await? {
        return Ok(EventLoopDirective::Continue);
    }

    let Some(subscription) = sup.stream_subscription.as_mut() else {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        return Ok(EventLoopDirective::Continue);
    };

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

            let directive = match &envelope.payload {
                ChainPayload::FlowControl(_) => {
                    match common::read_control(
                        ctx,
                        subscription,
                        JoinSubscriptionSide::Stream,
                        None,
                        Some(crate::stages::observer::JoinSide::Stream),
                        envelope,
                    )
                    .await?
                    {
                        Some(fact) => EventLoopDirective::Transition(fact),
                        None => EventLoopDirective::Continue,
                    }
                }
                payload if payload.consumes_data_credit() => {
                    // Edge identity comes from the reader slot that delivered
                    // the envelope, never from `event.writer_id`, which is
                    // preserved across stages for causal attribution.
                    let source_id = subscription
                        .last_delivered_upstream_stage()
                        .ok_or("No delivered upstream recorded for stream event")?;
                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
                    let event = envelope.authored();
                    let event_id = event.id;
                    let scope = ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        subscription.last_delivered_stage_input_position(),
                        subscription.last_delivered_generation(),
                    );
                    let delivery_snapshot = (ctx.observers.has_join()
                        && !scope.is_deterministic_replay())
                    .then(|| {
                        common::delivery_snapshot(
                            crate::stages::observer::JoinSide::Stream,
                            source_id,
                            subscription.last_delivered_stage_input_position(),
                            &envelope,
                            &ctx.reference_high_water_clock,
                        )
                    })
                    .transpose()?;

                    if let Some(heartbeat) = &ctx.heartbeat {
                        heartbeat.state.record_data_read(source_id, event_id);
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

                    if matches!(event.processing.status, ProcessingStatus::Error { .. }) {
                        tracing::info!(
                            stage_name = %ctx.stage_name,
                            event_id = %event.id,
                            status = ?event.processing.status,
                            "Join received pre-error-marked event; forwarding without handler processing"
                        );
                        if let Some(state) = &heartbeat_state {
                            state.record_last_consumed(event_id);
                        }
                        let mut outputs = vec![event.clone()];
                        common::observe_join_outputs(
                            ctx,
                            scope,
                            Some(&event),
                            delivery_snapshot.as_ref(),
                            None,
                            outputs.as_mut_slice(),
                            Some(&envelope),
                        )
                        .await?;
                        write_stage_outputs_and_ack(
                            subscription,
                            ctx,
                            source_id,
                            outputs.into(),
                            Some(&envelope),
                        )
                        .await?;
                        subscription
                            .maybe_check_contracts_tick(
                                &mut ctx.stream_contract_state[..],
                                &mut ctx.stream_last_contract_check,
                            )
                            .await
                            .unwrap_or(
                                crate::messaging::upstream_subscription::ContractStatus::Healthy,
                            )
                            .into_result()?;
                        return Ok(EventLoopDirective::Continue);
                    }

                    let merged_parent = envelope.clone();

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(state.clone(), Some(source_id), event_id)
                    });
                    let result = ctx
                        .handler
                        .as_ref()
                        .expect("handler available before cleanup")
                        .process_stream(
                            &mut ctx.handler_state,
                            event.clone(),
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
                            let (mut events, framework_eof) = invocation.into_parts();
                            ctx.instrumentation
                                .events_accumulated_total
                                .fetch_add(1, Ordering::Relaxed);
                            common::observe_join_outputs(
                                ctx,
                                scope,
                                Some(&event),
                                delivery_snapshot.as_ref(),
                                None,
                                events.as_mut_slice(),
                                Some(&merged_parent),
                            )
                            .await?;
                            if let Some(kind) = framework_eof {
                                events
                                    .push(ChainEventFactory::eof_event_with_kind(writer_id, kind));
                            }
                            write_stage_outputs_and_ack(
                                subscription,
                                ctx,
                                source_id,
                                events.into(),
                                Some(&merged_parent),
                            )
                            .await?;
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
                                if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
                                    reader.ack_consumed(1);
                                }
                                return Ok(EventLoopDirective::Transition(JoinEvent::Error(
                                    fatal.detail.clone(),
                                )));
                            }
                            let reason = format!("Join handler error during enrichment: {err:?}");
                            let mut error_event =
                                envelope.authored().mark_as_error(reason, err.kind());
                            ctx.instrumentation.record_error(err.kind());
                            common::observe_join_outputs(
                                ctx,
                                scope,
                                Some(&event),
                                delivery_snapshot.as_ref(),
                                None,
                                std::slice::from_mut(&mut error_event),
                                Some(&merged_parent),
                            )
                            .await?;

                            if route_to_error_journal(&error_event) {
                                crate::supervised_base::publication::append(
                                    &ctx.error_journal,
                                    error_event,
                                    AppendOptions::from_record(Some(&merged_parent))?,
                                )
                                .await
                                .map_err(|e| format!("Failed to write join error event: {e}"))?;
                                if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
                                    reader.ack_consumed(1);
                                }
                            } else {
                                write_stage_outputs_and_ack(
                                    subscription,
                                    ctx,
                                    source_id,
                                    VecDeque::from([error_event]),
                                    Some(&merged_parent),
                                )
                                .await?;
                            }
                        }
                    }

                    EventLoopDirective::Continue
                }
                _ => {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event_type(),
                        "Join received unexpected event content type during Enriching"
                    );
                    EventLoopDirective::Continue
                }
            };

            subscription
                .maybe_check_contracts_tick(
                    &mut ctx.stream_contract_state[..],
                    &mut ctx.stream_last_contract_check,
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
                    &mut ctx.stream_contract_state[..],
                    &mut ctx.stream_last_contract_check,
                )
                .await
            {
                match status {
                    crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream) => {
                        tracing::warn!(
                            stage_name = %ctx.stage_name,
                            upstream = ?upstream,
                            "Stream upstream stalled during join enriching"
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
                            "Stream contract violation during join enriching"
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
            "Stream subscription error: {e}"
        )))),
    }
}

async fn write_stage_outputs_and_ack<H: UnifiedJoinHandler>(
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    ctx: &mut JoinResources<H>,
    source_id: obzenflow_core::StageId,
    outputs: VecDeque<ChainEvent>,
    pending_parent: Option<&DeliveredRecord<ChainPayload>>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if outputs.is_empty() {
        if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
            reader.ack_consumed(1);
        }
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

    // FLOWIP-120n: freeze the position-derived execution scope onto each
    // deferred output. The triggering side's delivered generation rides
    // along so a recorded-prefix output flushed after handoff stays
    // reconstruction-scoped (F9).
    let scope = ctx.runtime_execution.dispatch_scope(
        ctx.stage_id,
        subscription.last_delivered_stage_input_position(),
        subscription.last_delivered_generation(),
    );
    let mut outputs: VecDeque<
        crate::stages::common::supervision::backpressure_drain::PendingOutput,
    > = outputs
        .into_iter()
        .map(
            |event| crate::stages::common::supervision::backpressure_drain::PendingOutput {
                causal: crate::supervised_base::publication::capture(),
                event,
                scope,
            },
        )
        .collect();

    while let Some(event) = outputs.pop_front() {
        match drain_one_pending(
            event,
            &flow_context,
            ctx.stage_id,
            ctx.heartbeat.as_ref().map(|h| h.state.clone()),
            &ctx.data_journal,
            pending_parent,
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_stall,
            Some(&ctx.output_contract),
            &mut outputs,
        )
        .await?
        {
            DrainOutcome::Committed { was_data } => {
                if was_data {
                    subscription.track_output_event();
                }
            }
            DrainOutcome::BackedOff => {
                ctx.pending_subscription_ack = Some(PendingSubscriptionAck {
                    side: JoinSubscriptionSide::Stream,
                    upstream: source_id,
                });
                ctx.pending_outputs = outputs;
                ctx.pending_parent = pending_parent.cloned();
                return Ok(());
            }
        }
    }

    if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
        reader.ack_consumed(1);
    }

    Ok(())
}
