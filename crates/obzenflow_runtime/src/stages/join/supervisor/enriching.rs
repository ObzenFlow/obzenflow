// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::messaging::PollResult;
use crate::stages::common::handlers::JoinHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::control_resolution::{
    resolve_control_event, resolve_forward_control_event, ControlResolution,
};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::EventEnvelope;
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::stages::join::fsm::{JoinContext, JoinEvent, JoinState};

pub(super) async fn dispatch_enriching<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state: &JoinState<H>,
    ctx: &mut JoinContext<H>,
) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    common::ensure_subscriptions(sup, ctx);

    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    match common::flush_pending_outputs(sup, ctx).await? {
        FlushOutcome::Blocked => return Ok(EventLoopDirective::Continue),
        FlushOutcome::DrainCompleteReady => {
            tracing::warn!(
                stage_name = %ctx.stage_name,
                "Join enriching observed DrainComplete-ready pending transition; ignoring"
            );
        }
        FlushOutcome::Drained => {}
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
            ctx.instrumentation.record_consumed(&envelope);
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);

            let directive = match &envelope.event.content {
                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                    if envelope.event.is_eof() {
                        ctx.buffered_eof = Some(envelope.event.clone());
                        ctx.drain_parent = Some(envelope.clone());
                    }

                    let contract_reader_count = ctx.stream_contract_state.len();
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
                            "Join delaying control event during Enriching"
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
                                let _ = subscription.take_last_eof_outcome();
                            }
                            EventLoopDirective::Continue
                        }
                        ControlResolution::ForwardAndDrain => {
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
                                        "Join (Enriching) evaluated EOF outcome for stream side"
                                    );
                                }
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
                                "Retry requested for control event (not implemented) during Enriching"
                            );
                            EventLoopDirective::Continue
                        }
                        ControlResolution::Skip => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Skipping control event (dangerous!) during Enriching"
                            );
                            EventLoopDirective::Continue
                        }
                    }
                }
                obzenflow_core::event::ChainEventContent::Data { .. } => {
                    let source_id = envelope
                        .event
                        .writer_id
                        .as_stage()
                        .copied()
                        .ok_or("Event writer is not a stage")?;
                    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
                    let event = envelope.event.clone();
                    let event_id = event.id;

                    if let Some(heartbeat) = &ctx.heartbeat {
                        heartbeat.state.record_data_read(source_id, event_id);
                    }
                    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());

                    if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
                        tracing::info!(
                            stage_name = %ctx.stage_name,
                            event_id = %event.id,
                            status = ?event.processing_info.status,
                            "Join received pre-error-marked event; forwarding without handler processing"
                        );
                        if let Some(state) = &heartbeat_state {
                            state.record_last_consumed(event_id);
                        }
                        write_stage_outputs_and_ack(
                            subscription,
                            ctx,
                            source_id,
                            VecDeque::from([event]),
                            Some(&envelope),
                        )
                        .await?;
                        drop(
                            subscription
                                .maybe_check_contracts_tick(
                                    &mut ctx.stream_contract_state[..],
                                    &mut ctx.stream_last_contract_check,
                                )
                                .await,
                        );
                        return Ok(EventLoopDirective::Continue);
                    }

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
                    let result = ctx.handler.process_event(
                        &mut ctx.handler_state,
                        event.clone(),
                        source_id,
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
                        Ok(events) => {
                            ctx.instrumentation
                                .events_accumulated_total
                                .fetch_add(1, Ordering::Relaxed);
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
                            let reason = format!("Join handler error during enrichment: {err:?}");
                            let error_event =
                                envelope.event.clone().mark_as_error(reason, err.kind());
                            ctx.instrumentation.record_error(err.kind());

                            if route_to_error_journal(&error_event) {
                                ctx.error_journal
                                    .append(error_event, Some(&merged_parent))
                                    .await
                                    .map_err(|e| {
                                        format!("Failed to write join error event: {e}")
                                    })?;
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
                        event_type = envelope.event.event_type(),
                        "Join received unexpected event content type during Enriching"
                    );
                    EventLoopDirective::Continue
                }
            };

            drop(
                subscription
                    .maybe_check_contracts_tick(
                        &mut ctx.stream_contract_state[..],
                        &mut ctx.stream_last_contract_check,
                    )
                    .await,
            );

            Ok(directive)
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

async fn write_stage_outputs_and_ack<H: JoinHandler>(
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    ctx: &mut JoinContext<H>,
    source_id: obzenflow_core::StageId,
    mut outputs: VecDeque<ChainEvent>,
    pending_parent: Option<&EventEnvelope<ChainEvent>>,
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

    while let Some(event) = outputs.pop_front() {
        match drain_one_pending(
            event,
            &flow_context,
            ctx.stage_id,
            &ctx.data_journal,
            &ctx.system_journal,
            pending_parent,
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_backoff,
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
                ctx.pending_ack_upstream = Some(source_id);
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
