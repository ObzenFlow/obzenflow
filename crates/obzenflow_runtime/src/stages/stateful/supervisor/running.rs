// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Accumulating and Emitting state event loops for the stateful supervisor

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation_no_count;
use crate::stages::common::handlers::StatefulHandler;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::control_resolution::{
    resolve_control_event, resolve_forward_control_event, ControlResolution,
};
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::super::fsm::{PendingTransition, StatefulContext, StatefulEvent, StatefulState};
use super::StatefulSupervisor;

pub(super) async fn dispatch_accumulating<
    H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut StatefulSupervisor<H>,
    state: &StatefulState<H>,
    ctx: &mut StatefulContext<H>,
) -> Result<EventLoopDirective<StatefulEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let loop_count = ctx
        .instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    tracing::trace!(
        target: "flowip-080o",
        stage_name = %ctx.stage_name,
        loop_iteration = loop_count + 1,
        "stateful: Accumulating state - starting event loop iteration"
    );

    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        sup.stage_id,
        StageType::Stateful,
    );

    if sup.subscription.is_none() {
        sup.subscription = ctx.subscription.take();
    }

    // Drain any pending outputs from a prior emission before polling upstream again
    // (bounded to one emission; FLOWIP-086k).
    while let Some(pending) = ctx.pending_outputs.pop_front() {
        match drain_one_pending(
            pending,
            &flow_context,
            sup.stage_id,
            &ctx.data_journal,
            &ctx.system_journal,
            ctx.last_consumed_envelope.as_ref(),
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

    if sup.subscription.is_none() {
        tracing::warn!(
            target: "flowip-080o",
            stage_name = %ctx.stage_name,
            loop_iteration = loop_count + 1,
            "stateful: No subscription available, sleeping"
        );
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        return Ok(EventLoopDirective::Continue);
    }

    let poll_result = sup
        .subscription
        .as_mut()
        .expect("subscription checked above")
        .poll_next_with_state(state.variant_name(), Some(&mut ctx.contract_state[..]))
        .await;

    match poll_result {
        PollResult::Event(envelope) => {
            use obzenflow_core::event::JournalEvent;
            tracing::trace!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                loop_iteration = loop_count + 1,
                event_type = %envelope.event.event_type_name(),
                event_id = ?envelope.event.id,
                "stateful: poll_next returned Event"
            );

            // Retain the last consumed upstream envelope (with a merged vector-clock) so that
            // any subsequently emitted aggregate events can be parented and preserve
            // happened-before via vector clocks.
            match ctx.last_consumed_envelope.as_mut() {
                Some(merged) => {
                    CausalOrderingService::update_with_parent(
                        &mut merged.vector_clock,
                        &envelope.vector_clock,
                    );
                    merged.journal_writer_id = envelope.journal_writer_id;
                    merged.timestamp = envelope.timestamp;
                    merged.event = envelope.event.clone();
                }
                None => ctx.last_consumed_envelope = Some(envelope.clone()),
            }
            ctx.instrumentation.record_consumed(&envelope);

            // We have work - increment loops with work
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);

            let directive = match &envelope.event.content {
                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                    let contract_reader_count = ctx.contract_state.len();
                    let upstream_stage = sup
                        .subscription
                        .as_ref()
                        .and_then(|subscription| subscription.last_delivered_upstream_stage());
                    let last_eof_outcome = sup
                        .subscription
                        .as_ref()
                        .and_then(|subscription| subscription.last_eof_outcome().cloned());

                    let mut resolution = resolve_control_event(
                        signal,
                        &envelope,
                        ctx.control_strategy.as_ref(),
                        /* cycle_config */ None,
                        /* cycle_guard */ None,
                        last_eof_outcome.as_ref(),
                        upstream_stage,
                        contract_reader_count,
                        /* drain_is_terminal */ true,
                    );

                    if let ControlResolution::Delay(duration) = resolution {
                        tracing::info!(
                            stage_name = %ctx.stage_name,
                            event_type = envelope.event.event_type(),
                            duration = ?duration,
                            "Delaying control event"
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
                            /* drain_is_terminal */ true,
                        );
                    }

                    match resolution {
                        ControlResolution::Forward => {
                            if envelope.event.is_eof() {
                                if let Some(subscription) = sup.subscription.as_mut() {
                                    drop(
                                        subscription
                                            .check_contracts(&mut ctx.contract_state[..])
                                            .await,
                                    );
                                    let _ = subscription.take_last_eof_outcome();
                                }
                            }
                            sup.forward_control_event(ctx, &envelope).await?;
                            EventLoopDirective::Continue
                        }
                        ControlResolution::ForwardAndDrain => {
                            ctx.buffered_eof = Some(envelope.event.clone());
                            sup.forward_control_event(ctx, &envelope).await?;
                            EventLoopDirective::Transition(StatefulEvent::ReceivedEOF)
                        }
                        ControlResolution::Suppress => EventLoopDirective::Continue,
                        ControlResolution::BufferAtEntryPoint { .. } => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Stateful stage received entry-point buffering resolution without cycle config"
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
                                "Retry requested for control event (not implemented)"
                            );
                            EventLoopDirective::Continue
                        }
                        ControlResolution::Skip => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Skipping control event (dangerous!)"
                            );
                            EventLoopDirective::Continue
                        }
                    }
                }
                obzenflow_core::event::ChainEventContent::Data { .. } => {
                    // Accumulate into state without emitting domain events yet,
                    // but still record per-event processing time + counts.
                    let mut handler = (*ctx.handler).clone();
                    let event = envelope.event.clone();

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    if ctx.emit_interval.is_some() && ctx.last_data_event_time.is_none() {
                        ctx.last_data_event_time = Some(start);
                    }
                    handler.accumulate(&mut ctx.current_state, event);
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
                    ctx.instrumentation
                        .events_accumulated_total
                        .fetch_add(1, Ordering::Relaxed);

                    // Track accumulated events for observability heartbeats.
                    ctx.events_since_last_heartbeat =
                        ctx.events_since_last_heartbeat.saturating_add(1);
                    if let Err(e) = sup.emit_stateful_heartbeat_if_due(ctx, false).await {
                        tracing::warn!(
                            stage_name = %ctx.stage_name,
                            error = ?e,
                            "Failed to emit stateful accumulator heartbeat"
                        );
                    }

                    // Check if we should emit based on updated state
                    if handler.should_emit(&ctx.current_state) {
                        EventLoopDirective::Transition(StatefulEvent::ShouldEmit)
                    } else {
                        // Backpressure ack: upstream input was consumed into state.
                        let upstream_stage = sup
                            .subscription
                            .as_ref()
                            .and_then(|subscription| subscription.last_delivered_upstream_stage());
                        if let Some(upstream) = upstream_stage {
                            if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                reader.ack_consumed(1);
                            }
                        }

                        EventLoopDirective::Continue
                    }
                }
                _ => {
                    // Other content types: forward.
                    sup.forward_control_event(ctx, &envelope).await?;
                    EventLoopDirective::Continue
                }
            };

            if let Some(subscription) = sup.subscription.as_mut() {
                if let Some(status) = subscription
                    .maybe_check_contracts_tick(
                        &mut ctx.contract_state[..],
                        &mut ctx.last_contract_check,
                    )
                    .await
                {
                    match status {
                        crate::messaging::upstream_subscription::ContractStatus::Stalled(
                            upstream,
                        ) => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                upstream = ?upstream,
                                "Upstream stalled detected during active processing"
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
                                "Contract violation detected during active processing"
                            );
                        }
                        _ => {}
                    }
                }
            }

            Ok(directive)
        }
        PollResult::NoEvents => {
            // No events available right now.
            if let Some(subscription) = sup.subscription.as_mut() {
                if let Some(status) = subscription
                    .maybe_check_contracts_tick(
                        &mut ctx.contract_state[..],
                        &mut ctx.last_contract_check,
                    )
                    .await
                {
                    match status {
                        crate::messaging::upstream_subscription::ContractStatus::Stalled(
                            upstream,
                        ) => {
                            tracing::warn!(
                                stage_name = %ctx.stage_name,
                                upstream = ?upstream,
                                "Upstream stalled detected during stateful processing"
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
                                "Contract violation detected during stateful processing"
                            );
                        }
                        _ => {}
                    }
                }
            }

            let mut directive = EventLoopDirective::Continue;

            if let (Some(baseline), Some(interval)) = (ctx.last_data_event_time, ctx.emit_interval)
            {
                if baseline.elapsed() >= interval {
                    let handler = (*ctx.handler).clone();
                    if handler.should_emit(&ctx.current_state) {
                        directive = EventLoopDirective::Transition(StatefulEvent::ShouldEmit);
                    }

                    // Reset baseline regardless to avoid hammering `should_emit` once the interval has elapsed.
                    ctx.last_data_event_time = Some(Instant::now());
                }
            }

            if matches!(directive, EventLoopDirective::Continue) {
                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    loop_iteration = loop_count + 1,
                    "stateful: poll_next returned NoEvents, sleeping"
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }

            Ok(directive)
        }
        PollResult::Error(e) => {
            tracing::error!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                loop_iteration = loop_count + 1,
                error = ?e,
                "stateful: poll_next returned Error"
            );
            Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                format!("Subscription error: {e}"),
            )))
        }
    }
}

pub(super) async fn dispatch_emitting<
    H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut StatefulSupervisor<H>,
    _state: &StatefulState<H>,
    ctx: &mut StatefulContext<H>,
) -> Result<EventLoopDirective<StatefulEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        sup.stage_id,
        StageType::Stateful,
    );

    if sup.subscription.is_none() {
        sup.subscription = ctx.subscription.take();
    }

    // If we have outputs from a previous emit blocked on downstream credits,
    // drain them first and complete the pending transition once empty.
    if !ctx.pending_outputs.is_empty()
        || matches!(
            ctx.pending_transition,
            Some(PendingTransition::EmitComplete)
        )
    {
        while let Some(pending) = ctx.pending_outputs.pop_front() {
            match drain_one_pending(
                pending,
                &flow_context,
                sup.stage_id,
                &ctx.data_journal,
                &ctx.system_journal,
                ctx.last_consumed_envelope.as_ref(),
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

        if ctx.pending_outputs.is_empty()
            && matches!(
                ctx.pending_transition,
                Some(PendingTransition::EmitComplete)
            )
        {
            ctx.pending_transition = None;
            return Ok(EventLoopDirective::Transition(StatefulEvent::EmitComplete));
        }
    }

    tracing::info!(
        target: "flowip-080o",
        stage_name = %ctx.stage_name,
        "stateful: Emitting state - about to emit aggregated events"
    );

    // Emit aggregated events. If downstream credits are exhausted, queue the output
    // events and complete the transition once they are fully written.
    let current_state = &mut ctx.current_state;
    let handler = (*ctx.handler).clone();
    let instrumentation = ctx.instrumentation.clone();

    let emit_result = process_with_instrumentation_no_count(&ctx.instrumentation, || async move {
        handler.emit(&mut *current_state).map_err(
            |err| -> Box<dyn std::error::Error + Send + Sync> {
                instrumentation.record_error(err.kind());
                err.into()
            },
        )
    })
    .await;

    match emit_result {
        Ok(events) if !events.is_empty() => {
            let stage_writer_id = ctx.writer_id.ok_or("No writer ID available")?;

            for mut event in events {
                event.writer_id = stage_writer_id;
                if route_to_error_journal(&event) {
                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        event_id = %event.id,
                        "Writing stateful emitted error event to error journal (FLOWIP-082h)"
                    );

                    // Error events are still data for output accounting.
                    if event.is_data() {
                        ctx.instrumentation.record_output_event(&event);
                        if let Some(subscription) = sup.subscription.as_mut() {
                            subscription.track_output_event();
                        }
                    }

                    ctx.error_journal
                        .append(event, ctx.last_consumed_envelope.as_ref())
                        .await
                        .map_err(|e| format!("Failed to write stateful error event: {e}"))?;
                } else {
                    ctx.pending_outputs.push_back(event);
                }
            }

            ctx.pending_transition = Some(PendingTransition::EmitComplete);

            if ctx.emit_interval.is_some() {
                ctx.last_data_event_time = Some(Instant::now());
            }

            Ok(EventLoopDirective::Continue)
        }
        Ok(_) => {
            if ctx.emit_interval.is_some() {
                ctx.last_data_event_time = Some(Instant::now());
            }
            Ok(EventLoopDirective::Transition(StatefulEvent::EmitComplete))
        }
        Err(e) => {
            tracing::error!(
                stage_name = %ctx.stage_name,
                error = ?e,
                "Failed to emit aggregated event, transitioning to Failed"
            );
            Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                format!("Emit error: {e}"),
            )))
        }
    }
}
