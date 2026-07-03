// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Accumulating and Emitting state event loops for the stateful supervisor

use crate::effects::EffectInvocationContext;
use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation_no_count;
use crate::stages::common::handlers::{StatefulOutputContext, UnifiedStatefulHandler};
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::catch_up::{
    flip_on_authored_eof, maybe_flip_caught_up, CatchUpDisposition, CatchUpStage,
};
use crate::stages::common::supervision::control_resolution::{
    resolve_control_event_awaiting_pauses, ControlAction,
};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::output_committer::{
    commit_framework_observability_events, is_framework_middleware_observability_event,
    FrameworkObservabilityCommit,
};
use crate::stages::observer::dispatch::{
    run_stateful_after_accumulate_observers, run_stateful_after_emit_observers,
    run_stateful_before_accumulate_observers,
};
use crate::stages::observer::StatefulObserverContext;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::super::fsm::{PendingTransition, StatefulContext, StatefulEvent, StatefulState};
use super::StatefulSupervisor;

pub(super) async fn dispatch_accumulating<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
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
            ctx.heartbeat.as_ref().map(|h| h.state.clone()),
            &ctx.data_journal,
            &ctx.system_journal,
            ctx.last_consumed_envelope.as_ref(),
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
            let stage_input_position = sup
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.last_delivered_stage_input_position());
            let delivered_generation = sup
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.last_delivered_generation());
            if envelope.event.is_data() {
                ctx.last_input_position = stage_input_position;
            }
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
                    // FLOWIP-120n: consume the catch-up watermark before the
                    // generic control resolution; each stage authors its own.
                    if let obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::CatchUpComplete {
                        generation: announced,
                        ..
                    } = signal
                    {
                        let subscription = sup
                            .subscription
                            .as_ref()
                            .expect("subscription checked above");
                        let disposition = maybe_flip_caught_up(
                            *announced,
                            subscription.all_readers_caught_up(*announced),
                            subscription.delivered_data_count(),
                            CatchUpStage {
                                stage_id: ctx.stage_id,
                                stage_name: &ctx.stage_name,
                                flow_name: &ctx.flow_name,
                                flow_id: &flow_id,
                                stage_type: StageType::Stateful,
                                writer_id: ctx.writer_id,
                                data_journal: &ctx.data_journal,
                                instrumentation: &ctx.instrumentation,
                            },
                            /* author_marker */ true,
                            &ctx.runtime_execution,
                            &mut ctx.catch_up_flip,
                        )
                        .await;
                        return Ok(match disposition {
                            CatchUpDisposition::Consumed => EventLoopDirective::Continue,
                            CatchUpDisposition::Failed(message) => {
                                EventLoopDirective::Transition(StatefulEvent::Error(message))
                            }
                        });
                    }

                    // FLOWIP-120n F17: an authored EOF can be the delivery
                    // that completes the caught-up frontier; no watermark
                    // follows, so re-run the flip before normal EOF handling.
                    if envelope.event.is_eof() {
                        let subscription = sup
                            .subscription
                            .as_ref()
                            .expect("subscription checked above");
                        if let Some(message) = flip_on_authored_eof(
                            subscription,
                            CatchUpStage {
                                stage_id: ctx.stage_id,
                                stage_name: &ctx.stage_name,
                                flow_name: &ctx.flow_name,
                                flow_id: &flow_id,
                                stage_type: StageType::Stateful,
                                writer_id: ctx.writer_id,
                                data_journal: &ctx.data_journal,
                                instrumentation: &ctx.instrumentation,
                            },
                            /* author_marker */ true,
                            &ctx.runtime_execution,
                            &mut ctx.catch_up_flip,
                        )
                        .await
                        {
                            return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                message,
                            )));
                        }
                    }

                    let contract_reader_count = ctx.contract_state.len();
                    let upstream_stage = sup
                        .subscription
                        .as_ref()
                        .and_then(|subscription| subscription.last_delivered_upstream_stage());
                    let last_eof_outcome = sup
                        .subscription
                        .as_ref()
                        .and_then(|subscription| subscription.last_eof_outcome().cloned());

                    // FLOWIP-095k: fold the joined terminal kind before resolution.
                    if envelope.event.is_eof() {
                        if let Some(kind) = last_eof_outcome.as_ref().and_then(|o| o.worst_kind) {
                            ctx.terminal_eof_kind = Some(
                                ctx.terminal_eof_kind
                                    .map_or(kind, |current| current.worst(kind)),
                            );
                        }
                    }

                    let resolution = resolve_control_event_awaiting_pauses(
                        signal,
                        &envelope,
                        ctx.control_strategy.as_ref(),
                        &mut ctx.processing_context,
                        /* cycle_config */ None,
                        /* cycle_guard */ None,
                        last_eof_outcome.as_ref(),
                        upstream_stage,
                        contract_reader_count,
                        /* drain_is_terminal */ true,
                        &ctx.stage_name,
                    )
                    .await;

                    match resolution {
                        ControlAction::Forward => {
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
                        ControlAction::ForwardAndDrain => {
                            ctx.buffered_eof = Some(envelope.event.clone());
                            sup.forward_control_event(ctx, &envelope).await?;
                            EventLoopDirective::Transition(StatefulEvent::ReceivedEOF)
                        }
                        ControlAction::Suppress => EventLoopDirective::Continue,
                        ControlAction::BufferAtEntryPoint { .. } => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Stateful stage received entry-point buffering resolution without cycle config"
                            );
                            EventLoopDirective::Transition(StatefulEvent::Error(format!(
                                "Stateful stage reached cycle-entry buffering without cycle config: {}",
                                envelope.event.event_type()
                            )))
                        }
                        ControlAction::Skip => {
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
                    let event_id = event.id;

                    let upstream_stage = sup
                        .subscription
                        .as_ref()
                        .and_then(|subscription| subscription.last_delivered_upstream_stage());
                    if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream_stage) {
                        heartbeat.state.record_data_read(upstream, event_id);
                    }

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
                            heartbeat_state: heartbeat_state.clone(),
                            parent: envelope.clone(),
                            effect_history: ctx.effect_history.clone(),
                            runtime_execution: ctx.runtime_execution.clone(),
                            effect_ports: ctx.effect_ports.clone(),
                            effect_declarations: ctx.effect_declarations.clone(),
                            synthesized_outcomes: Vec::new(),
                            output_contract: ctx.output_contract.clone(),
                            backpressure_writer: ctx.backpressure_writer.clone(),
                            emit_enabled: true,
                            effect_boundary: None,
                            boundary_control_events: std::sync::Arc::new(std::sync::Mutex::new(
                                Vec::new(),
                            )),
                        })
                    });
                    let boundary_control_events = effect_context
                        .as_ref()
                        .map(|context| context.boundary_control_events.clone());

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();
                    if ctx.emit_interval.is_some() && ctx.last_data_event_time.is_none() {
                        // FLOWIP-114o: the emit-interval baseline reads tokio time
                        // (paused-aware); `start` stays std for the latency metric.
                        ctx.last_data_event_time = Some(tokio::time::Instant::now());
                    }

                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id)
                    });

                    // FLOWIP-120c H3: per-event middleware execution scope.
                    let scope = ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        stage_input_position,
                        delivered_generation,
                    );
                    let observer_ctx = StatefulObserverContext {
                        stage_id: ctx.stage_id,
                        stage_name: &ctx.stage_name,
                        flow_context: &flow_context,
                        scope,
                        input: Some(&event),
                        stage_input_position: stage_input_position.map(|position| position.0),
                    };
                    run_stateful_before_accumulate_observers(
                        &ctx.observers,
                        &observer_ctx,
                        &ctx.data_journal,
                        &ctx.instrumentation,
                        Some(&envelope),
                    )
                    .await?;
                    let accumulate_result = handler
                        .accumulate(&mut ctx.current_state, event.clone(), effect_context, scope)
                        .await;
                    run_stateful_after_accumulate_observers(
                        &ctx.observers,
                        &observer_ctx,
                        &ctx.data_journal,
                        &ctx.instrumentation,
                        Some(&envelope),
                    )
                    .await?;
                    if let Some(buffer) = boundary_control_events {
                        commit_framework_observability_events(
                            EffectInvocationContext::drain_boundary_control_event_buffer(&buffer),
                            FrameworkObservabilityCommit {
                                flow_context: &flow_context,
                                data_journal: &ctx.data_journal,
                                system_journal: Some(&ctx.system_journal),
                                instrumentation: Some(&ctx.instrumentation),
                                heartbeat_state: ctx
                                    .heartbeat
                                    .as_ref()
                                    .map(|heartbeat| &heartbeat.state),
                                parent: Some(&envelope),
                                observer_scope: scope,
                            },
                        )
                        .await
                        .map_err(|e| {
                            format!("Failed to commit effect boundary observability events: {e}")
                        })?;
                    }

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
                    ctx.instrumentation
                        .events_accumulated_total
                        .fetch_add(1, Ordering::Relaxed);

                    if let Err(err) = accumulate_result {
                        ctx.instrumentation.record_error(err.kind());
                        let reason = format!("Stateful handler error: {err:?}");
                        let error_event = event.mark_as_error(reason, err.kind());

                        if route_to_error_journal(&error_event) {
                            ctx.error_journal
                                .append(error_event, Some(&envelope))
                                .await
                                .map_err(|e| {
                                    format!("Failed to write stateful accumulate error: {e}")
                                })?;
                        } else {
                            let flow_id = ctx.flow_id.to_string();
                            let flow_ctx = make_flow_context(
                                &ctx.flow_name,
                                &flow_id,
                                &ctx.stage_name,
                                ctx.stage_id,
                                StageType::Stateful,
                            );
                            let enriched_error = error_event
                                .with_flow_context(flow_ctx)
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());
                            ctx.data_journal
                                .append(enriched_error, Some(&envelope))
                                .await
                                .map_err(|e| {
                                    format!("Failed to write stateful accumulate error: {e}")
                                })?;
                        }

                        if let Some(upstream) = upstream_stage {
                            if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                reader.ack_consumed(1);
                            }
                        }

                        return Ok(EventLoopDirective::Continue);
                    }

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

                    // Backpressure ack: upstream input was consumed into state.
                    //
                    // Note: This is intentionally independent of emission. Emission is driven by
                    // accumulator state (and can also be timer-driven), whereas backpressure acks
                    // reflect input consumption on the upstream edge.
                    if let Some(upstream) = upstream_stage {
                        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                            reader.ack_consumed(1);
                        }
                    }

                    // Check if we should emit based on updated state.
                    if handler.should_emit(&mut ctx.current_state) {
                        EventLoopDirective::Transition(StatefulEvent::ShouldEmit)
                    } else {
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
            // FLOWIP-095d: a canonical merge that delivered nothing because an
            // input is quiet is idle-by-rule; name the awaited input.
            crate::stages::common::heartbeat::note_merge_wait(
                ctx.heartbeat.as_ref(),
                sup.subscription.as_ref().and_then(|s| s.merge_wait()),
            );

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
                    if handler.should_emit(&mut ctx.current_state) {
                        directive = EventLoopDirective::Transition(StatefulEvent::ShouldEmit);
                    }

                    // Reset baseline regardless to avoid hammering `should_emit` once the interval has elapsed.
                    ctx.last_data_event_time = Some(tokio::time::Instant::now());
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
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
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
    // Generation None: emit is not a delivery, so the scope follows the
    // stage frontier (FLOWIP-120n).
    let observer_scope =
        ctx.runtime_execution
            .dispatch_scope(ctx.stage_id, ctx.last_input_position, None);

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
                ctx.heartbeat.as_ref().map(|h| h.state.clone()),
                &ctx.data_journal,
                &ctx.system_journal,
                ctx.last_consumed_envelope.as_ref(),
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

    let output_context =
        ctx.writer_id
            .zip(ctx.last_consumed_envelope.as_ref())
            .map(|(writer_id, parent)| StatefulOutputContext {
                writer_id,
                parent,
                recorded_flow_id: ctx
                    .effect_history
                    .as_ref()
                    .map(|history| history.recorded_flow_id().as_str())
                    .unwrap_or(flow_id.as_str()),
                stage_key: &ctx.stage_name,
                input_seq: ctx.last_input_position.unwrap_or(
                    crate::messaging::upstream_subscription::StageInputPosition(0),
                ),
            });

    let emit_result = process_with_instrumentation_no_count(&ctx.instrumentation, || async move {
        handler
            .emit_with_context(&mut *current_state, output_context)
            .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                instrumentation.record_error(err.kind());
                err.into()
            })
    })
    .await;

    match emit_result {
        Ok(mut events) if !events.is_empty() => {
            let stage_writer_id = ctx.writer_id.ok_or("No writer ID available")?;
            let observer_ctx = StatefulObserverContext {
                stage_id: ctx.stage_id,
                stage_name: &ctx.stage_name,
                flow_context: &flow_context,
                scope: observer_scope,
                input: ctx
                    .last_consumed_envelope
                    .as_ref()
                    .map(|envelope| &envelope.event),
                stage_input_position: ctx.last_input_position.map(|position| position.0),
            };
            run_stateful_after_emit_observers(
                &ctx.observers,
                &observer_ctx,
                events.as_mut_slice(),
                &ctx.data_journal,
                &ctx.instrumentation,
                ctx.last_consumed_envelope.as_ref(),
            )
            .await?;

            for mut event in events {
                event.writer_id = stage_writer_id;
                if is_framework_middleware_observability_event(&event) {
                    commit_framework_observability_events(
                        vec![event],
                        FrameworkObservabilityCommit {
                            flow_context: &flow_context,
                            data_journal: &ctx.data_journal,
                            system_journal: Some(&ctx.system_journal),
                            instrumentation: Some(&ctx.instrumentation),
                            heartbeat_state: ctx
                                .heartbeat
                                .as_ref()
                                .map(|heartbeat| &heartbeat.state),
                            parent: ctx.last_consumed_envelope.as_ref(),
                            observer_scope,
                        },
                    )
                    .await
                    .map_err(|e| format!("Failed to commit framework observability event: {e}"))?;
                    continue;
                }

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
                    let scope = observer_scope;
                    ctx.pending_outputs.push_back(
                        crate::stages::common::supervision::backpressure_drain::PendingOutput {
                            event,
                            scope,
                        },
                    );
                }
            }

            ctx.pending_transition = Some(PendingTransition::EmitComplete);

            if ctx.emit_interval.is_some() {
                ctx.last_data_event_time = Some(tokio::time::Instant::now());
            }

            Ok(EventLoopDirective::Continue)
        }
        Ok(_) => {
            if ctx.emit_interval.is_some() {
                ctx.last_data_event_time = Some(tokio::time::Instant::now());
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
