// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Running state event loop for the transform supervisor

use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::control_resolution::{
    is_terminal_eof, resolve_control_event, resolve_forward_control_event, ControlResolution,
};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::TransformSupervisor;
use crate::stages::transform::fsm::{TransformContext, TransformEvent, TransformState};

/// Dispatch a single event-loop iteration for the Running state.
///
/// This is a free function taking `sup: &mut TransformSupervisor<H>` rather
/// than a trait method, keeping the decomposition mechanical and avoiding
/// trait complexity.
pub(super) async fn dispatch_running<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    state: &TransformState<H>,
    ctx: &mut TransformContext<H>,
) -> Result<EventLoopDirective<TransformEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let loop_count = ctx
        .instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    tracing::trace!(
        target: "flowip-080o",
        stage_name = %ctx.stage_name,
        loop_iteration = loop_count + 1,
        "transform: Running state - starting event loop iteration"
    );

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

    dispatch_running_inner(sup, state, ctx, loop_count, &flow_context).await
}

async fn dispatch_running_inner<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut TransformSupervisor<H>,
    state: &TransformState<H>,
    ctx: &mut TransformContext<H>,
    loop_count: u64,
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
            } else {
                tracing::warn!(
                    stage_name = %ctx.stage_name,
                    upstream = ?upstream,
                    "Transform backpressure ack skipped: missing reader handle"
                );
            }
        }
        ctx.pending_parent = None;
    }

    if let Some(directive) = sup.maybe_release_buffered_terminal(ctx).await? {
        return Ok(directive);
    }

    if sup.subscription.is_none() {
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Ok(EventLoopDirective::Continue);
    };

    tracing::trace!(
        target: "flowip-080o",
        stage_name = %ctx.stage_name,
        loop_iteration = loop_count + 1,
        "transform: about to call subscription.poll_next()"
    );

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
            use obzenflow_core::event::JournalEvent;

            tracing::trace!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                loop_iteration = loop_count + 1,
                event_type = %envelope.event.event_type_name(),
                "transform: poll_next returned Event"
            );

            ctx.instrumentation.record_consumed(&envelope);
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);

            let upstream_stage = sup
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.last_delivered_upstream_stage());
            let last_eof_outcome = sup
                .subscription
                .as_ref()
                .and_then(|subscription| subscription.last_eof_outcome().cloned());

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
                    "Failed to write cycle guard error event",
                )
                .await?;

            if suppress_event {
                let directive = EventLoopDirective::Continue;

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

                return Ok(directive);
            }

            let directive = match &envelope.event.content {
                obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
                    let cycle_config = ctx.cycle_guard_config.as_ref();
                    let is_cycle_entry_point = cycle_config.is_some_and(|cfg| cfg.is_entry_point);
                    let contract_reader_count = ctx.contract_state.len();

                    let mut resolution = resolve_control_event(
                        signal,
                        &envelope,
                        ctx.control_strategy.as_ref(),
                        cycle_config,
                        sup.cycle_guard.as_mut(),
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
                            cycle_config,
                            sup.cycle_guard.as_mut(),
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
                                    if is_cycle_entry_point {
                                        if is_terminal_eof(&envelope, upstream_stage) {
                                            drop(
                                                subscription
                                                    .check_contracts(&mut ctx.contract_state[..])
                                                    .await,
                                            );
                                        }
                                    } else {
                                        drop(
                                            subscription
                                                .check_contracts(&mut ctx.contract_state[..])
                                                .await,
                                        );
                                        let _ = subscription.take_last_eof_outcome();
                                    }
                                }
                            }

                            sup.forward_control_event_guarded(&envelope).await?;
                            EventLoopDirective::Continue
                        }
                        ControlResolution::ForwardAndDrain => {
                            ctx.buffered_eof = Some(envelope.event.clone());

                            if envelope.event.is_eof() {
                                if let Some(subscription) = sup.subscription.as_mut() {
                                    drop(
                                        subscription
                                            .check_contracts(&mut ctx.contract_state[..])
                                            .await,
                                    );
                                    if !is_cycle_entry_point {
                                        let _ = subscription.take_last_eof_outcome();
                                    }
                                }
                            }

                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                event_type = envelope.event.event_type(),
                                "Transform stage transitioning to draining"
                            );
                            sup.forward_control_event_guarded(&envelope).await?;
                            EventLoopDirective::Transition(TransformEvent::ReceivedEOF)
                        }
                        ControlResolution::BufferAtEntryPoint { is_drain } => {
                            ctx.buffered_terminal_envelope
                                .get_or_insert_with(|| envelope.clone());

                            if is_drain {
                                ctx.drain_received = true;
                                if ctx.buffered_eof.is_none() {
                                    ctx.buffered_eof = Some(envelope.event.clone());
                                }
                                tracing::info!(
                                    stage_name = %ctx.stage_name,
                                    "Transform entry point buffered drain signal"
                                );
                            } else {
                                ctx.buffered_eof = Some(envelope.event.clone());
                                if let Some(upstream) = upstream_stage {
                                    ctx.external_eofs_received.insert(upstream);
                                }
                                tracing::info!(
                                    stage_name = %ctx.stage_name,
                                    upstream = ?upstream_stage,
                                    "Transform entry point buffered external EOF"
                                );

                                if let Some(subscription) = sup.subscription.as_mut() {
                                    drop(
                                        subscription
                                            .check_contracts(&mut ctx.contract_state[..])
                                            .await,
                                    );
                                }
                            }

                            EventLoopDirective::Continue
                        }
                        ControlResolution::Suppress => {
                            if envelope.event.is_eof()
                                && is_cycle_entry_point
                                && is_terminal_eof(&envelope, upstream_stage)
                            {
                                if let Some(subscription) = sup.subscription.as_mut() {
                                    drop(
                                        subscription
                                            .check_contracts(&mut ctx.contract_state[..])
                                            .await,
                                    );
                                }
                            }

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
                    let envelope_clone = envelope.clone();
                    let handler = &ctx.handler;
                    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());

                    let result =
                        process_with_instrumentation(&ctx.instrumentation, || async move {
                            let event = envelope_clone.event.clone();
                            let event_id = event.id;

                            if matches!(
                                event.processing_info.status,
                                ProcessingStatus::Error { .. }
                            ) {
                                tracing::info!(
                                    "Transform supervisor received pre-error-marked event {}: {:?}",
                                    event.id,
                                    event.processing_info.status
                                );
                                if let Some(state) = &heartbeat_state {
                                    state.record_last_consumed(event_id);
                                }
                                return Ok(vec![event]);
                            }

                            let _processing = heartbeat_state.as_ref().map(|state| {
                                HeartbeatProcessingGuard::new(
                                    state.clone(),
                                    upstream_stage,
                                    event_id,
                                )
                            });

                            match handler.process(event).await {
                                Ok(outputs) => {
                                    if let Some(state) = &heartbeat_state {
                                        state.record_last_consumed(event_id);
                                    }
                                    Ok(outputs)
                                }
                                Err(err) => {
                                    let reason = format!("Transform handler error: {err:?}");
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
                        .await;

                    match result {
                        Ok(transformed_events) => {
                            // Error-journal events are written immediately; stage-journal
                            // outputs are gated by backpressure.
                            let mut stage_outputs = std::collections::VecDeque::<ChainEvent>::new();

                            for event in transformed_events {
                                if let ProcessingStatus::Error { kind, .. } =
                                    &event.processing_info.status
                                {
                                    let k = kind.clone().unwrap_or(ErrorKind::Unknown);
                                    ctx.instrumentation.record_error(k);
                                }

                                if route_to_error_journal(&event) {
                                    tracing::info!(
                                        stage_name = %ctx.stage_name,
                                        event_id = %event.id,
                                        "Writing error event to error journal (FLOWIP-082e)"
                                    );
                                    ctx.error_journal
                                        .append(event, Some(&envelope))
                                        .await
                                        .map_err(|e| format!("Failed to write error event: {e}"))?;
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
                                        // Queue remaining outputs and retry later (bounded to one input).
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
                                    } else {
                                        tracing::warn!(
                                            stage_name = %ctx.stage_name,
                                            upstream = ?upstream,
                                            "Transform backpressure ack skipped: missing reader handle"
                                        );
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = ?e,
                                "Transform processing error"
                            );
                        }
                    }

                    EventLoopDirective::Continue
                }
                _ => {
                    // Other content types: forward them.
                    sup.forward_control_event(&envelope).await?;
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
            if let Some(directive) = sup.maybe_release_buffered_terminal(ctx).await? {
                return Ok(directive);
            }

            let maybe_contract_check = if let Some(subscription) = sup.subscription.as_mut() {
                subscription
                    .maybe_check_contracts_tick(
                        &mut ctx.contract_state[..],
                        &mut ctx.last_contract_check,
                    )
                    .await
            } else {
                None
            };

            if let Some(status) = maybe_contract_check {
                match status {
                    crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream) => {
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

            tracing::trace!(stage_name = %ctx.stage_name, "No events available, sleeping");
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(EventLoopDirective::Continue)
        }
        PollResult::Error(e) => {
            tracing::error!(
                stage_name = %ctx.stage_name,
                error = ?e,
                "Subscription error"
            );
            Ok(EventLoopDirective::Transition(TransformEvent::Error(
                format!("Subscription error: {e}"),
            )))
        }
    }
}
