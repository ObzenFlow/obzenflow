// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Draining state event loop for the stateful supervisor

use crate::effects::EffectInvocationContext;
use crate::metrics::instrumentation::process_with_instrumentation_no_count;
use crate::stages::common::handlers::{StatefulOutputContext, UnifiedStatefulHandler};
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
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

use crate::messaging::PollResult;

use super::super::fsm::{PendingTransition, StatefulContext, StatefulEvent, StatefulState};
use super::StatefulSupervisor;

pub(super) async fn dispatch_draining<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut StatefulSupervisor<H>,
    state: &StatefulState<H>,
    ctx: &mut StatefulContext<H>,
) -> Result<EventLoopDirective<StatefulEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }

    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        sup.stage_id,
        StageType::Stateful,
    );
    // FLOWIP-120r: per-event observer dispatch scope (distinct from the frozen
    // scope each pending output now carries through the drain).
    // Generation None: drain follows the stage frontier (FLOWIP-120n).
    let observer_scope =
        ctx.runtime_execution
            .dispatch_scope(ctx.stage_id, ctx.last_input_position, None);
    if sup.subscription.is_none() {
        sup.subscription = ctx.subscription.take();
    }

    // Drain any pending stage outputs first (FLOWIP-086k).
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
            &mut ctx.backpressure_backoff,
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
            Some(PendingTransition::DrainComplete)
        )
    {
        ctx.pending_transition = None;
        return Ok(EventLoopDirective::Transition(StatefulEvent::DrainComplete));
    }

    // Drain any remaining events from the subscription queue.
    if let Some(subscription) = sup.subscription.as_mut() {
        match subscription
            .poll_next_with_state(state.variant_name(), Some(&mut ctx.contract_state[..]))
            .await
        {
            PollResult::Event(envelope) => {
                let stage_input_position = subscription.last_delivered_stage_input_position();
                if envelope.event.is_data() {
                    ctx.last_input_position = stage_input_position;
                }
                // Retain the last consumed upstream envelope (with a merged vector-clock) so that any
                // final drain emissions can be parented and preserve happened-before via vector clocks.
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

                if envelope.event.is_data() {
                    // Accumulate data events during draining, synchronously.
                    let event = envelope.event.clone();
                    let event_id = event.id;
                    let upstream_stage = subscription.last_delivered_upstream_stage();
                    let mut handler = (*ctx.handler).clone();
                    let effect_context = stage_input_position.and_then(|input_seq| {
                        ctx.writer_id.map(|writer_id| EffectInvocationContext {
                            flow_id: ctx.flow_id,
                            stage_id: ctx.stage_id,
                            stage_key: ctx.stage_name.clone(),
                            writer_id,
                            input_seq,
                            stage_logic_version: handler.stage_logic_version().to_string(),
                            data_journal: ctx.data_journal.clone(),
                            flow_context: Some(flow_context.clone()),
                            observers: Some(ctx.observers.clone()),
                            system_journal: Some(ctx.system_journal.clone()),
                            instrumentation: Some(ctx.instrumentation.clone()),
                            heartbeat_state: ctx.heartbeat.as_ref().map(|h| h.state.clone()),
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

                    if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream_stage) {
                        if event.is_data() {
                            heartbeat.state.record_data_read(upstream, event_id);
                        }
                    }
                    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());

                    ctx.instrumentation
                        .in_flight_count
                        .fetch_add(1, Ordering::Relaxed);
                    let start = Instant::now();

                    let _processing = heartbeat_state.as_ref().map(|state| {
                        HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id)
                    });

                    // FLOWIP-120c H3: per-event middleware execution scope.
                    // Generation None: drain follows the stage frontier
                    // (FLOWIP-120n).
                    let scope = ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        stage_input_position,
                        None,
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
                        let reason = format!("Stateful handler error during drain: {err:?}");
                        let error_event = event.mark_as_error(reason, err.kind());
                        if route_to_error_journal(&error_event) {
                            ctx.error_journal
                                .append(error_event, Some(&envelope))
                                .await
                                .map_err(|e| {
                                    format!("Failed to write stateful drain error: {e}")
                                })?;
                        } else {
                            let enriched_error = error_event
                                .with_flow_context(flow_context.clone())
                                .with_runtime_context(ctx.instrumentation.snapshot_with_control());
                            ctx.data_journal
                                .append(enriched_error, Some(&envelope))
                                .await
                                .map_err(|e| {
                                    format!("Failed to write stateful drain error: {e}")
                                })?;
                        }
                        return Ok(EventLoopDirective::Continue);
                    }

                    // Track accumulated events during drain for heartbeat visibility.
                    ctx.events_since_last_heartbeat =
                        ctx.events_since_last_heartbeat.saturating_add(1);
                    if let Err(e) = sup.emit_stateful_heartbeat_if_due(ctx, false).await {
                        tracing::warn!(
                            stage_name = %ctx.stage_name,
                            error = ?e,
                            "Failed to emit stateful accumulator heartbeat during draining"
                        );
                    }

                    // After accumulation, mirror Accumulating semantics: if the handler says we
                    // should emit, do so inline.
                    if handler.should_emit(&mut ctx.current_state) {
                        let output_context = ctx
                            .writer_id
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

                        match handler.emit_with_context(&mut ctx.current_state, output_context) {
                            Ok(mut events_to_emit) => {
                                if !events_to_emit.is_empty() {
                                    let stage_writer_id =
                                        ctx.writer_id.ok_or("No writer ID available")?;
                                    let observer_ctx = StatefulObserverContext {
                                        stage_id: ctx.stage_id,
                                        stage_name: &ctx.stage_name,
                                        flow_context: &flow_context,
                                        scope,
                                        input: Some(&event),
                                        stage_input_position: stage_input_position
                                            .map(|position| position.0),
                                    };
                                    run_stateful_after_emit_observers(
                                        &ctx.observers,
                                        &observer_ctx,
                                        events_to_emit.as_mut_slice(),
                                        &ctx.data_journal,
                                        &ctx.instrumentation,
                                        Some(&envelope),
                                    )
                                    .await?;

                                    for mut out in events_to_emit {
                                        out.writer_id = stage_writer_id;
                                        if is_framework_middleware_observability_event(&out) {
                                            commit_framework_observability_events(
                                                vec![out],
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
                                                    observer_scope: scope,
                                                },
                                            )
                                            .await
                                            .map_err(|e| {
                                                format!(
                                                    "Failed to commit framework observability event: {e}"
                                                )
                                            })?;
                                            continue;
                                        }

                                        if route_to_error_journal(&out) {
                                            tracing::info!(
                                                stage_name = %ctx.stage_name,
                                                event_id = %out.id,
                                                "Writing stateful drain emitted error event to error journal (FLOWIP-082h)"
                                            );

                                            if out.is_data() {
                                                ctx.instrumentation.record_output_event(&out);
                                                if let Some(subscription) =
                                                    sup.subscription.as_mut()
                                                {
                                                    subscription.track_output_event();
                                                }
                                            }

                                            ctx.error_journal
                                                .append(out, ctx.last_consumed_envelope.as_ref())
                                                .await
                                                .map_err(|e| {
                                                    format!(
                                                        "Failed to write stateful drain error event: {e}"
                                                    )
                                                })?;
                                        } else {
                                            let scope = observer_scope;
                                            ctx.pending_outputs.push_back(
                                                crate::stages::common::supervision::backpressure_drain::PendingOutput {
                                                    event: out,
                                                    scope,
                                                },
                                            );
                                        }
                                    }

                                    if let Some(upstream) = upstream_stage {
                                        ctx.pending_ack_upstream = Some(upstream);
                                    }
                                }
                            }
                            Err(err) => {
                                tracing::error!(
                                    stage_name = %ctx.stage_name,
                                    error = ?err,
                                    "Failed to emit aggregated events during draining; mapping to error-marked event"
                                );

                                // Per-record handler failure during draining: turn the input into an
                                // error-marked event and route it using the ErrorKind policy instead
                                // of failing the stage.
                                let reason =
                                    format!("Stateful handler emit error during drain: {err:?}");
                                let error_event =
                                    envelope.event.clone().mark_as_error(reason, err.kind());

                                // Count all error-marked events for lifecycle / flow rollups, even
                                // when they are not stage-fatal.
                                ctx.instrumentation.record_error(err.kind());

                                if route_to_error_journal(&error_event) {
                                    tracing::info!(
                                        stage_name = %ctx.stage_name,
                                        event_id = %error_event.id,
                                        "Writing stateful drain error event to error journal (FLOWIP-082h)"
                                    );

                                    // Error events are still data, so record them for transport
                                    // contracts and metrics.
                                    if error_event.is_data() {
                                        ctx.instrumentation.record_output_event(&error_event);
                                        if let Some(subscription) = sup.subscription.as_mut() {
                                            subscription.track_output_event();
                                        }
                                    }

                                    ctx.error_journal
                                        .append(error_event, Some(&envelope))
                                        .await
                                        .map_err(|e| {
                                            format!(
                                                "Failed to write stateful drain error event: {e}"
                                            )
                                        })?;
                                } else {
                                    if let Some(upstream) = upstream_stage {
                                        ctx.pending_ack_upstream = Some(upstream);
                                    }
                                    let scope = observer_scope;
                                    ctx.pending_outputs.push_back(
                                        crate::stages::common::supervision::backpressure_drain::PendingOutput {
                                            event: error_event,
                                            scope,
                                        },
                                    );
                                }

                                // Keep draining; do not transition to Failed on per-record handler errors.
                            }
                        }
                    }

                    // Backpressure ack: upstream input was consumed into state.
                    if envelope.event.is_data() && ctx.pending_outputs.is_empty() {
                        if let Some(upstream) = upstream_stage {
                            if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                                reader.ack_consumed(1);
                            }
                        }
                    }
                } else {
                    // Forward control events during draining so contract events are not lost.
                    tracing::debug!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Forwarding control event during stateful draining"
                    );

                    // Do not forward EOF again during draining: it will be sent after drain completes.
                    if !envelope.event.is_eof() {
                        sup.forward_control_event(ctx, &envelope).await?;
                    }
                }

                return Ok(EventLoopDirective::Continue);
            }
            PollResult::NoEvents => {
                // Queue is truly drained - no more events available.
                // Do a final contract check before draining.
                drop(
                    subscription
                        .check_contracts(&mut ctx.contract_state[..])
                        .await,
                );

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful subscription queue drained, calling handler.drain()"
                );
            }
            PollResult::Error(e) => {
                tracing::error!(
                    stage_name = %ctx.stage_name,
                    error = ?e,
                    "Error during draining"
                );
                return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                    format!("Drain error: {e}"),
                )));
            }
        }
    }

    // Flush any remaining accumulated events into a final heartbeat snapshot before emitting drain results.
    // For the final heartbeat we bypass the normal heartbeat interval threshold so short finite flows still
    // emit a snapshot.
    if ctx.events_since_last_heartbeat > 0 {
        if let Err(e) = sup.emit_stateful_heartbeat_if_due(ctx, true).await {
            tracing::warn!(
                stage_name = %ctx.stage_name,
                error = ?e,
                "Failed to emit final stateful accumulator heartbeat before drain"
            );
        }
    }

    // Call handler.drain() to emit final accumulated state.
    let final_state = ctx.current_state.clone();
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

    let drain_result = process_with_instrumentation_no_count(&ctx.instrumentation, || async move {
        handler
            .drain_with_context(&final_state, output_context)
            .await
            .map_err(|err| -> Box<dyn std::error::Error + Send + Sync> {
                instrumentation.record_error(err.kind());
                err.into()
            })
    })
    .await;

    match drain_result {
        Ok(mut drain_events) => {
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
                drain_events.as_mut_slice(),
                &ctx.data_journal,
                &ctx.instrumentation,
                ctx.last_consumed_envelope.as_ref(),
            )
            .await?;

            for mut event in drain_events {
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
                        "Writing stateful drain() error event to error journal (FLOWIP-082h)"
                    );

                    if event.is_data() {
                        ctx.instrumentation.record_output_event(&event);
                        if let Some(subscription) = sup.subscription.as_mut() {
                            subscription.track_output_event();
                        }
                    }

                    ctx.error_journal
                        .append(event, ctx.last_consumed_envelope.as_ref())
                        .await
                        .map_err(|e| {
                            format!("Failed to write stateful drain() error event: {e}")
                        })?;
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

            ctx.pending_transition = Some(PendingTransition::DrainComplete);
            Ok(EventLoopDirective::Continue)
        }
        Err(e) => {
            tracing::error!(
                stage_name = %ctx.stage_name,
                error = ?e,
                "Drain error"
            );
            Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                format!("Drain error: {e}"),
            )))
        }
    }
}
