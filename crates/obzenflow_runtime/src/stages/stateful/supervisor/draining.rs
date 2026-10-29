// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Draining state event loop for the stateful supervisor

use crate::backpressure::BackpressureReader;
use crate::effects::EffectInvocationContext;
use crate::metrics::instrumentation::process_with_instrumentation_no_count;
use crate::stages::common::handlers::{
    StatefulOutputContext, StatefulTerminationKind, TerminalValidation, UnifiedStatefulHandler,
};
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::backpressure_drain::{drain_one_pending, DrainOutcome};
use crate::stages::common::supervision::error_routing::{
    error_row_in_scope, processing_failed, route_to_error_journal,
};
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::output_committer::{
    commit_framework_observability_events, is_framework_middleware_observability_event,
    FrameworkObservabilityCommit,
};
use crate::stages::common::supervision::stage_fatal::{record_stage_fatal, StageFatalCommit};
use crate::stages::observer::dispatch::{
    run_stateful_after_accumulate_observers, run_stateful_after_emit_observers,
    run_stateful_before_accumulate_observers,
};
use crate::stages::observer::StatefulObserverContext;
use crate::supervised_base::EventLoopDirective;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::StateVariant;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::messaging::PollResult;

use super::super::fsm::{StatefulEvent, StatefulResources, StatefulState};
use super::StatefulSupervisor;

fn acknowledge_consumed_drain_input(
    readers: &HashMap<StageId, BackpressureReader>,
    upstream: Option<StageId>,
) {
    if let Some(reader) = upstream.and_then(|stage_id| readers.get(&stage_id)) {
        reader.ack_consumed(1);
    }
}

fn terminal_kind<H>(
    ctx: &StatefulResources<H>,
    drain_requested_by_handle: bool,
) -> StatefulTerminationKind
where
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    if drain_requested_by_handle {
        return StatefulTerminationKind::BeginDrain;
    }

    match ctx
        .terminal_envelope
        .as_ref()
        .map(|envelope| &envelope.payload)
    {
        Some(ChainPayload::FlowControl(FlowControlPayload::Drain)) => {
            StatefulTerminationKind::Drain
        }
        Some(ChainPayload::FlowControl(FlowControlPayload::PipelineAbort { .. })) => {
            StatefulTerminationKind::PipelineAbort
        }
        _ => match ctx.terminal_eof_kind.unwrap_or(EofKind::Natural) {
            EofKind::Natural => StatefulTerminationKind::NaturalEof,
            EofKind::Truncated => StatefulTerminationKind::TruncatedEof,
            EofKind::Poison => StatefulTerminationKind::PoisonedEof,
        },
    }
}

fn terminal_primary_cause<H>(ctx: &StatefulResources<H>) -> Option<obzenflow_core::EventId>
where
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    let envelope = ctx.terminal_envelope.as_ref()?;
    match &envelope.payload {
        ChainPayload::FlowControl(FlowControlPayload::Eof {
            last_event_id: Some(event_id),
            ..
        }) => Some(*event_id),
        _ => Some(envelope.envelope.provenance.event.id),
    }
}

pub(super) async fn dispatch_draining<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut StatefulSupervisor<H>,
    state: &StatefulState<H>,
    ctx: &mut StatefulResources<H>,
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
            ctx.last_consumed_envelope.as_ref(),
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_stall,
            Some(&ctx.output_contract),
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

    ctx.handler
        .as_ref()
        .expect("handler available before cleanup")
        .outputs_committed(&mut ctx.current_state);

    if let Some(upstream) = ctx.pending_ack_upstream.take() {
        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
            reader.ack_consumed(1);
        }
    }

    // Drain any remaining events from the subscription queue.
    if let Some(subscription) = sup.subscription.as_mut() {
        match subscription
            .poll_next_with_state(state.variant_name(), Some(&mut ctx.contract_state[..]))
            .await
        {
            PollResult::Event(envelope) => {
                let stage_input_position = subscription.last_delivered_stage_input_position();
                let delivered_upstream_stage = subscription
                    .last_delivered_upstream_stage()
                    .expect("delivered event must identify its upstream stage");
                if envelope.consumes_data_credit() {
                    ctx.last_input_position = stage_input_position;
                }
                // Keep the genuine delivered record for authored lineage. The owner
                // retains accumulated causal evidence independently of this payload.
                ctx.last_consumed_envelope = Some(envelope.clone());
                ctx.instrumentation
                    .record_consumed(&envelope, delivered_upstream_stage);

                if envelope.consumes_data_credit() {
                    // Accumulate data events during draining, synchronously.
                    let event = envelope.authored();
                    let event_id = event.id;
                    let upstream_stage = subscription.last_delivered_upstream_stage();

                    if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream_stage) {
                        if event.consumes_data_credit() {
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
                    let _observer_parent = ctx
                        .last_consumed_envelope
                        .as_ref()
                        .map(|envelope| envelope.authored());
                    let observer_ctx = StatefulObserverContext::new(
                        ctx.flow_id,
                        &flow_context,
                        Some(&event),
                        stage_input_position,
                    );
                    run_stateful_before_accumulate_observers(&ctx.observers, scope, &observer_ctx);

                    let mut handler = (**ctx
                        .handler
                        .as_ref()
                        .expect("handler available before cleanup"))
                    .clone();
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

                            instrumentation: Some(ctx.instrumentation.clone()),
                            heartbeat_state: ctx.heartbeat.as_ref().map(|h| h.state.clone()),
                            parent: envelope.clone(),
                            effect_history: ctx.effect_history.clone(),
                            current_run_history: ctx.current_run_history.clone(),
                            runtime_execution: ctx.runtime_execution.clone(),
                            effect_ports: ctx.effect_ports.clone(),
                            effect_declarations: ctx.effect_declarations.clone(),
                            output_contract: ctx.output_contract.clone(),
                            backpressure_writer: ctx.backpressure_writer.clone(),
                            emit_enabled: true,
                            effect_boundary: None,
                            catch_up: Some(ctx.catch_up.clone()),
                        })
                    });
                    let accumulate_result = handler
                        .accumulate(&mut ctx.current_state, event.clone(), effect_context, scope)
                        .await;
                    if let Err(err) = &accumulate_result {
                        if let Some(fatal) = err.as_fatal() {
                            let duration = start.elapsed();
                            ctx.instrumentation
                                .in_flight_count
                                .fetch_sub(1, Ordering::Relaxed);
                            ctx.instrumentation.record_processing_time(duration);
                            ctx.instrumentation.record_error(err.kind());
                            let writer_id = ctx.writer_id.ok_or_else(|| {
                                "fatal stateful drain input has no stage writer id".to_string()
                            })?;
                            record_stage_fatal(
                                fatal,
                                StageFatalCommit {
                                    error_journal: &ctx.error_journal,
                                    writer_id,
                                    stage_id: ctx.stage_id,
                                    stage_key: &ctx.stage_name,
                                    input_position: stage_input_position,
                                    parent: Some(&envelope),
                                    lineage: ctx.lineage_policy,
                                },
                            )
                            .await?;
                            acknowledge_consumed_drain_input(
                                &ctx.backpressure_readers,
                                upstream_stage,
                            );
                            if let Some(state) = &heartbeat_state {
                                state.record_last_consumed(event_id);
                            }
                            return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                fatal.detail.clone(),
                            )));
                        }
                        if let Some(directive) = super::contract_violation_directive::<H>(
                            err,
                            "draining",
                            &event,
                            &ctx.stage_name,
                        ) {
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
                            ctx.instrumentation.record_error(err.kind());
                            return Ok(directive);
                        }
                    }
                    run_stateful_after_accumulate_observers(&ctx.observers, scope, &observer_ctx);

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
                        let error_event = error_row_in_scope(
                            processing_failed(
                                WriterId::from(ctx.stage_id),
                                &flow_context,
                                &event,
                                &err,
                                ctx.lineage_policy,
                            ),
                            scope,
                        );
                        crate::supervised_base::publication::append(
                            &ctx.error_journal,
                            error_event,
                            AppendOptions::from_record(Some(&envelope))?,
                        )
                        .await
                        .map_err(|e| format!("Failed to write stateful drain error: {e}"))?;

                        // Match the running loop: once ordinary handler-error evidence is
                        // durable, the input has been consumed even though accumulation failed.
                        // Contract violations return through the stage-fatal branch above and
                        // deliberately do not acknowledge here.
                        acknowledge_consumed_drain_input(&ctx.backpressure_readers, upstream_stage);

                        return Ok(EventLoopDirective::Continue);
                    }

                    // Track accumulated events during drain for heartbeat visibility.
                    ctx.events_since_last_heartbeat =
                        ctx.events_since_last_heartbeat.saturating_add(1);
                    if let Err(e) =
                        StatefulSupervisor::<H>::emit_stateful_heartbeat_if_due(ctx, false).await
                    {
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
                                    super::clear_replay_context_of_live_work(
                                        &mut events_to_emit,
                                        observer_scope,
                                    );
                                    let stage_writer_id =
                                        ctx.writer_id.ok_or("No writer ID available")?;
                                    let _observer_parent = ctx
                                        .last_consumed_envelope
                                        .as_ref()
                                        .map(|envelope| envelope.authored());
                                    let observer_ctx = StatefulObserverContext::new(
                                        ctx.flow_id,
                                        &flow_context,
                                        Some(&event),
                                        stage_input_position,
                                    );
                                    run_stateful_after_emit_observers(
                                        &ctx.observers,
                                        scope,
                                        &observer_ctx,
                                        events_to_emit.as_slice(),
                                    );

                                    for mut out in events_to_emit {
                                        out.writer_id = stage_writer_id;
                                        if is_framework_middleware_observability_event(&out) {
                                            commit_framework_observability_events(
                                                vec![out],
                                                FrameworkObservabilityCommit {
                                                    flow_context: &flow_context,
                                                    data_journal: &ctx.data_journal,

                                                    instrumentation: Some(&ctx.instrumentation),
                                                    heartbeat_state: ctx
                                                        .heartbeat
                                                        .as_ref()
                                                        .map(|heartbeat| &heartbeat.state),
                                                    backpressure_writer: &ctx.backpressure_writer,
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

                                            if out.consumes_data_credit() {
                                                if let Some(subscription) =
                                                    sup.subscription.as_mut()
                                                {
                                                    subscription.track_output_event();
                                                }
                                            }

                                            crate::stages::common::supervision::output_committer::commit_error_output(&ctx.error_journal, &ctx.instrumentation, out, ctx.last_consumed_envelope.as_ref())
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
                        causal: crate::supervised_base::publication::capture(),
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
                            Err(fatal) => {
                                let writer_id = ctx.writer_id.ok_or_else(|| {
                                    "fatal stateful drain emission has no stage writer id"
                                        .to_string()
                                })?;
                                record_stage_fatal(
                                    &fatal,
                                    StageFatalCommit {
                                        error_journal: &ctx.error_journal,
                                        writer_id,
                                        stage_id: ctx.stage_id,
                                        stage_key: &ctx.stage_name,
                                        input_position: stage_input_position,
                                        parent: Some(&envelope),
                                        lineage: ctx.lineage_policy,
                                    },
                                )
                                .await?;
                                ctx.pending_ack_upstream = None;
                                acknowledge_consumed_drain_input(
                                    &ctx.backpressure_readers,
                                    upstream_stage,
                                );
                                return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                                    fatal.detail,
                                )));
                            }
                        }
                    }

                    // Backpressure ack: upstream input was consumed into state.
                    if envelope.consumes_data_credit() && ctx.pending_outputs.is_empty() {
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
                        event_type = envelope.event_type(),
                        "Forwarding control event during stateful draining"
                    );

                    // Do not forward EOF again during draining: it will be sent after drain completes.
                    if !envelope.is_eof() {
                        StatefulSupervisor::<H>::forward_control_event(ctx, &envelope).await?;
                    }
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
                // Queue is truly drained - no more events available.
                // Do a final contract check before draining.
                subscription
                    .check_contracts(&mut ctx.contract_state[..])
                    .await
                    .into_result()?;

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

    Ok(EventLoopDirective::Transition(
        StatefulEvent::DrainInputsCompleted,
    ))
}

pub(super) async fn validate_terminal<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut StatefulResources<H>,
    drain_requested_by_handle: bool,
) -> Result<EventLoopDirective<StatefulEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let kind = terminal_kind(ctx, drain_requested_by_handle);
    let validation = if matches!(
        kind,
        StatefulTerminationKind::PipelineAbort | StatefulTerminationKind::ForceShutdown
    ) {
        TerminalValidation::Clean
    } else {
        ctx.handler
            .as_ref()
            .expect("handler available before cleanup")
            .validate_terminal(&ctx.current_state, kind)
    };

    match validation {
        TerminalValidation::Clean => {}
        TerminalValidation::Primary(fatal) => {
            let writer_id = ctx
                .writer_id
                .ok_or_else(|| "terminal validation has no stateful stage writer id".to_string())?;
            record_stage_fatal(
                &fatal,
                StageFatalCommit {
                    error_journal: &ctx.error_journal,
                    writer_id,
                    stage_id: ctx.stage_id,
                    stage_key: &ctx.stage_name,
                    input_position: None,
                    parent: ctx.terminal_envelope.as_deref(),
                    lineage: ctx.lineage_policy,
                },
            )
            .await?;
            return Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                fatal.detail,
            )));
        }
        TerminalValidation::Secondary(mut fatal) => {
            if fatal.primary_cause_event_id.is_none() {
                fatal.primary_cause_event_id = terminal_primary_cause(ctx);
            }
            let result = match ctx.writer_id {
                Some(writer_id) => {
                    record_stage_fatal(
                        &fatal,
                        StageFatalCommit {
                            error_journal: &ctx.error_journal,
                            writer_id,
                            stage_id: ctx.stage_id,
                            stage_key: &ctx.stage_name,
                            input_position: None,
                            parent: ctx.terminal_envelope.as_deref(),
                            lineage: ctx.lineage_policy,
                        },
                    )
                    .await
                }
                None => Err(
                    "secondary terminal validation has no stateful stage writer id"
                        .to_string()
                        .into(),
                ),
            };
            if let Err(error) = result {
                tracing::warn!(
                    stage_name = %ctx.stage_name,
                    error = %error,
                    "suppressed secondary terminal-validation evidence append failure"
                );
            }
        }
    }
    Ok(EventLoopDirective::Transition(
        StatefulEvent::TerminalValidated,
    ))
}

pub(super) async fn forward_terminal<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut StatefulResources<H>,
) -> Result<EventLoopDirective<StatefulEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(envelope) = ctx.terminal_envelope.clone() {
        StatefulSupervisor::<H>::forward_control_event(ctx, &envelope).await?;
    }
    Ok(EventLoopDirective::Transition(
        StatefulEvent::TerminalForwarded,
    ))
}

pub(super) async fn produce_final_output<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut StatefulResources<H>,
    drain_requested_by_handle: bool,
) -> Result<EventLoopDirective<StatefulEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }

    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Stateful,
    );
    // FLOWIP-120r: per-event observer dispatch scope (distinct from the frozen
    // scope each pending output now carries through the drain).
    // Generation None: drain follows the stage frontier (FLOWIP-120n).
    let observer_scope =
        ctx.runtime_execution
            .dispatch_scope(ctx.stage_id, ctx.last_input_position, None);

    // Flush any remaining accumulated events into a final heartbeat snapshot before emitting drain results.
    // For the final heartbeat we bypass the normal heartbeat interval threshold so short finite flows still
    // emit a snapshot.
    if ctx.events_since_last_heartbeat > 0 {
        if let Err(e) = StatefulSupervisor::<H>::emit_stateful_heartbeat_if_due(ctx, true).await {
            tracing::warn!(
                stage_name = %ctx.stage_name,
                error = ?e,
                "Failed to emit final stateful accumulator heartbeat before drain"
            );
        }
    }

    let kind = terminal_kind(ctx, drain_requested_by_handle);
    let finalizes = ctx
        .handler
        .as_ref()
        .expect("handler available before cleanup")
        .finalizes_on(kind);
    if !finalizes {
        tracing::info!(
            stage_name = %ctx.stage_name,
            termination = ?kind,
            "termination does not finalize; skipping end-of-input finalization"
        );
        return Ok(EventLoopDirective::Transition(
            StatefulEvent::FinalOutputsPrepared,
        ));
    }

    // Call handler.drain() to emit final accumulated state.
    let final_state = &ctx.current_state;
    let handler = (**ctx
        .handler
        .as_ref()
        .expect("handler available before cleanup"))
    .clone();
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
            .drain_with_context(final_state, output_context)
            .await
            .inspect_err(|_| instrumentation.record_error(ErrorKind::Unknown))
    })
    .await;

    match drain_result {
        Ok(mut drain_events) => {
            super::clear_replay_context_of_live_work(&mut drain_events, observer_scope);
            let stage_writer_id = ctx.writer_id.ok_or("No writer ID available")?;
            if !drain_events.is_empty() {
                let observer_parent = ctx
                    .last_consumed_envelope
                    .as_ref()
                    .map(|envelope| envelope.authored());
                let observer_ctx = StatefulObserverContext::new(
                    ctx.flow_id,
                    &flow_context,
                    observer_parent.as_ref(),
                    ctx.last_input_position,
                );
                run_stateful_after_emit_observers(
                    &ctx.observers,
                    observer_scope,
                    &observer_ctx,
                    drain_events.as_slice(),
                );
            }

            for mut event in drain_events {
                event.writer_id = stage_writer_id;
                if is_framework_middleware_observability_event(&event) {
                    commit_framework_observability_events(
                        vec![event],
                        FrameworkObservabilityCommit {
                            flow_context: &flow_context,
                            data_journal: &ctx.data_journal,

                            instrumentation: Some(&ctx.instrumentation),
                            heartbeat_state: ctx
                                .heartbeat
                                .as_ref()
                                .map(|heartbeat| &heartbeat.state),
                            backpressure_writer: &ctx.backpressure_writer,
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

                    if event.consumes_data_credit() {
                        if let Some(subscription) = ctx.subscription.as_mut() {
                            subscription.track_output_event();
                        }
                    }

                    crate::stages::common::supervision::output_committer::commit_error_output(
                        &ctx.error_journal,
                        &ctx.instrumentation,
                        event,
                        ctx.last_consumed_envelope.as_ref(),
                    )
                    .await
                    .map_err(|e| format!("Failed to write stateful drain() error event: {e}"))?;
                } else {
                    let scope = observer_scope;
                    ctx.pending_outputs.push_back(
                        crate::stages::common::supervision::backpressure_drain::PendingOutput {
                            causal: crate::supervised_base::publication::capture(),
                            event,
                            scope,
                        },
                    );
                }
            }

            Ok(EventLoopDirective::Transition(
                StatefulEvent::FinalOutputsPrepared,
            ))
        }
        Err(fatal) => {
            let writer_id = ctx
                .writer_id
                .ok_or_else(|| "fatal stateful drain has no stage writer id".to_string())?;
            let parent = ctx
                .terminal_envelope
                .as_ref()
                .or(ctx.last_consumed_envelope.as_ref());
            record_stage_fatal(
                &fatal,
                StageFatalCommit {
                    error_journal: &ctx.error_journal,
                    writer_id,
                    stage_id: ctx.stage_id,
                    stage_key: &ctx.stage_name,
                    input_position: None,
                    parent: parent.map(crate::messaging::DeliveredRecord::record),
                    lineage: ctx.lineage_policy,
                },
            )
            .await?;
            Ok(EventLoopDirective::Transition(StatefulEvent::Error(
                fatal.detail,
            )))
        }
    }
}

pub(super) async fn drain_final_output<
    H: UnifiedStatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut StatefulResources<H>,
) -> Result<EventLoopDirective<StatefulEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    if let Some(heartbeat) = &ctx.heartbeat {
        heartbeat.state.mark_draining();
    }

    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Stateful,
    );
    // FLOWIP-120r: per-event observer dispatch scope (distinct from the frozen
    // scope each pending output now carries through the drain).
    // Generation None: drain follows the stage frontier (FLOWIP-120n).
    let _observer_scope =
        ctx.runtime_execution
            .dispatch_scope(ctx.stage_id, ctx.last_input_position, None);

    // Drain any pending stage outputs first (FLOWIP-086k).
    while let Some(pending) = ctx.pending_outputs.pop_front() {
        match drain_one_pending(
            pending,
            &flow_context,
            ctx.stage_id,
            ctx.heartbeat.as_ref().map(|h| h.state.clone()),
            &ctx.data_journal,
            ctx.last_consumed_envelope.as_ref(),
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_stall,
            Some(&ctx.output_contract),
            &mut ctx.pending_outputs,
        )
        .await?
        {
            DrainOutcome::Committed { was_data } => {
                if was_data {
                    if let Some(subscription) = ctx.subscription.as_mut() {
                        subscription.track_output_event();
                    }
                }
            }
            DrainOutcome::BackedOff => return Ok(EventLoopDirective::Continue),
        }
    }

    ctx.handler
        .as_ref()
        .expect("handler available before cleanup")
        .outputs_committed(&mut ctx.current_state);

    if let Some(upstream) = ctx.pending_ack_upstream.take() {
        if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
            reader.ack_consumed(1);
        }
    }

    Ok(EventLoopDirective::Transition(StatefulEvent::DrainComplete))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backpressure::{BackpressurePlan, BackpressureRegistry, BackpressureWriter};
    use crate::effects::{CurrentRunHistory, EffectPortRegistry};
    use crate::execution::{RuntimeExecution, RuntimeMode};
    use crate::feed_plan::StageOutputContract;
    use crate::id_conversions::StageIdExt;
    use crate::message_bus::FsmMessageBus;
    use crate::metrics::instrumentation::StageInstrumentation;
    use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
    use crate::stages::common::control_strategies::{JonestownSignalStrategy, ProcessingContext};
    use crate::stages::common::handlers::{
        StatefulEmission, TypedStatefulHandler, TypedStatefulHandlerAdapter,
    };
    use crate::stages::common::supervision::catch_up::CatchUpHandle;
    use crate::stages::observer::StageObserverBundle;
    use crate::stages::resources_builder::SubscriptionFactory;
    use crate::stages::stateful::fsm::{StatefulAction, StatefulContext};
    use crate::supervised_base::HandlerSupervised;
    use async_trait::async_trait;
    use obzenflow_core::config::LineagePolicy;
    use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
    use obzenflow_core::event::provenance::causality_context::CausalInput;
    use obzenflow_core::event::status::processing_status::{ErrorKind, ProcessingStatus};
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::event::{StageFatalCode, StageFatalReason};
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::{
        Journal, JournalError, JournalReader, JournalStorage, JournalStorageReader,
    };
    use obzenflow_core::{ChainEvent, EventId, FlowId, JournalId, JournalRecord, TypedPayload};
    use obzenflow_fsm::FsmAction;
    use obzenflow_topology::TopologyBuilder;
    use serde::{Deserialize, Serialize};
    use serde_json::{json, Value};
    use std::collections::VecDeque;
    use std::marker::PhantomData;
    use std::num::NonZeroU64;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn ordinary_accumulate_error_acknowledges_one_consumed_drain_input() {
        let mut builder = TopologyBuilder::new();
        let upstream_topology_id = builder.add_stage(Some("upstream".to_string()));
        let stateful_topology_id = builder.add_stage(Some("stateful".to_string()));
        let topology = builder.build_unchecked().expect("topology");
        let upstream = StageId::from_topology_id(upstream_topology_id);
        let stateful = StageId::from_topology_id(stateful_topology_id);
        let plan = BackpressurePlan::disabled().with_stage_enforced(
            upstream,
            NonZeroU64::new(1).expect("window"),
            std::time::Duration::from_secs(30),
        );
        let registry = BackpressureRegistry::new(&topology, &plan);
        registry
            .writer(upstream)
            .reserve(1)
            .expect("one input reservation")
            .commit(1);

        let readers = HashMap::from([(upstream, registry.reader(upstream, stateful))]);
        acknowledge_consumed_drain_input(&readers, Some(upstream));

        let snapshot = registry.metrics_snapshot();
        assert_eq!(snapshot.stage_writer_seq.get(&upstream), Some(&1));
        assert_eq!(snapshot.stage_min_reader_seq.get(&upstream), Some(&1));
        assert_eq!(snapshot.edge_in_flight.get(&(upstream, stateful)), Some(&0));
    }

    struct TestJournal {
        id: JournalId,
        owner: JournalOwner,
        records: Arc<Mutex<Vec<JournalRecord<ChainPayload>>>>,
    }

    impl TestJournal {
        fn owned_by(stage: StageId) -> Arc<dyn Journal<ChainEvent>> {
            Arc::new(Self {
                id: JournalId::new(),
                owner: JournalOwner::stage(stage),
                records: Arc::default(),
            })
        }
    }

    #[async_trait]
    impl JournalStorage<ChainEvent> for TestJournal {
        fn storage_id(&self) -> &JournalId {
            &self.id
        }

        fn storage_owner(&self) -> Option<&JournalOwner> {
            Some(&self.owner)
        }

        async fn storage_append(
            &self,
            event: ChainEvent,
            mut options: AppendOptions<ChainEvent>,
        ) -> Result<JournalRecord<ChainPayload>, JournalError> {
            let event = options.capture.prepare(0, event);
            let mut records = self.records.lock().unwrap();
            let record =
                crate::testing::causal_fixture::commit(self.id, event, &options, &records)?;
            records.push(record.clone());
            Ok(record)
        }

        async fn storage_read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            Ok(self.records.lock().unwrap().clone())
        }

        async fn storage_read_event(
            &self,
            event_id: &EventId,
        ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .iter()
                .find(|record| record.envelope.provenance.event.id == *event_id)
                .cloned())
        }

        async fn storage_reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
            Ok(Box::new(TestJournalReader {
                records: self.records.clone(),
                position: position as usize,
            }))
        }

        async fn storage_read_last_n(
            &self,
            count: usize,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            Ok(self
                .records
                .lock()
                .unwrap()
                .iter()
                .rev()
                .take(count)
                .cloned()
                .collect())
        }
    }

    struct TestJournalReader {
        records: Arc<Mutex<Vec<JournalRecord<ChainPayload>>>>,
        position: usize,
    }

    #[async_trait]
    impl JournalStorageReader<ChainEvent> for TestJournalReader {
        async fn storage_next(
            &mut self,
        ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
            let record = self.records.lock().unwrap().get(self.position).cloned();
            if record.is_some() {
                self.position += 1;
            }
            Ok(record)
        }

        fn storage_position(&self) -> u64 {
            self.position as u64
        }

        fn storage_is_at_end(&self) -> bool {
            self.position >= self.records.lock().unwrap().len()
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Reading {
        value: u64,
    }

    impl TypedPayload for Reading {
        const EVENT_TYPE: &'static str = "stateful_drain.reading";
    }

    #[derive(Clone, Debug, Deserialize)]
    struct Unpublishable;

    impl Serialize for Unpublishable {
        fn serialize<S: serde::Serializer>(&self, _: S) -> Result<S::Ok, S::Error> {
            Err(serde::ser::Error::custom("a total cannot be published"))
        }
    }

    impl TypedPayload for Unpublishable {
        const EVENT_TYPE: &'static str = "stateful_drain.unpublishable";
    }

    #[derive(Clone, Debug, Default)]
    struct UnpublishableTotal {
        ready: Arc<AtomicBool>,
    }

    impl TypedStatefulHandler for UnpublishableTotal {
        type State = u64;
        type Input = Reading;
        type Output = Unpublishable;

        fn initial_state(&self) -> u64 {
            0
        }

        fn accumulate(&self, total: &mut u64, reading: Reading) {
            *total += reading.value;
        }

        fn should_emit(&self, total: &u64) -> bool {
            self.ready.load(Ordering::SeqCst) && *total > 0
        }

        fn emit(&self, _: &u64) -> StatefulEmission<Unpublishable> {
            StatefulEmission::RetainEpoch {
                outputs: vec![Unpublishable],
            }
        }

        fn post_emit(&self, _: &mut u64) {}
    }

    type Total = TypedStatefulHandlerAdapter<UnpublishableTotal>;

    struct DrainingTotal {
        supervisor: StatefulSupervisor<Total>,
        context: StatefulContext<Total>,
        ready: Arc<AtomicBool>,
        registry: BackpressureRegistry,
        upstream_writer: BackpressureWriter,
        upstream: StageId,
        stage: StageId,
        downstream: StageId,
        upstream_journal: Arc<dyn Journal<ChainEvent>>,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
    }

    async fn draining_total() -> DrainingTotal {
        let mut builder = TopologyBuilder::new();
        let upstream_topology_id = builder.add_stage(Some("upstream".to_string()));
        let stage_topology_id = builder.add_stage(Some("total".to_string()));
        let downstream_topology_id = builder.add_stage(Some("downstream".to_string()));
        let topology = builder.build_unchecked().expect("topology");
        let upstream = StageId::from_topology_id(upstream_topology_id);
        let stage = StageId::from_topology_id(stage_topology_id);
        let downstream = StageId::from_topology_id(downstream_topology_id);
        let window = NonZeroU64::new(1).expect("window");
        let plan = BackpressurePlan::disabled()
            .with_stage_enforced(upstream, window, Duration::from_secs(30))
            .with_stage_enforced(stage, window, Duration::from_secs(30));
        let registry = BackpressureRegistry::new(&topology, &plan);

        let upstream_journal = TestJournal::owned_by(upstream);
        let data_journal = TestJournal::owned_by(stage);
        let error_journal = TestJournal::owned_by(stage);
        let mut upstream_subscription_factory = SubscriptionFactory::new(HashMap::from([
            (upstream, "upstream".to_string()),
            (stage, "total".to_string()),
            (downstream, "downstream".to_string()),
        ]))
        .bind(&[(upstream, upstream_journal.clone())]);
        upstream_subscription_factory.owner_label = "total".to_string();

        let total = UnpublishableTotal::default();
        let ready = total.ready.clone();
        let mut handler = TypedStatefulHandlerAdapter::new(total);
        handler.install_writer_id(WriterId::from(stage));
        let current_state = handler.initial_state();
        let instrumentation = Arc::new(StageInstrumentation::new());
        let runtime_execution = RuntimeExecution::new(RuntimeMode::Live, None);
        let flow_id = FlowId::new();
        let catch_up = CatchUpHandle::producing(
            make_flow_context(
                "drain_flow",
                &flow_id.to_string(),
                "total",
                stage,
                StageType::Stateful,
            ),
            data_journal.clone(),
            instrumentation.clone(),
            runtime_execution.clone(),
        );
        let mut context = StatefulContext::new(StatefulResources {
            handler: Some(Arc::new(handler)),
            stage_id: stage,
            stage_name: "total".to_string(),
            observers: StageObserverBundle::default(),
            flow_name: "drain_flow".to_string(),
            flow_id,
            current_state,
            data_journal: data_journal.clone(),
            effect_history: None,
            current_run_history: CurrentRunHistory::new(data_journal.clone()),
            runtime_execution,
            effect_ports: EffectPortRegistry::new(),
            effect_declarations: Vec::new(),
            last_input_position: None,
            error_journal: error_journal.clone(),
            bus: Arc::new(FsmMessageBus::new()),
            writer_id: None,
            lineage_policy: LineagePolicy::default(),
            subscription: None,
            contract_state: Vec::new(),
            last_contract_check: None,
            control_strategy: Arc::new(JonestownSignalStrategy),
            processing_context: ProcessingContext::default(),
            buffered_eof: None,
            terminal_envelope: None,
            terminal_eof_kind: None,
            last_consumed_envelope: None,
            instrumentation,
            upstream_subscription_factory,
            events_since_last_heartbeat: 0,
            heartbeat_interval: 0,
            last_data_event_time: None,
            emit_interval: None,
            backpressure_writer: registry.writer(stage),
            output_contract: StageOutputContract::empty(),
            backpressure_readers: HashMap::from([(upstream, registry.reader(upstream, stage))]),
            pending_outputs: VecDeque::new(),
            pending_transition: None,
            pending_ack_upstream: None,
            backpressure_pulse: BackpressureActivityPulse::new(),
            backpressure_stall: Default::default(),
            heartbeat: None,
            catch_up,
        });
        StatefulAction::AllocateResources
            .execute(&mut context)
            .await
            .expect("allocate resources");

        DrainingTotal {
            supervisor: StatefulSupervisor {
                name: "total".to_string(),
                stage_id: stage,
                subscription: None,
                _marker: PhantomData,
            },
            context,
            ready,
            upstream_writer: registry.writer(upstream),
            registry,
            upstream,
            stage,
            downstream,
            upstream_journal,
            data_journal,
            error_journal,
        }
    }

    impl DrainingTotal {
        async fn append(&mut self, payload: Value) -> ChainEvent {
            self.upstream_writer
                .reserve(1)
                .expect("upstream credit")
                .commit(1);
            self.upstream_journal
                .append(
                    ChainEventFactory::data_event(
                        WriterId::from(self.upstream),
                        Reading::event_type_name(),
                        Reading::payload_schema_version(),
                        payload,
                    ),
                    AppendOptions::default(),
                )
                .await
                .expect("append the reading")
                .authored()
        }

        async fn dispatch(
            &mut self,
            state: StatefulState<Total>,
        ) -> EventLoopDirective<StatefulEvent<Total>> {
            self.supervisor
                .dispatch_state(&state, &mut self.context)
                .await
                .expect("dispatch the stage")
        }

        async fn drain(&mut self, payload: Value) -> ChainEvent {
            let input = self.append(payload).await;
            let directive = self.dispatch(StatefulState::Draining).await;
            assert!(
                matches!(directive, EventLoopDirective::Continue),
                "the input leaves the stage draining: {directive:?}"
            );
            input
        }

        async fn assert_failed_on(
            &self,
            input: &ChainEvent,
            directive: EventLoopDirective<StatefulEvent<Total>>,
        ) {
            let EventLoopDirective::Transition(StatefulEvent::Error(detail)) = directive else {
                panic!("the emission fails the stage: {directive:?}");
            };
            assert!(detail.contains("a total cannot be published"), "{detail}");
            let errors = self
                .error_journal
                .read_causally_ordered()
                .await
                .expect("read the error journal");
            let [record] = errors.as_slice() else {
                panic!("the stage records one error: {errors:?}");
            };
            let ChainPayload::Execution(ExecutionPayload::StageFatalRecorded(fatal)) =
                &record.payload
            else {
                panic!("the error is a stage-fatal record: {:?}", record.payload);
            };
            assert_eq!(fatal.code, StageFatalCode::Protocol);
            assert_eq!(fatal.reason, StageFatalReason::OutputSerializationFailure);
            assert_eq!(fatal.detail, detail);
            assert_eq!(record.authored().causality.parent_ids, vec![input.id]);
            assert!(
                self.data_journal
                    .read_causally_ordered()
                    .await
                    .expect("read the data journal")
                    .iter()
                    .all(|record| !record.consumes_data_credit()),
                "the emission publishes nothing"
            );
            assert_eq!(
                self.registry.edge_in_flight(self.stage, self.downstream),
                Some(0),
                "nothing goes downstream"
            );
        }

        async fn assert_recorded(
            &self,
            input: &ChainEvent,
            kind: ErrorKind,
            detail: &str,
            payload: Value,
        ) {
            let errors = self
                .error_journal
                .read_causally_ordered()
                .await
                .expect("read the error journal");
            let [record] = errors.as_slice() else {
                panic!("the stage records one error: {errors:?}");
            };
            let ChainPayload::Execution(ExecutionPayload::StageProcessingFailed(failure)) =
                &record.payload
            else {
                panic!("the error is a processing failure: {:?}", record.payload);
            };
            assert_eq!(failure.kind, kind);
            assert_eq!(failure.detail, detail);
            assert_eq!(failure.input.payload, payload);

            let event = record.authored();
            assert_eq!(event.event_type(), "runtime.stage.processing_failed");
            assert_eq!(event.writer_id, WriterId::from(self.stage));
            assert_eq!(event.flow_context.stage_id, self.stage);
            assert_eq!(event.causality.parent_ids, vec![input.id]);
            assert_eq!(event.causality.inputs, vec![CausalInput::of(input)]);
            assert!(
                matches!(
                    &event.processing.status,
                    ProcessingStatus::Error { kind: Some(mark), message, .. }
                        if *mark == kind && message == detail
                ),
                "the record's mark matches its kind: {:?}",
                event.processing.status
            );

            assert!(
                self.data_journal
                    .read_causally_ordered()
                    .await
                    .expect("read the data journal")
                    .iter()
                    .all(|record| !record.consumes_data_credit()),
                "the failed input reaches no data record"
            );
            assert_eq!(
                self.registry.edge_in_flight(self.stage, self.downstream),
                Some(0),
                "nothing goes downstream"
            );
            assert_eq!(
                self.upstream_writer.min_downstream_credit(),
                1,
                "the failed input is acknowledged"
            );
        }
    }

    #[tokio::test]
    async fn a_draining_stage_records_the_input_it_cannot_accumulate() {
        let mut total = draining_total().await;
        let unreadable = json!({ "value": "seven" });
        let input = total.drain(unreadable.clone()).await;
        let decode_error = Reading::try_from_event(&input)
            .expect_err("the reading does not decode")
            .to_string();
        total
            .assert_recorded(
                &input,
                ErrorKind::Deserialization,
                &decode_error,
                unreadable,
            )
            .await;
    }

    #[tokio::test]
    async fn an_emission_an_input_triggers_fails_the_stage_when_its_output_cannot_serialize() {
        let mut total = draining_total().await;
        total.ready.store(true, Ordering::SeqCst);
        let input = total.append(json!({ "value": 7 })).await;
        let directive = total.dispatch(StatefulState::Accumulating).await;
        assert!(
            matches!(
                directive,
                EventLoopDirective::Transition(StatefulEvent::ShouldEmit)
            ),
            "{directive:?}"
        );
        let directive = total.dispatch(StatefulState::Emitting).await;
        total.assert_failed_on(&input, directive).await;
    }

    #[tokio::test]
    async fn an_emission_the_timer_triggers_fails_the_stage_when_its_output_cannot_serialize() {
        let mut total = draining_total().await;
        total
            .context
            .resources_mut()
            .expect("resources")
            .emit_interval = Some(Duration::ZERO);
        let input = total.append(json!({ "value": 7 })).await;
        let directive = total.dispatch(StatefulState::Accumulating).await;
        assert!(
            matches!(directive, EventLoopDirective::Continue),
            "{directive:?}"
        );
        total.ready.store(true, Ordering::SeqCst);
        let directive = total.dispatch(StatefulState::Accumulating).await;
        assert!(
            matches!(
                directive,
                EventLoopDirective::Transition(StatefulEvent::ShouldEmit)
            ),
            "the idle tick triggers the emission: {directive:?}"
        );
        let directive = total.dispatch(StatefulState::Emitting).await;
        total.assert_failed_on(&input, directive).await;
    }

    #[tokio::test]
    async fn an_emission_while_draining_fails_the_stage_when_its_output_cannot_serialize() {
        let mut total = draining_total().await;
        total.ready.store(true, Ordering::SeqCst);
        let input = total.append(json!({ "value": 7 })).await;
        let directive = total.dispatch(StatefulState::Draining).await;
        total.assert_failed_on(&input, directive).await;
    }

    #[tokio::test]
    async fn the_end_of_input_emission_fails_the_stage_when_its_output_cannot_serialize() {
        let mut total = draining_total().await;
        let input = total.drain(json!({ "value": 7 })).await;
        let directive =
            produce_final_output(total.context.resources_mut().expect("resources"), false)
                .await
                .expect("produce the final output");
        total.assert_failed_on(&input, directive).await;
    }
}
