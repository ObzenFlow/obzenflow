// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Running-state dispatch loop for the journal sink supervisor.

use crate::backpressure::BackpressureWriter;
use crate::effects::EffectInvocationContext;
use crate::feed_plan::StageOutputContract;
use crate::messaging::PollResult;
use crate::metrics::instrumentation::{process_with_instrumentation, snapshot_stage_metrics};
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::{
    SinkConsumeReport, SinkOperationError, SinkWriteFailure, SinkWriteFailureDisposition,
    UnifiedSinkHandler,
};
use crate::stages::common::heartbeat::HeartbeatProcessingGuard;
use crate::stages::common::supervision::catch_up::{
    flip_on_authored_eof, maybe_flip_caught_up, CatchUpDisposition, CatchUpStage,
};
use crate::stages::common::supervision::control_resolution::{
    resolve_control_event_awaiting_pauses, ControlAction,
};
use crate::stages::common::supervision::error_routing::route_to_error_journal;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::forward_control_event::forward_control_event as forward_control_event_helper;
use crate::stages::common::supervision::stage_fatal::{record_stage_fatal, StageFatalCommit};
use crate::stages::observer::dispatch::run_sink_delivery_observers;
use crate::stages::observer::{SinkDeliveryAttemptResult, SinkDeliveryObserverOutcome};
use crate::supervised_base::EventLoopDirective;
use futures::FutureExt;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryMethod, DeliveryPayload, DeliveryResult,
};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::payloads::observability_payload::ObservabilityPayload;
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::event::{
    ChainEventContent, ChainEventFactory, EventEnvelope, JournalEvent, SinkOperationFailed,
    SinkOperationPhase, StageFatalCode, StageFatalReason, SystemEvent,
};
use obzenflow_core::WriterId;
use obzenflow_core::{ChainEvent, TypedPayload};
use obzenflow_fsm::StateVariant;
use std::collections::HashSet;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::boundary::{
    SinkDeliveryAdmission, SinkDeliveryAttemptOutcome, SinkDeliveryPermit, SinkDeliveryRejection,
    SinkPolicyEvidenceBatch,
};
use super::super::fsm::{JournalSinkContext, JournalSinkEvent, JournalSinkState};
use super::super::journalled_delivery_event;
use super::JournalSinkSupervisor;
use obzenflow_core::MiddlewareExecutionScope;
use serde_json::json;

pub(super) async fn dispatch_running<
    H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JournalSinkSupervisor<H>,
    state: &JournalSinkState<H>,
    ctx: &mut JournalSinkContext<H>,
) -> Result<EventLoopDirective<JournalSinkEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let loop_count = ctx
        .instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    tracing::trace!(
        target: "flowip-080o",
        stage_name = %ctx.stage_name,
        loop_iteration = loop_count + 1,
        "sink: Running state - starting event loop iteration"
    );

    // Phase 1b follow-up: the subscription is supervisor-owned. AllocateResources
    // seeds it in `ctx.subscription`, then the first dispatch moves it here.
    if sup.subscription.is_none() {
        sup.subscription = ctx.subscription.take();
    }

    let Some(subscription) = sup.subscription.as_mut() else {
        tracing::warn!(
            target: "flowip-080o",
            stage_name = %ctx.stage_name,
            loop_iteration = loop_count + 1,
            "sink: No subscription available, sleeping"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
        return Ok(EventLoopDirective::Continue);
    };

    tracing::trace!(
        target: "flowip-080o",
        stage_name = %ctx.stage_name,
        loop_iteration = loop_count + 1,
        "sink: about to call subscription.poll_next()"
    );

    let poll_result = subscription
        .poll_next_with_state(state.variant_name(), Some(&mut ctx.contract_state[..]))
        .await;

    match poll_result {
        PollResult::Event(envelope) => {
            tracing::trace!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                loop_iteration = loop_count + 1,
                event_type = %envelope.event.event_type_name(),
                event_id = ?envelope.event.id,
                "sink: poll_next returned Event"
            );
            let delivered_upstream_stage = subscription
                .last_delivered_upstream_stage()
                .expect("delivered event must identify its upstream stage");
            ctx.instrumentation
                .record_consumed(&envelope, delivered_upstream_stage);
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);

            let is_data = envelope.event.is_data();
            let stage_input_position = subscription.last_delivered_stage_input_position();
            let directive =
                dispatch_event(ctx, subscription, &envelope, stage_input_position).await?;
            let received_eof = matches!(
                directive,
                EventLoopDirective::Transition(JournalSinkEvent::ReceivedEOF)
            );

            // Backpressure ack: upstream input was consumed by sink handler.
            if is_data && !received_eof {
                if let Some(upstream) = subscription.last_delivered_upstream_stage() {
                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                        reader.ack_consumed(1);
                    }
                }
            }

            if !received_eof {
                if let Some(status) = subscription
                    .maybe_check_contracts_tick_diagnostics_only(
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
                                "Upstream stalled detected during sink processing"
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
                                "Contract violation detected during sink processing"
                            );
                        }
                        _ => {}
                    }
                }
            }

            if received_eof {
                ctx.subscription = sup.subscription.take();
            }

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
            // FLOWIP-095d: a canonical merge that delivered nothing because an
            // input is quiet is idle-by-rule; name the awaited input.
            crate::stages::common::heartbeat::note_merge_wait(
                ctx.heartbeat.as_ref(),
                subscription.merge_wait(),
            );

            if let Some(status) = subscription
                .maybe_check_contracts_tick_diagnostics_only(
                    &mut ctx.contract_state[..],
                    &mut ctx.last_contract_check,
                )
                .await
            {
                match status {
                    crate::messaging::upstream_subscription::ContractStatus::Stalled(upstream) => {
                        tracing::warn!(
                            stage_name = %ctx.stage_name,
                            upstream = ?upstream,
                            "Upstream stalled detected during sink processing"
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
                            "Contract violation detected during sink processing"
                        );
                    }
                    _ => {}
                }
            }

            tracing::trace!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                loop_iteration = loop_count + 1,
                "sink: poll_next returned NoEvents, sleeping"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
            Ok(EventLoopDirective::Continue)
        }
        PollResult::Error(e) => {
            tracing::error!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                loop_iteration = loop_count + 1,
                error = ?e,
                "sink: poll_next returned Error"
            );
            Ok(EventLoopDirective::Transition(JournalSinkEvent::Error(
                format!("Subscription error: {e}"),
            )))
        }
    }
}

async fn dispatch_event<H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static>(
    ctx: &mut JournalSinkContext<H>,
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    envelope: &EventEnvelope<ChainEvent>,
    stage_input_position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
) -> Result<EventLoopDirective<JournalSinkEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    tracing::trace!(stage_name = %ctx.stage_name, "Sink processing event");

    let upstream_stage = subscription.last_delivered_upstream_stage();
    if let (Some(heartbeat), Some(upstream)) = (&ctx.heartbeat, upstream_stage) {
        if envelope.event.is_data() {
            heartbeat
                .state
                .record_data_read(upstream, envelope.event.id);
        }
    }

    match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
            dispatch_control_event(ctx, subscription, envelope, signal).await
        }
        obzenflow_core::event::ChainEventContent::Data { .. } => {
            dispatch_data_event(ctx, subscription, envelope, stage_input_position).await
        }
        _ => {
            // Typed sink writers are a Data-only authoring surface. Delivery
            // and observability rows remain runtime transport/lifecycle input.
            let event_id = envelope.event.id;
            let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());
            if let Some(state) = &heartbeat_state {
                state.record_last_consumed(event_id);
            }
            Ok(EventLoopDirective::Continue)
        }
    }
}

async fn dispatch_control_event<H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static>(
    ctx: &mut JournalSinkContext<H>,
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    envelope: &EventEnvelope<ChainEvent>,
    signal: &FlowControlPayload,
) -> Result<EventLoopDirective<JournalSinkEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    // FLOWIP-120n: consume the catch-up watermark before the generic control
    // resolution. The sink is terminal and authors nothing: a forwarded
    // marker would violate F8 for readers of the sink journal.
    if let FlowControlPayload::CatchUpComplete {
        generation: announced,
        ..
    } = signal
    {
        let disposition = maybe_flip_caught_up(
            *announced,
            subscription.all_readers_caught_up(*announced),
            subscription.delivered_data_count(),
            CatchUpStage {
                stage_id: ctx.stage_id,
                stage_name: &ctx.stage_name,
                flow_name: &ctx.flow_name,
                flow_id: &ctx.flow_id.to_string(),
                stage_type: StageType::Sink,
                writer_id: ctx.writer_id,
                data_journal: &ctx.data_journal,
                instrumentation: &ctx.instrumentation,
            },
            /* author_marker */ false,
            &ctx.runtime_execution,
            &mut ctx.catch_up_flip,
        )
        .await;
        return Ok(match disposition {
            CatchUpDisposition::Consumed => EventLoopDirective::Continue,
            CatchUpDisposition::Failed(message) => {
                EventLoopDirective::Transition(JournalSinkEvent::Error(message))
            }
        });
    }

    // FLOWIP-120n F17: an authored EOF can be the delivery that completes the
    // caught-up frontier; no watermark follows, so re-run the flip before
    // normal EOF handling.
    if envelope.event.is_eof() {
        if let Some(message) = flip_on_authored_eof(
            subscription,
            CatchUpStage {
                stage_id: ctx.stage_id,
                stage_name: &ctx.stage_name,
                flow_name: &ctx.flow_name,
                flow_id: &ctx.flow_id.to_string(),
                stage_type: StageType::Sink,
                writer_id: ctx.writer_id,
                data_journal: &ctx.data_journal,
                instrumentation: &ctx.instrumentation,
            },
            /* author_marker */ false,
            &ctx.runtime_execution,
            &mut ctx.catch_up_flip,
        )
        .await
        {
            return Ok(EventLoopDirective::Transition(JournalSinkEvent::Error(
                message,
            )));
        }
    }

    let upstream_stage = subscription.last_delivered_upstream_stage();
    let last_eof_outcome = subscription.last_eof_outcome().cloned();
    let contract_reader_count = ctx.contract_state.len();

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
        envelope,
        ctx.control_strategy.as_ref(),
        &mut ctx.processing_context,
        /* cycle_config */ None,
        /* cycle_guard */ None,
        last_eof_outcome.as_ref(),
        upstream_stage,
        contract_reader_count,
        /* drain_is_terminal */ false,
        &ctx.stage_name,
    )
    .await;

    match resolution {
        ControlAction::Forward => {
            if envelope.event.is_eof() {
                drop(
                    subscription
                        .check_contracts_diagnostics_only(&mut ctx.contract_state[..])
                        .await,
                );
                let _ = subscription.take_last_eof_outcome();

                let upstream_readers = subscription.upstream_count();
                match last_eof_outcome {
                    Some(outcome) => {
                        tracing::info!(
                            target: "flowip-080o",
                            stage_name = %ctx.stage_name,
                            upstream_stage_id = ?outcome.stage_id,
                            upstream_stage_name = %outcome.stage_name,
                            reader_index = outcome.reader_index,
                            eof_count = outcome.eof_count,
                            total_readers = outcome.total_readers,
                            is_final = outcome.is_final,
                            event_type = envelope.event.event_type(),
                            "Sink received EOF; evaluated drain decision"
                        );

                        tracing::info!(
                            stage_name = %ctx.stage_name,
                            "Sink EOF not final; continuing to consume remaining upstreams"
                        );
                    }
                    None => {
                        tracing::debug!(
                            target: "flowip-080o",
                            stage_name = %ctx.stage_name,
                            event_type = envelope.event.event_type(),
                            writer_id = ?envelope.event.writer_id,
                            upstream_readers = upstream_readers,
                            "Sink received EOF authored by a non-upstream writer; ignoring for EOF authority and continuing to consume"
                        );
                    }
                }

                return Ok(EventLoopDirective::Continue);
            }

            // Forward other control/control-like events to the sink journal.
            let _ = forward_control_event_helper(
                envelope,
                ctx.stage_id,
                &ctx.stage_name,
                StageType::Sink,
                &ctx.data_journal,
            )
            .await?;

            Ok(EventLoopDirective::Continue)
        }
        ControlAction::ForwardAndDrain => {
            // Final EOF (all authoritative upstream EOFs observed).
            drop(
                subscription
                    .check_contracts_diagnostics_only(&mut ctx.contract_state[..])
                    .await,
            );
            let _ = subscription.take_last_eof_outcome();

            if let Some(outcome) = last_eof_outcome {
                tracing::debug!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    upstream_stage_id = ?outcome.stage_id,
                    upstream_stage_name = %outcome.stage_name,
                    reader_index = outcome.reader_index,
                    eof_count = outcome.eof_count,
                    total_readers = outcome.total_readers,
                    is_final = outcome.is_final,
                    event_type = envelope.event.event_type(),
                    "Sink received EOF; evaluated drain decision"
                );
            }

            tracing::debug!(
                target: "flowip-080o",
                stage_name = %ctx.stage_name,
                "Sink EOF is final; triggering FSM transition to Drained"
            );

            Ok(EventLoopDirective::Transition(
                JournalSinkEvent::ReceivedEOF,
            ))
        }
        ControlAction::BufferAtEntryPoint { .. } | ControlAction::Suppress => {
            tracing::warn!(
                stage_name = %ctx.stage_name,
                event_type = envelope.event.event_type(),
                "Unexpected control resolution for sink; ignoring"
            );
            Ok(EventLoopDirective::Continue)
        }
        ControlAction::Skip => {
            tracing::warn!(
                stage_name = %ctx.stage_name,
                event_type = envelope.event.event_type(),
                "Sink skipping control event (dangerous!)"
            );
            Ok(EventLoopDirective::Continue)
        }
    }
}

enum SinkDispatchExecution {
    Attempted {
        outcome: SinkInvocationOutcome,
        permit: Option<Box<dyn SinkDeliveryPermit>>,
    },
    Rejected {
        rejection: SinkDeliveryRejection,
        evidence: SinkPolicyEvidenceBatch,
    },
    ProtocolFatal(StageFatal),
}

enum SinkInvocationOutcome {
    Delivered(Result<Box<SinkConsumeReport>, HandlerError>),
    Panicked,
}

enum RetainedAttemptDisposition {
    Success,
    HandlerError(HandlerError),
    OperationFailure(SinkWriteFailure),
    Panicked,
    Rejected,
}

fn write_failure_receipt_type(disposition: SinkWriteFailureDisposition) -> &'static str {
    match disposition {
        SinkWriteFailureDisposition::CurrentOnly => "sink_write_current_only_failed",
        SinkWriteFailureDisposition::ConfirmedRollback => "sink_batch_confirmed_rollback",
        SinkWriteFailureDisposition::Poisoned => "sink_materialisation_poisoned",
    }
}

async fn invoke_sink_once<H: UnifiedSinkHandler + Send + Sync>(
    handler: &mut H,
    event: ChainEvent,
    effect_context: Option<EffectInvocationContext>,
    scope: MiddlewareExecutionScope,
) -> SinkInvocationOutcome {
    let result = AssertUnwindSafe(handler.consume_report(event, effect_context, scope))
        .catch_unwind()
        .await;
    match result {
        Ok(result) => SinkInvocationOutcome::Delivered(result.map(Box::new)),
        Err(_) => SinkInvocationOutcome::Panicked,
    }
}

fn protocol_fatal(detail: impl Into<String>) -> StageFatal {
    StageFatal::new(
        StageFatalCode::Protocol,
        StageFatalReason::ProtocolInputIntegrity,
        detail,
    )
}

fn prepare_receipt_plan(
    subscription: &crate::messaging::UpstreamSubscription<ChainEvent>,
    contract_state: &[crate::messaging::upstream_subscription::ReaderProgress],
    current_envelope: &EventEnvelope<ChainEvent>,
    report: &mut SinkConsumeReport,
) -> Result<Vec<(EventEnvelope<ChainEvent>, DeliveryPayload)>, StageFatal> {
    if subscription
        .pending_receipt_envelope(current_envelope.event.id, contract_state)
        .is_none()
    {
        return Err(protocol_fatal(
            "current sink input has no exact pending receipt parent",
        ));
    }

    let primary_is_buffered = matches!(report.primary.result, DeliveryResult::Buffered { .. });
    let mut seen = HashSet::with_capacity(report.commit_receipts.len());
    let mut plan = Vec::with_capacity(report.commit_receipts.len() + 1);
    plan.push((current_envelope.clone(), report.primary.clone()));
    for commit in &report.commit_receipts {
        if !seen.insert(commit.parent_event_id) {
            return Err(protocol_fatal(
                "sink report contains a duplicate commit receipt",
            ));
        }
        if commit.parent_event_id == current_envelope.event.id && !primary_is_buffered {
            return Err(protocol_fatal(
                "terminal sink primary also returned a current-input commit receipt",
            ));
        }
        let Some((_upstream, parent)) =
            subscription.pending_receipt_envelope(commit.parent_event_id, contract_state)
        else {
            return Err(protocol_fatal(format!(
                "sink commit receipt parent {} is not pending",
                commit.parent_event_id
            )));
        };
        plan.push((parent, commit.payload.clone()));
    }
    report.commit_settlements().map_err(|error| {
        error
            .as_fatal()
            .cloned()
            .unwrap_or_else(|| protocol_fatal("sink settlement authority changed after validation"))
    })?;
    Ok(plan)
}

async fn record_protocol_fatal_and_transition<
    H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    fatal: StageFatal,
    input_position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
    parent: Option<&EventEnvelope<ChainEvent>>,
) -> Result<EventLoopDirective<JournalSinkEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let writer_id = ctx
        .writer_id
        .ok_or_else(|| "fatal sink input has no stage writer id".to_string())?;
    let recorded = record_stage_fatal(
        &fatal,
        StageFatalCommit {
            error_journal: &ctx.error_journal,
            writer_id,
            stage_id: ctx.stage_id,
            stage_key: &ctx.stage_name,
            input_position,
            parent,
            lineage: ctx.lineage_policy,
        },
    )
    .await?;
    ctx.failure_causal_event_id = Some(recorded.event.id);
    Ok(EventLoopDirective::Transition(JournalSinkEvent::Error(
        fatal.detail,
    )))
}

async fn journal_sink_operation_failure<
    H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    input: &ChainEvent,
    failed_receipt: &EventEnvelope<ChainEvent>,
    phase: SinkOperationPhase,
    error: &SinkOperationError,
    input_position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
) -> Result<EventEnvelope<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let writer_id = ctx
        .writer_id
        .unwrap_or_else(|| WriterId::from(ctx.stage_id));
    let payload = SinkOperationFailed {
        stage_id: ctx.stage_id,
        stage_key: ctx.stage_name.clone(),
        logical_destination: ctx.receipt_destination.clone(),
        causal_event_id: Some(input.id),
        input_position: input_position.map(|position| position.0),
        failed_delivery_event_id: Some(failed_receipt.event.id),
        phase,
        kind: error.kind(),
        destination_error_code: error.destination_error_code().cloned(),
        detail: error.detail(),
    };
    let mut event = ChainEventFactory::data_event(
        writer_id,
        SinkOperationFailed::versioned_event_type(),
        serde_json::to_value(payload)?,
    )
    .with_flow_context(make_flow_context(
        &ctx.flow_name,
        &ctx.flow_id.to_string(),
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Sink,
    ))
    .with_causality(CausalityContext::with_parent(failed_receipt.event.id))
    .with_correlation_from(input)
    .with_cycle_state_from(input)
    .mark_as_error(error.detail(), error.kind());
    event = event.try_with_composite_activations(input.composite_activations().to_vec())?;
    event = event.with_runtime_context(ctx.instrumentation.snapshot_with_control());
    ctx.instrumentation
        .record_error_journal_output_event(&event);
    Ok(ctx
        .error_journal
        .append(event, Some(failed_receipt))
        .await?)
}

async fn journal_fresh_error_route<
    H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    input: &ChainEvent,
    causal_parent: &EventEnvelope<ChainEvent>,
    detail: String,
    kind: ErrorKind,
) -> Result<EventEnvelope<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let ChainEventContent::Data {
        event_type,
        payload,
    } = &input.content
    else {
        return Err("sink error route requires a data input".into());
    };
    let writer_id = ctx
        .writer_id
        .unwrap_or_else(|| WriterId::from(ctx.stage_id));
    let mut event = ChainEventFactory::data_event(writer_id, event_type.clone(), payload.clone())
        .with_flow_context(make_flow_context(
            &ctx.flow_name,
            &ctx.flow_id.to_string(),
            &ctx.stage_name,
            ctx.stage_id,
            StageType::Sink,
        ))
        .with_causality(CausalityContext::with_parent(causal_parent.event.id))
        .with_correlation_from(input)
        .with_cycle_state_from(input)
        .mark_as_error(detail, kind);
    event.replay_context = input.replay_context.clone();
    event.ingress_context = input.ingress_context.clone();
    event = event.try_with_composite_activations(input.composite_activations().to_vec())?;
    event = event.with_runtime_context(ctx.instrumentation.snapshot_with_control());

    if route_to_error_journal(&event) {
        if event.is_data() {
            ctx.instrumentation
                .record_error_journal_output_event(&event);
        }
        Ok(ctx.error_journal.append(event, Some(causal_parent)).await?)
    } else {
        if event.is_data() {
            ctx.instrumentation.record_output_event(&event);
        }
        Ok(ctx.data_journal.append(event, Some(causal_parent)).await?)
    }
}

async fn journal_policy_evidence<
    H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    parent: &EventEnvelope<ChainEvent>,
    batch: SinkPolicyEvidenceBatch,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let writer_id = ctx
        .writer_id
        .unwrap_or_else(|| WriterId::from(ctx.stage_id));
    for evidence in batch.into_entries() {
        let mut event = ChainEventFactory::observability_event(
            writer_id,
            ObservabilityPayload::Middleware(evidence.into_lifecycle()),
        )
        .with_flow_context(make_flow_context(
            &ctx.flow_name,
            &ctx.flow_id.to_string(),
            &ctx.stage_name,
            ctx.stage_id,
            StageType::Sink,
        ))
        .with_causality(CausalityContext::with_parent(parent.event.id))
        .with_correlation_from(&parent.event)
        .with_cycle_state_from(&parent.event);
        event =
            event.try_with_composite_activations(parent.event.composite_activations().to_vec())?;
        event = event.with_runtime_context(ctx.instrumentation.snapshot_with_control());
        let written = ctx.data_journal.append(event, Some(parent)).await?;
        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
            &written,
            &ctx.system_journal,
        )
        .await;
    }
    Ok(())
}

async fn journal_poisoned_lifecycle<
    H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    causal_event_id: obzenflow_core::EventId,
    detail: String,
) -> Result<obzenflow_core::EventId, Box<dyn std::error::Error + Send + Sync>> {
    let event = SystemEvent::stage_failed_with_metrics_causal(
        ctx.stage_id,
        detail,
        false,
        snapshot_stage_metrics(ctx.instrumentation.as_ref()),
        causal_event_id,
    );
    let written = ctx.system_journal.append(event, None).await?;
    ctx.failure_lifecycle_recorded = true;
    ctx.failure_causal_event_id = Some(causal_event_id);
    Ok(written.event.id)
}

async fn dispatch_data_event<H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static>(
    ctx: &mut JournalSinkContext<H>,
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    envelope: &EventEnvelope<ChainEvent>,
    stage_input_position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
) -> Result<EventLoopDirective<JournalSinkEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let observer_input_position =
        stage_input_position.ok_or("sink delivered data input without StageInputPosition")?;
    let event_id = envelope.event.id;
    let upstream_stage = subscription.last_delivered_upstream_stage();
    let heartbeat_state = ctx
        .heartbeat
        .as_ref()
        .map(|heartbeat| heartbeat.state.clone());
    let effect_context = stage_input_position.and_then(|input_seq| {
        ctx.writer_id.map(|writer_id| EffectInvocationContext {
            flow_id: ctx.flow_id,
            stage_id: ctx.stage_id,
            stage_key: ctx.stage_name.clone(),
            writer_id,
            input_seq,
            lineage: ctx.lineage_policy,
            stage_logic_version: ctx.handler.stage_logic_version().to_string(),
            data_journal: ctx.data_journal.clone(),
            flow_context: None,
            observers: Some(ctx.observers.clone()),
            system_journal: None,
            instrumentation: None,
            heartbeat_state: None,
            parent: envelope.clone(),
            effect_history: ctx.effect_history.clone(),
            runtime_execution: ctx.runtime_execution.clone(),
            effect_ports: ctx.effect_ports.clone(),
            effect_declarations: ctx.effect_declarations.clone(),
            output_contract: StageOutputContract::empty(),
            backpressure_writer: BackpressureWriter::disabled(),
            emit_enabled: false,
            effect_boundary: None,
        })
    });
    let scope = ctx.runtime_execution.dispatch_scope(
        ctx.stage_id,
        stage_input_position,
        subscription.last_delivered_generation(),
    );
    let boundary = ctx.sink_delivery_boundary.clone();
    let input = envelope.event.clone();

    let execution = process_with_instrumentation(&ctx.instrumentation, || async {
        let _processing = heartbeat_state
            .as_ref()
            .map(|state| HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id));

        if !scope.is_deterministic_replay() {
            if let Some(boundary) = boundary {
                let admission = AssertUnwindSafe(boundary.admit_sink_delivery())
                    .catch_unwind()
                    .await;
                match admission {
                    Ok(SinkDeliveryAdmission::Rejected {
                        rejection,
                        evidence,
                    }) => {
                        return Ok::<_, Box<dyn std::error::Error + Send + Sync>>(
                            SinkDispatchExecution::Rejected {
                                rejection,
                                evidence,
                            },
                        );
                    }
                    Ok(SinkDeliveryAdmission::Admitted(permit)) => {
                        let outcome =
                            invoke_sink_once(&mut ctx.handler, input, effect_context, scope).await;
                        return Ok(SinkDispatchExecution::Attempted {
                            outcome,
                            permit: Some(permit),
                        });
                    }
                    Err(_) => {
                        return Ok(SinkDispatchExecution::ProtocolFatal(protocol_fatal(
                            "sink delivery admission panicked",
                        )));
                    }
                }
            }
        }

        let outcome = invoke_sink_once(&mut ctx.handler, input, effect_context, scope).await;
        Ok(SinkDispatchExecution::Attempted {
            outcome,
            permit: None,
        })
    })
    .await?;

    let (attempt_outcome, permit, rejection, mut policy_evidence) = match execution {
        SinkDispatchExecution::Attempted { outcome, permit } => {
            (Some(outcome), permit, None, SinkPolicyEvidenceBatch::new())
        }
        SinkDispatchExecution::Rejected {
            rejection,
            evidence,
        } => (None, None, Some(rejection), evidence),
        SinkDispatchExecution::ProtocolFatal(fatal) => {
            return record_protocol_fatal_and_transition(
                ctx,
                fatal,
                stage_input_position,
                Some(envelope),
            )
            .await;
        }
    };

    if let Some(SinkInvocationOutcome::Delivered(Err(error))) = &attempt_outcome {
        if let Some(fatal) = error.as_fatal() {
            return record_protocol_fatal_and_transition(
                ctx,
                fatal.clone(),
                stage_input_position,
                Some(envelope),
            )
            .await;
        }
        if error.is_contract_violation() || matches!(error, HandlerError::SinkOperation(_)) {
            return record_protocol_fatal_and_transition(
                ctx,
                protocol_fatal("invalid error authority crossed the sink write boundary"),
                stage_input_position,
                Some(envelope),
            )
            .await;
        }
    }

    let (mut report, disposition, observation_outcome) = match (attempt_outcome, &rejection) {
        (Some(SinkInvocationOutcome::Delivered(Ok(report))), None) => {
            let report = *report;
            let observation = SinkDeliveryAttemptOutcome::Delivered(Ok(Box::new(report.clone())));
            (
                report,
                RetainedAttemptDisposition::Success,
                Some(observation),
            )
        }
        (Some(SinkInvocationOutcome::Delivered(Err(HandlerError::SinkWrite(failure)))), None) => {
            let failure = (*failure).clone();
            let report = SinkConsumeReport::new(DeliveryPayload::failed(
                ctx.default_delivery_method
                    .clone()
                    .unwrap_or(DeliveryMethod::Noop),
                write_failure_receipt_type(failure.disposition()),
                failure.error().detail(),
            ));
            let observation = SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::SinkWrite(
                Box::new(failure.clone()),
            )));
            (
                report,
                RetainedAttemptDisposition::OperationFailure(failure),
                Some(observation),
            )
        }
        (Some(SinkInvocationOutcome::Delivered(Err(error))), None) => {
            let report = SinkConsumeReport::new(DeliveryPayload::failed(
                ctx.default_delivery_method
                    .clone()
                    .unwrap_or(DeliveryMethod::Noop),
                "sink_error",
                error.to_string(),
            ));
            (
                report,
                RetainedAttemptDisposition::HandlerError(error.clone()),
                Some(SinkDeliveryAttemptOutcome::Delivered(Err(error))),
            )
        }
        (Some(SinkInvocationOutcome::Panicked), None) => (
            SinkConsumeReport::new(DeliveryPayload::failed(
                ctx.default_delivery_method
                    .clone()
                    .unwrap_or(DeliveryMethod::Noop),
                "handler_panic",
                "sink connector panicked",
            )),
            RetainedAttemptDisposition::Panicked,
            Some(SinkDeliveryAttemptOutcome::Panicked {
                message: "sink connector panicked".to_string(),
            }),
        ),
        (None, Some(rejection)) => (
            SinkConsumeReport::new(
                DeliveryPayload::failed(
                    ctx.default_delivery_method
                        .clone()
                        .unwrap_or(DeliveryMethod::Noop),
                    "sink_policy_rejected",
                    format!("{}: {}", rejection.policy(), rejection.reason()),
                )
                .with_middleware_context(json!({
                    "kind": "middleware_rejection",
                    "surface": "sink_delivery",
                    "protected_unit": {
                        "stage_id": ctx.stage_id.to_string(),
                        "target": "stage"
                    },
                    "policy": rejection.policy(),
                    "reason": rejection.reason(),
                    "parent_event_id": event_id.to_string(),
                    "upstream_stage_id": upstream_stage.map(|stage_id| stage_id.to_string()),
                    "input_position": stage_input_position.map(|position| position.0)
                })),
            ),
            RetainedAttemptDisposition::Rejected,
            None,
        ),
        _ => return Err("invalid sink delivery execution state".into()),
    };

    let plan = match prepare_receipt_plan(subscription, &ctx.contract_state, envelope, &mut report)
    {
        Ok(plan) => plan,
        Err(fatal) => {
            return record_protocol_fatal_and_transition(
                ctx,
                fatal,
                stage_input_position,
                Some(envelope),
            )
            .await;
        }
    };

    let mut retained_receipts = Vec::with_capacity(plan.len());
    for (parent, payload) in plan {
        retained_receipts
            .push(journal_delivery_receipt(ctx, subscription, &parent, payload).await?);
    }
    let current_receipt = retained_receipts
        .first()
        .cloned()
        .ok_or("sink retained report had no primary receipt")?;
    let mut last_chain = retained_receipts
        .last()
        .cloned()
        .ok_or("sink retained report had no receipt")?;
    let mut poisoned_lifecycle_event_id = None;

    match &disposition {
        RetainedAttemptDisposition::HandlerError(error) => {
            ctx.instrumentation.record_error(error.kind());
            last_chain = journal_fresh_error_route(
                ctx,
                &envelope.event,
                &current_receipt,
                error.to_string(),
                error.kind(),
            )
            .await?;
        }
        RetainedAttemptDisposition::OperationFailure(failure) => {
            let operation = journal_sink_operation_failure(
                ctx,
                &envelope.event,
                &current_receipt,
                SinkOperationPhase::Write(failure.phase()),
                failure.error(),
                stage_input_position,
            )
            .await?;
            ctx.instrumentation.record_error(failure.error().kind());
            last_chain = journal_fresh_error_route(
                ctx,
                &envelope.event,
                &operation,
                failure.error().detail(),
                failure.error().kind(),
            )
            .await?;
            if failure.disposition() == SinkWriteFailureDisposition::Poisoned {
                poisoned_lifecycle_event_id = Some(
                    journal_poisoned_lifecycle(ctx, last_chain.event.id, failure.error().detail())
                        .await?,
                );
            }
        }
        RetainedAttemptDisposition::Success
        | RetainedAttemptDisposition::Panicked
        | RetainedAttemptDisposition::Rejected => {}
    }

    if ctx.observers.has_sink_delivery() && !scope.is_deterministic_replay() {
        let observer_outcome = match (&observation_outcome, &rejection) {
            (Some(SinkDeliveryAttemptOutcome::Delivered(Ok(report))), None) => {
                SinkDeliveryObserverOutcome::Attempted {
                    result: match &report.primary.result {
                        DeliveryResult::Success { .. } => {
                            SinkDeliveryAttemptResult::ReportedSuccess
                        }
                        DeliveryResult::Partial {
                            successful_count,
                            failed_count,
                            ..
                        } => SinkDeliveryAttemptResult::ReportedPartial {
                            successful_count: *successful_count,
                            failed_count: *failed_count,
                        },
                        DeliveryResult::Buffered { .. } => {
                            SinkDeliveryAttemptResult::ReportedBuffered
                        }
                        DeliveryResult::Failed { .. } => SinkDeliveryAttemptResult::ReportedFailure,
                    },
                }
            }
            (Some(SinkDeliveryAttemptOutcome::Delivered(Err(error))), None) => {
                SinkDeliveryObserverOutcome::Attempted {
                    result: SinkDeliveryAttemptResult::HandlerError { kind: error.kind() },
                }
            }
            (Some(SinkDeliveryAttemptOutcome::Panicked { .. }), None) => {
                SinkDeliveryObserverOutcome::Attempted {
                    result: SinkDeliveryAttemptResult::HandlerPanicked,
                }
            }
            (None, Some(rejection)) => SinkDeliveryObserverOutcome::Rejected {
                policy: Some(rejection.policy().to_string()),
            },
            _ => return Err("invalid sink observer state".into()),
        };
        let flow_context = make_flow_context(
            &ctx.flow_name,
            &ctx.flow_id.to_string(),
            &ctx.stage_name,
            ctx.stage_id,
            StageType::Sink,
        );
        run_sink_delivery_observers(
            &ctx.observers,
            ctx.flow_id,
            &flow_context,
            scope,
            &envelope.event,
            observer_input_position,
            observer_outcome,
        );
    }

    if let (Some(permit), Some(outcome)) = (permit, observation_outcome.as_ref()) {
        match std::panic::catch_unwind(AssertUnwindSafe(|| permit.observe(outcome))) {
            Ok(evidence) => policy_evidence = evidence,
            Err(_) => {
                let mut fatal = protocol_fatal("sink delivery observation panicked");
                if let Some(primary) = poisoned_lifecycle_event_id {
                    fatal = fatal.secondary_to(primary);
                }
                if let Some(state) = &heartbeat_state {
                    state.record_last_consumed(event_id);
                }
                return record_protocol_fatal_and_transition(
                    ctx,
                    fatal,
                    stage_input_position,
                    Some(&last_chain),
                )
                .await;
            }
        }
    }

    journal_policy_evidence(ctx, envelope, policy_evidence).await?;
    if let Some(state) = &heartbeat_state {
        state.record_last_consumed(event_id);
    }

    match disposition {
        RetainedAttemptDisposition::Panicked => {
            record_protocol_fatal_and_transition(
                ctx,
                protocol_fatal("sink connector panicked"),
                stage_input_position,
                Some(&last_chain),
            )
            .await
        }
        RetainedAttemptDisposition::OperationFailure(failure)
            if failure.disposition() == SinkWriteFailureDisposition::Poisoned =>
        {
            Ok(EventLoopDirective::Transition(JournalSinkEvent::Error(
                failure.error().detail(),
            )))
        }
        RetainedAttemptDisposition::Success
        | RetainedAttemptDisposition::HandlerError(_)
        | RetainedAttemptDisposition::OperationFailure(_)
        | RetainedAttemptDisposition::Rejected => Ok(EventLoopDirective::Continue),
    }
}

async fn journal_delivery_receipt<
    H: UnifiedSinkHandler + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    parent_envelope: &EventEnvelope<ChainEvent>,
    payload: DeliveryPayload,
) -> Result<EventEnvelope<ChainEvent>, Box<dyn std::error::Error + Send + Sync>> {
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Sink,
    );

    let writer_id = WriterId::from(ctx.stage_id);
    let delivery_event = journalled_delivery_event(writer_id, &ctx.receipt_destination, payload)
        .with_flow_context(flow_context)
        .with_causality(CausalityContext::with_parent(parent_envelope.event.id))
        .with_correlation_from(&parent_envelope.event)
        .with_cycle_state_from(&parent_envelope.event);
    let delivery_event = delivery_event
        .try_with_composite_activations(parent_envelope.event.composite_activations().to_vec())?;

    if delivery_event.is_data() || delivery_event.is_delivery() {
        ctx.instrumentation.record_output_event(&delivery_event);
    }

    let delivery_event =
        delivery_event.with_runtime_context(ctx.instrumentation.snapshot_with_control());
    let written = ctx
        .data_journal
        .append(delivery_event, Some(parent_envelope))
        .await?;
    crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
        &written,
        &ctx.system_journal,
    )
    .await;

    if let Some((seq, event_id, vector_clock)) =
        subscription.record_delivery_receipt(&written.event, &mut ctx.contract_state[..])
    {
        ctx.instrumentation
            .record_receipted_position(seq.0, event_id, vector_clock);
    }

    Ok(written)
}
