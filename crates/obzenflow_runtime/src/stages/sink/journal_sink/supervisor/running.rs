// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Running-state dispatch loop for the journal sink supervisor.

use crate::backpressure::BackpressureWriter;
use crate::effects::EffectInvocationContext;
use crate::feed_plan::StageOutputContract;
use crate::messaging::PollResult;
use crate::metrics::instrumentation::process_with_instrumentation;
use crate::stages::common::handlers::UnifiedSinkHandler;
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
use crate::stages::observer::dispatch::run_sink_delivery_observers;
use crate::stages::observer::SinkDeliveryObserverOutcome;
use crate::supervised_base::EventLoopDirective;
use futures::FutureExt;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventFactory, EventEnvelope, JournalEvent};
use obzenflow_core::ChainEvent;
use obzenflow_core::WriterId;
use obzenflow_fsm::StateVariant;
use std::panic::AssertUnwindSafe;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::super::boundary::{
    SinkDeliveryAttemptOutcome, SinkDeliveryBoundaryOutcome, SinkDeliveryExecutor,
};
use super::super::fsm::{JournalSinkContext, JournalSinkEvent, JournalSinkState};
use super::JournalSinkSupervisor;
use obzenflow_core::MiddlewareExecutionScope;
use serde_json::json;

pub(super) async fn dispatch_running<
    H: UnifiedSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
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
            ctx.instrumentation.record_consumed(&envelope);
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

async fn dispatch_event<H: UnifiedSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
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
            // For other content types, just consume without instrumentation.
            let envelope_event = envelope.event.clone();
            let event_id = envelope_event.id;
            let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());
            if let Err(e) = ctx
                .handler
                .consume_report(
                    envelope_event,
                    None,
                    ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        None,
                        subscription.last_delivered_generation(),
                    ),
                )
                .await
            {
                tracing::error!(
                    stage_name = %ctx.stage_name,
                    error = ?e,
                    "Failed to consume control/system event"
                );
            }
            if let Some(state) = &heartbeat_state {
                state.record_last_consumed(event_id);
            }
            Ok(EventLoopDirective::Continue)
        }
    }
}

async fn dispatch_control_event<
    H: UnifiedSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
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

            // For non-EOF control events, let handler consume if needed.
            let envelope_event = envelope.event.clone();
            if let Err(e) = ctx
                .handler
                .consume_report(
                    envelope_event,
                    None,
                    ctx.runtime_execution.dispatch_scope(
                        ctx.stage_id,
                        None,
                        subscription.last_delivered_generation(),
                    ),
                )
                .await
            {
                tracing::error!(
                    stage_name = %ctx.stage_name,
                    error = ?e,
                    "Failed to consume control event"
                );
            }
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

/// FLOWIP-115b: a re-invokable executor wrapping one data-event `consume_report`
/// attempt for the sink-delivery boundary. 115B calls `attempt` once; the
/// re-invokable shape lets FLOWIP-115h reintroduce boundary-owned retry.
struct ConsumeExecutor<'h, H> {
    handler: &'h mut H,
    event: Option<ChainEvent>,
    effect_context: Option<EffectInvocationContext>,
    scope: MiddlewareExecutionScope,
}

#[async_trait::async_trait]
impl<H: UnifiedSinkHandler + Send + Sync> SinkDeliveryExecutor for ConsumeExecutor<'_, H> {
    async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
        let event = self
            .event
            .take()
            .expect("sink delivery executor attempted more than once");
        let effect_context = self.effect_context.take();
        let result = AssertUnwindSafe(self.handler.consume_report(
            event,
            effect_context,
            self.scope,
        ))
        .catch_unwind()
        .await;
        match result {
            Ok(inner) => SinkDeliveryAttemptOutcome::Delivered(inner.map(Box::new)),
            Err(panic_payload) => {
                let message = panic_payload
                    .downcast_ref::<&str>()
                    .map(|s| (*s).to_string())
                    .or_else(|| panic_payload.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic payload".to_string());
                SinkDeliveryAttemptOutcome::Panicked { message }
            }
        }
    }
}

async fn dispatch_data_event<
    H: UnifiedSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    envelope: &EventEnvelope<ChainEvent>,
    stage_input_position: Option<crate::messaging::upstream_subscription::StageInputPosition>,
) -> Result<EventLoopDirective<JournalSinkEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    let envelope_event = envelope.event.clone();
    let event_id = envelope_event.id;
    let stage_name = ctx.stage_name.clone();
    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());
    let upstream_stage = subscription.last_delivered_upstream_stage();
    let effect_context = stage_input_position.and_then(|input_seq| {
        ctx.writer_id.map(|writer_id| EffectInvocationContext {
            flow_id: ctx.flow_id,
            stage_id: ctx.stage_id,
            stage_key: ctx.stage_name.clone(),
            writer_id,
            input_seq,
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
            synthesized_outcomes: Vec::new(),
            output_contract: StageOutputContract::empty(),
            backpressure_writer: BackpressureWriter::disabled(),
            emit_enabled: false,
            effect_boundary: None,
            boundary_control_events: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    });

    // FLOWIP-120c H3: per-event middleware execution scope, computed at
    // dispatch from the delivered position and generation.
    let scope = ctx.runtime_execution.dispatch_scope(
        ctx.stage_id,
        stage_input_position,
        subscription.last_delivered_generation(),
    );

    // FLOWIP-115b: the sink-delivery boundary wraps the data-event consume
    // attempt. Pre-extract the boundary so the closure borrows only
    // `ctx.handler` mutably, disjoint from `&ctx.instrumentation`.
    let sink_boundary = ctx.sink_delivery_boundary.clone();

    // Use instrumentation wrapper but keep handler-level failures as per-record
    // outcomes instead of stage-fatal errors.
    let ack_result = process_with_instrumentation(&ctx.instrumentation, || async {
        let _processing = heartbeat_state
            .as_ref()
            .map(|state| HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id));

        let mut executor = ConsumeExecutor {
            handler: &mut ctx.handler,
            event: Some(envelope_event),
            effect_context,
            scope,
        };

        // FLOWIP-115b AC48: during deterministic replay/resume reconstruction the
        // sink-delivery boundary is bypassed entirely, so the circuit-breaker sink
        // policy acquires no probe, transitions no state, and emits no fresh
        // lifecycle/summary rows. This mirrors the structural replay bypass the
        // source (ReplayDriver branch) and effect (recorded-history early return)
        // paths already have; the sink is the only live-I/O unit that re-consumes
        // its tape during replay, so it needs the explicit scope gate. The consume
        // executor still runs, so the delivery receipt is reconstructed normally.
        // FLOWIP-120n owns the future resume phase predicate that will split a
        // replayed prefix from a live tail by `StageInputPosition`. Until then,
        // `ResumeIncomplete` is treated as deterministic reconstruction here.
        let (outcome, control_events) = if scope.is_deterministic_replay() {
            (
                SinkDeliveryBoundaryOutcome::Attempted(executor.attempt().await),
                Vec::new(),
            )
        } else if let Some(boundary) = &sink_boundary {
            let report = boundary.around_sink_delivery(&mut executor).await;
            (report.outcome, report.control_events)
        } else {
            (
                SinkDeliveryBoundaryOutcome::Attempted(executor.attempt().await),
                Vec::new(),
            )
        };

        let observer_outcome = match &outcome {
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(
                _,
            ))) => SinkDeliveryObserverOutcome::Delivered,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Err(
                err,
            ))) => SinkDeliveryObserverOutcome::Failed {
                message: err.to_string(),
            },
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Panicked {
                message,
            }) => SinkDeliveryObserverOutcome::Failed {
                message: message.clone(),
            },
            SinkDeliveryBoundaryOutcome::Rejected(rejection) => {
                SinkDeliveryObserverOutcome::Rejected {
                    reason: format!("{}: {}", rejection.policy, rejection.reason),
                }
            }
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
            ctx.stage_id,
            &ctx.stage_name,
            &flow_context,
            scope,
            &envelope.event,
            stage_input_position.map(|position| position.0),
            observer_outcome,
            &ctx.data_journal,
            &ctx.instrumentation,
            envelope,
        )
        .await?;

        let mapped = match outcome {
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(
                report,
            ))) => {
                let mut report = *report;
                report.primary.destination = stage_name.clone();
                for commit in &mut report.commit_receipts {
                    commit.payload.destination = stage_name.clone();
                }
                (report, None, false)
            }
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Err(
                err,
            ))) => {
                let fail_payload = DeliveryPayload::failed(
                    stage_name.clone(),
                    DeliveryMethod::Noop,
                    "sink_error",
                    err.to_string(),
                    /* final_attempt */ false,
                );
                (
                    crate::stages::common::handlers::SinkConsumeReport::new(fail_payload),
                    Some(err),
                    false,
                )
            }
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Panicked {
                message,
            }) => {
                tracing::error!(
                    stage_name = %stage_name,
                    panic = %message,
                    "SinkHandler::consume() panicked"
                );
                let fail_payload = DeliveryPayload::failed(
                    stage_name.clone(),
                    DeliveryMethod::Noop,
                    "handler_panic",
                    message,
                    /* final_attempt */ true,
                );
                (
                    crate::stages::common::handlers::SinkConsumeReport::new(fail_payload),
                    None,
                    true,
                )
            }
            SinkDeliveryBoundaryOutcome::Rejected(rejection) => {
                // FLOWIP-115b AC16: a policy rejection is a failed delivery
                // receipt with structured metadata distinct from handler errors
                // and panics, never a successful `Noop`. It is not routed as a
                // handler error and is not stage-fatal.
                tracing::info!(
                    stage_name = %stage_name,
                    policy = %rejection.policy,
                    reason = %rejection.reason,
                    "Sink delivery rejected by policy (FLOWIP-115b)"
                );
                let fail_payload = DeliveryPayload::failed(
                    stage_name.clone(),
                    DeliveryMethod::Noop,
                    "sink_policy_rejected",
                    format!("{}: {}", rejection.policy, rejection.reason),
                    /* final_attempt */ false,
                )
                .with_middleware_context(json!({
                    "kind": "middleware_rejection",
                    "surface": "sink_delivery",
                    "protected_unit": {
                        "stage_id": ctx.stage_id.to_string(),
                        "target": "stage"
                    },
                    "policy": rejection.policy,
                    "reason": rejection.reason,
                    "parent_event_id": event_id.to_string(),
                    "upstream_stage_id": upstream_stage.map(|stage_id| stage_id.to_string()),
                    "input_position": stage_input_position.map(|position| position.0)
                }));
                (
                    crate::stages::common::handlers::SinkConsumeReport::new(fail_payload),
                    None,
                    false,
                )
            }
        };

        if let Some(state) = &heartbeat_state {
            state.record_last_consumed(event_id);
        }

        let (report, maybe_err, panicked) = mapped;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
            report,
            maybe_err,
            panicked,
            control_events,
        ))
    })
    .await;

    match ack_result {
        Ok((report, maybe_err, panicked, control_events)) => {
            journal_delivery_receipt(ctx, subscription, envelope, report.primary).await?;

            for commit in report.commit_receipts {
                if let Some((_upstream_stage, parent_envelope)) = subscription
                    .pending_receipt_envelope(commit.parent_event_id, &ctx.contract_state[..])
                {
                    journal_delivery_receipt(ctx, subscription, &parent_envelope, commit.payload)
                        .await?;
                } else {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        parent_event_id = %commit.parent_event_id,
                        "Skipping commit receipt with no pending parent metadata"
                    );
                }
            }

            // FLOWIP-115b: sink-policy observability/control rows are journalled
            // but do not advance receipt progress (AC17).
            for control_event in control_events {
                ctx.data_journal
                    .append(control_event, Some(envelope))
                    .await
                    .map_err(|je| format!("Failed to journal sink boundary control event: {je}"))?;
            }

            // Per-record handler errors are not stage-fatal. Surface them as
            // error-marked events, routed by ErrorKind policy.
            if let Some(handler_err) = maybe_err {
                ctx.instrumentation.record_error(handler_err.kind());
                let reason = format!("Sink handler error: {handler_err:?}");
                let error_event = envelope
                    .event
                    .clone()
                    .mark_as_error(reason, handler_err.kind());

                if route_to_error_journal(&error_event) {
                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        event_id = %error_event.id,
                        "Writing sink error event to error journal (FLOWIP-082h)"
                    );

                    if error_event.is_data() {
                        ctx.instrumentation.record_output_event(&error_event);
                    }

                    ctx.error_journal
                        .append(error_event, Some(envelope))
                        .await
                        .map_err(|je| format!("Failed to journal sink error event: {je}"))?;
                } else {
                    let flow_id = ctx.flow_id.to_string();
                    let flow_ctx = make_flow_context(
                        &ctx.flow_name,
                        &flow_id,
                        &ctx.stage_name,
                        ctx.stage_id,
                        StageType::Sink,
                    );

                    let enriched_error = error_event.with_flow_context(flow_ctx);
                    if enriched_error.is_data() {
                        ctx.instrumentation.record_output_event(&enriched_error);
                    }
                    let enriched_error = enriched_error
                        .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                    ctx.data_journal
                        .append(enriched_error, Some(envelope))
                        .await
                        .map_err(|je| {
                            format!("Failed to write sink error event to data journal: {je}")
                        })?;
                }
            }

            if panicked {
                Err("SinkHandler::consume() panicked".into())
            } else {
                Ok(EventLoopDirective::Continue)
            }
        }
        Err(e) => {
            // Instrumentation-level or unexpected failure: treat as stage-fatal.
            let fail_payload = DeliveryPayload::failed(
                ctx.stage_name.clone(),
                DeliveryMethod::Noop,
                "sink_error",
                e.to_string(),
                /* final_attempt */ false,
            );
            journal_delivery_receipt(ctx, subscription, envelope, fail_payload)
                .await
                .map_err(|je| format!("Failed to journal sink failure: {je}"))?;

            Err(format!("Sink consume failed: {e}").into())
        }
    }
}

async fn journal_delivery_receipt<
    H: UnifiedSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &mut JournalSinkContext<H>,
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    parent_envelope: &EventEnvelope<ChainEvent>,
    payload: DeliveryPayload,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let flow_id = ctx.flow_id.to_string();
    let flow_context = make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Sink,
    );

    let writer_id = WriterId::from(ctx.stage_id);
    let delivery_event = ChainEventFactory::delivery_event(writer_id, payload)
        .with_flow_context(flow_context)
        .with_causality(CausalityContext::with_parent(parent_envelope.event.id))
        .with_correlation_from(&parent_envelope.event);

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

    Ok(())
}
