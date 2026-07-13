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
use crate::stages::common::BoundaryStopReceiver;
use crate::stages::observer::dispatch::run_sink_delivery_observers;
use crate::stages::observer::SinkDeliveryObserverOutcome;
use crate::supervised_base::EventLoopDirective;
use futures::FutureExt;
use obzenflow_core::event::context::causality_context::CausalityContext;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::{
    DeliveryMethod, DeliveryPayload, DeliveryResult,
};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEventFactory, EventEnvelope, JournalEvent};
use obzenflow_core::ChainEvent;
use obzenflow_core::WriterId;
use obzenflow_fsm::StateVariant;
use std::panic::AssertUnwindSafe;
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;

use super::super::boundary::{
    SinkDeliveryAttemptOutcome, SinkDeliveryBoundary, SinkDeliveryBoundaryOutcome,
    SinkDeliveryBoundaryReport, SinkDeliveryExecutor,
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
            let delivered_upstream_stage = subscription
                .last_delivered_upstream_stage()
                .expect("delivered event must identify its upstream stage");
            ctx.instrumentation
                .record_consumed(&envelope, delivered_upstream_stage);
            ctx.instrumentation
                .event_loops_with_work_total
                .fetch_add(1, Ordering::Relaxed);

            let stage_input_position = subscription.last_delivered_stage_input_position();
            let directive =
                dispatch_event(ctx, subscription, &envelope, stage_input_position).await?;
            let received_eof = matches!(
                directive,
                EventLoopDirective::Transition(JournalSinkEvent::ReceivedEOF)
            );

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
    event: ChainEvent,
    effect_context: Option<EffectInvocationContext>,
    scope: MiddlewareExecutionScope,
}

#[async_trait::async_trait]
impl<H: UnifiedSinkHandler + Send + Sync> SinkDeliveryExecutor for ConsumeExecutor<'_, H> {
    fn parent_event_id(&self) -> obzenflow_core::EventId {
        *self.event.id()
    }

    async fn attempt(&mut self) -> SinkDeliveryAttemptOutcome {
        // A boundary retry is a fresh physical future over the same logical
        // delivery identity. The handler receives owned values, so clone the
        // immutable invocation seed for every call rather than consuming it on
        // the first attempt.
        let event = self.event.clone();
        let effect_context = self.effect_context.clone();
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

async fn execute_sink_delivery(
    scope: MiddlewareExecutionScope,
    boundary: Option<&Arc<dyn SinkDeliveryBoundary>>,
    stop: BoundaryStopReceiver,
    executor: &mut dyn SinkDeliveryExecutor,
) -> SinkDeliveryBoundaryReport {
    // Reconstruction preserves the existing one-call sink contract while
    // structurally suppressing all live policy and retry machinery.
    if scope.is_deterministic_replay() {
        return SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Attempted(executor.attempt().await),
            control_events: Vec::new(),
        };
    }

    match boundary {
        Some(boundary) => {
            boundary
                .around_retryable_sink_delivery(executor, stop)
                .await
        }
        None => SinkDeliveryBoundaryReport {
            outcome: SinkDeliveryBoundaryOutcome::Attempted(executor.attempt().await),
            control_events: Vec::new(),
        },
    }
}

fn deadline_outcome_unknown_report(
    message: String,
    receipt_destination: &str,
) -> crate::stages::common::handlers::SinkConsumeReport {
    let mut payload = DeliveryPayload::failed(
        DeliveryMethod::Noop,
        "sink_error",
        message,
        /* final_attempt */ true,
    );
    if let DeliveryResult::Failed { error_code, .. } = &mut payload.result {
        *error_code = Some("deadline_outcome_unknown".to_string());
    }
    payload.destination = receipt_destination.to_string();
    crate::stages::common::handlers::SinkConsumeReport::new(payload)
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
    // FLOWIP-120s single-writer receipt identity, resolved at build.
    let receipt_destination = ctx.receipt_destination.clone();
    let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());
    let upstream_stage = subscription.last_delivered_upstream_stage();
    let boundary_stop = ctx.boundary_stop.clone();
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
            synthesized_outcomes: Vec::new(),
            output_contract: StageOutputContract::empty(),
            backpressure_writer: BackpressureWriter::disabled(),
            emit_enabled: false,
            effect_boundary: None,
            boundary_stop: boundary_stop.clone(),
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
    let delivery_result = process_with_instrumentation(&ctx.instrumentation, || async {
        let _processing = heartbeat_state
            .as_ref()
            .map(|state| HeartbeatProcessingGuard::new(state.clone(), upstream_stage, event_id));

        let mut executor = ConsumeExecutor {
            handler: &mut ctx.handler,
            event: envelope_event,
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
        let boundary_report =
            execute_sink_delivery(scope, sink_boundary.as_ref(), boundary_stop, &mut executor)
                .await;
        let SinkDeliveryBoundaryReport {
            outcome,
            control_events,
        } = boundary_report;

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
            SinkDeliveryBoundaryOutcome::DeadlineOutcomeUnknown { message } => {
                SinkDeliveryObserverOutcome::Failed {
                    message: message.clone(),
                }
            }
        };

        let mapped = match outcome {
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(
                report,
            ))) => {
                let mut report = *report;
                report.primary.destination = receipt_destination.clone();
                for commit in &mut report.commit_receipts {
                    commit.payload.destination = receipt_destination.clone();
                }
                (report, None, false)
            }
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Err(
                err,
            ))) => {
                let mut fail_payload = DeliveryPayload::failed(
                    DeliveryMethod::Noop,
                    "sink_error",
                    err.to_string(),
                    /* final_attempt */ false,
                );
                fail_payload.destination = receipt_destination.clone();
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
                let mut fail_payload = DeliveryPayload::failed(
                    DeliveryMethod::Noop,
                    "handler_panic",
                    message,
                    /* final_attempt */ true,
                );
                fail_payload.destination = receipt_destination.clone();
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
                let mut fail_payload = DeliveryPayload::failed(
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
                fail_payload.destination = receipt_destination.clone();
                (
                    crate::stages::common::handlers::SinkConsumeReport::new(fail_payload),
                    None,
                    false,
                )
            }
            SinkDeliveryBoundaryOutcome::DeadlineOutcomeUnknown { message } => {
                tracing::warn!(
                    stage_name = %stage_name,
                    parent_event_id = %event_id,
                    "Sink delivery deadline elapsed with external completion in doubt"
                );
                (
                    deadline_outcome_unknown_report(message, &receipt_destination),
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
            observer_outcome,
            control_events,
        ))
    })
    .await;

    match delivery_result {
        Ok((report, maybe_err, panicked, observer_outcome, control_events)) => {
            // The primary and any commit receipts are authoritative terminal
            // truth. No retry attempt is visible here: a retry-capable boundary
            // has already reduced all physical calls to this one report.
            journal_delivery_receipt(ctx, subscription, envelope, report.primary).await?;

            // The current input's authoritative terminal receipt is committed.
            // Release its one held credit before commit receipts for older
            // buffered inputs or any non-authoritative evidence can fail.
            // Physical retry attempts never reach this supervisor and
            // therefore cannot acknowledge independently.
            acknowledge_sink_input(ctx, upstream_stage);

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

            // Observer diagnostics and retry/policy rows are non-authoritative
            // evidence. They run only after terminal receipts commit. A failed
            // evidence append is visible to operators, but must not turn a
            // committed external delivery back into an input redelivery.
            for control_event in control_events {
                if let Err(error) = ctx.data_journal.append(control_event, Some(envelope)).await {
                    tracing::error!(
                        stage_name = %ctx.stage_name,
                        parent_event_id = %event_id,
                        error = %error,
                        "Failed to append sink boundary evidence after terminal receipt; delivery remains committed"
                    );
                    // Preserve the occurrence-ordered outbox as a durable
                    // prefix. Appending later rows after a missing earlier row
                    // would manufacture a misleadingly complete trace.
                    break;
                }
            }

            let flow_context = make_flow_context(
                &ctx.flow_name,
                &ctx.flow_id.to_string(),
                &ctx.stage_name,
                ctx.stage_id,
                StageType::Sink,
            );
            if let Err(error) = run_sink_delivery_observers(
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
            .await
            {
                tracing::error!(
                    stage_name = %ctx.stage_name,
                    parent_event_id = %event_id,
                    error = %error,
                    "Failed to append sink observer evidence after terminal receipt; delivery remains committed"
                );
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
                        ctx.instrumentation
                            .record_error_journal_output_event(&error_event);
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
            let mut fail_payload = DeliveryPayload::failed(
                DeliveryMethod::Noop,
                "sink_error",
                e.to_string(),
                /* final_attempt */ false,
            );
            fail_payload.destination = ctx.receipt_destination.clone();
            journal_delivery_receipt(ctx, subscription, envelope, fail_payload)
                .await
                .map_err(|je| format!("Failed to journal sink failure: {je}"))?;

            acknowledge_sink_input(ctx, upstream_stage);

            Err(format!("Sink consume failed: {e}").into())
        }
    }
}

fn acknowledge_sink_input<
    H: UnifiedSinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    ctx: &JournalSinkContext<H>,
    upstream_stage: Option<obzenflow_core::StageId>,
) {
    if let Some(reader) =
        upstream_stage.and_then(|stage_id| ctx.backpressure_readers.get(&stage_id))
    {
        reader.ack_consumed(1);
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

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stages::common::handler_error::HandlerError;
    use crate::stages::common::handlers::{SinkConsumeReport, SinkLifecycleReport};
    use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
    use obzenflow_core::{EventId, StageId};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Mutex;

    #[derive(Clone, Debug)]
    struct ScriptedSink {
        calls: Arc<Mutex<Vec<EventId>>>,
        results: Arc<Mutex<VecDeque<Result<SinkConsumeReport, HandlerError>>>>,
    }

    #[async_trait::async_trait]
    impl UnifiedSinkHandler for ScriptedSink {
        async fn consume_report(
            &mut self,
            event: ChainEvent,
            _effect_context: Option<EffectInvocationContext>,
            _scope: MiddlewareExecutionScope,
        ) -> Result<SinkConsumeReport, HandlerError> {
            self.calls
                .lock()
                .expect("calls lock poisoned")
                .push(event.id);
            self.results
                .lock()
                .expect("results lock poisoned")
                .pop_front()
                .expect("scripted sink exhausted")
        }

        async fn flush_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
            Ok(SinkLifecycleReport::default())
        }

        async fn drain_report(&mut self) -> Result<SinkLifecycleReport, HandlerError> {
            Ok(SinkLifecycleReport::default())
        }
    }

    struct RetryOnceBoundary {
        calls: Arc<AtomicUsize>,
    }

    struct StopAwareBoundary {
        observed: Arc<Mutex<Vec<crate::stages::common::BoundaryStopIntent>>>,
    }

    #[async_trait::async_trait]
    impl SinkDeliveryBoundary for RetryOnceBoundary {
        async fn around_sink_delivery(
            &self,
            execute: &mut dyn SinkDeliveryExecutor,
        ) -> SinkDeliveryBoundaryReport {
            self.calls.fetch_add(1, Ordering::SeqCst);
            assert!(matches!(
                execute.attempt().await,
                SinkDeliveryAttemptOutcome::Delivered(Err(HandlerError::Remote(_)))
            ));
            SinkDeliveryBoundaryReport {
                outcome: SinkDeliveryBoundaryOutcome::Attempted(execute.attempt().await),
                control_events: Vec::new(),
            }
        }
    }

    #[async_trait::async_trait]
    impl SinkDeliveryBoundary for StopAwareBoundary {
        async fn around_sink_delivery(
            &self,
            _execute: &mut dyn SinkDeliveryExecutor,
        ) -> SinkDeliveryBoundaryReport {
            panic!("retry-aware sink entry point must be used for live delivery")
        }

        async fn around_retryable_sink_delivery(
            &self,
            execute: &mut dyn SinkDeliveryExecutor,
            stop: BoundaryStopReceiver,
        ) -> SinkDeliveryBoundaryReport {
            self.observed
                .lock()
                .expect("observed lock poisoned")
                .push(stop.intent());
            SinkDeliveryBoundaryReport {
                outcome: SinkDeliveryBoundaryOutcome::Attempted(execute.attempt().await),
                control_events: Vec::new(),
            }
        }
    }

    fn success_report() -> SinkConsumeReport {
        SinkConsumeReport::new(DeliveryPayload::success(DeliveryMethod::Noop, None))
    }

    fn input_event() -> ChainEvent {
        ChainEventFactory::data_event(
            WriterId::from(StageId::new()),
            "test.sink.retry_input",
            json!({ "value": 1 }),
        )
    }

    #[tokio::test]
    async fn live_boundary_can_reinvoke_the_same_logical_delivery() {
        let event = input_event();
        let event_id = event.id;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut handler = ScriptedSink {
            calls: calls.clone(),
            results: Arc::new(Mutex::new(VecDeque::from([
                Err(HandlerError::Remote("retry me".to_string())),
                Ok(success_report()),
            ]))),
        };
        let mut executor = ConsumeExecutor {
            handler: &mut handler,
            event,
            effect_context: None,
            scope: MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
        };
        let boundary_calls = Arc::new(AtomicUsize::new(0));
        let boundary: Arc<dyn SinkDeliveryBoundary> = Arc::new(RetryOnceBoundary {
            calls: boundary_calls.clone(),
        });

        let report = execute_sink_delivery(
            MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
            Some(&boundary),
            BoundaryStopReceiver::default(),
            &mut executor,
        )
        .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
        ));
        assert_eq!(boundary_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            calls.lock().expect("calls lock poisoned").as_slice(),
            &[event_id, event_id],
            "every physical attempt must retain the same parent identity"
        );
    }

    #[tokio::test]
    async fn deterministic_replay_bypasses_boundary_and_executes_once() {
        let event = input_event();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let mut handler = ScriptedSink {
            calls: calls.clone(),
            results: Arc::new(Mutex::new(VecDeque::from([Ok(success_report())]))),
        };
        let mut executor = ConsumeExecutor {
            handler: &mut handler,
            event,
            effect_context: None,
            scope: MiddlewareExecutionScope::StrictReplayHandler,
        };
        let boundary_calls = Arc::new(AtomicUsize::new(0));
        let boundary: Arc<dyn SinkDeliveryBoundary> = Arc::new(RetryOnceBoundary {
            calls: boundary_calls.clone(),
        });

        let report = execute_sink_delivery(
            MiddlewareExecutionScope::StrictReplayHandler,
            Some(&boundary),
            BoundaryStopReceiver::default(),
            &mut executor,
        )
        .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
        ));
        assert_eq!(boundary_calls.load(Ordering::SeqCst), 0);
        assert_eq!(calls.lock().expect("calls lock poisoned").len(), 1);
        assert!(report.control_events.is_empty());
    }

    #[tokio::test]
    async fn live_boundary_receives_runtime_stop_intent() {
        let event = input_event();
        let mut handler = ScriptedSink {
            calls: Arc::new(Mutex::new(Vec::new())),
            results: Arc::new(Mutex::new(VecDeque::from([Ok(success_report())]))),
        };
        let mut executor = ConsumeExecutor {
            handler: &mut handler,
            event,
            effect_context: None,
            scope: MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
        };
        let observed = Arc::new(Mutex::new(Vec::new()));
        let boundary: Arc<dyn SinkDeliveryBoundary> = Arc::new(StopAwareBoundary {
            observed: observed.clone(),
        });
        let (controller, stop) = crate::stages::common::boundary_stop_channel();
        controller.request_drain();

        let report = execute_sink_delivery(
            MiddlewareExecutionScope::LiveSinkDeliveryBoundary,
            Some(&boundary),
            stop,
            &mut executor,
        )
        .await;

        assert!(matches!(
            report.outcome,
            SinkDeliveryBoundaryOutcome::Attempted(SinkDeliveryAttemptOutcome::Delivered(Ok(_)))
        ));
        assert_eq!(
            observed.lock().expect("observed lock poisoned").as_slice(),
            &[crate::stages::common::BoundaryStopIntent::Drain]
        );
    }

    #[test]
    fn deadline_outcome_unknown_maps_to_an_in_doubt_terminal_receipt() {
        let report = deadline_outcome_unknown_report(
            "deadline elapsed while delivery was active".to_string(),
            "payments",
        );

        assert_eq!(report.primary.destination, "payments");
        match report.primary.result {
            DeliveryResult::Failed {
                error_code,
                final_attempt,
                ..
            } => {
                assert_eq!(error_code.as_deref(), Some("deadline_outcome_unknown"));
                assert!(final_attempt);
            }
            other => panic!("expected failed delivery receipt, got {other:?}"),
        }
    }
}
