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
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::ChainEvent;
use obzenflow_fsm::StateVariant;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::messaging::upstream_subscription::{selection_failure, NamedHead, RecordedStep};
use crate::stages::join::fsm::{
    JoinEvent, JoinReading, JoinResources, JoinState, JoinSubscriptionSide, LiveReads,
    PendingSubscriptionAck,
};

type LiveDirective =
    Result<EventLoopDirective<JoinEvent>, Box<dyn std::error::Error + Send + Sync>>;

type LiveStep =
    Result<Option<EventLoopDirective<JoinEvent>>, Box<dyn std::error::Error + Send + Sync>>;

pub(super) async fn dispatch_live<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state: &JoinState,
    reading: &JoinReading,
    ctx: &mut JoinResources<H>,
) -> LiveDirective {
    common::ensure_subscriptions(sup, ctx);

    ctx.instrumentation
        .event_loops_total
        .fetch_add(1, Ordering::Relaxed);

    if let FlushOutcome::Blocked = common::flush_pending_outputs(sup, ctx).await? {
        return Ok(EventLoopDirective::Continue);
    }

    let state_name = state.variant_name();
    match reading.plan.live_reads() {
        Some(LiveReads::Recorded) => {
            return Box::pin(dispatch_live_recorded(sup, state_name, ctx)).await
        }
        Some(LiveReads::Canonical) => {
            return dispatch_live_canonical(sup, state_name, ctx).await;
        }
        Some(LiveReads::Preference) => {}
        None => {
            return Err(format!(
                "join '{}' is live under a plan with no live phase",
                ctx.stage_name
            )
            .into())
        }
    }

    let cap = ctx.reference_batch_cap.unwrap_or(usize::MAX);
    let prefer_stream = ctx.reference_since_last_stream >= cap;

    if prefer_stream {
        if let Some(directive) = poll_live_stream(sup, state_name, ctx).await? {
            return Ok(directive);
        }
        if let Some(directive) = poll_live_reference(sup, state_name, ctx).await? {
            return Ok(directive);
        }
    } else {
        if let Some(directive) = poll_live_reference(sup, state_name, ctx).await? {
            return Ok(directive);
        }
        if let Some(directive) = poll_live_stream(sup, state_name, ctx).await? {
            return Ok(directive);
        }
    }

    if let Some(subscription) = sup.reference_subscription.as_mut() {
        subscription
            .maybe_check_contracts_tick(
                &mut ctx.reference_contract_state[..],
                &mut ctx.reference_last_contract_check,
            )
            .await
            .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
            .into_result()?;
    }
    if let Some(subscription) = sup.stream_subscription.as_mut() {
        subscription
            .maybe_check_contracts_tick(
                &mut ctx.stream_contract_state[..],
                &mut ctx.stream_last_contract_check,
            )
            .await
            .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
            .into_result()?;
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    Ok(EventLoopDirective::Continue)
}

async fn poll_live_reference<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state_name: &str,
    ctx: &mut JoinResources<H>,
) -> LiveStep {
    let poll = {
        let Some(subscription) = sup.reference_subscription.as_mut() else {
            return Ok(None);
        };
        subscription
            .poll_next_with_state(state_name, Some(&mut ctx.reference_contract_state[..]))
            .await
    };

    match poll {
        PollResult::Event(envelope) => handle_reference_envelope(sup, ctx, envelope).await,
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
            Ok(Some(EventLoopDirective::Continue))
        }
        PollResult::NoEvents => Ok(None),
        PollResult::Error(e) => Ok(Some(EventLoopDirective::Transition(JoinEvent::Error(
            format!("Reference subscription error: {e}"),
        )))),
    }
}

/// Handle one delivered reference-side envelope (extracted from the poll path
/// so the FLOWIP-095d canonical dispatch can deliver through the same code).
async fn handle_reference_envelope<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinResources<H>,
    envelope: DeliveredRecord<ChainPayload>,
) -> LiveStep {
    let Some(subscription) = sup.reference_subscription.as_mut() else {
        return Ok(None);
    };
    let delivered_upstream_stage = subscription
        .last_delivered_upstream_stage()
        .expect("delivered event must identify its upstream stage");
    ctx.instrumentation
        .record_consumed(&envelope, delivered_upstream_stage);
    ctx.instrumentation
        .event_loops_with_work_total
        .fetch_add(1, Ordering::Relaxed);

    // Capture reference-side ancestry for FLOWIP-071h (conservative high-water interim).
    common::observe_reference_envelope(ctx, &envelope);

    ctx.reference_since_last_stream = ctx.reference_since_last_stream.saturating_add(1);
    ctx.instrumentation
        .join_reference_since_last_stream
        .store(ctx.reference_since_last_stream as u64, Ordering::Relaxed);

    let directive = match &envelope.payload {
        ChainPayload::FlowControl(_) => {
            match common::read_control(
                ctx,
                subscription,
                sup.stream_subscription.as_ref(),
                Some(crate::stages::observer::JoinSide::Reference),
                false,
                envelope,
            )
            .await?
            {
                Some(fact) => Some(EventLoopDirective::Transition(fact)),
                None => Some(EventLoopDirective::Continue),
            }
        }
        payload if payload.consumes_data_credit() => {
            let event = envelope.authored();
            let event_id = event.id;
            let source_id = ctx.reference_stage_id;
            let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
            let scope = ctx.runtime_execution.dispatch_scope(
                ctx.stage_id,
                subscription.last_delivered_stage_input_position(),
                subscription.last_delivered_generation(),
            );
            let delivery_snapshot = (ctx.observers.has_join() && !scope.is_deterministic_replay())
                .then(|| {
                    common::delivery_snapshot(
                        crate::stages::observer::JoinSide::Reference,
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
                    JoinSubscriptionSide::Reference,
                    source_id,
                    outputs.into(),
                    Some(&envelope),
                )
                .await?;
                subscription
                    .maybe_check_contracts_tick(
                        &mut ctx.reference_contract_state[..],
                        &mut ctx.reference_last_contract_check,
                    )
                    .await
                    .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
                    .into_result()?;
                return Ok(Some(EventLoopDirective::Continue));
            }

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
                .process_reference(
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
                Ok(mut events) => {
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
                        Some(&envelope),
                    )
                    .await?;
                    write_stage_outputs_and_ack(
                        subscription,
                        ctx,
                        JoinSubscriptionSide::Reference,
                        source_id,
                        events.into(),
                        Some(&envelope),
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
                        return Ok(Some(EventLoopDirective::Transition(JoinEvent::Error(
                            fatal.detail.clone(),
                        ))));
                    }
                    let reason = format!("Join handler error during live reference: {err:?}");
                    let mut error_event = envelope.authored().mark_as_error(reason, err.kind());
                    ctx.instrumentation.record_error(err.kind());
                    common::observe_join_outputs(
                        ctx,
                        scope,
                        Some(&event),
                        delivery_snapshot.as_ref(),
                        None,
                        std::slice::from_mut(&mut error_event),
                        Some(&envelope),
                    )
                    .await?;

                    if route_to_error_journal(&error_event) {
                        crate::supervised_base::publication::append(
                            &ctx.error_journal,
                            error_event,
                            AppendOptions::from_record(Some(&envelope))?,
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
                            JoinSubscriptionSide::Reference,
                            source_id,
                            VecDeque::from([error_event]),
                            Some(&envelope),
                        )
                        .await?;
                    }
                }
            }

            Some(EventLoopDirective::Continue)
        }
        _ => Some(EventLoopDirective::Continue),
    };

    subscription
        .maybe_check_contracts_tick(
            &mut ctx.reference_contract_state[..],
            &mut ctx.reference_last_contract_check,
        )
        .await
        .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
        .into_result()?;

    Ok(directive)
}

async fn poll_live_stream<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state_name: &str,
    ctx: &mut JoinResources<H>,
) -> LiveStep {
    let poll = {
        let Some(subscription) = sup.stream_subscription.as_mut() else {
            return Ok(None);
        };
        subscription
            .poll_next_with_state(state_name, Some(&mut ctx.stream_contract_state[..]))
            .await
    };

    match poll {
        PollResult::Event(envelope) => handle_stream_envelope(sup, ctx, envelope).await,
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
            Ok(Some(EventLoopDirective::Continue))
        }
        PollResult::NoEvents => Ok(None),
        PollResult::Error(e) => Ok(Some(EventLoopDirective::Transition(JoinEvent::Error(
            format!("Stream subscription error: {e}"),
        )))),
    }
}

/// Handle one delivered stream-side envelope (extracted from the poll path so
/// the FLOWIP-095d canonical dispatch can deliver through the same code).
async fn handle_stream_envelope<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinResources<H>,
    envelope: DeliveredRecord<ChainPayload>,
) -> LiveStep {
    let Some(subscription) = sup.stream_subscription.as_mut() else {
        return Ok(None);
    };
    let delivered_upstream_stage = subscription
        .last_delivered_upstream_stage()
        .expect("delivered event must identify its upstream stage");
    ctx.instrumentation
        .record_consumed(&envelope, delivered_upstream_stage);
    ctx.instrumentation
        .event_loops_with_work_total
        .fetch_add(1, Ordering::Relaxed);

    ctx.reference_since_last_stream = 0;
    ctx.instrumentation
        .join_reference_since_last_stream
        .store(0, Ordering::Relaxed);

    let directive = match &envelope.payload {
        ChainPayload::FlowControl(_) => {
            match common::read_control(
                ctx,
                subscription,
                sup.reference_subscription.as_ref(),
                Some(crate::stages::observer::JoinSide::Stream),
                true,
                envelope,
            )
            .await?
            {
                Some(fact) => Some(EventLoopDirective::Transition(fact)),
                None => Some(EventLoopDirective::Continue),
            }
        }
        payload if payload.consumes_data_credit() => {
            // Edge identity comes from the reader slot that delivered the
            // envelope, never from `event.writer_id`, which is preserved
            // across stages for causal attribution (forwarded rows carry the
            // original author). Backpressure acks, pending-ack routing, and
            // heartbeat attribution are all keyed by the upstream edge.
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
            let delivery_snapshot = (ctx.observers.has_join() && !scope.is_deterministic_replay())
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
                    JoinSubscriptionSide::Stream,
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
                    .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
                    .into_result()?;
                return Ok(Some(EventLoopDirective::Continue));
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
                        events.push(ChainEventFactory::eof_event_with_kind(writer_id, kind));
                    }
                    write_stage_outputs_and_ack(
                        subscription,
                        ctx,
                        JoinSubscriptionSide::Stream,
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
                        return Ok(Some(EventLoopDirective::Transition(JoinEvent::Error(
                            fatal.detail.clone(),
                        ))));
                    }
                    let reason = format!("Join handler error during live enrichment: {err:?}");
                    let mut error_event = envelope.authored().mark_as_error(reason, err.kind());
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
                            JoinSubscriptionSide::Stream,
                            source_id,
                            VecDeque::from([error_event]),
                            Some(&merged_parent),
                        )
                        .await?;
                    }
                }
            }

            Some(EventLoopDirective::Continue)
        }
        _ => Some(EventLoopDirective::Continue),
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

/// Which join subscription a merge candidate came from (FLOWIP-095d).
///
/// The final tiebreak component of the cross-side rule: it keeps the key
/// total even when the same upstream feeds both sides. The derived `Ord`
/// (`Reference < Stream`) is part of the canonical order and is code-defined,
/// so it is stable across runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum JoinSide {
    Reference,
    Stream,
}

pub(crate) fn select_between(
    a: &crate::messaging::upstream_subscription::MergeCandidateMeta<'_>,
    a_side: JoinSide,
    b: &crate::messaging::upstream_subscription::MergeCandidateMeta<'_>,
    b_side: JoinSide,
) -> std::cmp::Ordering {
    if !a.is_authored_eof && !b.is_authored_eof {
        if CausalOrderingService::happened_before(a.vector_clock, b.vector_clock) {
            return std::cmp::Ordering::Less;
        }
        if CausalOrderingService::happened_before(b.vector_clock, a.vector_clock) {
            return std::cmp::Ordering::Greater;
        }
    }
    (a.generation, a.ordinal, a.key, a_side).cmp(&(b.generation, b.ordinal, b.key, b_side))
}

pub(crate) fn select_recorded(candidates: &[RecordedCandidate<'_>]) -> Option<(JoinSide, usize)> {
    let admissible = |candidate: &&RecordedCandidate<'_>| {
        let (_, _, meta) = candidate;
        meta.is_authored_eof
            || !candidates.iter().any(|other| {
                !std::ptr::eq(other, *candidate)
                    && !other.2.is_authored_eof
                    && CausalOrderingService::happened_before(
                        other.2.vector_clock,
                        meta.vector_clock,
                    )
            })
    };
    candidates
        .iter()
        .filter(admissible)
        .min_by(|left, right| recorded_rank(left).cmp(&recorded_rank(right)))
        .or_else(|| {
            candidates
                .iter()
                .min_by(|left, right| recorded_rank(left).cmp(&recorded_rank(right)))
        })
        .map(|(side, index, _)| (*side, *index))
}

pub(crate) type RecordedCandidate<'a> = (
    JoinSide,
    usize,
    crate::messaging::upstream_subscription::MergeCandidateMeta<'a>,
);

fn recorded_rank<'a>(
    (side, _, meta): &'a RecordedCandidate<'_>,
) -> (
    obzenflow_core::ReaderGeneration,
    JoinSide,
    crate::messaging::upstream_subscription::DeliveredOrdinal,
    &'a crate::messaging::upstream_subscription::ReaderTiebreakKey,
) {
    (meta.generation, *side, meta.ordinal, meta.key)
}

/// Run the periodic contract ticks for both join sides (the idle-path block
/// shared by the unordered and canonical dispatchers).
async fn run_live_contract_ticks<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinResources<H>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(subscription) = sup.reference_subscription.as_mut() {
        subscription
            .maybe_check_contracts_tick(
                &mut ctx.reference_contract_state[..],
                &mut ctx.reference_last_contract_check,
            )
            .await
            .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
            .into_result()?;
    }
    if let Some(subscription) = sup.stream_subscription.as_mut() {
        subscription
            .maybe_check_contracts_tick(
                &mut ctx.stream_contract_state[..],
                &mut ctx.stream_last_contract_check,
            )
            .await
            .unwrap_or(crate::messaging::upstream_subscription::ContractStatus::Healthy)
            .into_result()?;
    }
    Ok(())
}

/// Canonical deterministic dispatch for ordered live joins (FLOWIP-095d).
async fn dispatch_live_canonical<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state_name: &str,
    ctx: &mut JoinResources<H>,
) -> LiveDirective {
    {
        if let Some(subscription) = sup.reference_subscription.as_mut() {
            let mut progress = Some(&mut ctx.reference_contract_state[..]);
            match subscription
                .ensure_merge_candidate(state_name, &mut progress)
                .await
            {
                Err(e) => {
                    return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                        "Reference subscription error: {e}"
                    ))));
                }
                Ok(
                    crate::messaging::upstream_subscription::MergeCandidateStatus::CursorAdvanced {
                        upstream,
                        completed_data_rows,
                    },
                ) => {
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
                Ok(_) => {}
            }
        }
        if let Some(subscription) = sup.stream_subscription.as_mut() {
            let mut progress = Some(&mut ctx.stream_contract_state[..]);
            match subscription
                .ensure_merge_candidate(state_name, &mut progress)
                .await
            {
                Err(e) => {
                    return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                        "Stream subscription error: {e}"
                    ))));
                }
                Ok(
                    crate::messaging::upstream_subscription::MergeCandidateStatus::CursorAdvanced {
                        upstream,
                        completed_data_rows,
                    },
                ) => {
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
                Ok(_) => {}
            }
        }
    }

    let blocking_wait = sup
        .reference_subscription
        .as_ref()
        .and_then(|subscription| subscription.merge_wait())
        .is_some()
        || sup
            .stream_subscription
            .as_ref()
            .and_then(|subscription| subscription.merge_wait())
            .is_some();
    if blocking_wait {
        let wait = sup
            .reference_subscription
            .as_ref()
            .and_then(|subscription| subscription.merge_wait())
            .or_else(|| {
                sup.stream_subscription
                    .as_ref()
                    .and_then(|subscription| subscription.merge_wait())
            });
        crate::stages::common::heartbeat::note_merge_wait(ctx.heartbeat.as_ref(), wait);
        run_live_contract_ticks(sup, ctx).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        return Ok(EventLoopDirective::Continue);
    }

    let side = {
        let reference_meta = sup
            .reference_subscription
            .as_ref()
            .and_then(|subscription| subscription.merge_candidate());
        let stream_meta = sup
            .stream_subscription
            .as_ref()
            .and_then(|subscription| subscription.merge_candidate());
        match (reference_meta, stream_meta) {
            (Some(reference), Some(stream)) => {
                if select_between(&reference, JoinSide::Reference, &stream, JoinSide::Stream)
                    .is_le()
                {
                    Some(JoinSide::Reference)
                } else {
                    Some(JoinSide::Stream)
                }
            }
            (Some(_), None) => Some(JoinSide::Reference),
            (None, Some(_)) => Some(JoinSide::Stream),
            (None, None) => None,
        }
    };

    let Some(side) = side else {
        // Both sides exhausted: EOF directives were produced at EOF delivery;
        // keep cycling like the unordered path does when nothing polls.
        run_live_contract_ticks(sup, ctx).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        return Ok(EventLoopDirective::Continue);
    };

    let directive = match side {
        JoinSide::Reference => {
            let poll = {
                let Some(subscription) = sup.reference_subscription.as_mut() else {
                    return Ok(EventLoopDirective::Continue);
                };
                subscription
                    .take_merge_candidate(state_name, Some(&mut ctx.reference_contract_state[..]))
            };
            match poll {
                PollResult::Event(envelope) => {
                    handle_reference_envelope(sup, ctx, envelope).await?
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
                    Some(EventLoopDirective::Continue)
                }
                PollResult::NoEvents => None,
                PollResult::Error(e) => Some(EventLoopDirective::Transition(JoinEvent::Error(
                    format!("Reference subscription error: {e}"),
                ))),
            }
        }
        JoinSide::Stream => {
            let poll = {
                let Some(subscription) = sup.stream_subscription.as_mut() else {
                    return Ok(EventLoopDirective::Continue);
                };
                subscription
                    .take_merge_candidate(state_name, Some(&mut ctx.stream_contract_state[..]))
            };
            match poll {
                PollResult::Event(envelope) => handle_stream_envelope(sup, ctx, envelope).await?,
                PollResult::CursorAdvanced {
                    upstream,
                    completed_data_rows,
                } => {
                    crate::backpressure::complete_filtered_data_rows(
                        &ctx.backpressure_readers,
                        upstream,
                        completed_data_rows,
                    );
                    Some(EventLoopDirective::Continue)
                }
                PollResult::NoEvents => None,
                PollResult::Error(e) => Some(EventLoopDirective::Transition(JoinEvent::Error(
                    format!("Stream subscription error: {e}"),
                ))),
            }
        }
    };

    Ok(directive.unwrap_or(EventLoopDirective::Continue))
}

async fn dispatch_live_recorded<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state_name: &str,
    ctx: &mut JoinResources<H>,
) -> LiveDirective {
    let step = ctx
        .recorded_order
        .as_mut()
        .expect("recorded join holds its order")
        .step();
    match step {
        RecordedStep::Open { row } => {
            Box::pin(follow_recorded(sup, state_name, ctx, row, true)).await
        }
        RecordedStep::Continue { row } => {
            Box::pin(follow_recorded(sup, state_name, ctx, row, false)).await
        }
        RecordedStep::Canonical => Box::pin(dispatch_live_canonical(sup, state_name, ctx)).await,
        RecordedStep::Record => Box::pin(select_and_record(sup, state_name, ctx)).await,
    }
}

fn join_delivered_total<H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
    sup: &JoinSupervisor<H>,
) -> u64 {
    sup.reference_subscription
        .as_ref()
        .map_or(0, |subscription| subscription.delivered_total())
        + sup
            .stream_subscription
            .as_ref()
            .map_or(0, |subscription| subscription.delivered_total())
}

fn side_subscription<H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
    sup: &mut JoinSupervisor<H>,
    side: JoinSide,
) -> Option<&mut crate::messaging::UpstreamSubscription<ChainEvent>> {
    match side {
        JoinSide::Reference => sup.reference_subscription.as_mut(),
        JoinSide::Stream => sup.stream_subscription.as_mut(),
    }
}

async fn append_join_selection<H: UnifiedJoinHandler>(
    ctx: &JoinResources<H>,
    input: obzenflow_core::event::payloads::flow_control_payload::SelectedInput,
    delivered_count: u64,
    input_delivered_count: u64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
    let flow_id = ctx.flow_id.to_string();
    let event = ChainEventFactory::input_selected_event(
        writer_id,
        input,
        delivered_count,
        input_delivered_count,
    )
    .with_flow_context(make_flow_context(
        &ctx.flow_name,
        &flow_id,
        &ctx.stage_name,
        ctx.stage_id,
        StageType::Join,
    ));
    crate::supervised_base::publication::append(&ctx.data_journal, event, Default::default())
        .await?;
    Ok(())
}

async fn deliver_recorded<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state_name: &str,
    ctx: &mut JoinResources<H>,
    side: JoinSide,
    index: usize,
) -> LiveDirective {
    let poll = match side {
        JoinSide::Reference => match sup.reference_subscription.as_mut() {
            Some(subscription) => subscription.take_held_head(
                index,
                state_name,
                Some(&mut ctx.reference_contract_state[..]),
            ),
            None => return Ok(EventLoopDirective::Continue),
        },
        JoinSide::Stream => match sup.stream_subscription.as_mut() {
            Some(subscription) => subscription.take_held_head(
                index,
                state_name,
                Some(&mut ctx.stream_contract_state[..]),
            ),
            None => return Ok(EventLoopDirective::Continue),
        },
    };
    let directive = match poll {
        PollResult::Event(envelope) => match side {
            JoinSide::Reference => Box::pin(handle_reference_envelope(sup, ctx, envelope)).await?,
            JoinSide::Stream => Box::pin(handle_stream_envelope(sup, ctx, envelope)).await?,
        },
        PollResult::CursorAdvanced {
            upstream,
            completed_data_rows,
        } => {
            crate::backpressure::complete_filtered_data_rows(
                &ctx.backpressure_readers,
                upstream,
                completed_data_rows,
            );
            Some(EventLoopDirective::Continue)
        }
        PollResult::NoEvents => None,
        PollResult::Error(e) => Some(EventLoopDirective::Transition(JoinEvent::Error(format!(
            "Join subscription error: {e}"
        )))),
    };
    Ok(directive.unwrap_or(EventLoopDirective::Continue))
}

async fn follow_recorded<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state_name: &str,
    ctx: &mut JoinResources<H>,
    row: usize,
    opening: bool,
) -> LiveDirective {
    let archived = ctx
        .recorded_order
        .as_ref()
        .expect("recorded join holds its order")
        .row(row)
        .clone();
    let named = [JoinSide::Reference, JoinSide::Stream]
        .into_iter()
        .find_map(|side| {
            let subscription = match side {
                JoinSide::Reference => sup.reference_subscription.as_ref(),
                JoinSide::Stream => sup.stream_subscription.as_ref(),
            }?;
            subscription
                .reader_for_input(&archived.input)
                .map(|index| (side, index))
        });
    let Some((side, index)) = named else {
        return Ok(EventLoopDirective::Transition(JoinEvent::Error(
            selection_failure(
                &ctx.stage_name,
                &archived,
                "names an input this join does not read",
            )
            .to_string(),
        )));
    };
    let ensured = {
        let (subscription, mut progress) = match side {
            JoinSide::Reference => (
                sup.reference_subscription.as_mut(),
                Some(&mut ctx.reference_contract_state[..]),
            ),
            JoinSide::Stream => (
                sup.stream_subscription.as_mut(),
                Some(&mut ctx.stream_contract_state[..]),
            ),
        };
        let subscription = subscription.expect("named side is subscribed");
        subscription
            .ensure_named_head(index, state_name, &mut progress)
            .await
    };
    match ensured {
        Err(e) => {
            return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                "Join subscription error: {e}"
            ))));
        }
        Ok(NamedHead::CursorAdvanced {
            upstream,
            completed_data_rows,
        }) => {
            crate::backpressure::complete_filtered_data_rows(
                &ctx.backpressure_readers,
                upstream,
                completed_data_rows,
            );
            return Ok(EventLoopDirective::Continue);
        }
        Ok(NamedHead::Exhausted) => {
            return Ok(EventLoopDirective::Transition(JoinEvent::Error(
                selection_failure(
                    &ctx.stage_name,
                    &archived,
                    "names an input that is exhausted",
                )
                .to_string(),
            )));
        }
        Ok(NamedHead::Waiting) => {
            let wait = side_subscription(sup, side)
                .and_then(|subscription| subscription.merge_wait().cloned());
            crate::stages::common::heartbeat::note_merge_wait(
                ctx.heartbeat.as_ref(),
                wait.as_ref(),
            );
            run_live_contract_ticks(sup, ctx).await?;
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            return Ok(EventLoopDirective::Continue);
        }
        Ok(NamedHead::Held) => {}
    }
    if opening {
        let delivered_total = join_delivered_total(sup);
        let subscription = side_subscription(sup, side).expect("named side is subscribed");
        if let Err(error) = subscription.check_archived_counts_at(delivered_total, index, &archived)
        {
            return Ok(EventLoopDirective::Transition(JoinEvent::Error(
                error.to_string(),
            )));
        }
        append_join_selection(
            ctx,
            archived.input.clone(),
            archived.delivered_count,
            archived.input_delivered_count,
        )
        .await?;
        ctx.recorded_order
            .as_mut()
            .expect("recorded join holds its order")
            .opened();
    }
    let ends_input = side_subscription(sup, side)
        .is_some_and(|subscription| subscription.held_head_ends_recorded_rows(index));
    ctx.recorded_order
        .as_mut()
        .expect("recorded join holds its order")
        .followed(ends_input);
    deliver_recorded(sup, state_name, ctx, side, index).await
}

async fn select_and_record<
    H: UnifiedJoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    state_name: &str,
    ctx: &mut JoinResources<H>,
) -> LiveDirective {
    loop {
        let held = |sup: &JoinSupervisor<H>| {
            sup.reference_subscription
                .as_ref()
                .map_or(0, |subscription| subscription.held_head_count())
                + sup
                    .stream_subscription
                    .as_ref()
                    .map_or(0, |subscription| subscription.held_head_count())
        };
        let held_before = held(sup);
        for side in [JoinSide::Reference, JoinSide::Stream] {
            let acquired = {
                let (subscription, mut progress) = match side {
                    JoinSide::Reference => (
                        sup.reference_subscription.as_mut(),
                        Some(&mut ctx.reference_contract_state[..]),
                    ),
                    JoinSide::Stream => (
                        sup.stream_subscription.as_mut(),
                        Some(&mut ctx.stream_contract_state[..]),
                    ),
                };
                let Some(subscription) = subscription else {
                    continue;
                };
                subscription
                    .acquire_available_heads(state_name, &mut progress)
                    .await
            };
            match acquired {
                Err(e) => {
                    return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                        "Join subscription error: {e}"
                    ))));
                }
                Ok(Some((upstream, completed_data_rows))) => {
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
                Ok(None) => {}
            }
        }
        if held(sup) == held_before {
            break;
        }
    }

    let waiting: Vec<_> = [
        sup.reference_subscription.as_ref(),
        sup.stream_subscription.as_ref(),
    ]
    .into_iter()
    .flatten()
    .flat_map(|subscription| subscription.pre_crossing_quiet_inputs())
    .collect();
    if !waiting.is_empty() {
        let wait = crate::messaging::upstream_subscription::MergeWaitState {
            quiet_inputs: waiting,
        };
        crate::stages::common::heartbeat::note_merge_wait(ctx.heartbeat.as_ref(), Some(&wait));
        run_live_contract_ticks(sup, ctx).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        return Ok(EventLoopDirective::Continue);
    }

    let chosen = {
        let mut candidates = Vec::new();
        for (side, subscription) in [
            (JoinSide::Reference, sup.reference_subscription.as_ref()),
            (JoinSide::Stream, sup.stream_subscription.as_ref()),
        ] {
            let Some(subscription) = subscription else {
                continue;
            };
            for index in subscription.held_indices() {
                if let Some(meta) = subscription.held_meta(index) {
                    candidates.push((side, index, meta));
                }
            }
        }
        select_recorded(&candidates)
    };
    let Some((side, index)) = chosen else {
        run_live_contract_ticks(sup, ctx).await?;
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        return Ok(EventLoopDirective::Continue);
    };

    let delivered_total = join_delivered_total(sup);
    let (input, input_delivered) = {
        let subscription = side_subscription(sup, side).expect("chosen side is subscribed");
        (
            subscription.selected_input(index),
            subscription.delivered_count_of(index),
        )
    };
    let opens = ctx
        .recorded_order
        .as_ref()
        .expect("recorded join holds its order")
        .opens_selection(&input);
    if opens {
        append_join_selection(ctx, input.clone(), delivered_total, input_delivered).await?;
        ctx.recorded_order
            .as_mut()
            .expect("recorded join holds its order")
            .selected(input);
    }
    deliver_recorded(sup, state_name, ctx, side, index).await
}

async fn write_stage_outputs_and_ack<H: UnifiedJoinHandler>(
    subscription: &mut crate::messaging::UpstreamSubscription<ChainEvent>,
    ctx: &mut JoinResources<H>,
    side: JoinSubscriptionSide,
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
                    side,
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

#[cfg(test)]
mod tests {
    use super::{select_between, select_recorded, JoinSide};
    use crate::messaging::upstream_subscription::{
        DeliveredOrdinal, FeedIdentity, MergeCandidateMeta, ReaderTiebreakKey, StageKey,
    };
    use obzenflow_core::event::vector_clock::{CausalOrderingService, VectorClock};
    use std::cmp::Ordering;

    fn clock(entries: &[(&str, u64)]) -> VectorClock {
        let mut clock = VectorClock::new();
        for (writer, seq) in entries {
            for _ in 0..*seq {
                CausalOrderingService::increment(
                    &mut clock,
                    &crate::testing::causal_fixture::coordinate(writer),
                )
                .unwrap();
            }
        }
        clock
    }

    fn key(stage_key: &str) -> ReaderTiebreakKey {
        ReaderTiebreakKey {
            stage_key: StageKey::new(stage_key),
            feed_identity: FeedIdentity::unfiltered(),
        }
    }

    fn meta<'a>(
        ordinal: u64,
        key: &'a ReaderTiebreakKey,
        vector_clock: &'a VectorClock,
        is_authored_eof: bool,
    ) -> MergeCandidateMeta<'a> {
        MergeCandidateMeta {
            generation: obzenflow_core::ReaderGeneration(0),
            ordinal: DeliveredOrdinal(ordinal),
            key,
            vector_clock,
            is_authored_eof,
        }
    }

    #[test]
    fn recorded_generation_orders_ahead_of_live_cross_side() {
        let empty = VectorClock::new();
        let (key_a, key_z) = (key("a_stage"), key("z_stage"));
        // The live head has the better ordinal and key; the recorded head
        // still wins because generation is the coarsest axis (FLOWIP-120n).
        let mut recorded = meta(9, &key_z, &empty, false);
        recorded.generation = obzenflow_core::ReaderGeneration(0);
        let mut live = meta(1, &key_a, &empty, false);
        live.generation = obzenflow_core::ReaderGeneration(1);
        assert_eq!(
            select_between(&recorded, JoinSide::Stream, &live, JoinSide::Reference),
            Ordering::Less
        );
        assert_eq!(
            select_between(&live, JoinSide::Reference, &recorded, JoinSide::Stream),
            Ordering::Greater
        );
    }

    #[test]
    fn lower_ordinal_wins_regardless_of_stage_key() {
        let empty = VectorClock::new();
        let (key_z, key_a) = (key("z_stage"), key("a_stage"));
        let a = meta(1, &key_z, &empty, false);
        let b = meta(2, &key_a, &empty, false);
        assert_eq!(
            select_between(&a, JoinSide::Reference, &b, JoinSide::Stream),
            Ordering::Less
        );
    }

    #[test]
    fn ordinal_tie_breaks_by_stage_key_then_side() {
        let empty = VectorClock::new();
        let (key_a, key_b) = (key("a_stage"), key("b_stage"));
        let a = meta(1, &key_a, &empty, false);
        let b = meta(1, &key_b, &empty, false);
        assert_eq!(
            select_between(&a, JoinSide::Stream, &b, JoinSide::Reference),
            Ordering::Less
        );

        let key_same = key("same");
        let same_a = meta(1, &key_same, &empty, false);
        let same_b = meta(1, &key_same, &empty, false);
        assert_eq!(
            select_between(&same_a, JoinSide::Reference, &same_b, JoinSide::Stream),
            Ordering::Less,
            "the join side keeps the cross-side key total"
        );
    }

    #[test]
    fn happened_before_overrides_adverse_tiebreak() {
        let ancestor = clock(&[("wa", 2)]);
        let derived = clock(&[("wa", 2), ("wb", 1)]);
        // The derived candidate has the better ordinal and stage key, and
        // still loses to its causal ancestor.
        let (key_z, key_a) = (key("z_stage"), key("a_stage"));
        let a = meta(5, &key_z, &ancestor, false);
        let b = meta(1, &key_a, &derived, false);
        assert_eq!(
            select_between(&a, JoinSide::Stream, &b, JoinSide::Reference),
            Ordering::Less
        );
        assert_eq!(
            select_between(&b, JoinSide::Reference, &a, JoinSide::Stream),
            Ordering::Greater
        );
    }

    #[test]
    fn authored_eof_is_exempt_from_causality_and_orders_by_tiebreak() {
        let ancestor = clock(&[("wa", 1)]);
        let derived = clock(&[("wa", 1), ("wb", 1)]);
        // The EOF's clock happened-before the data candidate's clock, but the
        // exemption sends the decision to the tiebreak, where the data
        // candidate's lower ordinal wins.
        let (key_a, key_b) = (key("a_stage"), key("b_stage"));
        let eof = meta(2, &key_a, &ancestor, true);
        let data = meta(1, &key_b, &derived, false);
        assert_eq!(
            select_between(&eof, JoinSide::Reference, &data, JoinSide::Stream),
            Ordering::Greater
        );
    }

    #[test]
    fn recorded_join_prefers_an_eligible_reference_head() {
        let empty = VectorClock::new();
        let (accounts, tx) = (key("z_accounts"), key("a_tx"));
        let candidates = [
            (JoinSide::Stream, 0, meta(1, &tx, &empty, false)),
            (JoinSide::Reference, 0, meta(9, &accounts, &empty, false)),
        ];
        assert_eq!(select_recorded(&candidates), Some((JoinSide::Reference, 0)));
    }

    #[test]
    fn recorded_join_waits_a_reference_head_behind_a_stream_head_it_follows() {
        let first = clock(&[("wa", 1)]);
        let unrelated = clock(&[("wb", 1)]);
        let derived = clock(&[("wa", 1), ("wc", 1)]);
        let (accounts, early, late) = (key("accounts"), key("a_early"), key("b_late"));
        let candidates = [
            (JoinSide::Stream, 0, meta(1, &early, &unrelated, false)),
            (JoinSide::Stream, 1, meta(1, &late, &first, false)),
            (JoinSide::Reference, 0, meta(1, &accounts, &derived, false)),
        ];
        assert_eq!(select_recorded(&candidates), Some((JoinSide::Stream, 0)));
        let without_unrelated = &candidates[1..];
        assert_eq!(
            select_recorded(without_unrelated),
            Some((JoinSide::Stream, 1))
        );
    }

    #[test]
    fn recorded_join_orders_a_recorded_generation_ahead_of_the_reference_side() {
        let empty = VectorClock::new();
        let (accounts, tx) = (key("accounts"), key("tx"));
        let mut live_reference = meta(1, &accounts, &empty, false);
        live_reference.generation = obzenflow_core::ReaderGeneration(1);
        let candidates = [
            (JoinSide::Reference, 0, live_reference),
            (JoinSide::Stream, 0, meta(5, &tx, &empty, false)),
        ];
        assert_eq!(select_recorded(&candidates), Some((JoinSide::Stream, 0)));
    }
}
