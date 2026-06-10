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
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::time::Instant;

use super::common::{self, FlushOutcome};
use super::JoinSupervisor;
use crate::stages::join::fsm::{JoinContext, JoinEvent};

pub(super) async fn dispatch_live<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
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
                "Join live observed DrainComplete-ready pending transition; ignoring"
            );
        }
        FlushOutcome::Drained => {}
    }

    // FLOWIP-095d: an ordered join runs the canonical cross-subscription
    // merge; the availability-driven preference polling below (and its
    // fairness cap) applies to unordered joins only.
    if ctx.deterministic_fan_in {
        return dispatch_live_canonical(sup, ctx).await;
    }

    let cap = ctx.reference_batch_cap.unwrap_or(usize::MAX);
    let prefer_stream = ctx.reference_since_last_stream >= cap;

    if prefer_stream {
        if let Some(directive) = poll_live_stream(sup, ctx).await? {
            return Ok(directive);
        }
        if let Some(directive) = poll_live_reference(sup, ctx).await? {
            return Ok(directive);
        }
    } else {
        if let Some(directive) = poll_live_reference(sup, ctx).await? {
            return Ok(directive);
        }
        if let Some(directive) = poll_live_stream(sup, ctx).await? {
            return Ok(directive);
        }
    }

    if let Some(subscription) = sup.reference_subscription.as_mut() {
        drop(
            subscription
                .maybe_check_contracts_tick(
                    &mut ctx.reference_contract_state[..],
                    &mut ctx.reference_last_contract_check,
                )
                .await,
        );
    }
    if let Some(subscription) = sup.stream_subscription.as_mut() {
        drop(
            subscription
                .maybe_check_contracts_tick(
                    &mut ctx.stream_contract_state[..],
                    &mut ctx.stream_last_contract_check,
                )
                .await,
        );
    }

    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
    Ok(EventLoopDirective::Continue)
}

async fn poll_live_reference<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
) -> Result<Option<EventLoopDirective<JoinEvent<H>>>, Box<dyn std::error::Error + Send + Sync>> {
    let poll = {
        let Some(subscription) = sup.reference_subscription.as_mut() else {
            return Ok(None);
        };
        subscription
            .poll_next_with_state("Live", Some(&mut ctx.reference_contract_state[..]))
            .await
    };

    match poll {
        PollResult::Event(envelope) => handle_reference_envelope(sup, ctx, envelope).await,
        PollResult::NoEvents => Ok(None),
        PollResult::Error(e) => Ok(Some(EventLoopDirective::Transition(JoinEvent::Error(
            format!("Reference subscription error: {e}"),
        )))),
    }
}

/// Handle one delivered reference-side envelope (extracted from the poll path
/// so the FLOWIP-095d canonical dispatch can deliver through the same code).
async fn handle_reference_envelope<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
    envelope: EventEnvelope<ChainEvent>,
) -> Result<Option<EventLoopDirective<JoinEvent<H>>>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(subscription) = sup.reference_subscription.as_mut() else {
        return Ok(None);
    };
    ctx.instrumentation.record_consumed(&envelope);
    ctx.instrumentation
        .event_loops_with_work_total
        .fetch_add(1, Ordering::Relaxed);

    // Capture reference-side ancestry for FLOWIP-071h (conservative high-water interim).
    common::observe_reference_envelope(ctx, &envelope);

    ctx.reference_since_last_stream = ctx.reference_since_last_stream.saturating_add(1);
    ctx.instrumentation
        .join_reference_since_last_stream
        .store(ctx.reference_since_last_stream as u64, Ordering::Relaxed);

    let directive = match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(signal) => {
            let contract_reader_count = ctx.reference_contract_state.len();
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
                ControlResolution::Forward | ControlResolution::ForwardAndDrain => {
                    common::forward_control_event_and_mirror(ctx, &envelope).await?;
                    if envelope.event.is_eof() {
                        let _ = subscription.take_last_eof_outcome();
                    }
                }
                ControlResolution::Suppress | ControlResolution::BufferAtEntryPoint { .. } => {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Join received cycle-only control resolution without cycle config"
                    );
                }
                ControlResolution::Delay(_) => {
                    unreachable!("Delay is handled before executing the resolution")
                }
                ControlResolution::Skip => {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Skipping control event (dangerous!) during Live (reference)"
                    );
                }
            }

            Some(EventLoopDirective::Continue)
        }
        obzenflow_core::event::ChainEventContent::Data { .. } => {
            let event = envelope.event.clone();
            let event_id = event.id;
            let source_id = ctx.reference_stage_id;
            let writer_id = ctx.writer_id.ok_or("No writer ID available")?;
            if let Some(heartbeat) = &ctx.heartbeat {
                heartbeat.state.record_data_read(source_id, event_id);
            }
            let heartbeat_state = ctx.heartbeat.as_ref().map(|h| h.state.clone());

            if matches!(event.processing_info.status, ProcessingStatus::Error { .. }) {
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
                            &mut ctx.reference_contract_state[..],
                            &mut ctx.reference_last_contract_check,
                        )
                        .await,
                );
                return Ok(Some(EventLoopDirective::Continue));
            }

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
                        Some(&envelope),
                    )
                    .await?;
                }
                Err(err) => {
                    let reason = format!("Join handler error during live reference: {err:?}");
                    let error_event = envelope.event.clone().mark_as_error(reason, err.kind());
                    ctx.instrumentation.record_error(err.kind());

                    if route_to_error_journal(&error_event) {
                        ctx.error_journal
                            .append(error_event, Some(&envelope))
                            .await
                            .map_err(|e| format!("Failed to write join error event: {e}"))?;
                        if let Some(reader) = ctx.backpressure_readers.get(&source_id) {
                            reader.ack_consumed(1);
                        }
                    } else {
                        write_stage_outputs_and_ack(
                            subscription,
                            ctx,
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

    drop(
        subscription
            .maybe_check_contracts_tick(
                &mut ctx.reference_contract_state[..],
                &mut ctx.reference_last_contract_check,
            )
            .await,
    );

    Ok(directive)
}

async fn poll_live_stream<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
) -> Result<Option<EventLoopDirective<JoinEvent<H>>>, Box<dyn std::error::Error + Send + Sync>> {
    let poll = {
        let Some(subscription) = sup.stream_subscription.as_mut() else {
            return Ok(None);
        };
        subscription
            .poll_next_with_state("Live", Some(&mut ctx.stream_contract_state[..]))
            .await
    };

    match poll {
        PollResult::Event(envelope) => handle_stream_envelope(sup, ctx, envelope).await,
        PollResult::NoEvents => Ok(None),
        PollResult::Error(e) => Ok(Some(EventLoopDirective::Transition(JoinEvent::Error(
            format!("Stream subscription error: {e}"),
        )))),
    }
}

/// Handle one delivered stream-side envelope (extracted from the poll path so
/// the FLOWIP-095d canonical dispatch can deliver through the same code).
async fn handle_stream_envelope<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
    envelope: EventEnvelope<ChainEvent>,
) -> Result<Option<EventLoopDirective<JoinEvent<H>>>, Box<dyn std::error::Error + Send + Sync>> {
    let Some(subscription) = sup.stream_subscription.as_mut() else {
        return Ok(None);
    };
    ctx.instrumentation.record_consumed(&envelope);
    ctx.instrumentation
        .event_loops_with_work_total
        .fetch_add(1, Ordering::Relaxed);

    ctx.reference_since_last_stream = 0;
    ctx.instrumentation
        .join_reference_since_last_stream
        .store(0, Ordering::Relaxed);

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
                    Some(EventLoopDirective::Continue)
                }
                ControlResolution::ForwardAndDrain => {
                    common::forward_control_event_and_mirror(ctx, &envelope).await?;
                    if envelope.event.is_eof() {
                        let _ = subscription.take_last_eof_outcome();
                    }
                    Some(EventLoopDirective::Transition(JoinEvent::ReceivedEOF))
                }
                ControlResolution::Suppress | ControlResolution::BufferAtEntryPoint { .. } => {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Join received cycle-only control resolution without cycle config"
                    );
                    Some(EventLoopDirective::Continue)
                }
                ControlResolution::Delay(_) => {
                    unreachable!("Delay is handled before executing the resolution")
                }
                ControlResolution::Skip => {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        event_type = envelope.event.event_type(),
                        "Skipping control event (dangerous!) during Live (stream)"
                    );
                    Some(EventLoopDirective::Continue)
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
                return Ok(Some(EventLoopDirective::Continue));
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
                    let reason = format!("Join handler error during live enrichment: {err:?}");
                    let error_event = envelope.event.clone().mark_as_error(reason, err.kind());
                    ctx.instrumentation.record_error(err.kind());

                    if route_to_error_journal(&error_event) {
                        ctx.error_journal
                            .append(error_event, Some(&merged_parent))
                            .await
                            .map_err(|e| format!("Failed to write join error event: {e}"))?;
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

            Some(EventLoopDirective::Continue)
        }
        _ => Some(EventLoopDirective::Continue),
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

/// Cross-subscription comparison for the FLOWIP-095d canonical join dispatch.
///
/// Applies the same rule as the subscription-internal merge: happened-before
/// between non-EOF candidates first (authored EOFs are exempt and order by
/// tiebreak alone), then the (ordinal, reader tiebreak key, join side)
/// tiebreak.
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
    (a.ordinal, a.key, a_side).cmp(&(b.ordinal, b.key, b_side))
}

/// Run the periodic contract ticks for both join sides (the idle-path block
/// shared by the unordered and canonical dispatchers).
async fn run_live_contract_ticks<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
) {
    if let Some(subscription) = sup.reference_subscription.as_mut() {
        drop(
            subscription
                .maybe_check_contracts_tick(
                    &mut ctx.reference_contract_state[..],
                    &mut ctx.reference_last_contract_check,
                )
                .await,
        );
    }
    if let Some(subscription) = sup.stream_subscription.as_mut() {
        drop(
            subscription
                .maybe_check_contracts_tick(
                    &mut ctx.stream_contract_state[..],
                    &mut ctx.stream_last_contract_check,
                )
                .await,
        );
    }
}

/// Canonical deterministic dispatch for ordered live joins (FLOWIP-095d).
///
/// Both subscriptions select their internal merge candidates; the cross-side
/// choice applies the same rule to the two candidates with the side label as
/// the final tiebreak component. Any quiet side blocks delivery (the Kahn
/// discipline), reported through the heartbeat as idle-by-rule.
async fn dispatch_live_canonical<
    H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
>(
    sup: &mut JoinSupervisor<H>,
    ctx: &mut JoinContext<H>,
) -> Result<EventLoopDirective<JoinEvent<H>>, Box<dyn std::error::Error + Send + Sync>> {
    use crate::messaging::upstream_subscription::MergeCandidateStatus;

    let reference_status = match sup.reference_subscription.as_mut() {
        Some(subscription) => {
            let mut progress = Some(&mut ctx.reference_contract_state[..]);
            match subscription
                .ensure_merge_candidate("Live", &mut progress)
                .await
            {
                Ok(status) => status,
                Err(e) => {
                    return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                        "Reference subscription error: {e}"
                    ))))
                }
            }
        }
        None => MergeCandidateStatus::AllExhausted,
    };
    let stream_status = match sup.stream_subscription.as_mut() {
        Some(subscription) => {
            let mut progress = Some(&mut ctx.stream_contract_state[..]);
            match subscription
                .ensure_merge_candidate("Live", &mut progress)
                .await
            {
                Ok(status) => status,
                Err(e) => {
                    return Ok(EventLoopDirective::Transition(JoinEvent::Error(format!(
                        "Stream subscription error: {e}"
                    ))))
                }
            }
        }
        None => MergeCandidateStatus::AllExhausted,
    };

    if matches!(reference_status, MergeCandidateStatus::Quiet)
        || matches!(stream_status, MergeCandidateStatus::Quiet)
    {
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
        run_live_contract_ticks(sup, ctx).await;
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
        run_live_contract_ticks(sup, ctx).await;
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
                    .take_merge_candidate("Live", Some(&mut ctx.reference_contract_state[..]))
            };
            match poll {
                PollResult::Event(envelope) => {
                    handle_reference_envelope(sup, ctx, envelope).await?
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
                subscription.take_merge_candidate("Live", Some(&mut ctx.stream_contract_state[..]))
            };
            match poll {
                PollResult::Event(envelope) => handle_stream_envelope(sup, ctx, envelope).await?,
                PollResult::NoEvents => None,
                PollResult::Error(e) => Some(EventLoopDirective::Transition(JoinEvent::Error(
                    format!("Stream subscription error: {e}"),
                ))),
            }
        }
    };

    Ok(directive.unwrap_or(EventLoopDirective::Continue))
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
            ctx.heartbeat.as_ref().map(|h| h.state.clone()),
            &ctx.data_journal,
            &ctx.system_journal,
            pending_parent,
            &ctx.instrumentation,
            &ctx.backpressure_writer,
            &mut ctx.backpressure_pulse,
            &mut ctx.backpressure_backoff,
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

#[cfg(test)]
mod tests {
    use super::{select_between, JoinSide};
    use crate::messaging::upstream_subscription::{
        DeliveredOrdinal, FeedIdentity, MergeCandidateMeta, ReaderTiebreakKey, StageKey,
    };
    use obzenflow_core::event::vector_clock::{CausalOrderingService, VectorClock};
    use std::cmp::Ordering;

    fn clock(entries: &[(&str, u64)]) -> VectorClock {
        let mut clock = VectorClock::new();
        for (writer, seq) in entries {
            for _ in 0..*seq {
                CausalOrderingService::increment(&mut clock, writer);
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
            ordinal: DeliveredOrdinal(ordinal),
            key,
            vector_clock,
            is_authored_eof,
        }
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
}
