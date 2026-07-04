// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Backpressure-aware pending output drain loop
//!
//! `drain_one_pending()` handles the full bypass-pulse / reserve-or-requeue /
//! commit-and-reset cycle for a single pending output event. Supervisors call
//! this in a `while let` loop to flush their `pending_outputs` queue.

use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::{CreditWaker, WakeOn};
use crate::stages::common::supervision::suspension::suspend_until;
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::payloads::observability_payload::{
    BackpressureEvent, MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{ChainEventFactory, EventEnvelope};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use std::sync::Arc;

use crate::backpressure::{BackpressureWriter, LimitingEdgeDetail};
use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::output_committer::{CommitOptions, OutputCommitter};

/// Outcome of attempting to drain a single pending event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    /// The event was successfully written to the journal.
    Committed {
        /// `true` if the written event was a data event (caller should call
        /// `subscription.track_output_event()`).
        was_data: bool,
    },
    /// Backpressure prevented writing; one chunk of the credit wait ran and
    /// the event is back at the front of the queue. The caller returns early
    /// so the loop re-enters dispatch_state (queued control is observed).
    BackedOff,
}

/// Runtime-owned bound on one blocked suspend: the loop re-enters
/// `dispatch_state` at least this often so queued control is observed, while
/// the anchored stall deadline alone decides the stall (FLOWIP-115e).
pub(crate) const CONTROL_RESPONSIVENESS_CAP: std::time::Duration =
    std::time::Duration::from_millis(250);

/// Outcome of a single drain attempt without waiting.
///
/// This is used by supervisors that need to integrate the credit wait into a
/// larger `tokio::select!` (for example, async source supervisors that must
/// remain responsive to external control events while blocked).
#[derive(Debug, Clone)]
pub(crate) enum DrainAttempt {
    /// The event was successfully written to the journal.
    Committed {
        /// `true` if the written event was a data event (caller should call
        /// `subscription.track_output_event()`).
        was_data: bool,
    },
    /// Backpressure prevented writing; the event has been pushed back to the
    /// front of the queue. The caller owns one chunked credit wait:
    /// `suspend_until(&WakeOn::Notify(waker), Some(bound))`, where `bound` is
    /// already clamped to the remaining stall budget and the control cap.
    BackedOff {
        bound: std::time::Duration,
        waker: CreditWaker,
    },
}

/// Attempt to drain a single pending output event through the backpressure
/// gate, writing it to the data journal if credit is available.
///
/// The caller supplies all the context fields individually so that this helper
/// does not require a reference to the full FSM context. This avoids borrow
/// conflicts when the subscription is held separately.
/// A backpressure-deferred output carrying the execution scope frozen at the
/// moment it was produced (FLOWIP-120r). A late flush after a resume handoff
/// then stays scoped as it was produced rather than re-judged at drain time.
#[derive(Debug, Clone)]
pub(crate) struct PendingOutput {
    pub(crate) event: ChainEvent,
    pub(crate) scope: obzenflow_core::MiddlewareExecutionScope,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_one_pending(
    pending: PendingOutput,
    flow_context: &FlowContext,
    stage_id: StageId,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    pending_parent: Option<&EventEnvelope<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_stall: &mut Option<tokio::time::Instant>,
    output_contract: Option<&StageOutputContract>,
    observers: Option<&crate::stages::observer::StageObserverBundle>,
    pending_outputs: &mut std::collections::VecDeque<PendingOutput>,
) -> Result<DrainOutcome, Box<dyn std::error::Error + Send + Sync>> {
    match drain_one_pending_resolve(
        pending,
        flow_context,
        stage_id,
        heartbeat_state,
        data_journal,
        system_journal,
        pending_parent,
        instrumentation,
        backpressure_writer,
        backpressure_pulse,
        backpressure_stall,
        output_contract,
        observers,
        pending_outputs,
    )
    .await?
    {
        DrainAttempt::Committed { was_data } => Ok(DrainOutcome::Committed { was_data }),
        DrainAttempt::BackedOff { bound, waker } => {
            // One chunk of the credit wait: wake on the downstream ack or at
            // the chunk boundary, then return to dispatch_state either way.
            // Only the anchored deadline in `backpressure_stall` can author
            // the stall fact, so intermediate wakes never extend the episode.
            let wait_started = tokio::time::Instant::now();
            let _ = suspend_until(&WakeOn::Notify(waker), Some(bound)).await;
            let measured = wait_started.elapsed();
            backpressure_writer.record_wait(measured);
            emit_blocked_pulse(
                stage_id,
                flow_context,
                measured,
                data_journal,
                system_journal,
                instrumentation,
                backpressure_writer,
                backpressure_pulse,
            )
            .await;
            Ok(DrainOutcome::BackedOff)
        }
    }
}

/// Attempt to drain a single pending output event through the backpressure
/// gate, writing it to the data journal if credit is available.
///
/// Unlike `drain_one_pending`, this function never waits. If backpressure
/// blocks the write, it returns `DrainAttempt::BackedOff { bound, waker }`
/// and the caller owns the chunked wait (for example inside a
/// `tokio::select!` that stays responsive to external events).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_one_pending_resolve(
    pending: PendingOutput,
    flow_context: &FlowContext,
    stage_id: StageId,
    heartbeat_state: Option<Arc<HeartbeatState>>,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    pending_parent: Option<&EventEnvelope<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_stall: &mut Option<tokio::time::Instant>,
    output_contract: Option<&StageOutputContract>,
    observers: Option<&crate::stages::observer::StageObserverBundle>,
    pending_outputs: &mut std::collections::VecDeque<PendingOutput>,
) -> Result<DrainAttempt, Box<dyn std::error::Error + Send + Sync>> {
    let is_data = pending.event.is_data();

    // FLOWIP-120b Step 1: the commit core (flow/runtime enrichment, per-type
    // counting, journal append, heartbeat tracking, middleware mirror) is owned
    // by the shared OutputCommitter so the drain and the effects-layer effect
    // record append do not drift. The credit reservation, requeue, and pulses
    // stay in the drain until Step 2 moves backpressure to input admission.
    let committer = OutputCommitter {
        data_journal,
        flow_context: Some(flow_context),
        system_journal: Some(system_journal),
        instrumentation: Some(instrumentation),
        heartbeat_state: heartbeat_state.as_ref(),
        output_contract,
        observers,
        observer_scope: pending.scope,
    };

    if is_data {
        committer.validate_prebuilt(
            &pending.event,
            CommitOptions {
                count_output: true,
                validate_output_contract: true,
            },
        )?;

        // Reconstruction never blocks (FLOWIP-115e): a reconstruction-scoped
        // output reserves in track mode, so the accounting stays true for the
        // resume handoff while no wait, ceiling, stall fact, or pulse exists
        // on this path. The scope is the one frozen at production time.
        if pending.scope.is_deterministic_replay() {
            let reservation = backpressure_writer.reserve_tracked(1);
            committer
                .commit_prebuilt(
                    pending.event,
                    pending_parent,
                    CommitOptions {
                        count_output: true,
                        validate_output_contract: false,
                    },
                )
                .await
                .map_err(|e| format!("Failed to write pending output: {e}"))?;
            reservation.commit(1);
            *backpressure_stall = None;
            return Ok(DrainAttempt::Committed { was_data: true });
        }

        // Debug-only: emit activity pulses even when bypass is enabled, so
        // operators can see what *would* have blocked (FLOWIP-086k).
        emit_bypass_pulse_if_needed(
            stage_id,
            flow_context,
            data_journal,
            system_journal,
            instrumentation,
            backpressure_writer,
            backpressure_pulse,
        )
        .await;

        let Some(reservation) = backpressure_writer.reserve(1) else {
            pending_outputs.push_front(pending);

            // The stall episode anchors at the first credit miss and only
            // its exhaustion against the CURRENT limiting edge's ceiling
            // authors the stall fact; wakes and chunk boundaries never
            // reset it.
            let started = *backpressure_stall.get_or_insert_with(tokio::time::Instant::now);
            let detail = backpressure_writer
                .limiting_detail()
                .expect("a blocked writer has enforced downstream edges");
            let elapsed = started.elapsed();

            if elapsed >= detail.stall_timeout {
                emit_stalled_fact(
                    stage_id,
                    flow_context,
                    &detail,
                    elapsed,
                    data_journal,
                    system_journal,
                    instrumentation,
                )
                .await;
                emit_poison_eof(stage_id, flow_context, data_journal, instrumentation).await;
                return Err(format!(
                    "backpressure.stalled: edge {stage_id}|>{} exceeded its stall ceiling \
                     ({} ms elapsed, timeout {} ms, window {}, in flight {})",
                    detail.downstream,
                    elapsed.as_millis(),
                    detail.stall_timeout.as_millis(),
                    detail.window,
                    detail.in_flight,
                )
                .into());
            }

            let bound = (detail.stall_timeout - elapsed).min(CONTROL_RESPONSIVENESS_CAP);
            let waker = backpressure_writer
                .credit_waker()
                .expect("a blocked writer has backpressure state");
            return Ok(DrainAttempt::BackedOff { bound, waker });
        };

        committer
            .commit_prebuilt(
                pending.event,
                pending_parent,
                CommitOptions {
                    count_output: true,
                    validate_output_contract: false,
                },
            )
            .await
            .map_err(|e| format!("Failed to write pending output: {e}"))?;

        reservation.commit(1);
        *backpressure_stall = None;

        Ok(DrainAttempt::Committed { was_data: true })
    } else {
        // Non-data events bypass credit gating.
        committer
            .commit_prebuilt(
                pending.event,
                pending_parent,
                CommitOptions {
                    count_output: false,
                    validate_output_contract: false,
                },
            )
            .await
            .map_err(|e| format!("Failed to write pending output: {e}"))?;

        Ok(DrainAttempt::Committed { was_data: false })
    }
}

/// Emit a bypass-mode activity pulse when credit would have been zero.
async fn emit_bypass_pulse_if_needed(
    stage_id: StageId,
    flow_context: &FlowContext,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
) {
    if !BackpressureWriter::is_bypass_enabled() {
        return;
    }

    let Some((min_credit, limiting)) = backpressure_writer.min_downstream_credit_detail() else {
        return;
    };

    if min_credit >= 1 {
        return;
    }

    backpressure_pulse.record_delay(std::time::Duration::ZERO, Some(min_credit), Some(limiting));

    if let Some(pulse) = backpressure_pulse.maybe_emit() {
        let event = ChainEventFactory::observability_event(
            WriterId::from(stage_id),
            ObservabilityPayload::Middleware(MiddlewareLifecycle::Backpressure(pulse)),
        )
        .with_flow_context(flow_context.clone())
        .with_runtime_context(instrumentation.snapshot_with_control());

        match data_journal.append(event, None).await {
            Ok(written) => {
                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                    &written,
                    system_journal,
                )
                .await;
            }
            Err(e) => tracing::warn!(
                journal_error = %e,
                "Failed to append backpressure activity pulse"
            ),
        }
    }
}

/// Author the `backpressure.stalled` fact (FLOWIP-115e): the continuous
/// stall exceeded the limiting edge's ceiling. Live-scoped only; the caller
/// then emits poison EOF and fails the stage through the dispatch error path.
async fn emit_stalled_fact(
    stage_id: StageId,
    flow_context: &FlowContext,
    detail: &LimitingEdgeDetail,
    elapsed: std::time::Duration,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
) {
    let event = ChainEventFactory::observability_event(
        WriterId::from(stage_id),
        ObservabilityPayload::Middleware(MiddlewareLifecycle::Backpressure(
            BackpressureEvent::Stalled {
                upstream: stage_id,
                downstream: detail.downstream,
                window: detail.window,
                stall_timeout_ms: detail.stall_timeout.as_millis().min(u64::MAX as u128) as u64,
                elapsed_ms: elapsed.as_millis().min(u64::MAX as u128) as u64,
                in_flight: detail.in_flight,
            },
        )),
    )
    .with_flow_context(flow_context.clone())
    .with_runtime_context(instrumentation.snapshot_with_control());

    match data_journal.append(event, None).await {
        Ok(written) => {
            crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                &written,
                system_journal,
            )
            .await;
        }
        Err(e) => tracing::warn!(
            journal_error = %e,
            "Failed to append backpressure.stalled fact"
        ),
    }
}

/// Pass the existing Jonestown poison pill downstream before the stage fails.
async fn emit_poison_eof(
    stage_id: StageId,
    flow_context: &FlowContext,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
) {
    let writer_id = WriterId::from(stage_id);
    let runtime_context = instrumentation.snapshot_with_control();
    let writer_seq_by_event_type = instrumentation.data_writer_seq_by_event_type();

    let mut event = ChainEventFactory::eof_event_with_kind(writer_id, EofKind::Poison);
    if let obzenflow_core::event::ChainEventContent::FlowControl(FlowControlPayload::Eof {
        writer_id: ref mut eof_writer,
        writer_seq,
        writer_seq_by_event_type: eof_writer_seq_by_event_type,
        last_event_id,
        ..
    }) = &mut event.content
    {
        *eof_writer = Some(writer_id);
        *writer_seq = Some(SeqNo(runtime_context.writer_seq));
        *eof_writer_seq_by_event_type = writer_seq_by_event_type;
        *last_event_id = runtime_context.last_emitted_event_id;
    }

    event.flow_context = flow_context.clone();
    event.runtime_context = Some(runtime_context);
    instrumentation.record_emitted(&event);

    if let Err(e) = data_journal.append(event, None).await {
        tracing::warn!(
            journal_error = %e,
            "Failed to append backpressure poison EOF"
        );
    }
}

/// Feed the blocked-state pulse after one chunk of the credit wait, with the
/// measured blocked time (a wake can cut the bound short). The coalescer
/// still emits at most one pulse per second. Shared by the sync drain and the
/// async-source wait so both paths append and mirror identically.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_blocked_pulse(
    stage_id: StageId,
    flow_context: &FlowContext,
    measured: std::time::Duration,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
) {
    if let Some((min_credit, limiting)) = backpressure_writer.min_downstream_credit_detail() {
        backpressure_pulse.record_delay(measured, Some(min_credit), Some(limiting));
    } else {
        backpressure_pulse.record_delay(measured, None, None);
    }

    if let Some(pulse) = backpressure_pulse.maybe_emit() {
        let event = ChainEventFactory::observability_event(
            WriterId::from(stage_id),
            ObservabilityPayload::Middleware(MiddlewareLifecycle::Backpressure(pulse)),
        )
        .with_flow_context(flow_context.clone())
        .with_runtime_context(instrumentation.snapshot_with_control());

        match data_journal.append(event, None).await {
            Ok(written) => {
                crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
                    &written,
                    system_journal,
                )
                .await;
            }
            Err(e) => tracing::warn!(
                journal_error = %e,
                "Failed to append backpressure activity pulse"
            ),
        }
    }
}
