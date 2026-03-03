// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Backpressure-aware pending output drain loop
//!
//! `drain_one_pending()` handles the full bypass-pulse / reserve-or-requeue /
//! commit-and-reset cycle for a single pending output event. Supervisors call
//! this in a `while let` loop to flush their `pending_outputs` queue.

use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::supervised_base::idle_backoff::IdleBackoff;
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::payloads::observability_payload::{
    MiddlewareLifecycle, ObservabilityPayload,
};
use obzenflow_core::event::{ChainEventFactory, EventEnvelope};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use std::sync::Arc;

use crate::backpressure::BackpressureWriter;
use crate::metrics::instrumentation::StageInstrumentation;

/// Outcome of attempting to drain a single pending event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainOutcome {
    /// The event was successfully written to the journal.
    Committed {
        /// `true` if the written event was a data event (caller should call
        /// `subscription.track_output_event()`).
        was_data: bool,
    },
    /// Backpressure prevented writing; the event has been pushed back to the
    /// front of the queue and the caller should return early (after sleeping).
    BackedOff,
}

/// Outcome of a single drain attempt without sleeping.
///
/// This is used by supervisors that need to integrate backoff sleeps into a
/// larger `tokio::select!` (for example, async source supervisors that must
/// remain responsive to external control events while blocked).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DrainAttempt {
    /// The event was successfully written to the journal.
    Committed {
        /// `true` if the written event was a data event (caller should call
        /// `subscription.track_output_event()`).
        was_data: bool,
    },
    /// Backpressure prevented writing; the event has been pushed back to the
    /// front of the queue. The caller is responsible for sleeping for `delay`.
    BackedOff { delay: std::time::Duration },
}

/// Attempt to drain a single pending output event through the backpressure
/// gate, writing it to the data journal if credit is available.
///
/// The caller supplies all the context fields individually so that this helper
/// does not require a reference to the full FSM context. This avoids borrow
/// conflicts when the subscription is held separately.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_one_pending(
    pending: ChainEvent,
    flow_context: &FlowContext,
    stage_id: StageId,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    pending_parent: Option<&EventEnvelope<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_backoff: &mut IdleBackoff,
    pending_outputs: &mut std::collections::VecDeque<ChainEvent>,
) -> Result<DrainOutcome, Box<dyn std::error::Error + Send + Sync>> {
    match drain_one_pending_resolve(
        pending,
        flow_context,
        stage_id,
        data_journal,
        system_journal,
        pending_parent,
        instrumentation,
        backpressure_writer,
        backpressure_pulse,
        backpressure_backoff,
        pending_outputs,
    )
    .await?
    {
        DrainAttempt::Committed { was_data } => Ok(DrainOutcome::Committed { was_data }),
        DrainAttempt::BackedOff { delay } => {
            tokio::time::sleep(delay).await;
            Ok(DrainOutcome::BackedOff)
        }
    }
}

/// Attempt to drain a single pending output event through the backpressure
/// gate, writing it to the data journal if credit is available.
///
/// Unlike `drain_one_pending`, this function never sleeps. If backpressure
/// blocks the write, it returns `DrainAttempt::BackedOff { delay }` and the
/// caller is responsible for deciding how to wait (for example, using
/// `tokio::select!` to remain responsive to external events).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn drain_one_pending_resolve(
    pending: ChainEvent,
    flow_context: &FlowContext,
    stage_id: StageId,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    pending_parent: Option<&EventEnvelope<ChainEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
    backpressure_backoff: &mut IdleBackoff,
    pending_outputs: &mut std::collections::VecDeque<ChainEvent>,
) -> Result<DrainAttempt, Box<dyn std::error::Error + Send + Sync>> {
    let is_data = pending.is_data();

    if is_data {
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
            let delay = backpressure_backoff.next_delay();
            backpressure_writer.record_wait(delay);

            emit_backoff_pulse(
                stage_id,
                flow_context,
                delay,
                data_journal,
                system_journal,
                instrumentation,
                backpressure_writer,
                backpressure_pulse,
            )
            .await;

            return Ok(DrainAttempt::BackedOff { delay });
        };

        let enriched = pending.with_flow_context(flow_context.clone());
        if enriched.is_data() {
            instrumentation.record_output_event(&enriched);
        }
        let enriched = enriched.with_runtime_context(instrumentation.snapshot_with_control());

        let written = data_journal
            .append(enriched, pending_parent)
            .await
            .map_err(|e| format!("Failed to write pending output: {e}"))?;
        reservation.commit(1);
        backpressure_backoff.reset();
        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
            &written,
            system_journal,
        )
        .await;

        Ok(DrainAttempt::Committed { was_data: true })
    } else {
        // Non-data events bypass credit gating.
        let enriched = pending
            .with_flow_context(flow_context.clone())
            .with_runtime_context(instrumentation.snapshot_with_control());

        let written = data_journal
            .append(enriched, pending_parent)
            .await
            .map_err(|e| format!("Failed to write pending output: {e}"))?;
        crate::stages::common::middleware_mirror::mirror_middleware_event_to_system_journal(
            &written,
            system_journal,
        )
        .await;

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

/// Emit an activity pulse when backing off due to insufficient credit.
#[allow(clippy::too_many_arguments)]
async fn emit_backoff_pulse(
    stage_id: StageId,
    flow_context: &FlowContext,
    delay: std::time::Duration,
    data_journal: &Arc<dyn Journal<ChainEvent>>,
    system_journal: &Arc<dyn Journal<obzenflow_core::event::SystemEvent>>,
    instrumentation: &Arc<StageInstrumentation>,
    backpressure_writer: &BackpressureWriter,
    backpressure_pulse: &mut BackpressureActivityPulse,
) {
    if let Some((min_credit, limiting)) = backpressure_writer.min_downstream_credit_detail() {
        backpressure_pulse.record_delay(delay, Some(min_credit), Some(limiting));
    } else {
        backpressure_pulse.record_delay(delay, None, None);
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
