// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Control event resolution helpers.
//!
//! Pure/sync functions that compute what a supervisor should do when it
//! encounters EOF or Drain signals. The actual execution (forwarding, state
//! transitions) is left to the caller.

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::{ChainEvent, EventEnvelope, StageId};

use crate::messaging::upstream_subscription::EofOutcome;
use crate::pipeline::config::CycleGuardConfig;
use crate::stages::common::control_strategies::dispatch::dispatch_control_signal;
use crate::stages::common::control_strategies::{
    ControlEventAction, ControlEventStrategy, ProcessingContext,
};
use crate::stages::common::cycle_guard::CycleGuard;

/// What the supervisor should do after resolving a control event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ControlResolution {
    /// Forward the control event downstream and continue in the current state.
    Forward,
    /// Forward the control event downstream and then transition to Draining.
    ForwardAndDrain,
    /// Buffer this terminal signal at an SCC entry point (FLOWIP-051n).
    ///
    /// The caller is responsible for storing the envelope, updating any
    /// entry-point bookkeeping, and eventually releasing the buffered signal
    /// once the SCC is quiescent.
    BufferAtEntryPoint { is_drain: bool },
    /// Suppress the event entirely (e.g. internal cycle peer or non-terminal forwarded EOF).
    Suppress,
    /// Delay by the given duration, then forward.
    Delay(std::time::Duration),
    /// Skip the event entirely (dangerous, used by control strategies).
    Skip,
}

/// Resolve a control event into a `ControlResolution`.
///
/// This function performs the "resolve" phase (FLOWIP-051m): it computes the
/// decision synchronously without performing any I/O.
#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_control_event(
    signal: &FlowControlPayload,
    envelope: &EventEnvelope<ChainEvent>,
    strategy: &dyn ControlEventStrategy,
    cycle_config: Option<&CycleGuardConfig>,
    cycle_guard: Option<&mut CycleGuard>,
    eof_outcome: Option<&EofOutcome>,
    upstream_stage: Option<StageId>,
    contract_reader_count: usize,
    drain_is_terminal: bool,
) -> ControlResolution {
    let mut processing_ctx = ProcessingContext::new();
    let action = dispatch_control_signal(strategy, signal, envelope, &mut processing_ctx);

    match action {
        ControlEventAction::Delay(duration) => ControlResolution::Delay(duration),
        ControlEventAction::Skip => ControlResolution::Skip,
        ControlEventAction::Forward => resolve_forward_control_event(
            signal,
            envelope,
            cycle_config,
            cycle_guard,
            eof_outcome,
            upstream_stage,
            contract_reader_count,
            drain_is_terminal,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn resolve_forward_control_event(
    signal: &FlowControlPayload,
    envelope: &EventEnvelope<ChainEvent>,
    cycle_config: Option<&CycleGuardConfig>,
    mut cycle_guard: Option<&mut CycleGuard>,
    eof_outcome: Option<&EofOutcome>,
    upstream_stage: Option<StageId>,
    contract_reader_count: usize,
    drain_is_terminal: bool,
) -> ControlResolution {
    if envelope.event.is_eof() {
        // The SCC entry point buffers external EOF and suppresses all other EOF
        // signals, so it does not participate in cycle-boundary drain readiness.
        let is_entry_point = cycle_config.is_some_and(|cfg| cfg.is_entry_point);
        if !is_entry_point && is_terminal_eof(envelope, upstream_stage) {
            if let (Some(guard), Some(upstream)) = (cycle_guard.as_deref_mut(), upstream_stage) {
                guard.note_upstream_eof(upstream);
            }
        }

        return resolve_eof(
            envelope,
            cycle_config,
            cycle_guard.as_deref(),
            eof_outcome,
            upstream_stage,
            contract_reader_count,
        );
    }

    if matches!(signal, FlowControlPayload::Drain) {
        return resolve_drain(cycle_config, drain_is_terminal);
    }

    ControlResolution::Forward
}

/// Resolve an EOF event into a `ControlResolution`.
///
pub(crate) fn resolve_eof(
    envelope: &EventEnvelope<ChainEvent>,
    cycle_guard_config: Option<&CycleGuardConfig>,
    cycle_guard: Option<&CycleGuard>,
    eof_outcome: Option<&EofOutcome>,
    upstream_stage: Option<StageId>,
    contract_reader_count: usize,
) -> ControlResolution {
    let is_terminal_eof = is_terminal_eof(envelope, upstream_stage);

    // SCC entry point logic (FLOWIP-051n).
    if let Some(cfg) = cycle_guard_config {
        if cfg.is_entry_point {
            if !is_terminal_eof {
                return ControlResolution::Suppress;
            }

            if let Some(upstream) = upstream_stage {
                if cfg.external_upstreams.contains(&upstream) {
                    return ControlResolution::BufferAtEntryPoint { is_drain: false };
                }
            }

            // Internal or unknown upstream EOF at entry point: suppress.
            return ControlResolution::Suppress;
        }
    }

    // Cycle drain boundary (FLOWIP-051l).
    if cycle_guard.is_some_and(|guard| guard.has_seen_all_upstream_eofs(contract_reader_count)) {
        return ControlResolution::ForwardAndDrain;
    }

    // Normal (non-cycle) EOF resolution.
    match eof_outcome {
        Some(outcome) if outcome.is_final => ControlResolution::ForwardAndDrain,
        _ => ControlResolution::Forward,
    }
}

/// Resolve a Drain signal into a `ControlResolution`.
pub(crate) fn resolve_drain(
    cycle_guard_config: Option<&CycleGuardConfig>,
    drain_is_terminal: bool,
) -> ControlResolution {
    if let Some(cfg) = cycle_guard_config {
        if cfg.is_entry_point {
            return ControlResolution::BufferAtEntryPoint { is_drain: true };
        }
    }

    if drain_is_terminal {
        ControlResolution::ForwardAndDrain
    } else {
        ControlResolution::Forward
    }
}

pub(crate) fn is_terminal_eof(
    envelope: &EventEnvelope<ChainEvent>,
    upstream_stage: Option<StageId>,
) -> bool {
    let Some(upstream) = upstream_stage else {
        return true;
    };

    match &envelope.event.content {
        obzenflow_core::event::ChainEventContent::FlowControl(FlowControlPayload::Eof {
            writer_id,
            ..
        }) => match writer_id {
            Some(obzenflow_core::WriterId::Stage(eof_stage)) => *eof_stage == upstream,
            Some(_) => false,
            None => true,
        },
        _ => false,
    }
}
