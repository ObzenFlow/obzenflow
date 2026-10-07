// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Optional elapsed-time accounting for the one supervisor runner.
//!
//! The clock follows disjoint runner intervals, including suspension and resume
//! delay. It does not measure CPU time. A retained operation is charged only
//! while its select interval is active, never again for its total lifetime.

use obzenflow_core::event::payloads::supervisor_descriptor::{SupervisionMode, SupervisorKind};
use obzenflow_core::WriterId;
use std::time::Instant;
use tracing::Span;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Phase {
    InlineAction,
    PendingAction,
    PendingDispatch,
    DirectDispatch,
    Transition,
    Yield,
    Residual,
}

const PHASE_COUNT: usize = 7;

/// Which ready branch ended a retained select interval. These counts describe
/// selection, not a second elapsed interval or the lifetime of retained work.
#[derive(Clone, Copy, Debug)]
pub(super) enum Selection {
    PublicationFailure,
    Control,
    ActionComplete,
    DispatchComplete,
}

#[derive(Clone, Debug, Default)]
struct SelectionCounts {
    pending_action_publication_failure_wins: u64,
    pending_action_control_wins: u64,
    pending_action_complete_wins: u64,
    pending_dispatch_publication_failure_wins: u64,
    pending_dispatch_control_wins: u64,
    pending_dispatch_complete_wins: u64,
}

#[derive(Clone, Debug, Default)]
struct Totals {
    elapsed_ns: u64,
    outer_turns: u64,
    nanoseconds: [u64; PHASE_COUNT],
    entries: [u64; PHASE_COUNT],
    selections: SelectionCounts,
}

struct StateTotals {
    state: String,
    totals: Totals,
}

struct Accumulator {
    states: Vec<StateTotals>,
    current: usize,
    phase: Phase,
    changed_at: Instant,
}

impl Accumulator {
    fn new(state: &str, now: Instant) -> Self {
        let mut totals = Totals::default();
        totals.entries[Phase::Residual as usize] = 1;
        Self {
            states: vec![StateTotals {
                state: state.into(),
                totals,
            }],
            current: 0,
            phase: Phase::Residual,
            changed_at: now,
        }
    }

    fn charge(&mut self, now: Instant) {
        let elapsed = u64::try_from(now.duration_since(self.changed_at).as_nanos())
            .expect("supervisor timing interval exceeds u64 nanoseconds");
        let totals = &mut self.states[self.current].totals;
        totals.elapsed_ns += elapsed;
        totals.nanoseconds[self.phase as usize] += elapsed;
        self.changed_at = now;
    }

    fn phase(&mut self, phase: Phase, now: Instant) {
        self.charge(now);
        self.phase = phase;
        self.states[self.current].totals.entries[phase as usize] += 1;
    }

    fn state(&mut self, state: &str, now: Instant) {
        self.charge(now);
        debug_assert_eq!(self.phase, Phase::Residual);
        if self.states[self.current].state == state {
            return;
        }
        self.current = match self.states.iter().position(|bucket| bucket.state == state) {
            Some(index) => index,
            None => {
                self.states.push(StateTotals {
                    state: state.into(),
                    totals: Totals::default(),
                });
                self.states.len() - 1
            }
        };
        self.states[self.current].totals.entries[Phase::Residual as usize] += 1;
    }
}

struct Enabled {
    supervisor: String,
    kind: SupervisorKind,
    writer_id: WriterId,
    mode: &'static str,
    dispatch: tracing::Dispatch,
    accumulator: Option<Accumulator>,
    state: String,
    // Child handles close before their root. No entered guard survives an await.
    state_span: Span,
    root: Span,
}

/// No allocation or clock read when both diagnostic targets are disabled.
pub(super) struct RunnerTiming {
    enabled: Option<Box<Enabled>>,
    interrupted: bool,
}

impl RunnerTiming {
    pub(super) fn new(
        supervisor: &str,
        kind: SupervisorKind,
        writer_id: WriterId,
        mode: SupervisionMode,
        state: &str,
    ) -> Self {
        let timing =
            tracing::event_enabled!(target: "obzenflow::supervisor_timing", tracing::Level::DEBUG);
        let performance =
            tracing::span_enabled!(target: "obzenflow::performance", tracing::Level::DEBUG);
        let enabled = (timing || performance).then(|| {
            let mode = match mode {
                SupervisionMode::SelfSupervised => "self_supervised",
                SupervisionMode::HandlerSupervised => "handler_supervised",
            };
            let root = tracing::debug_span!(target: "obzenflow::performance", "supervisor",
                supervisor, supervisor_kind = ?kind, writer_id = %writer_id, supervision_mode = mode);
            let state_span = tracing::debug_span!(target: "obzenflow::performance",
                parent: &root, "supervisor_state", state);
            Box::new(Enabled {
                supervisor: supervisor.into(),
                kind,
                writer_id,
                mode,
                dispatch: tracing::dispatcher::get_default(Clone::clone),
                accumulator: timing.then(|| Accumulator::new(state, Instant::now())),
                state: state.into(),
                state_span,
                root,
            })
        });
        Self {
            enabled,
            interrupted: true,
        }
    }

    pub(super) fn root(&self) -> Span {
        self.enabled
            .as_ref()
            .map_or_else(Span::none, |enabled| enabled.root.clone())
    }

    pub(super) fn state(&mut self, state: &str) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        if enabled.state == state {
            return;
        }
        if let Some(accumulator) = &mut enabled.accumulator {
            accumulator.state(state, Instant::now());
        }
        enabled.state = state.into();
        enabled.state_span = tracing::debug_span!(target: "obzenflow::performance",
            parent: &enabled.root, "supervisor_state", state);
    }

    pub(super) fn turn(&mut self) {
        if let Some(accumulator) = self
            .enabled
            .as_mut()
            .and_then(|enabled| enabled.accumulator.as_mut())
        {
            accumulator.states[accumulator.current].totals.outer_turns += 1;
        }
    }

    pub(super) fn phase(&mut self, phase: Phase) -> PhaseGuard<'_> {
        let Some(enabled) = self.enabled.as_mut() else {
            return PhaseGuard {
                accumulator: None,
                span: Span::none(),
            };
        };
        if let Some(accumulator) = &mut enabled.accumulator {
            accumulator.phase(phase, Instant::now());
        }
        let parent = &enabled.state_span;
        let span = match phase {
            Phase::InlineAction => {
                tracing::debug_span!(target: "obzenflow::performance", parent: parent, "inline_action")
            }
            Phase::PendingAction => {
                tracing::debug_span!(target: "obzenflow::performance", parent: parent, "pending_action")
            }
            Phase::PendingDispatch => {
                tracing::debug_span!(target: "obzenflow::performance", parent: parent, "pending_dispatch")
            }
            Phase::DirectDispatch => {
                tracing::debug_span!(target: "obzenflow::performance", parent: parent, "direct_dispatch")
            }
            Phase::Transition => {
                tracing::debug_span!(target: "obzenflow::performance", parent: parent, "transition")
            }
            Phase::Yield => {
                tracing::debug_span!(target: "obzenflow::performance", parent: parent, "yield")
            }
            Phase::Residual => Span::none(),
        };
        PhaseGuard {
            accumulator: enabled.accumulator.as_mut(),
            span,
        }
    }

    pub(super) fn complete(&mut self) {
        self.interrupted = false;
    }
}

impl Drop for RunnerTiming {
    fn drop(&mut self) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        let Some(accumulator) = enabled.accumulator.as_mut() else {
            return;
        };
        accumulator.charge(Instant::now());
        // One bounded summary per actual state variant, including revisits.
        tracing::dispatcher::with_default(&enabled.dispatch, || {
            for bucket in &accumulator.states {
                let totals = &bucket.totals;
                let ns = &totals.nanoseconds;
                let entries = &totals.entries;
                let selections = &totals.selections;
                tracing::debug!(target: "obzenflow::supervisor_timing",
                supervisor = %enabled.supervisor, supervisor_kind = ?enabled.kind, writer_id = %enabled.writer_id,
                supervision_mode = enabled.mode, state = %bucket.state,
                elapsed_ns = totals.elapsed_ns, outer_turns = totals.outer_turns,
                inline_action_ns = ns[0], pending_action_ns = ns[1], pending_dispatch_ns = ns[2],
                direct_dispatch_ns = ns[3], transition_ns = ns[4], yield_ns = ns[5], residual_ns = ns[6],
                inline_action_entries = entries[0], pending_action_entries = entries[1], pending_dispatch_entries = entries[2],
                direct_dispatch_entries = entries[3], transition_entries = entries[4], yield_entries = entries[5], residual_entries = entries[6],
                pending_action_publication_failure_wins = selections.pending_action_publication_failure_wins,
                pending_action_control_wins = selections.pending_action_control_wins,
                pending_action_complete_wins = selections.pending_action_complete_wins,
                pending_dispatch_publication_failure_wins = selections.pending_dispatch_publication_failure_wins,
                pending_dispatch_control_wins = selections.pending_dispatch_control_wins,
                pending_dispatch_complete_wins = selections.pending_dispatch_complete_wins,
                interrupted = self.interrupted,
                "supervisor_loop_summary");
            }
        });
    }
}

pub(super) struct PhaseGuard<'a> {
    accumulator: Option<&'a mut Accumulator>,
    span: Span,
}

impl PhaseGuard<'_> {
    pub(super) fn span(&self) -> Span {
        self.span.clone()
    }

    /// Call only in the ready branch, after select futures own their span clones.
    /// A cancelled select has no winner and does not increment these counts.
    pub(super) fn selected(&mut self, selection: Selection) {
        let Some(accumulator) = &mut self.accumulator else {
            return;
        };
        let selections = &mut accumulator.states[accumulator.current].totals.selections;
        let counter = match (accumulator.phase, selection) {
            (Phase::PendingAction, Selection::PublicationFailure) => {
                &mut selections.pending_action_publication_failure_wins
            }
            (Phase::PendingAction, Selection::Control) => {
                &mut selections.pending_action_control_wins
            }
            (Phase::PendingAction, Selection::ActionComplete) => {
                &mut selections.pending_action_complete_wins
            }
            (Phase::PendingDispatch, Selection::PublicationFailure) => {
                &mut selections.pending_dispatch_publication_failure_wins
            }
            (Phase::PendingDispatch, Selection::Control) => {
                &mut selections.pending_dispatch_control_wins
            }
            (Phase::PendingDispatch, Selection::DispatchComplete) => {
                &mut selections.pending_dispatch_complete_wins
            }
            _ => unreachable!("selection must belong to its guarded pending phase"),
        };
        *counter += 1;
    }
}

impl Drop for PhaseGuard<'_> {
    fn drop(&mut self) {
        if let Some(accumulator) = &mut self.accumulator {
            accumulator.phase(Phase::Residual, Instant::now());
        }
    }
}

#[cfg(test)]
mod tests;
