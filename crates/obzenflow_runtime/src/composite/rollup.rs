// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The pure composite lifecycle fold (FLOWIP-128a B1/B2).
//!
//! `CompositeRollup` folds a composite's member `StageLifecycleEvent`s into the
//! composite's own aggregate `CompositeLifecycleEvent`, using the
//! PipelineSupervisor's rollup rules scoped to the members:
//!
//! - `Running` when the first member reaches `Running`;
//! - `Failed` fail-fast on any member `StageLifecycle::Failed`;
//! - `Cancelled`/`Completed` resolved once every member is terminal, with
//!   precedence `Failed` > `Cancelled` > `Completed`;
//! - at most one terminal is emitted per composite (the latch).
//!
//! The terminal state is a function of the multiset of member terminal states,
//! so it is order-insensitive. This type performs no I/O; the supervisor owns
//! the subscription and the journal writes.

use obzenflow_core::event::{CompositeLifecycleEvent, StageLifecycleEvent};
use obzenflow_core::id::{CompositeId, RoleId, StageId};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberTerminal {
    Completed,
    Cancelled,
}

/// The per-composite lifecycle fold. One instance per composite, owned by that
/// composite's monitor-supervisor.
pub struct CompositeRollup {
    composite_id: CompositeId,
    members: HashSet<StageId>,
    role_of: HashMap<StageId, RoleId>,
    terminal_of: HashMap<StageId, MemberTerminal>,
    first_cancel_reason: Option<String>,
    started: bool,
    latched: bool,
}

impl CompositeRollup {
    /// Build from the composite identity and its `(member stage, role)` set
    /// (resolved from manifest subgraph membership by the caller).
    pub fn new(composite_id: CompositeId, members: Vec<(StageId, RoleId)>) -> Self {
        let mut member_set = HashSet::with_capacity(members.len());
        let mut role_of = HashMap::with_capacity(members.len());
        for (stage, role) in members {
            member_set.insert(stage);
            role_of.insert(stage, role);
        }
        Self {
            composite_id,
            members: member_set,
            role_of,
            terminal_of: HashMap::new(),
            first_cancel_reason: None,
            started: false,
            latched: false,
        }
    }

    /// The composite this fold speaks for.
    pub fn composite_id(&self) -> &CompositeId {
        &self.composite_id
    }

    /// True once a terminal composite fact has been emitted.
    pub fn is_terminal(&self) -> bool {
        self.latched
    }

    /// Fold one member lifecycle transition. Returns the composite transition it
    /// advances, if any. Events for non-member stages, transitional states, and
    /// anything after the terminal latch return `None`.
    pub fn observe(
        &mut self,
        stage: StageId,
        event: &StageLifecycleEvent,
    ) -> Option<CompositeLifecycleEvent> {
        if self.latched || !self.members.contains(&stage) {
            return None;
        }

        match event {
            StageLifecycleEvent::Running => {
                if self.started {
                    None
                } else {
                    self.started = true;
                    Some(CompositeLifecycleEvent::Running)
                }
            }
            // Fail-fast: highest precedence, emitted on first sight.
            StageLifecycleEvent::Failed { error, .. } => {
                self.latched = true;
                let at = self
                    .role_of
                    .get(&stage)
                    .cloned()
                    .unwrap_or_else(|| RoleId::new("unknown"));
                Some(CompositeLifecycleEvent::Failed {
                    at,
                    error: error.clone(),
                })
            }
            // Cancelled and Completed are resolved at all-terminal, not fail-fast.
            StageLifecycleEvent::Completed { .. } | StageLifecycleEvent::Drained => {
                self.terminal_of.insert(stage, MemberTerminal::Completed);
                self.resolve_if_all_terminal()
            }
            StageLifecycleEvent::Cancelled { reason, .. } => {
                if self.first_cancel_reason.is_none() {
                    self.first_cancel_reason = Some(reason.clone());
                }
                self.terminal_of.insert(stage, MemberTerminal::Cancelled);
                self.resolve_if_all_terminal()
            }
            // Transitional; does not advance the composite.
            StageLifecycleEvent::Draining { .. } => None,
        }
    }

    fn resolve_if_all_terminal(&mut self) -> Option<CompositeLifecycleEvent> {
        if self.terminal_of.len() < self.members.len() {
            return None;
        }
        self.latched = true;
        let any_cancelled = self
            .terminal_of
            .values()
            .any(|t| matches!(t, MemberTerminal::Cancelled));
        if any_cancelled {
            let reason = self
                .first_cancel_reason
                .clone()
                .unwrap_or_else(|| "member cancelled".to_string());
            Some(CompositeLifecycleEvent::Cancelled { reason })
        } else {
            // Live metrics ride the rail; the completion summary is optional.
            Some(CompositeLifecycleEvent::Completed { metrics: None })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::id::StageId;

    fn cid() -> CompositeId {
        CompositeId::new("ai_map_reduce:digest")
    }

    fn running() -> StageLifecycleEvent {
        StageLifecycleEvent::Running
    }
    fn completed() -> StageLifecycleEvent {
        StageLifecycleEvent::Completed { metrics: None }
    }
    fn cancelled(reason: &str) -> StageLifecycleEvent {
        StageLifecycleEvent::Cancelled {
            reason: reason.to_string(),
            metrics: None,
        }
    }
    fn failed(error: &str) -> StageLifecycleEvent {
        StageLifecycleEvent::Failed {
            error: error.to_string(),
            recoverable: None,
            metrics: None,
        }
    }

    /// Two members with roles, returning the stage ids for driving the fold.
    fn two_member_rollup() -> (CompositeRollup, StageId, StageId) {
        let a = StageId::new();
        let b = StageId::new();
        let rollup = CompositeRollup::new(
            cid(),
            vec![(a, RoleId::new("map")), (b, RoleId::new("finalize"))],
        );
        (rollup, a, b)
    }

    #[test]
    fn running_emitted_once_on_first_member() {
        let (mut r, a, b) = two_member_rollup();
        assert!(matches!(
            r.observe(a, &running()),
            Some(CompositeLifecycleEvent::Running)
        ));
        // Second member running does not re-emit.
        assert!(r.observe(b, &running()).is_none());
    }

    #[test]
    fn completes_only_when_all_members_terminal() {
        let (mut r, a, b) = two_member_rollup();
        r.observe(a, &running());
        r.observe(b, &running());
        assert!(r.observe(a, &completed()).is_none(), "one member left");
        assert!(matches!(
            r.observe(b, &completed()),
            Some(CompositeLifecycleEvent::Completed { .. })
        ));
        assert!(r.is_terminal());
    }

    #[test]
    fn failed_is_fail_fast_without_waiting_for_siblings() {
        let (mut r, a, _b) = two_member_rollup();
        r.observe(a, &running());
        let ev = r.observe(a, &failed("boom"));
        match ev {
            Some(CompositeLifecycleEvent::Failed { at, error }) => {
                assert_eq!(at.as_str(), "map");
                assert_eq!(error, "boom");
            }
            other => panic!("expected fail-fast Failed, got {other:?}"),
        }
        assert!(r.is_terminal());
    }

    #[test]
    fn precedence_failed_outranks_a_prior_cancel() {
        // Cancelled on one member then Failed on another yields Failed, because
        // Cancelled is not latched until all-terminal while Failed is fail-fast.
        let (mut r, a, b) = two_member_rollup();
        r.observe(a, &running());
        r.observe(b, &running());
        assert!(r.observe(a, &cancelled("stopped")).is_none());
        assert!(matches!(
            r.observe(b, &failed("boom")),
            Some(CompositeLifecycleEvent::Failed { .. })
        ));
    }

    #[test]
    fn clean_stop_all_cancelled_yields_cancelled_not_failed_or_stuck() {
        let (mut r, a, b) = two_member_rollup();
        r.observe(a, &running());
        r.observe(b, &running());
        assert!(r.observe(a, &cancelled("operator stop")).is_none());
        match r.observe(b, &cancelled("operator stop")) {
            Some(CompositeLifecycleEvent::Cancelled { reason }) => {
                assert_eq!(reason, "operator stop");
            }
            other => panic!("expected Cancelled, got {other:?}"),
        }
    }

    #[test]
    fn terminal_latch_ignores_events_after_terminal() {
        let (mut r, a, b) = two_member_rollup();
        r.observe(a, &running());
        r.observe(a, &failed("boom"));
        // Sibling completing after the composite already failed is ignored.
        assert!(r.observe(b, &completed()).is_none());
        assert!(r.observe(b, &running()).is_none());
    }

    #[test]
    fn terminal_state_is_order_insensitive() {
        // Same terminal member multiset in two orders -> same composite terminal.
        let run_order = |order: &[(usize, StageLifecycleEvent)]| {
            let a = StageId::new();
            let b = StageId::new();
            let mut r = CompositeRollup::new(
                cid(),
                vec![(a, RoleId::new("map")), (b, RoleId::new("finalize"))],
            );
            let stages = [a, b];
            let mut last = None;
            for (idx, ev) in order {
                if let Some(e) = r.observe(stages[*idx], ev) {
                    last = Some(e);
                }
            }
            last
        };
        let forward = run_order(&[
            (0, running()),
            (1, running()),
            (0, completed()),
            (1, completed()),
        ]);
        let reverse = run_order(&[
            (1, running()),
            (0, running()),
            (1, completed()),
            (0, completed()),
        ]);
        assert!(matches!(
            forward,
            Some(CompositeLifecycleEvent::Completed { .. })
        ));
        assert!(matches!(
            reverse,
            Some(CompositeLifecycleEvent::Completed { .. })
        ));
    }

    #[test]
    fn non_member_events_are_ignored() {
        let (mut r, a, _b) = two_member_rollup();
        let stranger = StageId::new();
        assert!(r.observe(stranger, &running()).is_none());
        assert!(r.observe(stranger, &failed("boom")).is_none());
        // The composite is unaffected: its own member can still start it.
        assert!(matches!(
            r.observe(a, &running()),
            Some(CompositeLifecycleEvent::Running)
        ));
    }

    #[test]
    fn drained_counts_as_completed_terminal() {
        let (mut r, a, b) = two_member_rollup();
        r.observe(a, &running());
        r.observe(b, &running());
        r.observe(a, &StageLifecycleEvent::Drained);
        assert!(matches!(
            r.observe(b, &StageLifecycleEvent::Drained),
            Some(CompositeLifecycleEvent::Completed { .. })
        ));
    }
}
