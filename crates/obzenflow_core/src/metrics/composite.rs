// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Composite boundary metrics: the RED (rate, errors, duration) projection of a
//! composite as a black-box span (FLOWIP-128a B4).
//!
//! This is a pure projection over per-stage metrics, defined by the composite's
//! own boundary (entry-port member, exit-port member, and the member set) read
//! from the subgraph registry. The domain logic lives here as a unit-tested
//! function; the metrics aggregator and Prometheus exporter each provide a thin
//! `StageMetricsView` over their own maps and render the result uniformly.
//!
//! The composite already carries its structure, so this is pull-based: rate at
//! the exit port, input rate at the entry port, errors summed over members, and
//! boundary latency as the source-relative `exit − entry` difference (L1). The
//! exact per-activation entry-to-exit interval (L2) is FLOWIP-128c's
//! `CompositeSpan`, not this aggregate rail.

use crate::id::{CompositeId, StageId};
use serde::{Deserialize, Serialize};

/// A composite's boundary structure, derived once from the subgraph registry.
/// Pure data, no metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompositeBoundary {
    pub composite_id: CompositeId,
    /// Entry-port member stage.
    pub entry: StageId,
    /// Exit-port member stage.
    pub exit: StageId,
    /// All member stages of the composite.
    pub members: Vec<StageId>,
}

/// Read-only view over per-stage metrics. Keeps the projection free of the
/// concrete store or snapshot shape, and trivial to test with a fake view.
pub trait StageMetricsView {
    /// Events admitted (processed) by a member stage.
    fn events_in(&self, stage: StageId) -> u64;
    /// Events emitted by a member stage.
    fn events_out(&self, stage: StageId) -> u64;
    /// Errors observed at a member stage.
    fn errors(&self, stage: StageId) -> u64;
    /// End-to-end (correlation-relative) p95 latency at a member stage, in ms.
    fn latency_p95_ms(&self, stage: StageId) -> Option<u64>;
}

/// Composite RED metrics, the pure projection over per-stage metrics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompositeRed {
    /// Input rate proxy: events admitted at the entry-port member.
    pub events_in: u64,
    /// Output rate proxy: events emitted at the exit-port member.
    pub events_out: u64,
    /// Errors summed over all members.
    pub errors: u64,
    /// Boundary latency: the source-relative `exit − entry` interval (L1).
    /// `None` until both boundary members have a latency sample.
    pub boundary_latency_ms: Option<u64>,
}

impl CompositeRed {
    /// Project composite RED from a stage-metrics view, using the boundary to
    /// select the entry-port rate, exit-port rate, and boundary latency, and
    /// summing errors over the members.
    pub fn project(boundary: &CompositeBoundary, stages: &impl StageMetricsView) -> Self {
        CompositeRed {
            events_in: stages.events_in(boundary.entry),
            events_out: stages.events_out(boundary.exit),
            errors: boundary.members.iter().map(|&s| stages.errors(s)).sum(),
            boundary_latency_ms: match (
                stages.latency_p95_ms(boundary.exit),
                stages.latency_p95_ms(boundary.entry),
            ) {
                (Some(exit), Some(entry)) => Some(exit.saturating_sub(entry)),
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Default)]
    struct FakeView {
        events_in: HashMap<StageId, u64>,
        events_out: HashMap<StageId, u64>,
        errors: HashMap<StageId, u64>,
        latency: HashMap<StageId, u64>,
    }

    impl StageMetricsView for FakeView {
        fn events_in(&self, s: StageId) -> u64 {
            self.events_in.get(&s).copied().unwrap_or(0)
        }
        fn events_out(&self, s: StageId) -> u64 {
            self.events_out.get(&s).copied().unwrap_or(0)
        }
        fn errors(&self, s: StageId) -> u64 {
            self.errors.get(&s).copied().unwrap_or(0)
        }
        fn latency_p95_ms(&self, s: StageId) -> Option<u64> {
            self.latency.get(&s).copied()
        }
    }

    fn boundary(entry: StageId, exit: StageId, members: Vec<StageId>) -> CompositeBoundary {
        CompositeBoundary {
            composite_id: CompositeId::new("ai_map_reduce:digest"),
            entry,
            exit,
            members,
        }
    }

    #[test]
    fn rate_reads_entry_in_and_exit_out_not_a_sum() {
        let (entry, mid, exit) = (StageId::new(), StageId::new(), StageId::new());
        let mut v = FakeView::default();
        v.events_in.insert(entry, 100);
        v.events_in.insert(exit, 20); // must be ignored for input rate
        v.events_out.insert(exit, 5);
        v.events_out.insert(mid, 999); // must be ignored for output rate
        let red = CompositeRed::project(&boundary(entry, exit, vec![entry, mid, exit]), &v);
        assert_eq!(red.events_in, 100);
        assert_eq!(red.events_out, 5);
    }

    #[test]
    fn errors_sum_over_all_members() {
        let (entry, mid, exit) = (StageId::new(), StageId::new(), StageId::new());
        let mut v = FakeView::default();
        v.errors.insert(entry, 1);
        v.errors.insert(mid, 2);
        v.errors.insert(exit, 4);
        let red = CompositeRed::project(&boundary(entry, exit, vec![entry, mid, exit]), &v);
        assert_eq!(red.errors, 7);
    }

    #[test]
    fn boundary_latency_is_exit_minus_entry() {
        let (entry, exit) = (StageId::new(), StageId::new());
        let mut v = FakeView::default();
        v.latency.insert(entry, 30); // source -> entry
        v.latency.insert(exit, 80); // source -> exit
        let red = CompositeRed::project(&boundary(entry, exit, vec![entry, exit]), &v);
        assert_eq!(red.boundary_latency_ms, Some(50));
    }

    #[test]
    fn boundary_latency_is_none_until_both_boundaries_have_a_sample() {
        let (entry, exit) = (StageId::new(), StageId::new());
        let mut v = FakeView::default();
        v.latency.insert(exit, 80); // entry missing
        let red = CompositeRed::project(&boundary(entry, exit, vec![entry, exit]), &v);
        assert_eq!(red.boundary_latency_ms, None);
    }

    #[test]
    fn boundary_latency_saturates_when_exit_precedes_entry_sample() {
        // Skew (exit sample older than entry) must not underflow.
        let (entry, exit) = (StageId::new(), StageId::new());
        let mut v = FakeView::default();
        v.latency.insert(entry, 90);
        v.latency.insert(exit, 40);
        let red = CompositeRed::project(&boundary(entry, exit, vec![entry, exit]), &v);
        assert_eq!(red.boundary_latency_ms, Some(0));
    }
}
