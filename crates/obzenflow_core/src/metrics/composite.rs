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

use crate::event::system_event::{ContractName, ContractResultStatusLabel};
use crate::id::{CompositeId, StageId};
use crate::metrics::snapshots::{ContractMetricsSnapshot, ContractViolationCauseLabel};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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

/// Which boundary an external edge crosses, relative to the composite.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryDirection {
    /// An external producer into the composite's entry-port member.
    Inbound,
    /// The composite's exit-port member out to an external consumer.
    Outbound,
}

impl BoundaryDirection {
    pub const fn as_str(self) -> &'static str {
        match self {
            BoundaryDirection::Inbound => "inbound",
            BoundaryDirection::Outbound => "outbound",
        }
    }
}

/// One composite boundary edge's contract facts, re-keyed from the real member
/// edge to the composite identity (FLOWIP-128a B5). A pure relabel of existing
/// contract facts (a view), never a new fact: an inbound edge's real
/// `external -> entry_member` becomes `(peer = external, Inbound)`, an outbound
/// edge's `exit_member -> external` becomes `(peer = external, Outbound)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompositeContract {
    pub composite: CompositeId,
    /// The external peer stage on the far side of the boundary edge.
    pub peer: StageId,
    pub direction: BoundaryDirection,
    /// Per-contract result counts on this boundary edge.
    pub results: Vec<(ContractName, ContractResultStatusLabel, u64)>,
    /// Per-contract violation counts on this boundary edge.
    pub violations: Vec<(ContractName, ContractViolationCauseLabel, u64)>,
    /// Latest reader sequence at the boundary edge (max over its contracts).
    pub reader_seq: Option<u64>,
    /// Latest advertised writer sequence at the boundary edge.
    pub advertised_writer_seq: Option<u64>,
}

impl CompositeContract {
    fn empty(composite: CompositeId, peer: StageId, direction: BoundaryDirection) -> Self {
        CompositeContract {
            composite,
            peer,
            direction,
            results: Vec::new(),
            violations: Vec::new(),
            reader_seq: None,
            advertised_writer_seq: None,
        }
    }

    /// Project a composite's boundary contract views from the contract-metrics
    /// snapshot. Each entry whose edge touches a boundary member is re-keyed to
    /// the composite boundary; internal and unrelated edges are skipped. Returns
    /// one entry per `(peer, direction)` boundary edge, deterministically
    /// ordered. Single entry and exit today; a multi-member boundary
    /// (FLOWIP-128c) yields several entries per peer, which this `Vec` shape
    /// already accommodates.
    pub fn project(boundary: &CompositeBoundary, contracts: &ContractMetricsSnapshot) -> Vec<Self> {
        let classify = |upstream: StageId, downstream: StageId| {
            if downstream == boundary.entry {
                Some((upstream, BoundaryDirection::Inbound))
            } else if upstream == boundary.exit {
                Some((downstream, BoundaryDirection::Outbound))
            } else {
                None
            }
        };

        let mut acc: HashMap<(StageId, BoundaryDirection), CompositeContract> = HashMap::new();
        let composite_id = &boundary.composite_id;

        for (key, count) in &contracts.results_total {
            if let Some((peer, direction)) = classify(key.edge.upstream, key.edge.downstream) {
                acc.entry((peer, direction))
                    .or_insert_with(|| {
                        CompositeContract::empty(composite_id.clone(), peer, direction)
                    })
                    .results
                    .push((key.edge.contract.clone(), key.status, *count));
            }
        }
        for (key, count) in &contracts.violations_total {
            if let Some((peer, direction)) = classify(key.edge.upstream, key.edge.downstream) {
                acc.entry((peer, direction))
                    .or_insert_with(|| {
                        CompositeContract::empty(composite_id.clone(), peer, direction)
                    })
                    .violations
                    .push((key.edge.contract.clone(), key.cause.clone(), *count));
            }
        }
        for (key, seq) in &contracts.reader_seq {
            if let Some((peer, direction)) = classify(key.upstream, key.downstream) {
                let entry = acc.entry((peer, direction)).or_insert_with(|| {
                    CompositeContract::empty(composite_id.clone(), peer, direction)
                });
                entry.reader_seq = Some(entry.reader_seq.map_or(*seq, |cur| cur.max(*seq)));
            }
        }
        for (key, seq) in &contracts.advertised_writer_seq {
            if let Some((peer, direction)) = classify(key.upstream, key.downstream) {
                let entry = acc.entry((peer, direction)).or_insert_with(|| {
                    CompositeContract::empty(composite_id.clone(), peer, direction)
                });
                entry.advertised_writer_seq = Some(
                    entry
                        .advertised_writer_seq
                        .map_or(*seq, |cur| cur.max(*seq)),
                );
            }
        }

        let mut out: Vec<CompositeContract> = acc.into_values().collect();
        for entry in &mut out {
            entry
                .results
                .sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
            entry
                .violations
                .sort_by(|a, b| (a.0.as_str(), a.1.as_str()).cmp(&(b.0.as_str(), b.1.as_str())));
        }
        out.sort_by(|a, b| {
            (a.direction.as_str(), a.peer.to_string())
                .cmp(&(b.direction.as_str(), b.peer.to_string()))
        });
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::snapshots::{
        ContractMetricEdgeKey, ContractMetricResultKey, ContractMetricViolationKey,
    };
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

    fn edge_key(upstream: StageId, downstream: StageId, contract: &str) -> ContractMetricEdgeKey {
        ContractMetricEdgeKey {
            upstream,
            downstream,
            contract: ContractName::new(contract),
            selected_event_type: None,
            feed_role: None,
        }
    }

    #[test]
    fn inbound_edge_rekeys_external_peer_to_the_composite() {
        let (external, entry, exit) = (StageId::new(), StageId::new(), StageId::new());
        let mut snap = ContractMetricsSnapshot::default();
        snap.results_total.insert(
            ContractMetricResultKey {
                edge: edge_key(external, entry, "TransportContract"),
                status: ContractResultStatusLabel::Passed,
            },
            20,
        );
        let out = CompositeContract::project(&boundary(entry, exit, vec![entry, exit]), &snap);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].peer, external);
        assert_eq!(out[0].direction, BoundaryDirection::Inbound);
        assert_eq!(
            out[0].results,
            vec![(
                ContractName::new("TransportContract"),
                ContractResultStatusLabel::Passed,
                20
            )]
        );
    }

    #[test]
    fn outbound_edge_rekeys_and_carries_seq() {
        let (entry, exit, external) = (StageId::new(), StageId::new(), StageId::new());
        let mut snap = ContractMetricsSnapshot::default();
        snap.results_total.insert(
            ContractMetricResultKey {
                edge: edge_key(exit, external, "SinkContract"),
                status: ContractResultStatusLabel::Passed,
            },
            5,
        );
        snap.reader_seq
            .insert(edge_key(exit, external, "SinkContract"), 55);
        snap.advertised_writer_seq
            .insert(edge_key(exit, external, "SinkContract"), 55);
        let out = CompositeContract::project(&boundary(entry, exit, vec![entry, exit]), &snap);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].direction, BoundaryDirection::Outbound);
        assert_eq!(out[0].peer, external);
        assert_eq!(out[0].reader_seq, Some(55));
        assert_eq!(out[0].advertised_writer_seq, Some(55));
    }

    #[test]
    fn internal_edges_between_members_are_skipped() {
        let (entry, mid, exit) = (StageId::new(), StageId::new(), StageId::new());
        let mut snap = ContractMetricsSnapshot::default();
        snap.results_total.insert(
            ContractMetricResultKey {
                edge: edge_key(entry, mid, "Internal"),
                status: ContractResultStatusLabel::Passed,
            },
            9,
        );
        snap.results_total.insert(
            ContractMetricResultKey {
                edge: edge_key(mid, exit, "Internal"),
                status: ContractResultStatusLabel::Passed,
            },
            9,
        );
        let out = CompositeContract::project(&boundary(entry, exit, vec![entry, mid, exit]), &snap);
        assert!(out.is_empty());
    }

    #[test]
    fn violation_cause_survives_the_projection() {
        let (external, entry, exit) = (StageId::new(), StageId::new(), StageId::new());
        let mut snap = ContractMetricsSnapshot::default();
        snap.violations_total.insert(
            ContractMetricViolationKey {
                edge: edge_key(external, entry, "TransportContract"),
                cause: ContractViolationCauseLabel::new("seq_divergence"),
            },
            2,
        );
        let out = CompositeContract::project(&boundary(entry, exit, vec![entry, exit]), &snap);
        assert_eq!(out.len(), 1);
        assert_eq!(
            out[0].violations,
            vec![(
                ContractName::new("TransportContract"),
                ContractViolationCauseLabel::new("seq_divergence"),
                2
            )]
        );
    }
}
