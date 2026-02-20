// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime_services::backpressure::{BackpressurePlan, BackpressureRegistry};
use obzenflow_runtime_services::id_conversions::StageIdExt;
use obzenflow_topology::TopologyBuilder;

fn is_quiescent(
    registry: &BackpressureRegistry,
    edges: &[(obzenflow_core::StageId, obzenflow_core::StageId)],
) -> bool {
    if edges.is_empty() {
        return false;
    }
    edges
        .iter()
        .all(|(upstream, downstream)| registry.edge_in_flight(*upstream, *downstream) == Some(0))
}

#[test]
fn quiescence_predicate_holds_when_any_edge_has_in_flight() {
    let mut builder = TopologyBuilder::new();
    let a_top = builder.add_stage(Some("a".to_string()));
    let b_top = builder.add_stage(Some("b".to_string())); // a -> b
    builder.add_edge(b_top, a_top); // b -> a (cycle)
    let topology = builder.build_unchecked().expect("topology");

    let a = obzenflow_core::StageId::from_topology_id(a_top);
    let b = obzenflow_core::StageId::from_topology_id(b_top);

    let plan = BackpressurePlan::disabled()
        .track_only_edge(a, b)
        .track_only_edge(b, a);
    let registry = BackpressureRegistry::new(&topology, &plan);

    let edges = vec![(a, b), (b, a)];

    assert!(is_quiescent(&registry, &edges), "initially quiescent");

    // Simulate in-flight work on b -> a.
    let b_writer = registry.writer(b);
    b_writer.reserve(1).expect("reserve").commit(1);
    assert!(
        !is_quiescent(&registry, &edges),
        "in-flight should hold release"
    );

    // Quiescence resumes only after the downstream acks consumption.
    registry.reader(b, a).ack_consumed(1);
    assert!(is_quiescent(&registry, &edges), "acked should release");
}

#[test]
fn quiescence_predicate_counts_reserved_as_in_flight() {
    let mut builder = TopologyBuilder::new();
    let a_top = builder.add_stage(Some("a".to_string()));
    let b_top = builder.add_stage(Some("b".to_string())); // a -> b
    let topology = builder.build_unchecked().expect("topology");

    let a = obzenflow_core::StageId::from_topology_id(a_top);
    let b = obzenflow_core::StageId::from_topology_id(b_top);

    let plan = BackpressurePlan::disabled().track_only_edge(a, b);
    let registry = BackpressureRegistry::new(&topology, &plan);

    let edges = vec![(a, b)];
    assert!(is_quiescent(&registry, &edges));

    let a_writer = registry.writer(a);
    let reservation = a_writer.reserve(1).expect("reserve");
    assert!(
        !is_quiescent(&registry, &edges),
        "reserved work should prevent quiescence"
    );

    drop(reservation);
    assert!(
        is_quiescent(&registry, &edges),
        "dropping reservation should restore quiescence"
    );
}

#[test]
fn quiescence_predicate_fails_closed_on_untracked_edges() {
    let mut builder = TopologyBuilder::new();
    let a_top = builder.add_stage(Some("a".to_string()));
    let b_top = builder.add_stage(Some("b".to_string())); // a -> b
    let topology = builder.build_unchecked().expect("topology");

    let a = obzenflow_core::StageId::from_topology_id(a_top);
    let b = obzenflow_core::StageId::from_topology_id(b_top);

    let registry = BackpressureRegistry::new(&topology, &BackpressurePlan::disabled());
    assert!(
        !is_quiescent(&registry, &[(a, b)]),
        "untracked edges must prevent release"
    );
}
