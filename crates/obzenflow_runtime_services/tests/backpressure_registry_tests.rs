// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime_services::backpressure::{BackpressurePlan, BackpressureRegistry};
use obzenflow_runtime_services::id_conversions::StageIdExt;
use obzenflow_topology::TopologyBuilder;
use std::num::NonZeroU64;

#[test]
fn backpressure_blocks_after_window_until_downstream_acks() {
    let mut builder = TopologyBuilder::new();
    let s_top = builder.add_stage(Some("s".to_string()));
    let d_top = builder.add_stage(Some("d".to_string()));
    let topology = builder.build_unchecked().expect("topology");

    let s = obzenflow_core::StageId::from_topology_id(s_top);
    let d = obzenflow_core::StageId::from_topology_id(d_top);

    let plan =
        BackpressurePlan::disabled().with_stage_window(s, NonZeroU64::new(2).expect("window"));
    let registry = BackpressureRegistry::new(&topology, &plan);

    let writer = registry.writer(s);
    let reader = registry.reader(s, d);

    assert!(writer.is_enabled());
    assert!(reader.is_enabled());
    assert_eq!(writer.min_downstream_credit(), 2);

    writer.reserve(1).expect("first reserve").commit(1);
    assert_eq!(writer.min_downstream_credit(), 1);

    writer.reserve(1).expect("second reserve").commit(1);
    assert_eq!(writer.min_downstream_credit(), 0);

    assert!(writer.reserve(1).is_none(), "should block at window");

    reader.ack_consumed(1);
    assert_eq!(writer.min_downstream_credit(), 1);

    writer
        .reserve(1)
        .expect("third reserve after ack")
        .commit(1);
    assert_eq!(writer.min_downstream_credit(), 0);
}

#[test]
fn backpressure_fan_out_is_barrier_slowest_wins() {
    let mut builder = TopologyBuilder::new();
    let s_top = builder.add_stage(Some("s".to_string()));
    let a_top = builder.add_stage(Some("a".to_string())); // s -> a
    builder.reset_current();
    builder.set_current(s_top);
    let b_top = builder.add_stage(Some("b".to_string())); // s -> b
    let topology = builder.build_unchecked().expect("topology");

    let s = obzenflow_core::StageId::from_topology_id(s_top);
    let a = obzenflow_core::StageId::from_topology_id(a_top);
    let b = obzenflow_core::StageId::from_topology_id(b_top);

    let plan =
        BackpressurePlan::disabled().with_stage_window(s, NonZeroU64::new(2).expect("window"));
    let registry = BackpressureRegistry::new(&topology, &plan);

    let writer = registry.writer(s);
    let reader_a = registry.reader(s, a);
    let reader_b = registry.reader(s, b);

    assert!(writer.is_enabled());
    assert!(reader_a.is_enabled());
    assert!(reader_b.is_enabled());
    assert_eq!(writer.min_downstream_credit(), 2);

    writer.reserve(1).expect("w1").commit(1);
    writer.reserve(1).expect("w2").commit(1);
    assert_eq!(writer.min_downstream_credit(), 0);
    assert!(writer.reserve(1).is_none(), "blocked at window=2");

    // A is fast: acking A alone must NOT unblock the writer (barrier semantics).
    reader_a.ack_consumed(2);
    assert_eq!(writer.min_downstream_credit(), 0);
    assert!(writer.reserve(1).is_none(), "still blocked by slowest edge");

    // Once B advances, the writer can proceed.
    reader_b.ack_consumed(1);
    assert_eq!(writer.min_downstream_credit(), 1);
    writer.reserve(1).expect("w3").commit(1);
    assert_eq!(writer.min_downstream_credit(), 0);
}
