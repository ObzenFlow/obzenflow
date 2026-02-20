// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::{event::chain_event::ChainEventFactory, CycleDepth, SccId, StageId, WriterId};
use serde_json::json;
use ulid::Ulid;

fn test_scc_id(n: u128) -> SccId {
    SccId::from_ulid(Ulid::from(n))
}

#[test]
fn derived_event_propagates_cycle_state() {
    let writer_id = WriterId::from(StageId::new());
    let mut parent = ChainEventFactory::data_event(writer_id, "test.parent", json!({"x": 1}));
    parent.cycle_depth = Some(CycleDepth::new(7));
    parent.cycle_scc_id = Some(test_scc_id(42));

    let child =
        ChainEventFactory::derived_data_event(writer_id, &parent, "test.child", json!({"x": 2}));

    assert_eq!(child.cycle_depth, Some(CycleDepth::new(7)));
    assert_eq!(child.cycle_scc_id, Some(test_scc_id(42)));
}

#[test]
fn with_correlation_from_propagates_cycle_state() {
    let writer_id = WriterId::from(StageId::new());
    let mut parent = ChainEventFactory::data_event(writer_id, "test.parent", json!({"x": 1}));
    parent.cycle_depth = Some(CycleDepth::new(3));
    parent.cycle_scc_id = Some(test_scc_id(9));

    let child = ChainEventFactory::data_event(writer_id, "test.child", json!({"x": 2}))
        .with_correlation_from(&parent);

    assert_eq!(child.cycle_depth, Some(CycleDepth::new(3)));
    assert_eq!(child.cycle_scc_id, Some(test_scc_id(9)));
}
