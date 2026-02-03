// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::{event::chain_event::ChainEventFactory, StageId, WriterId};
use serde_json::json;

#[test]
fn test_full_lineage_propagation() {
    let writer_id = WriterId::from(StageId::new());

    // Create chain: A -> B -> C -> D
    let event_a = ChainEventFactory::data_event(writer_id, "test", json!({"level": "A"}));

    let event_b =
        ChainEventFactory::derived_data_event(writer_id, &event_a, "test", json!({"level": "B"}));

    let event_c =
        ChainEventFactory::derived_data_event(writer_id, &event_b, "test", json!({"level": "C"}));

    let event_d =
        ChainEventFactory::derived_data_event(writer_id, &event_c, "test", json!({"level": "D"}));

    // Event A should have no parents (root)
    assert_eq!(event_a.causality.parent_ids.len(), 0);
    assert!(event_a.causality.is_root());

    // Event B should only know about A
    assert_eq!(event_b.causality.parent_ids.len(), 1);
    assert!(event_b.causality.parent_ids.contains(&event_a.id));

    // Event C should know about B and A
    assert_eq!(event_c.causality.parent_ids.len(), 2);
    assert!(event_c.causality.parent_ids.contains(&event_b.id));
    assert!(event_c.causality.parent_ids.contains(&event_a.id));

    // Event D should know about all ancestors: C, B, and A
    assert_eq!(event_d.causality.parent_ids.len(), 3);
    assert!(event_d.causality.parent_ids.contains(&event_c.id));
    assert!(event_d.causality.parent_ids.contains(&event_b.id));
    assert!(event_d.causality.parent_ids.contains(&event_a.id));

    // Verify depth
    assert_eq!(event_a.causality.depth(), 0);
    assert_eq!(event_b.causality.depth(), 1);
    assert_eq!(event_c.causality.depth(), 2);
    assert_eq!(event_d.causality.depth(), 3);
}

#[test]
fn test_lineage_depth_limit() {
    let writer_id = WriterId::from(StageId::new());

    // Set a small limit for testing
    std::env::set_var("OBZENFLOW_MAX_LINEAGE_DEPTH", "5");

    let mut events = vec![];

    // Create first event
    events.push(ChainEventFactory::data_event(
        writer_id,
        "test",
        json!({"index": 0}),
    ));

    // Create 10 derived events (exceeds limit of 5)
    for i in 1..=10 {
        let parent = &events[i - 1].clone();
        events.push(ChainEventFactory::derived_data_event(
            writer_id,
            parent,
            "test",
            json!({"index": i}),
        ));
    }

    // Last event should have at most 5 parents (due to limit)
    let last_event = &events[10];
    assert!(last_event.causality.parent_ids.len() <= 5);

    // It should have the most recent 5 ancestors
    assert!(last_event.causality.parent_ids.contains(&events[9].id)); // immediate parent
    assert!(last_event.causality.parent_ids.contains(&events[8].id));
    assert!(last_event.causality.parent_ids.contains(&events[7].id));
    assert!(last_event.causality.parent_ids.contains(&events[6].id));
    assert!(last_event.causality.parent_ids.contains(&events[5].id));

    // But not the older ones
    assert!(!last_event.causality.parent_ids.contains(&events[4].id));
    assert!(!last_event.causality.parent_ids.contains(&events[0].id));

    // Clean up
    std::env::remove_var("OBZENFLOW_MAX_LINEAGE_DEPTH");
}

#[test]
fn test_cycle_detection() {
    let writer_id = WriterId::from(StageId::new());

    // Create a simple chain
    let event_a = ChainEventFactory::data_event(writer_id, "test", json!({"name": "A"}));

    let event_b =
        ChainEventFactory::derived_data_event(writer_id, &event_a, "test", json!({"name": "B"}));

    // B should not contain a cycle with itself
    assert!(!event_b.causality.contains_cycle(&event_b.id));

    // Manually create a cycle for testing
    let mut cyclic_event = event_b.clone();
    cyclic_event.causality.parent_ids.push(event_b.id);

    // Now it should detect the cycle
    assert!(cyclic_event.causality.contains_cycle(&event_b.id));

    // And find_cycle should return the cycle
    let cycle = cyclic_event.causality.find_cycle(&event_b.id);
    assert!(cycle.is_some());
    let cycle = cycle.unwrap();
    assert_eq!(cycle.len(), 2); // B -> B
    assert_eq!(cycle[0], event_b.id);
    assert_eq!(cycle[1], event_b.id);
}

#[test]
fn test_full_lineage_helper() {
    let writer_id = WriterId::from(StageId::new());

    let event_a = ChainEventFactory::data_event(writer_id, "test", json!({"name": "A"}));

    let event_b =
        ChainEventFactory::derived_data_event(writer_id, &event_a, "test", json!({"name": "B"}));

    let event_c =
        ChainEventFactory::derived_data_event(writer_id, &event_b, "test", json!({"name": "C"}));

    // Get full lineage of C
    let lineage = event_c.causality.full_lineage(event_c.id);

    // Should be [C, B, A]
    assert_eq!(lineage.len(), 3);
    assert_eq!(lineage[0], event_c.id);
    assert_eq!(lineage[1], event_b.id);
    assert_eq!(lineage[2], event_a.id);
}
