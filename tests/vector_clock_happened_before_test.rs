//! Comprehensive happened-before relationship tests for vector clocks
//! 
//! This test suite validates the fundamental property of vector clocks:
//! the ability to determine causal relationships between events.
//! 
//! For events A and B:
//! - A → B (A happened-before B): All of A's clock components ≤ B's, with at least one <
//! - A || B (A concurrent with B): Neither A → B nor B → A
//! - A = B (identical): All clock components are equal

use flowstate_rs::event_store::{EventStore, EventStoreConfig, WriterId, VectorClock, StageSemantics};
use flowstate_rs::chain_event::ChainEvent;
use flowstate_rs::topology::StageId;

/// Test helper to create a deterministic event
fn create_test_event(event_type: &str, id: u32) -> ChainEvent {
    ChainEvent::new(event_type, serde_json::json!({
        "id": id,
        "test": true
    }))
}

/// Verify that a happened-before b
fn assert_happened_before(a: &VectorClock, b: &VectorClock, msg: &str) {
    assert!(
        a.happened_before(b), 
        "{}: Expected {:?} → {:?} (happened-before), but it didn't", 
        msg, a, b
    );
    // Verify the inverse is false
    assert!(
        !b.happened_before(a),
        "{}: Expected NOT {:?} → {:?}, but it did",
        msg, b, a
    );
}

/// Verify that a and b are concurrent
fn assert_concurrent(a: &VectorClock, b: &VectorClock, msg: &str) {
    assert!(
        !a.happened_before(b) && !b.happened_before(a),
        "{}: Expected {:?} || {:?} (concurrent), but they weren't",
        msg, a, b
    );
}

#[tokio::test]
async fn test_single_writer_monotonic_vector_clocks() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::new(EventStoreConfig {
        path: temp_dir.path().to_path_buf(),
        max_segment_size: 10 * 1024 * 1024,
    }).await.unwrap();
    
    let mut writer = store.create_writer(StageId::from_u32(0), StageSemantics::Stateless).await.unwrap();
    
    let mut clocks = Vec::new();
    
    // Create a chain of events
    for i in 0..10 {
        let event = create_test_event("TestEvent", i);
        let envelope = writer.append(event, None).await.unwrap();
        clocks.push(envelope.vector_clock.clone());
    }
    
    // Verify strict monotonic ordering
    for i in 0..clocks.len() - 1 {
        assert_happened_before(
            &clocks[i], 
            &clocks[i + 1],
            &format!("Event {} → Event {}", i, i + 1)
        );
        
        // Verify transitivity: if i → i+1 and i+1 → i+2, then i → i+2
        if i < clocks.len() - 2 {
            assert_happened_before(
                &clocks[i],
                &clocks[i + 2],
                &format!("Transitivity: Event {} → Event {}", i, i + 2)
            );
        }
    }
    
    // Verify no event happens-before itself (irreflexivity)
    for (i, clock) in clocks.iter().enumerate() {
        assert!(
            !clock.happened_before(clock),
            "Event {} should not happen-before itself",
            i
        );
    }
}

#[tokio::test]
async fn test_causal_chains_with_parent_references() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::new(EventStoreConfig {
        path: temp_dir.path().to_path_buf(),
        max_segment_size: 10 * 1024 * 1024,
    }).await.unwrap();
    
    let mut writer = store.create_writer(StageId::from_u32(0), StageSemantics::Stateless).await.unwrap();
    
    // Create a root event (no parent)
    let root_event = create_test_event("Root", 0);
    let root_envelope = writer.append(root_event, None).await.unwrap();
    
    // Create two branches from the root
    let branch1_event = create_test_event("Branch1", 1);
    let branch1_envelope = writer.append(branch1_event, Some(&root_envelope)).await.unwrap();
    
    let branch2_event = create_test_event("Branch2", 2);
    let branch2_envelope = writer.append(branch2_event, Some(&root_envelope)).await.unwrap();
    
    // Verify root happened-before both branches
    assert_happened_before(
        &root_envelope.vector_clock,
        &branch1_envelope.vector_clock,
        "Root → Branch1"
    );
    assert_happened_before(
        &root_envelope.vector_clock,
        &branch2_envelope.vector_clock,
        "Root → Branch2"
    );
    
    // Verify branches are NOT concurrent (same writer)
    // With a single writer, branch2 should happen after branch1
    assert_happened_before(
        &branch1_envelope.vector_clock,
        &branch2_envelope.vector_clock,
        "Branch1 → Branch2 (same writer)"
    );
}

#[tokio::test]
async fn test_multiple_writers_concurrent_events() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::new(EventStoreConfig {
        path: temp_dir.path().to_path_buf(),
        max_segment_size: 10 * 1024 * 1024,
    }).await.unwrap();
    
    // Create three independent writers
    let mut writer_a = store.create_writer(StageId::from_u32(0), StageSemantics::Stateless).await.unwrap();
    let mut writer_b = store.create_writer(StageId::from_u32(1), StageSemantics::Stateless).await.unwrap();
    let mut writer_c = store.create_writer(StageId::from_u32(2), StageSemantics::Stateless).await.unwrap();
    
    // Each writer creates events independently
    let event_a1 = writer_a.append(create_test_event("A1", 1), None).await.unwrap();
    let event_b1 = writer_b.append(create_test_event("B1", 1), None).await.unwrap();
    let event_c1 = writer_c.append(create_test_event("C1", 1), None).await.unwrap();
    
    // Verify all three are concurrent (no causal relationship)
    assert_concurrent(
        &event_a1.vector_clock,
        &event_b1.vector_clock,
        "A1 || B1"
    );
    assert_concurrent(
        &event_b1.vector_clock,
        &event_c1.vector_clock,
        "B1 || C1"
    );
    assert_concurrent(
        &event_a1.vector_clock,
        &event_c1.vector_clock,
        "A1 || C1"
    );
    
    // Writer B processes A's event (creates causal dependency)
    let event_b2 = writer_b.append(
        create_test_event("B2", 2),
        Some(&event_a1)
    ).await.unwrap();
    
    // Verify causality
    assert_happened_before(
        &event_a1.vector_clock,
        &event_b2.vector_clock,
        "A1 → B2"
    );
    assert_happened_before(
        &event_b1.vector_clock,
        &event_b2.vector_clock,
        "B1 → B2"
    );
    
    // C1 is still concurrent with B2 (no causal path)
    assert_concurrent(
        &event_c1.vector_clock,
        &event_b2.vector_clock,
        "C1 || B2"
    );
}

#[tokio::test]
async fn test_complex_causal_graph() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::new(EventStoreConfig {
        path: temp_dir.path().to_path_buf(),
        max_segment_size: 10 * 1024 * 1024,
    }).await.unwrap();
    
    // Create a pipeline: Source → Filter → Transform → Sink
    let mut source = store.create_writer(StageId::from_u32(0), StageSemantics::Stateless).await.unwrap();
    let mut filter = store.create_writer(StageId::from_u32(1), StageSemantics::Stateless).await.unwrap();
    let mut transform = store.create_writer(StageId::from_u32(2), StageSemantics::Stateless).await.unwrap();
    let mut sink = store.create_writer(StageId::from_u32(3), StageSemantics::Stateless).await.unwrap();
    
    // Source emits events
    let s1 = source.append(create_test_event("Data", 1), None).await.unwrap();
    let s2 = source.append(create_test_event("Data", 2), None).await.unwrap();
    
    // Filter processes first event
    let f1 = filter.append(create_test_event("Filtered", 1), Some(&s1)).await.unwrap();
    
    // Transform processes filtered event
    let t1 = transform.append(create_test_event("Transformed", 1), Some(&f1)).await.unwrap();
    
    // Sink receives transformed event
    let k1 = sink.append(create_test_event("Stored", 1), Some(&t1)).await.unwrap();
    
    // Verify the complete causal chain
    assert_happened_before(&s1.vector_clock, &f1.vector_clock, "Source → Filter");
    assert_happened_before(&f1.vector_clock, &t1.vector_clock, "Filter → Transform");
    assert_happened_before(&t1.vector_clock, &k1.vector_clock, "Transform → Sink");
    
    // Verify transitivity
    assert_happened_before(&s1.vector_clock, &k1.vector_clock, "Source → Sink (transitive)");
    
    // S2 is concurrent with the processing chain (not yet processed)
    assert_concurrent(&s2.vector_clock, &f1.vector_clock, "S2 || F1");
    assert_concurrent(&s2.vector_clock, &t1.vector_clock, "S2 || T1");
    assert_concurrent(&s2.vector_clock, &k1.vector_clock, "S2 || K1");
}

#[tokio::test]
async fn test_vector_clock_merge_scenarios() {
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::new(EventStoreConfig {
        path: temp_dir.path().to_path_buf(),
        max_segment_size: 10 * 1024 * 1024,
    }).await.unwrap();
    
    let mut writer_a = store.create_writer(StageId::from_u32(0), StageSemantics::Stateless).await.unwrap();
    let mut writer_b = store.create_writer(StageId::from_u32(1), StageSemantics::Stateless).await.unwrap();
    
    // Create divergent histories
    let a1 = writer_a.append(create_test_event("A1", 1), None).await.unwrap();
    let a2 = writer_a.append(create_test_event("A2", 2), None).await.unwrap();
    let a3 = writer_a.append(create_test_event("A3", 3), None).await.unwrap();
    
    let b1 = writer_b.append(create_test_event("B1", 1), None).await.unwrap();
    let b2 = writer_b.append(create_test_event("B2", 2), None).await.unwrap();
    
    // B sees A's history at A2
    let b3 = writer_b.append(create_test_event("B3", 3), Some(&a2)).await.unwrap();
    
    // Verify relationships
    assert_happened_before(&a1.vector_clock, &b3.vector_clock, "A1 → B3");
    assert_happened_before(&a2.vector_clock, &b3.vector_clock, "A2 → B3");
    assert_happened_before(&b1.vector_clock, &b3.vector_clock, "B1 → B3");
    assert_happened_before(&b2.vector_clock, &b3.vector_clock, "B2 → B3");
    
    // A3 and B3 are concurrent (A3 happened after A2 but independently of B3)
    assert_concurrent(&a3.vector_clock, &b3.vector_clock, "A3 || B3");
}

#[tokio::test]
async fn test_vector_clock_properties() {
    // Test the fundamental mathematical properties of vector clocks
    
    // Property 1: Irreflexivity - no event happens before itself
    let mut clock = VectorClock::new();
    let writer_id = WriterId::new(StageId::from_u32(0));
    clock.tick(&writer_id);
    assert!(!clock.happened_before(&clock), "Irreflexivity violated");
    
    // Property 2: Antisymmetry - if A → B, then NOT B → A
    let mut clock_a = VectorClock::new();
    clock_a.tick(&writer_id);
    
    let mut clock_b = clock_a.clone();
    clock_b.tick(&writer_id);
    
    assert!(clock_a.happened_before(&clock_b), "A should happen-before B");
    assert!(!clock_b.happened_before(&clock_a), "B should NOT happen-before A");
    
    // Property 3: Transitivity - if A → B and B → C, then A → C
    let mut clock_c = clock_b.clone();
    clock_c.tick(&writer_id);
    
    assert!(clock_a.happened_before(&clock_b), "A → B");
    assert!(clock_b.happened_before(&clock_c), "B → C");
    assert!(clock_a.happened_before(&clock_c), "A → C (transitivity)");
    
    // Property 4: Concurrent events - neither happens before the other
    let writer_id_2 = WriterId::new(StageId::from_u32(1));
    let mut clock_d = VectorClock::new();
    clock_d.tick(&writer_id_2);
    
    assert!(!clock_a.happened_before(&clock_d), "A should not happen-before D");
    assert!(!clock_d.happened_before(&clock_a), "D should not happen-before A");
}

#[tokio::test]
async fn test_happened_before_with_multiple_parents() {
    // Future test for merge operations (when supported)
    // This documents the expected behavior for events with multiple parents
    
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::new(EventStoreConfig {
        path: temp_dir.path().to_path_buf(),
        max_segment_size: 10 * 1024 * 1024,
    }).await.unwrap();
    
    let mut writer_a = store.create_writer(StageId::from_u32(0), StageSemantics::Stateless).await.unwrap();
    let mut writer_b = store.create_writer(StageId::from_u32(1), StageSemantics::Stateless).await.unwrap();
    let mut writer_merge = store.create_writer(StageId::from_u32(2), StageSemantics::Stateless).await.unwrap();
    
    // Create two independent branches
    let a1 = writer_a.append(create_test_event("A1", 1), None).await.unwrap();
    let b1 = writer_b.append(create_test_event("B1", 1), None).await.unwrap();
    
    // Currently we can only have one parent, but document future behavior
    // When we support multiple parents, a merge event would take the max of all parent clocks
    let merge1 = writer_merge.append(
        create_test_event("Merge", 1),
        Some(&a1) // In future: vec![&a1, &b1]
    ).await.unwrap();
    
    // For now, verify current single-parent behavior
    assert_happened_before(&a1.vector_clock, &merge1.vector_clock, "A1 → Merge");
    // B1 remains concurrent since it's not in the parent chain
    assert_concurrent(&b1.vector_clock, &merge1.vector_clock, "B1 || Merge (no parent link)");
}

/// Test helper to visualize vector clock relationships
fn print_clock_comparison(label: &str, clock: &VectorClock, other_clocks: &[(&str, &VectorClock)]) {
    println!("\n{} vector clock: {:?}", label, clock);
    for (other_label, other_clock) in other_clocks {
        if clock.happened_before(other_clock) {
            println!("  {} → {} (happened-before)", label, other_label);
        } else if other_clock.happened_before(clock) {
            println!("  {} → {} (happened-after)", other_label, label);
        } else {
            println!("  {} || {} (concurrent)", label, other_label);
        }
    }
}

#[tokio::test]
async fn test_vector_clock_visualization() {
    // This test helps visualize vector clock relationships
    // Useful for debugging and understanding complex scenarios
    
    let temp_dir = tempfile::tempdir().unwrap();
    let store = EventStore::new(EventStoreConfig {
        path: temp_dir.path().to_path_buf(),
        max_segment_size: 10 * 1024 * 1024,
    }).await.unwrap();
    
    let mut w1 = store.create_writer(StageId::from_u32(0), StageSemantics::Stateless).await.unwrap();
    let mut w2 = store.create_writer(StageId::from_u32(1), StageSemantics::Stateless).await.unwrap();
    let mut w3 = store.create_writer(StageId::from_u32(2), StageSemantics::Stateless).await.unwrap();
    
    // Create a complex interaction pattern
    let e1 = w1.append(create_test_event("E1", 1), None).await.unwrap();
    let e2 = w2.append(create_test_event("E2", 2), None).await.unwrap();
    let e3 = w1.append(create_test_event("E3", 3), Some(&e2)).await.unwrap();
    let e4 = w3.append(create_test_event("E4", 4), Some(&e1)).await.unwrap();
    let e5 = w2.append(create_test_event("E5", 5), Some(&e3)).await.unwrap();
    
    // Print relationships for debugging
    if std::env::var("FLOWSTATE_DEBUG").is_ok() {
        let clocks = vec![
            ("E1", &e1.vector_clock),
            ("E2", &e2.vector_clock),
            ("E3", &e3.vector_clock),
            ("E4", &e4.vector_clock),
            ("E5", &e5.vector_clock),
        ];
        
        for (label, clock) in &clocks {
            print_clock_comparison(label, clock, &clocks);
        }
    }
    
    // Verify the expected relationships
    assert_happened_before(&e1.vector_clock, &e3.vector_clock, "E1 → E3");
    assert_happened_before(&e2.vector_clock, &e3.vector_clock, "E2 → E3");
    assert_happened_before(&e3.vector_clock, &e5.vector_clock, "E3 → E5");
    assert_happened_before(&e1.vector_clock, &e4.vector_clock, "E1 → E4");
    assert_concurrent(&e2.vector_clock, &e4.vector_clock, "E2 || E4");
}