use flowstate_rs::event_store::{EventStore, StageSemantics};
use flowstate_rs::step::ChainEvent;
use flowstate_rs::topology::StageId;
use serde_json::json;

#[tokio::test]
async fn test_event_store_basic_operations() {
    let store = EventStore::for_testing().await;
    
    // Create a writer
    let stage_id = StageId::next();
    let mut writer = store.create_writer(stage_id, StageSemantics::Stateless)
        .await
        .unwrap();
    
    // Append an event
    let event = ChainEvent::new("TestEvent", json!({
        "message": "Hello, EventStore!"
    }));
    
    let envelope = writer.append(event.clone(), None).await.unwrap();
    
    assert_eq!(envelope.writer_id.stage_id(), stage_id);
    assert!(envelope.event.event_type == "TestEvent");
    
    // Create a reader
    let reader = store.reader();
    
    // Read all events
    let events = reader.read_causal_order().await.unwrap();
    assert_eq!(events.len(), 1);
    assert!(events[0].event.event_type == "TestEvent");
}

#[tokio::test]
async fn test_causal_ordering() {
    let store = EventStore::for_testing().await;
    
    // Create two writers
    let stage1_id = StageId::next();
    let stage2_id = StageId::next();
    let mut writer1 = store.create_writer(stage1_id, StageSemantics::Stateless)
        .await
        .unwrap();
    let mut writer2 = store.create_writer(stage2_id, StageSemantics::Stateless)
        .await
        .unwrap();
    
    // Writer1 creates event A
    let event_a = ChainEvent::new("EventA", json!({"value": "A"}));
    let envelope_a = writer1.append(event_a, None).await.unwrap();
    
    // Writer2 creates event B that depends on A
    let event_b = ChainEvent::new("EventB", json!({"value": "B", "depends_on": "A"}));
    let envelope_b = writer2.append(event_b, Some(&envelope_a)).await.unwrap();
    
    // Check vector clock relationship
    assert!(envelope_a.vector_clock.happened_before(&envelope_b.vector_clock));
    assert!(!envelope_b.vector_clock.happened_before(&envelope_a.vector_clock));
    
    // Read events in causal order
    let reader = store.reader();
    let events = reader.read_causal_order().await.unwrap();
    
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].event.payload["value"], "A");
    assert_eq!(events[1].event.payload["value"], "B");
}

#[tokio::test]
async fn test_concurrent_events() {
    let store = EventStore::for_testing().await;
    
    // Create two writers
    let stage1_id = StageId::next();
    let stage2_id = StageId::next();
    let mut writer1 = store.create_writer(stage1_id, StageSemantics::Stateless)
        .await
        .unwrap();
    let mut writer2 = store.create_writer(stage2_id, StageSemantics::Stateless)
        .await
        .unwrap();
    
    // Both writers create independent events
    let event1 = ChainEvent::new("Event1", json!({"value": 1}));
    let envelope1 = writer1.append(event1, None).await.unwrap();
    
    let event2 = ChainEvent::new("Event2", json!({"value": 2}));
    let envelope2 = writer2.append(event2, None).await.unwrap();
    
    // Events should be concurrent
    assert!(envelope1.vector_clock.concurrent_with(&envelope2.vector_clock));
    assert!(envelope2.vector_clock.concurrent_with(&envelope1.vector_clock));
}

#[tokio::test]
async fn test_multi_worker_scaling() {
    let store = EventStore::for_testing().await;
    
    // Create initial worker
    let compute_stage_id = StageId::next();
    let writer1 = store.create_writer(compute_stage_id, StageSemantics::Stateless)
        .await
        .unwrap();
    assert_eq!(writer1.writer_id().stage_id(), compute_stage_id);
    assert_eq!(writer1.writer_id().worker_index(), None);
    
    // Scale up to add more workers
    let writer2 = store.scale_up_stage(compute_stage_id, StageSemantics::Stateless)
        .await
        .unwrap();
    assert_eq!(writer2.writer_id().stage_id(), compute_stage_id);
    assert_eq!(writer2.writer_id().worker_index(), Some(1));
    
    let writer3 = store.scale_up_stage(compute_stage_id, StageSemantics::Stateless)
        .await
        .unwrap();
    assert_eq!(writer3.writer_id().stage_id(), compute_stage_id);
    assert_eq!(writer3.writer_id().worker_index(), Some(2));
    
    // Verify worker count
    assert_eq!(store.stage_worker_count(compute_stage_id).await, 3);
    
    // Read events from specific stage (would merge all workers)
    let reader = store.reader();
    let stage_events = reader.read_stage_events(compute_stage_id).await.unwrap();
    assert_eq!(stage_events.len(), 0); // No events written yet
}