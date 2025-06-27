//! Integration tests for journal implementations
//!
//! These tests demonstrate how the journal trait works with
//! proper vector clocks and causal ordering.

use obzenflow_core::Journal;
use obzenflow_core::event::{
    chain_event::ChainEvent,
    event_id::EventId,
};
use obzenflow_core::WriterId;
use obzenflow_infra::journal::MemoryJournal;

#[tokio::test]
async fn test_journal_causal_ordering() {
    let journal = MemoryJournal::new();
    
    // Simulate a simple pipeline: source -> transform -> sink
    let source_writer = WriterId::new();
    let transform_writer = WriterId::new();
    let sink_writer = WriterId::new();
    
    // Source emits first event
    let source_event = ChainEvent::new(
        EventId::new(),
        source_writer.clone(),
        "data.received",
        serde_json::json!({
            "sensor": "temperature",
            "value": 23.5
        })
    );
    
    let source_envelope = journal.append(&source_writer, source_event, None)
        .await
        .expect("Failed to append source event");
    
    // Transform processes the source event
    let transform_event = ChainEvent::new(
        EventId::new(),
        transform_writer.clone(),
        "data.transformed",
        serde_json::json!({
            "sensor": "temperature",
            "celsius": 23.5,
            "fahrenheit": 74.3
        })
    );
    
    let transform_envelope = journal.append(
        &transform_writer, 
        transform_event, 
        Some(&source_envelope)  // Parent relationship
    ).await.expect("Failed to append transform event");
    
    // Sink acknowledges the transformed data
    let sink_event = ChainEvent::new(
        EventId::new(),
        sink_writer.clone(),
        "data.stored",
        serde_json::json!({
            "location": "timeseries_db",
            "status": "success"
        })
    );
    
    let _sink_envelope = journal.append(
        &sink_writer,
        sink_event,
        Some(&transform_envelope)  // Parent relationship
    ).await.expect("Failed to append sink event");
    
    // Read all events
    let all_events = journal.read_causally_ordered().await.expect("Failed to read events");
    assert_eq!(all_events.len(), 3);
    
    // Verify event types in order
    assert_eq!(all_events[0].event.event_type, "data.received");
    assert_eq!(all_events[1].event.event_type, "data.transformed");
    assert_eq!(all_events[2].event.event_type, "data.stored");
    
    // Read events after the source event
    let after_source = journal.read_causally_after(&source_envelope.event.id)
        .await
        .expect("Failed to read after source");
    assert_eq!(after_source.len(), 2);
    assert_eq!(after_source[0].event.event_type, "data.transformed");
}

#[tokio::test]
async fn test_journal_parallel_writers() {
    let journal = MemoryJournal::new();
    
    // Simulate parallel workers processing in parallel
    let worker1 = WriterId::new();
    let worker2 = WriterId::new();
    let worker3 = WriterId::new();
    
    // All workers start processing at the same time (no parent)
    let event1 = ChainEvent::new(
        EventId::new(),
        worker1.clone(),
        "work.started",
        serde_json::json!({"worker": 1, "task": "process_batch_1"})
    );
    
    let event2 = ChainEvent::new(
        EventId::new(),
        worker2.clone(),
        "work.started",
        serde_json::json!({"worker": 2, "task": "process_batch_2"})
    );
    
    let event3 = ChainEvent::new(
        EventId::new(),
        worker3.clone(),
        "work.started",
        serde_json::json!({"worker": 3, "task": "process_batch_3"})
    );
    
    // Append in parallel (in practice these would be concurrent)
    let _envelope1 = journal.append(&worker1, event1, None).await.unwrap();
    let envelope2 = journal.append(&worker2, event2, None).await.unwrap();
    let _envelope3 = journal.append(&worker3, event3, None).await.unwrap();
    
    // Worker 2 finishes first and emits result
    let result_event = ChainEvent::new(
        EventId::new(),
        worker2.clone(),
        "work.completed",
        serde_json::json!({"worker": 2, "result": "batch_2_processed"})
    );
    
    let _result_envelope = journal.append(&worker2, result_event, Some(&envelope2))
        .await.unwrap();
    
    // Read all events
    let all_events = journal.read_causally_ordered().await.unwrap();
    assert_eq!(all_events.len(), 4);
    
    // Count events by type
    let start_count = all_events.iter()
        .filter(|e| e.event.event_type == "work.started")
        .count();
    let complete_count = all_events.iter()
        .filter(|e| e.event.event_type == "work.completed")
        .count();
    
    assert_eq!(start_count, 3);
    assert_eq!(complete_count, 1);
}

#[tokio::test]
async fn test_journal_event_chain() {
    let journal = MemoryJournal::new();
    let writer = WriterId::new();
    
    // Create a chain of events
    let mut previous_envelope = None;
    
    for i in 0..5 {
        let event = ChainEvent::new(
            EventId::new(),
            writer.clone(),
            "chain.link",
            serde_json::json!({
                "sequence": i,
                "data": format!("Event {}", i)
            })
        );
        
        let envelope = journal.append(&writer, event, previous_envelope.as_ref())
            .await
            .expect("Failed to append chain event");
            
        previous_envelope = Some(envelope);
    }
    
    // Read entire chain
    let chain = journal.read_causally_ordered().await.unwrap();
    assert_eq!(chain.len(), 5);
    
    // Verify sequence
    for (i, envelope) in chain.iter().enumerate() {
        let sequence = envelope.event.payload["sequence"].as_u64().unwrap();
        assert_eq!(sequence, i as u64);
    }
    
    // Read from middle of chain
    let mid_chain = journal.read_causally_after(&chain[2].event.id).await.unwrap();
    assert_eq!(mid_chain.len(), 2);
    assert_eq!(mid_chain[0].event.payload["sequence"], 3);
    assert_eq!(mid_chain[1].event.payload["sequence"], 4);
}