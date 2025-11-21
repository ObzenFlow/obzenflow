//! Integration tests for journal implementations
//!
//! These tests demonstrate how the journal trait works with
//! proper vector clocks and causal ordering.

use obzenflow_core::event::{
    chain_event::{ChainEvent, ChainEventFactory},
    EventId,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_infra::journal::MemoryJournal;
use serde_json::json;

#[tokio::test]
async fn test_journal_causal_ordering() {
    let journal = MemoryJournal::with_owner(JournalOwner::stage(StageId::new()));

    // Simulate a simple pipeline: source -> transform -> sink
    let source_writer = WriterId::from(StageId::new());
    let transform_writer = WriterId::from(StageId::new());
    let sink_writer = WriterId::from(StageId::new());

    // Source emits first event
    let source_event = ChainEventFactory::data_event(
        source_writer,
        "data.received",
        json!({
            "sensor": "temperature",
            "value": 23.5
        }),
    );

    let source_envelope = journal
        .append(source_event, None)
        .await
        .expect("Failed to append source event");

    // Transform processes the source event
    let transform_event = ChainEventFactory::data_event(
        transform_writer,
        "data.transformed",
        json!({
            "sensor": "temperature",
            "celsius": 23.5,
            "fahrenheit": 74.3
        }),
    );

    let transform_envelope = journal
        .append(
            transform_event,
            Some(&source_envelope), // Parent relationship
        )
        .await
        .expect("Failed to append transform event");

    // Sink acknowledges the transformed data
    let sink_event = ChainEventFactory::data_event(
        sink_writer,
        "data.stored",
        json!({
            "location": "timeseries_db",
            "status": "success"
        }),
    );

    let _sink_envelope = journal
        .append(
            sink_event,
            Some(&transform_envelope), // Parent relationship
        )
        .await
        .expect("Failed to append sink event");

    // Read all events
    let all_events = journal
        .read_causally_ordered()
        .await
        .expect("Failed to read events");
    assert_eq!(all_events.len(), 3);

    // Verify event types in order
    assert_eq!(all_events[0].event.event_type(), "data.received");
    assert_eq!(all_events[1].event.event_type(), "data.transformed");
    assert_eq!(all_events[2].event.event_type(), "data.stored");

    // Read events after the source event
    let after_source = journal
        .read_causally_after(&source_envelope.event.id)
        .await
        .expect("Failed to read after source");
    assert_eq!(after_source.len(), 2);
    assert_eq!(after_source[0].event.event_type(), "data.transformed");
}

#[tokio::test]
async fn test_journal_parallel_writers() {
    let journal = MemoryJournal::with_owner(JournalOwner::stage(StageId::new()));

    // Simulate parallel workers processing in parallel
    let worker1 = WriterId::from(StageId::new());
    let worker2 = WriterId::from(StageId::new());
    let worker3 = WriterId::from(StageId::new());

    // All workers start processing at the same time (no parent)
    let event1 = ChainEventFactory::data_event(
        worker1,
        "work.started",
        json!({"worker": 1, "task": "process_batch_1"}),
    );

    let event2 = ChainEventFactory::data_event(
        worker2,
        "work.started",
        json!({"worker": 2, "task": "process_batch_2"}),
    );

    let event3 = ChainEventFactory::data_event(
        worker3,
        "work.started",
        json!({"worker": 3, "task": "process_batch_3"}),
    );

    // Append in parallel (in practice these would be concurrent)
    let _envelope1 = journal.append(event1, None).await.unwrap();
    let envelope2 = journal.append(event2, None).await.unwrap();
    let _envelope3 = journal.append(event3, None).await.unwrap();

    // Worker 2 finishes first and emits result
    let result_event = ChainEventFactory::data_event(
        worker2,
        "work.completed",
        json!({"worker": 2, "result": "batch_2_processed"}),
    );

    let _result_envelope = journal
        .append(result_event, Some(&envelope2))
        .await
        .unwrap();

    // Read all events
    let all_events = journal.read_causally_ordered().await.unwrap();
    assert_eq!(all_events.len(), 4);

    // Count events by type
    let start_count = all_events
        .iter()
        .filter(|e| e.event.event_type() == "work.started")
        .count();
    let complete_count = all_events
        .iter()
        .filter(|e| e.event.event_type() == "work.completed")
        .count();

    assert_eq!(start_count, 3);
    assert_eq!(complete_count, 1);
}

#[tokio::test]
async fn test_journal_event_chain() {
    let journal = MemoryJournal::with_owner(JournalOwner::stage(StageId::new()));
    let writer = WriterId::from(StageId::new());

    // Create a chain of events
    let mut previous_envelope = None;

    for i in 0..5 {
        let event = ChainEventFactory::data_event(
            writer,
            "chain.link",
            json!({
                "sequence": i,
                "data": format!("Event {}", i)
            }),
        );

        let envelope = journal
            .append(event, previous_envelope.as_ref())
            .await
            .expect("Failed to append chain event");

        previous_envelope = Some(envelope);
    }

    // Read entire chain
    let chain = journal.read_causally_ordered().await.unwrap();
    assert_eq!(chain.len(), 5);

    // Verify sequence
    for (i, envelope) in chain.iter().enumerate() {
        let payload = envelope.event.payload();
        let sequence = payload["sequence"].as_u64().unwrap();
        assert_eq!(sequence, i as u64);
    }

    // Read from middle of chain
    let mid_chain = journal
        .read_causally_after(&chain[2].event.id)
        .await
        .unwrap();
    assert_eq!(mid_chain.len(), 2);
    let payload0 = mid_chain[0].event.payload();
    let payload1 = mid_chain[1].event.payload();
    assert_eq!(payload0["sequence"], 3);
    assert_eq!(payload1["sequence"], 4);
}
