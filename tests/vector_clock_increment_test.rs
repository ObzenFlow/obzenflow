use flowstate_rs::event_store::{EventStore, StageSemantics};
use flowstate_rs::step::ChainEvent;
use flowstate_rs::topology::StageId;
use serde_json::json;

#[tokio::test]
async fn test_vector_clock_increments() {
    // Create event store
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
    
    println!("Created writers:");
    println!("  Writer 1: {:?}", writer1.writer_id());
    println!("  Writer 2: {:?}", writer2.writer_id());
    println!();
    
    // Writer1 creates first event
    let event1 = ChainEvent::new("Event1", json!({"value": 1}));
    let envelope1 = writer1.append(event1, None).await.unwrap();
    println!("Writer1 appended event (no parent):");
    println!("  Vector clock: {:?}", envelope1.vector_clock.all_clocks());
    let writer1_count = envelope1.vector_clock.get(writer1.writer_id());
    println!("  Writer1 component: {}", writer1_count);
    assert_eq!(writer1_count, 1, "First event should have vector clock component 1");
    println!();
    
    // Writer1 creates second event (with parent)
    let event2 = ChainEvent::new("Event2", json!({"value": 2}));
    let envelope2 = writer1.append(event2, Some(&envelope1)).await.unwrap();
    println!("Writer1 appended event (with parent from same writer):");
    println!("  Vector clock: {:?}", envelope2.vector_clock.all_clocks());
    let writer1_count = envelope2.vector_clock.get(writer1.writer_id());
    println!("  Writer1 component: {}", writer1_count);
    assert_eq!(writer1_count, 2, "Second event should have vector clock component 2");
    println!();
    
    // Writer2 creates event depending on writer1's event
    let event3 = ChainEvent::new("Event3", json!({"value": 3}));
    let envelope3 = writer2.append(event3, Some(&envelope2)).await.unwrap();
    println!("Writer2 appended event (with parent from writer1):");
    println!("  Vector clock: {:?}", envelope3.vector_clock.all_clocks());
    let writer1_count = envelope3.vector_clock.get(writer1.writer_id());
    let writer2_count = envelope3.vector_clock.get(writer2.writer_id());
    println!("  Writer1 component: {}", writer1_count);
    println!("  Writer2 component: {}", writer2_count);
    assert_eq!(writer1_count, 2, "Should preserve writer1's count");
    assert_eq!(writer2_count, 1, "Writer2's first event should have component 1");
    println!();
    
    // Writer1 creates another event independently (starting new causal chain)
    let event4 = ChainEvent::new("Event4", json!({"value": 4}));
    let envelope4 = writer1.append(event4, None).await.unwrap();
    println!("Writer1 appended event (no parent - new causal chain):");
    println!("  Vector clock: {:?}", envelope4.vector_clock.all_clocks());
    let writer1_count = envelope4.vector_clock.get(writer1.writer_id());
    let writer2_count = envelope4.vector_clock.get(writer2.writer_id());
    println!("  Writer1 component: {}", writer1_count);
    println!("  Writer2 component: {}", writer2_count);
    
    // This is where the issue might be - when starting a new causal chain,
    // does the vector clock continue from where it left off or reset?
    println!("  ISSUE: Expected writer1 component to be 3, but got {}", writer1_count);
    
    // Read back all events to verify
    let reader = store.reader();
    let all_events = reader.read_causal_order().await.unwrap();
    
    println!("\nAll events in causal order:");
    for (i, event) in all_events.iter().enumerate() {
        println!("  {}: {:?} - clock: {:?}", 
            i, 
            event.event.payload["value"], 
            event.vector_clock.all_clocks()
        );
    }
}