use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::EventId;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::{StageId, WriterId};
use obzenflow_infra::journal::disk::disk_journal::DiskJournal;
use serde_json::json;
use std::path::PathBuf;
use uuid::Uuid;

#[tokio::test]
async fn debug_flight_delays_issue() {
    // Test the same setup as flight_delays.rs
    let test_id = Uuid::new_v4();
    let journal_dir = PathBuf::from(format!("target/debug-journal-test-{}", test_id));
    std::fs::create_dir_all(&journal_dir).unwrap();
    let journal_path = journal_dir.join("test_journal.log");

    println!("Creating DiskJournal...");
    let owner = JournalOwner::stage(StageId::new());
    let journal = DiskJournal::with_owner(journal_path.clone(), owner).unwrap();

    println!("Testing single journal append...");
    let writer_id = WriterId::from(StageId::new());
    let event = ChainEventFactory::data_event(writer_id, "test.event", json!({"test": "data"}));

    match journal.append(event, None).await {
        Ok(_) => println!("✅ Journal append successful!"),
        Err(e) => {
            println!("❌ Journal append failed: {}", e);
            panic!("Single append failed: {}", e);
        }
    }

    // Test multiple appends like in the flight delays example
    println!("Testing multiple concurrent appends...");
    let mut handles = vec![];

    for i in 0..8 {
        // Same number as flight delays
        let journal_clone = journal.clone();
        let writer = WriterId::from(StageId::new());
        let handle = tokio::spawn(async move {
            let event = ChainEventFactory::data_event(
                writer,
                "FlightRecord",
                json!({
                    "carrier": "AA",
                    "batch": i,
                    "test": "concurrent_write"
                }),
            );
            journal_clone.append(event, None).await
        });
        handles.push(handle);
    }

    for (i, handle) in handles.into_iter().enumerate() {
        match handle.await.unwrap() {
            Ok(_) => println!("✅ Concurrent append {} successful!", i),
            Err(e) => {
                println!("❌ Concurrent append {} failed: {}", i, e);
                panic!("Concurrent append {} failed: {}", i, e);
            }
        }
    }

    // Verify all events were written
    let events = journal.read_causally_ordered().await.unwrap();
    println!("Total events written: {}", events.len());
    assert_eq!(events.len(), 9); // 1 + 8

    // Cleanup
    std::fs::remove_dir_all(&journal_dir).ok();
    println!("✅ Debug test completed successfully!");
}
