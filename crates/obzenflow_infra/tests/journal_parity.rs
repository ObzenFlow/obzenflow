//! Simple parity test to ensure DiskJournal and MemoryJournal behave identically

use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use obzenflow_core::Journal;
use obzenflow_core::event::{
    chain_event::ChainEvent,
    event_id::EventId,
};
use obzenflow_core::journal::writer_id::WriterId;
use serde_json::json;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn test_journal_parity() {
    // Create both journals
    let temp_dir = TempDir::new().unwrap();
    let disk_journal = Arc::new(
        DiskJournal::new(temp_dir.path().to_path_buf(), "parity_test")
            .await
            .unwrap()
    ) as Arc<dyn Journal + Send + Sync>;
    
    let memory_journal = Arc::new(MemoryJournal::new()) as Arc<dyn Journal + Send + Sync>;
    
    // Test data
    let writer1 = WriterId::new();
    let writer2 = WriterId::new();
    
    // Test both journals with identical operations
    for journal in [&disk_journal, &memory_journal] {
        // Event 1: No parent
        let event1 = ChainEvent::new(
            EventId::new(),
            writer1.clone(),
            "test.first",
            json!({ "value": 42 })
        );
        let envelope1 = journal.append(&writer1, event1, None).await.unwrap();
        
        // Event 2: Child of event 1
        let event2 = ChainEvent::new(
            EventId::new(),
            writer1.clone(),
            "test.second",
            json!({ "value": 84 })
        );
        let envelope2 = journal.append(&writer1, event2, Some(&envelope1)).await.unwrap();
        
        // Event 3: From different writer, no parent
        let event3 = ChainEvent::new(
            EventId::new(),
            writer2.clone(),
            "test.parallel",
            json!({ "value": 100 })
        );
        let _envelope3 = journal.append(&writer2, event3, None).await.unwrap();
        
        // Event 4: Child of event 2
        let event4 = ChainEvent::new(
            EventId::new(),
            writer1.clone(),
            "test.third",
            json!({ "value": 168 })
        );
        journal.append(&writer1, event4, Some(&envelope2)).await.unwrap();
    }
    
    // Compare results
    let disk_events = disk_journal.read_causally_ordered().await.unwrap();
    let memory_events = memory_journal.read_causally_ordered().await.unwrap();
    
    // Should have same number of events
    assert_eq!(disk_events.len(), 4, "DiskJournal should have 4 events");
    assert_eq!(memory_events.len(), 4, "MemoryJournal should have 4 events");
    
    // Compare each event
    for (i, (disk_event, memory_event)) in disk_events.iter().zip(memory_events.iter()).enumerate() {
        assert_eq!(
            disk_event.event.event_type, 
            memory_event.event.event_type,
            "Event type mismatch at index {}", i
        );
        
        assert_eq!(
            disk_event.event.payload,
            memory_event.event.payload,
            "Payload mismatch at index {}", i
        );
        
        assert_eq!(
            disk_event.event.writer_id,
            memory_event.event.writer_id,
            "Writer ID mismatch at index {}", i
        );
    }
    
    // Test read_event
    let first_disk_id = &disk_events[0].event.id;
    let first_memory_id = &memory_events[0].event.id;
    
    let disk_lookup = disk_journal.read_event(first_disk_id).await.unwrap();
    let memory_lookup = memory_journal.read_event(first_memory_id).await.unwrap();
    
    assert!(disk_lookup.is_some(), "DiskJournal should find event by ID");
    assert!(memory_lookup.is_some(), "MemoryJournal should find event by ID");
    
    // Test read_causally_after
    let disk_after = disk_journal.read_causally_after(first_disk_id).await.unwrap();
    let memory_after = memory_journal.read_causally_after(first_memory_id).await.unwrap();
    
    assert_eq!(disk_after.len(), 3, "DiskJournal should have 3 events after first");
    assert_eq!(memory_after.len(), 3, "MemoryJournal should have 3 events after first");
}