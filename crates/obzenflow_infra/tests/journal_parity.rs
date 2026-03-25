// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Simple parity test to ensure DiskJournal and MemoryJournal behave identically

use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
use obzenflow_core::event::types::EventId;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::Journal;
use obzenflow_core::{StageId, WriterId};
use obzenflow_infra::journal::{DiskJournal, MemoryJournal};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[tokio::test]
async fn test_journal_parity() {
    // Create both journals
    let temp_dir = TempDir::new().unwrap();
    let owner = JournalOwner::stage(StageId::new());
    let log_path = temp_dir.path().join("parity_test.log");
    let disk_journal = Arc::new(DiskJournal::with_owner(log_path, owner.clone()).unwrap())
        as Arc<dyn Journal<ChainEvent> + Send + Sync>;

    let memory_journal =
        Arc::new(MemoryJournal::with_owner(owner)) as Arc<dyn Journal<ChainEvent> + Send + Sync>;

    // Test data
    let writer1 = WriterId::from(StageId::new());
    let writer2 = WriterId::from(StageId::new());

    let event1 = ChainEventFactory::data_event(writer1, "test.first", json!({ "value": 42 }));
    let event2 = ChainEventFactory::data_event(writer1, "test.second", json!({ "value": 84 }));
    let event3 = ChainEventFactory::data_event(writer2, "test.parallel", json!({ "value": 100 }));
    let event4 = ChainEventFactory::data_event(writer1, "test.third", json!({ "value": 168 }));

    // Test both journals with identical operations
    for journal in [&disk_journal, &memory_journal] {
        // Event 1: No parent
        let envelope1 = journal.append(event1.clone(), None).await.unwrap();

        // Event 2: Child of event 1
        let envelope2 = journal
            .append(event2.clone(), Some(&envelope1))
            .await
            .unwrap();

        // Event 3: From different writer, no parent
        let _envelope3 = journal.append(event3.clone(), None).await.unwrap();

        // Event 4: Child of event 2
        journal
            .append(event4.clone(), Some(&envelope2))
            .await
            .unwrap();
    }

    // Compare results
    let disk_events = disk_journal.read_causally_ordered().await.unwrap();
    let memory_events = memory_journal.read_causally_ordered().await.unwrap();

    // Should have same number of events
    assert_eq!(disk_events.len(), 4, "DiskJournal should have 4 events");
    assert_eq!(memory_events.len(), 4, "MemoryJournal should have 4 events");

    // Compare each event
    for (i, (disk_event, memory_event)) in disk_events.iter().zip(memory_events.iter()).enumerate()
    {
        assert_eq!(
            disk_event.event.id, memory_event.event.id,
            "EventId mismatch at index {i}"
        );
        assert_eq!(
            disk_event.event.event_type(),
            memory_event.event.event_type(),
            "Event type mismatch at index {i}"
        );

        assert_eq!(
            disk_event.event.payload(),
            memory_event.event.payload(),
            "Payload mismatch at index {i}"
        );

        assert_eq!(
            disk_event.event.writer_id, memory_event.event.writer_id,
            "Writer ID mismatch at index {i}"
        );
    }

    // Test read_event
    let first_disk_id = &disk_events[0].event.id;
    let first_memory_id = &memory_events[0].event.id;

    let disk_lookup = disk_journal.read_event(first_disk_id).await.unwrap();
    let memory_lookup = memory_journal.read_event(first_memory_id).await.unwrap();

    assert!(disk_lookup.is_some(), "DiskJournal should find event by ID");
    assert!(
        memory_lookup.is_some(),
        "MemoryJournal should find event by ID"
    );

    // Test read_causally_after
    let disk_after = disk_journal
        .read_causally_after(first_disk_id)
        .await
        .unwrap();
    let memory_after = memory_journal
        .read_causally_after(first_memory_id)
        .await
        .unwrap();

    assert_eq!(
        disk_after.len(),
        3,
        "DiskJournal should have 3 events after first"
    );
    assert_eq!(
        memory_after.len(),
        3,
        "MemoryJournal should have 3 events after first"
    );
}

#[tokio::test]
async fn test_journal_concurrent_tiebreak_is_event_id() {
    let temp_dir = TempDir::new().unwrap();
    let owner = JournalOwner::stage(StageId::new());
    let log_path = temp_dir.path().join("tiebreak_test.log");

    let disk_journal = Arc::new(DiskJournal::with_owner(log_path, owner.clone()).unwrap())
        as Arc<dyn Journal<ChainEvent> + Send + Sync>;
    let memory_journal =
        Arc::new(MemoryJournal::with_owner(owner)) as Arc<dyn Journal<ChainEvent> + Send + Sync>;

    let writer_a = WriterId::from(StageId::new());
    let writer_b = WriterId::from(StageId::new());

    let mut a = ChainEventFactory::data_event(writer_a, "test.concurrent", json!({ "i": 1 }));
    a.id = EventId::new();

    let mut b = ChainEventFactory::data_event(writer_b, "test.concurrent", json!({ "i": 0 }));
    b.id = EventId::new();

    let (low, high) = if a.id < b.id { (a, b) } else { (b, a) };

    for journal in [&disk_journal, &memory_journal] {
        // Append in the opposite order of the desired causal-tie-break order.
        // If timestamps were used as the concurrent tie-break, this would tend to sort as:
        //   high (earlier append) then low (later append).
        journal.append(high.clone(), None).await.unwrap();
        tokio::time::sleep(Duration::from_millis(2)).await;
        journal.append(low.clone(), None).await.unwrap();

        let ordered = journal.read_causally_ordered().await.unwrap();
        let ordered_ids: Vec<_> = ordered.iter().map(|e| e.event.id).collect();
        assert_eq!(
            ordered_ids,
            vec![low.id, high.id],
            "concurrent tie-break should use EventId ordering"
        );
    }

    // Parity: disk and memory should agree on the deterministic order.
    let disk_ids: Vec<_> = disk_journal
        .read_causally_ordered()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.event.id)
        .collect();
    let memory_ids: Vec<_> = memory_journal
        .read_causally_ordered()
        .await
        .unwrap()
        .into_iter()
        .map(|e| e.event.id)
        .collect();
    assert_eq!(disk_ids, memory_ids);
}

#[tokio::test]
async fn test_read_causally_after_matches_slice_with_concurrent_events() {
    let temp_dir = TempDir::new().unwrap();
    let owner = JournalOwner::stage(StageId::new());
    let log_path = temp_dir.path().join("after_slice_test.log");

    let disk_journal = Arc::new(DiskJournal::with_owner(log_path, owner.clone()).unwrap())
        as Arc<dyn Journal<ChainEvent> + Send + Sync>;
    let memory_journal =
        Arc::new(MemoryJournal::with_owner(owner)) as Arc<dyn Journal<ChainEvent> + Send + Sync>;

    let writer_a = WriterId::from(StageId::new());
    let writer_b = WriterId::from(StageId::new());

    let event_a = ChainEventFactory::data_event(writer_a, "test.a", json!({ "seq": "a" }));
    let event_b = ChainEventFactory::data_event(writer_b, "test.b", json!({ "seq": "b" }));
    let event_c = ChainEventFactory::data_event(writer_a, "test.c", json!({ "seq": "c" }));

    for journal in [&disk_journal, &memory_journal] {
        let env_a = journal.append(event_a.clone(), None).await.unwrap();
        let _env_b = journal.append(event_b.clone(), None).await.unwrap();
        let _env_c = journal.append(event_c.clone(), Some(&env_a)).await.unwrap();

        let ordered = journal.read_causally_ordered().await.unwrap();
        assert_eq!(ordered.len(), 3);

        let reference_id = ordered[0].event.id;
        let expected_ids: Vec<_> = ordered.iter().skip(1).map(|e| e.event.id).collect();

        let after = journal.read_causally_after(&reference_id).await.unwrap();
        let after_ids: Vec<_> = after.into_iter().map(|e| e.event.id).collect();

        assert_eq!(after_ids, expected_ids);
    }
}
