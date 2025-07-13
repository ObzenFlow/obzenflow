//! In-memory journal implementation for testing
//!
//! This provides a simple, thread-safe in-memory implementation
//! of the Journal trait for use in tests.

use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::event_id::EventId;
use obzenflow_core::journal::writer_id::WriterId;
use obzenflow_core::event::vector_clock::{VectorClock, CausalOrderingService};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use chrono::Utc;

/// In-memory journal for testing
#[derive(Clone)]
pub struct MemoryJournal {
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope>>>,
    vector_clocks: Arc<Mutex<HashMap<WriterId, VectorClock>>>,
}

impl MemoryJournal {
    /// Create a new in-memory journal without an owner
    pub fn new() -> Self {
        Self {
            owner: None,
            events: Arc::new(Mutex::new(Vec::new())),
            vector_clocks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Create a new in-memory journal with specified owner
    pub fn with_owner(owner: JournalOwner) -> Self {
        Self {
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
            vector_clocks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get the current number of events
    pub fn len(&self) -> usize {
        self.events.lock().unwrap().len()
    }

    /// Check if the journal is empty
    pub fn is_empty(&self) -> bool {
        self.events.lock().unwrap().is_empty()
    }
}

#[async_trait]
impl Journal for MemoryJournal {
    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,  // Note: &self, not &mut self
        writer_id: &WriterId,
        event: ChainEvent,
        parent: Option<&EventEnvelope>
    ) -> Result<EventEnvelope, JournalError> {
        // Safety check: Ensure journal has an owner before allowing writes
        if self.owner.is_none() {
            return Err(JournalError::Implementation {
                message: "Cannot write to an unowned journal. Journal must have an owner.".to_string(),
                source: "Unowned journal write attempt".into(),
            });
        }
        let mut clocks = self.vector_clocks.lock().unwrap();

        // Get or create vector clock for this writer
        let mut vector_clock = clocks
            .get(writer_id)
            .cloned()
            .unwrap_or_else(VectorClock::new);

        // Update vector clock based on parent
        if let Some(parent_envelope) = parent {
            CausalOrderingService::update_with_parent(&mut vector_clock, &parent_envelope.vector_clock);
        }

        // Increment for this writer
        CausalOrderingService::increment(&mut vector_clock, &writer_id.to_string());

        // Update stored clock
        clocks.insert(writer_id.clone(), vector_clock.clone());
        drop(clocks);

        // Create envelope with proper vector clock
        let envelope = EventEnvelope {
            writer_id: writer_id.clone(),
            vector_clock,
            timestamp: Utc::now(),
            event,
        };

        // Store event
        let mut events = self.events.lock().unwrap();
        events.push(envelope.clone());

        Ok(envelope)
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope>, JournalError> {
        let events = self.events.lock().unwrap();
        let mut events_copy = events.clone();
        drop(events);

        // Sort by vector clock for causal ordering
        events_copy.sort_by(|a, b| {
            CausalOrderingService::causal_compare(&a.vector_clock, &b.vector_clock)
                .unwrap_or_else(|| {
                    // For concurrent events, use timestamp as tiebreaker
                    a.timestamp.cmp(&b.timestamp)
                })
        });

        Ok(events_copy)
    }

    async fn read_causally_after(&self, after_event_id: &EventId) -> Result<Vec<EventEnvelope>, JournalError> {
        let all_events = self.read_causally_ordered().await?;

        // Find the position of the reference event
        let position = all_events.iter()
            .position(|e| &e.event.id == after_event_id);

        match position {
            Some(pos) => Ok(all_events.into_iter().skip(pos + 1).collect()),
            None => Ok(Vec::new()), // Event not found, return empty vec
        }
    }

    async fn read_event(&self, event_id: &EventId) -> Result<Option<EventEnvelope>, JournalError> {
        let events = self.events.lock().unwrap();
        Ok(events.iter()
            .find(|e| &e.event.id == event_id)
            .cloned())
    }
    
    async fn reader(&self) -> Result<Box<dyn JournalReader>, JournalError> {
        // TODO: Implement MemoryJournalReader
        Err(JournalError::Implementation {
            message: "MemoryJournalReader not yet implemented".to_string(),
            source: "Not implemented".into(),
        })
    }
    
    async fn reader_from(&self, _position: u64) -> Result<Box<dyn JournalReader>, JournalError> {
        // TODO: Implement MemoryJournalReader
        Err(JournalError::Implementation {
            message: "MemoryJournalReader not yet implemented".to_string(),
            source: "Not implemented".into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_memory_journal_basic_operations() {
        // Create a test journal with a proper owner
        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let journal = MemoryJournal::with_owner(owner);
        let writer1 = WriterId::new();
        let writer2 = WriterId::new();

        // First event from writer1
        let event1 = ChainEvent::new(
            EventId::new(),
            writer1.clone(),
            "test.event.1",
            json!({"data": "first"})
        );
        let envelope1 = journal.append(&writer1, event1, None).await.unwrap();

        // Second event from writer2, with parent
        let event2 = ChainEvent::new(
            EventId::new(),
            writer2.clone(),
            "test.event.2",
            json!({"data": "second"})
        );
        let envelope2 = journal.append(&writer2, event2, Some(&envelope1)).await.unwrap();

        // Verify causal relationship
        assert!(CausalOrderingService::happened_before(
            &envelope1.vector_clock,
            &envelope2.vector_clock
        ));

        // Read all events
        let all_events = journal.read_causally_ordered().await.unwrap();
        assert_eq!(all_events.len(), 2);

        // Read after first event
        let after_first = journal.read_causally_after(&envelope1.event.id).await.unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].event.id, envelope2.event.id);

        // Test read_event
        let found = journal.read_event(&envelope1.event.id).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().event.id, envelope1.event.id);
    }

    #[tokio::test]
    async fn test_memory_journal_event_not_found() {
        let journal = MemoryJournal::new();
        let unknown_id = EventId::new();

        // Should return None for unknown event
        let result = journal.read_event(&unknown_id).await.unwrap();
        assert!(result.is_none());

        // Should return empty vec for read_causally_after
        let after_unknown = journal.read_causally_after(&unknown_id).await.unwrap();
        assert!(after_unknown.is_empty());
    }

    #[tokio::test]
    async fn test_memory_journal_causal_ordering() {
        // Create a test journal with a proper owner
        let test_pipeline_id = obzenflow_core::PipelineId::new();
        let owner = obzenflow_core::JournalOwner::pipeline(test_pipeline_id);
        let journal = MemoryJournal::with_owner(owner);
        let writer = WriterId::new();

        // Create a chain of events
        let event1 = ChainEvent::new(
            EventId::new(),
            writer.clone(),
            "event.1",
            json!({"seq": 1})
        );
        let envelope1 = journal.append(&writer, event1, None).await.unwrap();

        let event2 = ChainEvent::new(
            EventId::new(),
            writer.clone(),
            "event.2",
            json!({"seq": 2})
        );
        let envelope2 = journal.append(&writer, event2, Some(&envelope1)).await.unwrap();

        let event3 = ChainEvent::new(
            EventId::new(),
            writer.clone(),
            "event.3",
            json!({"seq": 3})
        );
        let _envelope3 = journal.append(&writer, event3, Some(&envelope2)).await.unwrap();

        // Verify causal ordering
        let ordered = journal.read_causally_ordered().await.unwrap();
        assert_eq!(ordered.len(), 3);
        for (i, event) in ordered.iter().enumerate() {
            assert_eq!(event.event.payload["seq"], i + 1);
        }
    }
}
