// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! In-memory journal implementation for testing
//!
//! This provides a simple, thread-safe in-memory implementation
//! of the Journal trait for use in tests.

use async_trait::async_trait;
use chrono::Utc;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::{EventId, JournalWriterId, WriterId};
use obzenflow_core::event::vector_clock::{CausalOrderingService, VectorClock};
use obzenflow_core::event::JournalEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// In-memory journal for testing
#[derive(Clone)]
pub struct MemoryJournal<T: JournalEvent> {
    owner: Option<JournalOwner>,
    journal_id: JournalId,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    writer_clocks: Arc<Mutex<HashMap<WriterId, VectorClock>>>,
    _phantom: std::marker::PhantomData<T>,
}

/// Reader for MemoryJournal
///
/// This reader iterates over the journal as events are appended (tail-like semantics).
/// When it reaches the current end of the journal it returns `Ok(None)` for that call,
/// but subsequent calls will observe newly appended events.
pub struct MemoryJournalReader<T: JournalEvent> {
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    position: u64,
}

impl<T: JournalEvent> MemoryJournalReader<T> {
    pub fn new(events: Arc<Mutex<Vec<EventEnvelope<T>>>>, position: u64) -> Self {
        let len = events.lock().unwrap().len() as u64;
        let clamped = position.min(len);
        Self {
            events,
            position: clamped,
        }
    }
}

impl<T: JournalEvent> Default for MemoryJournal<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: JournalEvent> MemoryJournal<T> {
    /// Create a new in-memory journal without an owner
    pub fn new() -> Self {
        Self {
            owner: None,
            journal_id: JournalId::new(),
            events: Arc::new(Mutex::new(Vec::new())),
            writer_clocks: Arc::new(Mutex::new(HashMap::new())),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new in-memory journal with specified owner
    pub fn with_owner(owner: JournalOwner) -> Self {
        Self {
            owner: Some(owner),
            journal_id: JournalId::new(),
            events: Arc::new(Mutex::new(Vec::new())),
            writer_clocks: Arc::new(Mutex::new(HashMap::new())),
            _phantom: std::marker::PhantomData,
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
impl<T: JournalEvent + 'static> Journal<T> for MemoryJournal<T> {
    fn storage_kind(&self) -> obzenflow_core::journal::JournalStorageKind {
        obzenflow_core::journal::JournalStorageKind::Memory
    }

    fn id(&self) -> &JournalId {
        &self.journal_id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self, // Note: &self, not &mut self
        event: T,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        // Safety check: Ensure journal has an owner before allowing writes
        if self.owner.is_none() {
            return Err(JournalError::Implementation {
                message: "Cannot write to an unowned journal. Journal must have an owner."
                    .to_string(),
                source: "Unowned journal write attempt".into(),
            });
        }
        // Get writer_id from the event
        let writer_id = *event.writer_id();

        let mut clocks = self.writer_clocks.lock().unwrap();

        // Get or create vector clock for this writer
        let mut vector_clock = clocks
            .get(&writer_id)
            .cloned()
            .unwrap_or_else(VectorClock::new);

        // Update vector clock based on parent
        if let Some(parent_envelope) = parent {
            CausalOrderingService::update_with_parent(
                &mut vector_clock,
                &parent_envelope.vector_clock,
            );
        }

        // Increment for this writer
        CausalOrderingService::increment(&mut vector_clock, &writer_id.to_string());

        // Update stored clock
        clocks.insert(writer_id, vector_clock.clone());
        drop(clocks);

        // Create envelope with proper vector clock
        let envelope = EventEnvelope {
            journal_writer_id: JournalWriterId::from(self.journal_id),
            vector_clock,
            timestamp: Utc::now(),
            event,
        };

        // Store event
        let mut events = self.events.lock().unwrap();
        events.push(envelope.clone());

        Ok(envelope)
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let events = self.events.lock().unwrap();
        let events_copy = events.clone();
        drop(events);

        CausalOrderingService::order_envelopes_by_event_id(events_copy)
    }

    async fn read_causally_after(
        &self,
        after_event_id: &EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let all_events = self.read_causally_ordered().await?;

        // Find the position of the reference event
        let position = all_events
            .iter()
            .position(|e| e.event.id() == after_event_id);

        match position {
            Some(pos) => Ok(all_events.into_iter().skip(pos + 1).collect()),
            None => Ok(Vec::new()), // Event not found, return empty vec
        }
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let events = self.events.lock().unwrap();
        Ok(events.iter().find(|e| e.event.id() == event_id).cloned())
    }

    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader::new(self.events.clone(), 0)))
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader::new(
            self.events.clone(),
            position,
        )))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let events = self.events.lock().unwrap();
        let len = events.len();
        let start = len.saturating_sub(count);

        // Return events in reverse order (most recent first)
        let mut result: Vec<_> = events[start..].to_vec();
        result.reverse();
        Ok(result)
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> JournalReader<T> for MemoryJournalReader<T> {
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let env = {
            let events = self.events.lock().unwrap();
            events.get(self.position as usize).cloned()
        };

        if env.is_some() {
            self.position += 1;
        } else {
            // This reader is frequently polled inside tight async loops that rely on timers.
            // Without an `.await` point here, `next()` can complete immediately forever and
            // starve the executor (preventing timeouts/other tasks from making progress).
            tokio::task::yield_now().await;
        }
        Ok(env)
    }

    async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
        let len = self.events.lock().unwrap().len() as u64;
        let start = self.position;
        let target = (self.position + n).min(len);
        self.position = target;
        Ok(target - start)
    }

    fn position(&self) -> u64 {
        self.position
    }

    fn is_at_end(&self) -> bool {
        self.position as usize >= self.events.lock().unwrap().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::id::StageId;
    use serde_json::json;

    #[tokio::test]
    async fn test_memory_journal_basic_operations() {
        // Create a test journal with a proper owner
        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let journal = MemoryJournal::with_owner(owner);
        let writer1 = WriterId::from(StageId::new());
        let writer2 = WriterId::from(StageId::new());

        // First event from writer1
        let event1 =
            ChainEventFactory::data_event(writer1, "test.event.1", json!({"data": "first"}));
        let envelope1 = journal.append(event1, None).await.unwrap();

        // Second event from writer2, with parent
        let event2 =
            ChainEventFactory::data_event(writer2, "test.event.2", json!({"data": "second"}));
        let envelope2 = journal.append(event2, Some(&envelope1)).await.unwrap();

        // Verify causal relationship
        assert!(CausalOrderingService::happened_before(
            &envelope1.vector_clock,
            &envelope2.vector_clock
        ));

        // Read all events
        let all_events = journal.read_causally_ordered().await.unwrap();
        assert_eq!(all_events.len(), 2);

        // Read after first event
        let after_first = journal
            .read_causally_after(&envelope1.event.id)
            .await
            .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(after_first[0].event.id, envelope2.event.id);

        // Test read_event
        let found = journal.read_event(&envelope1.event.id).await.unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().event.id, envelope1.event.id);
    }

    #[tokio::test]
    async fn test_memory_journal_event_not_found() {
        let journal = MemoryJournal::<ChainEvent>::new();
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
        let test_system_id = obzenflow_core::SystemId::new();
        let owner = obzenflow_core::JournalOwner::system(test_system_id);
        let journal = MemoryJournal::with_owner(owner);
        let writer = WriterId::from(StageId::new());

        // Create a chain of events
        let event1 = ChainEventFactory::data_event(writer, "event.1", json!({"seq": 1}));
        let envelope1 = journal.append(event1, None).await.unwrap();

        let event2 = ChainEventFactory::data_event(writer, "event.2", json!({"seq": 2}));
        let envelope2 = journal.append(event2, Some(&envelope1)).await.unwrap();

        let event3 = ChainEventFactory::data_event(writer, "event.3", json!({"seq": 3}));
        journal.append(event3, Some(&envelope2)).await.unwrap();

        // Verify causal ordering
        let ordered = journal.read_causally_ordered().await.unwrap();
        assert_eq!(ordered.len(), 3);
        for (i, event) in ordered.iter().enumerate() {
            assert_eq!(event.event.payload()["seq"], i + 1);
        }
    }

    #[tokio::test]
    async fn test_memory_journal_reader_sees_all_events_in_order() {
        // Create a test journal with a proper owner
        let test_stage_id = obzenflow_core::StageId::new();
        let owner = obzenflow_core::JournalOwner::stage(test_stage_id);
        let journal = MemoryJournal::with_owner(owner);
        let writer = WriterId::from(StageId::new());

        // Append a small sequence of events
        let e1 = ChainEventFactory::data_event(writer, "reader.test.1", json!({"seq": 1}));
        let e2 = ChainEventFactory::data_event(writer, "reader.test.2", json!({"seq": 2}));
        journal.append(e1, None).await.unwrap();
        journal.append(e2, None).await.unwrap();

        // Read via reader() and compare with read_causally_ordered()
        let all = journal.read_causally_ordered().await.unwrap();
        assert_eq!(all.len(), 2);

        let mut reader = journal.reader().await.unwrap();
        let mut seen = Vec::new();
        while let Some(env) = reader.next().await.unwrap() {
            seen.push(*env.event.id());
        }

        let all_ids: Vec<_> = all.iter().map(|e| *e.event.id()).collect();
        assert_eq!(seen, all_ids);
        assert!(reader.is_at_end());
    }
}
