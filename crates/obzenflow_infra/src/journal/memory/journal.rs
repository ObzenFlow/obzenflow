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
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::reader::MemoryJournalReader;

pub(super) struct MemoryJournalState<T: JournalEvent> {
    pub(super) events: Vec<EventEnvelope<T>>,
    writer_clocks: HashMap<WriterId, VectorClock>,
}

/// In-memory journal for testing
#[derive(Clone)]
pub struct MemoryJournal<T: JournalEvent> {
    owner: Option<JournalOwner>,
    journal_id: JournalId,
    state: Arc<Mutex<MemoryJournalState<T>>>,
    /// Flow-shared admission sequencer (FLOWIP-120n F18); see `DiskJournal`.
    admission_sequencer: Option<Arc<AtomicU64>>,
    _phantom: std::marker::PhantomData<T>,
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
            state: Arc::new(Mutex::new(MemoryJournalState {
                events: Vec::new(),
                writer_clocks: HashMap::new(),
            })),
            admission_sequencer: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new in-memory journal with specified owner
    pub fn with_owner(owner: JournalOwner) -> Self {
        Self {
            owner: Some(owner),
            journal_id: JournalId::new(),
            state: Arc::new(Mutex::new(MemoryJournalState {
                events: Vec::new(),
                writer_clocks: HashMap::new(),
            })),
            admission_sequencer: None,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Attach the flow-shared admission sequencer (FLOWIP-120n F18).
    pub fn with_admission_sequencer(mut self, sequencer: Arc<AtomicU64>) -> Self {
        self.admission_sequencer = Some(sequencer);
        self
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for MemoryJournal<T> {
    fn id(&self) -> &JournalId {
        &self.journal_id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self, // Note: &self, not &mut self
        mut event: T,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        crate::journal::ensure_owned(self.owner.as_ref())?;
        // Get writer_id from the event
        let writer_id = *event.writer_id();

        let mut state = self.state.lock().unwrap();

        // FLOWIP-120n F18: stamp under the state lock so sequence order equals
        // append order; re-admitted rows already carry theirs and keep it.
        if let Some(sequencer) = &self.admission_sequencer {
            if event.admission_seq().is_none() {
                event.set_admission_seq(obzenflow_core::AdmissionSeq(
                    sequencer.fetch_add(1, Ordering::Relaxed),
                ));
            }
        }

        // Compute and store this writer's next clock under the mutex.
        let current = state.writer_clocks.get(&writer_id).cloned();
        let vector_clock = CausalOrderingService::advance_for_append(
            current.as_ref(),
            &writer_id.to_string(),
            parent.map(|p| &p.vector_clock),
        );
        state.writer_clocks.insert(writer_id, vector_clock.clone());

        // Create envelope with proper vector clock
        let envelope = EventEnvelope {
            journal_writer_id: JournalWriterId::from(self.journal_id),
            vector_clock,
            timestamp: Utc::now(),
            event,
        };

        // Store event
        state.events.push(envelope.clone());

        Ok(envelope)
    }

    async fn append_group(
        &self,
        group_id: &str,
        mut events: Vec<T>,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        crate::journal::ensure_owned(self.owner.as_ref())?;
        if events.is_empty() {
            return Ok(Vec::new());
        }
        if group_id.is_empty() {
            return Err(JournalError::Implementation {
                message: "Atomic journal group id cannot be empty".to_string(),
                source: "empty atomic journal group id".into(),
            });
        }

        let mut state = self.state.lock().unwrap();
        if let Some(sequencer) = &self.admission_sequencer {
            for event in &mut events {
                if event.admission_seq().is_none() {
                    event.set_admission_seq(obzenflow_core::AdmissionSeq(
                        sequencer.fetch_add(1, Ordering::Relaxed),
                    ));
                }
            }
        }

        // Build against a private clock snapshot, then publish clocks and
        // events together while holding the single state mutex.
        let mut next_writer_clocks = state.writer_clocks.clone();
        let mut envelopes = Vec::with_capacity(events.len());
        for event in events {
            let writer_id = *event.writer_id();
            let vector_clock = CausalOrderingService::advance_for_append(
                next_writer_clocks.get(&writer_id),
                &writer_id.to_string(),
                parent.map(|p| &p.vector_clock),
            );
            next_writer_clocks.insert(writer_id, vector_clock.clone());
            envelopes.push(EventEnvelope {
                journal_writer_id: JournalWriterId::from(self.journal_id),
                vector_clock,
                timestamp: Utc::now(),
                event,
            });
        }
        state.writer_clocks = next_writer_clocks;
        state.events.extend(envelopes.iter().cloned());
        Ok(envelopes)
    }

    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let state = self.state.lock().unwrap();
        Ok(state.events.clone())
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let state = self.state.lock().unwrap();
        Ok(state
            .events
            .iter()
            .find(|e| e.event.id() == event_id)
            .cloned())
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader::new(
            self.state.clone(),
            position,
        )))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let state = self.state.lock().unwrap();
        let len = state.events.len();
        let start = len.saturating_sub(count);

        // Return events in reverse order (most recent first)
        let mut result: Vec<_> = state.events[start..].to_vec();
        result.reverse();
        Ok(result)
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
