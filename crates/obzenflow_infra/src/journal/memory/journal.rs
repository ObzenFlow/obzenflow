// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! In-memory journal implementation for testing
//!
//! This provides a simple, thread-safe in-memory implementation
//! of the Journal trait for use in tests.

use crate::journal::metrics_tail::{Carrier, MetricsTailIndex};
use crate::journal::observability::JournalObservability;
use crate::journal::observation_index::{locate, unavailable, ObservationIndex};
use async_trait::async_trait;
use chrono::Utc;
use obzenflow_core::event::identity::{EventId, JournalWriterId, WriterId};
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::provenance::{JournalGroupMember, JournalProvenance};
use obzenflow_core::event::JournalEvent;
use obzenflow_core::event::{CausalCommit, CausalCoordinate, CausalFrontier};
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::journal::{AppendOptions, JournalConfig};
use obzenflow_core::journal::{
    JournalObservationReader, LocatedObservation, ObservationKey, ObservationLookup,
};
use obzenflow_core::FlowId;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use super::reader::MemoryJournalReader;

pub(super) struct MemoryJournalState<T: JournalEvent> {
    pub(super) events: Vec<JournalRecord<T::Payload>>,
    writer_clocks: HashMap<WriterId, CausalCommit>,
    observations: ObservationIndex,
    metrics_tail: MetricsTailIndex,
}

/// In-memory journal for testing
#[derive(Clone)]
pub struct MemoryJournal<T: JournalEvent> {
    owner: Option<JournalOwner>,
    journal_id: JournalId,
    run_id: FlowId,
    state: Arc<Mutex<MemoryJournalState<T>>>,
    /// Flow-shared admission sequencer (FLOWIP-120n F18); see `DiskJournal`.
    admission_sequencer: Option<Arc<AtomicU64>>,
    observability: JournalObservability,
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
            run_id: FlowId::new(),
            state: Arc::new(Mutex::new(MemoryJournalState {
                events: Vec::new(),
                writer_clocks: HashMap::new(),
                observations: ObservationIndex::default(),
                metrics_tail: MetricsTailIndex::default(),
            })),
            admission_sequencer: None,
            observability: JournalObservability::default(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new in-memory journal with specified owner
    pub fn with_owner(owner: JournalOwner) -> Self {
        Self {
            owner: Some(owner),
            journal_id: JournalId::new(),
            run_id: FlowId::new(),
            state: Arc::new(Mutex::new(MemoryJournalState {
                events: Vec::new(),
                writer_clocks: HashMap::new(),
                observations: ObservationIndex::default(),
                metrics_tail: MetricsTailIndex::default(),
            })),
            admission_sequencer: None,
            observability: JournalObservability::default(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// Construct a new journal incarnation within a run before sharing it.
    pub fn with_owner_in_run(owner: JournalOwner, run_id: FlowId) -> Self {
        Self {
            run_id,
            ..Self::with_owner(owner)
        }
    }

    /// Attach the flow-shared admission sequencer (FLOWIP-120n F18).
    pub fn with_admission_sequencer(mut self, sequencer: Arc<AtomicU64>) -> Self {
        self.admission_sequencer = Some(sequencer);
        self
    }
}

impl<T: JournalEvent> MemoryJournal<T> {
    fn append_record(
        &self, // Note: &self, not &mut self
        mut event: T,
        frontier: &CausalFrontier,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
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

        let (commitment, causal) = CausalCommit::prepare(
            self.run_id,
            CausalCoordinate::new(self.journal_id.into(), writer_id),
            *event.id(),
            state.writer_clocks.get(&writer_id),
            frontier,
        )?;

        // Create envelope with proper vector clock
        let envelope = {
            let (authored, payload) = event.into_parts();
            JournalRecord::commit(
                authored,
                payload,
                JournalProvenance {
                    journal_writer_id: JournalWriterId::from(self.journal_id),
                    run_id: self.run_id,
                    causal,
                    vector_clock: commitment.clock.clone(),
                    timestamp: Utc::now(),
                    journal_group_id: None,
                    journal_group_member: None,
                },
            )
            .map_err(|error| JournalError::Implementation {
                message: "Invalid journal record".to_string(),
                source: Box::new(error),
            })?
        };

        // Store event
        state.writer_clocks.insert(writer_id, commitment);
        state.events.push(envelope.clone());
        state.index_committed(1);

        Ok(envelope)
    }

    fn append_records(
        &self,
        group_id: &str,
        mut events: Vec<T>,
        frontier: &CausalFrontier,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
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
        let group_size = u32::try_from(events.len()).map_err(|_| JournalError::Implementation {
            message: format!("Atomic journal group '{group_id}' exceeds u32 member capacity"),
            source: "atomic journal group is too large".into(),
        })?;
        let mut envelopes = Vec::with_capacity(events.len());
        let mut group_frontier = frontier.clone();
        for (index, event) in events.into_iter().enumerate() {
            let writer_id = *event.writer_id();
            let (commitment, causal) = CausalCommit::prepare(
                self.run_id,
                CausalCoordinate::new(self.journal_id.into(), writer_id),
                *event.id(),
                next_writer_clocks.get(&writer_id),
                &group_frontier,
            )?;
            group_frontier.merge(&commitment.frontier())?;
            next_writer_clocks.insert(writer_id, commitment.clone());
            envelopes.push({
                let (authored, payload) = event.into_parts();
                JournalRecord::commit(
                    authored,
                    payload,
                    JournalProvenance {
                        journal_writer_id: JournalWriterId::from(self.journal_id),
                        run_id: self.run_id,
                        causal,
                        vector_clock: commitment.clock.clone(),
                        timestamp: Utc::now(),
                        journal_group_id: Some(group_id.to_string()),
                        journal_group_member: Some(JournalGroupMember {
                            index: u32::try_from(index)
                                .expect("group size was checked against u32 capacity"),
                            size: group_size,
                        }),
                    },
                )
                .map_err(|error| JournalError::Implementation {
                    message: "Invalid journal record".to_string(),
                    source: Box::new(error),
                })?
            });
        }
        state.writer_clocks = next_writer_clocks;
        state.events.extend(envelopes.iter().cloned());
        state.index_committed(envelopes.len());
        Ok(envelopes)
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

    fn observation_reader(&self) -> Option<&dyn JournalObservationReader> {
        Some(self)
    }

    fn configure(&self, config: JournalConfig) -> Result<(), JournalError> {
        self.observability.configure(config.observability)
    }

    async fn append(
        &self,
        event: T,
        options: AppendOptions<T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        let AppendOptions { frontier, capture } = options;
        let (mut events, reservation) = self.observability.prepare(vec![event], capture);
        let result = self
            .append_record(events.pop().expect("one event"), &frontier)
            .map(|record| vec![record]);
        reservation.finish::<T>(&result);
        result.map(|mut records| records.pop().expect("one record"))
    }

    async fn append_group(
        &self,
        group_id: &str,
        events: Vec<T>,
        options: AppendOptions<T>,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        let AppendOptions { frontier, capture } = options;
        let (events, reservation) = self.observability.prepare(events, capture);
        let result = self.append_records(group_id, events, &frontier);
        reservation.finish::<T>(&result);
        result
    }

    async fn read_all_unordered(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        let state = self.state.lock().unwrap();
        Ok(state.events.clone())
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        let state = self.state.lock().unwrap();
        Ok(state.events.iter().find(|e| e.id() == event_id).cloned())
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader::new(
            self.state.clone(),
            position,
        )))
    }

    async fn read_metrics_tail(&self) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        let state = self.state.lock().unwrap();
        Ok(state
            .metrics_tail
            .carriers()
            .into_iter()
            .filter_map(|carrier| state.events.get(carrier.offset as usize).cloned())
            .collect())
    }

    async fn read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
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

impl<T: JournalEvent> MemoryJournalState<T> {
    fn index_committed(&mut self, new_records: usize) {
        for (position, record) in self
            .events
            .iter()
            .enumerate()
            .skip(self.events.len() - new_records)
        {
            self.metrics_tail.observe(
                record,
                Carrier {
                    offset: position as u64,
                    member: 0,
                },
            );
        }
        if self.observations.examined_through != (self.events.len() - new_records) as u64 {
            return;
        }
        // A failed optional update leaves the physical records committed. The
        // next lookup resumes from the last completely indexed prefix.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self.rebuild_observations(new_records);
        }));
    }

    fn rebuild_observations(&mut self, budget: usize) -> Result<(), JournalError> {
        for row in self
            .events
            .iter()
            .skip(self.observations.examined_through as usize)
            .take(budget)
        {
            self.observations
                .observe(row.envelope.observability.as_ref(), 0, 0)?;
        }
        Ok(())
    }

    fn lookup<R>(
        &mut self,
        lookup: impl FnOnce(&Self) -> Result<R, JournalError>,
    ) -> Result<ObservationLookup<R>, JournalError> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.rebuild_observations(256)?;
            let committed_len = self.events.len() as u64;
            if self.observations.examined_through < committed_len {
                return Ok(ObservationLookup::Rebuilding {
                    examined_through: self.observations.examined_through,
                    committed_len: Some(committed_len),
                });
            }
            Ok(ObservationLookup::Ready {
                committed_len,
                observation: lookup(self)?,
            })
        }))
        .unwrap_or_else(|_| Err(unavailable("memory observation lookup panicked")));
        if result.is_err() {
            self.observations = ObservationIndex::default();
        }
        result
    }

    fn observation(
        &self,
        key: &ObservationKey,
    ) -> Result<Option<LocatedObservation>, JournalError> {
        let Some(locator) = self
            .observations
            .entries
            .get(key)
            .and_then(|history| history.back())
        else {
            return Ok(None);
        };
        let packet = self
            .events
            .get(locator.position as usize)
            .and_then(|record| record.envelope.observability.clone())
            .ok_or_else(|| unavailable("missing memory carrier"))?;
        locate(key, locator, packet).map(Some)
    }
}

#[async_trait]
impl<T: JournalEvent> JournalObservationReader for MemoryJournal<T> {
    async fn latest_observation(
        &self,
        key: &ObservationKey,
    ) -> Result<ObservationLookup, JournalError> {
        self.state
            .lock()
            .unwrap()
            .lookup(|state| state.observation(key))
    }

    async fn latest_observations(
        &self,
        observer: WriterId,
    ) -> Result<ObservationLookup<Vec<LocatedObservation>>, JournalError> {
        self.state.lock().unwrap().lookup(|state| {
            state
                .observations
                .entries
                .keys()
                .filter(|key| key.observer == observer)
                .map(|key| {
                    state
                        .observation(key)
                        .map(|value| value.expect("indexed family"))
                })
                .collect::<Result<_, _>>()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::chain_event::{ChainEvent, ChainEventFactory};
    use obzenflow_core::event::vector_clock::CausalOrderingService;
    use obzenflow_core::id::StageId;
    use serde_json::json;

    #[tokio::test]
    async fn optional_lookup_panic_does_not_poison_memory_facts() {
        let stage = StageId::new();
        let journal = MemoryJournal::with_owner(JournalOwner::stage(stage));
        let event = crate::journal::observability::tests::event(stage, 1);
        journal
            .append(event.clone(), Default::default())
            .await
            .unwrap();
        assert!(journal
            .state
            .lock()
            .unwrap()
            .lookup::<()>(|_| panic!("optional lookup failure"))
            .is_err());
        journal.append(event, Default::default()).await.unwrap();
        assert!(matches!(
            journal.latest_observations(stage.into()).await.unwrap(),
            ObservationLookup::Ready {
                committed_len: 2,
                ..
            }
        ));
    }

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
        let envelope1 = journal.append(event1, Default::default()).await.unwrap();

        // Second event from writer2, with parent
        let event2 =
            ChainEventFactory::data_event(writer2, "test.event.2", json!({"data": "second"}));
        let envelope2 = journal
            .append(
                event2,
                AppendOptions::from_record(Some(&envelope1)).unwrap(),
            )
            .await
            .unwrap();

        // Verify causal relationship
        assert!(CausalOrderingService::happened_before(
            &envelope1.envelope.provenance.journal.vector_clock,
            &envelope2.envelope.provenance.journal.vector_clock
        ));

        // Read all events
        let all_events = journal.read_causally_ordered().await.unwrap();
        assert_eq!(all_events.len(), 2);

        // Read after first event
        let after_first = journal
            .read_causally_after(&envelope1.envelope.provenance.event.id)
            .await
            .unwrap();
        assert_eq!(after_first.len(), 1);
        assert_eq!(
            after_first[0].envelope.provenance.event.id,
            envelope2.envelope.provenance.event.id
        );

        // Test read_event
        let found = journal
            .read_event(&envelope1.envelope.provenance.event.id)
            .await
            .unwrap();
        assert!(found.is_some());
        assert_eq!(
            found.unwrap().envelope.provenance.event.id,
            envelope1.envelope.provenance.event.id
        );
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
        let envelope1 = journal.append(event1, Default::default()).await.unwrap();

        let event2 = ChainEventFactory::data_event(writer, "event.2", json!({"seq": 2}));
        let envelope2 = journal
            .append(
                event2,
                AppendOptions::from_record(Some(&envelope1)).unwrap(),
            )
            .await
            .unwrap();

        let event3 = ChainEventFactory::data_event(writer, "event.3", json!({"seq": 3}));
        journal
            .append(
                event3,
                AppendOptions::from_record(Some(&envelope2)).unwrap(),
            )
            .await
            .unwrap();

        // Verify causal ordering
        let ordered = journal.read_causally_ordered().await.unwrap();
        assert_eq!(ordered.len(), 3);
        for (i, event) in ordered.iter().enumerate() {
            assert_eq!(event.payload()["seq"], i + 1);
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
        journal.append(e1, Default::default()).await.unwrap();
        journal.append(e2, Default::default()).await.unwrap();

        // Read via reader() and compare with read_causally_ordered()
        let all = journal.read_causally_ordered().await.unwrap();
        assert_eq!(all.len(), 2);

        let mut reader = journal.reader().await.unwrap();
        let mut seen = Vec::new();
        while let Some(env) = reader.next().await.unwrap() {
            seen.push(*env.id());
        }

        let all_ids: Vec<_> = all.iter().map(|e| *e.id()).collect();
        assert_eq!(seen, all_ids);
        assert!(reader.is_at_end());
    }
}
