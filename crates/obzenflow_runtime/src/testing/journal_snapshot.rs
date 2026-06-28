// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Vector-clock test helpers (FLOWIP-114n).
//!
//! The core surface is [`JournalSnapshot`], which eagerly captures a journal's
//! current tail into memory and provides two deterministic views over the captured
//! envelopes:
//! - append order: the journal cursor order at capture time;
//! - causal order: a deterministic total order that preserves happened-before and
//!   breaks concurrent ties by `EventId` (the framework contract).
//!
//! All causal assertions operate on *envelope vector clocks*, never on payload-level
//! identity like `correlation_id` (FLOWIP-051p).

use crate::testing::probe::JournalProbeError;
use crate::testing::FlowTestHarness;
use obzenflow_core::event::chain_event::{ChainEvent, ChainEventContent};
use obzenflow_core::event::system_event::SystemEvent;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{EventEnvelope, JournalEvent, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_core::EventId;
use std::sync::Arc;
use thiserror::Error;

#[doc(hidden)]
pub trait DirectParentId {
    fn direct_parent_id(&self) -> Option<ParentEventId>;
}

impl DirectParentId for ChainEvent {
    fn direct_parent_id(&self) -> Option<ParentEventId> {
        self.causality
            .parent_ids
            .first()
            .copied()
            .map(ParentEventId::from_event_id)
    }
}

impl DirectParentId for SystemEvent {
    fn direct_parent_id(&self) -> Option<ParentEventId> {
        None
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalOrder {
    /// Journal cursor order at capture time.
    Append,
    /// Deterministic causal readback order over the captured envelopes.
    Causal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceMatchMode {
    /// The ordered snapshot must match the shaped sequence exactly.
    Exact,
    /// The ordered snapshot must start with the shaped sequence.
    Prefix,
    /// The shaped sequence must appear as a contiguous window within the ordered snapshot.
    ContiguousSubsequence,
    /// The shaped sequence must appear as an ordered (not necessarily contiguous) subsequence.
    OrderedSubsequence,
}

/// Marker wrapper around an event id that has been observed from a journal.
///
/// This is used to prevent grouping fan-out assertions by payload identity (like
/// `correlation_id`). A `ParentEventId` is obtained from an observed parent
/// [`EventEnvelope`], not constructed from an arbitrary ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ParentEventId(EventId);

impl ParentEventId {
    pub fn of<T: JournalEvent + 'static>(env: &EventEnvelope<T>) -> Self {
        Self(*env.event.id())
    }

    pub fn as_event_id(&self) -> EventId {
        self.0
    }

    fn from_event_id(id: EventId) -> Self {
        Self(id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParentSelector {
    /// Direct parent event id (matches `event.causality.parent_ids.first()` for `ChainEvent`).
    ParentEventId(ParentEventId),
    /// Exact vector-clock component `(writer_id, seq)`.
    VectorClockComponent { writer_id: WriterId, seq: u64 },
}

/// Fan-out grouping key.
///
/// Constructible only from explicit causal predecessor information (direct parent id
/// or exact vector-clock component), never from payload-level identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FanOutGroup {
    parent: ParentSelector,
}

impl FanOutGroup {
    pub fn new(parent: ParentSelector) -> Self {
        Self { parent }
    }

    pub fn parent_selector(&self) -> ParentSelector {
        self.parent
    }
}

type EventPredicate<T> = dyn Fn(&EventEnvelope<T>) -> bool + Send + Sync;

#[derive(Clone)]
pub struct EventShape<T: JournalEvent + 'static> {
    description: String,
    predicate: Arc<EventPredicate<T>>,
}

impl<T: JournalEvent + 'static> std::fmt::Debug for EventShape<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventShape")
            .field("description", &self.description)
            .finish()
    }
}

impl<T: JournalEvent + 'static> EventShape<T> {
    pub fn predicate(
        description: impl Into<String>,
        predicate: impl Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            description: description.into(),
            predicate: Arc::new(predicate),
        }
    }

    pub fn matches(&self, env: &EventEnvelope<T>) -> bool {
        (self.predicate)(env)
    }

    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn refine(
        self,
        extra_description: impl Into<String>,
        extra: impl Fn(&EventEnvelope<T>) -> bool + Send + Sync + 'static,
    ) -> Self {
        let prev = self.predicate.clone();
        let extra = Arc::new(extra);
        let description = format!("{} + {}", self.description, extra_description.into());
        Self {
            description,
            predicate: Arc::new(move |env| prev(env) && extra(env)),
        }
    }
}

impl EventShape<ChainEvent> {
    /// Match a chain data envelope by `event_type`.
    pub fn data_type(event_type: impl Into<String>) -> Self {
        let want = event_type.into();
        Self::predicate(format!("ChainEvent::Data({want})"), move |env| {
            match &env.event.content {
                ChainEventContent::Data { event_type, .. } => event_type == &want,
                _ => false,
            }
        })
    }

    /// Refine a data-type shape with a payload predicate.
    pub fn with_payload_predicate(
        self,
        description: impl Into<String>,
        predicate: impl Fn(&serde_json::Value) -> bool + Send + Sync + 'static,
    ) -> Self {
        let predicate = Arc::new(predicate);
        self.refine(description, move |env| match &env.event.content {
            ChainEventContent::Data { payload, .. } => predicate(payload),
            _ => false,
        })
    }

    /// Refine a shape with a predicate over `processing_info.status`.
    pub fn with_status_predicate(
        self,
        description: impl Into<String>,
        predicate: impl Fn(&obzenflow_core::event::status::processing_status::ProcessingStatus) -> bool
            + Send
            + Sync
            + 'static,
    ) -> Self {
        let predicate = Arc::new(predicate);
        self.refine(description, move |env| {
            predicate(&env.event.processing_info.status)
        })
    }
}

impl EventShape<SystemEvent> {
    /// Match a system envelope by predicate over its `SystemEventType`.
    pub fn system_event_predicate(
        description: impl Into<String>,
        predicate: impl Fn(&obzenflow_core::event::SystemEventType) -> bool + Send + Sync + 'static,
    ) -> Self {
        let predicate = Arc::new(predicate);
        Self::predicate(description, move |env| predicate(&env.event.event))
    }
}

#[derive(Debug, Clone)]
pub enum JournalExpectation<T: JournalEvent + 'static> {
    SequenceOrder {
        order: JournalOrder,
        mode: SequenceMatchMode,
        events: Vec<EventShape<T>>,
    },
    /// Parent event happens-before all shaped events (but ordering between shaped events is unconstrained).
    CausalPartialOrder {
        parent: EventShape<T>,
        events: Vec<EventShape<T>>,
    },
    UnorderedMultiset {
        fan_out_group: FanOutGroup,
        events: Vec<EventShape<T>>,
    },
}

#[derive(Debug, Error)]
pub enum JournalExpectationError {
    #[error("journal expectation failed: {message}")]
    Failed { message: String },
}

#[derive(Debug, Error)]
pub enum CausalAssertionError {
    #[error(
        "expected `{a_id}` to happen before `{b_id}`; vector clocks were not strictly ordered"
    )]
    NotHappensBefore { a_id: EventId, b_id: EventId },

    #[error("expected `{a_id}` and `{b_id}` to be concurrent; they were causally ordered")]
    NotConcurrent { a_id: EventId, b_id: EventId },
}

/// Assert strict happened-before (`a < b`) on envelope vector clocks.
pub fn assert_happens_before<T: JournalEvent>(
    a: &EventEnvelope<T>,
    b: &EventEnvelope<T>,
) -> Result<(), CausalAssertionError> {
    if CausalOrderingService::happened_before(&a.vector_clock, &b.vector_clock) {
        Ok(())
    } else {
        Err(CausalAssertionError::NotHappensBefore {
            a_id: *a.event.id(),
            b_id: *b.event.id(),
        })
    }
}

/// Assert concurrency on envelope vector clocks.
pub fn assert_concurrent<T: JournalEvent>(
    a: &EventEnvelope<T>,
    b: &EventEnvelope<T>,
) -> Result<(), CausalAssertionError> {
    if CausalOrderingService::are_concurrent(&a.vector_clock, &b.vector_clock) {
        Ok(())
    } else {
        Err(CausalAssertionError::NotConcurrent {
            a_id: *a.event.id(),
            b_id: *b.event.id(),
        })
    }
}

#[derive(Debug, Clone)]
struct SnapshotRow<T: JournalEvent + 'static> {
    #[allow(dead_code)]
    append_index: u64,
    envelope: EventEnvelope<T>,
}

/// Eager snapshot of a journal at a point in time.
#[derive(Debug, Clone)]
pub struct JournalSnapshot<T: JournalEvent + 'static> {
    rows: Vec<SnapshotRow<T>>,
}

impl JournalSnapshot<ChainEvent> {
    /// Capture a stage data journal by name through [`FlowTestHarness`].
    pub async fn capture(
        harness: &FlowTestHarness,
        stage: &str,
    ) -> Result<JournalSnapshot<ChainEvent>, JournalProbeError> {
        let (_stage_id, journal) = harness.stage_journal_for_test(stage)?;
        Self::capture_chain_journal(journal).await
    }

    /// Capture a directly supplied chain journal (for infra tests).
    pub async fn capture_chain_journal(
        journal: Arc<dyn Journal<ChainEvent>>,
    ) -> Result<JournalSnapshot<ChainEvent>, JournalProbeError> {
        capture_journal(journal).await
    }
}

impl JournalSnapshot<SystemEvent> {
    /// Capture the system journal from the harness.
    pub async fn capture_system_events(
        harness: &FlowTestHarness,
    ) -> Result<JournalSnapshot<SystemEvent>, JournalProbeError> {
        let journal = harness
            .system_journal()
            .ok_or(JournalProbeError::MissingSystemJournal)?;
        Self::capture_system_journal(journal).await
    }

    /// Capture a directly supplied system journal (for infra tests).
    pub async fn capture_system_journal(
        journal: Arc<dyn Journal<SystemEvent>>,
    ) -> Result<JournalSnapshot<SystemEvent>, JournalProbeError> {
        capture_journal(journal).await
    }
}

impl<T: JournalEvent + 'static> JournalSnapshot<T> {
    /// Captured envelopes in the requested order.
    pub fn events(&self, order: JournalOrder) -> Vec<&EventEnvelope<T>> {
        match order {
            JournalOrder::Append => self.rows.iter().map(|r| &r.envelope).collect(),
            JournalOrder::Causal => {
                let mut indices: Vec<usize> = (0..self.rows.len()).collect();
                indices.sort_by_cached_key(|&idx| {
                    let row = &self.rows[idx];
                    (
                        CausalOrderingService::causal_rank(&row.envelope.vector_clock),
                        *row.envelope.event.id(),
                    )
                });
                indices
                    .into_iter()
                    .map(|idx| &self.rows[idx].envelope)
                    .collect()
            }
        }
    }

    /// Find the Nth match (1-indexed) of `shape` in the requested order.
    pub fn find(
        &self,
        order: JournalOrder,
        shape: &EventShape<T>,
        n: u64,
    ) -> Option<&EventEnvelope<T>> {
        if n < 1 {
            return None;
        }
        let mut seen: u64 = 0;
        for env in self.events(order) {
            if shape.matches(env) {
                seen += 1;
                if seen == n {
                    return Some(env);
                }
            }
        }
        None
    }

    pub fn assert_expectation(
        &self,
        expectation: &JournalExpectation<T>,
    ) -> Result<(), JournalExpectationError>
    where
        T: DirectParentId,
    {
        match expectation {
            JournalExpectation::SequenceOrder {
                order,
                mode,
                events,
            } => assert_sequence(self, *order, *mode, events),
            JournalExpectation::CausalPartialOrder { parent, events } => {
                assert_causal_partial_order(self, parent, events)
            }
            JournalExpectation::UnorderedMultiset {
                fan_out_group,
                events,
            } => assert_unordered_multiset(self, *fan_out_group, events),
        }
    }

    pub fn matches(
        &self,
        expectation: &JournalExpectation<T>,
    ) -> Result<(), JournalExpectationError>
    where
        T: DirectParentId,
    {
        self.assert_expectation(expectation)
    }
}

async fn capture_journal<T: JournalEvent + 'static>(
    journal: Arc<dyn Journal<T>>,
) -> Result<JournalSnapshot<T>, JournalProbeError> {
    let mut reader = journal
        .reader()
        .await
        .map_err(|e| JournalProbeError::JournalRead(e.to_string()))?;

    let mut rows: Vec<SnapshotRow<T>> = Vec::new();
    loop {
        match reader.next().await {
            Ok(Some(env)) => {
                let append_index = rows.len() as u64;
                rows.push(SnapshotRow {
                    append_index,
                    envelope: env,
                });
            }
            Ok(None) => return Ok(JournalSnapshot { rows }),
            Err(e) => return Err(JournalProbeError::JournalRead(e.to_string())),
        }
    }
}

fn assert_sequence<T: JournalEvent + 'static>(
    snapshot: &JournalSnapshot<T>,
    order: JournalOrder,
    mode: SequenceMatchMode,
    expected: &[EventShape<T>],
) -> Result<(), JournalExpectationError> {
    let actual = snapshot.events(order);

    let matches_at = |start: usize| -> bool {
        if start + expected.len() > actual.len() {
            return false;
        }
        for (offset, shape) in expected.iter().enumerate() {
            if !shape.matches(actual[start + offset]) {
                return false;
            }
        }
        true
    };

    let ordered_subsequence_matches = || -> bool {
        let mut cursor: usize = 0;
        for shape in expected {
            let mut found = false;
            while cursor < actual.len() {
                if shape.matches(actual[cursor]) {
                    found = true;
                    cursor += 1;
                    break;
                }
                cursor += 1;
            }
            if !found {
                return false;
            }
        }
        true
    };

    let ok = match mode {
        SequenceMatchMode::Exact => actual.len() == expected.len() && matches_at(0),
        SequenceMatchMode::Prefix => matches_at(0),
        SequenceMatchMode::ContiguousSubsequence => (0..=actual.len()).any(matches_at),
        SequenceMatchMode::OrderedSubsequence => ordered_subsequence_matches(),
    };

    if ok {
        Ok(())
    } else {
        Err(JournalExpectationError::Failed {
            message: format!(
                "sequence expectation did not match (order={order:?}, mode={mode:?}, expected_len={}, actual_len={})",
                expected.len(),
                actual.len()
            ),
        })
    }
}

fn assert_causal_partial_order<T: JournalEvent + 'static>(
    snapshot: &JournalSnapshot<T>,
    parent: &EventShape<T>,
    expected: &[EventShape<T>],
) -> Result<(), JournalExpectationError> {
    let Some(parent_env) = snapshot.find(JournalOrder::Causal, parent, 1) else {
        return Err(JournalExpectationError::Failed {
            message: format!(
                "causal partial order missing parent shape `{}`",
                parent.description()
            ),
        });
    };

    let mut used: Vec<bool> = vec![false; snapshot.rows.len()];
    for shape in expected {
        let mut matched = false;
        for (idx, env) in snapshot
            .events(JournalOrder::Causal)
            .into_iter()
            .enumerate()
        {
            if used[idx] {
                continue;
            }
            if !shape.matches(env) {
                continue;
            }
            if assert_happens_before(parent_env, env).is_ok() {
                used[idx] = true;
                matched = true;
                break;
            }
        }

        if !matched {
            return Err(JournalExpectationError::Failed {
                message: format!(
                    "causal partial order missing match for shape `{}` after parent `{}`",
                    shape.description(),
                    parent.description()
                ),
            });
        }
    }

    Ok(())
}

fn assert_unordered_multiset<T: JournalEvent + DirectParentId + 'static>(
    snapshot: &JournalSnapshot<T>,
    fan_out_group: FanOutGroup,
    expected: &[EventShape<T>],
) -> Result<(), JournalExpectationError> {
    let selector = fan_out_group.parent_selector();

    let group: Vec<&EventEnvelope<T>> = snapshot
        .rows
        .iter()
        .map(|r| &r.envelope)
        .filter(|env| match selector {
            ParentSelector::VectorClockComponent { writer_id, seq } => {
                env.vector_clock.get(&writer_id.to_string()) == seq
            }
            ParentSelector::ParentEventId(parent_id) => {
                env.event.direct_parent_id() == Some(parent_id)
            }
        })
        .collect();

    let mut used = vec![false; group.len()];
    for shape in expected {
        let mut matched = false;
        for (idx, env) in group.iter().enumerate() {
            if used[idx] {
                continue;
            }
            if shape.matches(env) {
                used[idx] = true;
                matched = true;
                break;
            }
        }
        if !matched {
            return Err(JournalExpectationError::Failed {
                message: format!(
                    "unordered multiset expectation missing match for shape `{}` (group_len={})",
                    shape.description(),
                    group.len()
                ),
            });
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::vector_clock::VectorClock;
    use obzenflow_core::event::{ChainEventFactory, CorrelationId, JournalEvent};
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::StageId;
    use std::sync::{Arc, Mutex};

    struct MemoryJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    }

    impl<T: JournalEvent> Default for MemoryJournal<T> {
        fn default() -> Self {
            Self {
                id: JournalId::new(),
                owner: None,
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    struct MemoryJournalReader<T: JournalEvent> {
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
        pos: usize,
    }

    #[async_trait::async_trait]
    impl<T> JournalReader<T> for MemoryJournalReader<T>
    where
        T: JournalEvent,
    {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let guard = self
                .events
                .lock()
                .expect("MemoryJournalReader: poisoned lock");
            if self.pos >= guard.len() {
                return Ok(None);
            }
            let envelope = guard[self.pos].clone();
            drop(guard);
            self.pos += 1;
            Ok(Some(envelope))
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }
    }

    #[async_trait::async_trait]
    impl<T> Journal<T> for MemoryJournal<T>
    where
        T: JournalEvent + 'static,
    {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            let envelope =
                EventEnvelope::new(obzenflow_core::event::JournalWriterId::from(self.id), event);
            let mut guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            guard.push(envelope.clone());
            Ok(envelope)
        }

        async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            Ok(guard.clone())
        }

        async fn read_event(
            &self,
            event_id: &obzenflow_core::event::types::EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            Ok(guard.iter().find(|e| e.event.id() == event_id).cloned())
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            Ok(Box::new(MemoryJournalReader {
                events: Arc::clone(&self.events),
                pos: position as usize,
            }))
        }

        async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
            let len = guard.len();
            let start = len.saturating_sub(count);
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[tokio::test]
    async fn snapshot_capture_is_eager_and_does_not_observe_late_appends() {
        let stage = StageId::new();
        let owner = JournalOwner::stage(stage);
        let journal = MemoryJournal::<ChainEvent> {
            owner: Some(owner),
            ..Default::default()
        };

        let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(journal);
        let writer = WriterId::from(stage);

        journal
            .append(
                ChainEventFactory::data_event(writer, "a", serde_json::json!({})),
                None,
            )
            .await
            .expect("append a");
        journal
            .append(
                ChainEventFactory::data_event(writer, "b", serde_json::json!({})),
                None,
            )
            .await
            .expect("append b");

        let snapshot = JournalSnapshot::capture_chain_journal(journal.clone())
            .await
            .expect("capture");

        journal
            .append(
                ChainEventFactory::data_event(writer, "c", serde_json::json!({})),
                None,
            )
            .await
            .expect("append c");

        assert_eq!(snapshot.events(JournalOrder::Append).len(), 2);
    }

    #[test]
    fn assert_happens_before_is_strict() {
        let a_id = EventId::new();
        let b_id = EventId::new();

        let mut a_clock = VectorClock::new();
        a_clock.clocks.insert("writer_a".to_string(), 1);

        let mut b_clock = VectorClock::new();
        b_clock.clocks.insert("writer_a".to_string(), 2);

        let a = EventEnvelope {
            journal_writer_id: obzenflow_core::JournalWriterId::from(JournalId::new()),
            vector_clock: a_clock,
            timestamp: obzenflow_core::chrono::Utc::now(),
            event: SystemEvent {
                id: a_id,
                writer_id: WriterId::from(obzenflow_core::SystemId::new()),
                event: obzenflow_core::event::SystemEventType::PipelineLifecycle(
                    obzenflow_core::event::system_event::PipelineLifecycleEvent::Starting,
                ),
                timestamp: 0,
            },
        };

        let b = EventEnvelope {
            journal_writer_id: obzenflow_core::JournalWriterId::from(JournalId::new()),
            vector_clock: b_clock,
            timestamp: obzenflow_core::chrono::Utc::now(),
            event: SystemEvent {
                id: b_id,
                writer_id: WriterId::from(obzenflow_core::SystemId::new()),
                event: obzenflow_core::event::SystemEventType::PipelineLifecycle(
                    obzenflow_core::event::system_event::PipelineLifecycleEvent::Starting,
                ),
                timestamp: 0,
            },
        };

        assert_happens_before(&a, &b).expect("a should happen before b");
        assert!(
            assert_happens_before(&a, &a).is_err(),
            "strict happened-before must reject equality"
        );
    }

    #[test]
    fn sequence_match_mode_ordered_subsequence_allows_gaps() {
        let stage = StageId::new();
        let writer = WriterId::from(stage);

        let mk = |ty: &str| EventEnvelope {
            journal_writer_id: obzenflow_core::JournalWriterId::from(JournalId::new()),
            vector_clock: VectorClock::new(),
            timestamp: obzenflow_core::chrono::Utc::now(),
            event: ChainEventFactory::data_event(writer, ty, serde_json::json!({})),
        };

        let snapshot = JournalSnapshot::<ChainEvent> {
            rows: vec![
                SnapshotRow {
                    append_index: 0,
                    envelope: mk("a"),
                },
                SnapshotRow {
                    append_index: 1,
                    envelope: mk("x"),
                },
                SnapshotRow {
                    append_index: 2,
                    envelope: mk("b"),
                },
            ],
        };

        let expected = vec![EventShape::data_type("a"), EventShape::data_type("b")];
        snapshot
            .assert_expectation(&JournalExpectation::SequenceOrder {
                order: JournalOrder::Append,
                mode: SequenceMatchMode::OrderedSubsequence,
                events: expected,
            })
            .expect("ordered subsequence should match");
    }

    #[tokio::test]
    async fn fan_out_group_parent_event_id_distinguishes_children_even_when_correlation_id_is_shared(
    ) {
        let stage = StageId::new();
        let writer = WriterId::from(stage);

        let owner = JournalOwner::stage(stage);
        let journal = MemoryJournal::<ChainEvent> {
            owner: Some(owner),
            ..Default::default()
        };
        let journal: Arc<dyn Journal<ChainEvent>> = Arc::new(journal);

        let corr = CorrelationId::new();
        let mut parent =
            ChainEventFactory::data_event(writer, "parent", serde_json::json!({ "k": "v" }));
        parent.set_single_correlation(corr, None);
        let parent_env = journal.append(parent, None).await.expect("append parent");

        let child_a = ChainEventFactory::derived_data_event(
            writer,
            &parent_env.event,
            "child.a",
            serde_json::json!({ "i": 1 }),
        );
        let child_b = ChainEventFactory::derived_data_event(
            writer,
            &parent_env.event,
            "child.b",
            serde_json::json!({ "i": 2 }),
        );

        let child_a_env = journal
            .append(child_a, Some(&parent_env))
            .await
            .expect("append child.a");
        let child_b_env = journal
            .append(child_b, Some(&parent_env))
            .await
            .expect("append child.b");

        assert_eq!(
            child_a_env.event.correlation_id(),
            child_b_env.event.correlation_id(),
            "under fan-out, multiple derived children intentionally share correlation_id"
        );
        assert_eq!(
            child_a_env.event.correlation_id(),
            Some(corr),
            "derived children should inherit parent's correlation_id"
        );

        let snapshot = JournalSnapshot::capture_chain_journal(journal.clone())
            .await
            .expect("capture");

        let group = FanOutGroup::new(ParentSelector::ParentEventId(ParentEventId::of(
            &parent_env,
        )));

        snapshot
            .assert_expectation(&JournalExpectation::UnorderedMultiset {
                fan_out_group: group,
                events: vec![
                    EventShape::data_type("child.a"),
                    EventShape::data_type("child.b"),
                ],
            })
            .expect("should match the two fan-out children by direct parent id");
    }
}
