// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::vector_clock::VectorClock;
use crate::event::{JournalEvent, JournalWriterId};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Position of one logical event inside the physical atomic journal frame
/// that committed it.
///
/// The zero-based index and total size make a repeated use of one
/// deterministic group identity observable during replay. Without this
/// witness, two adjacent physical frames with the same `journal_group_id`
/// collapse into one indistinguishable list of logical events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalGroupMember {
    pub index: u32,
    pub size: u32,
}

/// Event envelope with vector clock for causal ordering
///
/// This wraps an event with metadata needed for distributed ordering
/// but without any storage-specific concerns
#[derive(Debug, Clone)]
pub struct EventEnvelope<T>
where
    T: JournalEvent,
{
    /// Writer that created this event
    pub journal_writer_id: JournalWriterId,
    /// Vector clock for causal ordering
    pub vector_clock: VectorClock,
    /// Timestamp for wall-clock time (informational only)
    pub timestamp: DateTime<Utc>,
    /// Deterministic identity of the atomic journal frame that made this
    /// record visible, when it was committed as a group.
    pub journal_group_id: Option<String>,
    /// Physical-frame membership witness for an atomic group.
    pub journal_group_member: Option<JournalGroupMember>,
    /// The actual event
    pub event: T,
}

// Manual implementations to avoid serde trait bound issues
impl<T> Serialize for EventEnvelope<T>
where
    T: JournalEvent,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("EventEnvelope", 6)?;
        state.serialize_field("journal_writer_id", &self.journal_writer_id)?;
        state.serialize_field("vector_clock", &self.vector_clock)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("journal_group_id", &self.journal_group_id)?;
        state.serialize_field("journal_group_member", &self.journal_group_member)?;
        state.serialize_field("event", &self.event)?;
        state.end()
    }
}

impl<'de, T> Deserialize<'de> for EventEnvelope<T>
where
    T: JournalEvent,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct EventEnvelopeData<T> {
            journal_writer_id: JournalWriterId,
            vector_clock: VectorClock,
            timestamp: DateTime<Utc>,
            #[serde(default)]
            journal_group_id: Option<String>,
            #[serde(default)]
            journal_group_member: Option<JournalGroupMember>,
            event: T,
        }

        let data = EventEnvelopeData::<T>::deserialize(deserializer)?;
        Ok(EventEnvelope {
            journal_writer_id: data.journal_writer_id,
            vector_clock: data.vector_clock,
            timestamp: data.timestamp,
            journal_group_id: data.journal_group_id,
            journal_group_member: data.journal_group_member,
            event: data.event,
        })
    }
}

impl<T> EventEnvelope<T>
where
    T: JournalEvent,
{
    pub fn new(journal_writer_id: JournalWriterId, event: T) -> Self {
        Self {
            journal_writer_id,
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
            journal_group_id: None,
            journal_group_member: None,
            event,
        }
    }
}

// Type aliases for convenience
use super::chain_event::ChainEvent;
use super::system_event::SystemEvent;

/// EventEnvelope containing a ChainEvent (for stage journals)
pub type ChainEventEnvelope = EventEnvelope<ChainEvent>;

/// EventEnvelope containing a SystemEvent (for control journal)
pub type SystemEventEnvelope = EventEnvelope<SystemEvent>;
