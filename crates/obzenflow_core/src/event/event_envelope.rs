
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use crate::event::{JournalWriterId, JournalEvent};
use crate::event::vector_clock::VectorClock;

/// Event envelope with vector clock for causal ordering
///
/// This wraps an event with metadata needed for distributed ordering
/// but without any storage-specific concerns
#[derive(Debug, Clone)]
pub struct EventEnvelope<T>
where
    T: JournalEvent
{
    /// Writer that created this event
    pub journal_writer_id: JournalWriterId,
    /// Vector clock for causal ordering
    pub vector_clock: VectorClock,
    /// Timestamp for wall-clock time (informational only)
    pub timestamp: DateTime<Utc>,
    /// The actual event
    pub event: T,
}

// Manual implementations to avoid serde trait bound issues
impl<T> Serialize for EventEnvelope<T>
where
    T: JournalEvent
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("EventEnvelope", 4)?;
        state.serialize_field("journal_writer_id", &self.journal_writer_id)?;
        state.serialize_field("vector_clock", &self.vector_clock)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("event", &self.event)?;
        state.end()
    }
}

impl<'de, T> Deserialize<'de> for EventEnvelope<T>
where
    T: JournalEvent
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
            event: T,
        }
        
        let data = EventEnvelopeData::<T>::deserialize(deserializer)?;
        Ok(EventEnvelope {
            journal_writer_id: data.journal_writer_id,
            vector_clock: data.vector_clock,
            timestamp: data.timestamp,
            event: data.event,
        })
    }
}

impl<T> EventEnvelope<T>
where
    T: JournalEvent
{
    pub fn new(journal_writer_id: JournalWriterId, event: T) -> Self {
        Self {
            journal_writer_id,
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
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
