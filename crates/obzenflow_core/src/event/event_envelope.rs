
use serde::{Deserialize, Serialize};
use chrono::{DateTime, Utc};
use crate::journal::writer_id::WriterId;
use super::vector_clock::VectorClock;
use super::chain_event::ChainEvent;

/// Event envelope with vector clock for causal ordering
///
/// This wraps an event with metadata needed for distributed ordering
/// but without any storage-specific concerns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    /// Writer that created this event
    pub writer_id: WriterId,
    /// Vector clock for causal ordering
    pub vector_clock: VectorClock,
    /// Timestamp for wall-clock time (informational only)
    pub timestamp: DateTime<Utc>,
    /// The actual event
    pub event: ChainEvent,
}

impl EventEnvelope {
    pub fn new(writer_id: WriterId, event: ChainEvent) -> Self {
        Self {
            writer_id,
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
            event,
        }
    }
}
