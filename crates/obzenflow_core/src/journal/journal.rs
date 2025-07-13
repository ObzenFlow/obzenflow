use crate::event::chain_event::ChainEvent;
use crate::event::event_envelope::EventEnvelope;
use crate::event::event_id::EventId;
use super::writer_id::WriterId;
use super::journal_error::JournalError;
use super::journal_owner::JournalOwner;
use super::journal_reader::JournalReader;

use async_trait::async_trait;

/// Core journal trait - defines what a journal must do
///
/// Infrastructure will implement this trait with actual storage
#[async_trait]
pub trait Journal: Send + Sync {
    /// Get the owner of this journal (if any)
    fn owner(&self) -> Option<&JournalOwner>;
    
    /// Append an event to the journal
    ///
    /// The implementation MUST:
    /// 1. Generate appropriate vector clock based on writer and parent
    /// 2. Ensure atomic append operation
    /// 3. Return the complete EventEnvelope with causal information
    async fn append(
        &self,
        writer_id: &WriterId,
        event: ChainEvent,
        parent: Option<&EventEnvelope>
    ) -> Result<EventEnvelope, JournalError>;

    /// Read all events and return them in causal order
    ///
    /// Events MUST be ordered such that if A happened-before B,
    /// then A appears before B in the result
    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope>, JournalError>;

    /// Read events causally after the given event
    ///
    /// Returns only events that causally follow the given event,
    /// in causal order
    async fn read_causally_after(&self, after_event_id: &EventId) -> Result<Vec<EventEnvelope>, JournalError>;

    /// Read a specific event by ID
    ///
    /// Returns None if the event doesn't exist
    async fn read_event(&self, event_id: &EventId) -> Result<Option<EventEnvelope>, JournalError>;
    
    /// Create a reader that starts from the beginning
    ///
    /// This reader maintains its own position and file handle for efficient
    /// sequential reading. Multiple readers can be created for the same journal.
    async fn reader(&self) -> Result<Box<dyn JournalReader>, JournalError>;
    
    /// Create a reader that starts from a specific position
    ///
    /// The position is journal-specific (e.g., line number for disk, index for memory).
    /// This is useful for resuming from a checkpoint.
    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader>, JournalError>;
}
