// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::journal_error::JournalError;
use super::journal_owner::JournalOwner;
use super::journal_reader::JournalReader;
use crate::event::event_envelope::EventEnvelope;
use crate::event::types::EventId;
use crate::event::vector_clock::CausalOrderingService;
use crate::event::JournalEvent;
use crate::id::JournalId;

use async_trait::async_trait;

/// Where a journal stores its data.
///
/// This is used for runtime resource preflight checks (e.g., estimating
/// file-descriptor usage for disk-backed journals).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JournalStorageKind {
    /// On-disk, file-backed journaling (consumes OS file descriptors).
    Disk,
    /// In-memory journaling (does not consume per-journal file descriptors).
    Memory,
    /// Unknown / user-provided journal implementation.
    Unknown,
}

/// Core journal trait - defines what a journal must do
///
/// Infrastructure will implement this trait with actual storage
/// Generic over T which is the event type (ChainEvent or SystemEvent)
#[async_trait]
pub trait Journal<T>: Send + Sync
where
    T: JournalEvent,
{
    /// Describe the underlying storage kind for this journal.
    ///
    /// Default is `Unknown` to avoid forcing custom journal implementations to
    /// provide a storage classification.
    fn storage_kind(&self) -> JournalStorageKind {
        JournalStorageKind::Unknown
    }

    /// Get the ID of this journal
    fn id(&self) -> &JournalId;

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
        event: T,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError>;

    /// Read every event in raw append/storage order, without causal sorting.
    ///
    /// The single raw-enumeration primitive the causal reads derive from. Disk
    /// reads through its full-scan framed reader; memory clones its in-memory
    /// vector.
    async fn read_all_unordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError>;

    /// Read all events and return them in causal order.
    ///
    /// If A happened-before B then A appears before B; concurrent events are
    /// broken by `EventId` ordering, not wall-clock time. Derived from
    /// `read_all_unordered`.
    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        CausalOrderingService::order_envelopes_by_event_id(self.read_all_unordered().await?)
    }

    /// Read events causally after the given event, in causal order. Empty if the
    /// event is absent. Derived from `read_causally_ordered`.
    async fn read_causally_after(
        &self,
        after_event_id: &EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let all = self.read_causally_ordered().await?;
        Ok(
            match all.iter().position(|e| e.event.id() == after_event_id) {
                Some(pos) => all.into_iter().skip(pos + 1).collect(),
                None => Vec::new(),
            },
        )
    }

    /// Read a specific event by ID
    ///
    /// Returns None if the event doesn't exist
    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError>;

    /// Create a reader that starts from the beginning. Multiple readers can be
    /// created for the same journal. Derived from `reader_from(0)`.
    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        self.reader_from(0).await
    }

    /// Create a reader that starts from a specific position.
    ///
    /// The position is the portable append index (`0, 1, 2, ...`), the count of
    /// committed records before the reader's first event. A reader created at
    /// position N is equivalent to a reader advanced past N records. Useful for
    /// resuming from a checkpoint.
    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError>;

    /// Read the last N events from the journal by scanning backwards from EOF.
    ///
    /// This is useful for efficiently getting recent events without loading the entire journal.
    /// Events are returned in reverse order (most recent first).
    ///
    /// Returns an empty vec if the journal is empty.
    /// This method should be O(n) where n is the requested count,
    /// not O(total_events) in the journal.
    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError>;
}
