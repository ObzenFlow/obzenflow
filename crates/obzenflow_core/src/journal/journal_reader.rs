// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal reader trait for efficient sequential reading
//!
//! This trait provides cursor-based reading that maintains position
//! and keeps file handles open for optimal performance.

use super::journal_error::JournalError;
use crate::event::event_envelope::EventEnvelope;
use crate::event::JournalEvent;
use async_trait::async_trait;

/// A reader that maintains position for efficient sequential journal reading
///
/// This trait is designed to solve the O(n²) performance problem with large journals
/// by keeping file handles open and tracking position, similar to database cursors.
#[async_trait]
pub trait JournalReader<T>: Send + Sync
where
    T: JournalEvent,
{
    /// Read the next event from the current position (append order).
    ///
    /// This is an append-order cursor, not a causal-order iterator. Callers that need
    /// deterministic causal ordering should use `Journal::read_causally_ordered()` /
    /// `Journal::read_causally_after(...)` instead of `JournalReader::next()`.
    ///
    /// Returns None if no more events are available (EOF).
    /// This method should be efficient - O(1) regardless of journal size.
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError>;

    /// Skip forward by n events without reading them
    ///
    /// Returns the number of events actually skipped (may be less than n if EOF reached).
    /// This is useful for resuming from a checkpoint.
    async fn skip(&mut self, n: u64) -> Result<u64, JournalError>;

    /// Get the current position in the journal
    ///
    /// The position is journal-specific (e.g., line number for disk, index for memory).
    /// This can be used for checkpointing and resuming.
    fn position(&self) -> u64;

    /// Check if we've reached the end of the journal
    ///
    /// This is a hint - `next()` may still return None even if this returns false
    /// (e.g., if new events are being written concurrently).
    fn is_at_end(&self) -> bool {
        // Default implementation - can be overridden for efficiency
        false
    }
}
