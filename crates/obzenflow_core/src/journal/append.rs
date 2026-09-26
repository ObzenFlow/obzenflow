// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Inputs shared by individual and atomic-group appends.

use crate::event::observability::ObservabilityContext;
use crate::event::{CausalFrontier, JournalEvent};
use std::panic::{catch_unwind, AssertUnwindSafe};

/// A selected member may construct its complete optional attachment lazily.
/// The event is read-only: capture cannot change its payload or provenance.
/// The member index allows an atomic group to project its committed prefix.
pub type ObservationCapture<T> = Box<dyn FnMut(usize, &T) -> Option<ObservabilityContext> + Send>;

pub enum JournalCapture<T: JournalEvent> {
    Live(Option<ObservationCapture<T>>),
    /// Republish recorded attachments without sampling or changing their stamps.
    Historical,
}

impl<T: JournalEvent> JournalCapture<T> {
    /// Prepare an attachment after the journal admits it. An absent callback,
    /// historical append, or panicking capture preserves the authored event.
    /// Only the optional packet can be replaced; facts remain untouched.
    pub fn prepare(&mut self, member: usize, event: T) -> T {
        if let Self::Live(Some(capture)) = self {
            if let Ok(packet) = catch_unwind(AssertUnwindSafe(|| capture(member, &event))) {
                let (mut envelope, payload) = event.into_parts();
                envelope.observability = packet;
                return T::from_parts(envelope, payload);
            }
        }
        event
    }
}

impl<T: JournalEvent> Default for JournalCapture<T> {
    fn default() -> Self {
        Self::Live(None)
    }
}

/// Owned, payload-independent causal evidence and preparation for an append. The journal applies its policy
/// to the complete attachment, including any inherited observations.
pub struct AppendOptions<T: JournalEvent> {
    pub frontier: CausalFrontier,
    pub capture: JournalCapture<T>,
}

impl<T: JournalEvent> AppendOptions<T> {
    pub fn new(frontier: CausalFrontier) -> Self {
        Self {
            frontier,
            capture: JournalCapture::default(),
        }
    }

    /// Convenience admission of one committed receipt/read. This is independent
    /// of the payload family being appended; bare event IDs cannot be admitted.
    pub fn from_record<P: crate::event::payloads::JournalPayload>(
        record: Option<&crate::event::JournalRecord<P>>,
    ) -> Result<Self, super::JournalError> {
        Ok(Self::new(match record {
            Some(record) => CausalFrontier::from_record(record)?,
            None => CausalFrontier::default(),
        }))
    }

    pub fn with_capture(mut self, capture: JournalCapture<T>) -> Self {
        self.capture = capture;
        self
    }
}

impl<T: JournalEvent> Default for AppendOptions<T> {
    fn default() -> Self {
        Self::new(CausalFrontier::default())
    }
}
