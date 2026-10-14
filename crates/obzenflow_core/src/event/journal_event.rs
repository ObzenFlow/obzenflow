// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Base trait for events that can be journaled
//!
//! This trait ensures type safety by restricting what can be written to journals

use crate::event::identity::WriterId;
use crate::event::types::EventId;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Private module to seal the trait
mod private {
    pub trait Sealed {}
}

/// Trait that all journal-writable events must implement
///
/// This is a sealed trait - only ChainEvent and SystemEvent can implement it.
/// The trait is sealed using the private::Sealed supertrait pattern.
pub trait JournalEvent:
    private::Sealed + 'static + Debug + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>
{
    type Payload: super::payloads::JournalPayload<Event = Self>;

    fn into_parts(
        self,
    ) -> (
        super::envelope::AuthoredEnvelope<
            <Self::Payload as super::payloads::JournalPayload>::Provenance,
        >,
        Self::Payload,
    );
    fn from_parts(
        envelope: super::envelope::AuthoredEnvelope<
            <Self::Payload as super::payloads::JournalPayload>::Provenance,
        >,
        payload: Self::Payload,
    ) -> Self;

    /// Get the event's unique ID
    fn id(&self) -> &EventId;

    /// Get the writer ID that created this event
    fn writer_id(&self) -> &WriterId;

    /// Get a human-readable event type for logging/debugging
    fn event_type_name(&self) -> &str;
}

// Export Sealed trait so chain_event.rs and system_event.rs can implement it
#[doc(hidden)]
pub use private::Sealed;
