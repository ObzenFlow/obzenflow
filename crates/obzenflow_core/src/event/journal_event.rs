// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Base trait for events that can be journaled
//!
//! This trait ensures type safety by restricting what can be written to journals

use crate::event::identity::WriterId;
use crate::event::types::{AdmissionSeq, EventId};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Whether an event participates in flow-global admission ordering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalAdmissionRole {
    Flow,
    ObserverEvidence,
}

/// Independent vector-clock state selected from durable event content.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JournalCausalLane {
    Flow(WriterId),
    ObserverEvidence(WriterId),
}

impl JournalCausalLane {
    pub fn clock_key(self) -> String {
        match self {
            Self::Flow(writer) => writer.to_string(),
            Self::ObserverEvidence(writer) => format!("observer:{writer}"),
        }
    }
}

/// Private module to seal the trait
mod private {
    pub trait Sealed {}
}

/// Trait that all journal-writable events must implement
///
/// This is a sealed trait - only ChainEvent and SystemEvent can implement it.
/// The trait is sealed using the private::Sealed supertrait pattern.
pub trait JournalEvent:
    private::Sealed + Debug + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>
{
    /// Get the event's unique ID
    fn id(&self) -> &EventId;

    /// Get the writer ID that created this event
    fn writer_id(&self) -> &WriterId;

    /// Get a human-readable event type for logging/debugging
    fn event_type_name(&self) -> &str;

    /// Durable-content classification for admission ordering.
    fn admission_role(&self) -> JournalAdmissionRole;

    /// Durable-content classification for vector-clock state.
    fn causal_lane(&self) -> JournalCausalLane;

    /// Flow-global admission order (FLOWIP-120n F18). The default answers for
    /// event types that never enter a merge (system rows).
    fn admission_seq(&self) -> Option<AdmissionSeq> {
        None
    }

    /// Stamp the admission order at the journal append (FLOWIP-120n F18).
    /// The default is a no-op for event types that carry no sequence.
    fn set_admission_seq(&mut self, _seq: AdmissionSeq) {}

    /// Remove an inapplicable sequence from non-admitted evidence.
    fn clear_admission_seq(&mut self) {}
}

// Export Sealed trait so chain_event.rs and system_event.rs can implement it
#[doc(hidden)]
pub use private::Sealed;

#[cfg(test)]
mod tests {

    // This test verifies that the trait is properly sealed
    // by attempting to implement it for a custom type.
    // This should fail to compile if the trait is sealed correctly.

    /* Uncomment to verify sealing works - this should NOT compile:

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct MyCustomEvent {
        id: EventId,
    }

    // This should fail because MyCustomEvent doesn't implement Sealed
    impl JournalEvent for MyCustomEvent {
        fn id(&self) -> &EventId {
            &self.id
        }

        fn event_type_name(&self) -> &str {
            "custom"
        }
    }
    */
}
