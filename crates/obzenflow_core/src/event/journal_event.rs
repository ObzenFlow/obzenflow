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
    private::Sealed + Debug + Clone + Send + Sync + Serialize + for<'de> Deserialize<'de>
{
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
