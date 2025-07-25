//! Core domain layer - pure abstractions with zero dependencies
//!
//! This is the innermost layer of the onion architecture.
//! Everything here is pure types and traits with no knowledge
//! of infrastructure, I/O, or external systems.

pub mod error;
pub mod event;
pub mod id;
pub mod journal;
pub mod metrics;
pub mod time;

// Re-export key types for convenience
pub use error::Result;
pub use event::EventId;
pub use event::{WriterId, JournalWriterId};
pub use event::chain_event::ChainEvent;
pub use event::event_envelope::EventEnvelope;
pub use journal::journal::Journal;
pub use journal::journal_error::JournalError;
pub use journal::journal_owner::JournalOwner;
pub use event::context::runtime_context;

// Re-export typed IDs
pub use id::{StageId, FlowId, JournalId, SystemId};

// Re-export Ulid for convenience since it's used in many IDs
pub use ulid::Ulid;