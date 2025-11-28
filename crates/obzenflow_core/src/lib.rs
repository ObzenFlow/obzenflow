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
pub mod contracts;
pub mod time;
pub mod web;

// Re-export key types for convenience
pub use error::Result;
pub use event::chain_event::ChainEvent;
pub use event::context::runtime_context;
pub use event::event_envelope::EventEnvelope;
pub use event::EventId;
pub use event::{JournalWriterId, WriterId};
pub use journal::journal::Journal;
pub use journal::journal_error::JournalError;
pub use journal::journal_owner::JournalOwner;

// Re-export schema types (FLOWIP-082a)
pub use event::schema::TypedPayload;

// Re-export typed IDs
pub use id::{FlowId, JournalId, StageId, SystemId};

// Re-export Ulid for convenience since it's used in many IDs
pub use ulid::Ulid;

// Re-export core contract types (FLOWIP-090c)
pub use contracts::{
    Contract, ContractContext, ContractEvidence, ContractReadContext, ContractResult,
    ContractState, ContractViolation, ContractWriteContext, HashMismatch, TransportContract,
    ViolationCause,
};
