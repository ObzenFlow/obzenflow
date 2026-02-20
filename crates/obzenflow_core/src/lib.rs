// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core domain layer - pure abstractions with minimal dependencies
//!
//! This is the innermost layer of the onion architecture.
//! Everything here is pure types and traits with no knowledge
//! of infrastructure, I/O, or external systems.

pub mod ai;
pub mod build_info;
pub mod contracts;
pub mod control_middleware;
pub mod error;
pub mod event;
pub mod http_client;
pub mod id;
pub mod journal;
pub mod metrics;
pub mod time;
pub mod web;

// Re-export key types for convenience
pub use error::Result;
pub use event::chain_event::ChainEvent;
pub use event::context::runtime_context;
pub use event::event_envelope::EventEnvelope;
pub use event::EventId;
pub use event::{JournalWriterId, WriterId};
pub use journal::journal_error::JournalError;
pub use journal::journal_owner::JournalOwner;
pub use journal::Journal;

// Re-export schema types (FLOWIP-082a)
pub use event::schema::TypedPayload;

// Re-export typed IDs
pub use id::{CycleDepth, FlowId, JournalId, SccId, StageId, SystemId};

// Re-export Ulid for convenience since it's used in many IDs
pub use ulid::Ulid;

// Re-export chrono so macro expansions can reference timestamps without
// requiring downstream crates to depend on chrono directly (FLOWIP-095a).
pub use chrono;

// Re-export core contract types (FLOWIP-090c)
pub use contracts::{
    Contract, ContractContext, ContractEvidence, ContractReadContext, ContractResult,
    ContractState, ContractViolation, ContractWriteContext, HashMismatch, SourceContract,
    TransportContract, ViolationCause,
};

// Re-export control middleware contracts so adapters/runtime can depend on them
// without reaching into internal modules.
pub use control_middleware::{
    CircuitBreakerContractInfo, CircuitBreakerContractMode, CircuitBreakerMetrics,
    ControlMiddlewareProvider, NoControlMiddleware, RateLimiterMetrics,
};
