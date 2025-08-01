//! Journal sink stage implementation
//!
//! Journal sinks are the standard terminal stages in a pipeline that consume events
//! and write them to external destinations (databases, files, APIs, etc.).
//!
//! Key features:
//! - Flush semantics for data durability
//! - Graceful draining to prevent data loss
//! - Automatic completion tracking

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;

// Re-export public API
pub use builder::JournalSinkBuilder;
pub use config::JournalSinkConfig;
pub use handle::{JournalSinkHandle, JournalSinkHandleExt};

// Re-export FSM types for users who need them
pub use fsm::{
    JournalSinkState,
    JournalSinkEvent,
};