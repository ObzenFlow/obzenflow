//! Error sink stage implementation (FLOWIP-082e)
//!
//! Error sinks are special sinks that aggregate error events from all stages
//! to prevent infinite loops in cyclic topologies.
//!
//! Key features:
//! - Subscribes to all stage error journals
//! - Deduplicates error events by correlation_id/origin_id
//! - Enforces hop budget to prevent error storms
//! - Writes deduplicated errors to its own journal

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub mod supervisor;

// Re-export public API
pub use builder::ErrorSinkBuilder;
pub use config::ErrorSinkConfig;
pub use handle::{ErrorSinkHandle, ErrorSinkHandleExt};

// Re-export FSM types for users who need them
pub use fsm::{
    ErrorSinkState,
    ErrorSinkEvent,
};