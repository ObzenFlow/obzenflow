//! Sink stage implementation
//!
//! Sinks are the terminal stages in a pipeline that consume events
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

// Public API - only expose builder, handle, and essential types
pub use builder::SinkBuilder;
pub use config::SinkConfig;
pub use handle::{SinkHandle, SinkHandleExt};
pub use fsm::{SinkState, SinkEvent};
pub use crate::stages::common::handlers::SinkHandler;

// Note: SinkSupervisor is NOT exported! It's an implementation detail.