//! Sink stage implementations
//!
//! Sinks are divided into types:
//! - Journal: Standard sinks that process events and write delivery facts
//! - Error: Special sink that aggregates error events from all stages (FLOWIP-082e)

pub mod journal_sink;
pub mod error_sink;