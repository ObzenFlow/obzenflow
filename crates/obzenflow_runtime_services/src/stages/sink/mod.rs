//! Sink stage implementations
//!
//! Sinks are journal-based stages that process events and write delivery facts.

pub mod journal_sink;
pub mod typed;

pub use typed::{FallibleSinkTyped, SinkTyped};
