//! Core type aliases used throughout the event system

// Re-export identity types from their newtype modules
pub use crate::event::identity::{CorrelationId, EventId, JournalWriterId, WriterId};

// === Pipeline types ===
// Note: StageId and SystemId are proper newtypes in the id module
pub type FlowId = String;
