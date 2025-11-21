//! Core event types
//!
//! This module contains all event-related domain types.

// Core modules
pub mod chain_event;
pub mod constants;
pub mod event_envelope;
pub mod journal_event;
pub mod system_event;
pub mod types;
pub mod vector_clock;

// Subdirectory modules
pub mod context;
pub mod identity;
pub mod payloads;
pub mod schema;
pub mod status;
pub mod utils;

// Re-export main types at root level for convenience
pub use chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
pub use event_envelope::EventEnvelope;
pub use journal_event::JournalEvent;
pub use system_event::{
    MetricsCoordinationEvent, PipelineLifecycleEvent, StageLifecycleEvent, SystemEvent,
    SystemEventFactory, SystemEventType,
};
pub use types::{CorrelationId, EventId, JournalWriterId, WriterId};

pub use utils::EventFilter;
