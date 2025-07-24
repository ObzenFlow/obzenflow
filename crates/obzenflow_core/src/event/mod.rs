//! Core event types
//!
//! This module contains all event-related domain types.

// Core modules
pub mod types;
pub mod journal_event;
pub mod chain_event;
pub mod system_event;
pub mod event_envelope;
pub mod constants;
pub mod vector_clock;

// Subdirectory modules
pub mod identity;
pub mod context;
pub mod payloads;
pub mod utils;
pub mod status;

// Re-export main types at root level for convenience
pub use types::{EventId, WriterId, JournalWriterId, CorrelationId};
pub use journal_event::JournalEvent;
pub use chain_event::{ChainEvent, ChainEventContent, ChainEventFactory};
pub use system_event::{SystemEvent, SystemEventType, SystemEventFactory, StageLifecycleEvent, PipelineLifecycleEvent, MetricsCoordinationEvent};
pub use event_envelope::EventEnvelope;

pub use utils::EventFilter;