// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core event types
//!
//! This module contains all event-related domain types.

// Core modules
pub mod chain_event;
pub mod constants;
pub mod event_envelope;
pub mod ingestion;
pub mod journal_event;
pub mod observability;
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
pub use chain_event::{
    ChainEvent, ChainEventContent, ChainEventFactory, CircuitBreakerSummaryEventParams,
    ConsumptionFinalEventParams, ConsumptionProgressEventParams, SourceContractEventParams,
};
pub use event_envelope::EventEnvelope;
pub use journal_event::JournalEvent;
pub use system_event::{
    EdgeLivenessState, MetricsCoordinationEvent, PipelineLifecycleEvent, ReplayLifecycleEvent,
    StageActivity, StageLifecycleEvent, SystemEvent, SystemEventFactory, SystemEventType,
};
pub use types::{CorrelationId, EventId, JournalWriterId, WriterId};

pub use utils::EventFilter;
