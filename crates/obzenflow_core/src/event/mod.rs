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
    ConsumptionFinalEventParams, ConsumptionProgressEventParams, CorrelationContext,
    SourceContractEventParams,
};
pub use event_envelope::EventEnvelope;
pub use journal_event::JournalEvent;
pub use payloads::effect_payload::{
    CanonicalInputHash, EffectCursor, EffectDescriptor, EffectDescriptorHash, EffectFactOrigin,
    EffectFactOwner, EffectFailureCause, EffectFailureCode, EffectFailureKind, EffectFailureSource,
    EffectInputPosition, EffectLabel, EffectOrdinal, EffectOutcomeGroupId, EffectOutcomePayload,
    EffectProvenance, EffectRecord, EffectSchemaVersion, EffectStageKey, EffectType,
    OutcomeFactOrdinal, RecordedFlowId, RetryDisposition, StageLogicVersion,
};
pub use system_event::{
    EdgeLivenessState, MetricsCoordinationEvent, PipelineLifecycleEvent, ReplayLifecycleEvent,
    StageActivity, StageLifecycleEvent, SystemEvent, SystemEventFactory, SystemEventType,
};
pub use types::{CorrelationId, EventId, EventType, JournalWriterId, ReaderGeneration, WriterId};

pub use utils::EventFilter;
