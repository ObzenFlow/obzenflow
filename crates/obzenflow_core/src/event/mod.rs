// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Core event types
//!
//! This module contains all event-related domain types.

// Core modules
pub mod causal;
pub mod chain_event;
pub mod constants;
pub mod envelope;
pub mod journal_event;
pub mod journal_record;
pub mod observability;
pub mod supervisor_record;
pub mod system_event;
pub use supervisor_record::SupervisorRecord;
pub mod types;
pub mod vector_clock;

// Subdirectory modules
pub mod context;
pub mod identity;
pub mod payloads;
pub mod provenance;
mod record_serde;
pub mod schema;
pub mod status;
pub mod utils;

// Re-export main types at root level for convenience
pub use causal::{
    CausalCommit, CausalCoordinate, CausalError, CausalFrontier, CausalWitnesses,
    CommittedCausalRef,
};
pub use chain_event::{
    ChainEvent, ChainEventFactory, ChainPayload, CircuitBreakerAttemptSettledEventParams,
    CircuitBreakerOpenedEventParams, CircuitBreakerRecoveryCompletedEventParams,
    CircuitBreakerSummaryEventParams, ConsumptionFinalEventParams, ConsumptionProgressEventParams,
    CorrelationContext, SourceContractEventParams,
};
pub use envelope::EventEnvelope;
pub use journal_event::JournalEvent;
pub use journal_record::JournalRecord;
pub use observability::{EdgeLivenessState, StageActivity};
pub use payloads::effect_payload::{
    effect_escape_controls_group_id, CanonicalInputHash, EffectAttemptOrdinal,
    EffectAttemptStarted, EffectCursor, EffectDescriptor, EffectDescriptorHash, EffectFactOrigin,
    EffectFactOwner, EffectFailureCause, EffectFailureCode, EffectFailureDetail, EffectFailureKind,
    EffectFailureSource, EffectInputPosition, EffectLabel, EffectOrdinal, EffectOutcomeGroupId,
    EffectOutcomePayload, EffectProvenance, EffectRecord, EffectRecoveryAbandoned,
    EffectSchemaVersion, EffectStageKey, EffectType, OutcomeFactOrdinal, RecordedFlowId,
    RetryDisposition, StageLogicVersion,
};
pub use payloads::sink_operation_payload::{
    SinkDestinationErrorCode, SinkDestinationErrorCodeError, SinkOperationFailed,
    SinkOperationPhase, SinkWritePhase, MAX_SINK_DESTINATION_ERROR_NAMESPACE_BYTES,
    MAX_SINK_DESTINATION_ERROR_VALUE_BYTES,
};
pub use payloads::stage_fatal_payload::{
    StageFatalCode, StageFatalReason, StageFatalRecorded, StageFatalSeverity,
};
pub use payloads::system_payload::{
    CommandDiscardDisposition, MetricsCoordinationEvent, PipelineCancellationCause,
    PipelineLifecycleEvent, PipelineStopAdmission, ReplayLifecycleEvent, StageLifecycleEvent,
    SystemPayload,
};
pub use system_event::{SystemEvent, SystemEventFactory};
pub use types::{
    AdmissionSeq, CorrelationId, EventId, EventType, JournalWriterId, ReaderGeneration, WriterId,
};

pub use utils::EventFilter;

pub use payloads::chain_payload::EventKind;
