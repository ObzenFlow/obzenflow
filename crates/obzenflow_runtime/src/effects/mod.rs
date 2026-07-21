// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe user effects.

use async_trait::async_trait;
use obzenflow_core::event::context::FlowContext;
pub use obzenflow_core::event::payloads::effect_payload::{
    effect_outcome_group_id, framework_effect_event_type, is_framework_effect_event_type,
    CanonicalInputHash, EffectCursor, EffectDescriptor, EffectDescriptorHash, EffectFactOrigin,
    EffectFactOwner, EffectFailureCause, EffectFailureCode, EffectFailureKind, EffectFailureSource,
    EffectInputPosition, EffectLabel, EffectOrdinal, EffectOutcomeGroupId, EffectOutcomePayload,
    EffectProvenance, EffectRecord, EffectSchemaVersion, EffectStageKey, EffectType,
    OutcomeFactCount, OutcomeFactOrdinal, RecordedFlowId, RetryDisposition, StageLogicVersion,
    CAPTURE_EVENT_TYPE, EFFECT_RECORD_EVENT_TYPE,
};
// `EffectOutcomeFacts` re-exports both the trait and its derive (FLOWIP-120m).
pub use obzenflow_core::event::schema::{
    EffectOutcomeFacts, TypedFact, TypedFactSet, TypedFactSetError, TypedFactType, TypedPayload,
};
use obzenflow_core::event::{ChainEventContent, ChainEventFactory, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventEnvelope, EventId, FlowId, StageId, WriterId};
use ring::digest::{digest, SHA256};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

use crate::backpressure::BackpressureWriter;
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::StageInputPosition;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::replay::{ReplayArchive, ReplayError};
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::output_committer::{CommitOptions, OutputCommitter};

mod boundary;
mod commit;
mod completion;
mod context;
mod declaration;
mod effect_set;
mod error;
mod history;
mod identity;
mod ports;
mod replay;
mod runtime;
mod typed;

#[cfg(test)]
mod tests;

pub(crate) use boundary::SingleUseEffectBoundaryOutcome;
pub use boundary::{
    EffectAbortReason, EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectIdentity,
    PhysicalCallObservation, PhysicalCallOutcome, PhysicalCallReceipt,
    PreparedRepeatableEffectCall, PreparedSingleUseEffectCall, RepeatableEffectOperation,
    SingleUseEffectBoundaryReport, SingleUseEffectExecution, SingleUseEffectOperation,
};
pub use commit::EffectCommitHandle;
pub use completion::StageCompletion;
pub use context::{
    EffectContext, EffectInvocationContext, SynthesizedOutcomeKind, SynthesizedOutcomeRegistration,
};
// FLOWIP-120r: `EffectRuntimeMode` survives only as a test parameterization aid.
#[cfg(test)]
pub(crate) use context::EffectRuntimeMode;
pub use declaration::{
    Effect, EffectDeclaration, EffectSafety, IdempotencyKey, IdempotencyKeyPolicy,
    SinkDeliverySafety, TransactionalEffectPort,
};
// FLOWIP-120z effect capability sets; the list traits are doc(hidden)
// plumbing that must stay reachable for the public `DeclaredEffectSet` impl.
pub use effect_set::{
    assert_distinct_effect_set, DeclaredEffectSet, EffectList, EffectSet, EffectTypeDisjoint,
};
pub use error::EffectError;
pub use history::{EffectHistory, EffectHistoryReader, EffectHistoryStore};
pub use identity::{
    deterministic_effect_record_event_id, deterministic_effect_record_event_time,
    deterministic_event_id, deterministic_event_time, deterministic_typed_output_event,
    EffectOutputOrdinal,
};
pub use ports::{EffectPortKey, EffectPortRegistry, EffectPortRequirement};
pub(crate) use runtime::EffectsCore;
// FLOWIP-120z/B9: the proof facades are doc-hidden public bounds so rustc can
// report failures in handler vocabulary without weakening the underlying set
// membership and containment proofs.
pub use typed::Effects;
#[doc(hidden)]
pub use typed::{AllowedEffectsAllowEffect, EffectOutcomeFitsOutput, OutputAllowsFact};

use commit::{
    append_domain_effect_success_facts, append_effect_record, CommittedEffectOutcome,
    EffectCommitHandleParams,
};
use identity::{descriptor_for_effect, descriptor_hash, hash_json_value};
use replay::{
    decode_effect_outcome, decode_effect_outcome_group, effect_fact_set_error,
    effect_record_from_event, effect_record_group_materialization, is_routable_output_fact,
    recorded_failure_from_outcome, validate_effect_outcome_group, EffectRecordMaterialization,
};
