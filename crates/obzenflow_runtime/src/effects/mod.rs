// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe user effects.

use async_trait::async_trait;
pub use obzenflow_core::event::payloads::effect_payload::{
    effect_escape_controls_group_id, effect_outcome_group_id, is_framework_effect_event_type,
    CanonicalInputHash, EffectAttemptOrdinal, EffectAttemptStarted, EffectCursor, EffectDescriptor,
    EffectDescriptorHash, EffectFactOrigin, EffectFactOwner, EffectFailureCause, EffectFailureCode,
    EffectFailureDetail, EffectFailureKind, EffectFailureSource, EffectInputPosition, EffectLabel,
    EffectObservation, EffectOrdinal, EffectOutcomeGroupId, EffectOutcomePayload, EffectPortReturn,
    EffectProvenance, EffectRecord, EffectRecoveryAbandoned, EffectSchemaVersion, EffectStageKey,
    EffectType, EffectUnresolvedReason, ExternalCompletion, OutcomeFactCount, OutcomeFactOrdinal,
    RecordedFlowId, RetryDisposition, StageLogicVersion, CAPTURE_EVENT_TYPE,
    EFFECT_ATTEMPT_STARTED_EVENT_TYPE, EFFECT_EXECUTION_SUCCEEDED_EVENT_TYPE,
    EFFECT_RECOVERY_ABANDONED_EVENT_TYPE,
};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::schema::EffectOutcomeFacts;
pub use obzenflow_core::event::schema::{
    TypedFact, TypedFactSet, TypedFactSetError, TypedFactType, TypedPayload,
};
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, EventId, FlowId, StageId, WriterId};
use ring::digest::{digest, SHA256};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use thiserror::Error;

use crate::backpressure::BackpressureWriter;
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::StageInputPosition;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::heartbeat::HeartbeatState;
use crate::stages::common::supervision::output_committer::{
    AtomicCommitEntry, CommitOptions, OutputCommitter, StageAppendIntent,
};
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};

mod actions;
mod binding;
mod boundary;
mod call;
mod commit;
mod completion;
mod context;
mod current_run;
mod declaration;
mod effect_set;
mod error;
mod fault;
mod history;
mod identity;
mod ports;
mod replay;
mod runtime;
mod system;
mod typed;

#[cfg(test)]
mod tests;

pub(crate) use actions::{
    ActionCheck, ActionDeparture, ActionRow, RecordedAction, RecordedActions,
};
pub use binding::{BindingIdentifierError, EffectBinding};
#[doc(hidden)]
pub use binding::{
    BoundEffect, Configured, ConfiguredEffect, EffectBindingEvidence, EffectBindingMode,
    EffectBindingUse, EffectInvocationBinding, EffectPortSlot, EffectPortSlotLabel,
    EffectPortSlotSet, LogicalEffectBindingName, Named, NamedEffect, NoPortMetadata,
};
pub(crate) use boundary::SingleUseEffectBoundaryOutcome;
pub use boundary::{
    AffineEffectBoundaryReport, AffineEffectExecution, AffineEffectOperation, EffectAbortReason,
    EffectBoundary, EffectBoundaryOutcome, EffectBoundaryReport, EffectIdentity,
    PhysicalCallObservation, PhysicalCallOutcome, PhysicalCallReceipt, PreparedAffineEffectCall,
    PreparedRepeatableEffectCall, PreparedSingleUseEffectCall, RepeatableEffectOperation,
    SingleUseEffectBoundaryReport, SingleUseEffectExecution, SingleUseEffectOperation,
};
#[doc(hidden)]
pub use call::KeyRule;
pub use call::{
    AtLeastOnce, Call, CallRequest, CallSafety, Idempotent, Keyed, RequiresKey, Transactional,
};
pub use commit::EffectCommitHandle;
pub use completion::StageCompletion;
pub use context::{EffectContext, EffectInvocationContext, EffectPortMetadataContext};
pub(crate) use current_run::CurrentRunFailure;
pub use current_run::CurrentRunHistory;
pub use obzenflow_core::SystemBindingEvidence;
pub use obzenflow_core_derive::{port, EffectHandler};
#[doc(hidden)]
pub use system::{
    assert_handler_performs_no_ai_effect, assert_port_answer,
    assert_stateful_handler_performs_no_ai_effect, effectful_stateful_effects,
    effectful_transform_effects, AcknowledgedSafety, AnswersEffect, DeclareEffects,
    EffectAcknowledgement, EverySystemAnswers, FindSystem, HandlerPerformableEffects, JoinMembers,
    MarkSystemUsed, NarrowedSystem, PortAnswer, PortEffects, PortImpl, RowAcknowledgedEffect,
    ServesPort, StageEffectsError, SystemAfter, SystemAnswersAnEffect, SystemEffects, SystemFound,
    SystemList, SystemMark, SystemServesRequest, SystemsEffects, Unused, Used, WholeSystem,
};
pub use system::{EffectHandler, StageSystem, System, SystemError};
// FLOWIP-120r: `EffectRuntimeMode` survives only as a test parameterization aid.
#[doc(hidden)]
pub use async_trait::async_trait as __async_trait;
#[cfg(test)]
pub(crate) use context::EffectRuntimeMode;
#[doc(hidden)]
pub use declaration::{DomainFacts, Effect, EffectOutcomeSemantics, RecordedReply};
pub use declaration::{
    EffectDeclaration, EffectOutcomeKind, EffectSafety, IdempotencyKey, IdempotencyKeyPolicy,
    PreparedEffectSuccess, SinkRedeliverySafety,
};
pub use effect_set::{
    assert_distinct_effect_set, DeclaredEffectSet, EffectList, EffectName, EffectSet,
    EffectTypeDisjoint, RequestList,
};
pub use error::{DescriptorComponent, DescriptorDrift, DriftBinding, EffectError};
pub use fault::{BindingAuthorityFault, BindingMismatchKind};
#[cfg(test)]
pub(crate) use history::current_cursor_history;
pub(crate) use history::{
    merge_cursor_histories, EffectCursorCoordinator, EffectCursorHistory, EffectHistorySelection,
};
pub use history::{EffectHistory, EffectHistoryReader, EffectHistoryScan, EffectHistoryStore};
pub use identity::{
    deterministic_effect_record_event_id, deterministic_effect_record_event_time,
    deterministic_event_id, deterministic_event_time, deterministic_payload_event_id,
    deterministic_typed_output_event, EffectOutputOrdinal,
};
pub use ports::{
    EffectBindingBuildError, EffectPortRegistry, EffectPortResolutionError, EffectPortResolver,
    EffectRegistrationCollectionError,
};
#[doc(hidden)]
pub use ports::{EffectPortResolverWithMetadata, EffectRegistrationBuilder, ResolvedEffectPort};
pub(crate) use runtime::EffectsCore;
// FLOWIP-120z/B9: the proof facades are doc-hidden public bounds so rustc can
// report failures in handler vocabulary without weakening the underlying set
// membership and containment proofs.
pub use typed::Effects;
#[doc(hidden)]
pub use typed::{
    AllowedEffectsAllowEffect, EffectOutcomeFitsOutput, OutputAllowsFact, RequestAllowed,
    RequestOutcomeFits,
};

use commit::{
    append_domain_effect_success_facts, append_effect_record, build_domain_effect_success_facts,
    build_effect_attempt_started_event, build_effect_record_event,
    build_effect_recovery_abandoned_event, EffectCommitHandleParams, PreparedEffectOutcome,
};
use identity::{
    descriptor_for_effect, descriptor_hash, deterministic_effect_evidence_event_id,
    hash_json_value, stamp_output_event,
};
use replay::{
    decode_effect_outcome, decode_effect_outcome_group, effect_fact_set_error,
    effect_record_from_event, effect_record_group_materialization, is_routable_output_fact,
    recorded_failure_from_outcome, validate_effect_outcome_group, EffectRecordMaterialization,
};
