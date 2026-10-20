// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe external operations, the requests systems answer, and their
//! typed bindings.

pub use obzenflow_core::{BindingEvidenceError, BoundedBindingEvidence, SystemBindingEvidence};
pub use obzenflow_runtime::effect_set;
pub use obzenflow_runtime::effects::{
    BindingAuthorityFault, BindingMismatchKind, EffectAttemptOrdinal, EffectCursor,
    EffectDescriptorHash, EffectInputPosition, EffectLabel, EffectOrdinal, EffectStageKey,
    EffectType, RecordedFlowId,
};
pub use obzenflow_runtime::effects::{
    BindingIdentifierError, DomainFacts, Effect, EffectBinding, EffectBindingBuildError,
    EffectBindingEvidence, EffectBindingFor, EffectBindingMode, EffectBindingUse, EffectContext,
    EffectError, EffectFailureCause, EffectFailureCode, EffectFailureDetail, EffectFailureKind,
    EffectFailureSource, EffectOutcomeKind, EffectOutcomeSemantics, EffectPortMetadataContext,
    EffectPortResolutionError, EffectPortResolver, EffectPortResolverWithMetadata, EffectPortSlot,
    EffectPortSlotLabel, EffectPortSlotSet, EffectRegistrationBuilder,
    EffectRegistrationCollectionError, EffectSafety, EffectSet, Effects, IdempotencyKey,
    IdempotencyKeyPolicy, LogicalEffectBindingName, Named, NamedEffect, NoPortMetadata, Portless,
    RecordedReply, ResolvedEffectPort, RetryDisposition, StageCompletion, TransactionalEffectPort,
};
pub use obzenflow_runtime::effects::{
    Call, CallRequest, HandlerSystems, StageSystem, System, SystemError, WithSystems,
};

// Proof bounds needed by application-owned typed effect helpers.
#[doc(hidden)]
pub use obzenflow_runtime::effects::{AllowedEffectsAllowEffect, EffectOutcomeFitsOutput};
