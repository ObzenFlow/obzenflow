// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe external operations and their typed bindings.

pub use obzenflow_core::{BindingEvidenceError, BoundedBindingEvidence};
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

// Proof bounds needed by application-owned typed effect helpers.
#[doc(hidden)]
pub use obzenflow_runtime::effects::{AllowedEffectsAllowEffect, EffectOutcomeFitsOutput};
