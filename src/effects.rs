// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-safe external operations: the requests systems answer, and the
//! systems a handler holds.

pub use obzenflow_core::{BindingEvidenceError, SystemBindingEvidence};
pub use obzenflow_runtime::effect_set;
pub use obzenflow_runtime::effects::{
    port, AtLeastOnce, Call, CallRequest, CallSafety, EffectHandler, Idempotent, Keyed,
    RequiresKey, StageSystem, System, SystemError, Transactional,
};
pub use obzenflow_runtime::effects::{
    BindingAuthorityFault, BindingMismatchKind, EffectAttemptOrdinal, EffectCursor,
    EffectDescriptorHash, EffectInputPosition, EffectLabel, EffectOrdinal, EffectStageKey,
    EffectType, RecordedFlowId,
};
pub use obzenflow_runtime::effects::{
    EffectBinding, EffectCommitHandle, EffectContext, EffectError, EffectFailureCause,
    EffectFailureCode, EffectFailureDetail, EffectFailureKind, EffectFailureSource,
    EffectPortResolutionError, EffectSet, Effects, IdempotencyKey, RetryDisposition,
    StageCompletion,
};

#[doc(hidden)]
pub use obzenflow_runtime::effects::{PortImpl, ServesPort};

/// Building blocks for simulated outside systems: a call log that answers a
/// repeated idempotency key with its first decided response and records every
/// call, across process kills.
pub mod simulated {
    pub use obzenflow_infra::simulated::{CallLog, CallLogError};
}
