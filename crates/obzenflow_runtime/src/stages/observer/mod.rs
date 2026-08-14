// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned observer stage ports and dispatch.
//!
//! This is the low-level neutral observer boundary used by the DSL and adapter
//! middleware layer. It is intentionally placed beside the stage dispatch sites
//! instead of at the runtime crate root.

mod composition;
pub(crate) mod dispatch;
pub mod ports;

pub use composition::StageObserverBundle;
#[doc(hidden)]
pub use composition::StageObserverBundleBuilder;
pub use ports::{
    EffectObserver, EffectObserverContext, EffectObserverOutcome, HandlerObserver,
    HandlerObserverContext, JoinCanonicalMergeMetadata, JoinDeliverySnapshot, JoinObserver,
    JoinObserverContext, JoinObserverOccurrence, JoinSide, JoinSignalKind, JoinSignalSnapshot,
    SinkDeliveryAttemptResult, SinkDeliveryObserver, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserver, SourcePollObserverContext,
    SourcePollObserverOutcome, StageLifecycleObserver, StageLifecycleObserverContext,
    StageLifecyclePhase, StatefulObserver, StatefulObserverContext,
};
