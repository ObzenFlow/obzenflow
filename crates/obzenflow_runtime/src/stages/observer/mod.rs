// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Runtime-owned observer stage ports and dispatch.
//!
//! This is the low-level neutral observer boundary used by the DSL and adapter
//! middleware layer. It is intentionally placed beside the stage dispatch sites
//! instead of at the runtime crate root.

pub(crate) mod dispatch;
pub mod ports;

pub use ports::{
    EffectObserver, EffectObserverContext, EffectObserverOutcome, HandlerObserver,
    HandlerObserverContext, JoinCanonicalMergeMetadata, JoinDeliverySnapshot, JoinObserver,
    JoinObserverContext, JoinSide, JoinSignalKind, JoinSignalSnapshot, ObserverCommitError,
    ObserverCommitResult, ObserverDeterminism, ObserverReport, OutputCommitObserver,
    OutputCommitObserverContext, SinkDeliveryObserver, SinkDeliveryObserverContext,
    SinkDeliveryObserverOutcome, SourcePollObserver, SourcePollObserverContext,
    SourcePollObserverOutcome, StageLifecycleObserver, StageLifecycleObserverContext,
    StageLifecyclePhase, StageObserverBundle, StatefulObserver, StatefulObserverContext,
};
