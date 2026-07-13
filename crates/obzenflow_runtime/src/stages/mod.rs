// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage implementations organized by type

pub mod common;
pub mod join;
pub mod observer;
pub mod resources_builder;
pub mod sink;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export commonly used types from common
pub use common::handlers::source::SourceError;
pub use common::{new_liveness_snapshots, LivenessSnapshots};
pub use common::{
    EffectfulStatefulHandler, EffectfulTransformHandler, FiniteSourceHandler, HeartbeatConfig,
    InfiniteSourceHandler, ObserverHandler, ProcessingContext, ResourceManaged, SignalDecision,
    SignalGate, SinkHandler, SourcePollRetryOwnership, SourcePollRetrySafety, StatefulHandler,
    TransformHandler,
};

// FLOWIP-115c runtime control-strategy hooks.
pub use common::{
    AdmissionDecision, AdmissionGate, AdmissionPosition, AttemptObserver, AttemptOutcome,
    CreditWaker, WakeOn,
};

// Re-export JoinHandler from common::handlers
pub use common::handlers::{JoinHandler, UnifiedJoinHandler};

// Re-export resources builder
pub use crate::typing::{JoinTyping, SinkTyping, SourceTyping, StatefulTyping, TransformTyping};
pub use resources_builder::{StageResources, StageResourcesBuilder, StageResourcesSet};
