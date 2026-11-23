// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stage implementations organized by type

pub mod common;
pub mod join;
pub mod merge;
pub mod merges;
pub mod observer;
pub mod resources_builder;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export commonly used types from common
pub use common::handlers::source::SourceError;
pub use common::{new_liveness_snapshots, LivenessSnapshots};
pub use common::{
    prove_journal_form, EffectfulStatefulHandler, EffectfulTransformHandler, HeartbeatConfig,
    HostedSource, HostedSubmission, IngressDecodeError, IngressDecoder, IngressReceiptSender,
    IngressRecord, ObserverHandler, ProcessingContext, ResourceManaged, SignalDecision, SignalGate,
    SourceObservationSink, StatefulEmission, TypedAsyncFiniteSourceHandler,
    TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
    TypedStatefulHandler,
};

pub use common::{CreditWaker, WakeOn};

// Public join authoring is the typed witness surface.
pub use common::handlers::{
    AsOf, AsOfJoin, AsOfReferenceView, JoinReferenceView, TypedAsOfJoinHandler, TypedJoinHandler,
    TypedTransformHandler,
};

// Re-export resources builder
pub use crate::typing::{SourceTyping, TransformTyping};
pub use resources_builder::{StageResources, StageResourcesBuilder, StageResourcesSet};
