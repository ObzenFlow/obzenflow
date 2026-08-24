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
    EffectfulStatefulHandler, EffectfulTransformHandler, HeartbeatConfig, HostedIngressSource,
    InferenceHandler, IngressDecodeError, IngressDecoder, InlineSink, ObserverHandler,
    ProcessingContext, ResourceManaged, SignalDecision, SignalGate, SinkConnector, SinkDescription,
    SinkWriter, SinkWriterInitContext, SourceObservationSink, StatefulEmission,
    TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
    TypedInfiniteSourceHandler, TypedStatefulHandler,
};

// FLOWIP-115c runtime control-strategy hooks.
pub use common::{
    AdmissionDecision, AdmissionGate, AdmissionPosition, AttemptObserver, AttemptOutcome,
    CreditWaker, WakeOn,
};

// Public join authoring is the typed witness surface.
pub use common::handlers::{JoinReferenceView, TypedJoinHandler, TypedTransformHandler};

// Re-export resources builder
pub use crate::typing::{SourceTyping, TransformTyping};
pub use resources_builder::{StageResources, StageResourcesBuilder, StageResourcesSet};
