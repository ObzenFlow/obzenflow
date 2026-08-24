// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![doc = include_str!("../README.md")]

// Escape-hatch target for `#[effect_outcome(crate = ...)]` (FLOWIP-120m):
// derive output resolves `::obzenflow_core` in the deriving crate, so a
// downstream crate that depends only on obzenflow_runtime points the derive
// at `obzenflow_runtime::obzenflow_core` instead.
#[doc(hidden)]
pub use obzenflow_core;

// Core modules
pub mod backpressure;
pub mod bootstrap;
pub mod contracts;
pub mod control_plane;
pub mod effects;
pub mod errors;
pub mod execution;
pub mod feed_plan;
pub mod id_conversions;
pub mod journal;
pub mod message_bus;
pub mod replay;
pub mod run_context;
pub mod runtime_config;
pub mod runtime_resource_limits;
pub mod supervised_base;
pub mod typing;

// Major subsystems
pub mod messaging;
pub mod metrics;
pub mod pipeline;
pub mod stages;

/// Cross-crate runtime erasure used by the DSL and low-level supervisor tests.
/// Authored joins use [`stages::TypedJoinHandler`] instead.
#[doc(hidden)]
pub mod __private {
    pub use crate::stages::common::handlers::join::{
        ErasedJoinInvocation, TypedJoinHandlerAdapter, UnifiedJoinHandler,
    };
    pub use crate::stages::common::handlers::sink::SinkWriterAdapter;
    pub use crate::stages::common::handlers::source::typed::{
        TypedAsyncFiniteSourceHandlerAdapter, TypedAsyncInfiniteSourceHandlerAdapter,
        TypedFiniteSourceHandlerAdapter, TypedInfiniteSourceHandlerAdapter,
    };
    pub use crate::stages::common::handlers::source::{
        ErasedSourceCompletion, ErasedSourceInvocation, ErasedSourceOutcome,
        UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler,
        UnifiedFiniteSourceHandler, UnifiedInfiniteSourceHandler,
    };
}

#[cfg(any(test, feature = "test-support"))]
pub mod testing;

/// Convenience re-exports of the most commonly used runtime types.
///
/// The prelude gathers types that almost every flow definition or stage
/// implementation needs, grouped into five categories:
///
/// **Errors** — [`FlowError`](crate::errors::FlowError),
/// [`MessageBusError`](crate::errors::MessageBusError),
/// [`PipelineSupervisorError`](crate::errors::PipelineSupervisorError),
/// and the [`RuntimeResult`](crate::errors::RuntimeResult) type alias.
///
/// **Pipeline** —
/// [`PipelineBuilder`](crate::pipeline::PipelineBuilder) and
/// [`PipelineStageConfig`](crate::pipeline::PipelineStageConfig) for
/// constructing flows,
/// [`FlowHandle`](crate::pipeline::FlowHandle) for controlling a running
/// pipeline, and
/// [`PipelineState`](crate::pipeline::PipelineState) and
/// [`PipelineEvent`](crate::pipeline::PipelineEvent) for observing lifecycle
/// transitions.
///
/// **Message bus** —
/// [`FsmMessageBus`](crate::message_bus::FsmMessageBus) (the inter-stage
/// transport) and
/// [`StageCommand`](crate::message_bus::StageCommand) (control signals
/// sent to individual stages).
///
/// **Handlers** — the user-facing handler traits:
/// [`TypedFiniteSourceHandler`](crate::stages::TypedFiniteSourceHandler),
/// [`TypedInfiniteSourceHandler`](crate::stages::TypedInfiniteSourceHandler),
/// [`TypedTransformHandler`](crate::stages::TypedTransformHandler),
/// [`SinkWriter`](crate::stages::SinkWriter),
/// [`TypedStatefulHandler`](crate::stages::TypedStatefulHandler),
/// [`TypedJoinHandler`](crate::stages::TypedJoinHandler),
/// [`ObserverHandler`](crate::stages::ObserverHandler), and the
/// [`ResourceManaged`](crate::stages::ResourceManaged) trait for stages
/// that own resources. Also includes
/// [`SourceError`](crate::stages::SourceError) for source-specific error
/// reporting.
///
/// **Metrics** —
/// [`DefaultMetricsConfig`](crate::metrics::DefaultMetricsConfig) for
/// configuring the built-in metrics subsystem.
///
/// **Event flow** —
/// [`UpstreamSubscription`](crate::messaging::UpstreamSubscription) for
/// wiring journal-based message delivery between stages.
pub mod prelude {
    // Errors
    pub use crate::errors::{FlowError, MessageBusError, PipelineSupervisorError, RuntimeResult};

    // Pipeline
    pub use crate::pipeline::{
        FlowHandle, FlowStopMode, ObserverConfig, PipelineBuilder, PipelineEvent,
        PipelineStageConfig, PipelineState,
    };

    // Message bus
    pub use crate::message_bus::{FsmMessageBus, StageCommand};

    // Handlers
    pub use crate::stages::{
        EffectfulStatefulHandler, EffectfulTransformHandler, HostedIngressSource, InferenceHandler,
        IngressDecodeError, IngressDecoder, InlineSink, ObserverHandler, ResourceManaged,
        SinkConnector, SinkDescription, SinkWriter, SourceError, SourceObservationSink,
        TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler,
        TypedInfiniteSourceHandler, TypedJoinHandler, TypedStatefulHandler, TypedTransformHandler,
    };
    pub use crate::typing::{SourceTyping, TransformTyping};

    // Event flow
    pub use crate::effects::{
        DomainFacts, Effect, EffectCommitHandle, EffectContext, EffectDeclaration, EffectError,
        EffectOutcomeKind, EffectOutcomePayload, EffectSafety, Effects, IdempotencyKey,
        RecordedReply, SinkRedeliverySafety, TransactionalEffectPort,
    };
    pub use crate::messaging::UpstreamSubscription;

    // Metrics
    pub use crate::metrics::DefaultMetricsConfig;
}
