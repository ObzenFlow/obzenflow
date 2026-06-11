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
pub mod effects;
pub mod errors;
pub mod feed_plan;
pub mod id_conversions;
pub mod journal;
pub mod message_bus;
pub mod replay;
pub(crate) mod runtime_resource_limits;
pub mod supervised_base;
pub mod typing;

// Major subsystems
pub mod messaging;
pub mod metrics;
pub mod pipeline;
pub mod stages;

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
/// [`FiniteSourceHandler`](crate::stages::FiniteSourceHandler),
/// [`InfiniteSourceHandler`](crate::stages::InfiniteSourceHandler),
/// [`TransformHandler`](crate::stages::TransformHandler),
/// [`SinkHandler`](crate::stages::SinkHandler),
/// [`StatefulHandler`](crate::stages::StatefulHandler),
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
        EffectfulSinkHandler, EffectfulStatefulHandler, EffectfulTransformHandler,
        FiniteSourceHandler, InfiniteSourceHandler, ObserverHandler, ResourceManaged, SinkHandler,
        SourceError, StatefulHandler, TransformHandler,
    };
    pub use crate::typing::{
        JoinTyping, SinkTyping, SourceTyping, StatefulTyping, TransformTyping,
    };

    // Event flow
    pub use crate::effects::{
        Effect, EffectCommitHandle, EffectContext, EffectDeclaration, EffectError,
        EffectOutcomePayload, EffectRuntimeMode, EffectSafety, Effects, IdempotencyKey,
        TransactionalEffectPort,
    };
    pub use crate::messaging::UpstreamSubscription;

    // Metrics
    pub use crate::metrics::DefaultMetricsConfig;
}
