// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-related components organized by type

pub mod inference;
pub(crate) mod join;
pub mod observer;
pub mod resource_managed;
pub mod sink;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export all handler traits for convenience
pub use inference::InferenceHandler;
pub(crate) use join::UnifiedJoinHandler;
pub use join::{JoinReferenceView, TypedJoinHandler};
pub use observer::ObserverHandler;
pub use resource_managed::ResourceManaged;
#[doc(hidden)]
pub use sink::{
    CommitReceipt, SinkConsumeReport, SinkHandler, SinkLifecycleReport, SinkWriterAdapter,
    UnifiedSinkHandler,
};
pub use sink::{
    DeliveryContext, DeliveryProvenance, InlineSink, PendingSinkInput, SinkAuditOutcome,
    SinkBufferedOutcome, SinkCommitReceipt, SinkConnector, SinkDescription,
    SinkDestinationErrorCode, SinkOperationError, SinkOperationErrorConversionError,
    SinkOperationResult, SinkPrimaryOutcome, SinkTerminalOutcome, SinkWriteContext,
    SinkWriteFailure, SinkWriteFailureDisposition, SinkWritePhase, SinkWriteReport,
    SinkWriteResult, SinkWriter, SinkWriterInitContext, SinkWriterLifecycleReport,
    WithRedeliverySafety,
};
pub use source::{
    HostedIngressSource, SourceError, SourceObservationSink, TypedAsyncFiniteSourceHandler,
    TypedAsyncInfiniteSourceHandler, TypedFiniteSourceHandler, TypedInfiniteSourceHandler,
    UnifiedAsyncFiniteSourceHandler, UnifiedAsyncInfiniteSourceHandler, UnifiedFiniteSourceHandler,
    UnifiedInfiniteSourceHandler,
};
pub use stateful::{
    EffectfulStatefulHandler, EffectfulStatefulHandlerAdapter, StatefulEmission,
    StatefulOutputContext, StatefulTerminationKind, TerminalValidation, TypedStatefulHandler,
    TypedStatefulHandlerAdapter, TypedStatefulInvocation, UnifiedStatefulHandler,
};
pub use transform::{
    EffectfulTransformHandler, EffectfulTransformHandlerAdapter, TransformHandler,
    TypedTransformHandler, TypedTransformHandlerAdapter, TypedTransformInvocation,
    UnifiedTransformHandler,
};
