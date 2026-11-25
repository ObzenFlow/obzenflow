// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-related components organized by type

pub(crate) mod join;
pub mod observer;
pub mod resource_managed;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export all handler traits for convenience
pub(crate) use join::UnifiedJoinHandler;
pub use join::{
    AsOf, AsOfJoin, AsOfReferenceView, JoinReferenceView, TypedAsOfJoinHandler, TypedJoinHandler,
};
pub use observer::ObserverHandler;
pub use resource_managed::ResourceManaged;
pub use source::{
    HostedSource, SourceError, TypedAsyncFiniteSourceHandler, TypedAsyncInfiniteSourceHandler,
    TypedFiniteSourceHandler, TypedInfiniteSourceHandler, UnifiedAsyncFiniteSourceHandler,
    UnifiedAsyncInfiniteSourceHandler,
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
