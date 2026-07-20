// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-related components organized by type

pub mod join;
pub mod observer;
pub mod resource_managed;
pub mod sink;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export all handler traits for convenience
pub use join::{JoinHandler, UnifiedJoinHandler};
pub use observer::ObserverHandler;
pub use resource_managed::ResourceManaged;
pub use sink::{
    CommitReceipt, Delivered, Delivery, SinkConsumeReport, SinkHandler, SinkLifecycleReport,
    UnifiedSinkHandler,
};
pub use source::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
pub use stateful::{
    EffectfulStatefulHandler, EffectfulStatefulHandlerAdapter, StatefulHandler, StatefulHandlerExt,
    StatefulHandlerWithEmission, StatefulOutputContext, UnifiedStatefulHandler,
};
pub use transform::{
    AsyncTransformHandler, EffectfulTransformHandler, EffectfulTransformHandlerAdapter,
    TransformHandler, TypedTransformHandler, TypedTransformHandlerAdapter, UnifiedTransformHandler,
};
