// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-related components organized by type

pub mod input_order;
pub mod join;
pub mod observer;
pub mod order_insensitive;
pub mod resource_managed;
pub mod sink;
pub mod source;
pub mod stateful;
pub mod transform;

// Re-export all handler traits for convenience
pub use input_order::{InputOrderSemantics, OrderInsensitiveProof};
pub use join::JoinHandler;
pub use observer::ObserverHandler;
pub use resource_managed::ResourceManaged;
pub use sink::{
    CommitReceipt, DestinationOrder, EffectfulSinkHandler, EffectfulSinkHandlerAdapter,
    SinkConsumeReport, SinkHandler, SinkLifecycleReport, UnifiedSinkHandler,
};
pub use source::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
pub use stateful::{
    EffectfulStatefulHandler, EffectfulStatefulHandlerAdapter, StatefulHandler, StatefulHandlerExt,
    StatefulHandlerWithEmission, UnifiedStatefulHandler,
};
pub use transform::{
    AsyncTransformHandler, EffectfulTransformHandler, EffectfulTransformHandlerAdapter,
    TransformHandler, UnifiedTransformHandler,
};
