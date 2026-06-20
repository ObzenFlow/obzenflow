// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-chain middleware contracts and stage adapters.
//!
//! The modules here adapt the generic [`Middleware`] chain to concrete runtime
//! handler traits. They are separate from
//! [`control::policy`](crate::middleware::control::policy), which owns live-I/O
//! admission and observation policies for source poll, effect invocation, sink
//! delivery, and ingress.

pub mod contract;
mod join;
mod sink;
mod source;
mod stateful;
mod transform;

pub(crate) use contract::observation_short_circuit;
pub use contract::{
    ErrorAction, Middleware, MiddlewareAbortCause, MiddlewareAction, SourceMiddlewarePhase,
};
pub use join::{JoinHandlerMiddlewareExt, JoinMiddlewareBuilder, MiddlewareJoin};
pub use sink::{MiddlewareSink, SinkHandlerExt, SinkMiddlewareBuilder};
pub use source::{
    AsyncFiniteSourceHandlerExt, AsyncFiniteSourceMiddlewareBuilder, AsyncInfiniteSourceHandlerExt,
    AsyncInfiniteSourceMiddlewareBuilder, FiniteSourceHandlerExt, FiniteSourceMiddlewareBuilder,
    InfiniteSourceHandlerExt, InfiniteSourceMiddlewareBuilder, MiddlewareAsyncFiniteSource,
    MiddlewareAsyncInfiniteSource, MiddlewareFiniteSource, MiddlewareInfiniteSource,
};
pub use stateful::{MiddlewareStateful, StatefulHandlerMiddlewareExt, StatefulMiddlewareBuilder};
pub use transform::{
    AsyncMiddlewareTransform, AsyncTransformHandlerExt, AsyncTransformMiddlewareBuilder,
    MiddlewareTransform, TransformHandlerExt, TransformMiddlewareBuilder,
    UnifiedMiddlewareTransform,
};
