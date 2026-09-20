// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow definitions and stage macros.
//!
//! Construct handlers inside a deferred [`FlowDefinition`] materialiser, then
//! pass those bindings to the stage macros. `handler_set!` is syntax consumed
//! by `sink!`, not a standalone handler value.

pub use obzenflow_dsl::backpressure;
pub use obzenflow_dsl::{
    ai_map_reduce, async_infinite_source, async_source, effectful_stateful, effectful_transform,
    flow, handler_set, inference, infinite_source, join, placeholder, sink, source, stateful,
    transform, FlowBuildError, FlowBuildFailure, FlowDefinition, StageCreationError,
    StageCreationResult,
};
