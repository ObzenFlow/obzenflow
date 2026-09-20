// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed join constructors owned by the runtime join capability.

pub use obzenflow_runtime::stages::join::{
    inner, inner_live, left, left_live, strict, strict_live,
};

pub use obzenflow_runtime::stages::common::handlers::{JoinReferenceView, TypedJoinHandler};
pub use obzenflow_runtime::stages::join::strategies::{
    InnerJoin, InnerJoinBuilder, LeftJoin, LeftJoinBuilder, StrictJoin, StrictJoinBuilder,
};
pub use obzenflow_runtime::stages::join::JoinReferenceMode;
