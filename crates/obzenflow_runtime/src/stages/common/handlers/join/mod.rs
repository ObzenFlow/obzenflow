// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join handler trait

mod traits;
mod typed;

pub use traits::{ErasedJoinInvocation, UnifiedJoinHandler};
pub use typed::TypedJoinHandlerAdapter;
pub(crate) use typed::TypedJoinInvocation;
pub use typed::{JoinReferenceView, TypedJoinHandler};
