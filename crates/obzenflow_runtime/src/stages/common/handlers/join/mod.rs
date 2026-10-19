// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join handler trait

mod as_of;
mod traits;
mod typed;

pub use as_of::{AsOf, AsOfJoin, AsOfReferenceView, TypedAsOfJoinHandler};
pub use traits::{ErasedJoinInvocation, JoinBinding, UnifiedJoinHandler};
pub use typed::TypedJoinHandlerAdapter;
pub(crate) use typed::TypedJoinInvocation;
pub use typed::{JoinReferenceView, TypedJoinHandler};
