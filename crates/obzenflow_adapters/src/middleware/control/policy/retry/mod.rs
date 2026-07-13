// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Surface-neutral retry coordination facts shared by adapter-owned boundaries.

mod budget;
mod lifecycle;
mod types;
mod wait;

pub use types::{AttemptDisposition, BoundaryRetryOwner, BoundaryRetryPolicy};

pub(crate) use budget::RetryBudget;
pub(crate) use lifecycle::{attempt_failed_event, exhausted_event, succeeded_event};
pub(crate) use wait::{
    await_active_execution, await_before_execution, await_settlement, RetryWaitError,
};

#[cfg(test)]
mod tests;
