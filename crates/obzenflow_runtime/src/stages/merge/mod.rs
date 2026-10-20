// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Merge stage, which combines several producers of one type into one journalled
//! stream in the order its framework strategy decides. It runs no author code.

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
#[cfg(test)]
mod lifecycle_tests;
pub(crate) mod supervisor;
#[cfg(test)]
mod transition_tests;

pub use builder::MergeBuilder;
pub use config::MergeConfig;
pub use fsm::{MergeEvent, MergeState};
pub use handle::{MergeHandle, MergeHandleExt};
