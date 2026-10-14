// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Merge stage implementation (FLOWIP-084q B7 to B9).
//!
//! A merge combines several producers of one type into one journaled stream,
//! in the order its framework strategy decides. It runs no author code: the
//! strategy is a closed set the supervisor interprets.

pub mod builder;
pub mod config;
pub mod fsm;
pub mod handle;
pub(crate) mod supervisor;

pub use builder::MergeBuilder;
pub use config::MergeConfig;
pub use fsm::{MergeEvent, MergeState};
pub use handle::{MergeHandle, MergeHandleExt};
