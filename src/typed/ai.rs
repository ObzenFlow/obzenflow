// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed AI handler facades.
//!
//! These helpers return concrete handlers for use on the right-hand side of
//! typed stage macros.

pub use obzenflow_adapters::ai::{inference_handler, InferenceHandler};
pub use obzenflow_core::ai::Many;
use obzenflow_runtime::stages::transform::ChunkByBudgetBuilder;

pub fn chunk_by_budget<In, Item>() -> ChunkByBudgetBuilder<In, Item> {
    ChunkByBudgetBuilder::new()
}
