// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed AI helper facades.
//!
//! These helpers construct handlers intended for use with typed stage macros.

use obzenflow_runtime::stages::transform::ChunkByBudgetBuilder;

pub fn chunk_by_budget<In, Item>() -> ChunkByBudgetBuilder<In, Item> {
    ChunkByBudgetBuilder::new()
}
