// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed AI helper facades.
//!
//! These helpers construct handlers intended for use with typed stage macros.

pub use obzenflow_dsl::dsl::composites::ai_map_reduce::{map_reduce, AiMapReduceBuilder};
pub use obzenflow_runtime::stages::stateful::CollectByInput;
use obzenflow_runtime::stages::transform::ChunkByBudgetBuilder;

pub fn chunk_by_budget<In, Item>() -> ChunkByBudgetBuilder<In, Item> {
    ChunkByBudgetBuilder::new()
}

pub fn collect_by_input<Partial, Collected, F>(
    initial: Collected,
    accumulate: F,
) -> CollectByInput<Partial, Collected>
where
    F: Fn(&mut Collected, &Partial) + Send + Sync + 'static,
{
    CollectByInput::new(initial, accumulate)
}
