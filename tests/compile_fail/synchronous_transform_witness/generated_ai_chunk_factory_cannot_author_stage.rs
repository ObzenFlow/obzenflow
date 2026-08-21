// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/synchronous_transform.rs"]
mod support;

use obzenflow_core::ai::TokenCount;
use obzenflow_core::id::CompositeId;
use obzenflow_runtime::stages::transform::strategies::ai_chunking::generated_ai_chunk_handler;
use obzenflow_runtime::stages::transform::ChunkByBudgetBuilder;
use support::{First, Input};

struct Debug; // Stabilises opaque-bound diagnostics across feature sets.

fn main() {
    let _ = Debug;
    let planner = ChunkByBudgetBuilder::new()
        .items(|_input: &Input| vec!["one".to_string()])
        .render(|item: &String, _context| item.clone())
        .budget(TokenCount::new(32))
        .build();
    let raw = generated_ai_chunk_handler(planner, CompositeId::new("compile-fail"));

    let _ = obzenflow_dsl::transform!(Input -> First => raw);
}
