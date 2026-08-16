// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::ai_map_reduce;

#[path = "support/ai_surface.rs"]
mod support;
use support::*;

fn main() {
    let chat = binding();
    let map_role = MapRole;
    let finalise_role = FinaliseRole;
    let map_policy = obzenflow_adapters::middleware::control::ai_resilience();
    let reduce_policy = obzenflow_adapters::middleware::control::ai_resilience();
    let _ = ai_map_reduce!(
        Seed -> Output => {
            map: [Item] ->{
                at_least_once(ChatCompletion) via chat with [map_policy]
            } Partial => map_role,
            reduce: (Seed, [Partial]) ->{
                at_least_once(ChatCompletion) via chat with reduce_policy
            } Output => finalise_role,
        },
        chunking: by_budget {
            items: |_seed: &Seed| Vec::<Item>::new(),
            render: |_item: &Item, _ctx: obzenflow_core::ai::ChunkRenderContext| String::new(),
            budget: obzenflow_core::ai::TokenCount::new(1),
            max_items: None,
            oversize: error,
        }
    );
}
