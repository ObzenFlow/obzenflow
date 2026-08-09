// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::ai_map_reduce;

fn main() {
    let _ = ai_map_reduce!(
        Seed -> Out => {
            map: [Item] ->{
                at_least_once(ChatCompletion) via chat with policy
            } Partial => map_role,
            reduce: (Seed, [Partial]) ->{
                at_least_once(ChatCompletion) via chat with policy
            } Out => finalise_role,
        },
        chunking: by_budget {
            estimator: estimator,
            placeholder
        }
    );
}
