// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::ai_map_reduce;

fn main() {
    let _ = ai_map_reduce!(
        Seed -> Out => {
            map: [Item] -> Partial uses at_least_once(ChatCompletion) via chat with policy => map_role,
            reduce: (Seed, [Partial]) -> Out uses at_least_once(ChatCompletion) via chat with policy => finalise_role,
        },
        chunking: by_budget {
            estimator: estimator,
            placeholder
        }
    );
}
