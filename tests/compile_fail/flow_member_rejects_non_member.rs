// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::flow;
use obzenflow_infra::journal::memory_journals;

fn main() {
    let _ = flow! {
        name: "not_a_member",
        journals: memory_journals(),
        middleware: [],

        stages: {
            bogus = 42_u32;
        },

        topology: {
        }
    };
}
