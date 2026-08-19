// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::test_flow;

fn main() {
    let _ = test_flow! {
        name: "removed_test_effect_ports",
        journals: (),
        effect_ports: (),
        stages: {},
        topology: {}
    };
}
