// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::flow;

fn main() {
    let _ = flow! {
        name: "retired_flow_bindings",
        journals: (),
        middleware: [],
        bindings: |_runtime_config| {
            let _handler = ();
        },

        stages: {},
        topology: {}
    };
}
