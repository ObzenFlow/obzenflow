// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::inference;

fn main() {
    let _ = inference!(
        [Input] ->{
            at_least_once(ChatCompletion) via chat with policy
        } Output => role
    );
}
