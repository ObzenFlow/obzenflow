// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Output;
struct Handler;

fn main() {
    let policy = ();
    let _ = obzenflow_dsl::source!(Output => Handler, [policy]);
    let _ = obzenflow_dsl::async_source!(Output => Handler, [policy]);
    let _ = obzenflow_dsl::infinite_source!(Output => Handler, [policy]);
    let _ = obzenflow_dsl::async_infinite_source!(Output => Handler, [policy]);
    let _ = obzenflow_dsl::source!(Output => Handler, []);
    let _ = obzenflow_dsl::async_source!(Output => Handler, []);
    let _ = obzenflow_dsl::infinite_source!(Output => Handler, []);
    let _ = obzenflow_dsl::async_infinite_source!(Output => Handler, []);
}
