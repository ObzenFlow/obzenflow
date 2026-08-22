// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Output;
struct Handler;

fn main() {
    let policy = ();
    let _ = obzenflow_dsl::source!(Output => Handler, ingress with [policy]);
    let _ = obzenflow_dsl::async_source!(Output => Handler, ingress with [policy]);
    let _ = obzenflow_dsl::infinite_source!(Output => Handler, ingress with [policy]);
    let _ = obzenflow_dsl::async_infinite_source!(Output => Handler, ingress with [policy]);

    let source_policy = ();
    let _ = obzenflow_dsl::source!(
        Output => Handler with [source_policy],
        ingress with [policy]
    );
    let _ = obzenflow_dsl::async_source!(
        Output => Handler with [source_policy],
        ingress with [policy]
    );
    let _ = obzenflow_dsl::infinite_source!(
        Output => Handler with [source_policy],
        ingress with [policy]
    );
    let _ = obzenflow_dsl::async_infinite_source!(
        Output => Handler with [source_policy],
        ingress with [policy]
    );
}
