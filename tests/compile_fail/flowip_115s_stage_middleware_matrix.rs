// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Input;
struct Output;
struct Reference;
struct Effect;
struct Handler;

fn main() {
    let observer = ();
    let _ = obzenflow_dsl::source!(Output => Handler, middleware: [observer]);
    let _ = obzenflow_dsl::async_source!(Output => Handler, middleware: [observer]);
    let _ = obzenflow_dsl::infinite_source!(Output => Handler, middleware: [observer]);
    let _ = obzenflow_dsl::async_infinite_source!(Output => Handler, middleware: [observer]);
    let _ = obzenflow_dsl::transform!(Input -> Output => Handler, middleware: [observer]);
    let _ = obzenflow_dsl::effectful_transform!(
        Input ->{ Effect } Output => Handler,
        middleware: [observer]
    );
    let _ = obzenflow_dsl::stateful!(Input -> Output => Handler, middleware: [observer]);
    let _ = obzenflow_dsl::effectful_stateful!(
        Input ->{ Effect } Output => Handler,
        middleware: [observer]
    );
    let _ = obzenflow_dsl::join!(
        catalog reference: Reference,
        Input -> Output => Handler,
        middleware: [observer]
    );
    let _ = obzenflow_dsl::sink!(Input => Handler, middleware: [observer]);
}
