// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Input;
struct Output;
struct Effect;
struct Handler;

fn main() {
    let first = ();
    let second = ();
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> Output uses Effect with [first] => Handler,
        observers: []
    );
    let _ = obzenflow_dsl::effectful_transform!(
        Input -> Output uses Effect with { first, second } => Handler,
        observers: []
    );
}
