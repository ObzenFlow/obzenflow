// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

struct Input;
struct Handler;

fn main() {
    let observer = ();
    let _ = obzenflow_dsl::sink!(
        Input => Handler,
        observers: [observer],
        delivery: idempotent
    );
}
