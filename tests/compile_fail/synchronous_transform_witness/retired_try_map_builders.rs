// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/synchronous_transform.rs"]
mod support;
use support::{First, Input};

fn mapper() -> impl obzenflow_runtime::stages::transform::TypedTransformHandler {
    obzenflow::transforms::try_map(|_input: Input| Ok::<First, &'static str>(First))
}

fn main() {
    let _ = mapper().on_error_journal();
    let _ = mapper().on_error_emit("failure.event");
    let _ = mapper().on_error_emit_with(|_| First);
    let _ = mapper().on_error_drop();
    let _ = mapper().on_error_with(|_| First);
}
