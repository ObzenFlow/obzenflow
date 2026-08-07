// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/synchronous_transform.rs"]
mod support;

use obzenflow_runtime::stages::common::handlers::TypedTransformInvocation;
use support::First;

fn main() {
    let _ = TypedTransformInvocation::<First> {
        output: First,
        framework_observability: None,
    };
}
