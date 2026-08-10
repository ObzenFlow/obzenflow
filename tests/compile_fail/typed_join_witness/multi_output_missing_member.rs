// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../support/typed_join.rs"]
mod support;
use support::{Exact, First, Reference, Second, Stream};

fn main() {
    let _ = obzenflow_dsl::join!(
        catalog reference_stage: Reference,
        Stream -> { First, Second } => Exact
    );
}
