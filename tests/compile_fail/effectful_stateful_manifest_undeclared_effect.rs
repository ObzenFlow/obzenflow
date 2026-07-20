// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, FirstEffect, Input, StatefulFirstOnly};

fn main() {
    let _ = obzenflow_dsl::effectful_stateful!(
        Input -> { First } => StatefulFirstOnly,
        effects: [FirstEffect],
        middleware: []
    );
}
