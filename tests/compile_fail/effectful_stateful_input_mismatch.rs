// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

#[path = "support/typed_effectful.rs"]
mod support;
use support::{First, StatefulFirstOnly};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OtherInput;

impl TypedPayload for OtherInput {
    const EVENT_TYPE: &'static str = "compile_fail.effectful.other_stateful_input";
}

fn main() {
    let _ = obzenflow_dsl::effectful_stateful!(
        OtherInput -> First => StatefulFirstOnly,
        effects: [],
        middleware: []
    );
}
