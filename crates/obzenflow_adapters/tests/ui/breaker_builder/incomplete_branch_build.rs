// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::middleware::control::CircuitBreaker;
use obzenflow_core::TypedPayload;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
struct Input;
impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "ui.input";
}

#[derive(Clone, Serialize, Deserialize)]
struct FallbackFact;
impl TypedPayload for FallbackFact {
    const EVENT_TYPE: &'static str = "ui.fallback";
}

fn incomplete_branch() {
    // A branch shape without its rejection producer has no build().
    let _ = CircuitBreaker::opens_after(1)
        .fallback_fact(|_: &Input| FallbackFact)
        .build();
}

fn main() {}
