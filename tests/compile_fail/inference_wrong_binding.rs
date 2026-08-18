// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::inference;

#[path = "support/ai_surface.rs"]
mod support;
use support::*;

fn main() {
    let embeddings = false;
    let handler = inference_handler();
    let policy = obzenflow_adapters::middleware::control::ai_resilience();
    let _ = inference!(
        Input -> Output uses at_least_once(ChatCompletion) via embeddings with policy => handler
    );
}
