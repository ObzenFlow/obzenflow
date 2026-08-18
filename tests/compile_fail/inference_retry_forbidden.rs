// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_dsl::inference;

#[path = "support/ai_surface.rs"]
mod support;
use support::*;

fn main() {
    let chat = binding();
    let handler = inference_handler();
    let _ = inference!(
        Input -> Output uses at_least_once(ChatCompletion) via chat with retry(3) => handler
    );
}
