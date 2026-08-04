// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::ai::ModelConfig;

fn main() {
    let config = ModelConfig::ollama("model");
    let _ = config.chat_builder();
    let _ = config.chat();
}
