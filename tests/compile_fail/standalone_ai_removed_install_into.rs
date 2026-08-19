// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::ai::ChatEffectBinding;
use obzenflow_runtime::effects::EffectPortRegistry;

fn main() {
    let _registry = EffectPortRegistry::new();
    let binding = ChatEffectBinding::ollama("model", None).unwrap();
    // Field-style lookup avoids a rust-src-dependent fuzzy `TryInto` suggestion.
    // Reintroducing an `install_into` method changes this diagnostic and fails the fixture.
    let _ = binding.install_into;
}
