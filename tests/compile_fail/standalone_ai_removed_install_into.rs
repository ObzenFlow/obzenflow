// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow::ai::ChatEffectBinding;
use obzenflow_runtime::effects::EffectPortRegistry;

fn main() {
    let mut registry = EffectPortRegistry::new();
    let binding = ChatEffectBinding::ollama("model", None).unwrap();
    let _ = binding.install_into(&mut registry);
}
