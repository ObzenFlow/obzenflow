// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::EffectPortResolutionError;

fn main() {
    let _ = EffectPortResolutionError::Other("credential-canary".to_string());
}
