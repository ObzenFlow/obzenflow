// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::EffectPortResolutionError;

fn classify(error: EffectPortResolutionError) -> u8 {
    match error {
        EffectPortResolutionError::CredentialUnavailable => 1,
        EffectPortResolutionError::ClientConstructionFailed => 2,
    }
}

fn main() {
    let _ = classify;
}
