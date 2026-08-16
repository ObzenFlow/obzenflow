// SPDX-License-Identifier: MIT OR Apache-2.0

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
