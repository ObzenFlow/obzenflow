// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_runtime::effects::EffectPortResolutionError;

fn main() {
    let _ = EffectPortResolutionError::Other("credential-canary".to_string());
}
