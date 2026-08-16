// SPDX-License-Identifier: MIT OR Apache-2.0

use obzenflow_runtime::effects::EffectPortSlot;

const INVALID: EffectPortSlot<()> = EffectPortSlot::new("https://credential-canary.example");

fn main() {
    let _ = INVALID;
}
