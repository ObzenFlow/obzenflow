// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::EffectPortSlot;

const INVALID: EffectPortSlot<()> = EffectPortSlot::new("https://credential-canary.example");

fn main() {
    let _ = INVALID;
}
