// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::RepeatableEffectOperation;

fn overlap(mut operation: RepeatableEffectOperation) {
    let first = operation.execute();
    let second = operation.execute();
    drop((first, second));
}

fn main() {}
