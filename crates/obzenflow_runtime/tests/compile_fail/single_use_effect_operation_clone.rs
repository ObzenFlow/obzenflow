// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::SingleUseEffectOperation;

fn clone_operation(operation: SingleUseEffectOperation) {
    let _copy = operation.clone();
}

fn main() {}
