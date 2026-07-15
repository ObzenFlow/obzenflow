// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::SingleUseEffectOperation;

fn reject_after_execute(operation: SingleUseEffectOperation) {
    let in_flight = operation.execute();
    drop(in_flight);
    let _report = operation.reject_fallback(None, Vec::new());
}

fn main() {}
