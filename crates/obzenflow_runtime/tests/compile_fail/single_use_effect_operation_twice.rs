// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::SingleUseEffectOperation;

async fn execute_twice(operation: SingleUseEffectOperation) {
    let _first = operation.execute().await;
    let _second = operation.execute().await;
}

fn main() {}
