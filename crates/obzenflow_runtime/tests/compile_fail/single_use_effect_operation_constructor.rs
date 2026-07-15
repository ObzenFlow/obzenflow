// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ChainEvent;
use obzenflow_runtime::effects::{EffectError, SingleUseEffectOperation};

fn main() {
    let _operation = SingleUseEffectOperation::new(|| async {
        Ok::<Vec<ChainEvent>, EffectError>(Vec::new())
    });
}
