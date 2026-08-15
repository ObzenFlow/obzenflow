// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::StageId;
use obzenflow_runtime::stages::observer::StageObserverBundle;
use obzenflow_runtime::stages::transform::TransformConfig;

fn main() {
    let mut config = TransformConfig::new(StageId::new(), "stage", "flow", Vec::new());
    config.observers = StageObserverBundle::default();
}
