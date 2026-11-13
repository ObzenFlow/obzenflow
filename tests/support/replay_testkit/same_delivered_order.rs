// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::delivered_order::project_delivered_order;
use std::path::Path;

/// Assert two runs delivered the same order at a fan-in stage.
pub async fn assert_same_delivered_order(
    run_a: &Path,
    run_b: &Path,
    stage_key: &str,
    upstream_stage_keys: &[&str],
) {
    let a = project_delivered_order(run_a, stage_key, upstream_stage_keys).await;
    let b = project_delivered_order(run_b, stage_key, upstream_stage_keys).await;
    a.assert_equal(&b);
}
