// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::delivered_order::project_delivered_order;
use obzenflow_runtime::testing::DeliveredOrderProjection;
use std::path::Path;

/// Assert `extended_run`'s delivered order at a fan-in stage begins with
/// `prefix_run`'s order (FLOWIP-120n prefix stability).
///
/// The canonical merge recomputes the recorded prefix order during catch-up,
/// and FLOWIP-120n's all-sources admission barrier is what keeps the
/// recomputed order prefix-stable once live tails appear. A pure replay is the
/// degenerate case where the extension is empty.
pub async fn assert_prefix_stable(
    prefix_run: &Path,
    extended_run: &Path,
    stage_key: &str,
    upstream_stage_keys: &[&str],
) {
    let prefix = project_delivered_order(prefix_run, stage_key, upstream_stage_keys).await;
    let extended = project_delivered_order(extended_run, stage_key, upstream_stage_keys).await;
    assert!(
        extended.rows.len() >= prefix.rows.len(),
        "extended run delivered fewer rows ({}) than the recorded prefix ({})",
        extended.rows.len(),
        prefix.rows.len()
    );
    let truncated = DeliveredOrderProjection {
        rows: extended.rows[..prefix.rows.len()].to_vec(),
    };
    prefix.assert_equal(&truncated);
}
