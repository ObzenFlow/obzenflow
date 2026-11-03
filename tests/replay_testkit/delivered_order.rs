// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::ordered_stage_journal::read_stage_envelopes;
use obzenflow_runtime::testing::DeliveredOrderProjection;
use std::path::Path;

/// Project a fan-in stage's delivered order from a run directory.
pub async fn project_delivered_order(
    run_dir: &Path,
    stage_key: &str,
    upstream_stage_keys: &[&str],
) -> DeliveredOrderProjection {
    let stage_outputs = read_stage_envelopes(run_dir, stage_key).await;
    let mut upstreams = Vec::new();
    for upstream in upstream_stage_keys {
        upstreams.push((
            upstream.to_string(),
            read_stage_envelopes(run_dir, upstream).await,
        ));
    }
    DeliveredOrderProjection::from_envelopes(&stage_outputs, &upstreams)
}
