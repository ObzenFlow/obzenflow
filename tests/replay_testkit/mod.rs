// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared replay-archive helpers for FLOWIP-095d determinism tests.
//!
//! Promoted from the private helpers proven in
//! `tests/effect_replay_suppression_test.rs`: locate the most recent run
//! directory, read a stage's journal through the run manifest, and project
//! delivered order for cross-run comparison. Include from a test binary with
//! `mod replay_testkit;`.

#![allow(dead_code)]

use obzenflow_core::event::{ChainEvent, EventEnvelope};
use obzenflow_core::id::StageId;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_runtime::testing::DeliveredOrderProjection;
use std::path::{Path, PathBuf};

/// The most recent run directory under `base/flows/`.
pub fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries
        .pop()
        .expect("a run should have produced a replay archive")
}

/// Parse a run directory's `run_manifest.json`.
pub fn archive_manifest(run_dir: &Path) -> serde_json::Value {
    let manifest_path = run_dir.join("run_manifest.json");
    serde_json::from_str(
        &std::fs::read_to_string(&manifest_path).expect("run_manifest.json should be readable"),
    )
    .expect("run_manifest.json should parse")
}

/// Read a stage's data journal (causally ordered envelopes) through the run
/// manifest.
pub async fn read_stage_envelopes(
    run_dir: &Path,
    stage_key: &str,
) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = archive_manifest(run_dir);
    let stage_journal = manifest["stages"][stage_key]["data_journal_file"]
        .as_str()
        .unwrap_or_else(|| panic!("manifest should contain data journal for '{stage_key}'"));
    let journal: obzenflow_infra::journal::DiskJournal<ChainEvent> =
        obzenflow_infra::journal::DiskJournal::with_owner(
            run_dir.join(stage_journal),
            JournalOwner::stage(StageId::new()),
        )
        .expect("stage journal should open");

    journal
        .read_causally_ordered()
        .await
        .expect("stage journal should read")
}

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
