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

use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::{ChainEvent, ChainEventContent, EventEnvelope};
use obzenflow_core::id::StageId;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::WriterId;
use obzenflow_runtime::testing::DeliveredOrderProjection;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// FLOWIP-120u: the host opens the replay/resume input archive and the
/// bootstrap snapshot carries it. Tests install through this helper so the
/// build actually enters replay/resume instead of silently running live.
pub async fn bootstrap_with_archive(
    replay: obzenflow_runtime::bootstrap::ReplayBootstrap,
) -> obzenflow_runtime::bootstrap::BootstrapConfig {
    let archive = obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive::open(
        replay.archive_path.clone(),
        replay.allow_incomplete_archive,
    )
    .await
    .expect("test replay archive must open");
    obzenflow_runtime::bootstrap::BootstrapConfig {
        replay: Some(replay),
        replay_archive: Some(std::sync::Arc::new(archive)),
        ..Default::default()
    }
}

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

/// Read a stage's data journal in physical append order, the order a
/// downstream subscription's sequential reader delivers.
///
/// `read_stage_envelopes` orders by event id, which matches append order for
/// a stage's own outputs (a single writer mints ids in sequence) but NOT for
/// forwarded control rows, which retain their original upstream ids. Any
/// position-sensitive comparison involving control rows must use this reader.
pub async fn read_stage_envelopes_appended(
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

    let mut reader = journal.reader().await.expect("stage journal reader");
    let mut envelopes = Vec::new();
    while let Some(envelope) = reader.next().await.expect("stage journal read") {
        envelopes.push(envelope);
    }
    envelopes
}

/// The per-run `StageId` an upstream's authored EOF names, resolved from the
/// EOF payload's `writer_id` (falling back to the chain event's writer).
fn authored_eof_stage_id(envelopes: &[EventEnvelope<ChainEvent>]) -> Option<StageId> {
    envelopes.iter().find_map(|envelope| {
        let ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) =
            &envelope.event.content
        else {
            return None;
        };
        match writer_id {
            Some(WriterId::Stage(id)) => Some(*id),
            Some(_) => None,
            None => match &envelope.event.writer_id {
                WriterId::Stage(id) => Some(*id),
                _ => None,
            },
        }
    })
}

/// Project a stage's journal as a transport-row signature: `data:{event_type}`
/// for data rows and `eof:{upstream key}` for EOF rows attributed through each
/// upstream's authored EOF (FLOWIP-095d). EOFs whose author is not one of the
/// named upstreams (the stage's own authored EOF) are labelled `eof:local`.
///
/// Rows are read in physical append order, the order a downstream
/// subscription delivers. `StageId`s are per-run, so cross-run comparison
/// must resolve them through stable stage keys; this helper does that
/// resolution per run. The position of a forwarded reference EOF in this
/// signature is the join's phase-transition witness: equality of signatures
/// across runs proves the EOF-relative ordering reproduced.
///
/// Assumes each named upstream's journal contains only its own authored EOF
/// (true for source stages); reader telemetry and observability rows are
/// ignored.
pub async fn transport_row_signature(
    run_dir: &Path,
    stage_key: &str,
    upstream_stage_keys: &[&str],
) -> Vec<String> {
    let mut stage_id_to_key: HashMap<StageId, String> = HashMap::new();
    for upstream in upstream_stage_keys {
        let envelopes = read_stage_envelopes(run_dir, upstream).await;
        let stage_id = authored_eof_stage_id(&envelopes).unwrap_or_else(|| {
            panic!("upstream '{upstream}' should have an authored EOF naming its stage id")
        });
        stage_id_to_key.insert(stage_id, (*upstream).to_string());
    }

    read_stage_envelopes_appended(run_dir, stage_key)
        .await
        .iter()
        .filter_map(|envelope| match &envelope.event.content {
            ChainEventContent::Data { .. } => Some(format!("data:{}", envelope.event.event_type())),
            ChainEventContent::FlowControl(FlowControlPayload::Eof { writer_id, .. }) => {
                let author = match writer_id {
                    Some(WriterId::Stage(id)) => Some(*id),
                    Some(_) => None,
                    None => match &envelope.event.writer_id {
                        WriterId::Stage(id) => Some(*id),
                        _ => None,
                    },
                };
                let key = author
                    .and_then(|id| stage_id_to_key.get(&id).cloned())
                    .unwrap_or_else(|| "local".to_string());
                Some(format!("eof:{key}"))
            }
            _ => None,
        })
        .collect()
}

/// Count reader-telemetry flow-control rows in a stage's journal
/// (FLOWIP-095d). Witness tests use this to prove a run actually crossed the
/// contract tick and emitted telemetry mid-stream, so a determinism assertion
/// downstream of the telemetry filter is exercising the condition it pins.
pub async fn count_reader_telemetry_rows(run_dir: &Path, stage_key: &str) -> usize {
    read_stage_envelopes(run_dir, stage_key)
        .await
        .iter()
        .filter(|envelope| {
            matches!(
                &envelope.event.content,
                obzenflow_core::event::ChainEventContent::FlowControl(payload)
                    if payload.is_reader_telemetry()
            )
        })
        .count()
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
