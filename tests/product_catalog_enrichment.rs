// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../examples/product_catalog_enrichment/support.rs"]
mod product_catalog_enrichment;

#[test]
#[ignore = "long-running end-to-end flow; enable when validating example behavior"]
fn product_catalog_enrichment_completes() {
    std::env::remove_var("INJECT_BAD_PAYMENT");
    product_catalog_enrichment::flow::run_example_in_tests()
        .expect("product_catalog_enrichment example should complete");
}

use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{ChainEvent, ChainPayload};
use obzenflow_core::journal::{RunManifest, JOURNAL_SCHEMA_VERSION};
use obzenflow_core::{Journal, JournalOwner, StageId, WriterId};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::DiskJournal;
use product_catalog_enrichment::flow::{build_for_proof, ProofProbe};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

fn recorded_run(root: &Path) -> PathBuf {
    let mut runs: Vec<_> = std::fs::read_dir(root.join("flows"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.join("run_manifest.json").is_file())
        .collect();
    runs.sort();
    assert_eq!(runs.len(), 1, "each proof run has an isolated output root");
    runs.pop().unwrap()
}

type Projection = BTreeMap<String, (Vec<serde_json::Value>, Vec<EofKind>)>;

async fn projection(run: &Path) -> Projection {
    let raw: serde_json::Value =
        serde_json::from_slice(&std::fs::read(run.join("run_manifest.json")).unwrap()).unwrap();
    assert_eq!(raw["journal_schema_version"], JOURNAL_SCHEMA_VERSION);
    assert!(raw.get("manifest_version").is_none());
    assert!(raw.get("journal_format_version").is_none());
    let manifest: RunManifest = serde_json::from_value(raw).unwrap();
    let mut result = BTreeMap::new();
    for (name, stage) in manifest.stages {
        let id: StageId = stage.stage_id.parse().unwrap();
        let journal = DiskJournal::<ChainEvent>::with_owner(
            run.join(stage.data_journal_file),
            JournalOwner::stage(id),
        )
        .unwrap();
        let mut reader = journal.reader().await.unwrap();
        let mut facts = Vec::new();
        let mut terminals = Vec::new();
        while let Some(record) = reader.next().await.unwrap() {
            match &record.payload {
                ChainPayload::Fact(value) => facts.push(value.clone()),
                ChainPayload::FlowControl(FlowControlPayload::Eof { kind, .. })
                    if record.envelope.provenance.event.writer_id == WriterId::from(id) =>
                {
                    terminals.push(*kind)
                }
                _ => {}
            }
        }
        result.insert(name, (facts, terminals));
    }
    result
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "current-schema live/replay journal proof for the rate-limited shipped example"]
async fn explicit_join_topology_current_schema_live_replay_and_rejected_control() {
    std::env::remove_var("INJECT_BAD_PAYMENT");
    let temp = tempfile::tempdir().unwrap();
    let control_root = temp.path().join("control");
    let control = ProofProbe {
        invalid_wiring: true,
        ..Default::default()
    };
    let error = FlowApplication::builder()
        .with_cli_args([OsString::from("catalog-proof")])
        .run_async(build_for_proof(control_root.clone(), control.clone()))
        .await
        .err()
        .unwrap();
    assert!(format!("{error:?}").contains("enriched_orders"), "{error}");
    assert_eq!(control.source_reads.load(Ordering::SeqCst), 0);
    assert_eq!(control.journal_providers.load(Ordering::SeqCst), 0);
    assert!(
        !control_root.exists(),
        "invalid topology must not create journals"
    );

    let live_root = temp.path().join("live");
    let live_probe = ProofProbe::default();
    FlowApplication::builder()
        .with_cli_args([OsString::from("catalog-proof")])
        .run_async(build_for_proof(live_root.clone(), live_probe.clone()))
        .await
        .unwrap();
    assert!(live_probe.source_reads.load(Ordering::SeqCst) > 0);
    assert_eq!(live_probe.journal_providers.load(Ordering::SeqCst), 1);
    let live = recorded_run(&live_root);
    let live_projection = projection(&live).await;
    for join in [
        "sku_products",
        "sku_full_dim",
        "payment_validated",
        "enriched_orders",
        "promo_enriched",
    ] {
        assert_eq!(
            live_projection[join].1,
            [EofKind::Natural],
            "{join} commits exactly one natural terminal"
        );
    }
    assert_eq!(live_projection["sku_full_dim"].0.len(), 6);
    let orders = &live_projection["promo_enriched"].0;
    assert_eq!(orders.len(), 5);
    assert_eq!(
        orders
            .iter()
            .filter(|order| !order["promo_code"].is_null())
            .count(),
        2
    );
    assert_eq!(live_projection["catalog_stats"].0[0]["order_count"], 5);
    assert_eq!(live_projection["catalog_stats"].0[0]["promo_orders"], 2);

    let replay_root = temp.path().join("replay");
    let replay_probe = ProofProbe::default();
    FlowApplication::builder()
        .with_cli_args([
            OsString::from("catalog-proof"),
            OsString::from("--replay-from"),
            live.into_os_string(),
            OsString::from("--verify"),
        ])
        .run_async(build_for_proof(replay_root.clone(), replay_probe.clone()))
        .await
        .unwrap();
    assert_eq!(
        replay_probe.source_reads.load(Ordering::SeqCst),
        0,
        "replay must never poll a live source"
    );
    assert_eq!(
        projection(&recorded_run(&replay_root)).await,
        live_projection,
        "stable stage identities, authored facts, and local terminal kinds must agree"
    );
}
