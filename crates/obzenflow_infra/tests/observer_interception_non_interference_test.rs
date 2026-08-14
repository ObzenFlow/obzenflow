// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-115m Part 2: execute the shipped observer example's exact flow in
//! control, tracing, and panicking treatments, then compare authoritative
//! journals and strict replay.

#[path = "../../../examples/observer_interception_points/support.rs"]
mod fixture;

use fixture::{build_flow, Mode, Probe, ProbeSnapshot, ORDER_COUNT};
use obzenflow_core::event::{ChainEventContent, EventEnvelope};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::run_manifest::{RunManifest, RUN_MANIFEST_FILENAME};
use obzenflow_core::journal::{ArchiveStatus, Journal};
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_infra::journal::DiskJournal;
use obzenflow_infra::verify::{verify_run_dirs, Verdict, VerifyOptions};
use serde_json::{json, Value};
use std::ffi::OsString;
use std::path::{Path, PathBuf};

fn flow_dir(base: &Path) -> PathBuf {
    let mut flow_dirs: Vec<_> = std::fs::read_dir(base.join("flows"))
        .expect("flows directory exists")
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    flow_dirs.sort();
    assert_eq!(flow_dirs.len(), 1, "the treatment must produce one run");
    flow_dirs.pop().expect("one run directory")
}

fn manifest(run_dir: &Path) -> RunManifest {
    let path = run_dir.join(RUN_MANIFEST_FILENAME);
    let body = std::fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_str(&body).unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

async fn stage_events(run_dir: &Path, stage_key: &str) -> Vec<EventEnvelope<ChainEvent>> {
    let manifest = manifest(run_dir);
    let stage = manifest
        .stages
        .get(stage_key)
        .unwrap_or_else(|| panic!("manifest contains stage {stage_key:?}"));
    let journal = DiskJournal::with_owner(
        run_dir.join(&stage.data_journal_file),
        JournalOwner::stage(StageId::new()),
    )
    .unwrap_or_else(|error| panic!("open {stage_key} journal: {error}"));
    journal
        .read_causally_ordered()
        .await
        .unwrap_or_else(|error| panic!("read {stage_key} journal: {error}"))
}

async fn delivery_receipts(run_dir: &Path) -> Vec<Value> {
    stage_events(run_dir, "delivered")
        .await
        .into_iter()
        .filter_map(|envelope| match envelope.event.content {
            ChainEventContent::Delivery(payload) => {
                let mut value = serde_json::to_value(payload).expect("serialise delivery receipt");
                value
                    .as_object_mut()
                    .expect("delivery receipt is an object")
                    .remove("processed_at");
                Some(value)
            }
            _ => None,
        })
        .collect()
}

async fn effect_facts(run_dir: &Path) -> Vec<Value> {
    let stage_keys: Vec<_> = manifest(run_dir).stages.keys().cloned().collect();
    let mut facts = Vec::new();
    for stage_key in stage_keys {
        for envelope in stage_events(run_dir, &stage_key).await {
            if envelope.event.effect_provenance.is_none() {
                continue;
            }
            if let ChainEventContent::Data {
                event_type,
                payload,
            } = envelope.event.content
            {
                facts.push(json!({
                    "stage": stage_key,
                    "event_type": event_type,
                    "payload": payload,
                }));
            }
        }
    }
    facts
}

async fn terminal_status(run_dir: &Path) -> ArchiveStatus {
    DiskReplayArchive::open(run_dir.to_path_buf(), false)
        .await
        .unwrap_or_else(|error| panic!("open replay archive {}: {error}", run_dir.display()))
        .status()
}

fn assert_certified_match(baseline: &Path, candidate: &Path, label: &str) {
    let outcome = verify_run_dirs(
        baseline,
        candidate,
        &VerifyOptions {
            write_report: false,
            ..VerifyOptions::default()
        },
    )
    .unwrap_or_else(|error| panic!("{label} verification failed: {error}"));
    assert_eq!(
        outcome.verdict(),
        Verdict::CertifiedMatch,
        "{label} must preserve the supported normalised journal projection"
    );
}

struct CompletedRun {
    run_dir: PathBuf,
    snapshot: ProbeSnapshot,
}

async fn run_live(base: &Path, mode: Mode) -> CompletedRun {
    let journal_root = base.join(mode.label());
    let probe = Probe::default();
    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(journal_root.clone(), mode, probe.clone()))
        .await
        .unwrap_or_else(|error| panic!("{} treatment completes: {error}", mode.label()));
    CompletedRun {
        run_dir: flow_dir(&journal_root),
        snapshot: probe.snapshot(),
    }
}

async fn run_strict_replay(base: &Path, mode: Mode, recorded_run: &Path) -> CompletedRun {
    let journal_root = base.join(format!("{}-replay", mode.label()));
    let probe = Probe::default();
    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            recorded_run.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(journal_root.clone(), mode, probe.clone()))
        .await
        .unwrap_or_else(|error| panic!("{} strict replay completes: {error}", mode.label()));
    CompletedRun {
        run_dir: flow_dir(&journal_root),
        snapshot: probe.snapshot(),
    }
}

#[tokio::test]
async fn observer_interception_non_interference() {
    let temp = tempfile::tempdir().expect("create isolated verifier directory");
    let live_root = temp.path().join("live");
    let replay_root = temp.path().join("replay");

    let control = run_live(&live_root, Mode::Control).await;
    let trace = run_live(&live_root, Mode::Trace).await;
    let panic = run_live(&live_root, Mode::Panic).await;

    let expected_external_calls = (ORDER_COUNT + 1, ORDER_COUNT);
    for (label, snapshot) in [
        ("control", control.snapshot),
        ("trace", trace.snapshot),
        ("panic", panic.snapshot),
    ] {
        assert_eq!(
            (snapshot.source_polls, snapshot.sink_writes),
            expected_external_calls,
            "{label} must make the same source and sink calls"
        );
    }
    assert_eq!(
        (
            control.snapshot.delivery_callbacks,
            control.snapshot.lifecycle_callbacks,
            control.snapshot.panicking_callbacks,
        ),
        (0, 0, 0)
    );
    assert_eq!(
        (
            trace.snapshot.delivery_callbacks,
            trace.snapshot.lifecycle_callbacks,
            trace.snapshot.panicking_callbacks,
        ),
        (ORDER_COUNT, 2, 0),
        "trace treatment must observe every delivery and both sink lifecycle phases"
    );
    assert_eq!(
        (
            panic.snapshot.delivery_callbacks,
            panic.snapshot.lifecycle_callbacks,
            panic.snapshot.panicking_callbacks,
        ),
        (ORDER_COUNT, 2, 1),
        "the panicking attachment is quarantined while its siblings continue"
    );

    assert_certified_match(&control.run_dir, &trace.run_dir, "control versus trace");
    assert_certified_match(&control.run_dir, &panic.run_dir, "control versus panic");

    let control_receipts = delivery_receipts(&control.run_dir).await;
    assert_eq!(control_receipts.len(), ORDER_COUNT);
    assert_eq!(delivery_receipts(&trace.run_dir).await, control_receipts);
    assert_eq!(delivery_receipts(&panic.run_dir).await, control_receipts);

    let control_effect_facts = effect_facts(&control.run_dir).await;
    assert!(
        control_effect_facts.is_empty(),
        "the focused sink example performs no effects"
    );
    assert_eq!(effect_facts(&trace.run_dir).await, control_effect_facts);
    assert_eq!(effect_facts(&panic.run_dir).await, control_effect_facts);

    assert_eq!(
        terminal_status(&control.run_dir).await,
        ArchiveStatus::Completed
    );
    assert_eq!(
        terminal_status(&trace.run_dir).await,
        ArchiveStatus::Completed
    );
    assert_eq!(
        terminal_status(&panic.run_dir).await,
        ArchiveStatus::Completed
    );

    for (mode, recorded) in [
        (Mode::Control, &control.run_dir),
        (Mode::Trace, &trace.run_dir),
        (Mode::Panic, &panic.run_dir),
    ] {
        let replay = run_strict_replay(&replay_root, mode, recorded).await;
        assert_eq!(
            replay.snapshot,
            ProbeSnapshot {
                source_polls: 0,
                sink_writes: ORDER_COUNT,
                delivery_callbacks: 0,
                lifecycle_callbacks: 0,
                panicking_callbacks: 0,
            },
            "{} strict replay must suppress source polling and every observer callback while re-driving the idempotent sink identically",
            mode.label()
        );
        assert_eq!(
            terminal_status(&replay.run_dir).await,
            ArchiveStatus::Completed
        );
        assert_certified_match(
            recorded,
            &replay.run_dir,
            &format!("{} live versus strict replay", mode.label()),
        );
        assert_eq!(delivery_receipts(&replay.run_dir).await, control_receipts);
        assert_eq!(effect_facts(&replay.run_dir).await, control_effect_facts);
    }
}
