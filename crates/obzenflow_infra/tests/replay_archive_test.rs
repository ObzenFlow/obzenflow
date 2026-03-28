// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use chrono::Utc;
use crc32fast::Hasher;
use obzenflow_core::build_info::OBZENFLOW_VERSION;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::types::DurationMs;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{PipelineLifecycleEvent, SystemEvent, SystemEventType};
use obzenflow_core::id::{JournalId, SystemId};
use obzenflow_core::journal::run_manifest::{
    RunManifest, RunManifestStage, RUN_MANIFEST_FILENAME, RUN_MANIFEST_VERSION,
};
use obzenflow_core::journal::ArchiveStatus;
use obzenflow_core::WriterId;
use obzenflow_infra::journal::disk::log_record::LogRecord;
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_runtime::replay::ReplayArchive;
use obzenflow_runtime::replay::ReplayError;
use std::collections::HashMap;
use std::path::Path;
use tempfile::tempdir;
use ulid::Ulid;

fn write_manifest(dir: &Path) {
    let mut stages = HashMap::new();
    stages.insert(
        "returns".to_string(),
        RunManifestStage {
            dsl_var: "source".to_string(),
            stage_type: StageType::FiniteSource,
            stage_id: "stage_01H000000000000000000000000".to_string(),
            data_journal_file: "FiniteSource_returns_stage_01H000000000000000000000000.log"
                .to_string(),
            error_journal_file: "FiniteSource_returns_error_stage_01H000000000000000000000000.log"
                .to_string(),
        },
    );

    let manifest = RunManifest {
        manifest_version: RUN_MANIFEST_VERSION.to_string(),
        obzenflow_version: OBZENFLOW_VERSION.to_string(),
        flow_id: "flow_01H000000000000000000000000".to_string(),
        flow_name: "test_flow".to_string(),
        created_at: Utc::now(),
        replay: None,
        stages,
        system_journal_file: "system.log".to_string(),
    };

    let body = serde_json::to_string_pretty(&manifest).unwrap();
    std::fs::write(dir.join(RUN_MANIFEST_FILENAME), body).unwrap();
}

fn write_system_log_completed(dir: &Path) {
    let writer_id = WriterId::from(SystemId::new());
    let event = SystemEvent::new(
        writer_id,
        SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Completed {
            duration_ms: DurationMs(1),
            metrics: obzenflow_core::metrics::FlowLifecycleMetricsSnapshot {
                events_in_total: 0,
                events_out_total: 0,
                errors_total: 0,
            },
        }),
    );

    let record = LogRecord {
        event_id: Ulid::new(),
        writer_id: event.writer_id,
        journal_id: JournalId::new(),
        vector_clock: VectorClock::new(),
        timestamp: Utc::now(),
        event,
    };

    write_framed_log_record(dir, &record);
}

fn write_framed_log_record(dir: &Path, record: &LogRecord<SystemEvent>) {
    let json_body = serde_json::to_vec(record).unwrap();
    let mut hasher = Hasher::new();
    hasher.update(&json_body);
    let crc = hasher.finalize();

    let mut bytes = format!("{}:{}:", json_body.len(), crc).into_bytes();
    bytes.extend_from_slice(&json_body);
    bytes.push(b'\n');

    std::fs::write(dir.join("system.log"), bytes).unwrap();
}

#[tokio::test]
async fn open_fails_when_manifest_missing() {
    let dir = tempdir().unwrap();
    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, ReplayError::MissingManifest { .. }));
}

#[tokio::test]
async fn open_fails_when_system_log_missing_unless_allowed() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, ReplayError::MissingSystemLog { .. }));

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), true)
        .await
        .unwrap();
    assert_eq!(archive.status(), ArchiveStatus::Unknown);
}

#[tokio::test]
async fn open_requires_completed_status_by_default() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());

    let writer_id = WriterId::from(SystemId::new());
    let failed_event = SystemEvent::new(
        writer_id,
        SystemEventType::PipelineLifecycle(PipelineLifecycleEvent::Failed {
            reason: "boom".to_string(),
            duration_ms: DurationMs(1),
            metrics: None,
            failure_cause: None,
        }),
    );

    let record = LogRecord {
        event_id: Ulid::new(),
        writer_id: failed_event.writer_id,
        journal_id: JournalId::new(),
        vector_clock: VectorClock::new(),
        timestamp: Utc::now(),
        event: failed_event,
    };

    write_framed_log_record(dir.path(), &record);

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, ReplayError::IncompleteArchive { .. }));
}

/// Write a manifest with a custom `obzenflow_version` for version-compat tests.
fn write_manifest_with_version(dir: &Path, version: &str) {
    let mut stages = HashMap::new();
    stages.insert(
        "returns".to_string(),
        RunManifestStage {
            dsl_var: "source".to_string(),
            stage_type: StageType::FiniteSource,
            stage_id: "stage_01H000000000000000000000000".to_string(),
            data_journal_file: "FiniteSource_returns_stage_01H000000000000000000000000.log"
                .to_string(),
            error_journal_file: "FiniteSource_returns_error_stage_01H000000000000000000000000.log"
                .to_string(),
        },
    );

    let manifest = RunManifest {
        manifest_version: RUN_MANIFEST_VERSION.to_string(),
        obzenflow_version: version.to_string(),
        flow_id: "flow_01H000000000000000000000000".to_string(),
        flow_name: "test_flow".to_string(),
        created_at: Utc::now(),
        replay: None,
        stages,
        system_journal_file: "system.log".to_string(),
    };

    let body = serde_json::to_string_pretty(&manifest).unwrap();
    std::fs::write(dir.join(RUN_MANIFEST_FILENAME), body).unwrap();
}

#[tokio::test]
async fn open_rejects_archive_from_newer_minor_version() {
    let dir = tempdir().unwrap();
    // Bump the minor version beyond what the framework reports.
    let (major, minor, _patch) = {
        let parts: Vec<u64> = OBZENFLOW_VERSION
            .split('.')
            .map(|p| p.parse().unwrap())
            .collect();
        (parts[0], parts[1], parts[2])
    };
    let future_version = format!("{}.{}.0", major, minor + 1);
    write_manifest_with_version(dir.path(), &future_version);
    write_system_log_completed(dir.path());

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, ReplayError::VersionMismatch { .. }),
        "expected VersionMismatch for newer minor, got: {err}"
    );
}

#[tokio::test]
async fn open_rejects_archive_from_older_minor_version() {
    let dir = tempdir().unwrap();
    let (major, minor, _patch) = {
        let parts: Vec<u64> = OBZENFLOW_VERSION
            .split('.')
            .map(|p| p.parse().unwrap())
            .collect();
        (parts[0], parts[1], parts[2])
    };

    // Only meaningful if minor > 0; otherwise skip gracefully.
    if minor == 0 {
        // Bump major instead to guarantee a mismatch.
        let old_version = format!("{}.999.0", major.saturating_sub(1));
        write_manifest_with_version(dir.path(), &old_version);
    } else {
        let old_version = format!("{}.{}.0", major, minor - 1);
        write_manifest_with_version(dir.path(), &old_version);
    }
    write_system_log_completed(dir.path());

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, ReplayError::VersionMismatch { .. }),
        "expected VersionMismatch for older minor, got: {err}"
    );
}

#[tokio::test]
async fn open_accepts_archive_with_different_patch_version() {
    let dir = tempdir().unwrap();
    let (major, minor, patch) = {
        let parts: Vec<u64> = OBZENFLOW_VERSION
            .split('.')
            .map(|p| p.parse().unwrap())
            .collect();
        (parts[0], parts[1], parts[2])
    };
    // Use a different patch version (if current is 0, use 99; otherwise use 0).
    let alt_patch = if patch == 0 { 99 } else { 0 };
    let compat_version = format!("{}.{}.{}", major, minor, alt_patch);
    write_manifest_with_version(dir.path(), &compat_version);
    write_system_log_completed(dir.path());

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .expect("same major.minor with different patch should be accepted");
    assert_eq!(archive.status(), ArchiveStatus::Completed);
}

#[tokio::test]
async fn open_rejects_archive_with_unparseable_version() {
    let dir = tempdir().unwrap();
    write_manifest_with_version(dir.path(), "not-a-version");
    write_system_log_completed(dir.path());

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, ReplayError::VersionMismatch { .. }),
        "unparseable version should fail closed as VersionMismatch, got: {err}"
    );
}

#[tokio::test]
async fn open_source_reader_errors_when_journal_missing() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());
    write_system_log_completed(dir.path());

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .unwrap();

    let err = archive
        .open_source_reader("returns", StageType::FiniteSource)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, ReplayError::MissingJournal { .. }));
}
