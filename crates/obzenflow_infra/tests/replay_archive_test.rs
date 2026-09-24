// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use chrono::Utc;
use crc32fast::Hasher;
use obzenflow_core::build_info::OBZENFLOW_VERSION;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::provenance::JournalProvenance;
use obzenflow_core::event::types::DurationMs;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{
    ChainEvent, ChainEventFactory, JournalRecord, PipelineLifecycleEvent, SystemEvent,
    SystemPayload,
};
use obzenflow_core::id::{JournalId, SystemId};
use obzenflow_core::journal::archive::manifest::{
    RunManifest, RunManifestStage, EFFECT_BINDING_DESCRIPTOR_CAPABILITY, JOURNAL_SCHEMA_VERSION,
    RUN_MANIFEST_FILENAME,
};
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::journal::ArchiveStatus;
use obzenflow_core::Journal;
use obzenflow_core::{JournalWriterId, WriterId};
use obzenflow_infra::journal::disk::log_record::LogRecord;
use obzenflow_infra::journal::disk::replay_archive::DiskReplayArchive;
use obzenflow_infra::journal::DiskJournal;
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use tempfile::tempdir;

fn binding_descriptor_capabilities() -> BTreeMap<String, u32> {
    BTreeMap::from([
        (EFFECT_BINDING_DESCRIPTOR_CAPABILITY.to_string(), 1),
        (
            obzenflow_core::journal::archive::manifest::OBSERVABILITY_CAPTURE_CAPABILITY
                .to_string(),
            1,
        ),
    ])
}

fn write_manifest(dir: &Path) {
    let mut stages = HashMap::new();
    stages.insert(
        "returns".to_string(),
        RunManifestStage {
            dsl_var: "source".to_string(),
            stage_type: StageType::FiniteSource,
            is_effectful: Some(false),
            stage_id: "stage_01H000000000000000000000000".to_string(),
            stage_logic_version: "1".to_string(),
            data_journal_file: "FiniteSource_returns_stage_01H000000000000000000000000.log"
                .to_string(),
            error_journal_file: "FiniteSource_returns_error_stage_01H000000000000000000000000.log"
                .to_string(),
            inbound: Vec::new(),
            ordered_delivery: true,
        },
    );

    let manifest = RunManifest {
        journal_schema_version: JOURNAL_SCHEMA_VERSION.to_string(),

        obzenflow_version: OBZENFLOW_VERSION.to_string(),
        flow_id: "flow_01H000000000000000000000000".to_string(),
        pipeline_writer_id: SystemId::new().into(),
        flow_name: "test_flow".to_string(),
        created_at: Utc::now(),
        replay: None,
        resume: None,
        stages,
        system_journal_file: "system.log".to_string(),
        effective_config: None,
        capabilities: binding_descriptor_capabilities(),
        bounded_direct_fact_admission: Vec::new(),
    };

    let body = serde_json::to_string_pretty(&manifest).unwrap();
    std::fs::write(dir.join(RUN_MANIFEST_FILENAME), body).unwrap();
}

async fn write_system_log_completed(dir: &Path) {
    let writer_id = WriterId::from(SystemId::new());
    let event = SystemEvent::new(
        writer_id,
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Completed {
            duration_ms: DurationMs(1),
            metrics: obzenflow_core::metrics::FlowLifecycleMetricsSnapshot {
                events_in_total: 0,
                events_out_total: 0,
                errors_total: 0,
            },
        }),
    );

    let record = JournalRecord::commit_event(
        event,
        JournalProvenance {
            journal_writer_id: JournalWriterId::from(JournalId::new()),
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .expect("valid record");

    write_framed_log_record(dir, &record).await;
}

async fn write_framed_log_record(dir: &Path, record: &LogRecord<SystemEvent>) {
    let journal = DiskJournal::<SystemEvent>::with_owner(
        dir.join("system.log"),
        obzenflow_core::JournalOwner::system(SystemId::new()),
    )
    .unwrap();
    journal
        .append(record.authored(), Default::default())
        .await
        .unwrap();
}

#[test]
fn archive_fixture_helpers_gate_schema_before_manifest_decode_or_journal_access() {
    use obzenflow_infra::testing::journal::{
        audit_archive, corrupt_chain_frame, omit_observations, retain_archive_frames,
    };

    for complete in [false, true] {
        for (version, expected) in [
            (None, "<missing>"),
            (Some(serde_json::json!(5.0)), "5.0"),
            (Some(serde_json::json!("4.0")), "4.0"),
            (Some(serde_json::json!("7.0")), "7.0"),
            (Some(serde_json::json!({"major": 5})), r#"{"major":5}"#),
            (Some(serde_json::Value::Null), "null"),
        ] {
            let temp = tempdir().unwrap();
            write_manifest(temp.path());
            let path = temp.path().join(RUN_MANIFEST_FILENAME);
            let mut manifest: serde_json::Value = if complete {
                serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap()
            } else {
                // Deliberately incomplete: the raw epoch gate must run before
                // RunManifest deserialisation can complain about missing fields.
                serde_json::json!({"system_journal_file": "system.log"})
            };
            manifest
                .as_object_mut()
                .unwrap()
                .remove("journal_schema_version");
            if let Some(version) = version {
                manifest["journal_schema_version"] = version;
            }
            let manifest_bytes = serde_json::to_vec(&manifest).unwrap();
            std::fs::write(&path, &manifest_bytes).unwrap();
            let journal = temp.path().join("system.log");
            let sentinel = b"must not be decoded or rewritten";
            std::fs::write(&journal, sentinel).unwrap();

            for (helper, result) in [
                ("audit", audit_archive(temp.path()).map(|_| ())),
                (
                    "omit",
                    omit_observations(temp.path(), |_| panic!("observation predicate reached"))
                        .map(|_| ()),
                ),
                (
                    "retain",
                    retain_archive_frames(temp.path(), |_, _| panic!("frame predicate reached"))
                        .map(|_| ()),
                ),
                (
                    "corrupt",
                    corrupt_chain_frame(temp.path(), &journal, |_| {
                        panic!("corruption predicate reached")
                    }),
                ),
            ] {
                let error = result
                    .expect_err("non-current schema must be refused")
                    .to_string();
                assert!(
                    error.contains(&format!("unsupported journal schema version: {expected}")),
                    "{helper}: {error}"
                );
                assert_eq!(std::fs::read(&journal).unwrap(), sentinel);
                assert_eq!(std::fs::read(&path).unwrap(), manifest_bytes);
            }
        }
    }
}

#[tokio::test]
async fn archive_fixture_helpers_accept_current_schema() {
    use obzenflow_infra::testing::journal::{
        audit_archive, omit_observations, retain_archive_frames,
    };

    let temp = tempdir().unwrap();
    write_manifest(temp.path());
    write_system_log_completed(temp.path()).await;
    assert_eq!(audit_archive(temp.path()).unwrap().records, 1);
    assert_eq!(omit_observations(temp.path(), |_| true).unwrap(), 0);
    assert_eq!(audit_archive(temp.path()).unwrap().records, 1);
    assert_eq!(retain_archive_frames(temp.path(), |_, _| false).unwrap(), 1);
    assert_eq!(audit_archive(temp.path()).unwrap().records, 0);
}

fn write_released_legacy_retry_row(dir: &Path) {
    let writer_id = WriterId::from(obzenflow_core::StageId::new());
    let event = ChainEventFactory::data_event(writer_id, "fixture.seed", serde_json::json!({}));
    let record = JournalRecord::commit_event(
        event,
        JournalProvenance {
            journal_writer_id: JournalWriterId::from(JournalId::new()),
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .expect("valid record");
    let mut frame = serde_json::json!({
        "frame_kind": "record_v2",
        "record": record,
    });
    frame["record"]["event"]["content"] = serde_json::json!({
        "content_type": "lifecycle",
        "observability_type": "middleware",
        "middleware_event": "retry",
        "details": {
            "action": "exhausted",
            "total_attempts": 3,
            "last_error": "legacy failure",
            "total_duration_ms": 250
        }
    });

    assert!(
        serde_json::from_value::<ChainEvent>(frame["record"]["event"].clone()).is_err(),
        "the fixture must exercise the removed standalone retry vocabulary"
    );

    let json_body = serde_json::to_vec(&frame).unwrap();
    let mut hasher = Hasher::new();
    hasher.update(&json_body);
    let crc = hasher.finalize();
    let mut bytes = format!("{}:{}:", json_body.len(), crc).into_bytes();
    bytes.extend_from_slice(&json_body);
    bytes.push(b'\n');
    std::fs::write(
        dir.join("FiniteSource_returns_stage_01H000000000000000000000000.log"),
        bytes,
    )
    .unwrap();
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
async fn open_gates_required_capabilities_before_journal_decode() {
    for capability in [
        EFFECT_BINDING_DESCRIPTOR_CAPABILITY,
        obzenflow_core::journal::archive::manifest::OBSERVABILITY_CAPTURE_CAPABILITY,
    ] {
        for version in [None, Some(2_u64)] {
            let dir = tempdir().unwrap();
            write_manifest(dir.path());
            std::fs::write(dir.path().join("system.log"), b"not a journal frame").unwrap();

            let manifest_path = dir.path().join(RUN_MANIFEST_FILENAME);
            let mut manifest: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&manifest_path).unwrap()).unwrap();
            match version {
                Some(version) => {
                    manifest["capabilities"][capability] = serde_json::json!(version);
                }
                None => {
                    manifest["capabilities"]
                        .as_object_mut()
                        .unwrap()
                        .remove(capability);
                }
            }
            std::fs::write(
                &manifest_path,
                serde_json::to_vec_pretty(&manifest).unwrap(),
            )
            .unwrap();

            let error = DiskReplayArchive::open(dir.path().to_path_buf(), true)
                .await
                .err()
                .expect("missing or wrong required capability must refuse the archive");
            assert!(matches!(
                error,
                ReplayError::UnsupportedArchiveCapability {
                    capability: found_capability,
                    found,
                    supported: 1,
                } if found == version && found_capability == capability
            ));
        }
    }
}

#[tokio::test]
async fn open_requires_completed_status_by_default() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());

    let writer_id = WriterId::from(SystemId::new());
    let failed_event = SystemEvent::new(
        writer_id,
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Failed {
            reason: "boom".to_string(),
            duration_ms: DurationMs(1),
            metrics: None,
            failure_cause: None,
        }),
    );

    let record = JournalRecord::commit_event(
        failed_event,
        JournalProvenance {
            journal_writer_id: JournalWriterId::from(JournalId::new()),
            vector_clock: VectorClock::new(),
            timestamp: Utc::now(),
            journal_group_id: None,
            journal_group_member: None,
        },
    )
    .expect("valid record");

    write_framed_log_record(dir.path(), &record).await;

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, ReplayError::IncompleteArchive { .. }));
}

#[tokio::test]
async fn current_schema_accepts_different_package_versions_as_provenance() {
    for package in ["0.0.0", "99.12.34", "0.2.2", "development-build"] {
        let dir = tempdir().unwrap();
        write_manifest(dir.path());
        let path = dir.path().join(RUN_MANIFEST_FILENAME);
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["obzenflow_version"] = serde_json::json!(package);
        std::fs::write(path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        write_system_log_completed(dir.path()).await;
        DiskReplayArchive::open(dir.path().to_path_buf(), false)
            .await
            .unwrap_or_else(|error| panic!("package {package} is provenance only: {error}"));
    }
}

/// FLOWIP-122a: an archive written under the previous manifest version is refused
/// with the unsupported-version error, never a missing-field parse error. The raw
/// JSON deliberately lacks current fields, so this
/// only passes when the version gate runs before typed deserialization.
#[tokio::test]
async fn open_rejects_previous_journal_schema_version_before_typed_parse() {
    let dir = tempdir().unwrap();
    let old_manifest = serde_json::json!({
        "journal_schema_version": "2.0",
        "obzenflow_version": OBZENFLOW_VERSION,
        "flow_id": "flow_01H000000000000000000000000",
        "flow_name": "test_flow",
        "created_at": Utc::now(),
        "stages": {
            "returns": {
                "dsl_var": "source",
                "stage_type": "finite_source",
                "stage_id": "stage_01H000000000000000000000000",
                "stage_logic_version": "1",
                "data_journal_file": "FiniteSource_returns_stage_01H000000000000000000000000.log",
                "error_journal_file": "FiniteSource_returns_error_stage_01H000000000000000000000000.log"
            }
        },
        "system_journal_file": "system.log"
    });
    std::fs::write(
        dir.path().join(RUN_MANIFEST_FILENAME),
        serde_json::to_string_pretty(&old_manifest).unwrap(),
    )
    .unwrap();
    write_system_log_completed(dir.path()).await;

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(
            err,
            ReplayError::UnsupportedJournalSchemaVersion { ref journal_schema_version, .. }
                if journal_schema_version == "2.0"
        ),
        "expected UnsupportedJournalSchemaVersion for a 2.0 archive, got: {err}"
    );
    assert!(
        err.to_string().contains("re-record"),
        "refusal must carry the re-record guidance, got: {err}"
    );
}

#[tokio::test]
async fn open_rejects_every_non_current_manifest_shape_before_journal_access() {
    for (version, expected) in [
        (None, "<missing>"),
        (Some(serde_json::json!(3.0)), "3.0"),
        (Some(serde_json::json!("2.0")), "2.0"),
        (Some(serde_json::json!("7.0")), "7.0"),
    ] {
        let dir = tempdir().unwrap();
        let mut manifest = serde_json::json!({

            "system_journal_file": "sentinel-system.log"
        });
        if let Some(version) = version {
            manifest["journal_schema_version"] = version;
        }
        std::fs::write(
            dir.path().join(RUN_MANIFEST_FILENAME),
            serde_json::to_vec_pretty(&manifest).unwrap(),
        )
        .unwrap();
        std::fs::write(
            dir.path().join("sentinel-system.log"),
            b"this must never be scanned",
        )
        .unwrap();

        let err = DiskReplayArchive::open(dir.path().to_path_buf(), true)
            .await
            .err()
            .expect("non-current manifest must be refused");
        assert!(matches!(
            err,
            ReplayError::UnsupportedJournalSchemaVersion { ref journal_schema_version, supported }
                if journal_schema_version == expected && supported == JOURNAL_SCHEMA_VERSION
        ));
    }
}

#[tokio::test]
async fn open_rejects_previous_journal_format_before_parsing_legacy_retry_rows() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());
    write_system_log_completed(dir.path()).await;
    write_released_legacy_retry_row(dir.path());

    let manifest_path = dir.path().join(RUN_MANIFEST_FILENAME);
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
    manifest["journal_schema_version"] = serde_json::json!("4.0");
    std::fs::write(
        &manifest_path,
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .unwrap();

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(
        matches!(err, ReplayError::UnsupportedJournalSchemaVersion { .. }),
        "expected the raw manifest gate to reject the old journal format, got: {err}"
    );
    assert!(
        err.to_string()
            .contains("unsupported journal schema version"),
        "refusal must identify the incompatible record format, got: {err}"
    );
}

#[tokio::test]
async fn open_source_reader_errors_when_journal_missing() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());
    write_system_log_completed(dir.path()).await;

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

/// Schema 5 manifests cannot relabel schema 4 frames, even as torn tails.
#[tokio::test]
async fn manifest_frame_mismatches_refuse_replay_inspect_export_and_verify() {
    use obzenflow_infra::journal::disk::inspect::{export_jsonl, inspect};
    use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions};
    for system in [false, true] {
        for allow_incomplete in [false, true] {
            let dir = tempdir().unwrap();
            write_manifest(dir.path());
            write_system_log_completed(dir.path()).await;
            let path = if system {
                dir.path().join("system.log")
            } else {
                dir.path()
                    .join("FiniteSource_returns_stage_01H000000000000000000000000.log")
            };
            // An old partial frame after valid current frames is still a schema mismatch.
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path)
                .unwrap()
                .write_all(b"OJF4")
                .unwrap();
            let error = DiskReplayArchive::open(dir.path().to_path_buf(), allow_incomplete)
                .await
                .err()
                .unwrap();
            assert!(
                matches!(error, ReplayError::UnsupportedJournalSchemaVersion { .. }),
                "{error}"
            );
            assert!(inspect(dir.path(), None, None).is_err());
            assert!(export_jsonl(dir.path(), Some(&dir.path().join("projection.jsonl"))).is_err());
            let outcome = verify_run_dirs(dir.path(), dir.path(), &VerifyOptions::default());
            assert!(outcome.is_err() || outcome.unwrap().exit_code() != 0);
        }
    }
}
