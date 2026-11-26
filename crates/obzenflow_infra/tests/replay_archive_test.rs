// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use chrono::Utc;
use crc32fast::Hasher;
use obzenflow_core::build_info::OBZENFLOW_VERSION;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::types::DurationMs;
use obzenflow_core::event::{
    ChainEvent, ChainEventFactory, JournalRecord, PipelineLifecycleEvent, SystemEvent,
    SystemPayload,
};
use obzenflow_core::id::SystemId;
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
use obzenflow_runtime::runtime_config::JournalSync;
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
    write_manifest_of(dir, FLOW_ID, None);
}

const FLOW_ID: &str = "flow_01H00000000000000000000000";
const SOURCE_LOG: &str = "FiniteSource_returns_stage_01H00000000000000000000000.log";

fn write_manifest_of(
    dir: &Path,
    flow_id: &str,
    resume: Option<obzenflow_core::journal::archive::manifest::RunManifestResumeConfig>,
) {
    let mut stages = HashMap::new();
    stages.insert(
        "returns".to_string(),
        RunManifestStage {
            dsl_var: "source".to_string(),
            stage_type: StageType::FiniteSource,
            is_effectful: false,
            stage_id: "stage_01H00000000000000000000000".to_string(),
            stage_logic_version: "1".to_string(),
            data_journal_file: "FiniteSource_returns_stage_01H00000000000000000000000.log"
                .to_string(),
            error_journal_file: "FiniteSource_returns_error_stage_01H00000000000000000000000.log"
                .to_string(),
            inbound: Vec::new(),
            delivery_order: obzenflow_core::journal::archive::DeliveryOrder::SingleInput,
            ai_models: Vec::new(),
            max_open_keys: None,
        },
    );

    let manifest = RunManifest {
        metrics_journals: None,
        journal_schema_version: JOURNAL_SCHEMA_VERSION.to_string(),

        obzenflow_version: OBZENFLOW_VERSION.to_string(),
        flow_id: flow_id.to_string(),
        recorded_flow_id: None,
        pipeline_writer_id: SystemId::new().into(),
        flow_name: "test_flow".to_string(),
        created_at: Utc::now(),
        replay: None,
        resume,
        interrupted_appends: Vec::new(),
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
    let event = SystemEvent::new(writer_id, SystemPayload::PipelineLifecycle(completed()));

    let record = JournalRecord::new(JournalWriterId::new(), event);

    write_framed_log_record(dir, &record).await;
}

async fn write_framed_log_record(dir: &Path, record: &LogRecord<SystemEvent>) {
    let journal = DiskJournal::<SystemEvent>::with_owner_in_run(
        dir.join("system.log"),
        obzenflow_core::JournalOwner::system(SystemId::new()),
        "flow_01H00000000000000000000000".parse().unwrap(),
        JournalSync::BeforeAck,
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
        audit_archive, corrupt_chain_frame, omit_observations,
        omit_observations_and_export_verified, retain_archive_frames,
    };

    let (major, _) = JOURNAL_SCHEMA_VERSION.split_once('.').unwrap();
    let future_version = format!("{}.0", major.parse::<u64>().unwrap() + 1);
    let previous_version = format!("{}.0", major.parse::<u64>().unwrap() - 1);
    for complete in [false, true] {
        for (version, expected) in [
            (None, "<missing>"),
            (Some(serde_json::json!(5.0)), "5.0"),
            (Some(serde_json::json!("4.0")), "4.0"),
            (
                Some(serde_json::json!(previous_version)),
                previous_version.as_str(),
            ),
            (
                Some(serde_json::json!(future_version)),
                future_version.as_str(),
            ),
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
            let output = temp.path().join("must-not-exist.jsonl");

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
                    "omit-and-export",
                    omit_observations_and_export_verified(
                        temp.path(),
                        &Default::default(),
                        &[],
                        &output,
                    )
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
                assert!(!output.exists());
            }
        }
    }
}

#[tokio::test]
async fn archive_fixture_helpers_accept_current_schema() {
    use obzenflow_infra::testing::journal::{
        audit_archive, omit_observations, omit_observations_and_export_verified,
        retain_archive_frames,
    };

    let temp = tempdir().unwrap();
    write_manifest(temp.path());
    write_system_log_completed(temp.path()).await;
    assert_eq!(audit_archive(temp.path()).unwrap().records, 1);
    assert_eq!(omit_observations(temp.path(), |_| true).unwrap(), 0);
    assert_eq!(audit_archive(temp.path()).unwrap().records, 1);
    let output = temp.path().join("current.jsonl");
    obzenflow_infra::journal::disk::inspect::export_jsonl(temp.path(), Some(&output)).unwrap();
    let expected: Vec<serde_json::Value> = std::fs::read_to_string(&output)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        omit_observations_and_export_verified(
            temp.path(),
            &Default::default(),
            &expected,
            &output,
        )
        .unwrap(),
        0
    );
    assert_eq!(retain_archive_frames(temp.path(), |_, _| false).unwrap(), 1);
    assert_eq!(audit_archive(temp.path()).unwrap().records, 0);
}

fn write_released_legacy_retry_row(dir: &Path) {
    let writer_id = WriterId::from(obzenflow_core::StageId::new());
    let event = ChainEventFactory::data_event(
        writer_id,
        "fixture.seed",
        std::num::NonZeroU32::MIN,
        serde_json::json!({}),
    );
    let record = JournalRecord::new(JournalWriterId::new(), event);
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
        dir.join("FiniteSource_returns_stage_01H00000000000000000000000.log"),
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
async fn archive_missing_system_log_requires_flag() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());

    let err = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .err()
        .unwrap();
    assert!(matches!(err, ReplayError::MissingSystemLog { .. }));
    let system_log = std::fs::canonicalize(dir.path())
        .unwrap()
        .join("system.log");
    assert_eq!(
        err.to_string(),
        format!(
            "Replay archive system.log missing at {}.\nRestore it from a copy of the run \
             directory, or pass --allow-incomplete-archive to open the run as interrupted from \
             its stage journals.",
            system_log.display()
        )
    );

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), true)
        .await
        .unwrap();
    assert_eq!(archive.status(), ArchiveStatus::Unknown);
}

#[tokio::test]
async fn damaged_system_log_with_flag_refused() {
    let dir = tempdir().unwrap();
    write_run(
        dir.path(),
        FLOW_ID,
        None,
        &[source_fact(1)],
        Some(completed()),
    )
    .await;
    flip_first_record_byte(&dir.path().join("system.log"));

    for allow_incomplete_archive in [false, true] {
        let Err(error) =
            DiskReplayArchive::open(dir.path().to_path_buf(), allow_incomplete_archive).await
        else {
            panic!("a damaged system.log opens with allow_incomplete_archive={allow_incomplete_archive}");
        };
        assert!(
            matches!(&error, ReplayError::Parse { message }
                if message.starts_with("system.log record corrupt at line 1 in ")),
            "{error}"
        );
    }
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
async fn open_reports_a_failed_status_for_core_to_refuse() {
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

    let record = JournalRecord::new(JournalWriterId::new(), failed_event);

    write_framed_log_record(dir.path(), &record).await;

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .unwrap();
    let evidence = archive.evidence();
    assert_eq!(evidence.status, ArchiveStatus::Failed);
    assert!(!evidence.live_writer);
    use obzenflow_core::journal::archive_actions::{
        decide, ArchiveOverrides, ArchiveRefusal, FlowEvidence, ReplayVerb,
    };
    assert!(matches!(
        decide(
            ReplayVerb::Replay,
            dir.path(),
            &evidence,
            &FlowEvidence::default(),
            ArchiveOverrides::default(),
        ),
        Err(ArchiveRefusal::Failed { .. })
    ));
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
        "flow_id": "flow_01H00000000000000000000000",
        "flow_name": "test_flow",
        "created_at": Utc::now(),
        "stages": {
            "returns": {
                "dsl_var": "source",
                "stage_type": "finite_source",
                "stage_id": "stage_01H00000000000000000000000",
                "stage_logic_version": "1",
                "data_journal_file": "FiniteSource_returns_stage_01H00000000000000000000000.log",
                "error_journal_file": "FiniteSource_returns_error_stage_01H00000000000000000000000.log"
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
async fn current_manifest_requires_stage_capability_before_journal_access() {
    for (value, expected) in [
        (None, "missing field `is_effectful`"),
        (Some(serde_json::Value::Null), "expected a boolean"),
    ] {
        let dir = tempdir().unwrap();
        write_manifest(dir.path());
        let manifest_path = dir.path().join(RUN_MANIFEST_FILENAME);
        let mut manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
        let stage = manifest["stages"]["returns"].as_object_mut().unwrap();
        stage.remove("is_effectful");
        if let Some(value) = value {
            stage.insert("is_effectful".into(), value);
        }
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

        // No journal files exist: both readers must reject the manifest before
        // trying to recover metadata from journal records, even in incomplete mode.
        let snapshot_error = obzenflow_infra::journal::read::open_disk_run(dir.path())
            .await
            .err()
            .expect("missing capability must fail snapshot admission");
        assert!(
            snapshot_error.to_string().contains(expected),
            "{snapshot_error}"
        );
        for allow_incomplete in [false, true] {
            let replay_error = DiskReplayArchive::open(dir.path().to_path_buf(), allow_incomplete)
                .await
                .err()
                .expect("missing capability must fail replay admission");
            assert!(
                replay_error.to_string().contains(expected),
                "{replay_error}"
            );
        }
    }
}

#[tokio::test]
async fn open_rejects_every_non_current_manifest_shape_before_journal_access() {
    let (major, _) = JOURNAL_SCHEMA_VERSION.split_once('.').unwrap();
    let future_version = format!("{}.0", major.parse::<u64>().unwrap() + 1);
    let previous_version = format!("{}.0", major.parse::<u64>().unwrap() - 1);
    for (version, expected) in [
        (None, "<missing>"),
        (Some(serde_json::json!(3.0)), "3.0"),
        (Some(serde_json::json!("2.0")), "2.0"),
        (
            Some(serde_json::json!(previous_version)),
            previous_version.as_str(),
        ),
        (
            Some(serde_json::json!(future_version)),
            future_version.as_str(),
        ),
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

/// Current manifests cannot relabel schema 4 frames, even as torn tails.
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
                    .join("FiniteSource_returns_stage_01H00000000000000000000000.log")
            };
            if !system {
                // Isolate the frame-epoch refusal behind a valid empty identity.
                drop(
                    DiskJournal::<ChainEvent>::with_owner_in_run(
                        path.clone(),
                        obzenflow_core::JournalOwner::stage(obzenflow_core::StageId::new()),
                        obzenflow_infra::journal::disk::identity::read_identity(
                            &dir.path().join("system.log"),
                        )
                        .unwrap()
                        .run_id,
                        JournalSync::BeforeAck,
                    )
                    .unwrap(),
                );
            }
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

async fn write_stopped_run(
    dir: &Path,
    flow_id: &str,
    resume: Option<obzenflow_core::journal::archive::manifest::RunManifestResumeConfig>,
    facts: &[ChainEvent],
) {
    write_run(dir, flow_id, resume, facts, None).await;
}

async fn write_run(
    dir: &Path,
    flow_id: &str,
    resume: Option<obzenflow_core::journal::archive::manifest::RunManifestResumeConfig>,
    facts: &[ChainEvent],
    ending: Option<PipelineLifecycleEvent>,
) {
    write_manifest_of(dir, flow_id, resume);
    let run: obzenflow_core::FlowId = flow_id.parse().unwrap();
    let system_id = SystemId::new();
    let system = DiskJournal::<SystemEvent>::with_owner_in_run(
        dir.join("system.log"),
        obzenflow_core::JournalOwner::system(system_id),
        run,
        JournalSync::BeforeAck,
    )
    .unwrap();
    system
        .append(
            SystemEvent::new(
                system_id.into(),
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
                    stage_count: Some(1),
                }),
            ),
            Default::default(),
        )
        .await
        .unwrap();
    if let Some(ending) = ending {
        system
            .append(
                SystemEvent::new(system_id.into(), SystemPayload::PipelineLifecycle(ending)),
                Default::default(),
            )
            .await
            .unwrap();
    }
    let stage: obzenflow_core::StageId = "stage_01H00000000000000000000000".parse().unwrap();
    let source = DiskJournal::<ChainEvent>::with_owner_in_run(
        dir.join(SOURCE_LOG),
        obzenflow_core::JournalOwner::stage(stage),
        run,
        JournalSync::BeforeAck,
    )
    .unwrap();
    for fact in facts {
        source
            .append(fact.clone(), Default::default())
            .await
            .unwrap();
    }
}

fn source_fact(row: u64) -> ChainEvent {
    ChainEventFactory::data_event(
        WriterId::from(
            "stage_01H00000000000000000000000"
                .parse::<obzenflow_core::StageId>()
                .unwrap(),
        ),
        "returns.row",
        std::num::NonZeroU32::MIN,
        serde_json::json!({ "row": row }),
    )
}

fn source_eof() -> ChainEvent {
    ChainEventFactory::eof_event(
        WriterId::from(
            "stage_01H00000000000000000000000"
                .parse::<obzenflow_core::StageId>()
                .unwrap(),
        ),
        true,
    )
}

fn completed() -> PipelineLifecycleEvent {
    PipelineLifecycleEvent::Completed {
        duration_ms: DurationMs(1),
        metrics: obzenflow_core::metrics::FlowLifecycleMetricsSnapshot {
            events_in_total: 0,
            events_out_total: 0,
            errors_total: 0,
        },
    }
}

fn cancelled() -> PipelineLifecycleEvent {
    PipelineLifecycleEvent::Cancelled {
        reason: "stopped by the operator".to_string(),
        duration_ms: DurationMs(1),
        metrics: None,
        failure_cause: None,
    }
}

fn flip_first_record_byte(journal: &Path) {
    let mut bytes = std::fs::read(journal).unwrap();
    bytes[40] ^= 0xff;
    std::fs::write(journal, &bytes).unwrap();
}

fn record_replay_of(dir: &Path, original: &Path) {
    let path = dir.join(RUN_MANIFEST_FILENAME);
    let mut manifest: RunManifest =
        serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    manifest.replay = Some(
        obzenflow_core::journal::archive::manifest::RunManifestReplayConfig {
            replay_from: original.display().to_string(),
            allow_incomplete_archive: false,
        },
    );
    std::fs::write(&path, serde_json::to_string_pretty(&manifest).unwrap()).unwrap();
}

async fn write_claimed_resume(
    base: &Path,
    parent_facts: &[ChainEvent],
    caught_up: usize,
    parent_ending: Option<PipelineLifecycleEvent>,
    resume_ending: Option<PipelineLifecycleEvent>,
) -> (std::path::PathBuf, std::path::PathBuf) {
    const RESUMED_ID: &str = "flow_01H00000000000000000000001";
    let (parent, resumed) = (base.join("r0"), base.join("r1"));
    std::fs::create_dir_all(&parent).unwrap();
    std::fs::create_dir_all(&resumed).unwrap();
    write_run(&parent, FLOW_ID, None, parent_facts, parent_ending).await;
    let parent = std::fs::canonicalize(&parent).unwrap();
    write_run(
        &resumed,
        RESUMED_ID,
        Some(
            obzenflow_core::journal::archive::manifest::RunManifestResumeConfig {
                resumed_from: parent.clone(),
                resume_generation: 1,
                merged_from: Vec::new(),
            },
        ),
        &parent_facts[..caught_up],
        resume_ending,
    )
    .await;
    let resumed = std::fs::canonicalize(&resumed).unwrap();
    DiskReplayArchive::open_for_resume(parent.clone(), false)
        .await
        .unwrap()
        .claim_continuation(
            RESUMED_ID.parse().unwrap(),
            &obzenflow_core::journal::factory::CurrentRunLocator::new(resumed.clone()),
        )
        .unwrap();
    (parent, resumed)
}

fn append_tail(journal: &Path, tail: &[u8]) -> u64 {
    let committed_end = std::fs::metadata(journal).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(journal)
        .unwrap();
    std::io::Write::write_all(&mut file, tail).unwrap();
    committed_end
}

fn archive_bytes(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .map(|path| {
            (
                path.file_name().unwrap().to_string_lossy().into_owned(),
                std::fs::read(&path).unwrap(),
            )
        })
        .collect()
}

#[tokio::test]
async fn admission_zero_tail_records_interrupted_append() {
    use obzenflow_core::journal::{RunManifestInterruptedAppend, TailKind};
    let dir = tempdir().unwrap();
    write_stopped_run(dir.path(), FLOW_ID, None, &[source_fact(1)]).await;
    let committed_end = append_tail(&dir.path().join("system.log"), &[0; 4096]);
    let before = archive_bytes(dir.path());

    let archive = DiskReplayArchive::open_for_resume(dir.path().to_path_buf(), false)
        .await
        .expect("an interrupted append is accepted");
    assert_eq!(
        archive.interrupted_appends(),
        [RunManifestInterruptedAppend {
            ancestor: None,
            journal: "system.log".into(),
            committed_end,
            left_bytes: 4096,
            tail_kind: TailKind::ZeroFilled,
        }]
    );
    assert_eq!(archive.archive_status(), ArchiveStatus::Unknown);
    assert_eq!(
        archive_bytes(dir.path()),
        before,
        "admission moves no bytes"
    );
}

#[tokio::test]
async fn admission_damaged_frame_records_torn_frame() {
    use obzenflow_core::journal::{RunManifestInterruptedAppend, TailKind};
    let dir = tempdir().unwrap();
    write_stopped_run(dir.path(), FLOW_ID, None, &[source_fact(1)]).await;
    let source = dir.path().join(SOURCE_LOG);
    let mut torn = std::fs::read(&source).unwrap();
    torn.truncate(torn.len() / 2);
    let committed_end = append_tail(&source, &torn);

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .unwrap();
    assert_eq!(
        archive.interrupted_appends(),
        [RunManifestInterruptedAppend {
            ancestor: None,
            journal: SOURCE_LOG.into(),
            committed_end,
            left_bytes: torn.len() as u64,
            tail_kind: TailKind::TornFrame,
        }]
    );
}

#[tokio::test]
async fn admission_damage_before_frame_records_nothing() {
    let dir = tempdir().unwrap();
    write_stopped_run(dir.path(), FLOW_ID, None, &[source_fact(1), source_fact(2)]).await;
    let source = dir.path().join(SOURCE_LOG);
    let mut bytes = std::fs::read(&source).unwrap();
    bytes[40] ^= 0xff;
    std::fs::write(&source, &bytes).unwrap();

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .expect("admission refuses nothing it accepted before");
    assert!(archive.interrupted_appends().is_empty());
    let mut reader = archive
        .open_source_reader("returns", StageType::FiniteSource)
        .await
        .unwrap();
    let Err(error) = reader.next().await else {
        panic!("the read path refuses damage that a valid frame follows");
    };
    assert!(
        error
            .to_string()
            .contains("a valid frame follows at offset"),
        "{error}"
    );
}

#[tokio::test]
async fn admission_completed_archive_records_nothing() {
    let dir = tempdir().unwrap();
    write_manifest(dir.path());
    write_system_log_completed(dir.path()).await;
    append_tail(&dir.path().join("system.log"), &[0; 512]);

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .unwrap();
    assert_eq!(archive.archive_status(), ArchiveStatus::Completed);
    assert!(archive.interrupted_appends().is_empty());
}

#[tokio::test]
async fn admission_merged_ancestor_tail_recorded() {
    use obzenflow_core::journal::{RunManifestInterruptedAppend, TailKind};
    const RESUMED_ID: &str = "flow_01H00000000000000000000001";
    let base = tempdir().unwrap();
    let (r0, r1) = (base.path().join("r0"), base.path().join("r1"));
    let fact = source_fact(1);
    std::fs::create_dir_all(&r0).unwrap();
    std::fs::create_dir_all(&r1).unwrap();
    write_stopped_run(&r0, FLOW_ID, None, std::slice::from_ref(&fact)).await;
    let committed_end = append_tail(&r0.join("system.log"), &[0; 4096]);
    let r0 = std::fs::canonicalize(&r0).unwrap();
    write_stopped_run(
        &r1,
        RESUMED_ID,
        Some(
            obzenflow_core::journal::archive::manifest::RunManifestResumeConfig {
                resumed_from: r0.clone(),
                resume_generation: 1,
                merged_from: Vec::new(),
            },
        ),
        std::slice::from_ref(&fact),
    )
    .await;
    let r1 = std::fs::canonicalize(&r1).unwrap();
    DiskReplayArchive::open_for_resume(r0.clone(), false)
        .await
        .unwrap()
        .claim_continuation(
            RESUMED_ID.parse().unwrap(),
            &obzenflow_core::journal::factory::CurrentRunLocator::new(r1.clone()),
        )
        .unwrap();

    let head = DiskReplayArchive::open_for_resume(r1, false).await.unwrap();
    assert_eq!(head.ancestors().len(), 1);
    assert_eq!(
        head.interrupted_appends(),
        [RunManifestInterruptedAppend {
            ancestor: Some(r0),
            journal: "system.log".into(),
            committed_end,
            left_bytes: 4096,
            tail_kind: TailKind::ZeroFilled,
        }]
    );
}

#[tokio::test]
async fn resume_with_damaged_ancestor_log_refused() {
    let base = tempdir().unwrap();
    let (parent, resumed) =
        write_claimed_resume(base.path(), &[source_fact(1)], 1, Some(cancelled()), None).await;
    flip_first_record_byte(&parent.join("system.log"));

    for allow_incomplete_archive in [false, true] {
        let Err(error) =
            DiskReplayArchive::open_for_resume(resumed.clone(), allow_incomplete_archive).await
        else {
            panic!("a damaged ancestor system.log opens with allow_incomplete_archive={allow_incomplete_archive}");
        };
        assert!(
            matches!(&error, ReplayError::AncestorUnreadable { ancestor, source, .. }
                if *ancestor == parent
                    && matches!(source.as_ref(), ReplayError::Parse { message }
                        if message.starts_with("system.log record corrupt at line 1 in "))),
            "{error}"
        );
    }
}

#[tokio::test]
async fn resume_with_missing_ancestor_log_refused() {
    let base = tempdir().unwrap();
    let (parent, resumed) =
        write_claimed_resume(base.path(), &[source_fact(1)], 1, None, None).await;
    let system_log = parent.join("system.log");
    std::fs::remove_file(&system_log).unwrap();

    let Err(error) = DiskReplayArchive::open_for_resume(resumed.clone(), false).await else {
        panic!("a resume opens an ancestor that has no system.log");
    };
    assert!(
        matches!(&error, ReplayError::AncestorMissingSystemLog { .. }),
        "{error}"
    );
    assert_eq!(
        error.to_string(),
        format!(
            "cannot resume {resumed}: its reconstruction did not complete, and the archive it \
             resumed from, {parent}, has no system.log.\nRestore {system_log} from a copy, then \
             resume {resumed} again, or resume it with --allow-incomplete-archive to open \
             {parent} as interrupted from its stage journals.",
            resumed = resumed.display(),
            parent = parent.display(),
            system_log = system_log.display()
        )
    );
}

#[tokio::test]
async fn missing_ancestor_log_with_flag_resumes() {
    let base = tempdir().unwrap();
    let (parent, resumed) =
        write_claimed_resume(base.path(), &[source_fact(1)], 1, None, None).await;
    std::fs::remove_file(parent.join("system.log")).unwrap();

    let head = DiskReplayArchive::open_for_resume(resumed, true)
        .await
        .expect("the flag opens the ancestor from its stage journals");
    let ancestors = head.ancestors();
    assert_eq!(ancestors.len(), 1);
    assert_eq!(ancestors[0].archive_path(), parent);
    assert_eq!(ancestors[0].archive_status(), ArchiveStatus::Unknown);
}

#[cfg(unix)]
#[tokio::test]
async fn archive_looped_system_log_reports_io() {
    use obzenflow_infra::verify::{render_verdict, verify_run_dirs, VerifyOptions};

    let base = tempdir().unwrap();
    let (parent, resumed) =
        write_claimed_resume(base.path(), &[source_fact(1)], 1, None, None).await;
    let log = parent.join("system.log");
    std::fs::remove_file(&log).unwrap();
    std::os::unix::fs::symlink("system.log", &log).unwrap();
    let cause = std::fs::File::open(&log).unwrap_err();

    for allow_incomplete in [true, false] {
        let Err(error) =
            DiskReplayArchive::open_for_resume(resumed.clone(), allow_incomplete).await
        else {
            panic!(
                "resume accepted an unreadable ancestor with allow_incomplete={allow_incomplete}"
            );
        };
        assert!(
            matches!(&error, ReplayError::AncestorUnreadable { ancestor, source, .. }
            if ancestor == &parent && matches!(source.as_ref(), ReplayError::Io { source, .. }
                if source.raw_os_error() == cause.raw_os_error())),
            "{error}"
        );
        let message = error.to_string();
        assert!(message.contains(&log.display().to_string()), "{message}");
        assert!(message.contains(&cause.to_string()), "{message}");
    }

    let verdict = verify_run_dirs(&parent, &resumed, &VerifyOptions::default()).unwrap();
    let message = render_verdict(&verdict);
    assert_eq!(verdict.exit_code(), 3);
    assert!(message.contains(&log.display().to_string()), "{message}");
    assert!(message.contains(&cause.to_string()), "{message}");
}

#[tokio::test]
async fn torn_resume_evidence_merges_ancestor() {
    use obzenflow_core::event::payloads::flow_control_payload::EofKind;
    use obzenflow_core::journal::archive_actions::{
        decide, ArchiveOverrides, ArchiveRefusal, FlowEvidence, ReplayVerb, ResumeAdvice,
    };
    let base = tempdir().unwrap();
    let parent_facts = [source_fact(1), source_fact(2), source_eof()];
    let (_, torn) = write_claimed_resume(base.path(), &parent_facts, 1, None, None).await;
    let flow = FlowEvidence {
        cycle_members: Vec::new(),
        sources: vec![(
            "returns".to_string(),
            obzenflow_core::source::SourceContinuation::Refuse,
        )],
    };

    let mut evidence = DiskReplayArchive::open(torn.clone(), false)
        .await
        .unwrap()
        .evidence();
    assert_eq!(evidence.sources[0].recorded_eof, None);
    let resume = DiskReplayArchive::resume_evidence(&torn).await.unwrap();
    assert_eq!(
        resume.archive.sources[0].recorded_eof,
        Some(EofKind::Natural)
    );
    evidence.resume = Some(Box::new(resume));
    assert_eq!(
        decide(
            ReplayVerb::Replay,
            &torn,
            &evidence,
            &flow,
            ArchiveOverrides::default()
        ),
        Err(ArchiveRefusal::Interrupted {
            run: torn.clone(),
            resume: Some(Box::new(ResumeAdvice {
                run: torn.clone(),
                continued_from: None,
                admission: Ok(()),
            })),
        })
    );
}

#[tokio::test]
async fn replay_completed_head_offers_replay() {
    use obzenflow_core::journal::archive_actions::{
        decide, shell_path, ArchiveOverrides, FlowEvidence, ReplayVerb,
    };
    let base = tempdir().unwrap();
    let facts = [source_fact(1), source_eof()];
    let (parent, head) = write_claimed_resume(
        &base.path().join("Pat's Runs"),
        &facts,
        facts.len(),
        None,
        Some(completed()),
    )
    .await;
    let before = (archive_bytes(&parent), archive_bytes(&head));
    let mut archive = DiskReplayArchive::open(parent.clone(), false)
        .await
        .unwrap()
        .evidence();
    archive.resume = Some(Box::new(
        DiskReplayArchive::resume_evidence(&parent).await.unwrap(),
    ));
    let refusal = decide(
        ReplayVerb::Replay,
        &parent,
        &archive,
        &FlowEvidence::default(),
        ArchiveOverrides::default(),
    )
    .unwrap_err();
    assert_eq!(refusal.to_string(), format!(
        "cannot replay {parent}: it has no terminal record, so it was interrupted.\n\
         Pass --allow-incomplete-archive to replay the recorded prefix of {parent}.\n\
         {head} continued {parent}.\n\
         cannot resume {head}: it completed naturally and every source is finite, so there is no work left to continue.\n\
         Replay it with --replay-from {operand}.",
        parent = parent.display(), head = head.display(), operand = shell_path(&head)
    ));
    assert_eq!((archive_bytes(&parent), archive_bytes(&head)), before);
}

#[tokio::test]
async fn resume_missing_successor_names_directory() {
    let base = tempdir().unwrap();
    let (parent, head) = write_claimed_resume(base.path(), &[], 0, None, None).await;
    std::fs::rename(&head, base.path().join("saved")).unwrap();
    assert!(matches!(DiskReplayArchive::resume_evidence(&parent).await,
        Err(ReplayError::ContinuationMissing { parent: from, successor }) if from == parent && successor == head
    ));
}

#[tokio::test]
async fn resume_mismatched_claim_reports_identity() {
    let base = tempdir().unwrap();
    let (parent, head) = write_claimed_resume(base.path(), &[], 0, None, None).await;
    std::fs::rename(&head, base.path().join("saved")).unwrap();
    std::fs::create_dir(&head).unwrap();
    write_stopped_run(&head, FLOW_ID, None, &[]).await;
    let error = DiskReplayArchive::resume_evidence(&parent)
        .await
        .unwrap_err();
    assert!(
        matches!(error, ReplayError::Parse { message } if message == format!(
            "The continuation claim in {} names flow_01H00000000000000000000001, but {} contains {FLOW_ID}.", parent.display(), head.display()
        ))
    );
}

#[tokio::test]
async fn resume_locked_head_reports_writer() {
    use obzenflow_core::journal::archive_actions::{ArchiveRefusal, FlowEvidence, ReplayVerb};
    let base = tempdir().unwrap();
    let (parent, head) = write_claimed_resume(base.path(), &[], 0, None, None).await;
    let writer = std::fs::File::open(head.join(SOURCE_LOG)).unwrap();
    writer.try_lock().unwrap();
    std::fs::remove_file(head.join("system.log")).unwrap();
    let target = DiskReplayArchive::resume_evidence(&parent).await.unwrap();
    assert_eq!(
        target.judge(&FlowEvidence::default()).admission,
        Err(ArchiveRefusal::StillRunning {
            verb: ReplayVerb::Resume,
            run: head
        })
    );
}

#[tokio::test]
async fn resume_unclaimed_child_selects_parent() {
    let base = tempdir().unwrap();
    let (parent, child) = write_claimed_resume(base.path(), &[], 0, None, None).await;
    std::fs::remove_file(parent.join("run_continuation.json")).unwrap();
    let target = DiskReplayArchive::resume_evidence(&child).await.unwrap();
    assert_eq!(target.run, parent);
    assert_eq!(target.continued_from, None);
}

#[tokio::test]
async fn replay_aliased_cycle_refused() {
    let base = tempdir().unwrap();
    let replay = base.path().join("replay");
    std::fs::create_dir(&replay).unwrap();
    write_stopped_run(&replay, FLOW_ID, None, &[]).await;
    record_replay_of(&replay, &replay.join("..").join("replay"));
    let error = DiskReplayArchive::resume_evidence(&replay)
        .await
        .unwrap_err();
    let replay = std::fs::canonicalize(replay).unwrap();
    assert!(matches!(error, ReplayError::Parse { message }
        if message == format!("the replays from {0} return to {0}", replay.display())));
}

#[tokio::test]
async fn resume_aliased_ancestor_refuses_cycle() {
    use obzenflow_core::journal::archive::manifest::RunManifestResumeConfig;
    let base = tempdir().unwrap();
    let (parent, head) = write_claimed_resume(base.path(), &[], 0, None, None).await;
    let path = parent.join(RUN_MANIFEST_FILENAME);
    let mut manifest: RunManifest = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    manifest.resume = Some(RunManifestResumeConfig {
        resumed_from: parent.join("..").join("r0"),
        resume_generation: 1,
        merged_from: Vec::new(),
    });
    std::fs::write(path, serde_json::to_vec_pretty(&manifest).unwrap()).unwrap();
    let error = DiskReplayArchive::resume_evidence(&head).await.unwrap_err();
    assert!(matches!(error, ReplayError::Parse { message }
        if message == format!("the resume lineage of {} returns to {}", head.display(), parent.display())));
}

#[tokio::test]
async fn linked_resume_names_replayed_run() {
    use obzenflow_core::journal::archive_actions::{
        decide, ArchiveOverrides, FlowEvidence, ReplayVerb,
    };
    const REPLAY_ID: &str = "flow_01H00000000000000000000001";
    let base = tempdir().unwrap();
    let (original, replay) = (base.path().join("original"), base.path().join("replay"));
    std::fs::create_dir_all(&original).unwrap();
    std::fs::create_dir_all(&replay).unwrap();
    write_run(
        &original,
        FLOW_ID,
        None,
        &[source_fact(1), source_eof()],
        Some(completed()),
    )
    .await;
    let original = std::fs::canonicalize(&original).unwrap();
    write_stopped_run(&replay, REPLAY_ID, None, &[source_fact(1)]).await;
    record_replay_of(&replay, &original);

    let mut evidence = DiskReplayArchive::open(replay.clone(), false)
        .await
        .unwrap()
        .evidence();
    let resume = DiskReplayArchive::resume_evidence(&replay).await.unwrap();
    assert_eq!(resume.run, original);
    evidence.resume = Some(Box::new(resume));
    let refusal = decide(
        ReplayVerb::Replay,
        &replay,
        &evidence,
        &FlowEvidence::default(),
        ArchiveOverrides::default(),
    )
    .unwrap_err();
    assert_eq!(
        refusal.to_string(),
        format!(
            "cannot replay {replay}: it has no terminal record, so it was interrupted.\nPass \
             --allow-incomplete-archive to replay its recorded prefix.\ncannot resume \
             {original}: it completed naturally and every source is finite, so there is no work \
             left to continue.\nReplay it with --replay-from {operand}.",
            replay = replay.display(),
            original = original.display(),
            operand = obzenflow_core::journal::archive_actions::shell_path(&original)
        )
    );
}

#[tokio::test]
async fn archive_read_moved_carrier_names_journal() {
    const ERROR_LOG: &str = "FiniteSource_returns_error_stage_01H00000000000000000000000.log";
    let dir = tempdir().unwrap();
    let carrier = DiskJournal::<ChainEvent>::with_owner_in_run(
        dir.path().join(ERROR_LOG),
        obzenflow_core::JournalOwner::stage("stage_01H00000000000000000000000".parse().unwrap()),
        FLOW_ID.parse().unwrap(),
        JournalSync::BeforeAck,
    )
    .unwrap();
    carrier
        .append(source_fact(0), Default::default())
        .await
        .unwrap();
    write_stopped_run(dir.path(), FLOW_ID, None, &[source_fact(1)]).await;
    drop(carrier);
    let error_log = dir.path().join(ERROR_LOG);
    std::fs::copy(&error_log, dir.path().join(format!("{ERROR_LOG}.0.damage"))).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&error_log)
        .unwrap()
        .set_len(0)
        .unwrap();

    let archive = DiskReplayArchive::open(dir.path().to_path_buf(), false)
        .await
        .unwrap();
    let mut reader = archive
        .open_source_reader("returns", StageType::FiniteSource)
        .await
        .unwrap();
    let Err(error) = reader.next().await else {
        panic!("a record whose definition was moved out of its carrier must fail");
    };
    assert!(
        error.to_string().contains(&format!(
            "its definition is carried by the frame at offset 0 of {ERROR_LOG}, which ends at \
             offset 0; restore {ERROR_LOG} from a copy, or put back the bytes moved out of it"
        )),
        "{error}"
    );
}
