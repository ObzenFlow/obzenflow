// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! JSONL export and human inspection over a run's framed journals (FLOWIP-120q).
//!
//! The raw `.log` files are internal checksummed binary storage. This
//! module is the supported public projection: `export_jsonl` writes one JSON
//! object per committed `LogRecord`, and `inspect` prints a filtered, human
//! view. Both go through the same sealed scanner and policy as replay and
//! verification, so they fail loud on corruption with path and record position
//! and never strip prefixes with a regex.

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use obzenflow_core::event::{ChainEvent, JournalEvent, SystemEvent};
use obzenflow_core::journal::archive::manifest::{
    RunManifest, JOURNAL_SCHEMA_VERSION, RUN_MANIFEST_FILENAME,
};
use obzenflow_core::journal::ArchiveStatus;
use thiserror::Error;

use super::codec::Decoder;
use super::manifest_gate::require_current_journal_schema_version;
use super::replay_archive::derive_status_derivation_from_system_log;
use super::scanner::{classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy};

#[derive(Debug, Error)]
pub enum JournalInspectError {
    #[error("io error at {path}: {source}")]
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("run manifest error at {path}: {message}")]
    Manifest { path: PathBuf, message: String },
    #[error("corrupt record in {path} at offset {offset}: {problem}")]
    Corrupt {
        path: PathBuf,
        offset: u64,
        problem: String,
    },
}

/// Export every committed record of a run's journals as JSONL. System, data, and
/// error journals are discovered from the manifest and emitted in a stable
/// order. Fails loud on corruption with path and record position.
pub fn export_jsonl(run_dir: &Path, output: Option<&Path>) -> Result<(), JournalInspectError> {
    let manifest = load_manifest(run_dir)?;
    let policy = archive_policy(run_dir, &manifest)?;

    let mut out: Box<dyn Write> = match output {
        Some(path) => Box::new(BufWriter::new(File::create(path).map_err(|source| {
            JournalInspectError::Io {
                path: path.to_path_buf(),
                source,
            }
        })?)),
        None => Box::new(std::io::stdout().lock()),
    };

    let system_path = run_dir.join(&manifest.system_journal_file);
    export_journal_file::<SystemEvent>(&system_path, policy, &mut out)?;

    for key in sorted_stage_keys(&manifest) {
        let stage = &manifest.stages[&key];
        export_journal_file::<ChainEvent>(
            &run_dir.join(&stage.data_journal_file),
            policy,
            &mut out,
        )?;
        let error_path = run_dir.join(&stage.error_journal_file);
        if error_path.exists() {
            export_journal_file::<ChainEvent>(&error_path, policy, &mut out)?;
        }
    }

    out.flush().map_err(|source| JournalInspectError::Io {
        path: output.map(Path::to_path_buf).unwrap_or_default(),
        source,
    })
}

/// Print a human-readable summary of a run plus a filtered listing of stage data
/// journals. `stage` filters to one stage key; `event_type` filters rows.
pub fn inspect(
    run_dir: &Path,
    stage: Option<&str>,
    event_type: Option<&str>,
) -> Result<(), JournalInspectError> {
    let manifest = load_manifest(run_dir)?;
    let policy = archive_policy(run_dir, &manifest)?;
    let status = archive_status(run_dir, &manifest)?;

    println!("flow_id:    {}", manifest.flow_id);
    println!("flow_name:  {}", manifest.flow_name);
    println!("status:     {status:?}");
    println!("schema:     {}", manifest.journal_schema_version);
    println!("stages:     {}", manifest.stages.len());

    for key in sorted_stage_keys(&manifest) {
        if stage.is_some_and(|filter| filter != key) {
            continue;
        }
        let stage_info = &manifest.stages[&key];
        println!(
            "\n[{key}] {} ({:?})",
            stage_info.data_journal_file, stage_info.stage_type
        );
        inspect_chain_journal(
            &run_dir.join(&stage_info.data_journal_file),
            policy,
            event_type,
        )?;
    }

    Ok(())
}

fn export_journal_file<R: JournalEvent>(
    path: &Path,
    policy: ReadPolicy,
    out: &mut dyn Write,
) -> Result<(), JournalInspectError> {
    if !path.exists() {
        return Ok(());
    }
    let mut reader = open_reader(path)?;
    let mut buf = Vec::new();
    let mut offset = 0u64;
    let mut decoder = Decoder::new(path);

    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|source| JournalInspectError::Io {
            path: path.to_path_buf(),
            source,
        })?
    {
        let record_offset = offset;
        offset += consumed as u64;
        match dispose(
            classify_frame::<R>(&buf, &mut decoder, record_offset),
            termination,
            policy,
        ) {
            Disposition::Yield(frame) => {
                for record in frame.into_records() {
                    serde_json::to_writer(&mut *out, &record).map_err(|e| {
                        JournalInspectError::Io {
                            path: path.to_path_buf(),
                            source: std::io::Error::other(e.to_string()),
                        }
                    })?;
                    out.write_all(b"\n")
                        .map_err(|source| JournalInspectError::Io {
                            path: path.to_path_buf(),
                            source,
                        })?;
                }
            }
            Disposition::EndOfCommittedRecords | Disposition::Skip => break,
            Disposition::Corrupt(problem) => {
                return Err(JournalInspectError::Corrupt {
                    path: path.to_path_buf(),
                    offset: record_offset,
                    problem: problem.to_string(),
                });
            }
        }
    }
    Ok(())
}

fn inspect_chain_journal(
    path: &Path,
    policy: ReadPolicy,
    event_type: Option<&str>,
) -> Result<(), JournalInspectError> {
    if !path.exists() {
        println!("  (no journal file)");
        return Ok(());
    }
    let mut reader = open_reader(path)?;
    let mut buf = Vec::new();
    let mut offset = 0u64;
    let mut decoder = Decoder::new(path);

    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|source| JournalInspectError::Io {
            path: path.to_path_buf(),
            source,
        })?
    {
        let record_offset = offset;
        offset += consumed as u64;
        match dispose(
            classify_frame::<ChainEvent>(&buf, &mut decoder, record_offset),
            termination,
            policy,
        ) {
            Disposition::Yield(frame) => {
                for record in frame.into_records() {
                    let ty = record.event_type();
                    if event_type.is_some_and(|filter| filter != ty.as_str()) {
                        continue;
                    }
                    println!("  {record_offset:>10}  {ty}");
                }
            }
            Disposition::EndOfCommittedRecords | Disposition::Skip => break,
            Disposition::Corrupt(problem) => {
                return Err(JournalInspectError::Corrupt {
                    path: path.to_path_buf(),
                    offset: record_offset,
                    problem: problem.to_string(),
                });
            }
        }
    }
    Ok(())
}

fn open_reader(path: &Path) -> Result<BufReader<File>, JournalInspectError> {
    let file = File::open(path).map_err(|source| JournalInspectError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    Ok(BufReader::new(file))
}

fn sorted_stage_keys(manifest: &RunManifest) -> Vec<String> {
    let mut keys: Vec<String> = manifest.stages.keys().cloned().collect();
    keys.sort();
    keys
}

fn archive_status(
    run_dir: &Path,
    manifest: &RunManifest,
) -> Result<ArchiveStatus, JournalInspectError> {
    let system_log = run_dir.join(&manifest.system_journal_file);
    match derive_status_derivation_from_system_log(&system_log) {
        Ok(derivation) => Ok(derivation.chosen),
        Err(
            error
            @ obzenflow_core::journal::archive::ReplayError::UnsupportedJournalSchemaVersion {
                ..
            },
        ) => Err(JournalInspectError::Manifest {
            path: system_log,
            message: error.to_string(),
        }),
        Err(_) => Ok(ArchiveStatus::Unknown),
    }
}

/// Tolerate a final torn tail only on a non-completed, current-schema archive.
fn archive_policy(
    run_dir: &Path,
    manifest: &RunManifest,
) -> Result<ReadPolicy, JournalInspectError> {
    Ok(ReadPolicy::SealedScan {
        tolerate_torn_tail: archive_status(run_dir, manifest)? != ArchiveStatus::Completed,
    })
}

pub(crate) fn load_manifest(run_dir: &Path) -> Result<RunManifest, JournalInspectError> {
    let manifest_path = run_dir.join(RUN_MANIFEST_FILENAME);
    let body =
        std::fs::read_to_string(&manifest_path).map_err(|source| JournalInspectError::Io {
            path: manifest_path.clone(),
            source,
        })?;
    let value: serde_json::Value =
        serde_json::from_str(&body).map_err(|e| JournalInspectError::Manifest {
            path: manifest_path.clone(),
            message: e.to_string(),
        })?;

    if let Err(version) = require_current_journal_schema_version(&value) {
        return Err(JournalInspectError::Manifest {
            path: manifest_path,
            message: format!("{version} (supported: {JOURNAL_SCHEMA_VERSION})"),
        });
    }
    super::manifest_gate::require_observability_capture(&value).map_err(|message| {
        JournalInspectError::Manifest {
            path: manifest_path.clone(),
            message,
        }
    })?;

    serde_json::from_value(value).map_err(|e| JournalInspectError::Manifest {
        path: manifest_path,
        message: e.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn observation_omission_preserves_full_records_through_reader_and_export() {
        use super::super::log_record::{serialize_atomic_group, serialize_record};
        use crate::journal::DiskJournal;
        use obzenflow_core::ai::AiMapReduceTaggedPartial;
        use obzenflow_core::event::observability::{
            CaptureReason, CaptureScope, CaptureSeq, CaptureStamp, ExecutionProgress,
            ObservabilityContext, RuntimeObservability, RuntimeSnapshot,
        };
        use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
        use obzenflow_core::event::provenance::RuntimeProvenance;
        use obzenflow_core::event::vector_clock::VectorClock;
        use obzenflow_core::event::{ChainEventFactory, ChainPayload, JournalRecord};
        use obzenflow_core::{
            FlowId, Journal, JournalOwner, JournalWriterId, StageId, TypedPayload, WriterId,
        };

        let dir = tempfile::tempdir().expect("temporary journal directory");
        let stage = StageId::new();
        let writer = WriterId::from(stage);
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: Default::default(),
        };
        let source =
            ChainEventFactory::data_event(writer, "application.null.v1", serde_json::Value::Null);
        let partial = AiMapReduceTaggedPartial {
            job_key: source.id,
            chunk_index: 0,
            chunk_count: 1,
            partial: serde_json::json!({"execution_type": "business-value", "items": [1, null]}),
        }
        .to_event(writer);
        let progress = ChainEventFactory::create_event(
            writer,
            ChainPayload::Execution(ExecutionPayload::AccumulatorProgress {
                inputs_since_last_report: 100,
            }),
        );
        let mut events = vec![source, partial, progress];
        for (index, event) in events.iter_mut().enumerate() {
            let mut packet = ObservabilityContext::new(CaptureStamp {
                capture_scope: scope,
                observer: writer,
                capture_seq: CaptureSeq(index as u64 + 1),
                capture_reason: CaptureReason::Record,
                observed_at_ms: 100 + index as u64,
            });
            packet.runtime = Some(RuntimeObservability {
                in_flight: Some(index as u32),
                ..Default::default()
            });
            let mut clock = VectorClock::new();
            clock.clocks.insert(writer.to_string(), index as u64 + 1);
            packet.runtime_snapshot = Some(RuntimeSnapshot {
                capture: packet.capture,
                progress: ExecutionProgress {
                    reader_seq: 12,
                    receipted_seq: 10,
                    writer_seq: index as u64 + 1,
                    last_consumed_event_id: Some(event.id),
                    last_consumed_writer: Some(JournalWriterId::new()),
                    last_consumed_vector_clock: Some(clock.clone()),
                    last_receipted_event_id: Some(event.id),
                    last_receipted_vector_clock: Some(clock),
                    last_emitted_event_id: Some(event.id),
                    last_emitted_writer: Some(writer),
                },
                fsm_state: "Running".into(),
            });
            let mut runtime = RuntimeProvenance::default();
            runtime.accounting.events_processed_total = 12;
            runtime.accounting.events_emitted_total = index as u64 + 1;
            event.runtime = Some(runtime);
            event.envelope.observability = Some(packet);
        }
        let journal = DiskJournal::<ChainEvent>::with_owner(
            dir.path().join("original.log"),
            JournalOwner::stage(stage),
        )
        .unwrap();
        let mut original = vec![journal
            .append(events.remove(0), Default::default())
            .await
            .unwrap()];
        original.extend(
            journal
                .append_group("omission-proof", events, Default::default())
                .await
                .unwrap(),
        );

        for mode in ["all", "none", "selected"] {
            let mut expected = original.clone();
            for (index, record) in expected.iter_mut().enumerate() {
                if mode == "none" || (mode == "selected" && index != 1) {
                    record.envelope.observability = None;
                }
                assert_eq!(
                    serde_json::to_value(&record.envelope.provenance).unwrap(),
                    serde_json::to_value(&original[index].envelope.provenance).unwrap()
                );
                assert_eq!(
                    serde_json::to_value(&record.payload).unwrap(),
                    serde_json::to_value(&original[index].payload).unwrap()
                );
            }
            let mut framed = Vec::new();
            for body in [
                serialize_record(&expected[0]).unwrap(),
                serialize_atomic_group("omission-proof", &expected[1..]).unwrap(),
            ] {
                framed.extend_from_slice(&body);
            }
            let path = dir.path().join(format!("{mode}.log"));
            std::fs::write(&path, framed).unwrap();
            let restored =
                DiskJournal::<ChainEvent>::with_owner(path.clone(), JournalOwner::stage(stage))
                    .unwrap();
            let mut reader = restored.reader().await.unwrap();
            for expected_record in &expected {
                let actual = reader.next().await.unwrap().unwrap();
                assert_eq!(
                    serde_json::to_value(actual).unwrap(),
                    serde_json::to_value(expected_record).unwrap()
                );
            }
            assert!(reader.next().await.unwrap().is_none());
            let mut jsonl = Vec::new();
            export_journal_file::<ChainEvent>(
                &path,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: false,
                },
                &mut jsonl,
            )
            .unwrap();
            let decoded: Vec<JournalRecord<ChainPayload>> = String::from_utf8(jsonl)
                .unwrap()
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(
                serde_json::to_value(decoded).unwrap(),
                serde_json::to_value(expected).unwrap()
            );
        }
    }

    #[test]
    fn inspection_rejects_non_current_manifest_before_output_or_journal_access() {
        for (version, expected) in [
            (None, "<missing>"),
            (Some(serde_json::json!(3.0)), "3.0"),
            (Some(serde_json::json!("2.0")), "2.0"),
            (Some(serde_json::json!("7.0")), "7.0"),
        ] {
            let temp = tempfile::tempdir().expect("temporary archive");
            let mut manifest = serde_json::json!({

                "system_journal_file": "sentinel-system.journal"
            });
            if let Some(version) = version {
                manifest["journal_schema_version"] = version;
            }
            std::fs::write(
                temp.path().join(RUN_MANIFEST_FILENAME),
                serde_json::to_vec(&manifest).unwrap(),
            )
            .unwrap();
            std::fs::write(
                temp.path().join("sentinel-system.journal"),
                b"this must never be decoded",
            )
            .unwrap();
            let output = temp.path().join("must-not-exist.jsonl");

            let error = export_jsonl(temp.path(), Some(&output))
                .expect_err("non-current manifest must refuse inspection");
            assert!(matches!(
                error,
                JournalInspectError::Manifest { ref message, .. }
                    if message.contains(expected)
                        && message.contains(JOURNAL_SCHEMA_VERSION)
            ));
            assert!(
                !output.exists(),
                "version refusal must precede output creation"
            );
        }
    }
}
