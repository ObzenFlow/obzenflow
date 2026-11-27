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
use std::io::{BufReader, BufWriter, Read, Take, Write};
use std::path::{Path, PathBuf};

use obzenflow_core::event::{ChainEvent, JournalEvent, SystemEvent};
use obzenflow_core::journal::archive::manifest::{
    RunManifest, JOURNAL_SCHEMA_VERSION, RUN_MANIFEST_FILENAME,
};
use obzenflow_core::journal::archive::ReplayError;
use obzenflow_core::journal::archive_actions::{
    decide, ArchiveEvidence, ArchiveOverrides, ArchiveRefusal, FailureEvidence, FlowEvidence,
    ReplayVerb,
};
use obzenflow_core::journal::ArchiveStatus;
use thiserror::Error;

use super::codec::Decoder;
use super::manifest_gate::require_current_journal_schema_version;
use super::replay_archive::{admission_run, scan_system_log, DiskReplayArchive};
use super::scanner::{
    classify_frame, dispose, read_frame_sync, tail_after, Disposition, ReadPolicy,
};
use crate::application::{
    archive_open_error, replay_error_text, shell_path, ApplicationError, ResumeAdvice,
};

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
    if let Some(metrics) = &manifest.metrics_journals {
        for file in [
            &metrics.coordination_journal_file,
            &metrics.export_journal_file,
        ] {
            export_journal_file::<SystemEvent>(&run_dir.join(file), policy, &mut out)?;
        }
    }

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

/// `inspect` writes a summary of a run to `out`, and then a filtered list of the
/// stage data journals of the run. The summary gives the state of the run and the
/// answers of the recorded flow to a plain replay and a plain resume. `stage`
/// selects one stage key, and `event_type` selects rows.
pub async fn inspect<W: Write>(
    run_dir: &Path,
    stage: Option<&str>,
    event_type: Option<&str>,
    out: &mut W,
) -> Result<(), JournalInspectError> {
    let manifest = load_manifest(run_dir)?;
    validate_identities(run_dir, &manifest)?;
    let state = RunState::read(run_dir, &manifest)?;
    let flow = recorded_flow_evidence(run_dir, &manifest);
    let mut verdicts = Vec::new();
    for verb in [ReplayVerb::Replay, ReplayVerb::Resume] {
        verdicts.push((verb, verdict(verb, run_dir, &flow, &state).await));
    }

    let out: &mut dyn Write = out;
    write_field(out, run_dir, "flow_id:", &manifest.flow_id)?;
    write_field(out, run_dir, "flow_name:", &manifest.flow_name)?;
    write_field(out, run_dir, "status:", &state.status_line())?;
    write_field(out, run_dir, "schema:", &manifest.journal_schema_version)?;
    write_field(out, run_dir, "stages:", &manifest.stages.len().to_string())?;
    write_field(out, run_dir, "admission:", "for the flow this run recorded")?;
    for (verb, verdict) in &verdicts {
        write_field(out, run_dir, &format!("{}:", verb.word()), verdict)?;
    }

    for key in sorted_stage_keys(&manifest) {
        if stage.is_some_and(|filter| filter != key) {
            continue;
        }
        let stage_info = &manifest.stages[&key];
        let path = run_dir.join(&stage_info.data_journal_file);
        write_line(
            out,
            &path,
            &format!(
                "\n[{key}] {} ({:?})",
                stage_info.data_journal_file, stage_info.stage_type
            ),
        )?;
        inspect_chain_journal(&path, state.read_policy(), event_type, out)?;
    }

    out.flush().map_err(|source| JournalInspectError::Io {
        path: run_dir.to_path_buf(),
        source,
    })
}

enum RunState {
    Live,
    Recorded(ArchiveStatus),
    Unreadable(String),
    UnprobedWriter { refusal: String, verdict: String },
}

impl RunState {
    fn read(run_dir: &Path, manifest: &RunManifest) -> Result<Self, JournalInspectError> {
        match super::continuation::has_live_writer(run_dir, manifest) {
            Ok(true) => return Ok(Self::Live),
            Ok(false) => {}
            Err(error) => {
                return Ok(Self::UnprobedWriter {
                    refusal: replay_error_text(&error),
                    verdict: opener_text(archive_open_error(error)),
                })
            }
        }
        let system_log = run_dir.join(&manifest.system_journal_file);
        match scan_system_log(&system_log) {
            Ok(scan) => Ok(Self::Recorded(scan.derivation.chosen)),
            Err(error @ ReplayError::UnsupportedJournalSchemaVersion { .. }) => {
                Err(JournalInspectError::Manifest {
                    path: system_log,
                    message: error.to_string(),
                })
            }
            Err(error) => Ok(Self::Unreadable(replay_error_text(&error))),
        }
    }

    fn status_line(&self) -> String {
        match self {
            Self::Live => "still running".to_string(),
            Self::Recorded(ArchiveStatus::NotStarted) => "not started".to_string(),
            Self::Recorded(ArchiveStatus::Unknown) => {
                "interrupted (no terminal record)".to_string()
            }
            Self::Recorded(
                status @ (ArchiveStatus::Completed
                | ArchiveStatus::Failed
                | ArchiveStatus::Cancelled),
            ) => format!("{status:?}"),
            Self::Unreadable(refusal) | Self::UnprobedWriter { refusal, .. } => {
                format!("unknown ({refusal})")
            }
        }
    }

    fn read_policy(&self) -> ReadPolicy {
        ReadPolicy::SealedScan {
            tolerate_torn_tail: !matches!(self, Self::Recorded(ArchiveStatus::Completed)),
        }
    }
}

async fn verdict(
    verb: ReplayVerb,
    run_dir: &Path,
    flow: &Result<FlowEvidence, JournalInspectError>,
    state: &RunState,
) -> String {
    let flow = match flow {
        Ok(flow) => flow,
        Err(error) => return error.to_string(),
    };
    let (opened, evidence) = match state {
        RunState::Live => (
            run_dir.to_path_buf(),
            ArchiveEvidence {
                status: ArchiveStatus::Unknown,
                live_writer: true,
                sources: Vec::new(),
                failure: FailureEvidence::Other,
            },
        ),
        RunState::UnprobedWriter { verdict, .. } => return verdict.clone(),
        RunState::Recorded(_) | RunState::Unreadable(_) => {
            match DiskReplayArchive::probed_evidence(run_dir.to_path_buf(), verb).await {
                Ok(opened) => opened,
                Err(error) => return opener_text(archive_open_error(error)),
            }
        }
    };
    let command = format!("--{}-from {}", verb.word(), shell_path(run_dir));
    match decide(
        verb,
        &admission_run(run_dir, &opened),
        &evidence,
        flow,
        ArchiveOverrides::default(),
    ) {
        Ok(None) => format!("admitted: {command}"),
        Ok(Some(_)) => format!("admitted, with a warning: {command}"),
        Err(refusal) => {
            let advice = match &refusal {
                ArchiveRefusal::Failed { .. }
                | ArchiveRefusal::Interrupted { .. }
                | ArchiveRefusal::NotStarted { .. } => {
                    Some(ResumeAdvice::for_replay_refusal(run_dir, &opened, flow).await)
                }
                _ => None,
            };
            ApplicationError::archive_refusal(&refusal, advice.as_ref()).to_string()
        }
    }
}

fn opener_text(error: ApplicationError) -> String {
    match error {
        ApplicationError::InvalidConfiguration(message)
        | ApplicationError::ResumeRefused(message) => message,
        other => other.to_string(),
    }
}

fn recorded_flow_evidence(
    run_dir: &Path,
    manifest: &RunManifest,
) -> Result<FlowEvidence, JournalInspectError> {
    let mut sources = Vec::new();
    for (stage_key, stage) in &manifest.stages {
        if !stage.stage_type.is_source() {
            continue;
        }
        let continuation =
            stage
                .source_continuation
                .ok_or_else(|| JournalInspectError::Manifest {
                    path: run_dir.join(RUN_MANIFEST_FILENAME),
                    message: format!("source stage '{stage_key}' records no continuation"),
                })?;
        sources.push((stage_key.clone(), continuation));
    }
    sources.sort_by(|(left, _), (right, _)| left.cmp(right));
    let mut cycle_members = manifest.cycle_members.clone();
    cycle_members.sort();
    Ok(FlowEvidence {
        cycle_members,
        sources,
    })
}

fn write_field(
    out: &mut dyn Write,
    run_dir: &Path,
    label: &str,
    text: &str,
) -> Result<(), JournalInspectError> {
    let mut lines = text.lines();
    write_line(
        out,
        run_dir,
        &format!("{label:<12}{}", lines.next().unwrap_or_default()),
    )?;
    for line in lines {
        write_line(out, run_dir, &format!("{:12}{line}", ""))?;
    }
    Ok(())
}

fn write_line(out: &mut dyn Write, path: &Path, line: &str) -> Result<(), JournalInspectError> {
    writeln!(out, "{line}").map_err(|source| JournalInspectError::Io {
        path: path.to_path_buf(),
        source,
    })
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
    let mut admission = super::identity::CommitmentAdmission::open(path).map_err(|error| {
        JournalInspectError::Corrupt {
            path: path.into(),
            offset: 0,
            problem: error.to_string(),
        }
    })?;

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
            || tail_after(path, &buf, record_offset, &mut reader),
        ) {
            Disposition::Yield(frame) => {
                let records = frame.into_records();
                for record in &records {
                    admission
                        .admit(record)
                        .map_err(|error| JournalInspectError::Corrupt {
                            path: path.into(),
                            offset: record_offset,
                            problem: error.to_string(),
                        })?;
                }
                for record in records {
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
            Disposition::EndOfCommittedRecords
            | Disposition::Interrupted(_)
            | Disposition::Skip => break,
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
    out: &mut dyn Write,
) -> Result<(), JournalInspectError> {
    if !path.exists() {
        return write_line(out, path, "  (no journal file)");
    }
    let mut reader = open_reader(path)?;
    let mut buf = Vec::new();
    let mut offset = 0u64;
    let mut decoder = Decoder::new(path);
    let mut admission = super::identity::CommitmentAdmission::open(path).map_err(|error| {
        JournalInspectError::Corrupt {
            path: path.into(),
            offset: 0,
            problem: error.to_string(),
        }
    })?;

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
            || tail_after(path, &buf, record_offset, &mut reader),
        ) {
            Disposition::Yield(frame) => {
                let records = frame.into_records();
                for record in &records {
                    admission
                        .admit(record)
                        .map_err(|error| JournalInspectError::Corrupt {
                            path: path.into(),
                            offset: record_offset,
                            problem: error.to_string(),
                        })?;
                }
                for record in records {
                    let ty = record.event_type();
                    if event_type.is_some_and(|filter| filter != ty.as_str()) {
                        continue;
                    }
                    write_line(out, path, &format!("  {record_offset:>10}  {ty}"))?;
                }
            }
            Disposition::EndOfCommittedRecords
            | Disposition::Interrupted(_)
            | Disposition::Skip => break,
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

fn open_reader(path: &Path) -> Result<BufReader<Take<File>>, JournalInspectError> {
    let io = |source| JournalInspectError::Io {
        path: path.to_path_buf(),
        source,
    };
    let file = File::open(path).map_err(io)?;
    let length = file.metadata().map_err(io)?.len();
    Ok(BufReader::new(file.take(length)))
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
    match scan_system_log(&system_log) {
        Ok(scan) => Ok(scan.derivation.chosen),
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
    validate_identities(run_dir, manifest)?;
    Ok(ReadPolicy::SealedScan {
        tolerate_torn_tail: archive_status(run_dir, manifest)? != ArchiveStatus::Completed,
    })
}

fn validate_identities(run_dir: &Path, manifest: &RunManifest) -> Result<(), JournalInspectError> {
    super::identity::validate_archive_identities(run_dir, manifest).map_err(|error| {
        JournalInspectError::Manifest {
            path: run_dir.join(RUN_MANIFEST_FILENAME),
            message: error.to_string(),
        }
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
    use obzenflow_core::journal::JournalReader;
    use obzenflow_runtime::runtime_config::JournalSync;

    #[tokio::test]
    async fn missing_commitments_cannot_produce_a_complete_export_or_terminal_status() {
        use crate::journal::DiskJournal;
        use obzenflow_core::event::{SystemEventFactory, SystemPayload};
        use obzenflow_core::{Journal, JournalOwner, SystemId};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("system.log");
        let system = SystemId::new();
        let journal = DiskJournal::<SystemEvent>::with_owner(
            path.clone(),
            JournalOwner::system(system),
            JournalSync::Off,
        )
        .unwrap();
        let factory = SystemEventFactory::new(system);
        journal
            .append(factory.pipeline_starting(), Default::default())
            .await
            .unwrap();
        let first_end = std::fs::metadata(&path).unwrap().len() as usize;
        journal
            .append(factory.pipeline_running(), Default::default())
            .await
            .unwrap();
        let middle_end = std::fs::metadata(&path).unwrap().len() as usize;
        journal
            .append(
                factory.pipeline_completed(
                    obzenflow_core::event::types::DurationMs(1),
                    obzenflow_core::metrics::FlowLifecycleMetricsSnapshot {
                        events_in_total: 0,
                        events_out_total: 0,
                        errors_total: 0,
                    },
                ),
                Default::default(),
            )
            .await
            .unwrap();
        let original = std::fs::read(&path).unwrap();
        for missing_first in [false, true] {
            let bytes = if missing_first {
                original[first_end..].to_vec()
            } else {
                [
                    original[..first_end].to_vec(),
                    original[middle_end..].to_vec(),
                ]
                .concat()
            };
            std::fs::write(&path, bytes).unwrap();
            assert!(scan_system_log(&path).is_err());
            let mut output = Vec::new();
            assert!(matches!(
                export_journal_file::<SystemEvent>(
                    &path,
                    ReadPolicy::SealedScan {
                        tolerate_torn_tail: false
                    },
                    &mut output,
                ),
                Err(JournalInspectError::Corrupt { .. })
            ));
            for line in String::from_utf8(output).unwrap().lines() {
                let record: obzenflow_core::JournalRecord<SystemPayload> =
                    serde_json::from_str(line).unwrap();
                assert_eq!(record.local_sequence(), 1);
            }
        }
    }

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
        let source = ChainEventFactory::data_event(
            writer,
            "application.null",
            std::num::NonZeroU32::MIN,
            serde_json::Value::Null,
        );
        let partial = AiMapReduceTaggedPartial {
            key: None,
            group: obzenflow_core::ai::GroupOrdinal::new(0),
            items: vec![serde_json::Value::Null],
            partial: serde_json::json!({"execution_type": "business-value", "items": [1, null]}),
        }
        .to_event(writer);
        let progress = ChainEventFactory::execution_event(
            writer,
            ExecutionPayload::AccumulatorProgress {
                inputs_since_last_report: 100,
            },
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
            clock.clocks.insert(
                obzenflow_core::event::CausalCoordinate::new(JournalWriterId::new()),
                index as u64 + 1,
            );
            packet.runtime_snapshot = Some(RuntimeSnapshot {
                capture: packet.capture,
                progress: ExecutionProgress {
                    reader_seq: 12,
                    writer_seq: index as u64 + 1,
                    last_consumed_event_id: Some(event.id),
                    last_consumed_writer: Some(JournalWriterId::new()),
                    last_consumed_vector_clock: Some(clock),
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
            JournalSync::Off,
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
            std::fs::copy(
                dir.path().join("original.identity.json"),
                path.with_extension("identity.json"),
            )
            .unwrap();
            let mut reader = super::super::reader::DiskJournalReader::<ChainEvent>::new(
                path.clone(),
                *journal.id(),
                std::sync::Arc::new(tokio::sync::RwLock::new(())),
            )
            .await
            .unwrap();
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

    async fn recorded_run(
        dir: &Path,
        lifecycle: impl FnOnce(&obzenflow_core::event::SystemEventFactory) -> Vec<SystemEvent>,
    ) -> crate::journal::DiskJournal<SystemEvent> {
        use obzenflow_core::journal::archive::manifest::{
            EFFECT_BINDING_DESCRIPTOR_CAPABILITY, OBSERVABILITY_CAPTURE_CAPABILITY,
        };
        use obzenflow_core::{FlowId, Journal, JournalOwner, SystemId};
        let system = SystemId::new();
        let flow = FlowId::new();
        let journal = crate::journal::DiskJournal::<SystemEvent>::with_owner_in_run(
            dir.join("system.log"),
            JournalOwner::system(system),
            flow,
            JournalSync::Off,
        )
        .unwrap();
        for event in lifecycle(&obzenflow_core::event::SystemEventFactory::new(system)) {
            journal.append(event, Default::default()).await.unwrap();
        }
        let manifest = RunManifest {
            journal_schema_version: JOURNAL_SCHEMA_VERSION.into(),
            obzenflow_version: env!("CARGO_PKG_VERSION").into(),
            flow_id: flow.to_string(),
            recorded_flow_id: None,
            pipeline_writer_id: system.into(),
            flow_name: "inspected".into(),
            created_at: chrono::Utc::now(),
            replay: None,
            resume: None,
            interrupted_appends: Vec::new(),
            stages: Default::default(),
            system_journal_file: "system.log".into(),
            metrics_journals: None,
            effective_config: None,
            capabilities: [
                (OBSERVABILITY_CAPTURE_CAPABILITY.into(), 1),
                (EFFECT_BINDING_DESCRIPTOR_CAPABILITY.into(), 1),
            ]
            .into(),
            bounded_direct_fact_admission: Vec::new(),
            cycle_members: Vec::new(),
        };
        std::fs::write(
            dir.join(RUN_MANIFEST_FILENAME),
            serde_json::to_vec(&manifest).unwrap(),
        )
        .unwrap();
        journal
    }

    async fn inspected(dir: &Path) -> String {
        let mut out = Vec::new();
        inspect(dir, None, None, &mut out).await.unwrap();
        String::from_utf8(out).unwrap()
    }

    fn completed(factory: &obzenflow_core::event::SystemEventFactory) -> SystemEvent {
        factory.pipeline_completed(
            obzenflow_core::event::types::DurationMs(1),
            obzenflow_core::metrics::FlowLifecycleMetricsSnapshot {
                events_in_total: 0,
                events_out_total: 0,
                errors_total: 0,
            },
        )
    }

    #[tokio::test]
    async fn inspect_held_lock_reports_still_running() {
        let dir = tempfile::tempdir().unwrap();
        let writer = recorded_run(dir.path(), |factory| vec![factory.pipeline_starting()]).await;
        let output = inspected(dir.path()).await;
        assert!(output.contains("\nstatus:     still running\n"), "{output}");
        assert!(
            output.contains(&format!(
                "\nreplay:     ObzenFlow cannot replay {}, because the run still has a live \
                 writer.\n            Stop the run. Then replay it again.\n",
                dir.path().display()
            )),
            "{output}"
        );
        drop(writer);
    }

    #[tokio::test]
    async fn inspect_missing_terminal_record_reports_interrupted() {
        let dir = tempfile::tempdir().unwrap();
        drop(recorded_run(dir.path(), |factory| vec![factory.pipeline_starting()]).await);
        let output = inspected(dir.path()).await;
        assert!(
            output.contains("\nstatus:     interrupted (no terminal record)\n"),
            "{output}"
        );
        assert!(output.contains("\nadmission:  for the flow this run recorded\n"));
        assert!(
            output.contains(&format!(
                "\nresume:     admitted: --resume-from {}\n",
                dir.path().display()
            )),
            "{output}"
        );
    }

    #[tokio::test]
    async fn inspect_not_started_record_reports_not_started() {
        let dir = tempfile::tempdir().unwrap();
        drop(recorded_run(dir.path(), |factory| vec![factory.pipeline_not_started()]).await);
        let output = inspected(dir.path()).await;
        assert!(output.contains("\nstatus:     not started\n"), "{output}");
        assert!(
            output.contains(&format!(
                "\nreplay:     ObzenFlow cannot replay {}, because the run did not start.\n",
                dir.path().display()
            )),
            "{output}"
        );
    }

    #[tokio::test]
    async fn inspect_corrupt_frame_reports_refusal() {
        let dir = tempfile::tempdir().unwrap();
        drop(
            recorded_run(dir.path(), |factory| {
                vec![factory.pipeline_starting(), completed(factory)]
            })
            .await,
        );
        let system_log = dir.path().join("system.log");
        let mut bytes = std::fs::read(&system_log).unwrap();
        bytes[40] ^= 0xff;
        std::fs::write(&system_log, bytes).unwrap();
        let output = inspected(dir.path()).await;
        let status = output
            .lines()
            .find(|line| line.starts_with("status:"))
            .unwrap();
        assert!(
            status.starts_with("status:     unknown (") && status.contains("system.log"),
            "{output}"
        );
        assert!(
            output.contains("\nreplay:     Failed to open replay archive: "),
            "{output}"
        );
    }

    #[tokio::test]
    async fn inspect_closed_writer_returns_error() {
        struct ClosedPipe;
        impl Write for ClosedPipe {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let dir = tempfile::tempdir().unwrap();
        drop(
            recorded_run(dir.path(), |factory| {
                vec![factory.pipeline_starting(), completed(factory)]
            })
            .await,
        );
        let error = inspect(dir.path(), None, None, &mut ClosedPipe)
            .await
            .unwrap_err();
        assert!(
            matches!(
                &error,
                JournalInspectError::Io { source, .. }
                    if source.kind() == std::io::ErrorKind::BrokenPipe
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn open_reader_later_append_returns_interrupted() {
        use obzenflow_core::{Journal, JournalOwner, StageId};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stage.log");
        let stage = StageId::new();
        let journal = crate::journal::DiskJournal::<ChainEvent>::with_owner(
            path.clone(),
            JournalOwner::stage(stage),
            JournalSync::Off,
        )
        .unwrap();
        for _ in 0..3 {
            journal
                .append(
                    obzenflow_core::event::ChainEventFactory::data_event(
                        obzenflow_core::WriterId::from(stage),
                        "application.null",
                        std::num::NonZeroU32::MIN,
                        serde_json::Value::Null,
                    ),
                    Default::default(),
                )
                .await
                .unwrap();
        }
        drop(journal);
        let full = std::fs::read(&path).unwrap();
        let mut frames = Vec::new();
        let mut scan = BufReader::new(full.as_slice());
        let mut buf = Vec::new();
        while let Some((consumed, _)) = read_frame_sync(&mut scan, &mut buf).unwrap() {
            frames.push(consumed);
        }
        let partial = frames[0] + frames[1] / 2;
        std::fs::write(&path, &full[..partial]).unwrap();
        let mut reader = open_reader(&path).unwrap();
        std::fs::write(&path, &full).unwrap();

        let mut decoder = Decoder::new(&path);
        let mut offset = 0u64;
        let mut dispositions = Vec::new();
        while let Some((consumed, termination)) = read_frame_sync(&mut reader, &mut buf).unwrap() {
            let record_offset = offset;
            offset += consumed as u64;
            let disposition = dispose(
                classify_frame::<ChainEvent>(&buf, &mut decoder, record_offset),
                termination,
                ReadPolicy::SealedScan {
                    tolerate_torn_tail: true,
                },
                || tail_after(&path, &buf, record_offset, &mut reader),
            );
            dispositions.push(match disposition {
                Disposition::Yield(_) => "yield",
                Disposition::Interrupted(_) => "interrupted",
                Disposition::EndOfCommittedRecords => "end",
                Disposition::Skip => "skip",
                Disposition::Corrupt(_) => "corrupt",
            });
        }
        assert_eq!(dispositions, ["yield", "interrupted"]);
    }

    #[test]
    fn flow_evidence_cyclic_manifest_returns_members() {
        use obzenflow_core::journal::archive::manifest::{DeliveryOrder, RunManifestStage};
        use obzenflow_core::source::SourceContinuation;
        let stage = |stage_type, source_continuation| RunManifestStage {
            dsl_var: "stage".into(),
            stage_type,
            is_effectful: false,
            stage_id: "stage_id".into(),
            stage_logic_version: "1".into(),
            data_journal_file: "stage.log".into(),
            error_journal_file: "stage_error.log".into(),
            inbound: Vec::new(),
            delivery_order: DeliveryOrder::SingleInput,
            ai_models: Vec::new(),
            max_open_keys: None,
            source_continuation,
        };
        let manifest: RunManifest = serde_json::from_value(serde_json::json!({
            "journal_schema_version": JOURNAL_SCHEMA_VERSION,
            "obzenflow_version": "0.4.0",
            "flow_id": "flow_cyclic",
            "pipeline_writer_id": obzenflow_core::event::WriterId::from(obzenflow_core::SystemId::new()),
            "flow_name": "cyclic",
            "created_at": chrono::Utc::now(),
            "stages": {
                "web_orders": stage(
                    obzenflow_core::event::context::StageType::FiniteSource,
                    Some(SourceContinuation::Refuse),
                ),
                "triage": stage(obzenflow_core::event::context::StageType::Transform, None),
            },
            "system_journal_file": "system.log",
            "metrics_journals": null,
            "cycle_members": ["triage", "review"],
        }))
        .unwrap();
        let flow = recorded_flow_evidence(Path::new("/runs/r1"), &manifest).unwrap();
        assert_eq!(flow.cycle_members, ["review", "triage"]);
        assert_eq!(
            flow.sources,
            [("web_orders".to_string(), SourceContinuation::Refuse)]
        );
    }

    #[test]
    fn flow_evidence_unrecorded_continuation_fails() {
        use obzenflow_core::journal::archive::manifest::{DeliveryOrder, RunManifestStage};
        let mut manifest: RunManifest = serde_json::from_value(serde_json::json!({
            "journal_schema_version": JOURNAL_SCHEMA_VERSION,
            "obzenflow_version": "0.4.0",
            "flow_id": "flow_unrecorded",
            "pipeline_writer_id": obzenflow_core::event::WriterId::from(obzenflow_core::SystemId::new()),
            "flow_name": "unrecorded",
            "created_at": chrono::Utc::now(),
            "stages": {},
            "system_journal_file": "system.log",
            "metrics_journals": null,
            "cycle_members": [],
        }))
        .unwrap();
        manifest.stages.insert(
            "web_orders".into(),
            RunManifestStage {
                dsl_var: "web_orders".into(),
                stage_type: obzenflow_core::event::context::StageType::FiniteSource,
                is_effectful: false,
                stage_id: "stage_id".into(),
                stage_logic_version: "1".into(),
                data_journal_file: "web_orders.log".into(),
                error_journal_file: "web_orders_error.log".into(),
                inbound: Vec::new(),
                delivery_order: DeliveryOrder::SingleInput,
                ai_models: Vec::new(),
                max_open_keys: None,
                source_continuation: None,
            },
        );
        let error = recorded_flow_evidence(Path::new("/runs/r1"), &manifest).unwrap_err();
        assert_eq!(
            error.to_string(),
            "run manifest error at /runs/r1/run_manifest.json: source stage 'web_orders' \
             records no continuation"
        );
    }

    #[test]
    fn inspection_rejects_non_current_manifest_before_output_or_journal_access() {
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
