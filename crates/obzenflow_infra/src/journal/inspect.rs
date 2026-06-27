// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! JSONL export and human inspection over a run's framed journals (FLOWIP-120q).
//!
//! The raw `.log` files are internal framed storage (`<len>:<crc>:<json>`). This
//! module is the supported public projection: `export_jsonl` writes one JSON
//! object per committed `LogRecord`, and `inspect` prints a filtered, human
//! view. Both go through the same sealed scanner and policy as replay and
//! verification, so they fail loud on corruption with path and record position
//! and never strip prefixes with a regex.

use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use obzenflow_core::event::{ChainEvent, JournalEvent, SystemEvent};
use obzenflow_core::journal::run_manifest::{
    RunManifest, JOURNAL_FORMAT_VERSION, RUN_MANIFEST_FILENAME, RUN_MANIFEST_VERSION,
};
use obzenflow_core::journal::ArchiveStatus;
use thiserror::Error;

use super::disk::replay_archive::derive_status_derivation_from_system_log;
use super::disk::scanner::{classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy};

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
    let policy = archive_policy(run_dir, &manifest);

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
    let policy = archive_policy(run_dir, &manifest);
    let status = archive_status(run_dir, &manifest);

    println!("flow_id:    {}", manifest.flow_id);
    println!("flow_name:  {}", manifest.flow_name);
    println!("status:     {status:?}");
    println!(
        "format:     manifest v{}, journal v{}",
        manifest.manifest_version, manifest.journal_format_version
    );
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

    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|source| JournalInspectError::Io {
            path: path.to_path_buf(),
            source,
        })?
    {
        let record_offset = offset;
        offset += consumed as u64;
        if buf.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match dispose(classify_frame::<R>(&buf), termination, policy) {
            Disposition::Yield(record) => {
                serde_json::to_writer(&mut *out, &record).map_err(|e| JournalInspectError::Io {
                    path: path.to_path_buf(),
                    source: std::io::Error::other(e.to_string()),
                })?;
                out.write_all(b"\n")
                    .map_err(|source| JournalInspectError::Io {
                        path: path.to_path_buf(),
                        source,
                    })?;
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

    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|source| JournalInspectError::Io {
            path: path.to_path_buf(),
            source,
        })?
    {
        let record_offset = offset;
        offset += consumed as u64;
        if buf.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        match dispose(classify_frame::<ChainEvent>(&buf), termination, policy) {
            Disposition::Yield(record) => {
                let ty = record.event.event_type();
                if event_type.is_some_and(|filter| filter != ty.as_str()) {
                    continue;
                }
                println!("  {record_offset:>10}  {ty}");
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

fn archive_status(run_dir: &Path, manifest: &RunManifest) -> ArchiveStatus {
    let system_log = run_dir.join(&manifest.system_journal_file);
    derive_status_derivation_from_system_log(&system_log)
        .map(|derivation| derivation.chosen)
        .unwrap_or(ArchiveStatus::Unknown)
}

/// Same sealed/full-scan policy as replay and verification: tolerate a final
/// torn tail only on a non-`Completed` archive.
fn archive_policy(run_dir: &Path, manifest: &RunManifest) -> ReadPolicy {
    ReadPolicy::SealedScan {
        tolerate_torn_tail: archive_status(run_dir, manifest) != ArchiveStatus::Completed,
    }
}

fn load_manifest(run_dir: &Path) -> Result<RunManifest, JournalInspectError> {
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

    let manifest_version = value
        .get("manifest_version")
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    if manifest_version != RUN_MANIFEST_VERSION {
        return Err(JournalInspectError::Manifest {
            path: manifest_path,
            message: format!(
                "unsupported manifest_version {manifest_version:?} (supported: {RUN_MANIFEST_VERSION})"
            ),
        });
    }
    let journal_format_version = value.get("journal_format_version").and_then(|v| v.as_u64());
    if journal_format_version != Some(u64::from(JOURNAL_FORMAT_VERSION)) {
        return Err(JournalInspectError::Manifest {
            path: manifest_path,
            message: format!(
                "unsupported journal_format_version {journal_format_version:?} (supported: {JOURNAL_FORMAT_VERSION})"
            ),
        });
    }

    serde_json::from_value(value).map_err(|e| JournalInspectError::Manifest {
        path: manifest_path,
        message: e.to_string(),
    })
}
