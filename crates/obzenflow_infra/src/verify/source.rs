// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Run-directory access for verification (FLOWIP-095j).
//!
//! [`RunSource`] is the seam between the pure comparator and the disk: the
//! walker and validation consume it, in-memory doubles implement it in tests,
//! and [`DiskRunSource`] is the production implementation over the framed
//! journal format. Reading is synchronous and streaming; both directories are
//! quiescent by the time verification runs, so no journal locking applies.

use std::fs::File;
use std::io::{BufRead, BufReader, Lines};
use std::path::{Path, PathBuf};

use obzenflow_core::event::ChainEvent;
use obzenflow_core::journal::run_manifest::{
    RunManifest, RUN_MANIFEST_FILENAME, RUN_MANIFEST_VERSION,
};
use obzenflow_core::journal::ArchiveStatus;

use crate::journal::disk::disk_journal::{parse_framed_record, ParseOutcome};
use crate::journal::disk::replay_archive::derive_status_derivation_from_system_log;

use super::error::{RefusalReason, VerifyError};

/// A verifiable run: its directory, manifest, terminal status, and streaming
/// access to its journals.
pub trait RunSource {
    fn run_dir(&self) -> &Path;
    fn manifest(&self) -> &RunManifest;
    fn status(&self) -> ArchiveStatus;
    fn stage_rows<'a>(
        &'a self,
        journal_file: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<ChainEvent, VerifyError>> + 'a>, VerifyError>;
}

/// Opening a run directory either yields a source, refuses for a stated
/// reason (exit 3), or fails operationally.
pub(crate) enum SourceOpenError {
    Refused(Box<RefusalReason>),
    Failed(VerifyError),
}

pub struct DiskRunSource {
    run_dir: PathBuf,
    manifest: RunManifest,
    status: ArchiveStatus,
}

impl DiskRunSource {
    pub(crate) fn open(run_dir: &Path) -> Result<Self, SourceOpenError> {
        let refused = |reason: RefusalReason| SourceOpenError::Refused(Box::new(reason));

        match std::fs::metadata(run_dir) {
            Ok(metadata) if metadata.is_dir() => {}
            _ => {
                return Err(refused(RefusalReason::ArchiveUnavailable {
                    path: run_dir.to_path_buf(),
                }))
            }
        }

        let manifest_path = run_dir.join(RUN_MANIFEST_FILENAME);
        if !manifest_path.exists() {
            return Err(refused(RefusalReason::ArchiveUnavailable {
                path: manifest_path,
            }));
        }

        let body = std::fs::read_to_string(&manifest_path).map_err(|source| {
            SourceOpenError::Failed(VerifyError::Io {
                path: manifest_path.clone(),
                source,
            })
        })?;

        // Version gate from the raw JSON before typed deserialization, so an
        // old archive refuses cleanly instead of failing on missing fields.
        let value: serde_json::Value = serde_json::from_str(&body).map_err(|e| {
            SourceOpenError::Failed(VerifyError::Parse {
                path: manifest_path.clone(),
                message: e.to_string(),
            })
        })?;
        let version = value
            .get("manifest_version")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        if version != RUN_MANIFEST_VERSION {
            return Err(refused(RefusalReason::ManifestVersion {
                path: manifest_path,
                found: version,
                supported: RUN_MANIFEST_VERSION.to_string(),
            }));
        }
        let manifest: RunManifest = serde_json::from_value(value).map_err(|e| {
            SourceOpenError::Failed(VerifyError::Parse {
                path: manifest_path.clone(),
                message: e.to_string(),
            })
        })?;

        let system_log = run_dir.join(&manifest.system_journal_file);
        let status = match derive_status_derivation_from_system_log(&system_log) {
            Ok(derivation) => derivation.chosen,
            Err(err) => {
                return Err(refused(RefusalReason::StatusGate {
                    run: run_dir.to_path_buf(),
                    status: format!("unknown ({err})"),
                }))
            }
        };

        Ok(Self {
            run_dir: run_dir.to_path_buf(),
            manifest,
            status,
        })
    }
}

impl RunSource for DiskRunSource {
    fn run_dir(&self) -> &Path {
        &self.run_dir
    }

    fn manifest(&self) -> &RunManifest {
        &self.manifest
    }

    fn status(&self) -> ArchiveStatus {
        self.status
    }

    fn stage_rows<'a>(
        &'a self,
        journal_file: &str,
    ) -> Result<Box<dyn Iterator<Item = Result<ChainEvent, VerifyError>> + 'a>, VerifyError> {
        let path = self.run_dir.join(journal_file);
        let file = File::open(&path).map_err(|source| VerifyError::Io {
            path: path.clone(),
            source,
        })?;
        Ok(Box::new(JournalRows {
            lines: BufReader::new(file).lines(),
            journal: journal_file.to_string(),
            path,
            line_no: 0,
        }))
    }
}

struct JournalRows {
    lines: Lines<BufReader<File>>,
    journal: String,
    path: PathBuf,
    line_no: u64,
}

impl Iterator for JournalRows {
    type Item = Result<ChainEvent, VerifyError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let line = match self.lines.next()? {
                Ok(line) => line,
                Err(source) => {
                    return Some(Err(VerifyError::Io {
                        path: self.path.clone(),
                        source,
                    }))
                }
            };
            self.line_no += 1;
            if line.trim().is_empty() {
                continue;
            }
            match parse_framed_record::<ChainEvent>(&line) {
                ParseOutcome::Complete(record) => return Some(Ok(record.event)),
                // A trailing partial record is a torn final write on a killed
                // run; the sealed history ends at the last complete record.
                ParseOutcome::Partial => continue,
                ParseOutcome::Corrupt(message) => {
                    return Some(Err(VerifyError::CorruptRecord {
                        journal: self.journal.clone(),
                        line: self.line_no,
                        message,
                    }))
                }
            }
        }
    }
}
