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
use std::io::BufReader;
use std::path::{Path, PathBuf};

use obzenflow_core::event::ChainEvent;
use obzenflow_core::journal::run_manifest::{
    RunManifest, JOURNAL_FORMAT_VERSION, RUN_MANIFEST_FILENAME, RUN_MANIFEST_VERSION,
};
use obzenflow_core::journal::ArchiveStatus;

use crate::journal::disk::replay_archive::derive_status_derivation_from_system_log;
use crate::journal::disk::scanner::{
    classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy,
};

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
        // FLOWIP-120q: refuse an archive whose framed record format this build
        // does not read, in the same raw-JSON gate as manifest_version.
        let journal_format_version = value.get("journal_format_version").and_then(|v| v.as_u64());
        if journal_format_version != Some(u64::from(JOURNAL_FORMAT_VERSION)) {
            return Err(SourceOpenError::Failed(VerifyError::Parse {
                path: manifest_path.clone(),
                message: format!(
                    "unsupported journal_format_version {journal_format_version:?} (supported: {JOURNAL_FORMAT_VERSION})"
                ),
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
            reader: BufReader::new(file),
            buf: Vec::new(),
            pending: std::collections::VecDeque::new(),
            journal: journal_file.to_string(),
            path,
            line_no: 0,
            byte_offset: 0,
            // FLOWIP-120q: verification is a sealed reader. Tolerate a final torn
            // tail only for a non-`Completed` source (cancelled-baseline prefix
            // comparison); fail loud on any committed corruption.
            policy: ReadPolicy::SealedScan {
                tolerate_torn_tail: self.status != ArchiveStatus::Completed,
            },
            done: false,
        }))
    }
}

struct JournalRows {
    reader: BufReader<File>,
    buf: Vec<u8>,
    pending: std::collections::VecDeque<ChainEvent>,
    journal: String,
    path: PathBuf,
    line_no: u64,
    byte_offset: u64,
    policy: ReadPolicy,
    done: bool,
}

impl Iterator for JournalRows {
    type Item = Result<ChainEvent, VerifyError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(event) = self.pending.pop_front() {
            return Some(Ok(event));
        }
        if self.done {
            return None;
        }
        loop {
            let (consumed, termination) = match read_frame_sync(&mut self.reader, &mut self.buf) {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    self.done = true;
                    return None;
                }
                Err(source) => {
                    self.done = true;
                    return Some(Err(VerifyError::Io {
                        path: self.path.clone(),
                        source,
                    }));
                }
            };
            let record_offset = self.byte_offset;
            self.byte_offset += consumed as u64;
            self.line_no += 1;
            if self.buf.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            match dispose(
                classify_frame::<ChainEvent>(&self.buf),
                termination,
                self.policy,
            ) {
                Disposition::Yield(frame) => {
                    self.pending
                        .extend(frame.into_records().into_iter().map(|record| record.event));
                    return self.pending.pop_front().map(Ok);
                }
                // A tolerated final torn tail ends the sealed history cleanly.
                Disposition::EndOfCommittedRecords | Disposition::Skip => {
                    self.done = true;
                    return None;
                }
                Disposition::Corrupt(problem) => {
                    self.done = true;
                    return Some(Err(VerifyError::CorruptRecord {
                        journal: self.journal.clone(),
                        line: self.line_no,
                        message: format!("at offset {record_offset}: {problem}"),
                    }));
                }
            }
        }
    }
}
