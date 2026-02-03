// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Disk-backed replay archive implementation (FLOWIP-095a).
//!
//! This is an outer-layer implementation that reads `run_manifest.json` and
//! archived disk journals, exposed to runtime services via the `ReplayArchive`
//! trait.

use super::disk_journal::parse_framed_record;
use super::disk_journal_reader::DiskJournalReader;
use async_trait::async_trait;
use obzenflow_core::build_info::OBZENFLOW_VERSION;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::run_manifest::{
    RunManifest, RUN_MANIFEST_FILENAME, RUN_MANIFEST_VERSION,
};
use obzenflow_core::journal::JournalReader;
use obzenflow_core::journal::{ArchiveStatus, StatusDerivation};
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_runtime_services::replay::{ReplayArchive, ReplayError};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct DiskReplayArchive {
    archive_path: PathBuf,
    manifest: RunManifest,
    status: ArchiveStatus,
    status_derivation: StatusDerivation,
    allow_incomplete_archive: bool,
    read_write_lock: Arc<RwLock<()>>,
}

impl DiskReplayArchive {
    pub async fn open(
        archive_path: PathBuf,
        allow_incomplete_archive: bool,
    ) -> Result<Self, ReplayError> {
        let metadata = std::fs::metadata(&archive_path).map_err(|e| ReplayError::Io {
            message: format!(
                "Failed to read replay archive metadata: {}",
                archive_path.display()
            ),
            source: e,
        })?;
        if !metadata.is_dir() {
            return Err(ReplayError::ArchivePathNotDirectory { path: archive_path });
        }

        let manifest_path = archive_path.join(RUN_MANIFEST_FILENAME);
        if !manifest_path.exists() {
            return Err(ReplayError::MissingManifest {
                path: manifest_path,
            });
        }

        let manifest_body =
            std::fs::read_to_string(&manifest_path).map_err(|e| ReplayError::Io {
                message: format!("Failed to read run manifest: {}", manifest_path.display()),
                source: e,
            })?;
        let manifest: RunManifest =
            serde_json::from_str(&manifest_body).map_err(|e| ReplayError::Parse {
                message: format!(
                    "Failed to parse run manifest JSON at {}: {}",
                    manifest_path.display(),
                    e
                ),
            })?;

        if manifest.manifest_version != RUN_MANIFEST_VERSION {
            return Err(ReplayError::UnsupportedManifestVersion {
                manifest_version: manifest.manifest_version.clone(),
                supported: RUN_MANIFEST_VERSION,
            });
        }

        if is_newer_semver(&manifest.obzenflow_version, OBZENFLOW_VERSION) {
            return Err(ReplayError::VersionMismatch {
                archive_version: manifest.obzenflow_version.clone(),
                current_version: OBZENFLOW_VERSION.to_string(),
            });
        }

        let system_log_path = archive_path.join(&manifest.system_journal_file);
        let status_derivation = match derive_status_derivation_from_system_log(&system_log_path) {
            Ok(derivation) => derivation,
            Err(err) => {
                if allow_incomplete_archive {
                    tracing::warn!(
                        archive_path = %archive_path.display(),
                        system_log_path = %system_log_path.display(),
                        error = %err,
                        "Replay archive system.log could not be read; proceeding due to allow_incomplete_archive"
                    );
                    StatusDerivation {
                        terminal_events_found: 0,
                        chosen: ArchiveStatus::Unknown,
                        warning: Some(err.to_string()),
                    }
                } else {
                    return Err(err);
                }
            }
        };
        let status = status_derivation.chosen;

        if status != ArchiveStatus::Completed && !allow_incomplete_archive {
            return Err(ReplayError::IncompleteArchive { status });
        }

        Ok(Self {
            archive_path,
            manifest,
            status,
            status_derivation,
            allow_incomplete_archive,
            read_write_lock: Arc::new(RwLock::new(())),
        })
    }

    pub fn status(&self) -> ArchiveStatus {
        self.status
    }

    pub fn status_derivation(&self) -> StatusDerivation {
        self.status_derivation.clone()
    }
}

#[async_trait]
impl ReplayArchive for DiskReplayArchive {
    async fn open_source_reader(
        &self,
        stage_key: &str,
        expected_type: StageType,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        let stage_info =
            self.manifest
                .stages
                .get(stage_key)
                .ok_or_else(|| ReplayError::StageNotInManifest {
                    stage_key: stage_key.to_string(),
                })?;

        if !stage_info.stage_type.is_source() || !expected_type.is_source() {
            return Err(ReplayError::StageTypeMismatch {
                stage_key: stage_key.to_string(),
                archived_type: stage_info.stage_type,
                expected_type,
            });
        }

        if self.status != ArchiveStatus::Completed && !self.allow_incomplete_archive {
            return Err(ReplayError::IncompleteArchive {
                status: self.status,
            });
        }

        let data_path = self.archive_path.join(&stage_info.data_journal_file);
        if !data_path.exists() {
            return Err(ReplayError::MissingJournal { path: data_path });
        }

        let reader = DiskJournalReader::<ChainEvent>::open_existing(
            data_path,
            JournalId::new(),
            self.read_write_lock.clone(),
        )
        .await
        .map_err(|e| ReplayError::Io {
            message: "Failed to open archived journal reader".to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;

        Ok(Box::new(reader))
    }

    fn source_data_journal_path(&self, stage_key: &str) -> Result<PathBuf, ReplayError> {
        let stage_info =
            self.manifest
                .stages
                .get(stage_key)
                .ok_or_else(|| ReplayError::StageNotInManifest {
                    stage_key: stage_key.to_string(),
                })?;
        Ok(self.archive_path.join(&stage_info.data_journal_file))
    }

    fn archive_flow_id(&self) -> &str {
        &self.manifest.flow_id
    }

    fn archived_stage_id(&self, stage_key: &str) -> Result<StageId, ReplayError> {
        let stage_info =
            self.manifest
                .stages
                .get(stage_key)
                .ok_or_else(|| ReplayError::StageNotInManifest {
                    stage_key: stage_key.to_string(),
                })?;

        StageId::from_str(&stage_info.stage_id).map_err(|e| ReplayError::Parse {
            message: format!(
                "Failed to parse archived stage_id '{}' for stage '{}': {}",
                stage_info.stage_id, stage_key, e
            ),
        })
    }

    fn archive_status(&self) -> ArchiveStatus {
        self.status
    }

    fn status_derivation(&self) -> StatusDerivation {
        self.status_derivation.clone()
    }

    fn allow_incomplete_archive(&self) -> bool {
        self.allow_incomplete_archive
    }

    fn source_stage_keys(&self) -> Vec<String> {
        let mut keys = self
            .manifest
            .stages
            .iter()
            .filter_map(|(k, v)| {
                if v.stage_type.is_source() {
                    Some(k.clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    fn archive_path(&self) -> &Path {
        &self.archive_path
    }
}

fn derive_status_derivation_from_system_log(path: &Path) -> Result<StatusDerivation, ReplayError> {
    if !path.exists() {
        return Err(ReplayError::MissingSystemLog {
            path: path.to_path_buf(),
        });
    }

    let file = File::open(path).map_err(|e| ReplayError::Io {
        message: format!("Failed to open system.log at {}", path.display()),
        source: e,
    })?;
    let reader = BufReader::new(file);

    let mut chosen = ArchiveStatus::Unknown;
    let mut terminal_events_found: u64 = 0;
    for (idx, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| ReplayError::Io {
            message: format!("Failed to read system.log at {}", path.display()),
            source: e,
        })?;
        if line.trim().is_empty() {
            continue;
        }

        match parse_framed_record::<SystemEvent>(&line) {
            super::disk_journal::ParseOutcome::Complete(record) => {
                if let obzenflow_core::event::SystemEventType::PipelineLifecycle(event) =
                    &record.event.event
                {
                    match event {
                        obzenflow_core::event::PipelineLifecycleEvent::Completed { .. } => {
                            terminal_events_found = terminal_events_found.saturating_add(1);
                            chosen = ArchiveStatus::Completed;
                        }
                        obzenflow_core::event::PipelineLifecycleEvent::Failed { .. } => {
                            terminal_events_found = terminal_events_found.saturating_add(1);
                            chosen = ArchiveStatus::Failed;
                        }
                        obzenflow_core::event::PipelineLifecycleEvent::Cancelled { .. } => {
                            terminal_events_found = terminal_events_found.saturating_add(1);
                            chosen = ArchiveStatus::Cancelled;
                        }
                        _ => {}
                    }
                }
            }
            super::disk_journal::ParseOutcome::Partial => {
                return Err(ReplayError::Parse {
                    message: format!(
                        "system.log appears partially written at line {} in {}",
                        idx + 1,
                        path.display()
                    ),
                });
            }
            super::disk_journal::ParseOutcome::Corrupt(e) => {
                return Err(ReplayError::Parse {
                    message: format!(
                        "system.log record corrupt at line {} in {}: {}",
                        idx + 1,
                        path.display(),
                        e
                    ),
                });
            }
        }
    }

    Ok(StatusDerivation {
        terminal_events_found,
        chosen,
        warning: if terminal_events_found > 1 {
            Some(
                "Multiple pipeline terminal events found in system.log; derived status uses last"
                    .to_string(),
            )
        } else {
            None
        },
    })
}

fn is_newer_semver(archive_version: &str, current_version: &str) -> bool {
    let Some(archive) = parse_semver_triplet(archive_version) else {
        return false;
    };
    let Some(current) = parse_semver_triplet(current_version) else {
        return false;
    };
    archive > current
}

fn parse_semver_triplet(version: &str) -> Option<(u64, u64, u64)> {
    let version = version.trim().strip_prefix('v').unwrap_or(version.trim());
    let mut parts = version.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next()?.parse().ok()?;
    let patch_str = parts.next()?;
    let patch_str = patch_str
        .split_once('-')
        .map(|(p, _)| p)
        .unwrap_or(patch_str);
    let patch = patch_str.parse().ok()?;
    Some((major, minor, patch))
}
