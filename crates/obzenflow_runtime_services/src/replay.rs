// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-from-archive interfaces (FLOWIP-095a).
//!
//! Onion architecture note: runtime services define stable interfaces; infra
//! provides concrete implementations (e.g., disk-backed archive readers).

use async_trait::async_trait;
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::{ArchiveStatus, StatusDerivation};
use obzenflow_core::WriterId;
use obzenflow_core::{ChainEvent, StageId};
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("Replay archive path is not a directory: {path}")]
    ArchivePathNotDirectory { path: PathBuf },

    #[error("Replay archive is missing run_manifest.json at {path}")]
    MissingManifest { path: PathBuf },

    #[error("Replay archive manifest version '{manifest_version}' is unsupported (supported: {supported})")]
    UnsupportedManifestVersion {
        manifest_version: String,
        supported: &'static str,
    },

    #[error("Replay archive was created with newer ObzenFlow version {archive_version} (current: {current_version})")]
    VersionMismatch {
        archive_version: String,
        current_version: String,
    },

    #[error("Replay archive system.log missing at {path}")]
    MissingSystemLog { path: PathBuf },

    #[error("Replay archive status is '{status:?}' and replay requires a completed archive; re-run with --allow-incomplete-archive to override")]
    IncompleteArchive { status: ArchiveStatus },

    #[error("Stage '{stage_key}' not found in run manifest")]
    StageNotInManifest { stage_key: String },

    #[error("Stage '{stage_key}' is not a source in archive (archived: {archived_type:?}, expected: {expected_type:?})")]
    StageTypeMismatch {
        stage_key: String,
        archived_type: StageType,
        expected_type: StageType,
    },

    #[error("Replay archive journal missing at {path}")]
    MissingJournal { path: PathBuf },

    #[error("Replay archive journal appears corrupted at position {record_position} in {path}: {message}")]
    CorruptedArchive {
        path: PathBuf,
        record_position: u64,
        message: String,
    },

    #[error("Replay archive I/O error: {message}")]
    Io {
        message: String,
        #[source]
        source: std::io::Error,
    },

    #[error("Replay archive parse error: {message}")]
    Parse { message: String },
}

#[async_trait]
pub trait ReplayArchive: Send + Sync {
    async fn open_source_reader(
        &self,
        stage_key: &str,
        expected_type: StageType,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError>;

    fn source_data_journal_path(&self, stage_key: &str) -> Result<PathBuf, ReplayError>;

    fn archive_flow_id(&self) -> &str;

    fn archived_stage_id(&self, stage_key: &str) -> Result<StageId, ReplayError>;

    fn archive_status(&self) -> ArchiveStatus;

    fn status_derivation(&self) -> StatusDerivation;

    fn allow_incomplete_archive(&self) -> bool;

    fn source_stage_keys(&self) -> Vec<String>;

    fn archive_path(&self) -> &Path;
}

#[derive(Debug, Clone)]
pub struct ReplayContextTemplate {
    pub original_flow_id: String,
    pub original_stage_id: StageId,
    pub archive_path: PathBuf,
}

pub struct ReplayDriver {
    archive_reader: Box<dyn JournalReader<ChainEvent>>,
    journal_path: PathBuf,
    replay_context: ReplayContextTemplate,
    replayed_events: u64,
    skipped_events: u64,
}

impl ReplayDriver {
    pub fn new(
        archive_reader: Box<dyn JournalReader<ChainEvent>>,
        journal_path: PathBuf,
        replay_context: ReplayContextTemplate,
    ) -> Self {
        Self {
            archive_reader,
            journal_path,
            replay_context,
            replayed_events: 0,
            skipped_events: 0,
        }
    }

    pub fn replayed_events(&self) -> u64 {
        self.replayed_events
    }

    pub fn skipped_events(&self) -> u64 {
        self.skipped_events
    }

    pub async fn next_replayed_event(
        &mut self,
        writer_id: WriterId,
        stage_name: &str,
        flow_context: FlowContext,
    ) -> Result<Option<ChainEvent>, ReplayError> {
        loop {
            let next =
                self.archive_reader
                    .next()
                    .await
                    .map_err(|e| ReplayError::CorruptedArchive {
                        path: self.journal_path.clone(),
                        record_position: self.archive_reader.position(),
                        message: e.to_string(),
                    })?;

            let Some(envelope) = next else {
                if self.archive_reader.is_at_end() {
                    return Ok(None);
                }
                return Err(ReplayError::CorruptedArchive {
                    path: self.journal_path.clone(),
                    record_position: self.archive_reader.position(),
                    message: "archive record appears partial".to_string(),
                });
            };

            let original_event = envelope.event;
            if !original_event.is_replayable() {
                self.skipped_events = self.skipped_events.saturating_add(1);
                continue;
            }

            let mut new_event = ChainEventFactory::source_event(
                writer_id,
                stage_name.to_string(),
                original_event.content.clone(),
            );
            new_event.flow_context = flow_context;
            new_event.intent = original_event.intent.clone();
            new_event.replay_context = Some(obzenflow_core::event::context::ReplayContext {
                original_event_id: original_event.id,
                original_flow_id: self.replay_context.original_flow_id.clone(),
                original_stage_id: self.replay_context.original_stage_id,
                archive_path: self.replay_context.archive_path.clone(),
                replayed_at: chrono::Utc::now(),
            });

            self.replayed_events = self.replayed_events.saturating_add(1);
            return Ok(Some(new_event));
        }
    }
}
