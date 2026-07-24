// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay-from-archive interfaces
//!
//! Onion architecture note: runtime services define stable interfaces; infra
//! provides concrete implementations (e.g., disk-backed archive readers).

use async_trait::async_trait;
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::ChainEventContent;
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

    #[error("Replay archive manifest version '{manifest_version}' is unsupported (supported: {supported}); re-record the run with this build of ObzenFlow")]
    UnsupportedManifestVersion {
        manifest_version: String,
        supported: &'static str,
    },

    #[error("Replay archive version {archive_version} is incompatible with running framework version {current_version} (major.minor must match)")]
    VersionMismatch {
        archive_version: String,
        current_version: String,
    },

    #[error("Replay archive system.log missing at {path}")]
    MissingSystemLog { path: PathBuf },

    #[error("Replay archive status is '{status:?}' and replay requires a completed or cancelled archive; re-run with --allow-incomplete-archive to override")]
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

    async fn open_effect_history(
        &self,
        stage_key: &str,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError>;

    fn source_data_journal_path(&self, stage_key: &str) -> Result<PathBuf, ReplayError>;

    fn archive_flow_id(&self) -> &str;

    fn archived_stage_id(&self, stage_key: &str) -> Result<StageId, ReplayError>;

    fn archive_status(&self) -> ArchiveStatus;

    fn status_derivation(&self) -> StatusDerivation;

    fn allow_incomplete_archive(&self) -> bool;

    fn source_stage_keys(&self) -> Vec<String>;

    fn archive_path(&self) -> &Path;

    /// Versioned archive capability advertised by the run manifest.
    fn manifest_capability(&self, _name: &str) -> Option<u32> {
        None
    }

    /// Stable descriptor-proved direct-fact metadata from the archive.
    fn bounded_direct_fact_admission(
        &self,
    ) -> &[obzenflow_core::journal::run_manifest::RunManifestDirectFactAdmission] {
        &[]
    }

    /// Maximum recorded resume generation in this archive (FLOWIP-120n). A
    /// resume run enters this plus one. The default answers for archives with
    /// no recorded catch-up boundary; `DiskReplayArchive` overrides it
    /// manifest-first with a source-journal scan fallback.
    fn max_recorded_generation(&self) -> obzenflow_core::ReaderGeneration {
        obzenflow_core::ReaderGeneration(0)
    }

    /// Maximum recorded admission sequence in this archive's source journals
    /// (FLOWIP-120n F18). The run's flow sequencer is seeded above it so live
    /// stamps order after every re-admitted sequence. The default answers for
    /// archives predating the field; `DiskReplayArchive` overrides it from the
    /// same single-pass source-journal scan as `max_recorded_generation`.
    fn max_recorded_admission_seq(&self) -> obzenflow_core::AdmissionSeq {
        obzenflow_core::AdmissionSeq(0)
    }
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
    archived_eof_kind: Option<EofKind>,
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
            archived_eof_kind: None,
        }
    }

    pub fn replayed_events(&self) -> u64 {
        self.replayed_events
    }

    pub fn skipped_events(&self) -> u64 {
        self.skipped_events
    }

    /// The archive's recorded completion kind, captured while skipping the
    /// archived EOF (FLOWIP-095k). `None`: the archive committed no EOF.
    pub fn archived_eof_kind(&self) -> Option<EofKind> {
        self.archived_eof_kind
    }

    pub async fn next_replayed_event(
        &mut self,
        _writer_id: WriterId,
        _stage_name: &str,
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
                // FLOWIP-120q: the reader applies the archive's status-derived
                // torn-tail policy, so `None` is always a clean end (true EOF or
                // a tolerated final torn tail) and corruption arrives as the
                // `Err` mapped above. The reader owns finality; the driver no
                // longer second-guesses it via `is_at_end`.
                return Ok(None);
            };

            let original_event = envelope.event;
            if !original_event.is_source_replayable() {
                if let ChainEventContent::FlowControl(fc) = &original_event.content {
                    if let Some(kind) = fc.eof_kind() {
                        self.archived_eof_kind = Some(kind);
                    }
                }
                self.skipped_events = self.skipped_events.saturating_add(1);
                continue;
            }

            let original_event_id = original_event.id;
            let mut new_event = original_event;
            new_event.flow_context = flow_context;
            new_event.replay_context = Some(obzenflow_core::event::context::ReplayContext {
                original_event_id,
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
