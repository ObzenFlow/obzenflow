// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Archive access contracts. Providers implement these; Runtime executes replay.

use crate::event::context::StageType;
use crate::journal::reader::JournalReader;
use crate::journal::{ArchiveStatus, StatusDerivation};
use crate::{ChainEvent, StageId};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("Replay archive path is not a directory: {path}")]
    ArchivePathNotDirectory { path: PathBuf },

    #[error("Replay archive is missing run_manifest.json at {path}")]
    MissingManifest { path: PathBuf },

    #[error("unsupported provenance schema version: {manifest_version} (supported: {supported}); re-record the run with this build of ObzenFlow")]
    UnsupportedManifestVersion {
        manifest_version: String,
        supported: &'static str,
    },

    #[error("Replay archive capability '{capability}' has unsupported version {found:?} (supported: {supported}); re-record the run with this build of ObzenFlow")]
    UnsupportedArchiveCapability {
        capability: &'static str,
        found: Option<u64>,
        supported: u32,
    },

    #[error("Replay archive build version {archive_version} does not exactly match running framework version {current_version}; re-record the run with this build of ObzenFlow")]
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
    ) -> &[crate::journal::archive::manifest::RunManifestDirectFactAdmission] {
        &[]
    }

    /// Maximum recorded resume generation in this archive (FLOWIP-120n). A
    /// resume run enters this plus one. The default answers for archives with
    /// no recorded catch-up boundary; `DiskReplayArchive` overrides it
    /// manifest-first with a source-journal scan fallback.
    fn max_recorded_generation(&self) -> crate::ReaderGeneration {
        crate::ReaderGeneration(0)
    }

    /// Maximum recorded admission sequence in this archive's source journals
    /// (FLOWIP-120n F18). The run's flow sequencer is seeded above it so live
    /// stamps order after every re-admitted sequence. The default answers for
    /// archives predating the field; `DiskReplayArchive` overrides it from the
    /// same single-pass source-journal scan as `max_recorded_generation`.
    fn max_recorded_admission_seq(&self) -> crate::AdmissionSeq {
        crate::AdmissionSeq(0)
    }
}
