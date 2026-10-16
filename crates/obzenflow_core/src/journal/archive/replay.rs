// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Archive access contracts. Providers implement these; Runtime executes replay.

use crate::event::context::StageType;
use crate::event::payloads::flow_control_payload::EofKind;
use crate::journal::factory::CurrentRunLocator;
use crate::journal::reader::JournalReader;
use crate::journal::{ArchiveStatus, StatusDerivation};
use crate::{ChainEvent, FlowId, StageId};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("Replay archive path is not a directory: {path}")]
    ArchivePathNotDirectory { path: PathBuf },

    #[error("Replay archive is missing run_manifest.json at {path}")]
    MissingManifest { path: PathBuf },

    #[error("unsupported journal schema version: {journal_schema_version} (supported: {supported}); re-record the run with this build of ObzenFlow")]
    UnsupportedJournalSchemaVersion {
        journal_schema_version: String,
        supported: &'static str,
    },

    #[error("Replay archive capability '{capability}' has unsupported version {found:?} (supported: {supported}); re-record the run with this build of ObzenFlow")]
    UnsupportedArchiveCapability {
        capability: &'static str,
        found: Option<u64>,
        supported: u32,
    },

    #[error("Replay archive system.log missing at {path}")]
    MissingSystemLog { path: PathBuf },

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

    /// A torn resumed run merges with the archive it resumed from, which is
    /// missing or unreadable (FLOWIP-134i B6).
    #[error(
        "cannot resume {}: its reconstruction did not complete, and the archive it resumed \
         from, {}, is missing.\nRestore {}, then resume {} again.",
        .archive.display(),
        .ancestor.display(),
        .ancestor.display(),
        .archive.display()
    )]
    AncestorMissing { archive: PathBuf, ancestor: PathBuf },

    #[error(
        "cannot resume {}: its reconstruction did not complete, and the archive it resumed \
         from, {}, cannot be read: {}.\nRestore {}, then resume {} again.",
        .archive.display(),
        .ancestor.display(),
        .source,
        .ancestor.display(),
        .archive.display()
    )]
    AncestorUnreadable {
        archive: PathBuf,
        ancestor: PathBuf,
        source: Box<ReplayError>,
    },

    /// A torn resumed run's source records are not a prefix of its ancestor's.
    #[error(
        "cannot resume {}: its reconstruction did not complete, and the archive it resumed \
         from, {}, disagrees with it at source '{}'.\nRestore {} as it was recorded, then \
         resume {} again.",
        .archive.display(),
        .ancestor.display(),
        .stage_key,
        .ancestor.display(),
        .archive.display()
    )]
    AncestorDisagrees {
        archive: PathBuf,
        ancestor: PathBuf,
        stage_key: String,
    },

    /// Archives of one merged lineage record different identity namespaces
    /// (FLOWIP-134k B8).
    #[error(
        "cannot resume {}: its reconstruction did not complete, and the archive it resumed \
         from, {}, records a different identity namespace.\nRestore {} as it was recorded, \
         then resume {} again.",
        .archive.display(),
        .ancestor.display(),
        .ancestor.display(),
        .archive.display()
    )]
    NamespaceDisagrees { archive: PathBuf, ancestor: PathBuf },

    /// Only the head of a history resumes (FLOWIP-134i B7).
    #[error(
        "cannot resume {}: it was already resumed by {}, the head of this history.\nResume {} \
         instead.",
        .archive.display(),
        .head.display(),
        .head.display()
    )]
    AlreadyContinued { archive: PathBuf, head: PathBuf },

    /// A resumed run whose parent carries no claim never took it over (B12).
    #[error(
        "cannot resume {}: it stopped before taking over {}.\nResume {} instead.",
        .archive.display(),
        .parent.display(),
        .parent.display()
    )]
    StoppedBeforeTakeover { archive: PathBuf, parent: PathBuf },

    /// A resumed run that lost its parent's claim to another run (B12).
    #[error(
        "cannot resume {}: {} was continued by {}, not by this run.\nResume {} instead.",
        .archive.display(),
        .parent.display(),
        .head.display(),
        .head.display()
    )]
    ContinuedByAnother {
        archive: PathBuf,
        parent: PathBuf,
        head: PathBuf,
    },

    /// The run's location cannot hold the claim a resume records (B12).
    #[error(
        "cannot resume {}: its run location is not writable, so this resume cannot record \
         that it continues it.\nMake it writable, or copy it elsewhere to start a separate \
         history.",
        .archive.display()
    )]
    LocationNotWritable { archive: PathBuf },

    /// Resuming a replay resumes the run it reproduces, which is missing (B13).
    #[error(
        "cannot resume {}: the run it replays, {}, is missing.\nRestore {}, then resume {} \
         again.",
        .replay.display(),
        .replayed.display(),
        .replayed.display(),
        .replay.display()
    )]
    ReplayedRunMissing { replay: PathBuf, replayed: PathBuf },
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

    /// The lineage namespace the archive's manifest records (FLOWIP-134k
    /// B8); `None` for an archive written before B8.
    fn recorded_flow_id(&self) -> Option<&str> {
        None
    }

    /// Each stage's recorded inbound edges and delivery order, which order
    /// certification compares with the resumed flow (FLOWIP-134k B3).
    fn stage_orders(&self) -> super::certification::StageOrders {
        super::certification::StageOrders::default()
    }

    /// The stage that wrote each archived data journal, by the journal writer
    /// its records name, so a sink can key its receipts by upstream edge
    /// (FLOWIP-134k B3).
    fn journal_stages(&self) -> std::collections::HashMap<crate::JournalWriterId, String> {
        std::collections::HashMap::new()
    }

    fn archived_stage_id(&self, stage_key: &str) -> Result<StageId, ReplayError>;

    fn archive_status(&self) -> ArchiveStatus;

    fn status_derivation(&self) -> StatusDerivation;

    fn allow_incomplete_archive(&self) -> bool;

    fn source_stage_keys(&self) -> Vec<String>;

    /// The EOF kind a source journal committed, `None` when it committed none
    /// (FLOWIP-134i B8).
    fn recorded_source_eof(&self, stage_key: &str) -> Option<EofKind>;

    /// The committed frontier a source journal recorded, folded as a replay
    /// reads it (FLOWIP-134i B22).
    fn recorded_source_frontier(&self, stage_key: &str) -> crate::source::RecordedSourceFrontier;

    fn archive_path(&self) -> &Path;

    /// Take this archive's write-once continuation claim for the run resuming
    /// it (FLOWIP-134i B12), once that run's manifest is durable and before
    /// any of its stages exists. A claim already present refuses, naming the
    /// head of the history.
    fn claim_continuation(
        &self,
        successor: FlowId,
        location: &CurrentRunLocator,
    ) -> Result<(), ReplayError>;

    /// The archives a resume merges with, nearest first (FLOWIP-134i B6):
    /// each ancestor up to the first that recorded reconstruction completion.
    /// Admission verified that every source's records agree across them.
    fn ancestors(&self) -> Vec<std::sync::Arc<dyn ReplayArchive>> {
        Vec::new()
    }

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
    /// no recorded catch-up boundary; `DiskReplayArchive` derives it from
    /// committed boundaries in the source journals.
    fn max_recorded_generation(&self) -> crate::ReaderGeneration {
        crate::ReaderGeneration(0)
    }
}
