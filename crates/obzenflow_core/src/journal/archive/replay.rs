// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Archive access contracts. Providers implement these; Runtime executes replay.

use crate::event::context::StageType;
use crate::event::payloads::flow_control_payload::EofKind;
use crate::journal::archive_actions::ReplayVerb;
use crate::journal::factory::CurrentRunLocator;
use crate::journal::reader::JournalReader;
use crate::journal::{ArchiveStatus, StatusDerivation};
use crate::{ChainEvent, FlowId, StageId};
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// `ReplayError` tells why a provider cannot open, read or resume an archive.
/// Its `Display` text gives only the cause.
#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("Replay archive path is not a directory: {path}")]
    ArchivePathNotDirectory { path: PathBuf },

    #[error("Replay archive is missing run_manifest.json at {path}")]
    MissingManifest { path: PathBuf },

    #[error(
        "This build of ObzenFlow does not support journal schema version \
         {journal_schema_version}. It supports version {supported}."
    )]
    UnsupportedJournalSchemaVersion {
        journal_schema_version: String,
        supported: &'static str,
    },

    #[error(
        "This build of ObzenFlow does not support version {found:?} of the archive capability \
         '{capability}'. It supports version {supported}."
    )]
    UnsupportedArchiveCapability {
        capability: &'static str,
        found: Option<u64>,
        supported: u32,
    },

    #[error("The replay archive has no system.log at {path}.")]
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

    #[error("An I/O operation on the replay archive failed. {message}: {source}")]
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
        "ObzenFlow cannot {} {}. Its reconstruction did not complete, and the archive that it \
         resumed from, {}, is missing.",
        .verb.phrase(),
        .archive.display(),
        .ancestor.display()
    )]
    AncestorMissing {
        verb: ReplayVerb,
        archive: PathBuf,
        ancestor: PathBuf,
    },

    #[error(
        "ObzenFlow cannot {} {}. Its reconstruction did not complete, and ObzenFlow cannot read \
         the archive that it resumed from, {}. {}",
        .verb.phrase(),
        .archive.display(),
        .ancestor.display(),
        .source
    )]
    AncestorUnreadable {
        verb: ReplayVerb,
        archive: PathBuf,
        ancestor: PathBuf,
        source: Box<ReplayError>,
    },

    /// The ancestor that a torn resumed run merges with has no `system.log`.
    #[error(
        "ObzenFlow cannot {} {}. Its reconstruction did not complete, and the archive that it \
         resumed from, {}, has no system.log.",
        .verb.phrase(),
        .archive.display(),
        .ancestor.display()
    )]
    AncestorMissingSystemLog {
        verb: ReplayVerb,
        archive: PathBuf,
        ancestor: PathBuf,
        system_log: PathBuf,
    },

    /// A torn resumed run's source records are not a prefix of its ancestor's.
    #[error(
        "ObzenFlow cannot {} {}. Its reconstruction did not complete, and the archive that it \
         resumed from, {}, disagrees with it at source '{}'.",
        .verb.phrase(),
        .archive.display(),
        .ancestor.display(),
        .stage_key
    )]
    AncestorDisagrees {
        verb: ReplayVerb,
        archive: PathBuf,
        ancestor: PathBuf,
        stage_key: String,
    },

    /// Archives of one merged lineage record different identity namespaces.
    #[error(
        "ObzenFlow cannot {} {}. Its reconstruction did not complete, and the archive that it \
         resumed from, {}, records a different identity namespace.",
        .verb.phrase(),
        .archive.display(),
        .ancestor.display()
    )]
    NamespaceDisagrees {
        verb: ReplayVerb,
        archive: PathBuf,
        ancestor: PathBuf,
    },

    /// Another run resumed this run. A resume or a rebuild starts only from the
    /// head of a history.
    #[error(
        "ObzenFlow cannot {} {}, because another run resumed it. {} is the head of this history.",
        .verb.phrase(),
        .archive.display(),
        .head.display()
    )]
    AlreadyContinued {
        verb: ReplayVerb,
        archive: PathBuf,
        head: PathBuf,
    },

    #[error(
        "The continuation claim in {parent} names {successor}, but the directory {successor} is missing.",
        parent = .parent.display(),
        successor = .successor.display()
    )]
    ContinuationMissing { parent: PathBuf, successor: PathBuf },

    /// A resumed run whose parent carries no claim never took it over (B12).
    #[error(
        "ObzenFlow cannot {} {}, because it stopped before it took over {}.",
        .verb.phrase(),
        .archive.display(),
        .parent.display()
    )]
    StoppedBeforeTakeover {
        verb: ReplayVerb,
        archive: PathBuf,
        parent: PathBuf,
    },

    /// A resumed run that lost its parent's claim to another run (B12).
    #[error(
        "ObzenFlow cannot {} {}, because {} continued {} and this run did not.",
        .verb.phrase(),
        .archive.display(),
        .head.display(),
        .parent.display()
    )]
    ContinuedByAnother {
        verb: ReplayVerb,
        archive: PathBuf,
        parent: PathBuf,
        head: PathBuf,
    },

    /// The run's location cannot hold the claim a resume records (B12).
    #[error(
        "ObzenFlow cannot resume {}, because its run location is not writable. The resume \
         cannot record that it continues the run.",
        .archive.display()
    )]
    LocationNotWritable { archive: PathBuf },

    /// Resuming a replay resumes the run it reproduces, which is missing (B13).
    #[error(
        "ObzenFlow cannot {} {}, because the run that it replays, {}, is missing.",
        .verb.phrase(),
        .replay.display(),
        .replayed.display()
    )]
    ReplayedRunMissing {
        verb: ReplayVerb,
        replay: PathBuf,
        replayed: PathBuf,
    },

    /// A rebuild run's link leads to the run that it rebuilt, which is missing.
    #[error(
        "ObzenFlow cannot {} {}, because the run that it rebuilt, {}, is missing.",
        .verb.phrase(),
        .rebuild.display(),
        .rebuilt.display()
    )]
    RebuiltRunMissing {
        verb: ReplayVerb,
        rebuild: PathBuf,
        rebuilt: PathBuf,
    },
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

    /// The identity namespace the archive's manifest records, if any.
    fn recorded_flow_id(&self) -> Option<&str> {
        None
    }

    /// Each stage's recorded inbound edges and delivery order.
    fn stage_orders(&self) -> super::certification::StageOrders {
        super::certification::StageOrders::default()
    }

    /// The stage that wrote each archived data journal, keyed by the journal
    /// writer its records name.
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

    /// The model specs a stage recorded, one per AI effect.
    fn recorded_ai_models(&self, _: &str) -> Vec<crate::ai::AiModelSpec> {
        Vec::new()
    }

    /// The open-key limit a keyed AI stage recorded.
    fn recorded_max_open_keys(&self, _: &str) -> Option<u64> {
        None
    }

    /// `recorded_guarantees` returns the guarantees that the manifest of the
    /// archive records for a stage.
    fn recorded_guarantees(&self, _: &str) -> Vec<super::manifest::EffectGuarantee> {
        Vec::new()
    }

    /// Each journal of this archive, and of the ancestors it merges, that ends
    /// at an interrupted append; an ancestor's entries name it.
    fn interrupted_appends(
        &self,
    ) -> &[crate::journal::archive::manifest::RunManifestInterruptedAppend] {
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
