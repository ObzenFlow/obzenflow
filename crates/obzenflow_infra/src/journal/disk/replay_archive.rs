// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Disk-backed replay archive implementation
//!
//! This is an outer-layer implementation that reads `run_manifest.json` and
//! archived disk journals, exposed to runtime services via the `ReplayArchive`
//! trait.

use super::codec::Decoder;
use super::manifest_gate::require_current_journal_schema_version;
use super::reader::DiskJournalReader;
use super::scanner::{classify_frame, dispose, read_frame_sync, Disposition, ReadPolicy};
use async_trait::async_trait;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::{SystemEvent, SystemPayload};
use obzenflow_core::journal::archive::manifest::{
    RunManifest, EFFECT_BINDING_DESCRIPTOR_CAPABILITY, JOURNAL_SCHEMA_VERSION,
    RUN_MANIFEST_FILENAME,
};
use obzenflow_core::journal::archive::{ReplayArchive, ReplayError};
use obzenflow_core::journal::{ArchiveStatus, JournalReader, StatusDerivation};
use obzenflow_core::source::RecordedSourceFrontier;
use obzenflow_core::{ChainEvent, StageId};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
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
    max_recorded_generation: obzenflow_core::ReaderGeneration,
    /// Each source journal's committed EOF kind (FLOWIP-134i B8), from the
    /// same scan; under a merged lineage, the kind ending its merged records.
    recorded_source_eof: HashMap<String, EofKind>,
    /// Each source journal's committed frontier (FLOWIP-134i B22), from the
    /// same scan; under a merged lineage, the frontier of its merged records.
    recorded_frontier: HashMap<String, RecordedSourceFrontier>,
    /// Each source journal's count of records a replay re-admits or
    /// preserves, from the same scan.
    replayable_by_source: HashMap<String, u64>,
    /// The generation whose reconstruction this run recorded as complete
    /// (FLOWIP-134i B6).
    reconstruction_completed: Option<u64>,
    /// The runs a resume of this one merges with, nearest first (B6).
    ancestors: Vec<Arc<DiskReplayArchive>>,
    live_writer: bool,
    failure: obzenflow_core::journal::archive_actions::FailureEvidence,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Liveness {
    Probe,
    Stopped,
}

impl DiskReplayArchive {
    /// Open an archive to replay it. The archive's status never refuses here;
    /// admission decides from [`Self::evidence`].
    pub async fn open(
        archive_path: PathBuf,
        allow_incomplete_archive: bool,
    ) -> Result<Self, ReplayError> {
        Self::open_replay(archive_path, allow_incomplete_archive, Liveness::Probe).await
    }

    async fn open_replay(
        archive_path: PathBuf,
        allow_incomplete_archive: bool,
        liveness: Liveness,
    ) -> Result<Self, ReplayError> {
        let (archive_path, manifest) = Self::read_manifest(archive_path)?;
        let live_writer = liveness == Liveness::Probe
            && super::continuation::has_live_writer(&archive_path, &manifest)?;
        Self::open_manifest(
            archive_path,
            manifest,
            allow_incomplete_archive,
            live_writer,
        )
        .await
    }

    /// The evidence archive admission reads.
    pub fn evidence(&self) -> obzenflow_core::journal::archive_actions::ArchiveEvidence {
        use obzenflow_core::journal::archive_actions::{ArchiveEvidence, SourceEvidence};
        ArchiveEvidence {
            status: self.status,
            live_writer: self.live_writer,
            sources: self
                .source_stage_keys()
                .into_iter()
                .map(|stage_key| SourceEvidence {
                    finite: self.manifest.stages[&stage_key].stage_type == StageType::FiniteSource,
                    recorded_eof: self.recorded_source_eof(&stage_key),
                    frontier: self.recorded_source_frontier(&stage_key),
                    stage_key,
                })
                .collect(),
            failure: self.failure,
        }
    }

    /// The evidence of a run this process just finished writing, as `verb`
    /// would open it. The run must have stopped and be the head of its history.
    pub async fn written_run_evidence(
        run: PathBuf,
        verb: obzenflow_core::journal::archive_actions::ReplayVerb,
    ) -> Result<obzenflow_core::journal::archive_actions::ArchiveEvidence, ReplayError> {
        use obzenflow_core::journal::archive_actions::ReplayVerb;
        let archive = match verb {
            ReplayVerb::Replay => Self::open_replay(run, false, Liveness::Stopped).await?,
            ReplayVerb::Resume => Self::open_resume(run, false, Liveness::Stopped).await?,
        };
        Ok(archive.evidence())
    }

    /// An archive's canonical path and its gated manifest, before any journal
    /// is read.
    pub(crate) fn read_manifest(
        archive_path: PathBuf,
    ) -> Result<(PathBuf, RunManifest), ReplayError> {
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
        // Lineage links and claims name archives canonically (FLOWIP-134i B6).
        let archive_path = std::fs::canonicalize(&archive_path).map_err(|e| ReplayError::Io {
            message: format!(
                "Failed to resolve replay archive path: {}",
                archive_path.display()
            ),
            source: e,
        })?;

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
        // FLOWIP-095j: gate on the version from the raw JSON before typed
        // deserialization. Required fields added in newer journal schemas would
        // otherwise turn an old archive into a confusing missing-field parse error
        // instead of the clean unsupported-version refusal.
        let manifest_value: serde_json::Value =
            serde_json::from_str(&manifest_body).map_err(|e| ReplayError::Parse {
                message: format!(
                    "Failed to parse run manifest JSON at {}: {}",
                    manifest_path.display(),
                    e
                ),
            })?;
        if let Err(version) = require_current_journal_schema_version(&manifest_value) {
            return Err(ReplayError::UnsupportedJournalSchemaVersion {
                journal_schema_version: version.found().to_string(),
                supported: JOURNAL_SCHEMA_VERSION,
            });
        }
        // FLOWIP-132a: descriptor shape is an archive interpretation contract,
        // not an optional serde field. Gate the raw manifest before typed
        // deserialization or any system/stage journal scan so an older archive
        // cannot be mistaken for a portless binding.
        let binding_descriptor_version = manifest_value
            .get("capabilities")
            .and_then(|value| value.as_object())
            .and_then(|capabilities| capabilities.get(EFFECT_BINDING_DESCRIPTOR_CAPABILITY))
            .and_then(|value| value.as_u64());
        if binding_descriptor_version != Some(1) {
            return Err(ReplayError::UnsupportedArchiveCapability {
                capability: EFFECT_BINDING_DESCRIPTOR_CAPABILITY,
                found: binding_descriptor_version,
                supported: 1,
            });
        }
        super::manifest_gate::require_observability_capture(&manifest_value).map_err(|_| {
            ReplayError::UnsupportedArchiveCapability {
                capability:
                    obzenflow_core::journal::archive::manifest::OBSERVABILITY_CAPTURE_CAPABILITY,
                found: manifest_value["capabilities"]
                    [obzenflow_core::journal::archive::manifest::OBSERVABILITY_CAPTURE_CAPABILITY]
                    .as_u64(),
                supported: 1,
            }
        })?;
        let manifest: RunManifest =
            serde_json::from_value(manifest_value).map_err(|e| ReplayError::Parse {
                message: format!(
                    "Failed to parse run manifest JSON at {}: {}",
                    manifest_path.display(),
                    e
                ),
            })?;
        Ok((archive_path, manifest))
    }

    async fn open_manifest(
        archive_path: PathBuf,
        manifest: RunManifest,
        allow_incomplete_archive: bool,
        live_writer: bool,
    ) -> Result<Self, ReplayError> {
        // Journal incarnations are manifest-bound, including empty journals.
        // Incomplete admission cannot turn a conflicting descriptor into evidence.
        super::identity::validate_archive_identities(&archive_path, &manifest)
            .map_err(causal_admission_error)?;

        if live_writer {
            return Ok(Self {
                archive_path,
                manifest,
                status: ArchiveStatus::Unknown,
                status_derivation: StatusDerivation {
                    terminal_events_found: 0,
                    chosen: ArchiveStatus::Unknown,
                    warning: None,
                },
                allow_incomplete_archive,
                read_write_lock: Arc::new(RwLock::new(())),
                max_recorded_generation: obzenflow_core::ReaderGeneration(0),
                recorded_source_eof: HashMap::new(),
                recorded_frontier: HashMap::new(),
                replayable_by_source: HashMap::new(),
                reconstruction_completed: None,
                ancestors: Vec::new(),
                live_writer,
                failure: Default::default(),
            });
        }

        let system_log_path = archive_path.join(&manifest.system_journal_file);
        let (status_derivation, reconstruction_completed, contract_violation) =
            match scan_system_log(&system_log_path) {
                Ok(scan) => (
                    scan.derivation,
                    scan.reconstruction_completed,
                    scan.contract_violation,
                ),
                Err(err) => {
                    if allow_incomplete_archive
                        && !matches!(err, ReplayError::UnsupportedJournalSchemaVersion { .. })
                    {
                        tracing::warn!(
                            archive_path = %archive_path.display(),
                            system_log_path = %system_log_path.display(),
                            error = %err,
                            "Replay archive system.log could not be read; proceeding due to allow_incomplete_archive"
                        );
                        (
                            StatusDerivation {
                                terminal_events_found: 0,
                                chosen: ArchiveStatus::Unknown,
                                warning: Some(err.to_string()),
                            },
                            None,
                            false,
                        )
                    } else {
                        return Err(err);
                    }
                }
            };
        let status = status_derivation.chosen;
        let failure = if status == ArchiveStatus::Failed && manifest.resume.is_some() {
            scan_failure_evidence(&archive_path, &manifest, contract_violation)?
        } else {
            Default::default()
        };

        let scan = scan_source_journals(
            &archive_path,
            &manifest,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: status != ArchiveStatus::Completed,
            },
        )?;
        let entered_generation = manifest
            .resume
            .as_ref()
            .map(|resume| resume.resume_generation)
            .unwrap_or(0);
        if entered_generation > scan.max_generation.0 {
            tracing::debug!(
                archive_path = %archive_path.display(),
                manifest_generation = entered_generation,
                scanned_generation = scan.max_generation.0,
                "torn catch-up: the manifest's entered generation has no recorded \
                 boundary in any source journal; the journal scan is authoritative"
            );
        }

        Ok(Self {
            archive_path,
            manifest,
            status,
            status_derivation,
            allow_incomplete_archive,
            read_write_lock: Arc::new(RwLock::new(())),
            max_recorded_generation: scan.max_generation,
            recorded_source_eof: scan.eof_by_source,
            recorded_frontier: scan.frontier_by_source,
            replayable_by_source: scan.replayable_by_source,
            reconstruction_completed,
            ancestors: Vec::new(),
            live_writer,
            failure,
        })
    }

    /// Open the head of a history to resume it. A replay resolves to the run it
    /// reproduces, and a resume whose reconstruction never completed merges
    /// with its ancestors, which must agree. A live run opens without these checks.
    pub async fn open_for_resume(
        archive_path: PathBuf,
        allow_incomplete_archive: bool,
    ) -> Result<Self, ReplayError> {
        Self::open_resume(archive_path, allow_incomplete_archive, Liveness::Probe).await
    }

    async fn open_resume(
        archive_path: PathBuf,
        allow_incomplete_archive: bool,
        liveness: Liveness,
    ) -> Result<Self, ReplayError> {
        let (archive_path, manifest) = Self::resolve_replayed_run(archive_path)?;
        let live_writer = match liveness {
            Liveness::Probe => super::continuation::admit(&archive_path, &manifest)?,
            Liveness::Stopped => false,
        };
        let mut head = Self::open_manifest(
            archive_path,
            manifest,
            allow_incomplete_archive,
            live_writer,
        )
        .await?;
        if live_writer {
            return Ok(head);
        }
        let mut ancestors: Vec<Arc<Self>> = Vec::new();
        let mut parent = head.unfinished_parent();
        while let Some(ancestor) = parent {
            if ancestor == head.archive_path || ancestors.iter().any(|a| a.archive_path == ancestor)
            {
                return Err(ReplayError::Parse {
                    message: format!(
                        "the resume lineage of {} returns to {}",
                        head.archive_path.display(),
                        ancestor.display()
                    ),
                });
            }
            if !ancestor.exists() {
                return Err(ReplayError::AncestorMissing {
                    archive: head.archive_path.clone(),
                    ancestor,
                });
            }
            let opened = Self::open_replay(ancestor.clone(), true, Liveness::Stopped)
                .await
                .map_err(|error| ReplayError::AncestorUnreadable {
                    archive: head.archive_path.clone(),
                    ancestor,
                    source: Box::new(error),
                })?;
            parent = opened.unfinished_parent();
            ancestors.push(Arc::new(opened));
        }
        head.merge_lineage(ancestors)?;
        Ok(head)
    }

    /// The first run a chain of replays reproduces (B13): a replay adds
    /// nothing a resume reads.
    pub(crate) fn resolve_replayed_run(
        archive_path: PathBuf,
    ) -> Result<(PathBuf, RunManifest), ReplayError> {
        let (mut path, mut manifest) = Self::read_manifest(archive_path)?;
        let mut visited = vec![path.clone()];
        while let Some(replayed) = replayed_run(&manifest) {
            if visited.contains(&replayed) {
                return Err(ReplayError::Parse {
                    message: format!(
                        "the replays from {} return to {}",
                        visited[0].display(),
                        replayed.display()
                    ),
                });
            }
            if !replayed.exists() {
                return Err(ReplayError::ReplayedRunMissing {
                    replay: path,
                    replayed,
                });
            }
            (path, manifest) = Self::read_manifest(replayed)?;
            visited.push(path.clone());
        }
        Ok((path, manifest))
    }

    /// The run this one resumed from, while this run has not recorded its
    /// reconstruction as complete.
    fn unfinished_parent(&self) -> Option<PathBuf> {
        match (&self.manifest.resume, self.reconstruction_completed) {
            (Some(resume), None) => Some(resume.resumed_from.clone()),
            _ => None,
        }
    }

    /// Each source continues from the longest of its records across the
    /// lineage, which every shorter member must prefix by recorded event id.
    /// A tie keeps the ancestor: its descendant added nothing, so the torn
    /// run's own EOF is dropped.
    fn merge_lineage(&mut self, ancestors: Vec<Arc<Self>>) -> Result<(), ReplayError> {
        if ancestors.is_empty() {
            return Ok(());
        }
        let mut recorded = self
            .manifest
            .recorded_flow_id
            .as_ref()
            .map(|namespace| (namespace, &self.archive_path));
        for ancestor in &ancestors {
            let Some(namespace) = ancestor.manifest.recorded_flow_id.as_ref() else {
                continue;
            };
            match recorded {
                Some((first, _)) if first != namespace => {
                    return Err(ReplayError::NamespaceDisagrees {
                        archive: self.archive_path.clone(),
                        ancestor: ancestor.archive_path.clone(),
                    });
                }
                Some(_) => {}
                None => recorded = Some((namespace, &ancestor.archive_path)),
            }
        }
        let mut merged_eof = HashMap::new();
        let mut merged_frontier = HashMap::new();
        for stage_key in self.source_stage_keys() {
            let members: Vec<&Self> = ancestors
                .iter()
                .rev()
                .map(Arc::as_ref)
                .chain(std::iter::once(&*self))
                .filter(|member| member.manifest.stages.contains_key(&stage_key))
                .collect();
            let replayable = |member: &Self| {
                member
                    .replayable_by_source
                    .get(&stage_key)
                    .copied()
                    .unwrap_or(0)
            };
            let Some(longest) = members.iter().copied().reduce(|longest, member| {
                if replayable(member) > replayable(longest) {
                    member
                } else {
                    longest
                }
            }) else {
                continue;
            };
            for member in &members {
                if !std::ptr::eq(*member, longest) {
                    verify_source_prefix(member, longest, &stage_key, &self.archive_path)?;
                }
            }
            if let Some(frontier) = longest.recorded_frontier.get(&stage_key) {
                merged_frontier.insert(stage_key.clone(), frontier.clone());
            }
            if let Some(kind) = longest.recorded_source_eof.get(&stage_key) {
                merged_eof.insert(stage_key, *kind);
            }
        }
        self.recorded_source_eof = merged_eof;
        self.recorded_frontier = merged_frontier;
        for ancestor in &ancestors {
            self.max_recorded_generation = self
                .max_recorded_generation
                .max(ancestor.max_recorded_generation);
        }
        self.ancestors = ancestors;
        Ok(())
    }

    fn source_journal(&self, stage_key: &str) -> Option<PathBuf> {
        self.manifest
            .stages
            .get(stage_key)
            .map(|stage| self.archive_path.join(&stage.data_journal_file))
    }

    pub fn status(&self) -> ArchiveStatus {
        self.status
    }

    pub fn status_derivation(&self) -> StatusDerivation {
        self.status_derivation.clone()
    }

    /// FLOWIP-120q torn-tail policy. A cleanly `Completed` run flushed every
    /// record before its terminal event, so a torn final record is corruption.
    /// Any other status was interrupted, so an unterminated final record is a
    /// tolerated torn tail. `allow_incomplete_archive` governs only whether a
    /// non-terminal archive may be opened, never read-time tolerance.
    fn read_policy(&self) -> ReadPolicy {
        ReadPolicy::SealedScan {
            tolerate_torn_tail: self.status != ArchiveStatus::Completed,
        }
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

        let data_path = self.archive_path.join(&stage_info.data_journal_file);
        if !data_path.exists() {
            return Err(ReplayError::MissingJournal { path: data_path });
        }

        let reader = DiskJournalReader::<ChainEvent>::open_existing(
            data_path.clone(),
            super::identity::read_identity(&data_path)
                .map_err(|error| ReplayError::Io {
                    message: "Admit archived journal identity".into(),
                    source: std::io::Error::other(error.to_string()),
                })?
                .journal_id,
            self.read_write_lock.clone(),
            self.read_policy(),
        )
        .await
        .map_err(|e| ReplayError::Io {
            message: "Failed to open archived journal reader".to_string(),
            source: std::io::Error::other(e.to_string()),
        })?;

        Ok(Box::new(reader))
    }

    async fn open_effect_history(
        &self,
        stage_key: &str,
    ) -> Result<Box<dyn JournalReader<ChainEvent>>, ReplayError> {
        let stage_info =
            self.manifest
                .stages
                .get(stage_key)
                .ok_or_else(|| ReplayError::StageNotInManifest {
                    stage_key: stage_key.to_string(),
                })?;

        let data_path = self.archive_path.join(&stage_info.data_journal_file);
        if !data_path.exists() {
            return Err(ReplayError::MissingJournal { path: data_path });
        }

        let reader = DiskJournalReader::<ChainEvent>::open_existing(
            data_path.clone(),
            super::identity::read_identity(&data_path)
                .map_err(|error| ReplayError::Io {
                    message: "Admit archived journal identity".into(),
                    source: std::io::Error::other(error.to_string()),
                })?
                .journal_id,
            self.read_write_lock.clone(),
            self.read_policy(),
        )
        .await
        .map_err(|e| ReplayError::Io {
            message: "Failed to open archived effect-history reader".to_string(),
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

    fn recorded_flow_id(&self) -> Option<&str> {
        self.manifest.recorded_flow_id.as_deref()
    }

    fn stage_orders(&self) -> obzenflow_core::journal::archive::certification::StageOrders {
        (&self.manifest).into()
    }

    fn journal_stages(&self) -> HashMap<obzenflow_core::JournalWriterId, String> {
        self.manifest
            .stages
            .iter()
            .filter_map(|(stage_key, stage)| {
                let path = self.archive_path.join(&stage.data_journal_file);
                super::identity::read_identity(&path).ok().map(|identity| {
                    (
                        obzenflow_core::JournalWriterId::from_journal_id(identity.journal_id),
                        stage_key.clone(),
                    )
                })
            })
            .collect()
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

    fn recorded_source_eof(&self, stage_key: &str) -> Option<EofKind> {
        self.recorded_source_eof.get(stage_key).copied()
    }

    fn recorded_source_frontier(&self, stage_key: &str) -> RecordedSourceFrontier {
        self.recorded_frontier
            .get(stage_key)
            .cloned()
            .unwrap_or_default()
    }

    fn archive_path(&self) -> &Path {
        &self.archive_path
    }

    fn claim_continuation(
        &self,
        successor: obzenflow_core::FlowId,
        location: &obzenflow_core::journal::factory::CurrentRunLocator,
    ) -> Result<(), ReplayError> {
        super::continuation::publish_claim(&self.archive_path, successor, location.path())
    }

    fn ancestors(&self) -> Vec<Arc<dyn ReplayArchive>> {
        self.ancestors
            .iter()
            .map(|ancestor| ancestor.clone() as Arc<dyn ReplayArchive>)
            .collect()
    }

    fn manifest_capability(&self, name: &str) -> Option<u32> {
        self.manifest.capabilities.get(name).copied()
    }

    fn bounded_direct_fact_admission(
        &self,
    ) -> &[obzenflow_core::journal::archive::manifest::RunManifestDirectFactAdmission] {
        &self.manifest.bounded_direct_fact_admission
    }

    /// The highest generation with a recorded catch-up boundary in any source
    /// or effect-stage journal, scanned at open.
    fn max_recorded_generation(&self) -> obzenflow_core::ReaderGeneration {
        self.max_recorded_generation
    }
}

/// What one open-time scan of the archive's source journals recorded.
struct SourceJournalScan {
    max_generation: obzenflow_core::ReaderGeneration,
    /// Each source's committed EOF kind, by stage key (FLOWIP-134i B8).
    eof_by_source: HashMap<String, EofKind>,
    /// Each source's count of records a replay re-admits or preserves.
    replayable_by_source: HashMap<String, u64>,
    /// Each source's committed frontier, folded as a replay reads it (B22).
    frontier_by_source: HashMap<String, RecordedSourceFrontier>,
}

/// A shorter member's records for `stage_key` must open the longest member's,
/// by recorded event id (FLOWIP-134i B6). Streams both journals.
fn verify_source_prefix(
    shorter: &DiskReplayArchive,
    longest: &DiskReplayArchive,
    stage_key: &str,
    head: &Path,
) -> Result<(), ReplayError> {
    let disagrees = || ReplayError::AncestorDisagrees {
        archive: head.to_path_buf(),
        ancestor: if shorter.archive_path == head {
            longest.archive_path.clone()
        } else {
            shorter.archive_path.clone()
        },
        stage_key: stage_key.to_string(),
    };
    if shorter
        .replayable_by_source
        .get(stage_key)
        .copied()
        .unwrap_or(0)
        == 0
    {
        return Ok(());
    }
    let (Some(prefix_path), Some(extended_path)) = (
        shorter.source_journal(stage_key),
        longest.source_journal(stage_key),
    ) else {
        return Err(disagrees());
    };
    let mut prefix = ReplayableIds::open(prefix_path, shorter.read_policy())?;
    let mut extended = ReplayableIds::open(extended_path, longest.read_policy())?;
    while let Some(id) = prefix.next_id()? {
        if extended.next_id()? != Some(id) {
            return Err(disagrees());
        }
    }
    Ok(())
}

/// The event ids of the records a replay of one source journal re-admits or
/// preserves, in journal order.
struct ReplayableIds {
    path: PathBuf,
    reader: BufReader<File>,
    buf: Vec<u8>,
    decoder: Decoder,
    policy: ReadPolicy,
    offset: u64,
    pending: std::collections::VecDeque<obzenflow_core::EventId>,
    done: bool,
}

impl ReplayableIds {
    fn open(path: PathBuf, policy: ReadPolicy) -> Result<Self, ReplayError> {
        let file = File::open(&path).map_err(|e| ReplayError::Io {
            message: format!("Failed to open source journal at {}", path.display()),
            source: e,
        })?;
        let decoder = Decoder::new(&path);
        Ok(Self {
            path,
            reader: BufReader::new(file),
            buf: Vec::new(),
            decoder,
            policy,
            offset: 0,
            pending: std::collections::VecDeque::new(),
            done: false,
        })
    }

    fn next_id(&mut self) -> Result<Option<obzenflow_core::EventId>, ReplayError> {
        use obzenflow_core::event::chain_event::ReplayDisposition;
        loop {
            if let Some(id) = self.pending.pop_front() {
                return Ok(Some(id));
            }
            if self.done {
                return Ok(None);
            }
            let Some((consumed, termination)) = read_frame_sync(&mut self.reader, &mut self.buf)
                .map_err(|e| ReplayError::Io {
                    message: format!("Failed to read source journal at {}", self.path.display()),
                    source: e,
                })?
            else {
                self.done = true;
                continue;
            };
            let record_offset = self.offset;
            self.offset += consumed as u64;
            match dispose(
                classify_frame::<ChainEvent>(&self.buf, &mut self.decoder, record_offset),
                termination,
                self.policy,
            ) {
                Disposition::Yield(frame) => self.pending.extend(
                    frame
                        .into_records()
                        .into_iter()
                        .filter(|record| record.replay_disposition() != ReplayDisposition::ReAuthor)
                        .map(|record| *record.id()),
                ),
                Disposition::EndOfCommittedRecords | Disposition::Skip => self.done = true,
                Disposition::Corrupt(problem) => {
                    return Err(ReplayError::CorruptedArchive {
                        path: self.path.clone(),
                        record_position: record_offset,
                        message: problem.to_string(),
                    })
                }
            }
        }
    }
}

fn scan_source_journals(
    archive_path: &Path,
    manifest: &RunManifest,
    policy: ReadPolicy,
) -> Result<SourceJournalScan, ReplayError> {
    use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
    use obzenflow_core::event::ChainPayload;

    use obzenflow_core::event::chain_event::ReplayDisposition;

    let mut max_generation = 0u64;
    let mut eof_by_source = HashMap::new();
    let mut replayable_by_source = HashMap::new();
    let mut frontier_by_source = HashMap::new();
    for (stage_key, stage_info) in &manifest.stages {
        if stage_info.is_effectful && !stage_info.stage_type.is_source() {
            let path = archive_path.join(&stage_info.data_journal_file);
            max_generation = max_generation.max(scan_marker_generation(&path, policy)?);
            continue;
        }
        if !stage_info.stage_type.is_source() {
            continue;
        }
        let path = archive_path.join(&stage_info.data_journal_file);
        if !path.exists() {
            // A missing journal records no boundary; the reader open path
            // raises MissingJournal when resume actually needs it.
            continue;
        }
        let file = File::open(&path).map_err(|e| ReplayError::Io {
            message: format!("Failed to open source journal at {}", path.display()),
            source: e,
        })?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        let mut line_no = 0u64;
        let mut offset = 0u64;
        let mut decoder = Decoder::new(&path);
        let mut admission =
            super::identity::CommitmentAdmission::open(&path).map_err(causal_admission_error)?;
        loop {
            let Some((consumed, termination)) =
                read_frame_sync(&mut reader, &mut buf).map_err(|e| ReplayError::Io {
                    message: format!("Failed to read source journal at {}", path.display()),
                    source: e,
                })?
            else {
                break;
            };
            line_no += 1;
            let record_offset = offset;
            offset += consumed as u64;
            match dispose(
                classify_frame::<ChainEvent>(&buf, &mut decoder, record_offset),
                termination,
                policy,
            ) {
                Disposition::Yield(frame) => {
                    for record in frame.into_records() {
                        admission.admit(&record).map_err(causal_admission_error)?;
                        if record.replay_disposition() != ReplayDisposition::ReAuthor {
                            *replayable_by_source
                                .entry(stage_key.clone())
                                .or_insert(0u64) += 1;
                            frontier_by_source
                                .entry(stage_key.clone())
                                .or_insert_with(RecordedSourceFrontier::default)
                                .observe(
                                    &record.payload,
                                    record.envelope.provenance.event.source_progress.as_ref(),
                                );
                        }
                        if let ChainPayload::FlowControl(control) = &record.payload {
                            if let FlowControlPayload::CatchUpComplete { generation, .. } = control
                            {
                                max_generation = max_generation.max(generation.0);
                            }
                            if let Some(kind) = control.eof_kind() {
                                eof_by_source.insert(stage_key.clone(), kind);
                            }
                        }
                    }
                }
                Disposition::EndOfCommittedRecords | Disposition::Skip => break,
                Disposition::Corrupt(problem) => {
                    if matches!(problem, super::scanner::ParseProblem::SchemaMismatch) {
                        return Err(ReplayError::UnsupportedJournalSchemaVersion {
                            journal_schema_version: format!("frame marker in {}", path.display()),
                            supported: JOURNAL_SCHEMA_VERSION,
                        });
                    }
                    // The scan only collects boundaries. The resume read path
                    // re-reads this journal and fails loud on the same record
                    // (FLOWIP-120q), before any boundary from it could matter.
                    tracing::debug!(
                        path = %path.display(),
                        line = line_no,
                        %problem,
                        "generation scan stopped at a corrupt record; the resume \
                         read path fails loud on it"
                    );
                    break;
                }
            }
        }
    }
    Ok(SourceJournalScan {
        max_generation: obzenflow_core::ReaderGeneration(max_generation),
        eof_by_source,
        replayable_by_source,
        frontier_by_source,
    })
}

fn scan_marker_generation(path: &Path, policy: ReadPolicy) -> Result<u64, ReplayError> {
    use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
    use obzenflow_core::event::ChainPayload;
    if !path.exists() {
        return Ok(0);
    }
    let file = File::open(path).map_err(|e| ReplayError::Io {
        message: format!("Failed to open stage journal at {}", path.display()),
        source: e,
    })?;
    let mut reader = BufReader::new(file);
    let mut buf = Vec::new();
    let mut offset = 0u64;
    let mut decoder = Decoder::new(path);
    let mut max_generation = 0u64;
    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|e| ReplayError::Io {
            message: format!("Failed to read stage journal at {}", path.display()),
            source: e,
        })?
    {
        let record_offset = offset;
        offset += consumed as u64;
        match dispose(
            classify_frame::<ChainEvent>(&buf, &mut decoder, record_offset),
            termination,
            policy,
        ) {
            Disposition::Yield(frame) => {
                for record in frame.into_records() {
                    if let ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
                        generation,
                        ..
                    }) = &record.payload
                    {
                        max_generation = max_generation.max(generation.0);
                    }
                }
            }
            Disposition::EndOfCommittedRecords | Disposition::Skip => break,
            Disposition::Corrupt(problem) => {
                if matches!(problem, super::scanner::ParseProblem::SchemaMismatch) {
                    return Err(ReplayError::UnsupportedJournalSchemaVersion {
                        journal_schema_version: format!("frame marker in {}", path.display()),
                        supported: JOURNAL_SCHEMA_VERSION,
                    });
                }
                break;
            }
        }
    }
    Ok(max_generation)
}

fn scan_failure_evidence(
    archive_path: &Path,
    manifest: &RunManifest,
    contract_violation: bool,
) -> Result<obzenflow_core::journal::archive_actions::FailureEvidence, ReplayError> {
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::journal::archive_actions::{FailureEvidence, FailureFold};
    let policy = ReadPolicy::SealedScan {
        tolerate_torn_tail: true,
    };
    let mut fold = FailureFold::default();
    for stage_info in manifest.stages.values() {
        let path = archive_path.join(&stage_info.error_journal_file);
        if !path.exists() {
            continue;
        }
        let file = File::open(&path).map_err(|e| ReplayError::Io {
            message: format!("Failed to open error journal at {}", path.display()),
            source: e,
        })?;
        let mut reader = BufReader::new(file);
        let mut buf = Vec::new();
        let mut offset = 0u64;
        let mut decoder = Decoder::new(&path);
        while let Some((consumed, termination)) =
            read_frame_sync(&mut reader, &mut buf).map_err(|e| ReplayError::Io {
                message: format!("Failed to read error journal at {}", path.display()),
                source: e,
            })?
        {
            let record_offset = offset;
            offset += consumed as u64;
            match dispose(
                classify_frame::<ChainEvent>(&buf, &mut decoder, record_offset),
                termination,
                policy,
            ) {
                Disposition::Yield(frame) => {
                    for record in frame.into_records() {
                        if let ChainPayload::Execution(execution) = &record.payload {
                            fold.observe(execution);
                        }
                    }
                }
                Disposition::EndOfCommittedRecords | Disposition::Skip => break,
                Disposition::Corrupt(_) => return Ok(FailureEvidence::Other),
            }
        }
    }
    Ok(fold.finish(contract_violation))
}

/// What a run's system log records about how the run ended.
pub(crate) struct SystemLogScan {
    pub(crate) derivation: StatusDerivation,
    /// The generation whose reconstruction the run recorded as complete
    /// (FLOWIP-134i B6).
    pub(crate) reconstruction_completed: Option<u64>,
    pub(crate) contract_violation: bool,
}

pub(crate) fn scan_system_log(path: &Path) -> Result<SystemLogScan, ReplayError> {
    if !path.exists() {
        return Err(ReplayError::MissingSystemLog {
            path: path.to_path_buf(),
        });
    }

    let file = File::open(path).map_err(|e| ReplayError::Io {
        message: format!("Failed to open system.log at {}", path.display()),
        source: e,
    })?;
    let mut reader = BufReader::new(file);
    let mut buf = Vec::new();

    let mut chosen = ArchiveStatus::Unknown;
    let mut terminal_events_found: u64 = 0;
    let mut reconstruction_completed = None;
    let mut contract_violation = false;
    let mut line_no = 0u64;
    let mut offset = 0u64;
    let mut decoder = Decoder::new(path);
    let mut admission =
        super::identity::CommitmentAdmission::open(path).map_err(causal_admission_error)?;

    // FLOWIP-120q: the status-derivation bootstrap tolerates a torn tail, because
    // a crashed run legitimately leaves a torn final system record; that tail
    // just means no terminal event was committed. Mid-file corruption fails loud.
    while let Some((consumed, termination)) =
        read_frame_sync(&mut reader, &mut buf).map_err(|e| ReplayError::Io {
            message: format!("Failed to read system.log at {}", path.display()),
            source: e,
        })?
    {
        line_no += 1;
        let record_offset = offset;
        offset += consumed as u64;

        match dispose(
            classify_frame::<SystemEvent>(&buf, &mut decoder, record_offset),
            termination,
            ReadPolicy::SealedScan {
                tolerate_torn_tail: true,
            },
        ) {
            Disposition::Yield(frame) => {
                for record in frame.into_records() {
                    admission.admit(&record).map_err(causal_admission_error)?;
                    if let SystemPayload::PipelineLifecycle(event) = &record.payload {
                        match event {
                            obzenflow_core::event::PipelineLifecycleEvent::Completed { .. } => {
                                terminal_events_found = terminal_events_found.saturating_add(1);
                                chosen = ArchiveStatus::Completed;
                                contract_violation = false;
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::Failed {
                                failure_cause,
                                ..
                            } => {
                                terminal_events_found = terminal_events_found.saturating_add(1);
                                chosen = ArchiveStatus::Failed;
                                contract_violation = failure_cause.is_some();
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::Cancelled { .. } => {
                                terminal_events_found = terminal_events_found.saturating_add(1);
                                chosen = ArchiveStatus::Cancelled;
                                contract_violation = false;
                            }
                            obzenflow_core::event::PipelineLifecycleEvent::ReconstructionCompleted {
                                generation,
                            } => reconstruction_completed = Some(*generation),
                            _ => {}
                        }
                    }
                }
            }
            Disposition::EndOfCommittedRecords | Disposition::Skip => break,
            Disposition::Corrupt(problem) => {
                if matches!(problem, super::scanner::ParseProblem::SchemaMismatch) {
                    return Err(ReplayError::UnsupportedJournalSchemaVersion {
                        journal_schema_version: format!("frame marker in {}", path.display()),
                        supported: JOURNAL_SCHEMA_VERSION,
                    });
                }
                return Err(ReplayError::Parse {
                    message: format!(
                        "system.log record corrupt at line {line_no} in {}: {problem}",
                        path.display()
                    ),
                });
            }
        }
    }

    Ok(SystemLogScan {
        derivation: StatusDerivation {
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
        },
        reconstruction_completed,
        contract_violation,
    })
}

/// Whether a run recorded reconstruction completion (FLOWIP-134i B6). A system
/// log that cannot be read records none.
pub(crate) fn recorded_reconstruction_completion(run: &Path, manifest: &RunManifest) -> bool {
    scan_system_log(&run.join(&manifest.system_journal_file))
        .is_ok_and(|scan| scan.reconstruction_completed.is_some())
}

/// The run a replay reproduces; `None` for a live or resumed run.
pub(crate) fn replayed_run(manifest: &RunManifest) -> Option<PathBuf> {
    match (&manifest.replay, &manifest.resume) {
        (Some(replay), None) => Some(PathBuf::from(&replay.replay_from)),
        _ => None,
    }
}

fn causal_admission_error(error: obzenflow_core::journal::JournalError) -> ReplayError {
    ReplayError::Parse {
        message: format!("Invalid archived causal commitment: {error}"),
    }
}
