// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete read-only run handles. No writer factory, repair, background polling,
//! delivery acknowledgement or application control is involved in observation.

use super::disk::{inspect::load_manifest, reader::DiskJournalReader};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{
    CausalCoordinate, ChainEvent, ChainPayload, PipelineLifecycleEvent, SystemEvent, SystemPayload,
};
use obzenflow_core::id::{FlowId, JournalId, StageId};
use obzenflow_core::journal::read::*;
use obzenflow_core::journal::{JournalError, JournalReader, RunManifest};
use obzenflow_core::{EventId, JournalWriterId};
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path, PathBuf};

mod config_changes;
mod lineage;
mod merge;
mod rejections;
pub use config_changes::{
    compare_with_runs, read_config_changes, read_configuration_refusals,
    read_logic_version_changes, ConfigChanges, LogicVersionChange, RunConfigChanges,
};
pub use lineage::{read_run_lineage, EvidenceHolder, Rebuild, ResumedFrom, RunLineage, Standing};
pub use obzenflow_runtime::runtime_config::ConfigDifference;
pub use rejections::{LineageRejections, RecordedAt};

#[derive(Debug, thiserror::Error)]
pub enum JournalReadError {
    #[error(transparent)]
    Admission(#[from] super::disk::inspect::JournalInspectError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    /// A continuation claim on the run's lineage could not be read.
    #[error(transparent)]
    Lineage(#[from] obzenflow_core::journal::archive::ReplayError),
    #[error("invalid run archive: {0}")]
    Invalid(String),
    #[error("conflicting run evidence: {0}")]
    Integrity(String),
}

enum Reader {
    System(DiskJournalReader<SystemEvent>),
    Chain(DiskJournalReader<ChainEvent>),
}

impl Reader {
    fn initial_prefix_complete(&self) -> Result<bool, JournalError> {
        match self {
            Self::System(r) => r.initial_prefix_complete(),
            Self::Chain(r) => r.initial_prefix_complete(),
        }
    }
    fn position(&self) -> u64 {
        match self {
            Self::System(r) => r.position(),
            Self::Chain(r) => r.position(),
        }
    }
    fn is_at_end(&self) -> bool {
        match self {
            Self::System(r) => r.is_at_end(),
            Self::Chain(r) => r.is_at_end(),
        }
    }
    async fn next(&mut self) -> Result<Option<RunRecordData>, JournalError> {
        match self {
            Self::System(r) => Ok(r
                .next()
                .await?
                .map(|record| RunRecordData::System(Box::new(record)))),
            Self::Chain(r) => Ok(r
                .next()
                .await?
                .map(|record| RunRecordData::Chain(Box::new(record)))),
        }
    }
}

struct SelectedJournal {
    journal: RunJournal,
    reader: Reader,
}

/// A finite per-journal committed prefix, with independent cursors.
pub struct RunSnapshot {
    state: ReadState,
}

/// Pull-based observation of the admitted run; dropping it only detaches.
pub struct RunTail {
    state: ReadState,
}

struct ReadState {
    run_dir: PathBuf,
    identity: RunIdentity,
    manifest: RunManifest,
    journals: Vec<SelectedJournal>,
    merge: merge::CausalMerge<RunRecord>,
    earlier: merge::EarlierRuns<RunRecord>,
    progress: RunReadProgress,
    settled_ends: BTreeMap<JournalId, JournalPosition>,
    failure: Option<String>,
}

/// Admit all manifest-selected journals without creating or changing any file.
pub async fn open_disk_run(path: &Path) -> Result<RunSnapshot, JournalReadError> {
    let manifest = load_manifest(path)?;
    let flow_id: FlowId = manifest
        .flow_id
        .parse()
        .map_err(|e| JournalReadError::Invalid(format!("flow identity: {e}")))?;
    if !manifest.pipeline_writer_id.is_system() {
        return Err(JournalReadError::Invalid(
            "pipeline writer must be a system writer".into(),
        ));
    }
    let identity = RunIdentity {
        flow_id,
        pipeline_writer_id: manifest.pipeline_writer_id,
    };
    let mut files = vec![(
        manifest.system_journal_file.clone(),
        RunJournalKind::System,
        None,
    )];
    if let Some(metrics) = &manifest.metrics_journals {
        if !metrics.writer_id.is_system() || metrics.writer_id == manifest.pipeline_writer_id {
            return Err(JournalReadError::Invalid("invalid metrics owner".into()));
        }
        files.push((
            metrics.coordination_journal_file.clone(),
            RunJournalKind::MetricsCoordination,
            None,
        ));
        files.push((
            metrics.export_journal_file.clone(),
            RunJournalKind::MetricsExport,
            None,
        ));
    }
    let mut stages: Vec<_> = manifest.stages.iter().collect();
    stages.sort_by(|a, b| a.0.cmp(b.0));
    let mut stage_ids = HashSet::new();
    for (key, stage) in stages {
        let id: StageId = stage
            .stage_id
            .parse()
            .map_err(|e| JournalReadError::Invalid(format!("stage identity: {e}")))?;
        if !stage_ids.insert(id) {
            return Err(JournalReadError::Invalid("duplicate stage identity".into()));
        }
        let stage_info = RunStage {
            key: key.clone(),
            id,
            stage_type: stage.stage_type,
            is_effectful: stage.is_effectful,
        };
        files.push((
            stage.data_journal_file.clone(),
            RunJournalKind::Data,
            Some(stage_info.clone()),
        ));
        files.push((
            stage.error_journal_file.clone(),
            RunJournalKind::Error,
            Some(stage_info),
        ));
    }
    let mut names = HashSet::new();
    let mut journals = Vec::with_capacity(files.len());
    let mut incarnations = HashSet::new();
    for (name, kind, stage) in files {
        let mut components = Path::new(&name).components();
        if !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
            || !names.insert(name.clone())
        {
            return Err(JournalReadError::Invalid(format!(
                "invalid or duplicate journal filename: {name}"
            )));
        }
        let file = path.join(&name);
        let metadata = std::fs::symlink_metadata(&file)
            .map_err(|e| JournalReadError::Invalid(format!("{}: {e}", file.display())))?;
        if !metadata.is_file() {
            return Err(JournalReadError::Invalid(format!(
                "not a regular journal: {}",
                file.display()
            )));
        }
        let descriptor = super::disk::identity::read_identity(&file)?;
        if descriptor.run_id != flow_id || !incarnations.insert(descriptor.journal_id) {
            return Err(JournalReadError::Integrity(
                "journal belongs to another run".into(),
            ));
        }
        let id = descriptor.journal_id;
        let reader = match kind {
            RunJournalKind::System
            | RunJournalKind::MetricsCoordination
            | RunJournalKind::MetricsExport => {
                Reader::System(DiskJournalReader::open_observer(file, id).await?)
            }
            _ => Reader::Chain(DiskJournalReader::open_observer(file, id).await?),
        };
        journals.push(SelectedJournal {
            journal: RunJournal { id, kind, stage },
            reader,
        });
    }
    let merge = merge::CausalMerge::new(journals.iter().map(|selected| selected.journal.id));
    Ok(RunSnapshot {
        state: ReadState {
            run_dir: path.to_path_buf(),
            identity,
            manifest,
            journals,
            merge,
            earlier: merge::EarlierRuns::default(),
            progress: RunReadProgress::default(),
            settled_ends: BTreeMap::new(),
            failure: None,
        },
    })
}

impl RunSnapshot {
    pub fn identity(&self) -> &RunIdentity {
        &self.state.identity
    }
    /// Persisted manifest admitted with this handle. Run outcomes come from journals.
    pub fn manifest(&self) -> &RunManifest {
        &self.state.manifest
    }
    /// The generation this run entered, 0 for a run that resumed nothing.
    pub fn generation(&self) -> u64 {
        lineage::entered_generation(&self.state.manifest)
    }
    /// Manifest-selected journal identities, available before reading any rows.
    pub fn journals(&self) -> impl ExactSizeIterator<Item = &RunJournal> {
        self.state.journals.iter().map(|selected| &selected.journal)
    }
    /// The run's place in its history, read now from manifests and claims.
    pub fn lineage(&self) -> Result<RunLineage, JournalReadError> {
        read_run_lineage(&self.state.run_dir)
    }
    /// Returns records in causal order, lowest causal rank first with ties broken
    /// by event ID, so every record follows each record its clock counts.
    pub async fn next(&mut self) -> Result<Option<RunRecord>, JournalReadError> {
        self.state.next(true).await
    }
    /// Adds an earlier run's source error records before reading. Each one
    /// follows the record of this run that re-admitted the row it followed.
    pub fn merge_earlier_run<'a>(
        &mut self,
        journals: impl IntoIterator<Item = &'a RunJournal>,
        records: Vec<RunRecord>,
    ) {
        self.state.merge_earlier_run(journals, records);
    }
    pub fn into_tail(self) -> RunTail {
        RunTail { state: self.state }
    }
}

impl RunTail {
    pub fn identity(&self) -> &RunIdentity {
        &self.state.identity
    }
    /// The same persisted manifest admitted by the snapshot; no live state lookup.
    pub fn manifest(&self) -> &RunManifest {
        &self.state.manifest
    }
    /// The same fixed journal selection admitted by the snapshot.
    pub fn journals(&self) -> impl ExactSizeIterator<Item = &RunJournal> {
        self.state.journals.iter().map(|selected| &selected.journal)
    }
    pub fn progress(&self) -> &RunReadProgress {
        &self.state.progress
    }
    /// The run's place in its history, read now from manifests and claims.
    pub fn lineage(&self) -> Result<RunLineage, JournalReadError> {
        read_run_lineage(&self.state.run_dir)
    }
    /// How the run's configuration differs from the runs it rebuilt.
    pub fn config_changes(&self) -> Result<Vec<RunConfigChanges>, JournalReadError> {
        read_config_changes(&self.state.run_dir)
    }
    /// The lowest-ranked record whose clock's journals have been returned up to
    /// their counts, or `Pending`. At settlement every held record is returned.
    pub async fn read_next(&mut self) -> Result<TailRead, JournalReadError> {
        Ok(match self.state.next(false).await? {
            Some(record) => TailRead::Record(record),
            None => TailRead::Pending,
        })
    }
    /// Every record still held, in rank order, then any earlier-run record this
    /// run never re-admitted a row for.
    pub fn take_held(&mut self) -> Vec<RunRecord> {
        let mut held = self.state.merge.drain();
        held.extend(std::iter::from_fn(|| self.state.earlier.take_unreached()));
        held
    }
}

impl ReadState {
    async fn next(&mut self, snapshot: bool) -> Result<Option<RunRecord>, JournalReadError> {
        if let Some(error) = &self.failure {
            return Err(JournalReadError::Integrity(error.clone()));
        }
        for index in 0..self.journals.len() {
            let selected = &mut self.journals[index];
            if self.merge.holds(&selected.journal.id)
                || (snapshot && selected.reader.initial_prefix_complete()?)
            {
                continue;
            }
            let position = JournalPosition(selected.reader.position());
            let next = match selected.reader.next().await {
                Ok(next) => next,
                Err(error) => {
                    self.failure = Some(error.to_string());
                    return Err(error.into());
                }
            };
            let at_end = selected.reader.is_at_end();
            let journal = selected.journal.clone();
            match next {
                Some(record) => {
                    if let RunRecordData::System(system) = &record {
                        if let Err(error) =
                            self.fold_system(system, journal.id, JournalPosition(position.0 + 1))
                        {
                            self.failure = Some(error.to_string());
                            return Err(error);
                        }
                    }
                    let kind = classify(&journal, &record);
                    self.hold(RunRecord {
                        version: RUN_RECORD_VERSION,
                        run: self.identity.clone(),
                        journal,
                        position,
                        kind,
                        record,
                    });
                }
                None if !snapshot
                    && at_end
                    && self.progress.drained_event_id.is_some()
                    && journal.kind != RunJournalKind::System =>
                {
                    self.settled_ends.entry(journal.id).or_insert(position);
                }
                None => {}
            }
        }
        let next = if snapshot || self.every_end_observed() {
            self.merge
                .lowest()
                .or_else(|| self.earlier.take_unreached())
        } else {
            self.merge.lowest_ready()
        };
        self.refresh_settlement();
        Ok(next)
    }

    fn hold(&mut self, record: RunRecord) {
        if let Some(source) = source_data_stage(&record.journal).map(str::to_owned) {
            let clock = clock_of(&record.record);
            for (journal, earlier) in self.earlier.reached(&source, clock) {
                let mut order = clock.clone();
                order
                    .clocks
                    .insert(coordinate_of(journal), sequence_of(&earlier.record));
                let event = event_of(&earlier.record);
                self.merge
                    .push(journal, sequence_of(&earlier.record), order, event, earlier);
            }
        }
        self.merge.push(
            record.journal.id,
            sequence_of(&record.record),
            clock_of(&record.record).clone(),
            event_of(&record.record),
            record,
        );
    }

    fn merge_earlier_run<'a>(
        &mut self,
        journals: impl IntoIterator<Item = &'a RunJournal>,
        records: Vec<RunRecord>,
    ) {
        let sources: BTreeMap<String, JournalId> = journals
            .into_iter()
            .filter_map(|journal| {
                source_data_stage(journal).map(|key| (key.to_owned(), journal.id))
            })
            .collect();
        for record in records {
            let source = record
                .journal
                .stage
                .as_ref()
                .filter(|stage| stage.stage_type.is_source())
                .and_then(|stage| {
                    sources
                        .get(&stage.key)
                        .map(|anchor| (stage.key.clone(), coordinate_of(*anchor)))
                });
            match source {
                Some((source, anchor)) => {
                    let count = clock_of(&record.record).get(&anchor);
                    self.earlier
                        .wait(record.journal.id, source, anchor, count, record);
                }
                None => self.earlier.unplaced(record),
            }
        }
    }

    fn every_end_observed(&self) -> bool {
        self.progress.outcome.is_some()
            && self.progress.drained_event_id.is_some()
            && self.settled_ends.len() == self.journals.len()
    }

    fn refresh_settlement(&mut self) {
        if self.progress.settled_prefix.is_none()
            && self.settled_ends.len() == self.journals.len()
            && self.merge.is_empty()
            && self.earlier.is_empty()
        {
            if let (Some(outcome), Some(drained)) =
                (&self.progress.outcome, self.progress.drained_event_id)
            {
                self.progress.settled_prefix = Some(SettledRunPrefix {
                    terminal_event_id: outcome.event_id,
                    drained_event_id: drained,
                    end_positions: self.settled_ends.clone(),
                });
            }
        }
    }

    fn fold_system(
        &mut self,
        record: &obzenflow_core::event::journal_record::SystemJournalRecord,
        journal: JournalId,
        end: JournalPosition,
    ) -> Result<(), JournalReadError> {
        if *record.writer_id() != self.identity.pipeline_writer_id {
            return Ok(());
        }
        let SystemPayload::PipelineLifecycle(event) = &record.payload else {
            return Ok(());
        };
        let outcome = match event {
            PipelineLifecycleEvent::Completed { .. } => Some(RunOutcome::Completed),
            PipelineLifecycleEvent::Failed { reason, .. } => Some(RunOutcome::Failed {
                reason: reason.clone(),
            }),
            PipelineLifecycleEvent::Cancelled { reason, .. } => Some(RunOutcome::Cancelled {
                reason: reason.clone(),
            }),
            PipelineLifecycleEvent::NotStarted => Some(RunOutcome::NotStarted),
            PipelineLifecycleEvent::Drained => {
                if self.progress.outcome.is_none() || self.progress.drained_event_id.is_some() {
                    return Err(JournalReadError::Integrity(
                        "pipeline drain requires one preceding terminal outcome".into(),
                    ));
                }
                self.progress.drained_event_id = Some(*record.id());
                // Only fresh stage-end observations after this marker can cover
                // the settled execution. Earlier EOF is not evidence of coverage.
                self.settled_ends.clear();
                self.settled_ends.insert(journal, end);
                None
            }
            _ => None,
        };
        if let Some(outcome) = outcome {
            if self.progress.outcome.is_some() {
                return Err(JournalReadError::Integrity(
                    "multiple pipeline terminal outcomes".into(),
                ));
            }
            self.progress.outcome = Some(RecordedRunOutcome {
                event_id: *record.id(),
                outcome,
            });
        }
        Ok(())
    }
}

fn source_data_stage(journal: &RunJournal) -> Option<&str> {
    journal
        .stage
        .as_ref()
        .filter(|stage| journal.kind == RunJournalKind::Data && stage.stage_type.is_source())
        .map(|stage| stage.key.as_str())
}

fn coordinate_of(journal: JournalId) -> CausalCoordinate {
    CausalCoordinate::new(JournalWriterId::from(journal))
}

fn clock_of(record: &RunRecordData) -> &VectorClock {
    match record {
        RunRecordData::Chain(row) => &row.envelope.provenance.journal.vector_clock,
        RunRecordData::System(row) => &row.envelope.provenance.journal.vector_clock,
    }
}

fn event_of(record: &RunRecordData) -> EventId {
    match record {
        RunRecordData::Chain(row) => *row.id(),
        RunRecordData::System(row) => *row.id(),
    }
}

fn sequence_of(record: &RunRecordData) -> u64 {
    match record {
        RunRecordData::Chain(row) => row.local_sequence(),
        RunRecordData::System(row) => row.local_sequence(),
    }
}

fn classify(journal: &RunJournal, record: &RunRecordData) -> RunRecordKind {
    match record {
        RunRecordData::System(record) => match record.payload {
            SystemPayload::PipelineLifecycle(_) => RunRecordKind::Lifecycle,
            _ => RunRecordKind::System,
        },
        RunRecordData::Chain(record) => match &record.payload {
            ChainPayload::Fact(_)
                if journal
                    .stage
                    .as_ref()
                    .is_some_and(|stage| stage.stage_type.is_source()) =>
            {
                RunRecordKind::SourceFact
            }
            ChainPayload::Fact(_) => RunRecordKind::StageOutput,
            ChainPayload::CompositeData(_) => RunRecordKind::CompositeData,
            ChainPayload::FlowControl(_) => RunRecordKind::FlowSignal,
            ChainPayload::Delivery(_) => RunRecordKind::Delivery,
            ChainPayload::Execution(
                ExecutionPayload::EffectRecord(_)
                | ExecutionPayload::EffectAttemptStarted(_)
                | ExecutionPayload::EffectRecoveryAbandoned(_),
            ) => RunRecordKind::Effect,
            ChainPayload::Execution(ExecutionPayload::StageLifecycle(_)) => {
                RunRecordKind::Lifecycle
            }
            ChainPayload::Execution(_) => RunRecordKind::Execution,
        },
    }
}

#[cfg(test)]
mod tests;
