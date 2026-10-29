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
mod earlier;
mod lineage;
mod merge;
mod rejections;
pub use config_changes::{
    compare_with_runs, read_config_changes, read_configuration_refusals,
    read_logic_version_changes, ConfigChanges, LogicVersionChange, RunConfigChanges,
};
pub use earlier::EarlierRun;
pub use lineage::{read_run_lineage, EvidenceHolder, Rebuild, ResumedFrom, RunLineage, Standing};
pub use obzenflow_runtime::runtime_config::ConfigDifference;
pub use rejections::RecordedAt;

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

/// One journal of an earlier run, read from its start one record at a time up
/// to the end it had when opened.
struct JournalCursor {
    run: RunIdentity,
    journal: RunJournal,
    path: PathBuf,
    reader: Option<DiskJournalReader<ChainEvent>>,
    exhausted: bool,
}

impl JournalCursor {
    fn new(run: RunIdentity, journal: RunJournal, path: PathBuf) -> Self {
        Self {
            run,
            journal,
            path,
            reader: None,
            exhausted: false,
        }
    }

    async fn open(&mut self) -> Result<(), JournalReadError> {
        if self.reader.is_none() && !self.exhausted {
            self.reader =
                Some(DiskJournalReader::open_observer(self.path.clone(), self.journal.id).await?);
        }
        Ok(())
    }

    async fn next(&mut self) -> Result<Option<RunRecord>, JournalReadError> {
        self.open().await?;
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        if reader.initial_prefix_complete()? {
            self.close();
            return Ok(None);
        }
        let position = JournalPosition(reader.position());
        match reader.next().await? {
            Some(record) => Ok(Some(run_record(
                &self.run,
                self.journal.clone(),
                position,
                RunRecordData::Chain(Box::new(record)),
            ))),
            None => {
                self.close();
                Ok(None)
            }
        }
    }

    fn restart(&mut self) {
        self.reader = None;
        self.exhausted = false;
    }

    fn close(&mut self) {
        self.reader = None;
        self.exhausted = true;
    }
}

/// An earlier run's source journals, by source.
struct SourceJournals {
    run: RunIdentity,
    data: BTreeMap<String, (RunJournal, PathBuf)>,
    errors: BTreeMap<String, (RunJournal, PathBuf)>,
}

impl SourceJournals {
    fn of(run: &AdmittedRun) -> Self {
        let mut sources = Self {
            run: run.identity.clone(),
            data: BTreeMap::new(),
            errors: BTreeMap::new(),
        };
        for (journal, path) in &run.journals {
            let Some(stage) = journal
                .stage
                .as_ref()
                .filter(|stage| stage.stage_type.is_source())
            else {
                continue;
            };
            let journals = match journal.kind {
                RunJournalKind::Data => &mut sources.data,
                RunJournalKind::Error => &mut sources.errors,
                _ => continue,
            };
            journals.insert(stage.key.clone(), (journal.clone(), path.clone()));
        }
        sources
    }
}

/// A run's identity, manifest and selected journals, checked without reading
/// any record.
struct AdmittedRun {
    identity: RunIdentity,
    manifest: RunManifest,
    journals: Vec<(RunJournal, PathBuf)>,
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
    earlier: earlier::EarlierRuns,
    repeats: rejections::Repeats,
    progress: RunReadProgress,
    settled_ends: BTreeMap<JournalId, JournalPosition>,
    failure: Option<String>,
}

/// Admit all manifest-selected journals without creating or changing any file.
pub async fn open_disk_run(path: &Path) -> Result<RunSnapshot, JournalReadError> {
    let AdmittedRun {
        identity,
        manifest,
        journals: files,
    } = admit_run(path)?;
    let mut journals = Vec::with_capacity(files.len());
    for (journal, file) in files {
        let reader = match journal.kind {
            RunJournalKind::System
            | RunJournalKind::MetricsCoordination
            | RunJournalKind::MetricsExport => {
                Reader::System(DiskJournalReader::open_observer(file, journal.id).await?)
            }
            _ => Reader::Chain(DiskJournalReader::open_observer(file, journal.id).await?),
        };
        journals.push(SelectedJournal { journal, reader });
    }
    let merge = merge::CausalMerge::new(journals.iter().map(|selected| selected.journal.id));
    Ok(RunSnapshot {
        state: ReadState {
            run_dir: path.to_path_buf(),
            identity,
            manifest,
            journals,
            merge,
            earlier: earlier::EarlierRuns::default(),
            repeats: rejections::Repeats::default(),
            progress: RunReadProgress::default(),
            settled_ends: BTreeMap::new(),
            failure: None,
        },
    })
}

fn admit_run(path: &Path) -> Result<AdmittedRun, JournalReadError> {
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
        journals.push((
            RunJournal {
                id: descriptor.journal_id,
                kind,
                stage,
            },
            file,
        ));
    }
    Ok(AdmittedRun {
        identity,
        manifest,
        journals,
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
    /// Lists the source error records of `runs`, oldest first, before reading.
    /// Each follows the record of this run that re-admitted the row it followed.
    /// Their journals are read one record at a time as this run reaches them,
    /// and a run that cannot be admitted is skipped.
    pub async fn list_earlier_runs<'a>(
        &mut self,
        runs: impl IntoIterator<Item = &'a Path>,
    ) -> Result<Vec<EarlierRun>, JournalReadError> {
        self.state.list_earlier_runs(runs).await
    }
    /// The first record of the row `record` rejects again: the oldest listed
    /// earlier run's, while its source pinned one input across those runs.
    pub async fn repeat_of(
        &mut self,
        record: &RunRecord,
    ) -> Result<Option<RecordedAt>, JournalReadError> {
        self.state
            .repeats
            .repeat_of(&self.state.identity, record)
            .await
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
    /// The first record of the row `record` rejects again: the oldest listed
    /// earlier run's, while its source pinned one input across those runs.
    pub async fn repeat_of(
        &mut self,
        record: &RunRecord,
    ) -> Result<Option<RecordedAt>, JournalReadError> {
        self.state
            .repeats
            .repeat_of(&self.state.identity, record)
            .await
    }
    /// The next record still held, in rank order, then each earlier-run record
    /// this run never re-admitted a row for.
    pub async fn next_held(&mut self) -> Result<Option<RunRecord>, JournalReadError> {
        self.state.fill_earlier().await?;
        let next = self
            .state
            .merge
            .lowest()
            .or_else(|| self.state.earlier.take_unreached());
        if let Some(record) = &next {
            self.state.earlier.taken(record);
        }
        Ok(next)
    }
}

impl ReadState {
    async fn next(&mut self, snapshot: bool) -> Result<Option<RunRecord>, JournalReadError> {
        if let Some(error) = &self.failure {
            return Err(JournalReadError::Integrity(error.clone()));
        }
        self.fill_earlier().await?;
        for index in 0..self.journals.len() {
            let selected = &mut self.journals[index];
            if self.merge.holds(&selected.journal.id)
                || (snapshot && selected.reader.initial_prefix_complete()?)
                || source_data_stage(&selected.journal)
                    .is_some_and(|source| self.earlier.holds_back(source))
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
                    let record = run_record(&self.identity, journal, position, record);
                    self.hold(record);
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
        if let Some(record) = &next {
            self.earlier.taken(record);
        }
        self.refresh_settlement();
        Ok(next)
    }

    async fn list_earlier_runs<'a>(
        &mut self,
        runs: impl IntoIterator<Item = &'a Path>,
    ) -> Result<Vec<EarlierRun>, JournalReadError> {
        let mut listed = Vec::new();
        for path in runs {
            let Ok(admitted) = admit_run(path) else {
                continue;
            };
            let sources = SourceJournals::of(&admitted);
            let Ok(streams) = open_streams(&sources).await else {
                continue;
            };
            for (cursor, anchor) in streams {
                self.earlier.add(cursor, anchor);
            }
            self.repeats.add(sources);
            listed.push(EarlierRun {
                generation: lineage::entered_generation(&admitted.manifest),
                identity: admitted.identity,
                journals: admitted
                    .journals
                    .into_iter()
                    .map(|(journal, _)| journal)
                    .collect(),
            });
        }
        self.fill_earlier().await?;
        Ok(listed)
    }

    async fn fill_earlier(&mut self) -> Result<(), JournalReadError> {
        let placed = match self.earlier.fill().await {
            Ok(placed) => placed,
            Err(error) => {
                self.failure = Some(error.to_string());
                return Err(error);
            }
        };
        for (earlier, clock) in placed {
            self.place(earlier, &clock);
        }
        Ok(())
    }

    fn place(&mut self, earlier: RunRecord, clock: &VectorClock) {
        let journal = earlier.journal.id;
        let sequence = sequence_of(&earlier.record);
        let mut order = clock.clone();
        order.clocks.insert(coordinate_of(journal), sequence);
        let event = event_of(&earlier.record);
        self.merge.push(journal, sequence, order, event, earlier);
    }

    fn hold(&mut self, record: RunRecord) {
        if let Some(source) = source_data_stage(&record.journal) {
            let clock = clock_of(&record.record);
            for earlier in self.earlier.reach(source, clock) {
                self.place(earlier, clock);
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

    #[cfg(test)]
    fn held(&self) -> usize {
        self.merge.len() + self.earlier.waiting()
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

async fn open_streams(
    sources: &SourceJournals,
) -> Result<Vec<(JournalCursor, Option<(String, CausalCoordinate)>)>, JournalReadError> {
    let mut streams = Vec::new();
    for (key, (journal, path)) in &sources.errors {
        let mut cursor = JournalCursor::new(sources.run.clone(), journal.clone(), path.clone());
        cursor.open().await?;
        let anchor = sources
            .data
            .get(key)
            .map(|(data, _)| (key.clone(), coordinate_of(data.id)));
        streams.push((cursor, anchor));
    }
    Ok(streams)
}

fn run_record(
    run: &RunIdentity,
    journal: RunJournal,
    position: JournalPosition,
    record: RunRecordData,
) -> RunRecord {
    RunRecord {
        version: RUN_RECORD_VERSION,
        run: run.clone(),
        kind: classify(&journal, &record),
        journal,
        position,
        record,
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
