// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete read-only run handles. No writer factory, repair, background polling,
//! delivery acknowledgement or application control is involved in observation.

use super::disk::{inspect::load_manifest, reader::DiskJournalReader};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::{
    ChainEvent, ChainPayload, PipelineLifecycleEvent, SystemEvent, SystemPayload,
};
use obzenflow_core::id::{FlowId, JournalId, StageId};
use obzenflow_core::journal::read::*;
use obzenflow_core::journal::{JournalError, JournalReader};
use std::collections::{BTreeMap, HashSet};
use std::path::{Component, Path};

#[derive(Debug, thiserror::Error)]
pub enum JournalReadError {
    #[error(transparent)]
    Admission(#[from] super::disk::inspect::JournalInspectError),
    #[error(transparent)]
    Journal(#[from] JournalError),
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
    identity: RunIdentity,
    journals: Vec<SelectedJournal>,
    next_journal: usize,
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
    let mut files = vec![(manifest.system_journal_file, RunJournalKind::System, None)];
    let mut stages: Vec<_> = manifest.stages.into_iter().collect();
    stages.sort_by(|a, b| a.0.cmp(&b.0));
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
            key,
            id,
            stage_type: stage.stage_type,
            is_effectful: stage.is_effectful,
        };
        files.push((
            stage.data_journal_file,
            RunJournalKind::Data,
            Some(stage_info.clone()),
        ));
        files.push((
            stage.error_journal_file,
            RunJournalKind::Error,
            Some(stage_info),
        ));
    }
    let mut names = HashSet::new();
    let mut journals = Vec::with_capacity(files.len());
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
        // Disk JournalId values are process-local today. This read identity is
        // stable across independent handles and scoped to the admitted archive.
        let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
        digest.update(&flow_id.as_ulid().to_bytes());
        digest.update(name.as_bytes());
        let bytes: [u8; 16] = digest.finish().as_ref()[..16]
            .try_into()
            .expect("SHA256 prefix");
        let id = JournalId::from_ulid(ulid::Ulid::from_bytes(bytes));
        let reader = match kind {
            RunJournalKind::System => {
                Reader::System(DiskJournalReader::open_observer(file, id).await?)
            }
            _ => Reader::Chain(DiskJournalReader::open_observer(file, id).await?),
        };
        journals.push(SelectedJournal {
            journal: RunJournal { id, kind, stage },
            reader,
        });
    }
    Ok(RunSnapshot {
        state: ReadState {
            identity,
            journals,
            next_journal: 0,
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
    /// Manifest-selected journal identities, available before reading any rows.
    pub fn journals(&self) -> impl ExactSizeIterator<Item = &RunJournal> {
        self.state.journals.iter().map(|selected| &selected.journal)
    }
    pub async fn next(&mut self) -> Result<Option<RunRecord>, JournalReadError> {
        self.state.next(true).await
    }
    pub fn into_tail(self) -> RunTail {
        RunTail { state: self.state }
    }
}

impl RunTail {
    pub fn identity(&self) -> &RunIdentity {
        &self.state.identity
    }
    /// The same fixed journal selection admitted by the snapshot.
    pub fn journals(&self) -> impl ExactSizeIterator<Item = &RunJournal> {
        self.state.journals.iter().map(|selected| &selected.journal)
    }
    pub fn progress(&self) -> &RunReadProgress {
        &self.state.progress
    }
    pub async fn read_next(&mut self) -> Result<TailRead, JournalReadError> {
        Ok(match self.state.next(false).await? {
            Some(record) => TailRead::Record(record),
            None => TailRead::Pending,
        })
    }
}

impl ReadState {
    async fn next(&mut self, snapshot: bool) -> Result<Option<RunRecord>, JournalReadError> {
        if let Some(error) = &self.failure {
            return Err(JournalReadError::Integrity(error.clone()));
        }
        for _ in 0..self.journals.len() {
            let index = self.next_journal;
            // A cancelled poll resumes at this journal. A successful poll advances
            // synchronously with the underlying cursor and returned record.
            let selected = &mut self.journals[index];
            if snapshot && selected.reader.initial_prefix_complete()? {
                self.next_journal = (index + 1) % self.journals.len();
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
            self.next_journal = (index + 1) % self.journals.len();
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
                    self.refresh_settlement();
                    let kind = classify(&journal, &record);
                    return Ok(Some(RunRecord {
                        version: RUN_RECORD_VERSION,
                        run: self.identity.clone(),
                        journal,
                        position,
                        kind,
                        record,
                    }));
                }
                None if !snapshot
                    && at_end
                    && self.progress.drained_event_id.is_some()
                    && journal.kind != RunJournalKind::System =>
                {
                    self.settled_ends.entry(journal.id).or_insert(position);
                    self.refresh_settlement();
                }
                None => {}
            }
        }
        Ok(None)
    }

    fn refresh_settlement(&mut self) {
        if self.progress.settled_prefix.is_none() && self.settled_ends.len() == self.journals.len()
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

fn classify(journal: &RunJournal, record: &RunRecordData) -> RunRecordKind {
    match record {
        RunRecordData::System(record) => match record.payload {
            SystemPayload::PipelineLifecycle(_)
            | SystemPayload::StageLifecycle { .. }
            | SystemPayload::ReplayLifecycle(_) => RunRecordKind::Lifecycle,
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
