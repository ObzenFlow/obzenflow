// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rows a lineage's sources rejected. A row is one source's record index while
//! that source pinned one input across the lineage, so a later run that
//! rejects the same row again records a repeat.

use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::journal::read::{
    JournalPosition, RunIdentity, RunJournal, RunJournalKind, RunRecord, RunRecordData,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};

/// The run, journal and position that hold a record.
#[derive(Debug, Clone, Serialize)]
pub struct RecordedAt {
    pub run: RunIdentity,
    pub journal: RunJournal,
    pub position: JournalPosition,
}

impl RecordedAt {
    pub fn of(record: &RunRecord) -> Self {
        Self {
            run: record.run.clone(),
            journal: record.journal.clone(),
            position: record.position,
        }
    }

    fn holds(&self, record: &RunRecord) -> bool {
        self.run == record.run
            && self.journal.id == record.journal.id
            && self.position == record.position
    }
}

/// The rows a lineage's sources rejected, folded from its runs oldest first.
#[derive(Debug, Default)]
pub struct LineageRejections {
    inputs: BTreeMap<String, BTreeSet<String>>,
    first: BTreeMap<(String, u64), RecordedAt>,
}

impl LineageRejections {
    /// Folds one run's records, after those of every run before it.
    pub fn fold_run(&mut self, records: &[RunRecord]) {
        for record in records {
            if let Some((source, digest)) = pinned_input(record) {
                self.inputs
                    .entry(source.to_owned())
                    .or_default()
                    .insert(digest);
            }
        }
        for record in records {
            if let Some(row) = rejected_row(record) {
                self.first
                    .entry(row)
                    .or_insert_with(|| RecordedAt::of(record));
            }
        }
    }

    /// The record that first rejected the row `record` rejects again.
    pub fn repeat_of(&self, record: &RunRecord) -> Option<&RecordedAt> {
        let row = rejected_row(record)?;
        if self
            .inputs
            .get(&row.0)
            .is_none_or(|inputs| inputs.len() != 1)
        {
            return None;
        }
        self.first.get(&row).filter(|first| !first.holds(record))
    }
}

fn source_key(record: &RunRecord) -> Option<&str> {
    record
        .journal
        .stage
        .as_ref()
        .filter(|stage| stage.stage_type.is_source())
        .map(|stage| stage.key.as_str())
}

fn pinned_input(record: &RunRecord) -> Option<(&str, String)> {
    if record.journal.kind != RunJournalKind::Data {
        return None;
    }
    let source = source_key(record)?;
    let RunRecordData::Chain(row) = &record.record else {
        return None;
    };
    let digest = match &row.payload {
        ChainPayload::Execution(ExecutionPayload::SourceInputPinned(pin)) => pin.identity.digest(),
        ChainPayload::Execution(
            ExecutionPayload::SourcePositioned(resumed)
            | ExecutionPayload::SourceStartedOnResume(resumed),
        ) => resumed.identity_digest.clone(),
        _ => return None,
    };
    Some((source, digest))
}

fn rejected_row(record: &RunRecord) -> Option<(String, u64)> {
    if record.journal.kind != RunJournalKind::Error {
        return None;
    }
    let source = source_key(record)?;
    let RunRecordData::Chain(row) = &record.record else {
        return None;
    };
    let ChainPayload::Execution(ExecutionPayload::SourceRecordRejected(rejected)) = &row.payload
    else {
        return None;
    };
    Some((
        source.to_owned(),
        rejected.diagnostic.location().record_index()?,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::context::StageType;
    use obzenflow_core::event::payloads::execution_payload::{
        SourceInputPinnedFact, SourceRecordRejectedFact, SourceResumeFact,
    };
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{
        ChainEventFactory, JournalRecord, SourceDiagnostic, SourceDiagnosticReason,
    };
    use obzenflow_core::id::{FlowId, JournalId, StageId};
    use obzenflow_core::journal::read::{RunRecordKind, RunStage, RUN_RECORD_VERSION};
    use obzenflow_core::source::SourceInputIdentity;
    use obzenflow_core::{JournalWriterId, SystemId, WriterId};

    struct Run {
        identity: RunIdentity,
        stage: StageId,
        data: JournalId,
        error: JournalId,
        records: Vec<RunRecord>,
    }

    impl Run {
        fn new() -> Self {
            Self {
                identity: RunIdentity {
                    flow_id: FlowId::new(),
                    pipeline_writer_id: SystemId::new().into(),
                },
                stage: StageId::new(),
                data: JournalId::new(),
                error: JournalId::new(),
                records: Vec::new(),
            }
        }

        fn journal(&self, id: JournalId, kind: RunJournalKind) -> RunJournal {
            RunJournal {
                id,
                kind,
                stage: Some(RunStage {
                    key: "transactions".into(),
                    id: self.stage,
                    stage_type: StageType::FiniteSource,
                    is_effectful: false,
                }),
            }
        }

        fn push(&mut self, kind: RunJournalKind, payload: ExecutionPayload) -> &RunRecord {
            let journal = match kind {
                RunJournalKind::Error => self.error,
                _ => self.data,
            };
            let position = JournalPosition(
                self.records
                    .iter()
                    .filter(|record| record.journal.id == journal)
                    .count() as u64,
            );
            let event = ChainEventFactory::execution_event(WriterId::from(self.stage), payload);
            self.records.push(RunRecord {
                version: RUN_RECORD_VERSION,
                run: self.identity.clone(),
                journal: self.journal(journal, kind),
                position,
                kind: RunRecordKind::Execution,
                record: RunRecordData::Chain(Box::new(JournalRecord::new(
                    JournalWriterId::new(),
                    event,
                ))),
            });
            self.records.last().expect("pushed record")
        }

        fn pin(&mut self, file: &str) {
            let identity = SourceInputIdentity::new("csv", &serde_json::json!({ "file": file }))
                .expect("identity");
            let beginning = serde_json::from_value(serde_json::json!({
                "schema_version": 1,
                "value": 0
            }))
            .expect("position");
            self.push(
                RunJournalKind::Data,
                ExecutionPayload::SourceInputPinned(SourceInputPinnedFact {
                    identity,
                    beginning,
                }),
            );
        }

        fn positioned(&mut self, file: &str) {
            let identity_digest =
                SourceInputIdentity::new("csv", &serde_json::json!({ "file": file }))
                    .expect("identity")
                    .digest();
            self.push(
                RunJournalKind::Data,
                ExecutionPayload::SourcePositioned(SourceResumeFact {
                    identity_digest,
                    generation: 1,
                }),
            );
        }

        fn reject(&mut self, index: u64) -> RunRecord {
            let diagnostic =
                SourceDiagnostic::new(SourceDiagnosticReason::InvalidRecord).record(index);
            self.push(
                RunJournalKind::Error,
                ExecutionPayload::SourceRecordRejected(SourceRecordRejectedFact {
                    kind: ErrorKind::Validation,
                    detail: format!("record {index}: {diagnostic}"),
                    diagnostic,
                }),
            )
            .clone()
        }
    }

    #[test]
    fn a_row_rejected_again_after_a_resume_repeats_the_first_record() {
        let mut killed = Run::new();
        killed.pin("transactions.csv");
        let first = killed.reject(164);
        let other = killed.reject(117);
        let mut resumed = Run::new();
        resumed.positioned("transactions.csv");
        let again = resumed.reject(164);
        let new = resumed.reject(201);

        let mut lineage = LineageRejections::default();
        lineage.fold_run(&killed.records);
        lineage.fold_run(&resumed.records);

        assert!(lineage.repeat_of(&first).is_none());
        assert!(lineage.repeat_of(&other).is_none());
        assert!(lineage.repeat_of(&new).is_none());
        let repeated = lineage.repeat_of(&again).expect("a repeat");
        assert!(repeated.holds(&first));
        assert_eq!(repeated.run, killed.identity);
    }

    #[test]
    fn a_shown_run_repeats_a_row_its_lineage_rejected() {
        let mut killed = Run::new();
        killed.pin("transactions.csv");
        let first = killed.reject(164);
        let mut resumed = Run::new();
        let again = resumed.reject(164);

        let mut lineage = LineageRejections::default();
        lineage.fold_run(&killed.records);

        let repeated = lineage.repeat_of(&again).expect("a repeat");
        assert!(repeated.holds(&first));
    }

    #[test]
    fn rows_repeat_only_while_their_source_pinned_one_input() {
        let mut unpinned = Run::new();
        unpinned.reject(164);
        let mut resumed = Run::new();
        let again = resumed.reject(164);
        let mut lineage = LineageRejections::default();
        lineage.fold_run(&unpinned.records);
        lineage.fold_run(&resumed.records);
        assert!(
            lineage.repeat_of(&again).is_none(),
            "no pin names the input"
        );

        let mut first = Run::new();
        first.pin("monday.csv");
        first.reject(164);
        let mut second = Run::new();
        second.positioned("tuesday.csv");
        let again = second.reject(164);
        let mut lineage = LineageRejections::default();
        lineage.fold_run(&first.records);
        lineage.fold_run(&second.records);
        assert!(lineage.repeat_of(&again).is_none(), "two inputs");
    }

    #[test]
    fn a_rejection_that_names_no_record_never_repeats() {
        let mut killed = Run::new();
        killed.pin("transactions.csv");
        let unplaced = || {
            ExecutionPayload::SourceRecordRejected(SourceRecordRejectedFact {
                kind: ErrorKind::Validation,
                detail: "invalid record".into(),
                diagnostic: SourceDiagnosticReason::InvalidRecord.into(),
            })
        };
        killed.push(RunJournalKind::Error, unplaced());
        let mut resumed = Run::new();
        let again = resumed.push(RunJournalKind::Error, unplaced()).clone();
        let mut lineage = LineageRejections::default();
        lineage.fold_run(&killed.records);
        lineage.fold_run(&resumed.records);
        assert!(lineage.repeat_of(&again).is_none());
    }
}
