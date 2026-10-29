// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Rows a lineage's sources rejected. A row is one source's record index while
//! that source pinned one input across the earlier runs, so a later run that
//! rejects the same row again records a repeat. Cursors read the earlier
//! journals one record at a time, in row order, and hold no rejection beyond
//! their own.

use super::{JournalCursor, JournalReadError, SourceJournals};
use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
use obzenflow_core::event::ChainPayload;
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::read::{
    JournalPosition, RunIdentity, RunJournal, RunJournalKind, RunRecord, RunRecordData,
};
use serde::Serialize;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

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
}

/// Finds the first record of a rejected row among the earlier runs, oldest
/// first. A cursor per source and pair of runs walks the older run's
/// rejections forward while the newer run's rejections rise.
#[derive(Default)]
pub(super) struct Repeats {
    runs: Vec<SourceJournals>,
    inputs: BTreeMap<String, BTreeSet<String>>,
    ordered: BTreeMap<JournalId, bool>,
    cursors: BTreeMap<(String, usize, usize), RowCursor>,
}

struct RowCursor {
    journal: JournalCursor,
    front: Option<(u64, RecordedAt)>,
    last: u64,
}

impl Repeats {
    pub(super) fn add(&mut self, run: SourceJournals) {
        self.runs.push(run);
    }

    pub(super) async fn repeat_of(
        &mut self,
        shown: &RunIdentity,
        record: &RunRecord,
    ) -> Result<Option<RecordedAt>, JournalReadError> {
        let Some((source, row)) = rejected_row(record) else {
            return Ok(None);
        };
        let newer = if record.run == *shown {
            self.runs.len()
        } else {
            match self.runs.iter().position(|run| run.run == record.run) {
                Some(newer) => newer,
                None => return Ok(None),
            }
        };
        if newer == 0 || self.inputs(&source).await?.len() != 1 {
            return Ok(None);
        }
        self.cursors
            .retain(|(key, by, _), _| key != &source || *by >= newer);
        for older in 0..newer {
            let Some((journal, path)) = self.runs[older].errors.get(&source).cloned() else {
                continue;
            };
            let run = self.runs[older].run.clone();
            let ordered = self.ordered(&run, &journal, &path).await?;
            let cursor = self
                .cursors
                .entry((source.clone(), newer, older))
                .or_insert_with(|| RowCursor {
                    journal: JournalCursor::new(run, journal, path),
                    front: None,
                    last: 0,
                });
            if let Some(first) = cursor.find(row, ordered).await? {
                return Ok(Some(first));
            }
        }
        Ok(None)
    }

    async fn inputs(&mut self, source: &str) -> Result<&BTreeSet<String>, JournalReadError> {
        if !self.inputs.contains_key(source) {
            let mut inputs = BTreeSet::new();
            for run in &self.runs {
                let Some((journal, path)) = run.data.get(source) else {
                    continue;
                };
                let mut cursor = JournalCursor::new(run.run.clone(), journal.clone(), path.clone());
                while let Some(record) = cursor.next().await? {
                    if let Some((_, digest)) = pinned_input(&record) {
                        inputs.insert(digest);
                    }
                }
            }
            self.inputs.insert(source.to_owned(), inputs);
        }
        Ok(&self.inputs[source])
    }

    async fn ordered(
        &mut self,
        run: &RunIdentity,
        journal: &RunJournal,
        path: &Path,
    ) -> Result<bool, JournalReadError> {
        if let Some(ordered) = self.ordered.get(&journal.id) {
            return Ok(*ordered);
        }
        let mut cursor = JournalCursor::new(run.clone(), journal.clone(), path.to_path_buf());
        let mut ordered = true;
        let mut last = 0;
        while let Some(record) = cursor.next().await? {
            if let Some((_, row)) = rejected_row(&record) {
                if row < last {
                    ordered = false;
                    break;
                }
                last = row;
            }
        }
        self.ordered.insert(journal.id, ordered);
        Ok(ordered)
    }
}

impl RowCursor {
    async fn find(
        &mut self,
        row: u64,
        ordered: bool,
    ) -> Result<Option<RecordedAt>, JournalReadError> {
        if !ordered || row < self.last {
            self.journal.restart();
            self.front = None;
        }
        self.last = row;
        loop {
            if self.front.is_none() {
                self.front = self.next_row().await?;
            }
            match &self.front {
                None => return Ok(None),
                Some((at, first)) if *at == row => return Ok(Some(first.clone())),
                Some((at, _)) if ordered && *at > row => return Ok(None),
                Some(_) => self.front = None,
            }
        }
    }

    async fn next_row(&mut self) -> Result<Option<(u64, RecordedAt)>, JournalReadError> {
        while let Some(record) = self.journal.next().await? {
            if let Some((_, row)) = rejected_row(&record) {
                return Ok(Some((row, RecordedAt::of(&record))));
            }
        }
        Ok(None)
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
