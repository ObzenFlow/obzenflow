// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded presentation context. Parent links come only from recorded IDs;
//! clock components never stand in for input records or journal positions.

use obzenflow::journal::read::*;
use obzenflow_core::event::CausalCoordinate;
use obzenflow_core::journal::causal::{CausalProof, CausalProofCache};
use std::collections::{btree_map::Entry, BTreeMap, BTreeSet, VecDeque};

const MAX_REFERENCES: usize = 4096;

#[derive(Clone)]
pub(super) struct Reference {
    pub event_type: String,
    pub parents: Vec<String>,
}

pub(super) struct Stage {
    pub writer: String,
    pub key: String,
}

pub(super) struct ClockComponent {
    pub coordinate: CausalCoordinate,
    pub number: usize,
    pub value: u64,
}

pub(super) struct ObservedClock {
    pub position: JournalPosition,
    pub values: BTreeMap<CausalCoordinate, u64>,
}

pub(super) struct JournalContext {
    pub journal: RunJournal,
    pub name: String,
    pub last_clock: Option<ObservedClock>,
}

impl JournalContext {
    fn new(journal: &RunJournal) -> Self {
        let name = match (&journal.stage, journal.kind) {
            (Some(stage), RunJournalKind::Error) => format!("{}/error", stage.key),
            (Some(stage), _) => stage.key.clone(),
            (_, RunJournalKind::System) => "pipeline".into(),
            (_, RunJournalKind::MetricsCoordination) => "metrics/coordination".into(),
            (_, RunJournalKind::MetricsExport) => "metrics/export".into(),
            _ => journal.id.to_string(),
        };
        Self {
            journal: journal.clone(),
            name,
            last_clock: None,
        }
    }
}

pub(super) struct Context {
    causal: CausalProofCache,
    causal_error: Option<String>,
    pub stages: Vec<Stage>,
    pub supervisors: BTreeMap<String, SupervisorDescriptor>,
    pub journals: BTreeMap<obzenflow_core::JournalId, JournalContext>,
    journal_numbers: BTreeMap<obzenflow_core::JournalId, usize>,
    journal_label_updates: BTreeSet<obzenflow_core::JournalId>,
    references: BTreeMap<String, Reference>,
    insertion_order: VecDeque<String>,
}

impl Context {
    pub fn new<'a>(journals: impl Iterator<Item = &'a RunJournal>) -> Self {
        let mut stages = BTreeMap::new();
        let mut journal_contexts = BTreeMap::new();
        for journal in journals {
            journal_contexts.insert(journal.id, JournalContext::new(journal));
            if let Some(stage) = &journal.stage {
                stages.insert(stage.key.clone(), stage.clone());
            }
        }
        let mut stages: Vec<_> = stages.into_values().collect();
        stages.sort_by_key(|stage| {
            let rank = match stage.stage_type {
                StageType::FiniteSource | StageType::InfiniteSource => 0,
                StageType::Sink => 2,
                _ => 1,
            };
            (rank, stage.key.clone())
        });
        // Number the whole inventory before observing records. Empty error
        // journals keep their slots even when omitted from the final matrix.
        // Put data journals first so the common clocks use the small numbers.
        let mut ordered: Vec<_> = journal_contexts.values().collect();
        ordered.sort_by_key(|journal| {
            let kind = match journal.journal.kind {
                RunJournalKind::System => 0,
                RunJournalKind::MetricsCoordination => 1,
                RunJournalKind::MetricsExport => 2,
                RunJournalKind::Data => 3,
                RunJournalKind::Error => 4,
            };
            let stage = journal.journal.stage.as_ref().map_or(0, |owner| {
                stages
                    .iter()
                    .position(|stage| stage.key == owner.key)
                    .unwrap_or(stages.len())
            });
            (kind, stage, journal.journal.id)
        });
        let journal_numbers: BTreeMap<_, _> = ordered
            .into_iter()
            .enumerate()
            .map(|(index, journal)| (journal.journal.id, index + 1))
            .collect();
        let journal_label_updates = journal_numbers.keys().copied().collect();
        Self {
            causal: CausalProofCache::new(MAX_REFERENCES, MAX_REFERENCES * 256),
            causal_error: None,
            stages: stages
                .into_iter()
                .map(|stage| Stage {
                    writer: format!("writer_{}", stage.id),
                    key: stage.key,
                })
                .collect(),
            journals: journal_contexts,
            journal_numbers,
            journal_label_updates,
            references: BTreeMap::new(),
            supervisors: BTreeMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    pub fn remember(&mut self, record: &RunRecord) {
        if let Entry::Vacant(entry) = self.journals.entry(record.journal.id) {
            // An earlier clock may have referenced this journal before its
            // metadata arrived. Resolve its legend name without renumbering.
            entry.insert(JournalContext::new(&record.journal));
            self.journal_label_updates.insert(record.journal.id);
        }
        self.number_journal(record.journal.id);
        for coordinate in clock(record).keys() {
            self.number_journal(*coordinate.journal_writer_id.as_journal_id());
        }
        // Physical journal position chooses the last clock, independently of
        // display filtering, cross-journal visitation and forwarded event IDs.
        let journal = self
            .journals
            .get_mut(&record.journal.id)
            .expect("observed journal has metadata");
        if journal
            .last_clock
            .as_ref()
            .is_none_or(|last| last.position < record.position)
        {
            journal.last_clock = Some(ObservedClock {
                position: record.position,
                values: clock(record).clone(),
            });
        }
        if let Err(error) = self.causal.admit_run_record(record) {
            self.causal_error = Some(error.to_string());
        }
        let id = event_id(record);
        if self.references.contains_key(&id) {
            return;
        }
        if self.references.len() == MAX_REFERENCES {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.references.remove(&oldest);
            }
        }
        self.references.insert(
            id.clone(),
            Reference {
                event_type: event_type(record).into(),
                parents: parent_ids(record),
            },
        );
        self.insertion_order.push_back(id);
    }

    pub fn is_effectful(&self, record: &RunRecord) -> bool {
        record
            .journal
            .stage
            .as_ref()
            .is_some_and(|stage| stage.is_effectful)
    }

    pub fn parents_available(&self, record: &RunRecord) -> bool {
        parent_ids(record)
            .iter()
            .all(|id| self.references.contains_key(id))
    }

    /// Omit a redundant ancestor only when another recorded parent explicitly
    /// names it. No guessing from clock dominance, correlation or adjacency.
    pub fn inputs(&self, record: &RunRecord) -> Vec<Option<&Reference>> {
        let parents = parent_ids(record);
        parents
            .iter()
            .filter(|id| {
                !parents.iter().any(|other| {
                    other != *id
                        && self
                            .references
                            .get(other)
                            .is_some_and(|reference| reference.parents.contains(id))
                })
            })
            .map(|id| self.references.get(id))
            .collect()
    }

    pub fn writer_name<'a>(&'a self, writer: &'a str) -> &'a str {
        self.stages
            .iter()
            .find(|stage| stage.writer == writer)
            .map(|stage| stage.key.as_str())
            .or_else(|| {
                self.supervisors
                    .get(writer)
                    .map(|descriptor| descriptor.name.as_str())
            })
            .unwrap_or(writer)
    }

    pub fn register_supervisor(&mut self, record: &RunRecord) -> Result<(), super::Error> {
        let descriptor = match &record.record {
            RunRecordData::System(row) => match &row.payload {
                SystemPayload::SupervisorRegistered { descriptor } => Some(descriptor),
                _ => None,
            },
            RunRecordData::Chain(row) => match &row.payload {
                obzenflow_core::event::ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SupervisorRegistered { descriptor }) => Some(descriptor),
                _ => None,
            },
        };
        if let Some(descriptor) = descriptor {
            let writer = writer_id(record);
            if self
                .supervisors
                .get(&writer)
                .is_some_and(|known| known != descriptor)
            {
                return Err(
                    format!("conflicting recorded supervisor identities for {writer}").into(),
                );
            }
            self.supervisors.insert(writer, descriptor.clone());
        }
        Ok(())
    }

    pub fn causal_proof(&self, record: &RunRecord) -> CausalProof {
        match &self.causal_error {
            Some(reason) => CausalProof::Invalid {
                reason: reason.clone(),
            },
            None => self.causal.verify_run_record(record),
        }
    }

    fn number_journal(&mut self, id: obzenflow_core::JournalId) {
        let next = self.journal_numbers.len() + 1;
        if let Entry::Vacant(entry) = self.journal_numbers.entry(id) {
            entry.insert(next);
            self.journal_label_updates.insert(id);
        }
    }

    pub fn journal_number(&self, id: &obzenflow_core::JournalId) -> usize {
        self.journal_numbers[id]
    }

    pub fn take_journal_labels(&mut self) -> Vec<(usize, String)> {
        let mut labels: Vec<_> = std::mem::take(&mut self.journal_label_updates)
            .into_iter()
            .map(|id| {
                let name = self
                    .journals
                    .get(&id)
                    .map(|journal| journal.name.clone())
                    .unwrap_or_else(|| id.to_string());
                (self.journal_number(&id), name)
            })
            .collect();
        labels.sort_by_key(|(number, _)| *number);
        labels
    }

    pub fn clock_components(
        &self,
        values: &BTreeMap<CausalCoordinate, u64>,
    ) -> Vec<ClockComponent> {
        let mut components: Vec<_> = values
            .iter()
            .map(|(coordinate, value)| ClockComponent {
                coordinate: *coordinate,
                number: self.journal_number(coordinate.journal_writer_id.as_journal_id()),
                value: *value,
            })
            .collect();
        components.sort_by_key(|component| component.number);
        components
    }
}

pub(super) fn event_type(record: &RunRecord) -> &str {
    match &record.record {
        RunRecordData::Chain(row) => row.event_type_name(),
        RunRecordData::System(row) => row.event_type_name(),
    }
}

pub(super) fn event_id(record: &RunRecord) -> String {
    match &record.record {
        RunRecordData::Chain(row) => row.id().to_string(),
        RunRecordData::System(row) => row.id().to_string(),
    }
}

pub(super) fn parent_ids(record: &RunRecord) -> Vec<String> {
    match &record.record {
        RunRecordData::Chain(row) => row
            .envelope
            .provenance
            .event
            .causality
            .parent_ids
            .iter()
            .map(ToString::to_string)
            .collect(),
        RunRecordData::System(_) => Vec::new(),
    }
}

pub(super) fn clock(record: &RunRecord) -> &BTreeMap<CausalCoordinate, u64> {
    match &record.record {
        RunRecordData::Chain(row) => &row.envelope.provenance.journal.vector_clock.clocks,
        RunRecordData::System(row) => &row.envelope.provenance.journal.vector_clock.clocks,
    }
}

pub(super) fn writer_id(record: &RunRecord) -> String {
    match &record.record {
        RunRecordData::Chain(row) => row.writer_id().to_string(),
        RunRecordData::System(row) => row.writer_id().to_string(),
    }
}
