// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded presentation context. Parent links come only from recorded IDs;
//! clock components never stand in for input records or journal positions.

use obzenflow::journal::read::*;
use obzenflow_core::event::CausalCoordinate;
use obzenflow_core::journal::causal::{CausalProof, CausalProofCache};
use std::collections::{BTreeMap, VecDeque};

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
    pub name: String,
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
            references: BTreeMap::new(),
            supervisors: BTreeMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    pub fn remember(&mut self, record: &RunRecord) {
        // Physical journal position chooses the last clock, independently of
        // display filtering, cross-journal visitation and forwarded event IDs.
        let journal = self
            .journals
            .entry(record.journal.id)
            .or_insert_with(|| JournalContext::new(&record.journal));
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

    pub fn clock_components(
        &self,
        values: &BTreeMap<CausalCoordinate, u64>,
        _vector: bool,
        _run: &RunIdentity,
    ) -> Vec<ClockComponent> {
        values
            .iter()
            .map(|(coordinate, value)| {
                let name = self
                    .journals
                    .get(coordinate.journal_writer_id.as_journal_id())
                    .map(|journal| journal.name.clone())
                    .unwrap_or_else(|| coordinate.journal_writer_id.as_journal_id().to_string());
                ClockComponent {
                    coordinate: *coordinate,
                    name,
                    value: *value,
                }
            })
            .collect()
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
