// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Bounded presentation context. Parent links come only from recorded IDs;
//! clock components never stand in for input records or journal positions.

use obzenflow::journal::read::*;
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

pub(super) struct ClockComponent<'a> {
    pub writer: &'a str,
    pub name: Option<&'a str>,
    pub value: u64,
}

pub(super) struct Context {
    pub stages: Vec<Stage>,
    references: BTreeMap<String, Reference>,
    insertion_order: VecDeque<String>,
}

impl Context {
    pub fn new<'a>(journals: impl Iterator<Item = &'a RunJournal>) -> Self {
        let mut stages = BTreeMap::new();
        for journal in journals {
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
            stages: stages
                .into_iter()
                .map(|stage| Stage {
                    writer: format!("writer_{}", stage.id),
                    key: stage.key,
                })
                .collect(),
            references: BTreeMap::new(),
            insertion_order: VecDeque::new(),
        }
    }

    pub fn remember(&mut self, record: &RunRecord) {
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
            .map_or(writer, |stage| stage.key.as_str())
    }

    pub fn clock_components<'a>(
        &'a self,
        values: &'a BTreeMap<String, u64>,
        vector: bool,
        run: &RunIdentity,
    ) -> Vec<ClockComponent<'a>> {
        if values.is_empty() {
            return Vec::new();
        }
        let mut components = Vec::new();
        if vector {
            components.extend(self.stages.iter().map(|stage| ClockComponent {
                writer: &stage.writer,
                name: None,
                value: values.get(&stage.writer).copied().unwrap_or(0),
            }));
        }
        let pipeline_writer = run.pipeline_writer_id.to_string();
        for (writer, value) in values {
            if vector && self.stages.iter().any(|stage| stage.writer == *writer) {
                continue;
            }
            let name = if *writer == pipeline_writer {
                "pipeline"
            } else {
                self.writer_name(writer)
            };
            components.push(ClockComponent {
                writer,
                name: Some(name),
                value: *value,
            });
        }
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

pub(super) fn clock(record: &RunRecord) -> &BTreeMap<String, u64> {
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
