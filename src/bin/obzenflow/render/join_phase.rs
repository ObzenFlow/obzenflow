// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::context::event_id;
use super::{RunJournalKind, RunRecord, RunRecordData};
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, JoinPhaseFact};
use obzenflow_core::event::ChainPayload;
use std::collections::{BTreeMap, VecDeque};

const MAX_FACTS: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Phase {
    name: &'static str,
    generation: u64,
}

#[derive(Default)]
pub(super) struct JoinPhases {
    latest: BTreeMap<String, Phase>,
    by_fact: BTreeMap<String, Phase>,
    fact_order: VecDeque<String>,
}

impl JoinPhases {
    pub(super) fn record(&mut self, record: &RunRecord) {
        if !matches!(record.journal.kind, RunJournalKind::Data) {
            return;
        }
        let Some(stage) = &record.journal.stage else {
            return;
        };
        let RunRecordData::Chain(row) = &record.record else {
            return;
        };
        match &row.payload {
            ChainPayload::Execution(ExecutionPayload::JoinPhase(fact)) => {
                let name = match fact {
                    JoinPhaseFact::EnrichingEntered { .. } => "enriching",
                    JoinPhaseFact::LiveEntered { .. } => "live",
                };
                self.latest.insert(
                    stage.key.clone(),
                    Phase {
                        name,
                        generation: fact.generation(),
                    },
                );
            }
            ChainPayload::Fact(_) => {
                let Some(phase) = self.latest.get(&stage.key).copied() else {
                    return;
                };
                if self.by_fact.len() == MAX_FACTS {
                    if let Some(oldest) = self.fact_order.pop_front() {
                        self.by_fact.remove(&oldest);
                    }
                }
                let id = event_id(record);
                self.fact_order.push_back(id.clone());
                self.by_fact.insert(id, phase);
            }
            _ => {}
        }
    }

    pub(super) fn heading_suffix(&self, record: &RunRecord) -> Option<String> {
        self.by_fact.get(&event_id(record)).map(|phase| {
            format!(
                ", phase: {} since generation {}",
                phase.name, phase.generation
            )
        })
    }
}
