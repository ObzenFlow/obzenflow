// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Each stage's records, counted once: data a stage recorded or a sink
//! delivered, and errors by kind. Runtime and effect entries are not records.

use obzenflow::journal::read::{ChainPayload, ExecutionPayload};
use obzenflow::journal::read::{DeliveryResult, RunJournalKind, RunRecord, RunRecordData};
use obzenflow::journal::ProcessingStatus;
use obzenflow_core::event::SinkOperationPhase;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct StageRecords {
    pub data: u64,
    pub errors: BTreeMap<String, u64>,
}

impl StageRecords {
    pub(super) fn error_cell(&self) -> String {
        if self.errors.is_empty() {
            return "0".into();
        }
        self.errors
            .iter()
            .map(|(kind, count)| format!("{count} {kind}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

#[derive(Default)]
pub(super) struct RecordCounts {
    stages: BTreeMap<String, StageRecords>,
    repeats: u64,
}

impl RecordCounts {
    pub(super) fn record(&mut self, record: &RunRecord, repeat: bool) {
        let (Some(stage), RunRecordData::Chain(row)) = (&record.journal.stage, &record.record)
        else {
            return;
        };
        match record.journal.kind {
            RunJournalKind::Data => {
                let data = match &row.payload {
                    ChainPayload::Fact(_) => {
                        row.envelope.provenance.event.processing.status.is_success()
                    }
                    ChainPayload::Delivery(delivery) => {
                        matches!(delivery.result, DeliveryResult::Success { .. })
                    }
                    _ => false,
                };
                if data {
                    self.stages.entry(stage.key.clone()).or_default().data += 1;
                }
            }
            RunJournalKind::Error => {
                let ProcessingStatus::Error { kind, .. } =
                    &row.envelope.provenance.event.processing.status
                else {
                    return;
                };
                if matches!(&row.payload, ChainPayload::Execution(ExecutionPayload::SinkOperationFailed(failed)) if matches!(failed.phase, SinkOperationPhase::Write(_)))
                {
                    return;
                }
                if repeat {
                    self.repeats += 1;
                    return;
                }
                let kind = kind
                    .as_ref()
                    .map_or_else(|| "Unknown".into(), |kind| format!("{kind:?}"));
                *self
                    .stages
                    .entry(stage.key.clone())
                    .or_default()
                    .errors
                    .entry(kind)
                    .or_default() += 1;
            }
            _ => {}
        }
    }

    pub(super) fn stage(&self, key: &str) -> Option<&StageRecords> {
        self.stages.get(key)
    }

    pub(super) fn repeats(&self) -> u64 {
        self.repeats
    }
}
