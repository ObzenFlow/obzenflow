// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Keeps each stage's latest status and any final metrics for Studio's initial
//! display, including stages that finished before the browser connected.

use obzenflow_core::event::journal_record::ChainJournalRecord;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::ChainPayload;
use obzenflow_core::{web::SseFrame, StageId};
use std::collections::BTreeMap;

#[derive(Clone, Default)]
pub(super) struct StageLifecycleView {
    latest_by_stage: BTreeMap<StageId, ChainJournalRecord>,
}

impl StageLifecycleView {
    pub(super) fn observe(&mut self, record: &ChainJournalRecord) {
        let ChainPayload::Execution(ExecutionPayload::StageLifecycle(event)) = &record.payload
        else {
            return;
        };
        let stage_id = event.stage_id();
        let previous =
            self.latest_by_stage
                .get(&stage_id)
                .and_then(|previous| match &previous.payload {
                    ChainPayload::Execution(ExecutionPayload::StageLifecycle(event)) => Some(event),
                    _ => None,
                });
        // A repeated outcome lacking accounting cannot erase settled totals.
        let keep = matches!(
            (previous, event),
            (
                Some(StageLifecycleFact::Completed {
                    accounting: Some(_),
                    ..
                }),
                StageLifecycleFact::Completed {
                    accounting: None,
                    ..
                }
            ) | (
                Some(StageLifecycleFact::Cancelled {
                    accounting: Some(_),
                    ..
                }),
                StageLifecycleFact::Cancelled {
                    accounting: None,
                    ..
                }
            ) | (
                Some(StageLifecycleFact::Failed {
                    accounting: Some(_),
                    ..
                }),
                StageLifecycleFact::Failed {
                    accounting: None,
                    ..
                }
            )
        );
        if !keep {
            self.latest_by_stage.insert(stage_id, record.clone());
        }
    }

    pub(super) fn snapshot_frames(&self) -> Vec<SseFrame> {
        self.latest_by_stage
            .values()
            .filter_map(super::facts::stage_message)
            .map(|message| message.frame(None))
            .collect()
    }
}
