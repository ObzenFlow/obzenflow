// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! How each source of a resumed run continued, projected from that run's own
//! source facts (FLOWIP-134i B8, B15, FLOWIP-122i), and where a replay's
//! recording ended (B13).

use super::{RunRecord, RunRecordData};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, ReattachGuarantee, SourcePollContinuation,
};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::{
    ChainPayload, ReplayLifecycleEvent, SourceDiagnosticReason, StageFatalReason,
};
use std::collections::BTreeMap;

/// One source's continuation, as its journal records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SourceContinuation {
    InputChanged,
    Positioned {
        generation: u64,
    },
    Reattached {
        generation: u64,
        guarantee: ReattachGuarantee,
    },
    /// It reproduced its lineage's completion and never opened.
    JournalOnly(EofKind),
}

impl SourceContinuation {
    fn then(self, next: Self) -> Self {
        match (self, next) {
            (Self::InputChanged, _) => Self::InputChanged,
            (_, next) => next,
        }
    }
}

/// The stage and input where a replay reached the end of its recording.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct RecordingEnd {
    pub stage_key: String,
    pub input_position: Option<u64>,
}

#[derive(Default)]
pub(super) struct Continuations {
    sources: BTreeMap<String, SourceContinuation>,
    recording_end: Option<RecordingEnd>,
}

impl Continuations {
    pub(super) fn record(&mut self, record: &RunRecord) {
        let RunRecordData::Chain(row) = &record.record else {
            return;
        };
        if let ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
            generation, ..
        }) = &row.payload
        {
            if record
                .journal
                .stage
                .as_ref()
                .is_some_and(|stage| stage.stage_type.is_source())
            {
                self.continue_with(
                    record,
                    SourceContinuation::Positioned {
                        generation: generation.0,
                    },
                );
            }
            return;
        }
        let ChainPayload::Execution(execution) = &row.payload else {
            return;
        };
        if let ExecutionPayload::StageFatalRecorded(fatal) = execution {
            // The first by stage key, as the replay's refusal names it.
            if fatal.reason == StageFatalReason::RecordingEnded
                && self
                    .recording_end
                    .as_ref()
                    .is_none_or(|end| fatal.stage_key < end.stage_key)
            {
                self.recording_end = Some(RecordingEnd {
                    stage_key: fatal.stage_key.clone(),
                    input_position: fatal.input_position,
                });
            }
            return;
        }
        let continued = match execution {
            ExecutionPayload::SourcePollFailed(failure)
                if failure.continuation == SourcePollContinuation::Terminal
                    && failure.diagnostic.reason() == SourceDiagnosticReason::InputChanged =>
            {
                SourceContinuation::InputChanged
            }
            ExecutionPayload::SourceReattached(fact) => SourceContinuation::Reattached {
                generation: fact.generation,
                guarantee: fact.guarantee,
            },
            ExecutionPayload::ReplayLifecycle(ReplayLifecycleEvent::Completed {
                synthesized_eof_kind: Some(kind),
                ..
            }) => SourceContinuation::JournalOnly(*kind),
            _ => return,
        };
        self.continue_with(record, continued);
    }

    fn continue_with(&mut self, record: &RunRecord, continued: SourceContinuation) {
        let Some(stage) = &record.journal.stage else {
            return;
        };
        let merged = match self.sources.remove(&stage.key) {
            Some(current) => current.then(continued),
            None => continued,
        };
        self.sources.insert(stage.key.clone(), merged);
    }

    pub(super) fn source(&self, stage_key: &str) -> Option<&SourceContinuation> {
        self.sources.get(stage_key)
    }

    pub(super) fn recording_end(&self) -> Option<&RecordingEnd> {
        self.recording_end.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::ReattachGuarantee;
    use super::SourceContinuation::*;

    #[test]
    fn changed_input_continuation_stays_final() {
        let reattached = Reattached {
            generation: 2,
            guarantee: ReattachGuarantee::AtMostOnce,
        };
        assert_eq!(InputChanged.then(reattached.clone()), InputChanged);
        assert_eq!(
            JournalOnly(super::EofKind::Natural).then(reattached.clone()),
            reattached
        );
    }
}
