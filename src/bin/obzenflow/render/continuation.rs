// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! How each source of a resumed run continued, projected from that run's own
//! source facts (FLOWIP-134i B8, B15), and where a replay's recording ended
//! (B13).

use super::{RunRecord, RunRecordData};
use obzenflow_core::event::payloads::execution_payload::{
    ExecutionPayload, SourceContinuationRefusal,
};
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::{ChainPayload, ReplayLifecycleEvent, StageFatalReason};
use std::collections::BTreeMap;

/// One source's continuation, as its journal records it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum SourceContinuation {
    Refused(SourceContinuationRefusal),
    SeekFailed,
    /// It never pinned, so it started from its beginning.
    Started {
        generation: u64,
    },
    Positioned {
        generation: u64,
    },
    Reattached {
        generation: u64,
    },
    /// It reproduced its lineage's completion and never opened.
    JournalOnly(EofKind),
}

impl SourceContinuation {
    /// A refusal or failed seek is final; positioning names a live handoff
    /// more exactly than the reattachment every handoff announces.
    fn then(self, next: Self) -> Self {
        match (self, next) {
            (settled @ (Self::Refused(_) | Self::SeekFailed), _) => settled,
            (exact @ (Self::Started { .. } | Self::Positioned { .. }), Self::Reattached { .. }) => {
                exact
            }
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
            ExecutionPayload::SourceContinuationRefused(fact) => {
                SourceContinuation::Refused(fact.refusal.clone())
            }
            ExecutionPayload::SourceSeekFailed(_) => SourceContinuation::SeekFailed,
            ExecutionPayload::SourceStartedOnResume(fact) => SourceContinuation::Started {
                generation: fact.generation,
            },
            ExecutionPayload::SourcePositioned(fact) => SourceContinuation::Positioned {
                generation: fact.generation,
            },
            ExecutionPayload::ReplayLifecycle(ReplayLifecycleEvent::ResumedLive {
                generation,
                ..
            }) => SourceContinuation::Reattached {
                generation: *generation,
            },
            ExecutionPayload::ReplayLifecycle(ReplayLifecycleEvent::Completed {
                synthesized_eof_kind: Some(kind),
                ..
            }) => SourceContinuation::JournalOnly(*kind),
            _ => return,
        };
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
    use super::SourceContinuation::*;

    #[test]
    fn a_positioned_handoff_outranks_its_live_announcement_and_refusals_are_final() {
        let positioned = Positioned { generation: 1 };
        assert_eq!(
            positioned.clone().then(Reattached { generation: 1 }),
            positioned
        );
        assert_eq!(
            Reattached { generation: 1 }.then(Started { generation: 1 }),
            Started { generation: 1 }
        );
        assert_eq!(SeekFailed.then(Reattached { generation: 2 }), SeekFailed);
    }
}
