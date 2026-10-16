// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::event::schema::TypedPayload;
use crate::{EventId, StageId};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageFatalSeverity {
    Primary,
    Secondary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageFatalCode {
    Protocol,
    Replay,
    Configuration,
    Journal,
    Coordination,
    Resource,
    Termination,
}

/// Closed metric and diagnostic authority for stage-fatal failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StageFatalReason {
    ProtocolInputIntegrity,
    ReplayDivergence,
    /// A replay reached a cursor its recording holds no outcome for; only a
    /// resume finishes that work (FLOWIP-134i B13).
    RecordingEnded,
    /// A rebuilt effect's identity differs from its recording.
    DescriptorMismatch,
    /// The rebuild performed an effect its recording lacks.
    MissingRecordedEffect,
    /// The rebuild skipped an effect its recording holds.
    UnusedEffectHistory,
    /// A resume emitted a fact its recording lacks before the recording ends
    /// (FLOWIP-134k B7).
    MissingRecordedFact,
    /// A resume skipped a fact its recording holds (FLOWIP-134k B7).
    UnusedFactHistory,
    ConfigurationInvariant,
    EffectTargetAssertionMismatch,
    EffectPortRegistrationMissing,
    EffectPortResolutionFailed,
    EffectPortBindingMismatch,
    EffectPortTargetInvariantViolation,
    JournalFailure,
    CoordinationFailure,
    ResourceExhaustion,
    IncompleteTermination,
}

impl StageFatalReason {
    /// A refusal a resume makes when its build or configuration departs from
    /// its archive (FLOWIP-010t B11, FLOWIP-134k B7, B10).
    pub fn departs_from_archive(self) -> bool {
        matches!(
            self,
            Self::DescriptorMismatch
                | Self::MissingRecordedEffect
                | Self::UnusedEffectHistory
                | Self::MissingRecordedFact
                | Self::UnusedFactHistory
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageFatalRecorded {
    pub severity: StageFatalSeverity,
    pub stage_id: StageId,
    pub stage_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub causal_event_id: Option<EventId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_position: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_cause_event_id: Option<EventId>,
    pub code: StageFatalCode,
    pub reason: StageFatalReason,
    pub detail: String,
}

impl TypedPayload for StageFatalRecorded {
    const EVENT_KIND: crate::event::payloads::chain_payload::EventKind =
        crate::event::payloads::chain_payload::EventKind::Execution;
    fn into_chain_payload(self) -> Result<crate::event::ChainPayload, serde_json::Error> {
        Ok(crate::event::ChainPayload::Execution(
            super::execution_payload::ExecutionPayload::StageFatalRecorded(self),
        ))
    }
    fn accepts_payload(payload: &crate::event::ChainPayload) -> bool {
        matches!(
            payload,
            crate::event::ChainPayload::Execution(
                super::execution_payload::ExecutionPayload::StageFatalRecorded(_)
            )
        )
    }

    const EVENT_TYPE: &'static str = "obzenflow.stage_fatal_recorded";
    const SCHEMA_VERSION: u32 = 1;
}
