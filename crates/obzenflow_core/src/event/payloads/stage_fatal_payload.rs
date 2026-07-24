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
    const EVENT_TYPE: &'static str = "obzenflow.stage_fatal_recorded";
    const SCHEMA_VERSION: u32 = 1;
}
