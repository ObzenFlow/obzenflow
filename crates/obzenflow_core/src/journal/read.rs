// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Read-only run evidence. Positions count committed logical records within one
//! journal; they neither order different journals nor acknowledge runtime delivery.

use crate::event::context::StageType;
use crate::event::journal_record::{ChainJournalRecord, SystemJournalRecord};
use crate::event::WriterId;
use crate::id::{FlowId, JournalId, StageId};
use crate::EventId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Version of the consumer record projection, independent of the archive epoch.
pub const RUN_RECORD_VERSION: u16 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunIdentity {
    pub flow_id: FlowId,
    pub pipeline_writer_id: WriterId,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct JournalPosition(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunJournalKind {
    System,
    Data,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStage {
    pub key: String,
    pub id: StageId,
    pub stage_type: StageType,
    /// Recorded ability to use replay-suppressed effects, required by the
    /// current journal schema.
    pub is_effectful: bool,
}

/// Stable identity derived by the provider from the admitted run and journal.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunJournal {
    pub id: JournalId,
    pub kind: RunJournalKind,
    pub stage: Option<RunStage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunRecordKind {
    SourceFact,
    StageOutput,
    CompositeData,
    Effect,
    Delivery,
    Lifecycle,
    FlowSignal,
    Execution,
    System,
}

/// Keeps the canonical typed envelope and payload, including recorded provenance.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunRecordData {
    Chain(Box<ChainJournalRecord>),
    System(Box<SystemJournalRecord>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRecord {
    pub version: u16,
    pub run: RunIdentity,
    pub journal: RunJournal,
    /// Zero-based append ordinal of this record, scoped to run and journal.
    pub position: JournalPosition,
    pub kind: RunRecordKind,
    pub record: RunRecordData,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunOutcome {
    Completed,
    Failed { reason: String },
    Cancelled { reason: String },
    NotStarted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedRunOutcome {
    pub event_id: EventId,
    pub outcome: RunOutcome,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SettledRunPrefix {
    pub terminal_event_id: EventId,
    pub drained_event_id: EventId,
    /// Exclusive logical ends. Later host facts do not extend this boundary.
    pub end_positions: BTreeMap<JournalId, JournalPosition>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunReadProgress {
    pub outcome: Option<RecordedRunOutcome>,
    pub drained_event_id: Option<EventId>,
    pub settled_prefix: Option<SettledRunPrefix>,
}

#[derive(Debug, Clone)]
pub enum TailRead {
    Record(RunRecord),
    /// No committed record available now; never a completion assertion.
    Pending,
}
