// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Diagnostic copies of runtime positions and state, not recovery authority.

use crate::event::observability::CaptureStamp;
use crate::event::vector_clock::VectorClock;
use crate::{EventId, JournalWriterId, WriterId};
use serde::{Deserialize, Serialize};

/// One runtime owner's captured progress and FSM label. Select this family as
/// a whole; its positions never establish journal coverage or lifecycle state.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeSnapshot {
    /// Separate from the surrounding packet: an output may retain another
    /// owner's measurements while receiving the appending stage's snapshot.
    pub capture: CaptureStamp,
    pub progress: ExecutionProgress,
    pub fsm_state: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionProgress {
    pub reader_seq: u64,
    pub receipted_seq: u64,
    pub writer_seq: u64,
    pub last_consumed_event_id: Option<EventId>,
    pub last_consumed_writer: Option<JournalWriterId>,
    pub last_consumed_vector_clock: Option<VectorClock>,
    pub last_receipted_event_id: Option<EventId>,
    pub last_receipted_vector_clock: Option<VectorClock>,
    pub last_emitted_event_id: Option<EventId>,
    pub last_emitted_writer: Option<WriterId>,
}
