// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

pub use super::provenance::EventEnvelope;
use serde::{Deserialize, Serialize};

/// Position of one logical event inside the physical atomic journal frame
/// that committed it.
///
/// The zero-based index and total size make a repeated use of one
/// deterministic group identity observable during replay. Without this
/// witness, two adjacent physical frames with the same `journal_group_id`
/// collapse into one indistinguishable list of logical events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalGroupMember {
    pub index: u32,
    pub size: u32,
}
