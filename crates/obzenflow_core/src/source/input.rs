// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::num::NonZeroU64;

/// `EventReceived` is one event that a source took from the system behind its event
/// port. The source records the event before its fact.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventReceived {
    pub recorded_flow_id: String,
    pub stage_key: String,
    pub number: NonZeroU64,
    pub event: Value,
}
