// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Protected execution accounting. These counts survive observation omission.

use crate::event::status::processing_status::ErrorKind;
use crate::{EventType, StageId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeProvenance {
    pub accounting: ExecutionAccounting,
}

/// Counts retain their existing physical input/output populations. In particular,
/// composite protocol and framework effect rows are not erased by the `fact` kind.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionAccounting {
    pub events_processed_total: u64,
    pub events_accumulated_total: u64,
    pub events_emitted_total: u64,
    pub terminal_groups_committed_total: u64,
    pub terminal_group_commit_failures_total: u64,
    pub errors_total: u64,
    pub failures_total: u64,
    pub errors_by_kind: HashMap<ErrorKind, u64>,
    pub data_outputs_by_event_type: Vec<EventTypeCountContext>,
    pub data_inputs_by_upstream_event_type: Vec<UpstreamEventTypeCountContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTypeCountContext {
    pub event_type: EventType,
    pub total: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpstreamEventTypeCountContext {
    pub upstream: StageId,
    pub event_type: EventType,
    pub total: u64,
}
