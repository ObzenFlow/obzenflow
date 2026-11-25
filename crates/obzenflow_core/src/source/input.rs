// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// `MEMBER_RECORDED_EVENT_TYPE` is the event type of an admitted member.
pub const MEMBER_RECORDED_EVENT_TYPE: &str = "runtime.ingress.member_recorded";

/// `MemberRecorded` is one member that a sender pushed and the source admitted. The
/// source records the member before the fact that it produces.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberRecorded {
    pub recorded_flow_id: String,
    pub stage_key: String,
    pub number: u64,
    pub member: Value,
}
