// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// The row name of an admitted member.
pub const MEMBER_RECORDED_EVENT_TYPE: &str = "runtime.ingress.member_recorded";

/// One member that a sender pushed and the source admitted, as the source recorded
/// it before the fact that it produces. Read numbers start at one.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberRecorded {
    pub recorded_flow_id: String,
    pub stage_key: String,
    pub number: u64,
    pub member: Value,
}
