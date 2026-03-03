// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Replay provenance context

use crate::event::types::EventId;
use crate::id::StageId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayContext {
    pub original_event_id: EventId,
    pub original_flow_id: String,
    pub original_stage_id: StageId,
    pub archive_path: PathBuf,
    pub replayed_at: DateTime<Utc>,
}
