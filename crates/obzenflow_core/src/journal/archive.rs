// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Archive metadata shared across layers
//!
//! These are pure schema types (no I/O). Outer layers derive/compute values
//! and inject them into runtime services via stable interfaces.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArchiveStatus {
    Completed,
    Failed,
    Cancelled,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatusDerivation {
    pub terminal_events_found: u64,
    pub chosen: ArchiveStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warning: Option<String>,
}
