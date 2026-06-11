// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Run manifest schema for on-disk archives
//!
//! This is a pure schema module (no I/O). Infra is responsible for reading and
//! writing `run_manifest.json` in the run directory.

use crate::event::context::StageType;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub const RUN_MANIFEST_FILENAME: &str = "run_manifest.json";
pub const RUN_MANIFEST_VERSION: &str = "2.0";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub manifest_version: String,
    pub obzenflow_version: String,
    pub flow_id: String,
    pub flow_name: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay: Option<RunManifestReplayConfig>,
    pub stages: HashMap<String, RunManifestStage>,
    pub system_journal_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifestReplayConfig {
    /// User-supplied archive path from `--replay-from` (as provided; not normalized).
    pub replay_from: String,
    /// Whether replay proceeded despite missing/corrupt terminal evidence.
    pub allow_incomplete_archive: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifestStage {
    pub dsl_var: String,
    pub stage_type: StageType,
    pub stage_id: String,
    /// FLOWIP-120a: the stage logic version, sourced at flow build from
    /// `StageDescriptor::stage_logic_version()` and folded into the effect
    /// descriptor hash so a deliberate bump invalidates effect replay matches for a
    /// changed stage. It is a real, handler-supplied field, not a runtime default.
    /// Most stages report `"1"` today because the descriptor and handler traits
    /// default to `"1"` unless a handler overrides the method.
    pub stage_logic_version: String,
    pub data_journal_file: String,
    pub error_journal_file: String,
    /// FLOWIP-095j: upstream stage keys delivering into this stage over forward
    /// edges, sorted and deduplicated. Together with `ordered_delivery` this makes
    /// the archive self-describing for order-certification at verify time.
    pub inbound: Vec<String>,
    /// FLOWIP-095j: whether this stage's input delivery order is deterministic
    /// (zero or one inbound edge outside a cycle, an FLOWIP-095d marked fan-in,
    /// or a structural orderer such as the hydrating join). Cycle members are
    /// always false; backflow arrivals interleave by timing.
    pub ordered_delivery: bool,
}
