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
pub const RUN_MANIFEST_VERSION: &str = "1.0";

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
    /// changed stage. It is a real, handler-supplied field, not a runtime default;
    /// the `serde(default)` below applies only when loading an archive written before
    /// the field existed. Most stages report `"1"` today because the descriptor and
    /// handler traits default to `"1"` unless a handler overrides the method.
    #[serde(default = "default_stage_logic_version")]
    pub stage_logic_version: String,
    pub data_journal_file: String,
    pub error_journal_file: String,
}

/// Legacy-archive deserialization fallback for `stage_logic_version` (FLOWIP-120a).
/// This is not the runtime source of the value; live runs populate the field from
/// the stage descriptor at flow build. It only fills the field for archives written
/// before `stage_logic_version` was added to the manifest.
fn default_stage_logic_version() -> String {
    "1".to_string()
}
