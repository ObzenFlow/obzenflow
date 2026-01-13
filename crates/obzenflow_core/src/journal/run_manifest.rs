//! Run manifest schema for on-disk archives (FLOWIP-095a).
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
    pub data_journal_file: String,
    pub error_journal_file: String,
}
