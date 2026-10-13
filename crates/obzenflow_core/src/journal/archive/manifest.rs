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
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

pub const RUN_MANIFEST_FILENAME: &str = "run_manifest.json";
/// One version for the journal records, physical frames, and run manifest.
/// Breaking any of these contracts requires a bump here and fresh archives.
/// Framework package versions are provenance only, never archive admission gates.
pub const JOURNAL_SCHEMA_VERSION: &str = "20.0";
pub const EFFECT_ATTEMPT_HISTORY_CAPABILITY: &str = "effect_attempt_history";
pub const BOUNDED_DIRECT_FACT_ADMISSION_CAPABILITY: &str = "bounded_direct_fact_admission";
/// Every persisted effect descriptor carries an explicit portless/named binding identity.
pub const EFFECT_BINDING_DESCRIPTOR_CAPABILITY: &str = "effect_binding_descriptor";
/// Family-stamped optional attachments, independent of protected accounting.
pub const OBSERVABILITY_CAPTURE_CAPABILITY: &str = "observability_capture";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub journal_schema_version: String,
    pub obzenflow_version: String,
    pub flow_id: String,
    /// Authoritative pipeline lifecycle writer allocated for this output run.
    pub pipeline_writer_id: crate::event::WriterId,
    pub flow_name: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay: Option<RunManifestReplayConfig>,
    /// Present when this run is a resume of a recorded archive (FLOWIP-120n).
    /// Omission is the current schema's representation of a non-resume run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resume: Option<RunManifestResumeConfig>,
    pub stages: HashMap<String, RunManifestStage>,
    pub system_journal_file: String,
    pub metrics_journals: Option<RunManifestMetrics>,
    /// FLOWIP-010 §6a: redacted effective config with provenance, recorded
    /// at flow build. Omission is the current schema's representation of no
    /// effective-config evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_config: Option<crate::config::EffectiveConfigEvidence>,
    /// Versioned runtime contracts required to interpret this archive.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub capabilities: BTreeMap<String, u32>,
    /// Descriptor-proved live direct-fact bounds, sorted by stage key and
    /// exact versioned physical input event type.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bounded_direct_fact_admission: Vec<RunManifestDirectFactAdmission>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifestMetrics {
    pub writer_id: crate::event::WriterId,
    pub coordination_journal_file: String,
    pub export_journal_file: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RunManifestDirectFactAdmission {
    pub stage_key: String,
    pub input_event_type: String,
    pub max_live_data_rows: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifestReplayConfig {
    /// The archive this run read, canonicalized (FLOWIP-134i B16).
    pub replay_from: String,
    /// Whether replay proceeded despite missing/corrupt terminal evidence.
    pub allow_incomplete_archive: bool,
}

/// Present when this run is a resume of a recorded archive (FLOWIP-120n).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifestResumeConfig {
    /// The archive this run resumed from, canonicalized.
    pub resumed_from: PathBuf,
    /// The generation this run enters (max recorded generation + 1).
    pub resume_generation: u64,
    /// Ancestors merged because `resumed_from` had not recorded reconstruction
    /// completion, nearest first (FLOWIP-134i B6).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merged_from: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifestStage {
    pub dsl_var: String,
    pub stage_type: StageType,
    /// Required authoring capability from `StageDescriptor::is_effectful()`,
    /// distinct from the runtime stage family.
    pub is_effectful: bool,
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
    /// edges, sorted and deduplicated. Together with `delivery_order` this makes
    /// the archive self-describing for order-certification at verify time.
    pub inbound: Vec<String>,
    /// How this stage's input order is fixed, written once at flow build.
    pub delivery_order: DeliveryOrder,
}

/// How a stage's input delivery order is fixed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryOrder {
    /// Several inputs with no order guarantee, such as cycle members.
    Unordered,
    /// Zero or one inbound edge, so the stage inherits its upstream's order.
    SingleInput,
    /// The stage's structure fixes the order, as in a hydrating join with one
    /// stream reader.
    Structural,
    /// An ordered fan-in whose roots are all finite; the canonical merge gives
    /// the same order in every run.
    Canonical,
    /// An ordered fan-in with a live root; its order is chosen at run time and
    /// written as decision rows.
    Recorded,
}

impl DeliveryOrder {
    /// Whether independent runs over the same inputs deliver in the same order.
    pub const fn agrees_across_runs(self) -> bool {
        matches!(self, Self::SingleInput | Self::Structural | Self::Canonical)
    }
}
