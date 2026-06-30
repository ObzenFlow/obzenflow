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
pub const RUN_MANIFEST_VERSION: &str = "3.0";

/// On-disk journal record format version (FLOWIP-120q). Bumped only when the
/// framed record byte format changes. Readers gate on this in the same raw-JSON
/// check that gates `manifest_version`, so an archive written by an incompatible
/// format is refused before any record is parsed. There is no mixed-format file:
/// append and resume across a changed format refuse or start a new segment.
pub const JOURNAL_FORMAT_VERSION: u32 = 1;

/// FLOWIP-095l. The build's input-ordering verdict for a stage, replacing the
/// FLOWIP-095j `ordered_delivery` boolean. Self-describing so the verifier can
/// distinguish a non-deterministic cycle member from a legitimately
/// order-insensitive fan-in, and compare two runs only under a matching regime.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OrderDecision {
    /// The canonical merge runs, or the stage is single-input or a structural
    /// orderer: delivery order is deterministic and certifiable.
    Ordered,
    /// No descendant observes the order, so it is free: two correct runs may
    /// interleave differently while reconstructing equal state and content.
    OrderInsensitive,
    /// A cycle member: backflow interleaves by timing, so order is
    /// non-deterministic and the region is not certifiable.
    CycleNonDeterministic,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunManifest {
    pub manifest_version: String,
    /// FLOWIP-120q: framed record format version. See `JOURNAL_FORMAT_VERSION`.
    pub journal_format_version: u32,
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
    /// edges, sorted and deduplicated. Together with `order_decision` this makes
    /// the archive self-describing for order-certification at verify time.
    pub inbound: Vec<String>,
    /// FLOWIP-095l: the build's input-ordering verdict for this stage, replacing
    /// the 095j `ordered_delivery` boolean. `Ordered` when single-input, a 095d
    /// marked fan-in, or a structural orderer; `CycleNonDeterministic` for a cycle
    /// member; `OrderInsensitive` for a fan-in with no order-observing descendant.
    pub order_decision: OrderDecision,
}
