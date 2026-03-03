// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join metadata representation for topology export
//!
//! This module defines a simple structural type that tracks, per join stage,
//! which upstream stages are treated as catalog/reference sources vs
//! streaming sources. It is populated during flow construction and exposed
//! via the topology HTTP API.

use obzenflow_core::StageId;
use serde::{Deserialize, Serialize};

/// Internal join metadata keyed by core StageId (join stage id).
///
/// This mirrors the public API shape but uses StageId internally so that
/// both the runtime and web layers can convert to their preferred string
/// or topology id representations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinMetadata {
    /// Stage IDs whose outputs are catalog/reference inputs.
    pub catalog_source_ids: Vec<StageId>,
    /// Stage IDs whose outputs are stream inputs.
    pub stream_source_ids: Vec<StageId>,
}
