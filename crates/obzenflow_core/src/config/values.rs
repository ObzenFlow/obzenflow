// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Build-resolved per-run value structs consumed on the data path
//! (FLOWIP-010 §7 carrier: threaded as data, no global read).

/// Built-in default for `runtime.max_lineage_depth`.
pub const DEFAULT_MAX_LINEAGE_DEPTH: usize = 100;

/// Per-run lineage policy (`runtime.max_lineage_depth`), resolved at flow
/// build and carried in stage resources. Deliberately no serde: config must
/// never enter persisted state snapshots (the `TraceState` trap).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineagePolicy {
    /// Max ancestor ids carried per derived event; resolver validates >= 1.
    pub max_lineage_depth: usize,
}

impl Default for LineagePolicy {
    fn default() -> Self {
        Self {
            max_lineage_depth: DEFAULT_MAX_LINEAGE_DEPTH,
        }
    }
}
