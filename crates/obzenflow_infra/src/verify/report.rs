// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The machine-readable verification report (FLOWIP-095j), a versioned
//! contract from day one. Field deltas are truncated with full-value SHA-256
//! digests and journal coordinates, so the journals stay the source of truth
//! and the report stays a pointer.

use std::collections::BTreeMap;
use std::path::PathBuf;

use ring::digest::{digest, SHA256};
use serde::Serialize;

pub const REPORT_VERSION: u32 = 2;

/// Per-side truncation budget for divergence deltas, in bytes.
pub const DELTA_TRUNCATION_BYTES: usize = 1024;

#[derive(Debug, Clone, Serialize)]
pub struct VerificationReport {
    pub report_version: u32,
    pub verdict: String,
    pub exit_code: u8,
    /// `whole_run` or `baseline_prefix_of_candidate`.
    pub mode: String,
    /// `lineage` or `positional`.
    pub identity_mode: String,
    pub baseline: RunSummary,
    pub candidate: RunSummary,
    pub totals: Totals,
    pub stages: BTreeMap<String, StageReport>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunSummary {
    pub run_dir: PathBuf,
    pub flow_id: String,
    pub flow_name: String,
    pub status: String,
    pub manifest_version: String,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct Totals {
    pub stages: u64,
    pub matched: u64,
    pub vacuous: u64,
    pub diverged: u64,
    pub not_order_certified: u64,
    pub divergences: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StageReport {
    /// `matched`, `diverged`, or `not_order_certified`.
    pub status: String,
    /// True when the stage sits outside the order-certified region but its
    /// projection contains no positionally compared rows, so it is vacuously
    /// certified (FLOWIP-095j section 2).
    pub vacuous: bool,
    /// True when the stage carries a certificate, either positional (delivery
    /// order pinned) or by content (order proven irrelevant). False only for a
    /// not-certifiable stage (cycle or regime mismatch).
    pub order_certified: bool,
    /// Which certificate was applied (FLOWIP-095l): `"positional"` (compared row
    /// by row), `"content"` (compared as an order-insensitive multiset), or
    /// `"none"` (not certifiable; per-type count advisory only).
    pub certification: String,
    /// Ancestors (possibly including the stage itself) whose delivery order
    /// is unconstrained; empty when certified.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub blocking: Vec<String>,
    /// True when `stage_logic_version` differs between the runs; divergences
    /// in such a stage are classed `changed-code` (advisory).
    pub logic_version_changed: bool,
    pub positional_rows_baseline: u64,
    pub positional_rows_candidate: u64,
    pub divergence_count: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub divergences: Vec<DivergenceReport>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub informational: Vec<String>,
    /// Per-event-type positional row counts, reported for uncertified stages
    /// as an order-insensitive advisory (never certifying).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub counts_by_type: Option<CountsByType>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CountsByType {
    pub baseline: BTreeMap<String, u64>,
    pub candidate: BTreeMap<String, u64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DivergenceReport {
    /// Which journal the divergence sits in (`data` or `error`).
    pub journal: String,
    /// Position within the projected sequence of that journal.
    pub position: u64,
    /// First differing field at that position (`kind`, `payload`, `status`,
    /// `effect_identity`, `missing_row`, `eof_evidence`).
    pub field: String,
    pub baseline: DeltaSide,
    pub candidate: DeltaSide,
    /// `changed-code` when the stage's logic version differs between runs,
    /// `unexpected` otherwise. Advisory; never changes exit codes.
    pub classification: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct DeltaSide {
    pub truncated: String,
    pub sha256: String,
    pub bytes: usize,
}

/// Build one side of a divergence delta: truncated value plus full-value
/// digest, so large payloads never bloat the report and the journals remain
/// the place to look.
pub fn delta_side(raw: &str) -> DeltaSide {
    let bytes = raw.as_bytes();
    let hash = digest(&SHA256, bytes);
    let sha256 = hash
        .as_ref()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let truncated = if bytes.len() <= DELTA_TRUNCATION_BYTES {
        raw.to_string()
    } else {
        let mut end = DELTA_TRUNCATION_BYTES;
        while !raw.is_char_boundary(end) {
            end -= 1;
        }
        format!(
            "{}… [truncated {} of {} bytes]",
            &raw[..end],
            end,
            bytes.len()
        )
    };
    DeltaSide {
        truncated,
        sha256,
        bytes: bytes.len(),
    }
}

/// A side for a row that exists in one run only.
pub fn absent_side() -> DeltaSide {
    DeltaSide {
        truncated: "<absent>".to_string(),
        sha256: String::new(),
        bytes: 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn small_values_are_carried_whole_with_a_digest() {
        let side = delta_side("{\"id\":7}");
        assert_eq!(side.truncated, "{\"id\":7}");
        assert_eq!(side.bytes, 8);
        assert_eq!(side.sha256.len(), 64);
    }

    #[test]
    fn large_values_truncate_at_the_budget_and_keep_the_full_digest() {
        let raw = "x".repeat(DELTA_TRUNCATION_BYTES * 3);
        let side = delta_side(&raw);
        assert!(side.truncated.len() < raw.len());
        assert!(side.truncated.contains("truncated"));
        assert_eq!(side.bytes, raw.len());
        assert_eq!(side.sha256, delta_side(&raw).sha256, "digest is stable");
    }

    #[test]
    fn truncation_respects_utf8_boundaries() {
        let raw = "é".repeat(DELTA_TRUNCATION_BYTES);
        let side = delta_side(&raw);
        assert!(side.truncated.contains("truncated"));
    }
}
