// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Lineage-scoped identity comparison (FLOWIP-095j).
//!
//! Effect-lane rows carry deterministic ids minted in the recorded run's
//! namespace (`cursor.recorded_flow_id`, FLOWIP-120b): a live run mints in
//! its own flow id's namespace, and every replay generation inherits
//! generation zero's. Identity equality is therefore meaningful exactly when
//! the two runs share a replay lineage.
//!
//! The candidate manifest's `replay` block is the fast first check that a
//! lineage is asserted. The authoritative proof is namespace equality read
//! from the journals themselves, which is what makes verification robust to
//! deleted intermediate generations: replay-of-replay verifies against the
//! generation-zero baseline because both mint in generation zero's namespace,
//! with no need to walk manifests through directories that may no longer
//! exist. A namespace mismatch under `Lineage` mode is a refusal
//! (`RefusalReason::LineageMismatch`), never a divergence: it means the runs
//! are unrelated, not that replay diverged.
//!
//! Two independent live runs (no replay block on the candidate) compare under
//! the positional projection only. A flow with no effect-lane rows offers no
//! namespace evidence, and identity comparison over zero deterministic-id
//! rows is vacuous, so positional mode loses nothing there.

use obzenflow_core::journal::run_manifest::RunManifest;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdentityMode {
    /// Effect-lane identity (event id, deterministic event time, provenance)
    /// is part of equality; a namespace mismatch refuses the comparison.
    Lineage,
    /// Positional projection only; run-minted identity is excluded.
    Positional,
}

impl IdentityMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Lineage => "lineage",
            Self::Positional => "positional",
        }
    }
}

/// A lineage is asserted when the candidate records that it replayed an
/// archive. The per-row namespace proof in the walker is what validates it.
pub fn identity_mode(candidate: &RunManifest) -> IdentityMode {
    if candidate.replay.is_some() {
        IdentityMode::Lineage
    } else {
        IdentityMode::Positional
    }
}
