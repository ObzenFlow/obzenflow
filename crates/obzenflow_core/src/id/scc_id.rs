// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! SCC identifier for cycle iteration tracking (FLOWIP-051p)

use serde::{Deserialize, Serialize};
use std::fmt;
use ulid::Ulid;

/// Identifies which strongly connected component (SCC) a stage belongs to.
///
/// Parallel to `obzenflow_topology::SccId` but lives in core so that
/// `ChainEvent` can carry SCC context without depending on the topology crate.
/// Conversion between the two is handled by `SccIdExt` in runtime_services.
///
/// Each SCC's identity is derived from the minimum `StageId` in its member
/// set, making it deterministic for a given topology without requiring
/// sequential index allocation. This is consistent with every other
/// identifier in the system, which uses ULID-based IDs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SccId(Ulid);

impl SccId {
    /// Create from a specific ULID.
    pub fn from_ulid(ulid: Ulid) -> Self {
        Self(ulid)
    }

    /// Get the underlying ULID.
    pub fn as_ulid(&self) -> Ulid {
        self.0
    }
}

impl fmt::Display for SccId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "scc_{}", self.0)
    }
}

impl From<Ulid> for SccId {
    fn from(ulid: Ulid) -> Self {
        Self(ulid)
    }
}

impl From<SccId> for Ulid {
    fn from(scc_id: SccId) -> Self {
        scc_id.0
    }
}
