// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Conversion helpers between core StageId and topology StageId
//!
//! ARCHITECTURE: The ID Bridge Pattern
//! ====================================
//! This module bridges between two different ID systems:
//!
//! 1. obzenflow_core::StageId - Uses standard ulid crate (real ULIDs with crypto randomness)
//! 2. obzenflow_topology::StageId - Uses obzenflow-idkit (type-only, no generation)
//!
//! Why this bridge exists:
//! - Backend (flowstate_rs) generates real ULIDs for production use
//! - Frontend (obzen-flow-ui) uses sequential IDs due to WASM limitations  
//! - Topology acts as a type-safe bridge between both worlds
//!
//! The conversion works because both ID types share the same underlying ULID
//! representation, they just use different crates to handle them.

use obzenflow_core::StageId;

/// Extension trait for converting between core StageId and topology StageId
pub trait StageIdExt {
    fn to_topology_id(&self) -> obzenflow_topology::StageId;
    fn from_topology_id(topology_id: obzenflow_topology::StageId) -> Self;
}

impl StageIdExt for StageId {
    fn to_topology_id(&self) -> obzenflow_topology::StageId {
        // Convert from core's ulid::Ulid to topology's obzenflow-idkit Id
        // Both use the same underlying ULID representation
        obzenflow_topology::StageId::from_ulid(self.as_ulid())
    }

    fn from_topology_id(topology_id: obzenflow_topology::StageId) -> Self {
        // Convert from topology's obzenflow-idkit Id to core's ulid::Ulid
        StageId::from_ulid(topology_id.ulid())
    }
}
