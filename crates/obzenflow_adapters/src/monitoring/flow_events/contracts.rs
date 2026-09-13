// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Logical aliases enrich one physical contract occurrence without adding cursors.

use obzenflow_core::{id::CompositeId, StageId};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractBoundaryDirection {
    Inbound,
    Outbound,
}

/// One validated topology boundary, prepared by application assembly.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
pub struct ContractBoundaryAlias {
    pub composite_id: CompositeId,
    pub port: String,
    pub direction: ContractBoundaryDirection,
}

#[derive(Clone, Debug, Default)]
pub struct ContractBoundaryAliases {
    by_edge: Arc<HashMap<(StageId, StageId), Vec<ContractBoundaryAlias>>>,
}

impl ContractBoundaryAliases {
    /// Retain immutable aliases in deterministic order for each physical edge.
    pub fn new(mut by_edge: HashMap<(StageId, StageId), Vec<ContractBoundaryAlias>>) -> Self {
        for aliases in by_edge.values_mut() {
            aliases.sort();
            aliases.dedup();
        }
        Self {
            by_edge: Arc::new(by_edge),
        }
    }
}

pub(super) fn attach_contract_boundary_aliases(
    data: &mut serde_json::Value,
    aliases: &ContractBoundaryAliases,
    upstream: StageId,
    reader: StageId,
) {
    if let Some(aliases) = aliases
        .by_edge
        .get(&(upstream, reader))
        .filter(|v| !v.is_empty())
    {
        data["composite_boundaries"] = serde_json::json!(aliases);
    }
}
