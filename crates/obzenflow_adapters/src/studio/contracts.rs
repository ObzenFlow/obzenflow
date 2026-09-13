// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stores the composite ports for connections between stages. Studio uses these
//! labels to show delivery check results on the group's input and output ports.

use obzenflow_core::{id::CompositeId, StageId};
use std::{collections::HashMap, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ContractBoundaryDirection {
    Inbound,
    Outbound,
}

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
    pub fn new(mut by_edge: HashMap<(StageId, StageId), Vec<ContractBoundaryAlias>>) -> Self {
        for aliases in by_edge.values_mut() {
            aliases.sort();
            aliases.dedup();
        }
        Self {
            by_edge: Arc::new(by_edge),
        }
    }

    pub(super) fn for_edge(&self, upstream: StageId, reader: StageId) -> &[ContractBoundaryAlias] {
        self.by_edge
            .get(&(upstream, reader))
            .map(Vec::as_slice)
            .unwrap_or_default()
    }
}
