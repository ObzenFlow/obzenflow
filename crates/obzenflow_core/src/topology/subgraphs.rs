// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::StageId;
use serde::{Deserialize, Serialize};

/// Stage-level membership facts for logical subgraphs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageSubgraphMembership {
    pub subgraph_id: String,
    pub kind: String,
    pub binding: String,
    pub role: String,
    pub order: u16,
    pub is_entry: bool,
    pub is_exit: bool,
}

/// Graph-level registry entry describing one logical subgraph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopologySubgraphInfo {
    pub subgraph_id: String,
    pub kind: String,
    pub binding: String,
    pub label: String,
    pub member_stage_ids: Vec<StageId>,
    pub internal_edges: Vec<SubgraphInternalEdge>,
    pub entry_stage_ids: Vec<StageId>,
    pub exit_stage_ids: Vec<StageId>,
    pub parent_subgraph_id: Option<String>,
    pub collapsible: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubgraphInternalEdge {
    pub from_stage_id: StageId,
    pub to_stage_id: StageId,
    pub role: String,
}

