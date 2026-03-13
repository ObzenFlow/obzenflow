// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Composite stage descriptors lowered during `flow!` materialisation.
//!
//! Composites are an authoring convenience: they expand into ordinary stages and
//! edges before topology validation and runtime build so journalling, replay, and
//! middleware semantics remain stage-visible.

pub mod ai_map_reduce;

use crate::dsl::stage_descriptor::StageDescriptor;
use crate::dsl::FlowBuildError;
use obzenflow_core::topology::subgraphs::StageSubgraphMembership;
use obzenflow_topology::EdgeKind;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct SubgraphInternalEdgeSpec {
    pub from_stage: String,
    pub to_stage: String,
    pub role: String,
}

#[derive(Debug, Clone)]
pub struct TopologySubgraphSpec {
    pub subgraph_id: String,
    pub kind: String,
    pub binding: String,
    pub label: String,
    pub member_stage_names: Vec<String>,
    pub internal_edges: Vec<SubgraphInternalEdgeSpec>,
    pub entry_stage_names: Vec<String>,
    pub exit_stage_names: Vec<String>,
    pub parent_subgraph_id: Option<String>,
    pub collapsible: bool,
}

#[derive(Debug, Default, Clone)]
pub struct LoweringArtifacts {
    pub stage_subgraphs: HashMap<String, StageSubgraphMembership>,
    pub subgraphs: Vec<TopologySubgraphSpec>,
}

pub struct CompositeLowering {
    pub stages: Vec<(String, Box<dyn StageDescriptor>)>,
    pub edges: Vec<(String, String, EdgeKind)>,
    pub entry_stage: String,
    pub exit_stage: String,
    pub artifacts: LoweringArtifacts,
}

pub fn lower_composites(
    stages: &mut HashMap<String, Box<dyn StageDescriptor>>,
    connections: &mut Vec<(String, String, EdgeKind)>,
) -> Result<LoweringArtifacts, FlowBuildError> {
    let composite_bindings: Vec<String> = stages
        .iter()
        .filter(|(_, descriptor)| descriptor.is_composite())
        .map(|(binding, _)| binding.clone())
        .collect();

    if composite_bindings.is_empty() {
        return Ok(LoweringArtifacts::default());
    }

    let mut artifacts = LoweringArtifacts::default();

    for binding in composite_bindings {
        let descriptor = stages.remove(&binding).ok_or_else(|| {
            FlowBuildError::StageResourcesFailed(format!(
                "Missing composite stage '{binding}' during lowering"
            ))
        })?;

        let lowering = descriptor.try_lower_composite(&binding)?.ok_or_else(|| {
            FlowBuildError::StageResourcesFailed(format!(
                "Stage '{binding}' reported is_composite() but did not produce a lowering result"
            ))
        })?;

        // Collision detection: internal stage bindings must not collide with any existing stage
        // binding names (including other composites not yet lowered).
        for (internal_name, _) in &lowering.stages {
            if stages.contains_key(internal_name) {
                return Err(FlowBuildError::StageResourcesFailed(format!(
                    "ai_map_reduce: lowering collision for composite '{binding}': internal stage '{internal_name}' already exists"
                )));
            }
        }

        // Insert lowered stages first so subsequent composites detect collisions.
        for (name, stage) in lowering.stages {
            stages.insert(name, stage);
        }

        // Append internal edges.
        connections.extend(lowering.edges);

        // Rewrite external edges that referenced the logical composite binding.
        for (from, to, _) in connections.iter_mut() {
            if from == &binding {
                *from = lowering.exit_stage.clone();
            }
            if to == &binding {
                *to = lowering.entry_stage.clone();
            }
        }

        // Merge lowering artifacts.
        for (stage_name, membership) in lowering.artifacts.stage_subgraphs {
            if artifacts.stage_subgraphs.contains_key(&stage_name) {
                return Err(FlowBuildError::StageResourcesFailed(format!(
                    "ai_map_reduce: duplicate stage subgraph membership entry for '{stage_name}'"
                )));
            }
            artifacts.stage_subgraphs.insert(stage_name, membership);
        }
        artifacts.subgraphs.extend(lowering.artifacts.subgraphs);
    }

    // Best-effort sanity check: subgraph IDs should be unique.
    let mut seen = HashSet::new();
    for subgraph in &artifacts.subgraphs {
        if !seen.insert(subgraph.subgraph_id.clone()) {
            return Err(FlowBuildError::StageResourcesFailed(format!(
                "ai_map_reduce: duplicate subgraph_id '{}' in lowering artifacts",
                subgraph.subgraph_id
            )));
        }
    }

    Ok(artifacts)
}
