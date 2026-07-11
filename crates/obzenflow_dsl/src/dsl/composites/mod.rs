// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Composite lowering during `flow!` materialisation (FLOWIP-128a).
//!
//! `lower_composites` is the phase transition: it consumes `FlowMember`s and
//! returns pure stage descriptors, so post-lowering code cannot name a
//! composite. Expansion content is validated in `composition::finish`;
//! this module validates cross-composite collisions and rewrites external
//! edges through boundary ports (D1): edges into a composite bind its input
//! port, edges out resolve by the downstream's declared input type with
//! default-port fallback, and ambiguity fails the build.

pub mod ai_map_reduce;

use crate::dsl::composition::{
    CompositeBuildContext, CompositeExpansion, FlowMember, PortResolveError, ResolvedBoundary,
    ResolvedInternalFeed, ResolvedPort,
};
use crate::dsl::stage_descriptor::StageDescriptor;
use crate::dsl::typing::TypeHint;
use crate::dsl::FlowBuildError;
use obzenflow_topology::{CompositePortRef, EdgeKind, StageSubgraphMembership};
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct SubgraphInternalEdgeSpec {
    pub from_stage: String,
    pub to_stage: String,
    /// Lane label (D3); serialized to the internal edge's `role` field.
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
    pub schema_version: u32,
    pub boundary_ports: Vec<ResolvedPort>,
}

/// Port annotations retained when a composite binding is rewritten to its
/// physical member edge. This is the hand-off from type-directed DSL
/// resolution to the durable topology manifest (FLOWIP-128a B3).
#[derive(Debug, Clone)]
pub struct BoundaryEdgeBindingSpec {
    pub from_stage: String,
    pub to_stage: String,
    pub kind: EdgeKind,
    pub ports: Vec<CompositePortRef>,
}

#[derive(Default)]
pub struct LoweringArtifacts {
    pub stage_subgraphs: HashMap<String, StageSubgraphMembership>,
    pub subgraphs: Vec<TopologySubgraphSpec>,
    /// Declared internal feeds, member-stage resolved (D3); consumed by
    /// feed-plan derivation and edge validation.
    pub internal_feeds: Vec<ResolvedInternalFeed>,
    /// Composite boundaries keyed by binding, for join-reference resolution
    /// (A4) and boundary diagnostics.
    pub boundaries: HashMap<String, ResolvedBoundary>,
    /// Named port identities on rewritten physical boundary edges (B3).
    pub boundary_edges: Vec<BoundaryEdgeBindingSpec>,
}

fn build_error(message: String) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(message)
}

/// The downstream's declared input hint for port resolution: a transform's
/// input type, or a join's stream type. Reference edges resolve separately
/// in the join pass (A4).
fn downstream_input_hint(
    stages: &HashMap<String, Box<dyn StageDescriptor>>,
    downstream: &str,
) -> Option<TypeHint> {
    let metadata = stages.get(downstream)?.typing_metadata()?;
    if matches!(metadata.input_type, TypeHint::Exact { .. }) {
        return Some(metadata.input_type.clone());
    }
    if matches!(metadata.stream_type, TypeHint::Exact { .. }) {
        return Some(metadata.stream_type.clone());
    }
    None
}

#[allow(clippy::result_large_err)]
#[allow(clippy::type_complexity)]
pub fn lower_composites(
    members: HashMap<String, FlowMember>,
    connections: &mut Vec<(String, String, EdgeKind)>,
) -> Result<(HashMap<String, Box<dyn StageDescriptor>>, LoweringArtifacts), FlowBuildError> {
    let mut stages: HashMap<String, Box<dyn StageDescriptor>> = HashMap::new();
    let mut composites: Vec<(
        String,
        Box<dyn crate::dsl::composition::CompositeDescriptor>,
    )> = Vec::new();

    for (binding, member) in members {
        match member {
            FlowMember::Stage(descriptor) => {
                stages.insert(binding, descriptor);
            }
            FlowMember::Composite(descriptor) => composites.push((binding, descriptor)),
        }
    }

    if composites.is_empty() {
        return Ok((stages, LoweringArtifacts::default()));
    }

    // Deterministic lowering order (A2): artifacts and edge appends must not
    // depend on map iteration.
    composites.sort_by(|left, right| left.0.cmp(&right.0));

    // Expand everything first so composite-to-composite edges can resolve
    // against both boundaries.
    let mut expansions: Vec<(String, CompositeExpansion)> = Vec::new();
    for (binding, descriptor) in composites {
        let kind = descriptor.kind();
        let schema_version = descriptor.schema_version();
        let mut ctx = CompositeBuildContext::new(kind);
        descriptor
            .expand(&mut ctx)
            .map_err(|err| build_error(err.to_string()))?;
        let expansion = ctx
            .finish(&binding, schema_version)
            .map_err(|err| build_error(err.to_string()))?;
        expansions.push((binding, expansion));
    }

    // Cross-collision check: member names against outer stages and against
    // other expansions (intra-expansion duplicates are duplicate roles,
    // already rejected by finish).
    let mut reserved: HashSet<String> = stages.keys().cloned().collect();
    for (binding, expansion) in &expansions {
        if reserved.contains(binding.as_str()) {
            return Err(build_error(format!(
                "composite '{binding}': lowering collision: a stage already uses the binding name"
            )));
        }
        for member in &expansion.members {
            if !reserved.insert(member.stage_name.clone()) {
                return Err(build_error(format!(
                    "composite '{binding}': lowering collision: member stage '{}' already exists",
                    member.stage_name
                )));
            }
        }
    }

    let mut artifacts = LoweringArtifacts::default();

    // Capture manifest data, then merge member stages and internal edges.
    for (binding, mut expansion) in expansions {
        let subgraph_id = format!("{}:{binding}", expansion.kind);
        expansion.boundary.subgraph_id = subgraph_id.clone();

        let mut member_stage_names = Vec::with_capacity(expansion.members.len());
        let mut entry_stage_names = Vec::new();
        let mut exit_stage_names = Vec::new();
        for member in &expansion.members {
            member_stage_names.push(member.stage_name.clone());
            if member.is_entry {
                entry_stage_names.push(member.stage_name.clone());
            }
            if member.is_exit {
                exit_stage_names.push(member.stage_name.clone());
            }

            let mut membership = StageSubgraphMembership::new(
                subgraph_id.clone(),
                expansion.kind,
                binding.clone(),
                member.role.clone(),
                member.order,
                member.is_entry,
                member.is_exit,
            );
            if let Some(class) = &member.class {
                membership = membership.with_class(class.clone());
            }
            artifacts
                .stage_subgraphs
                .insert(member.stage_name.clone(), membership);
        }

        let internal_edges = expansion
            .internal_edges
            .iter()
            .map(|(from, to, lane)| SubgraphInternalEdgeSpec {
                from_stage: from.clone(),
                to_stage: to.clone(),
                role: lane.clone(),
            })
            .collect();

        artifacts.subgraphs.push(TopologySubgraphSpec {
            subgraph_id: subgraph_id.clone(),
            kind: expansion.kind.to_string(),
            binding: binding.clone(),
            label: binding.clone(),
            member_stage_names,
            internal_edges,
            entry_stage_names,
            exit_stage_names,
            parent_subgraph_id: None,
            collapsible: true,
            schema_version: expansion.schema_version,
            boundary_ports: expansion
                .boundary
                .outputs
                .iter()
                .chain(expansion.boundary.inputs.iter())
                .cloned()
                .collect(),
        });

        artifacts.internal_feeds.extend(expansion.feeds);
        artifacts
            .boundaries
            .insert(binding.clone(), expansion.boundary);

        for (from, to, _lane) in &expansion.internal_edges {
            connections.push((from.clone(), to.clone(), EdgeKind::Forward));
        }
        for member in expansion.members {
            stages.insert(member.stage_name, member.descriptor);
        }
    }

    // Port-directed external edge rewrite (D1). Inputs bind the single input
    // port; outputs resolve by the downstream's declared input type with
    // default fallback; ambiguity fails loud. Backward edges use the same
    // rule. Targets are rewritten first so composite-to-composite edges see
    // the resolved member when the source side looks up its hint.
    for (from, to, kind) in connections.iter_mut() {
        let mut ports = Vec::new();
        if let Some(boundary) = artifacts.boundaries.get(to.as_str()) {
            let input = boundary.input();
            ports.push(CompositePortRef::new(
                boundary.subgraph_id.clone(),
                input.name.clone(),
            ));
            *to = input.stage_name.clone();
        }
        if let Some(boundary) = artifacts.boundaries.get(from.as_str()) {
            let binding = from.clone();
            let hint = downstream_input_hint(&stages, to);
            let port = boundary.resolve_output(hint.as_ref()).map_err(|err| match err {
                PortResolveError::Ambiguous {
                    ports,
                    input_display,
                } => {
                    let listed = ports
                        .iter()
                        .map(|p| format!("'{p}'"))
                        .collect::<Vec<_>>()
                        .join(" and ");
                    build_error(format!(
                        "composite '{binding}': ambiguous output port for downstream '{to}' (input '{input_display}' matches ports {listed}); bind explicitly via the composite's branch clause"
                    ))
                }
                PortResolveError::NoDefaultPort => build_error(format!(
                    "composite '{binding}': no default output port for downstream '{to}'"
                )),
            })?;
            ports.push(CompositePortRef::new(
                boundary.subgraph_id.clone(),
                port.name.clone(),
            ));
            *from = port.stage_name.clone();
        }
        if !ports.is_empty() {
            artifacts.boundary_edges.push(BoundaryEdgeBindingSpec {
                from_stage: from.clone(),
                to_stage: to.clone(),
                kind: *kind,
                ports,
            });
        }
    }

    Ok((stages, artifacts))
}
