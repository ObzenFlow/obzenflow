// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for the DSL

pub mod ai_map_reduce_lowering_test;
pub mod archive_sink_gate_test;
pub mod composite_source_guards_test;
pub mod composite_substrate_test;
pub mod cycle_detection_test;
pub mod join_tuple_syntax_test;
pub mod lowering_helper_contract_test;
pub mod placeholder_handlers_test;
pub mod typed_decoration_matrix_test;
pub mod typed_stage_contracts_test;

/// Existing non-join graph fixtures still assert physical edge rewrites.
#[allow(clippy::type_complexity, clippy::result_large_err)]
fn lower_edges(
    members: std::collections::HashMap<String, super::composition::FlowMember>,
    connections: &mut Vec<(String, String, obzenflow_topology::EdgeKind)>,
) -> Result<
    (
        std::collections::HashMap<String, Box<dyn super::stage_descriptor::StageDescriptor>>,
        super::composites::LoweringArtifacts,
    ),
    super::FlowBuildError,
> {
    let authored = connections
        .iter()
        .map(|(from, to, kind)| {
            super::topology::AuthoredConnection::edge(from.clone(), to.clone(), *kind)
        })
        .collect();
    let lowered = super::composites::lower_composites(members, authored)?;
    *connections = lowered.connections;
    Ok((lowered.stages, lowered.artifacts))
}
