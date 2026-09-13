// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Prepare validated immutable topology inputs before binding the host.

use crate::web::host_error::ManagedWebHostError;
use obzenflow_adapters::studio::{
    ContractBoundaryAlias, ContractBoundaryAliases, ContractBoundaryDirection,
};
use obzenflow_core::StageId;
use std::collections::HashMap;

pub(crate) fn contract_boundary_aliases(
    topology: &obzenflow_topology::Topology,
) -> Result<ContractBoundaryAliases, ManagedWebHostError> {
    topology.validate_composite_boundaries().map_err(|error| {
        ManagedWebHostError::Implementation {
            message: format!("invalid composite contract boundary projection: {error}"),
            source: Some(Box::new(error)),
        }
    })?;

    let subgraphs = topology
        .subgraphs()
        .iter()
        .map(|subgraph| (subgraph.subgraph_id.as_str(), subgraph))
        .collect::<HashMap<_, _>>();
    let mut by_edge = HashMap::new();

    for edge in topology.edges() {
        let mut aliases = Vec::with_capacity(edge.composite_ports.len());
        for port_ref in &edge.composite_ports {
            let subgraph = subgraphs
                .get(port_ref.subgraph_id.as_str())
                .ok_or_else(|| ManagedWebHostError::Implementation {
                    message: format!(
                        "edge {} -> {} references missing composite {}",
                        edge.from, edge.to, port_ref.subgraph_id
                    ),
                    source: None,
                })?;
            let port = subgraph
                .boundary_ports
                .iter()
                .find(|port| port.name == port_ref.port_name)
                .ok_or_else(|| ManagedWebHostError::Implementation {
                    message: format!(
                        "edge {} -> {} references missing port {}.{}",
                        edge.from, edge.to, port_ref.subgraph_id, port_ref.port_name
                    ),
                    source: None,
                })?;
            let direction = match port.direction {
                obzenflow_topology::PortDirection::Input => ContractBoundaryDirection::Inbound,
                obzenflow_topology::PortDirection::Output => ContractBoundaryDirection::Outbound,
            };
            aliases.push(ContractBoundaryAlias {
                composite_id: obzenflow_core::id::CompositeId::new(port_ref.subgraph_id.clone()),
                port: port_ref.port_name.clone(),
                direction,
            });
        }

        aliases.sort();
        aliases.dedup();
        if !aliases.is_empty() {
            by_edge.insert(
                (
                    StageId::from_ulid(edge.from.ulid()),
                    StageId::from_ulid(edge.to.ulid()),
                ),
                aliases,
            );
        }
    }

    Ok(ContractBoundaryAliases::new(by_edge))
}
