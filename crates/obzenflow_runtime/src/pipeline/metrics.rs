// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline metrics preparation, lifecycle rollups and topology boundary metadata.

use super::fsm::PipelineContext;
use crate::id_conversions::StageIdExt;
use crate::supervised_base::BuilderError;
use obzenflow_core::metrics::FlowLifecycleMetricsSnapshot;

pub(super) async fn prepare_metrics(
    context: &PipelineContext,
) -> Result<Option<crate::metrics::builder::PreparedMetricsAggregator>, BuilderError> {
    use crate::metrics::{MetricsAggregatorBuilder, MetricsInputs};
    let Some(exporter) = context.metrics_exporter.clone() else {
        return Ok(None);
    };
    let inputs = MetricsInputs::new(
        context.stage_data_journals.clone(),
        context.stage_error_journals.clone(),
    )
    .with_backpressure_registry_opt(context.backpressure_registry.clone());
    let metadata = context
        .stage_supervisors
        .iter()
        .chain(context.source_supervisors.iter())
        .filter_map(|(id, handle)| {
            context
                .topology
                .stages()
                .find(|stage| stage.id == id.to_topology_id())
                .map(|stage| {
                    (
                        *id,
                        obzenflow_core::metrics::StageMetadata {
                            name: stage.name.clone(),
                            stage_type: handle.stage_type(),
                            reference_mode: None,
                            flow_name: context.flow_name.clone(),
                            flow_id: Some(context.flow_id),
                        },
                    )
                })
        })
        .collect();
    let builder = MetricsAggregatorBuilder::new(inputs, context.system_journal.clone(), exporter)
        .with_pipeline_writer(context.system_id.into())
        .with_stage_metadata(metadata)
        .with_composite_boundaries(composite_boundaries_from_topology(&context.topology))
        .with_export_interval(1);
    builder.prepare().await.map(Some)
}

/// Compute flow-level lifecycle metrics from per-stage snapshots in the context.
pub(crate) fn compute_flow_lifecycle_metrics(
    context: &PipelineContext,
) -> FlowLifecycleMetricsSnapshot {
    use obzenflow_core::event::context::StageType as CoreStageType;

    let mut events_in_total: u64 = 0;
    let mut events_out_total: u64 = 0;
    let mut errors_total: u64 = 0;

    for (stage_id, snapshot) in &context.stage_lifecycle_metrics {
        // Map core StageId to topology StageId
        let topo_stage_id = stage_id.to_topology_id();

        // Look up stage info to determine semantic type
        if let Some(stage_info) = context.topology.stages().find(|s| s.id == topo_stage_id) {
            // Map topology StageType to core StageType (they share the same shape)
            let core_type = match stage_info.stage_type {
                obzenflow_topology::StageType::FiniteSource => CoreStageType::FiniteSource,
                obzenflow_topology::StageType::InfiniteSource => CoreStageType::InfiniteSource,
                obzenflow_topology::StageType::Transform => CoreStageType::Transform,
                obzenflow_topology::StageType::Sink => CoreStageType::Sink,
                obzenflow_topology::StageType::Stateful => CoreStageType::Stateful,
                obzenflow_topology::StageType::Join => CoreStageType::Join,
            };

            match core_type {
                CoreStageType::FiniteSource | CoreStageType::InfiniteSource => {
                    events_in_total =
                        events_in_total.saturating_add(snapshot.events_processed_total);
                }
                CoreStageType::Sink => {
                    events_out_total =
                        events_out_total.saturating_add(snapshot.events_processed_total);
                }
                _ => {}
            }
        }

        // Always include errors for all stages
        errors_total = errors_total.saturating_add(snapshot.errors_total);
    }

    FlowLifecycleMetricsSnapshot {
        events_in_total,
        events_out_total,
        errors_total,
    }
}

/// Build exact named graph cuts from durable topology edge-port bindings
/// (FLOWIP-128a B3). `collapsible` is presentation metadata and never gates
/// backend projection.
pub(crate) fn composite_boundaries_from_topology(
    topology: &obzenflow_topology::Topology,
) -> Vec<obzenflow_core::metrics::CompositeBoundary> {
    use crate::id_conversions::StageIdExt;
    use obzenflow_core::id::{CompositeId, StageId};
    use obzenflow_core::metrics::{
        BoundaryDirection, CompositeBoundary, CompositeBoundaryEdge, CompositeBoundaryPort,
    };

    let mut boundaries: Vec<_> = topology
        .subgraphs()
        .iter()
        .map(|subgraph| {
            let members = subgraph
                .member_stage_ids
                .iter()
                .map(|id| StageId::from_topology_id(*id))
                .collect();

            let mut ports: Vec<_> = subgraph
                .boundary_ports
                .iter()
                .map(|port| CompositeBoundaryPort {
                    name: port.name.clone(),
                    direction: match port.direction {
                        obzenflow_topology::PortDirection::Input => BoundaryDirection::Inbound,
                        obzenflow_topology::PortDirection::Output => BoundaryDirection::Outbound,
                    },
                    member: StageId::from_topology_id(port.member_stage_id),
                    payload_event_types: port
                        .payload_event_types
                        .iter()
                        .cloned()
                        .map(obzenflow_core::EventType::from)
                        .collect(),
                })
                .collect();
            ports.sort_by(|left, right| {
                (left.direction.as_str(), left.name.as_str())
                    .cmp(&(right.direction.as_str(), right.name.as_str()))
            });

            let mut edges = Vec::new();
            for edge in topology.edges() {
                for port_ref in &edge.composite_ports {
                    if port_ref.subgraph_id != subgraph.subgraph_id {
                        continue;
                    }
                    let Some(port) = ports.iter().find(|port| port.name == port_ref.port_name)
                    else {
                        // Topology validation rejects this before runtime build.
                        continue;
                    };
                    let upstream = StageId::from_topology_id(edge.from);
                    let downstream = StageId::from_topology_id(edge.to);
                    let (member, peer) = match port.direction {
                        BoundaryDirection::Inbound => (downstream, upstream),
                        BoundaryDirection::Outbound => (upstream, downstream),
                    };
                    edges.push(CompositeBoundaryEdge {
                        port: port.name.clone(),
                        direction: port.direction,
                        member,
                        peer,
                        upstream,
                        downstream,
                    });
                }
            }
            edges.sort_by(|left, right| {
                (
                    left.direction.as_str(),
                    left.port.as_str(),
                    left.upstream,
                    left.downstream,
                )
                    .cmp(&(
                        right.direction.as_str(),
                        right.port.as_str(),
                        right.upstream,
                        right.downstream,
                    ))
            });

            CompositeBoundary {
                composite_id: CompositeId::new(subgraph.subgraph_id.clone()),
                members,
                ports,
                edges,
            }
        })
        .collect();
    boundaries.sort_by(|left, right| left.composite_id.cmp(&right.composite_id));
    boundaries
}
