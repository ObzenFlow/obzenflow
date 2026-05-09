// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Topology HTTP endpoint for flow visualization
//!
//! Provides `/api/topology` endpoint that returns the flow structure
//! for visualization in UI tools.

use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use obzenflow_core::StageId;
use std::sync::Arc;

type StageMetadataMap = std::collections::HashMap<StageId, StageMetadata>;
type ContractAttachments = Arc<std::collections::HashMap<(StageId, StageId), Vec<String>>>;

// FLOWIP-114b: Wire DTOs (FlowTopologyResponse, StageApiInfo, EdgeApiInfo,
// StageTypingApiInfo, TypeHintApiInfo, JoinMetadataApiInfo, MiddlewareApiInfo,
// CircuitBreakerApiInfo, RateLimiterApiInfo, RetryApiInfo, EdgeTypingApiInfo,
// ContractApiInfo, SubgraphApiInfo, StageSubgraphApiInfo, plus their *Api enum
// counterparts) used to live here. They have been removed; the endpoint
// serialises the canonical `obzenflow_topology::Topology` directly.

/// HTTP endpoint that serves flow topology.
///
/// As of FLOWIP-114b, the canonical `obzenflow_topology::Topology` carries
/// stage typing, edge typing, join metadata, middleware, subgraph
/// membership, the subgraph registry, role, and cycle membership directly.
/// The endpoint serialises that `Topology` as the response body, overlaying
/// only the two side maps that are not (yet) baked in:
///
/// - `stages_metadata` — runtime-tracked status per stage.
/// - `contract_attachments` — derived in `PipelineBuilder::build` from
///   topology shape, not at flow-build time.
pub struct TopologyHttpEndpoint {
    topology: Arc<obzenflow_topology::Topology>,
    stages_metadata: Arc<StageMetadataMap>,
    contract_attachments: Option<ContractAttachments>,
}

/// Runtime-tracked stage status.
#[derive(Debug, Clone)]
pub struct StageMetadata {
    pub status: StageStatus,
}

#[derive(Debug, Clone)]
pub enum StageStatus {
    Running,
    Completed,
    Failed,
    Pending,
}

/// Map the infra-internal stage status enum to the canonical `StageStatus`.
fn canonical_stage_status(status: &StageStatus) -> obzenflow_topology::StageStatus {
    match status {
        StageStatus::Running => obzenflow_topology::StageStatus::Running,
        StageStatus::Completed => obzenflow_topology::StageStatus::Completed,
        StageStatus::Failed => obzenflow_topology::StageStatus::Failed,
        StageStatus::Pending => obzenflow_topology::StageStatus::Pending,
    }
}

/// Flags for optional sections controlled via query params
#[derive(Debug, Default, Clone, Copy)]
struct IncludeFlags {
    middleware: bool,
    contracts: bool,
}

fn parse_include_flags(_request: &Request) -> IncludeFlags {
    // FLOWIP-059 spec change:
    // The topology endpoint now includes all structural observability
    // sections (middleware + contracts) by default. The `include`
    // query parameter is accepted for backwards compatibility but
    // no longer gates payload sections.
    //
    // We still parse the parameter so existing clients that send it
    // don't break, but we treat the absence/presence as equivalent.
    IncludeFlags {
        middleware: true,
        contracts: true,
    }
}

impl TopologyHttpEndpoint {
    pub fn new(
        topology: Arc<obzenflow_topology::Topology>,
        stages_metadata: Arc<StageMetadataMap>,
        contract_attachments: Option<ContractAttachments>,
    ) -> Self {
        Self {
            topology,
            stages_metadata,
            contract_attachments,
        }
    }

    /// Produce the canonical `Topology` value to serve in the response body.
    ///
    /// The topology is cloned and overlaid with the two side facts that
    /// are not yet baked into the canonical model: per-stage runtime status
    /// from `stages_metadata`, and per-edge contract names from
    /// `contract_attachments`.
    fn build_response(&self, include: IncludeFlags) -> obzenflow_topology::Topology {
        let _ = (include.middleware, include.contracts);

        let stages: Vec<obzenflow_topology::StageInfo> = self
            .topology
            .stages()
            .map(|stage_info| {
                let core_stage_id = StageId::from_ulid(stage_info.id.ulid());
                let mut info = stage_info.clone();
                if let Some(meta) = self.stages_metadata.get(&core_stage_id) {
                    info.status = Some(canonical_stage_status(&meta.status));
                }
                info
            })
            .collect();

        let edges: Vec<obzenflow_topology::DirectedEdge> = self
            .topology
            .edges()
            .iter()
            .map(|edge| {
                let from_core = StageId::from_ulid(edge.from.ulid());
                let to_core = StageId::from_ulid(edge.to.ulid());
                let mut e = edge.clone();
                if let Some(map) = self.contract_attachments.as_ref() {
                    if let Some(names) = map.get(&(from_core, to_core)) {
                        e.contracts = Some(
                            names
                                .iter()
                                .cloned()
                                .map(obzenflow_topology::ContractInfo::new)
                                .collect(),
                        );
                    }
                }
                e
            })
            .collect();

        // Reuse the canonical top-level annotations (flow_name,
        // api_version, subgraphs registry) that were baked in at flow
        // build time; rebuild via `new_unvalidated` so cycle/SCC caches
        // stay consistent with any structural changes.
        let subgraphs = self.topology.subgraphs().to_vec();
        let mut topology = obzenflow_topology::Topology::new_unvalidated(stages, edges)
            .expect("annotated topology rebuilds from a valid input topology")
            .with_subgraphs(subgraphs)
            .with_api_version("0.5");
        if let Some(name) = self.topology.flow_name_annotation() {
            topology = topology.with_flow_name(name);
        }
        topology.populate_derived_stage_annotations()
    }
}

#[async_trait]
impl HttpEndpoint for TopologyHttpEndpoint {
    fn path(&self) -> &str {
        "/api/topology"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        let include = parse_include_flags(&request);
        let topology_response = self.build_response(include);

        let json_body = serde_json::to_string(&topology_response).map_err(|e| {
            WebError::RequestHandlingFailed {
                message: format!("Failed to serialize topology: {e}"),
                source: None,
            }
        })?;

        let mut response = Response::ok();
        response
            .headers
            .insert("Content-Type".to_string(), "application/json".to_string());
        response
            .headers
            .insert("Cache-Control".to_string(), "max-age=60".to_string());
        response.body = json_body.into_bytes();

        Ok(response.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::id_conversions::StageIdExt;
    use obzenflow_runtime::pipeline::MiddlewareStackConfig;
    use obzenflow_topology::{
        DirectedEdge, EdgeKind, EdgeTypingLabelSource, EdgeTypingRole, JoinMetadataInfo,
        StageInfo, StageSubgraphMembership, StageType as TopologyStageType, StageTypingInfo,
        SubgraphInternalEdge, TopologyBuilder, TopologySubgraphInfo, TypeHintInfo,
    };
    use std::collections::HashMap;

    fn core_id(stage_id: obzenflow_topology::StageId) -> StageId {
        StageId::from_ulid(stage_id.ulid())
    }

    fn exact(name: &str) -> TypeHintInfo {
        TypeHintInfo::Exact {
            name: name.to_string(),
        }
    }

    fn stage_typing(
        input_type: TypeHintInfo,
        output_type: TypeHintInfo,
        reference_type: TypeHintInfo,
        stream_type: TypeHintInfo,
    ) -> StageTypingInfo {
        StageTypingInfo {
            input_type,
            output_type,
            boundary_in_type: TypeHintInfo::Unspecified,
            boundary_out_type: TypeHintInfo::Unspecified,
            reference_type,
            stream_type,
            is_placeholder: false,
            placeholder_message: None,
        }
    }

    fn stage_metadata(ids: &[StageId]) -> HashMap<StageId, StageMetadata> {
        ids.iter()
            .copied()
            .map(|id| {
                (
                    id,
                    StageMetadata {
                        status: StageStatus::Running,
                    },
                )
            })
            .collect()
    }

    fn edge_between(
        response: &obzenflow_topology::Topology,
        from: obzenflow_topology::StageId,
        to: obzenflow_topology::StageId,
    ) -> &DirectedEdge {
        response
            .edges()
            .iter()
            .find(|edge| edge.from == from && edge.to == to)
            .expect("edge should be present")
    }

    #[tokio::test]
    async fn topology_endpoint_exports_weighted_rate_limiter_config() {
        let mut builder = TopologyBuilder::new();
        let stage = builder.add_stage(Some("rate_limited".to_string()));
        builder.reset_current();
        let sink = builder.add_stage(Some("sink".to_string()));
        builder.reset_current();
        builder.add_edge(stage, sink);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let core_stage_id = StageId::from_ulid(stage.ulid());
        let sink_stage_id = StageId::from_ulid(sink.ulid());

        let mut stages_metadata: HashMap<StageId, StageMetadata> = HashMap::new();
        stages_metadata.insert(
            core_stage_id,
            StageMetadata {
                status: StageStatus::Running,
            },
        );
        stages_metadata.insert(
            sink_stage_id,
            StageMetadata {
                status: StageStatus::Running,
            },
        );

        // Bake the middleware annotation onto the stage in the canonical
        // topology, then construct the endpoint with the simplified
        // 3-arg signature.
        let stage_topology_id = stage;
        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == stage_topology_id {
                    s.middleware = Some(obzenflow_topology::MiddlewareInfo {
                        stack: vec!["rate_limiter".to_string()],
                        circuit_breaker: None,
                        rate_limiter: Some(obzenflow_topology::RateLimiterInfo {
                            tokens_per_sec: 2.0,
                            burst_capacity: 5.0,
                            configured_burst_capacity: None,
                            cost_per_event: 5.0,
                            limit_rate: 0.4,
                        }),
                        retry: None,
                    });
                }
                s
            })
            .collect();
        let edges: Vec<DirectedEdge> = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .with_flow_name("test_flow"),
        );

        let endpoint =
            TopologyHttpEndpoint::new(topology, Arc::new(stages_metadata), None);

        let response = endpoint
            .handle(Request::new(HttpMethod::Get, "/api/topology".to_string()))
            .await
            .expect("endpoint should handle request");
        let response = match response {
            ManagedResponse::Unary(resp) => resp,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        };

        assert_eq!(response.status, 200);

        let parsed: obzenflow_topology::Topology =
            serde_json::from_slice(&response.body).expect("response should be valid JSON");
        let stage = parsed
            .stages()
            .find(|stage| stage.name == "rate_limited")
            .expect("stage should be present");
        let rate_limiter = stage
            .middleware
            .as_ref()
            .and_then(|middleware| middleware.rate_limiter.as_ref())
            .expect("rate limiter config should be present");

        assert_eq!(rate_limiter.tokens_per_sec, 2.0);
        assert_eq!(rate_limiter.burst_capacity, 5.0);
        assert_eq!(rate_limiter.configured_burst_capacity, None);
        assert_eq!(rate_limiter.cost_per_event, 5.0);
        assert!((rate_limiter.limit_rate - 0.4).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn topology_endpoint_exports_stage_typing_schema_v0_5() {
        let mut builder = TopologyBuilder::new();
        let stage = builder.add_stage(Some("typed_stage".to_string()));
        builder.reset_current();
        let sink = builder.add_stage(Some("sink".to_string()));
        builder.reset_current();
        builder.add_edge(stage, sink);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let core_stage_id = StageId::from_ulid(stage.ulid());
        let sink_stage_id = StageId::from_ulid(sink.ulid());

        let mut stages_metadata: HashMap<StageId, StageMetadata> = HashMap::new();
        stages_metadata.insert(
            core_stage_id,
            StageMetadata {
                status: StageStatus::Running,
            },
        );
        stages_metadata.insert(
            sink_stage_id,
            StageMetadata {
                status: StageStatus::Running,
            },
        );

        let typed_stage_id = stage;
        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == typed_stage_id {
                    s.typing = Some(StageTypingInfo {
                        input_type: TypeHintInfo::Mixed,
                        output_type: TypeHintInfo::exact(
                            "product_catalog::domain::EnrichedOrder",
                        ),
                        boundary_in_type: TypeHintInfo::Unspecified,
                        boundary_out_type: TypeHintInfo::Unspecified,
                        reference_type: TypeHintInfo::Unspecified,
                        stream_type: TypeHintInfo::Unspecified,
                        is_placeholder: false,
                        placeholder_message: None,
                    });
                }
                s
            })
            .collect();
        let edges = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .with_flow_name("test_flow")
                .derive_edge_typings(),
        );

        let endpoint =
            TopologyHttpEndpoint::new(topology, Arc::new(stages_metadata), None);

        let response = endpoint
            .handle(
                Request::new(HttpMethod::Get, "/api/topology".to_string())
                    .with_query_param("include".to_string(), "middleware".to_string()),
            )
            .await
            .expect("endpoint should handle request");
        let response = match response {
            ManagedResponse::Unary(resp) => resp,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        };

        let parsed: obzenflow_topology::Topology =
            serde_json::from_slice(&response.body).expect("response should be valid JSON");
        assert_eq!(parsed.api_version(), Some("0.5"));

        let typed_stage = parsed
            .stages()
            .find(|stage| stage.name == "typed_stage")
            .expect("typed stage should be present");
        let typing = typed_stage
            .typing
            .as_ref()
            .expect("typed stage should include typing");

        assert!(matches!(typing.input_type, TypeHintInfo::Mixed));
        assert_eq!(typing.input_type.display_name(), None);
        assert!(matches!(typing.output_type, TypeHintInfo::Exact { .. }));
        match &typing.output_type {
            TypeHintInfo::Exact { name } => {
                assert_eq!(name, "product_catalog::domain::EnrichedOrder");
            }
            _ => unreachable!(),
        }
        assert_eq!(
            typing.output_type.display_name().as_deref(),
            Some("EnrichedOrder")
        );

        let sink_stage = parsed
            .stages()
            .find(|stage| stage.name == "sink")
            .expect("sink stage should be present");
        assert!(sink_stage.typing.is_none());

        let edge = edge_between(&parsed, stage, sink);
        let edge_typing = edge
            .typing
            .as_ref()
            .expect("ordinary forward edge should include typing");
        assert_eq!(edge_typing.role, EdgeTypingRole::Input);
        assert_eq!(
            edge_typing.label_source,
            EdgeTypingLabelSource::UpstreamOutputType
        );
        match &edge_typing.payload_type {
            TypeHintInfo::Exact { name } => {
                assert_eq!(name, "product_catalog::domain::EnrichedOrder");
            }
            _ => panic!("expected exact payload type"),
        }
        assert_eq!(
            edge_typing.payload_type.display_name().as_deref(),
            Some("EnrichedOrder")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_exports_per_edge_typing_for_fan_in() {
        let mut builder = TopologyBuilder::new();
        let source_a = builder.add_stage(Some("source_a".to_string()));
        builder.reset_current();
        let source_b = builder.add_stage(Some("source_b".to_string()));
        builder.reset_current();
        let merge = builder.add_stage(Some("merge".to_string()));
        builder.reset_current();
        builder.add_edge(source_a, merge);
        builder.add_edge(source_b, merge);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let source_a_core = core_id(source_a);
        let source_b_core = core_id(source_b);
        let merge_core = core_id(merge);

        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == source_a {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("Sku"),
                        TypeHintInfo::Unspecified,
                        TypeHintInfo::Unspecified,
                    ));
                } else if s.id == source_b {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("Promotion"),
                        TypeHintInfo::Unspecified,
                        TypeHintInfo::Unspecified,
                    ));
                }
                s
            })
            .collect();
        let edges = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .derive_edge_typings(),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[source_a_core, source_b_core, merge_core])),
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());

        let source_a_edge = edge_between(&parsed, source_a, merge)
            .typing
            .as_ref()
            .expect("source_a edge should include typing");
        assert_eq!(source_a_edge.role, EdgeTypingRole::Input);
        assert_eq!(
            source_a_edge.label_source,
            EdgeTypingLabelSource::UpstreamOutputType
        );
        match &source_a_edge.payload_type {
            TypeHintInfo::Exact { name } => assert_eq!(name, "Sku"),
            _ => panic!("expected exact payload type"),
        }
        assert_eq!(
            source_a_edge.payload_type.display_name().as_deref(),
            Some("Sku")
        );

        let source_b_edge = edge_between(&parsed, source_b, merge)
            .typing
            .as_ref()
            .expect("source_b edge should include typing");
        assert_eq!(source_b_edge.role, EdgeTypingRole::Input);
        assert_eq!(
            source_b_edge.label_source,
            EdgeTypingLabelSource::UpstreamOutputType
        );
        match &source_b_edge.payload_type {
            TypeHintInfo::Exact { name } => assert_eq!(name, "Promotion"),
            _ => panic!("expected exact payload type"),
        }
        assert_eq!(
            source_b_edge.payload_type.display_name().as_deref(),
            Some("Promotion")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_can_fall_back_to_downstream_input_for_single_input_edges() {
        let mut builder = TopologyBuilder::new();
        let source = builder.add_stage(Some("source".to_string()));
        builder.reset_current();
        let transform = builder.add_stage(Some("transform".to_string()));
        builder.reset_current();
        builder.add_edge(source, transform);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let source_core = core_id(source);
        let transform_core = core_id(transform);

        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == transform {
                    s.typing = Some(stage_typing(
                        exact("ValidatedOrder"),
                        exact("EnrichedOrder"),
                        TypeHintInfo::Unspecified,
                        TypeHintInfo::Unspecified,
                    ));
                }
                s
            })
            .collect();
        let edges = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .derive_edge_typings(),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[source_core, transform_core])),
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());
        let edge_typing = edge_between(&parsed, source, transform)
            .typing
            .as_ref()
            .expect("single-input edge should use downstream input typing");

        assert_eq!(edge_typing.role, EdgeTypingRole::Input);
        assert_eq!(
            edge_typing.label_source,
            EdgeTypingLabelSource::DownstreamInputType
        );
        match &edge_typing.payload_type {
            TypeHintInfo::Exact { name } => assert_eq!(name, "ValidatedOrder"),
            _ => panic!("expected exact payload type"),
        }
        assert_eq!(
            edge_typing.payload_type.display_name().as_deref(),
            Some("ValidatedOrder")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_exports_join_edge_typing_from_downstream_join_contract() {
        let mut builder = TopologyBuilder::new();
        let catalog = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(1_u128.to_be_bytes()),
            Some("catalog".to_string()),
            TopologyStageType::FiniteSource,
        );
        builder.reset_current();
        let stream = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(2_u128.to_be_bytes()),
            Some("orders".to_string()),
            TopologyStageType::FiniteSource,
        );
        builder.reset_current();
        let join = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(3_u128.to_be_bytes()),
            Some("promo_enriched".to_string()),
            TopologyStageType::Join,
        );
        builder.reset_current();
        builder.add_edge(catalog, join);
        builder.add_edge(stream, join);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let catalog_core = core_id(catalog);
        let stream_core = core_id(stream);
        let join_core = core_id(join);

        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == catalog {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("CatalogOutputShouldNotDriveJoinEdge"),
                        TypeHintInfo::Unspecified,
                        TypeHintInfo::Unspecified,
                    ));
                } else if s.id == stream {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("StreamOutputShouldNotDriveJoinEdge"),
                        TypeHintInfo::Unspecified,
                        TypeHintInfo::Unspecified,
                    ));
                } else if s.id == join {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("EnrichedOrderWithPromo"),
                        exact("Promotion"),
                        exact("EnrichedOrder"),
                    ));
                    s.join_metadata = Some(JoinMetadataInfo::new(vec![catalog], vec![stream]));
                }
                s
            })
            .collect();
        let edges = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .derive_edge_typings(),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[catalog_core, stream_core, join_core])),
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());

        let catalog_edge = edge_between(&parsed, catalog, join)
            .typing
            .as_ref()
            .expect("catalog edge should include reference typing");
        assert_eq!(catalog_edge.role, EdgeTypingRole::Reference);
        assert_eq!(
            catalog_edge.label_source,
            EdgeTypingLabelSource::DownstreamReferenceType
        );
        match &catalog_edge.payload_type {
            TypeHintInfo::Exact { name } => assert_eq!(name, "Promotion"),
            _ => panic!("expected exact payload type"),
        }
        assert_eq!(
            catalog_edge.payload_type.display_name().as_deref(),
            Some("Promotion")
        );

        let stream_edge = edge_between(&parsed, stream, join)
            .typing
            .as_ref()
            .expect("stream edge should include stream typing");
        assert_eq!(stream_edge.role, EdgeTypingRole::Stream);
        assert_eq!(
            stream_edge.label_source,
            EdgeTypingLabelSource::DownstreamStreamType
        );
        match &stream_edge.payload_type {
            TypeHintInfo::Exact { name } => assert_eq!(name, "EnrichedOrder"),
            _ => panic!("expected exact payload type"),
        }
        assert_eq!(
            stream_edge.payload_type.display_name().as_deref(),
            Some("EnrichedOrder")
        );
    }

    #[tokio::test]
    async fn topology_endpoint_omits_join_edge_typing_without_join_metadata() {
        let mut builder = TopologyBuilder::new();
        let upstream = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(11_u128.to_be_bytes()),
            Some("catalog".to_string()),
            TopologyStageType::FiniteSource,
        );
        builder.reset_current();
        let join = builder.add_stage_with_id(
            obzenflow_topology::StageId::from_bytes(12_u128.to_be_bytes()),
            Some("join".to_string()),
            TopologyStageType::Join,
        );
        builder.reset_current();
        builder.add_edge(upstream, join);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let upstream_core = core_id(upstream);
        let join_core = core_id(join);

        // No `join_metadata` on the join stage: the canonical
        // `derive_edge_typings` rule omits typing on join inputs when
        // catalog/stream classification is unavailable.
        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == upstream {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("Promotion"),
                        TypeHintInfo::Unspecified,
                        TypeHintInfo::Unspecified,
                    ));
                } else if s.id == join {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("EnrichedOrder"),
                        exact("Promotion"),
                        exact("ValidatedOrder"),
                    ));
                }
                s
            })
            .collect();
        let edges = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .derive_edge_typings(),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[upstream_core, join_core])),
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());

        assert!(edge_between(&parsed, upstream, join).typing.is_none());
    }

    #[tokio::test]
    async fn topology_endpoint_omits_typing_for_backward_edges() {
        let mut builder = TopologyBuilder::new();
        let from = builder.add_stage(Some("retry_source".to_string()));
        builder.reset_current();
        let to = builder.add_stage(Some("retry_target".to_string()));
        builder.reset_current();
        builder.add_backward_edge(from, to);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let from_core = core_id(from);
        let to_core = core_id(to);

        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == from {
                    s.typing = Some(stage_typing(
                        TypeHintInfo::Unspecified,
                        exact("RetryPayload"),
                        TypeHintInfo::Unspecified,
                        TypeHintInfo::Unspecified,
                    ));
                }
                s
            })
            .collect();
        let edges = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .derive_edge_typings(),
        );

        let endpoint = TopologyHttpEndpoint::new(
            topology,
            Arc::new(stage_metadata(&[from_core, to_core])),
            None,
        );

        let parsed = endpoint.build_response(IncludeFlags::default());
        let edge = edge_between(&parsed, from, to);

        assert_eq!(edge.kind, EdgeKind::Backward);
        assert!(edge.typing.is_none());
    }

    #[tokio::test]
    async fn topology_endpoint_exports_subgraphs_schema_v0_5() {
        let mut builder = TopologyBuilder::new();
        let stage_a = builder.add_stage(Some("a".to_string()));
        builder.reset_current(); // avoid implicit chaining edges
        let stage_b = builder.add_stage(Some("b".to_string()));
        builder.reset_current(); // avoid implicit chaining edges
        builder.add_edge(stage_a, stage_b);

        let topology = Arc::new(
            builder
                .build_unchecked()
                .expect("topology should build with structural validation"),
        );

        let a_core = StageId::from_ulid(stage_a.ulid());
        let b_core = StageId::from_ulid(stage_b.ulid());

        let mut stages_metadata: HashMap<StageId, StageMetadata> = HashMap::new();
        stages_metadata.insert(
            a_core,
            StageMetadata {
                status: StageStatus::Running,
            },
        );
        stages_metadata.insert(
            b_core,
            StageMetadata {
                status: StageStatus::Running,
            },
        );

        let membership = StageSubgraphMembership {
            subgraph_id: "ai_map_reduce:digest".to_string(),
            kind: "ai_map_reduce".to_string(),
            binding: "digest".to_string(),
            role: "chunk".to_string(),
            order: 0,
            is_entry: true,
            is_exit: false,
        };

        let subgraph = TopologySubgraphInfo {
            subgraph_id: "ai_map_reduce:digest".to_string(),
            kind: "ai_map_reduce".to_string(),
            binding: "digest".to_string(),
            label: "digest".to_string(),
            member_stage_ids: vec![a_core.to_topology_id(), b_core.to_topology_id()],
            internal_edges: vec![SubgraphInternalEdge {
                from_stage_id: a_core.to_topology_id(),
                to_stage_id: b_core.to_topology_id(),
                role: "data".to_string(),
            }],
            entry_stage_ids: vec![a_core.to_topology_id()],
            exit_stage_ids: vec![b_core.to_topology_id()],
            parent_subgraph_id: None,
            collapsible: true,
        };

        let stages: Vec<StageInfo> = topology
            .stages()
            .cloned()
            .map(|mut s| {
                if s.id == stage_a {
                    s.subgraph = Some(membership.clone());
                }
                s
            })
            .collect();
        let edges = topology.edges().to_vec();
        let topology = Arc::new(
            obzenflow_topology::Topology::new_unvalidated(stages, edges)
                .expect("rebuild")
                .with_subgraphs(vec![subgraph])
                .with_flow_name("test_flow"),
        );

        let endpoint =
            TopologyHttpEndpoint::new(topology, Arc::new(stages_metadata), None);

        let response = endpoint
            .handle(Request::new(HttpMethod::Get, "/api/topology".to_string()))
            .await
            .expect("endpoint should handle request");
        let response = match response {
            ManagedResponse::Unary(resp) => resp,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        };

        assert_eq!(response.status, 200);

        let parsed: obzenflow_topology::Topology =
            serde_json::from_slice(&response.body).expect("response should be valid JSON");
        assert_eq!(parsed.api_version(), Some("0.5"));
        assert_eq!(parsed.subgraphs().len(), 1);

        let a_topology = a_core.to_topology_id();
        let b_topology = b_core.to_topology_id();

        let a_info = parsed.stage_info(a_topology).expect("stage a present");
        assert!(a_info.subgraph.is_some());

        let b_info = parsed.stage_info(b_topology).expect("stage b present");
        assert!(b_info.subgraph.is_none());

        let expected_members = vec![a_topology, b_topology];
        assert_eq!(parsed.subgraphs()[0].member_stage_ids, expected_members);
        assert_eq!(parsed.subgraphs()[0].internal_edges.len(), 1);
        assert_eq!(parsed.subgraphs()[0].internal_edges[0].role, "data");
    }
}
