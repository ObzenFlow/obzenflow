// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Assembly of the application-owned managed host.

use crate::web::host_config::HostConfig;
use crate::web::host_error::ManagedWebHostError;
use obzenflow_core::composite::{CompositeDefinition, CompositeLifecycleProjection};
use obzenflow_core::id::{CompositeId, RoleId};
use obzenflow_core::web::EndpointError;
use obzenflow_core::web::{HttpEndpoint, HttpMethod};
use obzenflow_core::StageId;
use obzenflow_runtime::pipeline::FlowHandle;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_topology::Topology;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use super::surface_metrics::HttpSurfaceMetricsCollector;
use super::RuntimeInstanceId;

pub(crate) type ContractAttachments = Arc<HashMap<(StageId, StageId), Vec<String>>>;

pub(crate) struct ManagedHostInput {
    /// Canonical topology for this flow. Carries FLOWIP-114b annotations
    /// (stage typing, join metadata, middleware, subgraph membership,
    /// subgraph registry, role, cycle membership, flow name, API version)
    /// directly on `StageInfo` / `DirectedEdge` / `Topology`.
    pub topology: Arc<Topology>,
    /// Structural contract names per edge. Still passed alongside the
    /// topology because contracts are derived in `PipelineBuilder::build`
    /// from the topology shape and are not yet baked into the canonical
    /// `Topology`.
    pub contract_attachments: Option<ContractAttachments>,
    #[cfg(feature = "prometheus")]
    pub metrics_endpoint: Option<super::endpoints::PrometheusMetricsEndpoint>,
    pub flow_handle: Arc<FlowHandle>,
    pub extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    pub surface_metrics: Option<Arc<HttpSurfaceMetricsCollector>>,
    /// FLOWIP-010: the owned resolved snapshot; presence turns on the seven
    /// read-only `/api/config/*` routes.
    pub runtime_config: Arc<obzenflow_runtime::runtime_config::ResolvedRuntimeConfig>,
    /// FLOWIP-114d: per-process incarnation identity, stamped into the SSE
    /// bootstrap event for data-path generation detection.
    pub runtime_instance_id: RuntimeInstanceId,
    /// FLOWIP-114d gap 8: fires after the terminal pipeline state; the
    /// listener then closes gracefully and SSE producers end their streams.
    pub shutdown: tokio::sync::watch::Sender<bool>,
}

fn composite_definitions_from_topology(
    topology: &Topology,
) -> Result<Vec<CompositeDefinition>, ManagedWebHostError> {
    let mut definitions = Vec::with_capacity(topology.subgraphs().len());

    for subgraph in topology.subgraphs() {
        let mut members = Vec::with_capacity(subgraph.member_stage_ids.len());
        for member_id in &subgraph.member_stage_ids {
            let stage = topology
                .stages()
                .find(|stage| stage.id == *member_id)
                .ok_or_else(|| ManagedWebHostError::Implementation {
                    message: format!(
                        "composite {} references missing member stage {}",
                        subgraph.subgraph_id, member_id
                    ),
                    source: None,
                })?;
            let membership = stage
                .subgraph
                .as_ref()
                .filter(|membership| membership.subgraph_id == subgraph.subgraph_id)
                .ok_or_else(|| ManagedWebHostError::Implementation {
                    message: format!(
                        "composite {} member {} has no matching subgraph membership",
                        subgraph.subgraph_id, member_id
                    ),
                    source: None,
                })?;

            members.push((
                StageId::from_ulid(stage.id.ulid()),
                RoleId::new(membership.role.clone()),
            ));
        }

        definitions.push(CompositeDefinition::new(
            CompositeId::new(subgraph.subgraph_id.clone()),
            members,
        ));
    }

    CompositeLifecycleProjection::new(definitions.clone()).map_err(|error| {
        ManagedWebHostError::Implementation {
            message: format!("invalid composite lifecycle projection: {error}"),
            source: Some(Box::new(error)),
        }
    })?;

    Ok(definitions)
}

fn is_reserved_built_in_path(path: &str) -> bool {
    // Paths owned by the framework (built-in endpoints). Attached surfaces must not register
    // any of these routes because registration order shadowing is not a coherent operator story.
    //
    // Note: `/api/flow/*` is reserved as a prefix tree because it includes both control and
    // streaming endpoints (e.g. SSE).
    matches!(
        path,
        "/metrics" | "/health" | "/ready" | "/api/topology" | "/api/flow/events"
    ) || path == "/api/flow"
        || path.starts_with("/api/flow/")
        || path == "/api/config"
        || path.starts_with("/api/config/")
}

fn validate_extra_endpoints(
    extra_endpoints: &[Box<dyn HttpEndpoint>],
) -> Result<(), ManagedWebHostError> {
    use super::routing::{matchit_template_to_public, public_template_to_matchit};
    use matchit::Router as MatchItRouter;

    const ALL_METHODS: [HttpMethod; 7] = [
        HttpMethod::Get,
        HttpMethod::Post,
        HttpMethod::Put,
        HttpMethod::Delete,
        HttpMethod::Patch,
        HttpMethod::Head,
        HttpMethod::Options,
    ];

    let mut methods_by_template: HashMap<String, HashSet<HttpMethod>> = HashMap::new();
    for endpoint in extra_endpoints {
        let path = endpoint.path().to_string();
        if is_reserved_built_in_path(&path) {
            return Err(ManagedWebHostError::EndpointRegistrationFailed {
                path,
                message: "Reserved built-in path; choose a different route".to_string(),
            });
        }

        let claimed_methods: Vec<HttpMethod> = if endpoint.methods().is_empty() {
            ALL_METHODS.to_vec()
        } else {
            endpoint.methods().to_vec()
        };

        for method in claimed_methods {
            let entry = methods_by_template.entry(path.clone()).or_default();
            if !entry.insert(method) {
                return Err(ManagedWebHostError::EndpointRegistrationFailed {
                    path: path.clone(),
                    message: format!("Duplicate route: {} {}", method.as_str(), path),
                });
            }
        }
    }

    let mut templates: Vec<String> = methods_by_template.keys().cloned().collect();
    templates.sort();

    let mut router = MatchItRouter::new();
    for template in templates {
        let matchit_path = public_template_to_matchit(&template).map_err(|message| {
            ManagedWebHostError::EndpointRegistrationFailed {
                path: template.clone(),
                message,
            }
        })?;

        if let Err(err) = router.insert(matchit_path, ()) {
            let message = match err {
                matchit::InsertError::Conflict { with } => format!(
                    "Route template conflicts with previously registered route: {}",
                    matchit_template_to_public(&with)
                ),
                other => other.to_string(),
            };
            return Err(ManagedWebHostError::EndpointRegistrationFailed {
                path: template.clone(),
                message,
            });
        }
    }

    Ok(())
}

/// Assemble the required application resources and return an already bound host.
pub(crate) async fn bind_managed_host(
    resources: ManagedHostInput,
    server_config: HostConfig,
) -> Result<super::managed_host::ManagedWebHost, ManagedWebHostError> {
    use super::endpoints::topology::{StageMetadata, StageStatus};
    use super::endpoints::{FlowControlEndpoint, TopologyHttpEndpoint};

    let ManagedHostInput {
        topology,
        contract_attachments,
        #[cfg(feature = "prometheus")]
        metrics_endpoint,
        flow_handle,
        extra_endpoints,
        surface_metrics,
        runtime_config,
        runtime_instance_id,
        shutdown,
    } = resources;

    validate_extra_endpoints(&extra_endpoints)?;

    let mut server = super::warp::WarpWebHost::new();
    server.with_composite_definitions(composite_definitions_from_topology(&topology)?);
    server.with_contract_boundary_aliases(&topology)?;
    if let Some(collector) = surface_metrics {
        server.with_surface_metrics(collector);
    }
    server.with_runtime_instance_id(runtime_instance_id);
    let pipeline_state = flow_handle.state_receiver();

    // Initial per-stage runtime status; the canonical topology already
    // carries the structural stage type, so this map only carries status.
    let mut stages_metadata = HashMap::new();
    for stage_info in topology.stages() {
        let core_stage_id = StageId::from_ulid(stage_info.id.ulid());
        stages_metadata.insert(
            core_stage_id,
            StageMetadata {
                status: StageStatus::Pending,
            },
        );
    }

    server.register_endpoint(Box::new(TopologyHttpEndpoint::new(
        topology.clone(),
        Arc::new(stages_metadata),
        contract_attachments,
    )))?;

    // FLOWIP-010: the seven read-only config introspection routes, gated on
    // the owned snapshot being threaded (control-plane auth applies).
    {
        let snapshot = runtime_config;
        use super::endpoints::{ConfigHttpEndpoint, ConfigReadModel, ConfigRoute};

        let flow_name = flow_handle.flow_name().to_string();
        let flow_id = flow_handle
            .run_substrate()
            .locator()
            .and_then(|locator| {
                locator
                    .path()
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| flow_name.clone());
        let flow_effective = flow_handle.flow_effective_config().cloned();

        let model = Arc::new(ConfigReadModel::new(
            snapshot,
            flow_effective,
            topology.clone(),
            flow_name,
            flow_id,
        ));
        for route in ConfigRoute::ALL {
            server.register_endpoint(Box::new(ConfigHttpEndpoint::new(model.clone(), route)))?;
        }
    }

    #[cfg(feature = "prometheus")]
    if let Some(endpoint) = metrics_endpoint {
        server.register_endpoint(Box::new(endpoint))?;
    }

    if let Some(journal) = flow_handle.system_journal() {
        server.with_system_journal(journal);
    }
    server.register_endpoint(Box::new(FlowControlEndpoint::new(flow_handle)))?;

    for endpoint in extra_endpoints {
        server.register_endpoint(endpoint)?;
    }

    // Add health and ready endpoints
    server.register_endpoint(Box::new(SimpleHealthEndpoint))?;
    server.register_endpoint(Box::new(PipelineReadyEndpoint::new(pipeline_state)))?;
    server.bind(server_config, shutdown).await
}

// Built-in health and readiness endpoints
use async_trait::async_trait;
use obzenflow_core::web::{ManagedResponse, Request, Response};

/// Simple health endpoint
struct SimpleHealthEndpoint;

#[async_trait]
impl HttpEndpoint for SimpleHealthEndpoint {
    fn path(&self) -> &str {
        "/health"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
        Ok(Response::ok().with_text("OK").into())
    }
}

/// Pipeline readiness endpoint.
///
/// Reads Runtime's current state directly (Running => 200, otherwise 503).
struct PipelineReadyEndpoint {
    state: tokio::sync::watch::Receiver<PipelineState>,
}

impl PipelineReadyEndpoint {
    fn new(state: tokio::sync::watch::Receiver<PipelineState>) -> Self {
        Self { state }
    }
}

#[async_trait]
impl HttpEndpoint for PipelineReadyEndpoint {
    fn path(&self) -> &str {
        "/ready"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
        if matches!(*self.state.borrow(), PipelineState::Running) {
            Ok(Response::ok().with_text("READY").into())
        } else {
            Ok(Response::new(503).with_text("NOT_READY").into())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::StageLifecycleEvent;
    use obzenflow_topology::{StageSubgraphMembership, TopologyBuilder, TopologySubgraphInfo};

    struct TestEndpoint {
        path: String,
        methods: Vec<HttpMethod>,
    }

    impl TestEndpoint {
        fn new(path: &str, methods: Vec<HttpMethod>) -> Self {
            Self {
                path: path.to_string(),
                methods,
            }
        }
    }

    #[async_trait]
    impl HttpEndpoint for TestEndpoint {
        fn path(&self) -> &str {
            &self.path
        }

        fn methods(&self) -> &[HttpMethod] {
            &self.methods
        }

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, EndpointError> {
            Ok(Response::ok().into())
        }
    }

    #[test]
    fn topology_builds_lifecycle_definitions_for_every_composite() {
        let mut builder = TopologyBuilder::new();
        let map = builder.add_stage(Some("map".to_string()));
        builder.reset_current();
        let finish = builder.add_stage(Some("finish".to_string()));
        builder.reset_current();
        builder.add_edge(map, finish);

        let topology = builder.build_unchecked().unwrap();
        let stages = topology
            .stages()
            .cloned()
            .map(|mut stage| {
                let (role, order, is_entry, is_exit) = if stage.id == map {
                    ("map", 0, true, false)
                } else {
                    ("finalize", 1, false, true)
                };
                stage.subgraph = Some(StageSubgraphMembership::new(
                    "ai_map_reduce:digest",
                    "ai_map_reduce",
                    "digest",
                    role,
                    order,
                    is_entry,
                    is_exit,
                ));
                stage
            })
            .collect();
        let subgraph = TopologySubgraphInfo::new(
            "ai_map_reduce:digest",
            "ai_map_reduce",
            "digest",
            "digest",
            vec![map, finish],
            Vec::new(),
            vec![map],
            vec![finish],
            false,
        );
        let topology = Topology::new_unvalidated(stages, topology.edges().to_vec())
            .unwrap()
            .with_subgraphs(vec![subgraph]);

        let definitions = composite_definitions_from_topology(&topology).unwrap();
        let mut projection = CompositeLifecycleProjection::new(definitions).unwrap();
        let map = StageId::from_ulid(map.ulid());
        let finish = StageId::from_ulid(finish.ulid());
        let composite = CompositeId::new("ai_map_reduce:digest");

        projection
            .apply(map, &StageLifecycleEvent::Running)
            .unwrap();
        projection
            .apply(map, &StageLifecycleEvent::Completed { metrics: None })
            .unwrap();
        projection
            .apply(finish, &StageLifecycleEvent::Completed { metrics: None })
            .unwrap();

        assert_eq!(
            projection.status(&composite),
            Some(obzenflow_core::composite::CompositeStatus::Completed)
        );
    }

    #[test]
    fn validate_extra_endpoints_rejects_reserved_paths() {
        let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
            Box::new(TestEndpoint::new("/metrics", vec![HttpMethod::Get])),
            Box::new(TestEndpoint::new(
                "/api/flow/control",
                vec![HttpMethod::Post],
            )),
        ];

        let err = validate_extra_endpoints(&endpoints).unwrap_err();
        match err {
            ManagedWebHostError::EndpointRegistrationFailed { path, .. } => {
                // Should fail on the first reserved path encountered.
                assert_eq!(path, "/metrics");
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_extra_endpoints_rejects_duplicate_routes_same_method() {
        let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
            Box::new(TestEndpoint::new("/foo", vec![HttpMethod::Post])),
            Box::new(TestEndpoint::new("/foo", vec![HttpMethod::Post])),
        ];

        let err = validate_extra_endpoints(&endpoints).unwrap_err();
        match err {
            ManagedWebHostError::EndpointRegistrationFailed { path, message } => {
                assert_eq!(path, "/foo");
                assert!(
                    message.contains("Duplicate route"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    #[test]
    fn validate_extra_endpoints_allows_same_path_different_methods() {
        let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
            Box::new(TestEndpoint::new("/foo", vec![HttpMethod::Get])),
            Box::new(TestEndpoint::new("/foo", vec![HttpMethod::Post])),
        ];

        validate_extra_endpoints(&endpoints).unwrap();
    }

    #[test]
    fn validate_extra_endpoints_treats_empty_methods_as_all_methods() {
        let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
            // Empty = supports all methods.
            Box::new(TestEndpoint::new("/foo", vec![])),
            Box::new(TestEndpoint::new("/foo", vec![HttpMethod::Get])),
        ];

        let err = validate_extra_endpoints(&endpoints).unwrap_err();
        match err {
            ManagedWebHostError::EndpointRegistrationFailed { path, message } => {
                assert_eq!(path, "/foo");
                assert!(
                    message.contains("Duplicate route"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }

    /// FLOWIP-010 gap 10: the auth gate (`is_control_plane_path`) and the
    /// reservation gate (`is_reserved_built_in_path`) must agree on every
    /// framework-owned route, or a route could be reachable without
    /// credentials while still reserved (or vice versa).
    #[cfg(feature = "warp-server")]
    #[test]
    fn control_plane_and_reservation_matchers_stay_in_sync() {
        use crate::web::warp::warp_server::is_control_plane_path;

        let control_plane_probes = [
            "/api/topology",
            "/metrics",
            "/api/flow",
            "/api/flow/control",
            "/api/flow/events",
            "/api/config",
            "/api/config/overlay",
            "/api/config/effective",
            "/api/config/schema",
            "/api/config/diff",
            "/api/config/flows/f1",
            "/api/config/flows/f1/stages/s1",
        ];
        for path in control_plane_probes {
            assert!(
                is_control_plane_path(path),
                "{path} must be behind control-plane auth"
            );
            assert!(
                is_reserved_built_in_path(path),
                "{path} must be reserved against attached surfaces"
            );
        }

        // Liveness endpoints are reserved but auth-exempt by design.
        for path in ["/health", "/ready"] {
            assert!(is_reserved_built_in_path(path));
            assert!(!is_control_plane_path(path));
        }

        // Attached-surface routes are neither reserved nor gated.
        for path in ["/api/ingest/events", "/custom"] {
            assert!(!is_reserved_built_in_path(path));
            assert!(!is_control_plane_path(path));
        }
    }
}
