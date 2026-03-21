// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Web server factory for all flow endpoints
//!
//! Provides a simple way to start a web server with topology, metrics, and health endpoints

use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ServerConfig, WebError, WebServer};
use obzenflow_core::StageId;
use obzenflow_runtime::pipeline::fsm::PipelineState;
use obzenflow_runtime::pipeline::FlowHandle;
use obzenflow_topology::Topology;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::surface_metrics::HttpSurfaceMetricsCollector;

pub type MiddlewareStacks =
    Arc<HashMap<StageId, obzenflow_runtime::pipeline::MiddlewareStackConfig>>;
pub type ContractAttachments = Arc<HashMap<(StageId, StageId), Vec<String>>>;
pub type JoinMetadataMap = Arc<HashMap<StageId, obzenflow_runtime::pipeline::JoinMetadata>>;
pub type StageSubgraphMembershipMap =
    Arc<HashMap<StageId, obzenflow_core::topology::subgraphs::StageSubgraphMembership>>;
pub type SubgraphRegistry = Arc<Vec<obzenflow_core::topology::subgraphs::TopologySubgraphInfo>>;

pub struct WebServerResources {
    pub topology: Arc<Topology>,
    pub flow_name: String,
    pub middleware_stacks: Option<MiddlewareStacks>,
    pub contract_attachments: Option<ContractAttachments>,
    pub join_metadata: Option<JoinMetadataMap>,
    pub subgraph_membership: Option<StageSubgraphMembershipMap>,
    pub subgraphs: Option<SubgraphRegistry>,
    pub metrics_exporter: Option<Arc<dyn MetricsExporter>>,
    pub flow_handle: Option<Arc<FlowHandle>>,
    pub extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    pub surface_metrics: Option<Arc<HttpSurfaceMetricsCollector>>,
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
}

fn validate_extra_endpoints(extra_endpoints: &[Box<dyn HttpEndpoint>]) -> Result<(), WebError> {
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
            return Err(WebError::EndpointRegistrationFailed {
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
                return Err(WebError::EndpointRegistrationFailed {
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
            WebError::EndpointRegistrationFailed {
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
            return Err(WebError::EndpointRegistrationFailed {
                path: template.clone(),
                message,
            });
        }
    }

    Ok(())
}

/// Start a web server with all flow endpoints
///
/// This function creates a single server with all endpoints:
/// - `/api/topology` - Flow structure and stage information
/// - `/metrics` - Prometheus metrics (if metrics_exporter provided)
/// - `/health` - Health check endpoint
/// - `/ready` - Readiness check endpoint
///
/// # Example
/// ```ignore
/// use obzenflow_infra::web::start_web_server;
///
/// // Start server with topology and metrics
/// let handle = start_web_server(WebServerResources {
///     topology: flow_topology,
///     flow_name: "my_flow".to_string(),
///     middleware_stacks: None,
///     contract_attachments: None,
///     join_metadata: None,
///     subgraph_membership: None,
///     subgraphs: None,
///     metrics_exporter: Some(metrics_exporter),
///     flow_handle: None,
///     extra_endpoints: vec![],
///     surface_metrics: None,
/// }, 9090).await?;
/// ```
#[cfg(feature = "warp-server")]
pub async fn start_web_server(
    resources: WebServerResources,
    port: u16,
) -> Result<tokio::task::JoinHandle<()>, WebError> {
    start_web_server_with_config(resources, ServerConfig::localhost(port)).await
}

/// Start a web server with all flow endpoints using an explicit `ServerConfig`.
#[cfg(feature = "warp-server")]
pub async fn start_web_server_with_config(
    resources: WebServerResources,
    server_config: ServerConfig,
) -> Result<tokio::task::JoinHandle<()>, WebError> {
    use super::endpoints::topology::{StageMetadata, StageStatus, StageType};
    use super::endpoints::{FlowControlEndpoint, MetricsHttpEndpoint, TopologyHttpEndpoint};

    let WebServerResources {
        topology,
        flow_name,
        middleware_stacks,
        contract_attachments,
        join_metadata,
        subgraph_membership,
        subgraphs,
        metrics_exporter,
        flow_handle,
        extra_endpoints,
        surface_metrics,
    } = resources;

    validate_extra_endpoints(&extra_endpoints)?;

    let mut server = super::warp::WarpServer::new();
    if let Some(collector) = surface_metrics {
        server.with_surface_metrics(collector);
    }
    let pipeline_ready = flow_handle.as_ref().map(|handle| {
        let ready = Arc::new(AtomicBool::new(false));
        let ready_for_task = ready.clone();
        let mut state_rx = handle.state_receiver();
        tokio::spawn(async move {
            let initial_running = matches!(state_rx.borrow().clone(), PipelineState::Running);
            ready_for_task.store(initial_running, Ordering::Release);
            loop {
                if state_rx.changed().await.is_err() {
                    break;
                }
                let is_running = matches!(state_rx.borrow().clone(), PipelineState::Running);
                ready_for_task.store(is_running, Ordering::Release);
            }
        });
        ready
    });

    // Create stage metadata for topology endpoint
    let mut stages_metadata = HashMap::new();
    for stage_info in topology.stages() {
        // Convert topology StageId to core StageId for the HashMap key
        let core_stage_id = StageId::from_ulid(stage_info.id.ulid());
        stages_metadata.insert(
            core_stage_id,
            StageMetadata {
                stage_type: if topology.source_stages().contains(&stage_info.id) {
                    StageType::Source
                } else if topology.sink_stages().contains(&stage_info.id) {
                    StageType::Sink
                } else {
                    StageType::Transform
                },
                status: StageStatus::Pending,
            },
        );
    }

    // Always add topology endpoint
    server.register_endpoint(Box::new(TopologyHttpEndpoint::new(
        topology.clone(),
        Arc::new(stages_metadata),
        flow_name,
        middleware_stacks,
        contract_attachments,
        join_metadata,
        subgraph_membership,
        subgraphs,
    )))?;

    let has_metrics_endpoint = metrics_exporter.is_some();
    if let Some(exporter) = metrics_exporter {
        server.register_endpoint(Box::new(MetricsHttpEndpoint::new(exporter)))?;
    }

    // Add flow control endpoint if a handle is available
    if let Some(handle) = flow_handle {
        // Configure SSE system journal if available
        if let Some(journal) = handle.system_journal() {
            server.with_system_journal(journal);
        }
        server.register_endpoint(Box::new(FlowControlEndpoint::new(handle)))?;
    }

    for endpoint in extra_endpoints {
        server.register_endpoint(endpoint)?;
    }

    // Add health and ready endpoints (reuse from metrics_server)
    server.register_endpoint(Box::new(SimpleHealthEndpoint))?;
    if let Some(pipeline_ready) = pipeline_ready {
        server.register_endpoint(Box::new(PipelineReadyEndpoint::new(pipeline_ready)))?;
    } else {
        server.register_endpoint(Box::new(SimpleReadyEndpoint))?;
    }

    // Start server in background
    let addr = server_config.address();
    let handle = tokio::spawn(async move {
        if let Err(e) = server.start(server_config).await {
            tracing::error!("Web server failed: {}", e);
        }
    });

    // Log available endpoints
    tracing::info!("📊 Web server started on http://{}", addr);
    tracing::info!("   /api/topology  - Flow structure");
    if has_metrics_endpoint {
        tracing::info!("   /metrics       - Prometheus metrics");
    }
    tracing::info!("   /health        - Health check");
    tracing::info!("   /ready         - Readiness check");

    Ok(handle)
}

// Reuse simple endpoints from metrics_server module
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

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
        Ok(Response::ok().with_text("OK").into())
    }
}

/// Simple ready endpoint
struct SimpleReadyEndpoint;

#[async_trait]
impl HttpEndpoint for SimpleReadyEndpoint {
    fn path(&self) -> &str {
        "/ready"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
        Ok(Response::ok().with_text("READY").into())
    }
}

/// Pipeline readiness endpoint.
///
/// If a FlowHandle is available, this reflects pipeline state (Running => 200, otherwise 503).
struct PipelineReadyEndpoint {
    ready: Arc<AtomicBool>,
}

impl PipelineReadyEndpoint {
    fn new(ready: Arc<AtomicBool>) -> Self {
        Self { ready }
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

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
        if self.ready.load(Ordering::Acquire) {
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

        async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
            Ok(Response::ok().into())
        }
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
            WebError::EndpointRegistrationFailed { path, .. } => {
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
            WebError::EndpointRegistrationFailed { path, message } => {
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
            WebError::EndpointRegistrationFailed { path, message } => {
                assert_eq!(path, "/foo");
                assert!(
                    message.contains("Duplicate route"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("Unexpected error: {other:?}"),
        }
    }
}
