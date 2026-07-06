// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Web server factory for all flow endpoints
//!
//! Provides a simple way to start a web server with topology, metrics, and health endpoints

use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ServerConfig, WebError, WebServer};
use obzenflow_core::StageId;
use obzenflow_runtime::pipeline::FlowHandle;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_topology::Topology;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use super::surface_metrics::HttpSurfaceMetricsCollector;
use super::RuntimeInstanceId;

pub type ContractAttachments = Arc<HashMap<(StageId, StageId), Vec<String>>>;

pub struct WebServerResources {
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
    pub metrics_exporter: Option<Arc<dyn MetricsExporter>>,
    pub flow_handle: Option<Arc<FlowHandle>>,
    pub extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    pub surface_metrics: Option<Arc<HttpSurfaceMetricsCollector>>,
    /// FLOWIP-010: the owned resolved snapshot; presence turns on the seven
    /// read-only `/api/config/*` routes.
    pub runtime_config: Option<Arc<obzenflow_runtime::runtime_config::ResolvedRuntimeConfig>>,
    /// FLOWIP-114d: per-process incarnation identity, stamped into the SSE
    /// bootstrap event for data-path generation detection.
    pub runtime_instance_id: Option<RuntimeInstanceId>,
    /// FLOWIP-114d gap 8: fires after the terminal pipeline state; the
    /// listener then closes gracefully and SSE producers end their streams.
    pub shutdown: Option<tokio::sync::watch::Receiver<bool>>,
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
///     contract_attachments: None,
///     metrics_exporter: Some(metrics_exporter),
///     flow_handle: None,
///     extra_endpoints: vec![],
///     surface_metrics: None,
///     runtime_config: None,
///     runtime_instance_id: None,
///     shutdown: None,
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
    use super::endpoints::topology::{StageMetadata, StageStatus};
    use super::endpoints::{FlowControlEndpoint, MetricsHttpEndpoint, TopologyHttpEndpoint};

    let WebServerResources {
        topology,
        contract_attachments,
        metrics_exporter,
        flow_handle,
        extra_endpoints,
        surface_metrics,
        runtime_config,
        runtime_instance_id,
        shutdown,
    } = resources;

    validate_extra_endpoints(&extra_endpoints)?;

    let mut server = super::warp::WarpServer::new();
    if let Some(collector) = surface_metrics {
        server.with_surface_metrics(collector);
    }
    if let Some(runtime_instance_id) = runtime_instance_id {
        server.with_runtime_instance_id(runtime_instance_id);
    }
    if let Some(shutdown) = shutdown {
        server.with_shutdown(shutdown);
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
    if let Some(snapshot) = runtime_config {
        use super::endpoints::{ConfigHttpEndpoint, ConfigReadModel, ConfigRoute};

        let flow_name = flow_handle
            .as_ref()
            .map(|handle| handle.flow_name().to_string())
            .unwrap_or_else(|| "unnamed_flow".to_string());
        let flow_id = flow_handle
            .as_ref()
            .and_then(|handle| handle.run_substrate().locator())
            .and_then(|locator| {
                locator
                    .path()
                    .file_name()
                    .map(|name| name.to_string_lossy().into_owned())
            })
            .unwrap_or_else(|| flow_name.clone());
        let flow_effective = flow_handle
            .as_ref()
            .and_then(|handle| handle.flow_effective_config().cloned());

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
