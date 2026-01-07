//! Web server factory for all flow endpoints
//!
//! Provides a simple way to start a web server with topology, metrics, and health endpoints

use obzenflow_core::web::{HttpEndpoint, ServerConfig, WebError, WebServer};
use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use obzenflow_runtime_services::pipeline::FlowHandle;
use obzenflow_runtime_services::pipeline::fsm::PipelineState;

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
/// let handle = start_web_server(
///     flow_topology,
///     Some(metrics_exporter),
///     9090
/// ).await?;
/// ```
#[cfg(feature = "warp-server")]
pub async fn start_web_server(
    topology: Arc<Topology>,
    flow_name: String,
    middleware_stacks: Option<Arc<HashMap<StageId, obzenflow_runtime_services::pipeline::MiddlewareStackConfig>>>,
    contract_attachments: Option<Arc<HashMap<(StageId, StageId), Vec<String>>>>,
    join_metadata: Option<Arc<HashMap<StageId, obzenflow_runtime_services::pipeline::JoinMetadata>>>,
    metrics_exporter: Option<Arc<dyn MetricsExporter>>,
    flow_handle: Option<Arc<FlowHandle>>,
    extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    port: u16,
) -> Result<tokio::task::JoinHandle<()>, WebError> {
    start_web_server_with_config(
        topology,
        flow_name,
        middleware_stacks,
        contract_attachments,
        join_metadata,
        metrics_exporter,
        flow_handle,
        extra_endpoints,
        ServerConfig::localhost(port),
    )
    .await
}

/// Start a web server with all flow endpoints using an explicit `ServerConfig`.
#[cfg(feature = "warp-server")]
pub async fn start_web_server_with_config(
    topology: Arc<Topology>,
    flow_name: String,
    middleware_stacks: Option<Arc<HashMap<StageId, obzenflow_runtime_services::pipeline::MiddlewareStackConfig>>>,
    contract_attachments: Option<Arc<HashMap<(StageId, StageId), Vec<String>>>>,
    join_metadata: Option<Arc<HashMap<StageId, obzenflow_runtime_services::pipeline::JoinMetadata>>>,
    metrics_exporter: Option<Arc<dyn MetricsExporter>>,
    flow_handle: Option<Arc<FlowHandle>>,
    extra_endpoints: Vec<Box<dyn HttpEndpoint>>,
    server_config: ServerConfig,
) -> Result<tokio::task::JoinHandle<()>, WebError> {
    use super::endpoints::topology::{StageMetadata, StageType, StageStatus};
    use super::endpoints::{FlowControlEndpoint, MetricsHttpEndpoint, TopologyHttpEndpoint};
    
    let mut server = super::warp::WarpServer::new();
    let pipeline_ready = flow_handle.as_ref().map(|handle| {
        let ready = Arc::new(AtomicBool::new(false));
        let ready_for_task = ready.clone();
        let mut state_rx = handle.state_receiver();
        let _ = tokio::spawn(async move {
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
                stage_type: if topology.source_stages().iter().any(|s| *s == stage_info.id) {
                    StageType::Source
                } else if topology.sink_stages().iter().any(|s| *s == stage_info.id) {
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
use obzenflow_core::web::{HttpMethod, Request, Response};
use async_trait::async_trait;

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
    
    async fn handle(&self, _request: Request) -> Result<Response, WebError> {
        Ok(Response::ok().with_text("OK"))
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
    
    async fn handle(&self, _request: Request) -> Result<Response, WebError> {
        Ok(Response::ok().with_text("READY"))
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

    async fn handle(&self, _request: Request) -> Result<Response, WebError> {
        if self.ready.load(Ordering::Acquire) {
            Ok(Response::ok().with_text("READY"))
        } else {
            Ok(Response::new(503).with_text("NOT_READY"))
        }
    }
}
