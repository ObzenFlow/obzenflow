//! Web server factory for all flow endpoints
//!
//! Provides a simple way to start a web server with topology, metrics, and health endpoints

use obzenflow_core::web::{WebServer, ServerConfig, WebError};
use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::StageId;
use obzenflow_topology::Topology;
use std::sync::Arc;
use std::collections::HashMap;

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
    metrics_exporter: Option<Arc<dyn MetricsExporter>>,
    port: u16,
) -> Result<tokio::task::JoinHandle<()>, WebError> {
    use super::endpoints::topology::{StageMetadata, StageType, StageStatus};
    use super::endpoints::{TopologyHttpEndpoint, MetricsHttpEndpoint};
    
    let mut server = super::warp::WarpServer::new();
    
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
    )))?;
    
    // Add metrics endpoint if exporter available
    if let Some(ref metrics) = metrics_exporter {
        server.register_endpoint(Box::new(MetricsHttpEndpoint::new(metrics.clone())))?;
    }
    
    // Add health and ready endpoints (reuse from metrics_server)
    server.register_endpoint(Box::new(SimpleHealthEndpoint))?;
    server.register_endpoint(Box::new(SimpleReadyEndpoint))?;
    
    // Start server in background
    let handle = tokio::spawn(async move {
        let config = ServerConfig::localhost(port);
        if let Err(e) = server.start(config).await {
            tracing::error!("Web server failed: {}", e);
        }
    });
    
    // Log available endpoints
    tracing::info!("📊 Web server started on http://localhost:{}", port);
    tracing::info!("   /api/topology  - Flow structure");
    if metrics_exporter.is_some() {
        tracing::info!("   /metrics       - Prometheus metrics");
    }
    tracing::info!("   /health        - Health check");
    tracing::info!("   /ready         - Readiness check");
    
    Ok(handle)
}

// Reuse simple endpoints from metrics_server module
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Request, Response};
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