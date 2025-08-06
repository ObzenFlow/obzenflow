//! Simple metrics server factory
//!
//! Provides a one-line way to start a metrics server

use obzenflow_core::web::{WebServer, ServerConfig, HttpEndpoint, HttpMethod, Request, Response, WebError};
use obzenflow_core::metrics::MetricsExporter;
use std::sync::Arc;
use async_trait::async_trait;

/// Start a metrics web server with one line
/// 
/// # Example
/// ```ignore
/// use obzenflow_infra::web::start_metrics_server;
/// 
/// // Start server in background
/// start_metrics_server(metrics_exporter, 9090).await?;
/// ```
#[cfg(feature = "warp-server")]
pub async fn start_metrics_server(
    metrics_exporter: Arc<dyn MetricsExporter>,
    port: u16,
) -> Result<tokio::task::JoinHandle<()>, WebError> {
    let mut server = super::warp::WarpServer::new();
    
    // Add metrics endpoint
    server.register_endpoint(Box::new(MetricsHttpEndpoint::new(metrics_exporter.clone())))?;
    
    // Add health endpoint
    server.register_endpoint(Box::new(SimpleHealthEndpoint))?;
    
    // Add ready endpoint  
    server.register_endpoint(Box::new(SimpleReadyEndpoint))?;
    
    // Start server in background
    let handle = tokio::spawn(async move {
        let config = ServerConfig::localhost(port);
        let _ = server.start(config).await;
    });
    
    Ok(handle)
}

/// Internal adapter for MetricsExporter to HttpEndpoint
struct MetricsHttpEndpoint {
    exporter: Arc<dyn MetricsExporter>,
}

impl MetricsHttpEndpoint {
    fn new(exporter: Arc<dyn MetricsExporter>) -> Self {
        Self { exporter }
    }
}

#[async_trait]
impl HttpEndpoint for MetricsHttpEndpoint {
    fn path(&self) -> &str {
        "/metrics"
    }
    
    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }
    
    async fn handle(&self, _request: Request) -> Result<Response, WebError> {
        match self.exporter.render_metrics() {
            Ok(metrics) => Ok(Response::ok()
                .with_header("Content-Type".to_string(), "text/plain; version=0.0.4; charset=utf-8".to_string())
                .with_body(metrics.into_bytes())),
            Err(e) => Ok(Response::internal_error()
                .with_text(&format!("Failed to render metrics: {}", e))),
        }
    }
}

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