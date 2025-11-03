//! Spring Boot-style application framework for ObzenFlow
//!
//! Provides automatic lifecycle management for flows including:
//! - CLI argument parsing
//! - HTTP server management
//! - Graceful shutdown handling

use super::{ApplicationError, FlowConfig};
use clap::Parser;
use obzenflow_runtime_services::prelude::FlowHandle;
use std::future::Future;
use tokio::task::JoinHandle;

/// The main application framework for running ObzenFlow flows
/// 
/// This provides a Spring Boot-style experience where users just call
/// `FlowApplication::run()` with their flow and the framework handles everything:
/// - CLI parsing (--server, --server-port)
/// - Server startup if requested
/// - Flow execution
/// - Graceful shutdown
/// 
/// # Example
/// ```ignore
/// use obzenflow_infra::application::FlowApplication;
/// use obzenflow_dsl_infra::flow;
/// 
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     FlowApplication::run(flow! {
///         name: "my_flow",
///         // ... flow definition
///     }).await?;
///     Ok(())
/// }
/// ```
pub struct FlowApplication;

impl FlowApplication {
    /// Run a flow with automatic lifecycle management
    /// 
    /// This is the only public method users need to call. It:
    /// 1. Parses CLI arguments automatically
    /// 2. Builds the flow from the provided future
    /// 3. Starts HTTP server if --server flag is present
    /// 4. Runs the flow to completion
    /// 5. Manages server lifecycle after flow completes
    /// 
    /// # Arguments
    /// * `flow_future` - A future that builds and returns a FlowHandle
    /// 
    /// # Returns
    /// * `Ok(())` if flow completes successfully
    /// * `Err(ApplicationError)` if flow fails or cannot start
    pub async fn run<F, E>(flow_future: F) -> Result<(), ApplicationError>
    where
        F: Future<Output = Result<FlowHandle, E>>,
        E: std::fmt::Display,
    {
        // Parse CLI arguments automatically (like Spring Boot)
        let config = FlowConfig::parse();
        
        // Initialize logging
        tracing_subscriber::fmt()
            .with_max_level(tracing::Level::INFO)
            .init();
        
        tracing::info!("🚀 Starting FlowApplication");
        
        // Build the flow (this executes the flow! macro)
        let flow_handle = flow_future.await
            .map_err(|e| ApplicationError::FlowBuildFailed(e.to_string()))?;
        
        // Start server if --server flag present
        let server_handle = if config.server {
            Self::start_server(&flow_handle, config.server_port).await?
        } else {
            None
        };
        
        // Run the flow to completion
        tracing::info!("▶️  Starting flow execution");
        flow_handle.run().await
            .map_err(|e| ApplicationError::FlowExecutionFailed(e.to_string()))?;
        
        // Flow completed - handle post-completion
        if let Some(_handle) = server_handle {
            // Server was started, keep it running
            tracing::info!("✅ Flow completed successfully");
            tracing::info!("📊 Server still running on port {}", config.server_port);
            tracing::info!("⏸️  Press Ctrl+C to stop server...");
            
            // Wait for Ctrl+C
            tokio::signal::ctrl_c().await?;
            tracing::info!("👋 Shutting down server");
        } else {
            // No server, just exit
            tracing::info!("✅ Flow completed successfully");
        }
        
        Ok(())
    }
    
    /// Internal: Start the web server with all endpoints
    async fn start_server(
        flow_handle: &FlowHandle, 
        port: u16
    ) -> Result<Option<JoinHandle<()>>, ApplicationError> {
        #[cfg(feature = "warp-server")]
        {
            use crate::web::start_web_server;
            
            // Every flow has a topology - it's required to run
            let topology = flow_handle.topology()
                .ok_or_else(|| ApplicationError::ServerStartFailed(
                    "Flow missing topology - this should never happen".to_string()
                ))?;
            let metrics = flow_handle.metrics_exporter();
            let has_metrics = metrics.is_some();

            let flow_name = flow_handle.flow_name().to_string();

            let handle = start_web_server(topology, flow_name, metrics, port).await
                .map_err(|e| ApplicationError::ServerStartFailed(e.to_string()))?;
            
            tracing::info!("📊 Web server started on http://localhost:{}", port);
            tracing::info!("   /api/topology  - Flow structure");
            if has_metrics {
                tracing::info!("   /metrics       - Prometheus metrics");
            }
            tracing::info!("   /health        - Health status");
            tracing::info!("   /ready         - Readiness status");
            
            Ok(Some(handle))
        }
        
        #[cfg(not(feature = "warp-server"))]
        {
            tracing::warn!("⚠️  --server flag requires warp-server feature");
            tracing::warn!("   Recompile with --features obzenflow_infra/warp-server");
            tracing::warn!("");
            tracing::warn!("   Continuing without HTTP server...");
            // Don't fail, just continue without server
            Ok(None)
        }
    }
}