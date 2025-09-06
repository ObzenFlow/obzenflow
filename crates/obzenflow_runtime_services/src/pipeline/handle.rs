use super::fsm::{PipelineEvent, PipelineState};
use crate::errors::FlowError;
use crate::supervised_base::{SupervisorHandle, HandleError, StandardHandle};
use obzenflow_topology::Topology;
use std::sync::Arc;

/// Flow handle for external control - the public API returned by the DSL
/// 
/// This is a wrapper that combines:
/// - A standard handle for FSM control (event sending, state watching, lifecycle)
/// - Pipeline-specific functionality (metrics export)
/// 
/// This is the only supervisor handle that gets exposed to DSL users,
/// so it needs to provide all functionality they might need.
pub struct FlowHandle {
    /// The standard handle for FSM control
    handle: StandardHandle<PipelineEvent, PipelineState>,
    
    /// Pipeline-specific: Metrics access (read-only)
    metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
    
    /// Flow topology for visualization (read-only)
    topology: Option<Arc<Topology>>,
}

impl FlowHandle {
    /// Create a new flow handle from a standard handle and extras
    pub(crate) fn new(
        handle: StandardHandle<PipelineEvent, PipelineState>,
        metrics_exporter: Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>,
        topology: Option<Arc<Topology>>,
    ) -> Self {
        Self {
            handle,
            metrics_exporter,
            topology,
        }
    }
    
    /// Run the pipeline and wait for completion
    /// This is the primary method users should call after creating a flow
    pub async fn run(self) -> Result<(), FlowError> {
        // Send the Run event to transition from Materialized to Running
        // This will trigger NotifySourceStart action
        self.send_event(PipelineEvent::Run).await?;
        
        // Now wait for it to complete
        self.wait_for_completion().await
    }
    
    /// Run the pipeline and wait for completion, returning the metrics exporter
    /// Use this when you need to access metrics after the flow completes
    /// Typically used with finite sources (not infinite sources)
    pub async fn run_with_metrics(self) -> Result<Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>>, FlowError> {
        // Send the Run event to transition from Materialized to Running
        // This will trigger NotifySourceStart action
        self.send_event(PipelineEvent::Run).await?;
        
        // Save metrics exporter before consuming self
        let metrics = self.metrics_exporter.clone();
        
        // Now wait for it to complete
        self.wait_for_completion().await?;
        
        Ok(metrics)
    }
    
    /// Graceful shutdown by sending Shutdown event to FSM
    pub async fn shutdown(&self) -> Result<(), FlowError> {
        self.send_event(PipelineEvent::Shutdown).await
    }
    
    /// Force shutdown by sending Error event to FSM
    pub async fn abort(&self, reason: &str) -> Result<(), FlowError> {
        self.send_event(PipelineEvent::Error {
            message: format!("Force abort: {}", reason),
        }).await
    }
    
    /// Check if the pipeline is still running
    pub fn is_running(&self) -> bool {
        self.handle.is_running()
    }
    
    /// Get a receiver for watching state changes
    pub fn state_receiver(&self) -> tokio::sync::watch::Receiver<PipelineState> {
        self.handle.state_receiver()
    }
    
    /// Get the metrics exporter for concurrent access during flow execution
    /// 
    /// This allows starting a metrics server before running the flow,
    /// enabling real-time monitoring of long-running flows.
    /// The exporter is thread-safe and can be accessed concurrently.
    pub fn metrics_exporter(&self) -> Option<Arc<dyn obzenflow_core::metrics::MetricsExporter>> {
        self.metrics_exporter.clone()
    }
    
    /// Get the flow topology for visualization
    /// 
    /// This provides access to the flow's structure (stages and connections)
    /// for visualization tools and monitoring dashboards.
    /// The topology is immutable and thread-safe.
    pub fn topology(&self) -> Option<Arc<Topology>> {
        self.topology.clone()
    }
    
    /// Render metrics based on the wrapped exporter's format
    pub async fn render_metrics(&self) -> Result<String, FlowError> {
        if let Some(ref exporter) = self.metrics_exporter {
            exporter.render_metrics().map_err(|e| {
                FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                )))
            })
        } else {
            Err(FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "No metrics exporter configured",
            ))))
        }
    }
}

// Custom implementation for SupervisorHandle trait to use FlowError
#[async_trait::async_trait]
impl SupervisorHandle for FlowHandle {
    type Event = PipelineEvent;
    type State = PipelineState;
    type Error = FlowError;
    
    async fn send_event(&self, event: Self::Event) -> Result<(), Self::Error> {
        self.handle.send_event(event).await
            .map_err(|e| match e {
                HandleError::SupervisorNotRunning => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Pipeline supervisor is not running",
                    )))
                }
                HandleError::SupervisorFailed(msg) => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        msg,
                    )))
                }
                HandleError::SupervisorPanicked(msg) => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Task panicked: {}", msg),
                    )))
                }
                _ => FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))),
            })
    }
    
    fn current_state(&self) -> Self::State {
        self.handle.current_state()
    }
    
    async fn wait_for_completion(self) -> Result<(), Self::Error> {
        self.handle.wait_for_completion().await
            .map_err(|e| match e {
                HandleError::SupervisorNotRunning => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "Pipeline supervisor is not running",
                    )))
                }
                HandleError::SupervisorFailed(msg) => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        msg,
                    )))
                }
                HandleError::SupervisorPanicked(msg) => {
                    FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Task panicked: {}", msg),
                    )))
                }
                _ => FlowError::ExecutionFailed(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    e.to_string(),
                ))),
            })
    }
}