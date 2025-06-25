//! Drainable trait for graceful shutdown coordination
//!
//! This trait provides a unified abstraction for components that need to
//! participate in graceful shutdown, regardless of whether they are stages,
//! observers, exporters, or other async components.

use async_trait::async_trait;
use std::time::Duration;
use obzenflow_core::Result;

/// Components that can be gracefully drained during shutdown
#[async_trait]
pub trait Drainable: Send + Sync {
    /// Get the unique identifier for this component
    fn id(&self) -> &str;

    /// Get the component type for debugging/logging
    fn component_type(&self) -> ComponentType;

    /// Signal that this component is ready to start processing
    /// Called after initialization but before processing begins
    async fn signal_ready(&self) -> Result<()>;

    /// Begin draining - stop accepting new work and process remaining
    /// Returns immediately after initiating drain
    async fn begin_drain(&mut self) -> Result<()>;

    /// Check if all pending work is complete
    fn is_drained(&self) -> bool;

    /// Get count of pending items (for monitoring)
    fn pending_count(&self) -> usize {
        0
    }

    /// Wait for drain to complete (with timeout)
    /// Returns true if drained, false if timeout
    async fn await_drained(&self, timeout: Duration) -> Result<bool> {
        let deadline = tokio::time::Instant::now() + timeout;

        while !self.is_drained() {
            if tokio::time::Instant::now() > deadline {
                return Ok(false);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        Ok(true)
    }

    /// Force immediate shutdown (may lose work)
    async fn force_shutdown(&mut self) -> Result<()>;
}

/// Type of drainable component for logging and debugging
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentType {
    /// Data processing stage (source, transform, sink)
    Stage,
    /// Flow-level observer (monitoring, logging)
    FlowObserver,
    /// Metrics exporter (Prometheus, StatsD)
    MetricsExporter,
    /// Event log writer
    LogWriter,
    /// Custom component
    Custom(&'static str),
}

impl std::fmt::Display for ComponentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ComponentType::Stage => write!(f, "Stage"),
            ComponentType::FlowObserver => write!(f, "FlowObserver"),
            ComponentType::MetricsExporter => write!(f, "MetricsExporter"),
            ComponentType::LogWriter => write!(f, "LogWriter"),
            ComponentType::Custom(name) => write!(f, "Custom({})", name),
        }
    }
}

/// State of a drainable component
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DrainState {
    /// Not yet initialized
    Uninitialized,
    /// Ready and processing
    Running,
    /// Draining remaining work
    Draining,
    /// All work complete
    Drained,
    /// Force shutdown
    Terminated,
}
