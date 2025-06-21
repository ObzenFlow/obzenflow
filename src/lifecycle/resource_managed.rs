//! Resource management trait for components with external dependencies

use async_trait::async_trait;
use crate::step::Result;

/// Components that manage external resources (connections, files, etc.)
#[async_trait]
pub trait ResourceManaged: Send + Sync {
    /// Initialize resources before processing begins
    /// Called once during component startup
    async fn initialize(&mut self) -> Result<()> {
        Ok(()) // Default: no initialization needed
    }
    
    /// Check health of managed resources
    /// Used for monitoring and circuit breaking
    async fn health_check(&self) -> HealthStatus {
        HealthStatus::Healthy // Default: always healthy
    }
    
    /// Cleanup resources before shutdown
    /// Called during graceful shutdown
    async fn cleanup(&mut self) -> Result<()> {
        Ok(()) // Default: no cleanup needed
    }
    
    /// List managed resources for monitoring
    fn resources(&self) -> Vec<ResourceInfo> {
        vec![] // Default: no resources
    }
}

/// Health status of a component's resources
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HealthStatus {
    /// All resources are functioning normally
    Healthy,
    /// Some resources are degraded but functional
    Degraded(String),
    /// Resources are not functional
    Unhealthy(String),
}

/// Information about a managed resource
#[derive(Debug, Clone)]
pub struct ResourceInfo {
    /// Type of resource (e.g., "database", "file", "http_connection")
    pub resource_type: String,
    /// Unique identifier for the resource
    pub identifier: String,
    /// Current status of the resource
    pub status: HealthStatus,
}