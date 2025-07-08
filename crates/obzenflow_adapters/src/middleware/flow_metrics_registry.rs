//! Registry for flow-level metrics
//!
//! Provides centralized access to all flow metrics for Prometheus export.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use crate::middleware::flow_boundary::FlowMetrics;
use crate::monitoring::metrics::core::MetricSnapshot;

/// Flow identifier
type FlowId = String;

/// Central registry for all flow metrics
pub struct FlowMetricsRegistry {
    /// All registered flow metrics by flow ID
    flows: Arc<RwLock<HashMap<FlowId, Arc<FlowMetrics>>>>,
}

impl FlowMetricsRegistry {
    /// Create a new registry
    pub fn new() -> Self {
        Self {
            flows: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Register a flow's metrics
    pub async fn register_flow(&self, flow_id: impl Into<String>, metrics: Arc<FlowMetrics>) {
        let mut flows = self.flows.write().await;
        flows.insert(flow_id.into(), metrics);
    }
    
    /// Unregister a flow's metrics
    pub async fn unregister_flow(&self, flow_id: &str) {
        let mut flows = self.flows.write().await;
        flows.remove(flow_id);
    }
    
    /// Get metrics for a specific flow
    pub async fn get_flow_metrics(&self, flow_id: &str) -> Option<Arc<FlowMetrics>> {
        let flows = self.flows.read().await;
        flows.get(flow_id).cloned()
    }
    
    /// Export all flow metrics for Prometheus
    pub async fn export_all_metrics(&self) -> Vec<MetricSnapshot> {
        let flows = self.flows.read().await;
        let mut all_metrics = Vec::new();
        
        for (_flow_id, metrics) in flows.iter() {
            all_metrics.extend(metrics.export_metrics());
        }
        
        all_metrics
    }
    
    /// Get count of registered flows
    pub async fn flow_count(&self) -> usize {
        self.flows.read().await.len()
    }
    
    /// Clean up metrics for inactive flows
    pub async fn cleanup_inactive_flows(&self) {
        let mut flows = self.flows.write().await;
        
        // Remove flows with no active correlations and no recent activity
        // Note: This is simplified - in production would track activity better
        flows.retain(|_flow_id, _metrics| {
            true // Keep all for now
        });
    }
}

impl Default for FlowMetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Note: Global registry removed - should be managed by the application
// Example usage:
// let registry = Arc::new(FlowMetricsRegistry::new());
// Pass registry to components that need it

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_registry_operations() {
        let registry = FlowMetricsRegistry::new();
        
        // Register a flow
        let metrics = Arc::new(FlowMetrics::new("test_flow"));
        registry.register_flow("flow1", metrics.clone()).await;
        
        // Check it's registered
        assert_eq!(registry.flow_count().await, 1);
        assert!(registry.get_flow_metrics("flow1").await.is_some());
        
        // Export metrics
        let snapshots = registry.export_all_metrics().await;
        assert!(!snapshots.is_empty());
        
        // Unregister
        registry.unregister_flow("flow1").await;
        assert_eq!(registry.flow_count().await, 0);
    }
}