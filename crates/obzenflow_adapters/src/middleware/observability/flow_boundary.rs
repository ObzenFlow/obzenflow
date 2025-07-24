//! Flow boundary tracking middleware for FLOWIP-055
//!
//! Tracks events at flow entry (source) and exit (sink) points to provide
//! true end-to-end latency and throughput metrics.

use obzenflow_core::event::chain_event::ChainEvent;
use crate::middleware::{Middleware, MiddlewareAction, ErrorAction, MiddlewareContext};
use crate::monitoring::metrics::core::{
    Metric,
    MetricType, 
    MetricValue, 
    MetricSnapshot
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;
use obzenflow_core::event::context::StageType;

/// Flow ID is just a string for now
type FlowId = String;

/// Type of boundary crossing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryType {
    /// Event entering the flow (at source)
    Entry,
    /// Event exiting the flow (at sink)
    Exit,
}

/// Configuration for boundary tracking
#[derive(Debug, Clone)]
pub struct BoundaryConfig {
    /// Maximum time to track a request before considering it lost
    pub timeout: Duration,
    /// Whether to track individual request traces
    pub trace_requests: bool,
    /// Sampling rate for detailed tracing (0.0 to 1.0)
    pub trace_sample_rate: f64,
}

impl Default for BoundaryConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300), // 5 minutes
            trace_requests: false,
            trace_sample_rate: 0.01, // 1% sampling
        }
    }
}

/// Tracks flow boundary crossings for end-to-end metrics
pub struct FlowBoundaryTracker {
    flow_id: FlowId,
    flow_name: String,

    // Metrics for the flow
    metrics: Arc<FlowMetrics>,

    // Configuration
    config: BoundaryConfig,
}

impl FlowBoundaryTracker {
    pub fn new(
        flow_id: FlowId,
        flow_name: String,
        metrics: Arc<FlowMetrics>,
        config: BoundaryConfig,
    ) -> Self {
        Self {
            flow_id,
            flow_name,
            metrics,
            config,
        }
    }

    /// Check if an event is entering the flow
    pub fn is_flow_entry(&self, event: &ChainEvent) -> bool {
        // Entry events are from source stages
        (event.flow_context.stage_type == StageType::FiniteSource) |
            (event.flow_context.stage_type == StageType::InfiniteSource)
    }

    /// Check if an event is exiting the flow
    pub fn is_flow_exit(&self, event: &ChainEvent) -> bool {
        // Exit events are at sink stages
        event.flow_context.stage_type == obzenflow_core::event::context::StageType::Sink
    }

    /// Record flow entry
    pub async fn record_entry(&self, event: &ChainEvent) {
        // Record entry metric - new correlation-based tracking
        self.metrics.on_event_start(event);
    }

    /// Record flow exit and calculate end-to-end metrics
    pub async fn record_exit(&self, event: &ChainEvent) {
        // Record exit metric - correlation-based latency calculation
        self.metrics.on_event_complete(event, &[], Duration::default());
    }

}

/// Middleware that tracks flow boundaries
pub struct BoundaryTrackingMiddleware {
    tracker: Arc<FlowBoundaryTracker>,
    boundary_type: BoundaryType,
}

impl BoundaryTrackingMiddleware {
    pub fn entry(tracker: Arc<FlowBoundaryTracker>) -> Self {
        Self {
            tracker,
            boundary_type: BoundaryType::Entry,
        }
    }

    pub fn exit(tracker: Arc<FlowBoundaryTracker>) -> Self {
        Self {
            tracker,
            boundary_type: BoundaryType::Exit,
        }
    }
}

impl Middleware for BoundaryTrackingMiddleware {
    fn pre_handle(&self, event: &ChainEvent, _ctx: &mut MiddlewareContext) -> MiddlewareAction {
        if self.boundary_type == BoundaryType::Entry && self.tracker.is_flow_entry(event) {
            // Clone for async task
            let tracker = self.tracker.clone();
            let event = event.clone();
            tokio::spawn(async move {
                tracker.record_entry(&event).await;
            });
        }
        MiddlewareAction::Continue
    }

    fn post_handle(&self, _event: &ChainEvent, results: &[ChainEvent], _ctx: &mut MiddlewareContext) {
        if self.boundary_type == BoundaryType::Exit {
            // For exit tracking, we track all results
            for result in results.iter() {
                let tracker = self.tracker.clone();
                let result = result.clone();
                tokio::spawn(async move {
                    tracker.record_exit(&result).await;
                });
            }
        }
    }

    fn on_error(&self, _event: &ChainEvent, _ctx: &mut MiddlewareContext) -> ErrorAction {
        // Could track failed requests here
        ErrorAction::Propagate
    }
}

use crate::monitoring::metrics::{
    rate::RateMetric,
    errors::ErrorMetric,
    duration::DurationMetric,
};
use obzenflow_core::event::CorrelationId;

/// Flow-specific metrics that focus on end-to-end performance
pub struct FlowMetrics {
    flow_name: String,
    
    // Core metrics
    entry_counter: RateMetric,
    exit_counter: RateMetric,
    latency_metric: DurationMetric,
    error_counter: ErrorMetric,
    
    // Track active correlations: CorrelationId → entry_time
    active_correlations: Arc<RwLock<HashMap<CorrelationId, Instant>>>,
    
    // Timeout for cleaning up lost correlations
    correlation_timeout: Duration,
}

impl FlowMetrics {
    pub fn new(flow_name: &str) -> Self {
        Self {
            flow_name: flow_name.to_string(),
            entry_counter: RateMetric::new(format!("{}_entries", flow_name)),
            exit_counter: RateMetric::new(format!("{}_exits", flow_name)),
            latency_metric: DurationMetric::new(format!("{}_latency", flow_name)),
            error_counter: ErrorMetric::new(format!("{}_errors", flow_name)),
            active_correlations: Arc::new(RwLock::new(HashMap::new())),
            correlation_timeout: Duration::from_secs(300), // 5 minutes default
        }
    }

    pub fn on_event_start(&self, event: &ChainEvent) {
        // Only track events with correlation IDs (source events)
        if let Some(correlation_id) = &event.correlation_id {
            self.entry_counter.record_event();
            
            // Track when this correlation started
            let correlations = self.active_correlations.clone();
            let timeout = self.correlation_timeout;
            let correlation_id_copy = *correlation_id;
            tokio::spawn(async move {
                let mut correlations = correlations.write().await;
                correlations.insert(correlation_id_copy, Instant::now());
                
                // Clean up old correlations if map is getting large
                if correlations.len() > 10000 {
                    let now = Instant::now();
                    correlations.retain(|_, &mut start_time| {
                        now.duration_since(start_time) < timeout
                    });
                }
            });
        }
    }

    pub fn on_event_complete(&self, event: &ChainEvent, _results: &[ChainEvent], _stage_duration: Duration) {
        // Track events exiting at sinks
        if let Some(correlation_id) = &event.correlation_id {
            self.exit_counter.record_event();
            
            // Calculate end-to-end latency using correlation payload
            if let Some(latency) = event.correlation_latency() {
                self.latency_metric.record_duration(latency);
            }
            
            // Clean up tracking
            let correlations = self.active_correlations.clone();
            let correlation_id_copy = *correlation_id;
            tokio::spawn(async move {
                correlations.write().await.remove(&correlation_id_copy);
            });
        }
    }
    
    pub fn on_event_error(&self, event: &ChainEvent) {
        if event.correlation_id.is_some() {
            self.error_counter.record_error();
        }
    }
    
    /// Get count of correlations that haven't completed yet
    pub async fn active_correlation_count(&self) -> usize {
        self.active_correlations.read().await.len()
    }
    
    /// Clean up timed-out correlations and return count of timeouts
    pub async fn cleanup_timeouts(&self) -> usize {
        let mut correlations = self.active_correlations.write().await;
        let timeout = self.correlation_timeout;
        let now = Instant::now();
        let initial_count = correlations.len();
        
        correlations.retain(|_, &mut start_time| {
            now.duration_since(start_time) < timeout
        });
        
        initial_count - correlations.len()
    }

    pub fn export_metrics(&self) -> Vec<MetricSnapshot> {
        let mut snapshots = vec![];
        
        // Add flow-level labels
        let mut add_flow_labels = |mut snapshot: MetricSnapshot| -> MetricSnapshot {
            snapshot.labels.insert("level".to_string(), "flow".to_string());
            snapshot.labels.insert("flow_name".to_string(), self.flow_name.clone());
            snapshot
        };
        
        // Export all metrics with flow labels
        snapshots.push(add_flow_labels(self.entry_counter.snapshot()));
        snapshots.push(add_flow_labels(self.exit_counter.snapshot()));
        snapshots.push(add_flow_labels(self.latency_metric.snapshot()));
        snapshots.push(add_flow_labels(self.error_counter.snapshot()));
        
        // Add a gauge for active correlations
        // Note: We can't easily get async count in sync context, so we'll skip for now
        // In production, this would be handled by a background task that updates a cached value
        snapshots.push(MetricSnapshot {
            name: format!("{}_active_correlations", self.flow_name),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(0.0), // TODO: Cache active count
            timestamp: Instant::now(),
            labels: {
                let mut labels = HashMap::new();
                labels.insert("level".to_string(), "flow".to_string());
                labels.insert("flow_name".to_string(), self.flow_name.clone());
                labels
            },
        });
        
        snapshots
    }
}
