//! Flow boundary tracking middleware for FLOWIP-055
//! 
//! Tracks events at flow entry (source) and exit (sink) points to provide
//! true end-to-end latency and throughput metrics.

use crate::prelude::*;
use crate::middleware::{Middleware, MiddlewareAction, ErrorAction};
use crate::monitoring::MetricSnapshot;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;
use crate::event_types::FlowId;
use ulid::Ulid;

/// Simple event ID type for tracking
type EventId = String;

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
    
    // Pending requests (entry time by event ID)
    pending_requests: Arc<RwLock<HashMap<EventId, Instant>>>,
    
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
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            metrics,
            config,
        }
    }

    /// Check if an event is entering the flow
    pub fn is_flow_entry(&self, event: &ChainEvent) -> bool {
        // Check if it's from a source stage (no parent events)
        event.causality.parent_ids.is_empty()
    }
    
    /// Check if an event is exiting the flow
    pub fn is_flow_exit(&self, _event: &ChainEvent) -> bool {
        // For now, we'll need to track this differently
        // Since we don't have stage_type in the event
        // This will be determined by the stage that's processing it
        false
    }
    
    /// Record flow entry
    pub async fn record_entry(&self, event: &ChainEvent) {
        let event_id = event.ulid.to_string();
        
        // Start tracking this request
        {
            let mut requests = self.pending_requests.write().await;
            requests.insert(event_id.clone(), Instant::now());
            
            // Clean up old entries periodically
            if requests.len() > 10000 {
                self.cleanup_expired_entries(&mut requests);
            }
        }
        
        // Record entry metric
        self.metrics.on_event_start(event);
    }
    
    /// Record flow exit and calculate end-to-end metrics
    pub async fn record_exit(&self, event: &ChainEvent) {
        // Find the original entry event
        let origin_id = self.find_origin_event(event).await;
        
        if let Some(origin_id) = origin_id {
            let entry_time = {
                let mut requests = self.pending_requests.write().await;
                requests.remove(&origin_id)
            };
            
            if let Some(entry_time) = entry_time {
                let duration = entry_time.elapsed();
                
                // Record end-to-end metrics
                let results = vec![event.clone()];
                self.metrics.on_event_complete(event, &results, duration);
            }
        }
    }
    
    /// Trace back to find the original entry event
    async fn find_origin_event(&self, event: &ChainEvent) -> Option<EventId> {
        let event_id = event.ulid.to_string();
        
        let requests = self.pending_requests.read().await;
        
        // Check if this event itself is tracked
        if requests.contains_key(&event_id) {
            return Some(event_id);
        }
        
        // Follow causality chain if we have parents
        for parent_ulid in &event.causality.parent_ids {
            let parent_id = parent_ulid.to_string();
            if requests.contains_key(&parent_id) {
                return Some(parent_id);
            }
        }
        
        None
    }
    
    fn cleanup_expired_entries(&self, requests: &mut HashMap<EventId, Instant>) {
        let now = Instant::now();
        let timeout = self.config.timeout;
        
        requests.retain(|_, entry_time| {
            now.duration_since(*entry_time) < timeout
        });
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
    fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
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
    
    fn post_handle(&self, _event: &ChainEvent, results: &mut Vec<ChainEvent>) {
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
    
    fn on_error(&self, _event: &ChainEvent, _error: &crate::middleware::StepError) -> ErrorAction {
        // Could track failed requests here
        ErrorAction::Propagate
    }
}

/// Flow-specific metrics that focus on end-to-end performance
pub struct FlowMetrics {
    flow_name: String,
    // We'll use simple counters for now
    // In a full implementation, we'd have proper metrics
}

impl FlowMetrics {
    pub fn new(flow_name: &str) -> Self {
        Self {
            flow_name: flow_name.to_string(),
        }
    }
    
    pub fn on_event_start(&self, _event: &ChainEvent) {
        // Record flow entry
        // In full implementation: increment entry counter
    }
    
    pub fn on_event_complete(&self, _event: &ChainEvent, _results: &[ChainEvent], _duration: Duration) {
        // Record flow exit and latency
        // In full implementation: increment exit counter, record latency histogram
    }
    
    pub fn export_metrics(&self) -> Vec<MetricSnapshot> {
        // Export flow-level metrics
        vec![]
    }
}