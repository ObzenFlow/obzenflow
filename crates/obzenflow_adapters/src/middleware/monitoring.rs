//! Monitoring middleware implementation
//! 
//! This module implements FLOWIP-050a: Monitoring as Middleware.
//! 
//! ## Key Components
//! 
//! - `MetricRecorder` - Trait for recording metrics from middleware
//! - `MonitoringMiddleware<T>` - Generic middleware for any taxonomy T
//! 
//! ## Example
//! 
//! Monitoring middleware is typically used via the taxonomy-specific `monitoring()` methods:
//! 
//! ```rust
//! use obzenflow_adapters::monitoring::taxonomies::red::RED;
//! use obzenflow_adapters::middleware::MiddlewareFactory;
//! 
//! // Get a monitoring middleware factory (used by stage descriptors)
//! let monitoring_factory = RED::monitoring();
//! ```
//! 
//! ## Direct Usage with Handlers
//! 
//! You can apply monitoring middleware to handlers using the extension traits:
//! 
//! ```rust
//! use obzenflow_adapters::middleware::{MonitoringMiddleware, TransformHandlerExt};
//! use obzenflow_adapters::monitoring::taxonomies::red::RED;
//! use obzenflow_runtime_services::control_plane::stages::handler_traits::TransformHandler;
//! use obzenflow_core::event::chain_event::ChainEvent;
//! use obzenflow_topology_services::stages::StageId;
//! 
//! struct MyProcessor;
//! 
//! impl TransformHandler for MyProcessor {
//!     fn process(&self, event: ChainEvent) -> Vec<ChainEvent> {
//!         vec![event]
//!     }
//! }
//! 
//! // Apply monitoring middleware (now requires stage_id)
//! let handler_with_monitoring = MyProcessor
//!     .middleware()
//!     .with(MonitoringMiddleware::<RED>::new("my_processor", StageId::new()))
//!     .build();
//! ```

use super::{Middleware, MiddlewareAction, MiddlewareContext};
use obzenflow_core::event::chain_event::ChainEvent;
use obzenflow_topology_services::stages::StageId;
use crate::monitoring::Taxonomy;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};
use dashmap::DashMap;
use serde_json;

/// Trait that all taxonomy metrics must implement for recording from middleware
pub trait MetricRecorder: Send + Sync {
    /// Called when an event enters the stage
    fn on_event_start(&self, event: &ChainEvent);
    
    /// Called when an event completes processing  
    fn on_event_complete(&self, event: &ChainEvent, results: &[ChainEvent], duration: Duration);
    
    /// Process raw middleware events to update metrics
    fn on_middleware_event(&self, source: &str, event_type: &str, data: &serde_json::Value);
    
    /// Optional: Called for queue depth updates (for saturation metrics)
    fn on_queue_depth(&self, _depth: usize) {}
    
    /// Export metrics in a standard format
    fn export_metrics(&self) -> Vec<MetricSnapshot>;
}

/// Monitoring middleware that records metrics for a specific taxonomy
pub struct MonitoringMiddleware<T: Taxonomy> {
    metrics: Arc<T::Metrics>,
    stage_name: String,
    start_times: Arc<DashMap<ulid::Ulid, Instant>>,
    _taxonomy: PhantomData<T>,
}

impl<T: Taxonomy> MonitoringMiddleware<T> {
    /// Create a new monitoring middleware for the given stage
    pub fn new(stage_name: impl Into<String>, stage_id: StageId) -> Self 
    where 
        T::Metrics: MetricRecorder,
    {
        let stage_name = stage_name.into();
        Self {
            metrics: Arc::new(T::create_metrics(&stage_name, stage_id)),
            stage_name,
            start_times: Arc::new(DashMap::new()),
            _taxonomy: PhantomData,
        }
    }
    
    /// Get a reference to the underlying metrics
    pub fn metrics(&self) -> &T::Metrics {
        &self.metrics
    }
    
    /// Get the stage name
    pub fn stage_name(&self) -> &str {
        &self.stage_name
    }
    
}

impl<T> Middleware for MonitoringMiddleware<T>
where
    T: Taxonomy,
    T::Metrics: MetricRecorder,
{
    fn pre_handle(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> MiddlewareAction {
        // Record the start time
        let start_time = Instant::now();
        
        // Store the start time for this event
        self.start_times.insert(event.id.as_ulid(), start_time);
        
        // Notify metrics that processing started
        self.metrics.on_event_start(event);
        
        // Emit monitoring event
        ctx.emit_event("monitoring", "event_started", serde_json::json!({
            "stage_name": self.stage_name,
            "event_id": event.id.as_str(),
            "taxonomy": std::any::type_name::<T>()
        }));
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, event: &ChainEvent, results: &[ChainEvent], ctx: &mut MiddlewareContext) {
        // Calculate duration from stored start time
        let duration = self.start_times
            .remove(&event.id.as_ulid())
            .map(|(_, start)| start.elapsed())
            .unwrap_or_else(|| Duration::from_millis(0));
        
        // First, observe all raw middleware events from operational middleware
        for mw_event in &ctx.events {
            self.metrics.on_middleware_event(&mw_event.source, &mw_event.event_type, &mw_event.data);
        }
        
        // Then record the completion
        self.metrics.on_event_complete(event, results, duration);
        
        // Emit monitoring event with metrics
        ctx.emit_event("monitoring", "event_completed", serde_json::json!({
            "stage_name": self.stage_name,
            "event_id": event.id.as_str(),
            "duration_ms": duration.as_millis(),
            "results_count": results.len(),
            "taxonomy": std::any::type_name::<T>()
        }));
    }
    
    fn on_error(&self, event: &ChainEvent, ctx: &mut MiddlewareContext) -> super::ErrorAction {
        // Clean up the stored start time on error
        let duration = self.start_times
            .remove(&event.id.as_ulid())
            .map(|(_, start)| start.elapsed())
            .unwrap_or_else(|| Duration::from_millis(0));
        
        // Emit error monitoring event
        ctx.emit_event("monitoring", "event_error", serde_json::json!({
            "stage_name": self.stage_name,
            "event_id": event.id.as_str(),
            "duration_ms": duration.as_millis(),
            "taxonomy": std::any::type_name::<T>()
        }));
        
        // Note: Error metrics are already recorded in post_handle by checking ProcessingOutcome
        
        super::ErrorAction::Propagate
    }
}

/// A placeholder for metric snapshots
/// You'll need to implement this based on your actual metrics infrastructure
#[derive(Debug, Clone)]
pub struct MetricSnapshot {
    pub name: String,
    pub value: f64,
    pub timestamp: Instant,
}

// Re-export for convenience
pub use crate::monitoring::{RED, USE, SAAFE, GoldenSignals};

// Import the metrics types we need
use crate::monitoring::taxonomies::{
    red::REDMetrics,
    use_taxonomy::USEMetrics,
    golden_signals::GoldenSignalsMetrics,
    saafe::SAAFEMetrics,
};

/// MetricRecorder implementation for RED taxonomy
impl MetricRecorder for REDMetrics {
    fn on_event_start(&self, _event: &ChainEvent) {
        // Rate is recorded on completion to avoid counting incomplete events
    }
    
    fn on_event_complete(&self, _event: &ChainEvent, results: &[ChainEvent], duration: Duration) {
        // Check if this was an error (no results = failure)
        if results.is_empty() {
            self.record_error(duration);
        } else {
            self.record_success(duration);
        }
    }
    
    fn on_middleware_event(&self, source: &str, event_type: &str, data: &serde_json::Value) {
        // Subscribe to raw events from operational middleware
        match (source, event_type) {
            // Circuit breaker events affect error rate
            ("circuit_breaker", "rejected") => {
                // Record as error with zero duration (rejected before processing)
                self.record_error(Duration::from_millis(0));
            }
            
            // Rate limiter events affect rate
            ("rate_limiter", "event_dropped") => {
                // Don't count dropped events in rate
            }
            
            // Retry events can affect all metrics
            ("retry", "attempt_failed") => {
                // Record error with whatever duration we have
                if let Some(duration_ms) = data.get("duration_ms").and_then(|v| v.as_u64()) {
                    self.record_error(Duration::from_millis(duration_ms));
                } else {
                    self.record_error(Duration::from_millis(0));
                }
            }
            ("retry", "exhausted") => {
                if let Some(duration_ms) = data.get("total_duration_ms").and_then(|v| v.as_u64()) {
                    self.record_error(Duration::from_millis(duration_ms));
                } else {
                    self.record_error(Duration::from_millis(0));
                }
            }
            
            // Timeout events are errors
            ("timeout", "timed_out") => {
                if let Some(duration_ms) = data.get("duration_ms").and_then(|v| v.as_u64()) {
                    self.record_error(Duration::from_millis(duration_ms));
                } else {
                    self.record_error(Duration::from_millis(0));
                }
            }
            
            _ => {}
        }
    }
    
    fn export_metrics(&self) -> Vec<MetricSnapshot> {
        // For now, return empty vec - we'll implement proper export later
        vec![]
    }
}

/// MetricRecorder implementation for USE taxonomy  
impl MetricRecorder for USEMetrics {
    fn on_event_start(&self, _event: &ChainEvent) {
        // Mark as working when processing starts
        self.start_work();
    }
    
    fn on_event_complete(&self, _event: &ChainEvent, results: &[ChainEvent], _duration: Duration) {
        // Mark as idle when processing completes
        self.end_work();
        
        // Check if this was an error (no results = failure)
        if results.is_empty() {
            self.record_error();
        }
    }
    
    fn on_middleware_event(&self, source: &str, event_type: &str, data: &serde_json::Value) {
        // Subscribe to raw events from operational middleware
        match (source, event_type) {
            // Circuit breaker events indicate saturation
            ("circuit_breaker", "rejected") => {
                self.record_error();
                // Circuit open indicates high saturation
                self.record_saturation(1000, 1000); // Max saturation
            }
            ("circuit_breaker", "opened") => {
                // Circuit opened = max saturation
                self.record_saturation(1000, 1000); // Max saturation
            }
            ("circuit_breaker", "closed") => {
                // Circuit closed = normal operation
                self.record_saturation(0, 1000); // No saturation
            }
            
            // Rate limiter events indicate saturation
            ("rate_limiter", "event_dropped") => {
                // Rate limiting = high saturation
                self.record_saturation(1000, 1000); // Max saturation
            }
            
            // Retry events indicate errors and affect utilization
            ("retry", "attempt_started") => {
                self.start_waiting(); // Waiting for retry delay
            }
            ("retry", "attempt_failed") => {
                self.record_error();
            }
            ("retry", "exhausted") => {
                self.record_error();
                self.end_work(); // Give up
            }
            
            // Queue depth events directly set saturation
            ("queue", "depth_update") => {
                if let (Some(depth), Some(max)) = (
                    data.get("current_depth").and_then(|v| v.as_u64()),
                    data.get("max_depth").and_then(|v| v.as_u64())
                ) {
                    self.record_saturation(depth as usize, max as usize);
                }
            }
            
            _ => {}
        }
    }
    
    fn on_queue_depth(&self, depth: usize) {
        // USE tracks saturation via queue depth
        // We need a max depth to calculate ratio - for now use 1000 as default
        self.record_saturation(depth, 1000);
    }
    
    fn export_metrics(&self) -> Vec<MetricSnapshot> {
        vec![]
    }
}

/// MetricRecorder implementation for Golden Signals
impl MetricRecorder for GoldenSignalsMetrics {
    fn on_event_start(&self, _event: &ChainEvent) {
        // Traffic is recorded on completion to avoid counting incomplete events
    }
    
    fn on_event_complete(&self, _event: &ChainEvent, results: &[ChainEvent], duration: Duration) {
        // Check if this was an error (no results = failure)
        if results.is_empty() {
            self.record_error(duration);
        } else {
            self.record_success(duration);
        }
    }
    
    fn on_middleware_event(&self, source: &str, event_type: &str, data: &serde_json::Value) {
        // Golden Signals tracks Latency, Traffic, Errors, and Saturation from raw events
        match (source, event_type) {
            // Circuit breaker events affect errors and saturation
            ("circuit_breaker", "rejected") => {
                self.record_error(Duration::from_millis(0)); // Rejected before processing
                self.record_saturation(1000, 1000); // Circuit open = max saturation
            }
            ("circuit_breaker", "opened") => {
                self.record_saturation(1000, 1000); // Max saturation
            }
            ("circuit_breaker", "closed") => {
                self.record_saturation(0, 1000); // Normal operation
            }
            
            // Rate limiter events affect traffic and saturation
            ("rate_limiter", "event_dropped") => {
                self.record_error(Duration::from_millis(0)); // Dropped event
                self.record_saturation(1000, 1000); // Rate limiting = saturation
            }
            
            // Retry events affect all signals
            ("retry", "attempt_started") => {
                if let Some(attempt) = data.get("attempt").and_then(|v| v.as_u64()) {
                    if attempt > 1 {
                        // Retries indicate errors
                        self.record_error(Duration::from_millis(0));
                    }
                }
            }
            ("retry", "exhausted") => {
                if let Some(duration_ms) = data.get("total_duration_ms").and_then(|v| v.as_u64()) {
                    self.record_error(Duration::from_millis(duration_ms));
                } else {
                    self.record_error(Duration::from_millis(0));
                }
            }
            
            // Timeout events are errors with known latency
            ("timeout", "timed_out") => {
                if let Some(timeout_ms) = data.get("timeout_ms").and_then(|v| v.as_u64()) {
                    self.record_error(Duration::from_millis(timeout_ms));
                } else {
                    self.record_error(Duration::from_millis(0));
                }
            }
            
            // Queue events directly indicate saturation
            ("queue", "depth_update") => {
                if let (Some(depth), Some(max)) = (
                    data.get("current_depth").and_then(|v| v.as_u64()),
                    data.get("max_depth").and_then(|v| v.as_u64())
                ) {
                    self.record_saturation(depth as usize, max as usize);
                }
            }
            
            _ => {}
        }
    }
    
    fn on_queue_depth(&self, depth: usize) {
        // Golden Signals tracks saturation
        self.record_saturation(depth, 1000); // Assuming max depth of 1000
    }
    
    fn export_metrics(&self) -> Vec<MetricSnapshot> {
        vec![]
    }
}

/// MetricRecorder implementation for SAAFE
impl MetricRecorder for SAAFEMetrics {
    fn on_event_start(&self, _event: &ChainEvent) {
        // SAAFE focuses on infrastructure metrics, not individual event timing
    }
    
    fn on_event_complete(&self, _event: &ChainEvent, results: &[ChainEvent], _duration: Duration) {
        // Check if this was an error (no results = failure)
        if results.is_empty() {
            self.record_error();
        }
    }
    
    fn on_middleware_event(&self, source: &str, event_type: &str, data: &serde_json::Value) {
        // SAAFE tracks Saturation, Amendments, Anomalies, Failures, and Errors
        match (source, event_type) {
            // Circuit breaker events indicate saturation and failures
            ("circuit_breaker", "rejected") => {
                self.record_error();
                self.record_saturation(1000, 1000); // Max saturation
                // Multiple rejections might indicate an anomaly
                if let Some(failures) = data.get("consecutive_failures").and_then(|v| v.as_u64()) {
                    if failures > 10 {
                        self.record_anomaly(0.8);
                    }
                }
            }
            ("circuit_breaker", "opened") => {
                self.record_failure(); // Circuit opening is a failure
                self.record_saturation(1000, 1000); // Max saturation
                self.record_amendment(crate::monitoring::metrics::AmendmentType::Stop); // Circuit open stops normal operation
            }
            ("circuit_breaker", "closed") => {
                self.record_saturation(0, 1000); // Normal operation
                self.record_amendment(crate::monitoring::metrics::AmendmentType::Start); // Circuit closed resumes operation
            }
            
            // Rate limiter events indicate saturation
            ("rate_limiter", "event_dropped") => {
                self.record_saturation(1000, 1000); // Max saturation
                // Frequent drops might be anomalous
                self.record_anomaly(0.3);
            }
            
            // Retry events indicate transient errors vs failures
            ("retry", "attempt_failed") => {
                self.record_error(); // Transient error
            }
            ("retry", "exhausted") => {
                self.record_failure(); // Persistent failure
                self.record_error();
            }
            ("retry", "succeeded_after_retry") => {
                // Recovery from error state
                if let Some(attempts) = data.get("attempts_needed").and_then(|v| v.as_u64()) {
                    if attempts > 2 {
                        // Multiple retries needed = potential anomaly
                        self.record_anomaly(0.5);
                    }
                }
            }
            
            // Timeout events are failures
            ("timeout", "timed_out") => {
                self.record_failure();
                self.record_error();
            }
            
            // Queue events directly indicate saturation
            ("queue", "depth_update") => {
                if let (Some(depth), Some(max)) = (
                    data.get("current_depth").and_then(|v| v.as_u64()),
                    data.get("max_depth").and_then(|v| v.as_u64())
                ) {
                    self.record_saturation(depth as usize, max as usize);
                    
                    // Queue at 90%+ capacity is anomalous
                    let ratio = depth as f64 / max as f64;
                    if ratio > 0.9 {
                        self.record_anomaly(ratio);
                    }
                }
            }
            
            // Lifecycle events are amendments
            ("lifecycle", event_type) => {
                match event_type {
                    "started" => self.record_amendment(crate::monitoring::metrics::AmendmentType::Start),
                    "stopped" => self.record_amendment(crate::monitoring::metrics::AmendmentType::Stop),
                    "restarted" => {
                        // Record restart as stop then start
                        self.record_amendment(crate::monitoring::metrics::AmendmentType::Stop);
                        self.record_amendment(crate::monitoring::metrics::AmendmentType::Start);
                    }
                    "config_updated" => {
                        // Config update is a stop/start cycle
                        self.record_amendment(crate::monitoring::metrics::AmendmentType::Stop);
                        self.record_amendment(crate::monitoring::metrics::AmendmentType::Start);
                    }
                    _ => {}
                }
            }
            
            _ => {}
        }
    }
    
    fn on_queue_depth(&self, depth: usize) {
        // SAAFE tracks saturation
        self.record_saturation(depth, 1000); // Assuming max depth of 1000
    }
    
    fn export_metrics(&self) -> Vec<MetricSnapshot> {
        vec![]
    }
}