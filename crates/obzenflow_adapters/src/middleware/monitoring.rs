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
//! ```rust
//! use flowstate::middleware::{MonitoringMiddleware, StepExt};
//! use flowstate::monitoring::RED;
//! 
//! let step = MyProcessor::new()
//!     .middleware()
//!     .with(MonitoringMiddleware::<RED>::new("my_processor"))
//!     .build();
//! ```

use super::{Middleware, MiddlewareAction};
use obzenflow_core::event::chain_event::ChainEvent;
use crate::monitoring::Taxonomy;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

/// Trait that all taxonomy metrics must implement for recording from middleware
pub trait MetricRecorder: Send + Sync {
    /// Called when an event enters the stage
    fn on_event_start(&self, event: &ChainEvent);
    
    /// Called when an event completes processing  
    fn on_event_complete(&self, event: &ChainEvent, results: &[ChainEvent], duration: Duration);
    
    /// Optional: Called for queue depth updates (for saturation metrics)
    fn on_queue_depth(&self, _depth: usize) {}
    
    /// Export metrics in a standard format
    fn export_metrics(&self) -> Vec<MetricSnapshot>;
}

/// Monitoring middleware that records metrics for a specific taxonomy
pub struct MonitoringMiddleware<T: Taxonomy> {
    metrics: T::Metrics,
    stage_name: String,
    _taxonomy: PhantomData<T>,
}

impl<T: Taxonomy> MonitoringMiddleware<T> {
    /// Create a new monitoring middleware for the given stage
    pub fn new(stage_name: impl Into<String>) -> Self 
    where 
        T::Metrics: MetricRecorder,
    {
        let stage_name = stage_name.into();
        Self {
            metrics: T::create_metrics(&stage_name),
            stage_name,
            _taxonomy: PhantomData,
        }
    }
    
    /// Get a reference to the underlying metrics
    pub fn metrics(&self) -> &T::Metrics {
        &self.metrics
    }
}

// We need a way to track timing between pre and post handle.
// For now, we'll use a thread-local approach.
thread_local! {
    static EVENT_START_TIMES: std::cell::RefCell<std::collections::HashMap<ulid::Ulid, Instant>> = 
        std::cell::RefCell::new(std::collections::HashMap::new());
}

impl<T> Middleware for MonitoringMiddleware<T>
where
    T: Taxonomy,
    T::Metrics: MetricRecorder,
{
    fn pre_handle(&self, event: &ChainEvent) -> MiddlewareAction {
        // Record the start time
        let start_time = Instant::now();
        
        // Store the start time for this event
        EVENT_START_TIMES.with(|times| {
            times.borrow_mut().insert(event.id.as_ulid(), start_time);
        });
        
        // Notify metrics that processing started
        self.metrics.on_event_start(event);
        
        MiddlewareAction::Continue
    }
    
    fn post_handle(&self, event: &ChainEvent, results: &mut Vec<ChainEvent>) {
        // Calculate duration from stored start time
        let duration = EVENT_START_TIMES.with(|times| {
            times.borrow_mut()
                .remove(&event.id.as_ulid())
                .map(|start| start.elapsed())
                .unwrap_or_else(|| Duration::from_millis(1))
        });
        
        self.metrics.on_event_complete(event, results, duration);
    }
    
    fn on_error(&self, event: &ChainEvent, _error: &super::StepError) -> super::ErrorAction {
        // Clean up the stored start time on error
        EVENT_START_TIMES.with(|times| {
            times.borrow_mut().remove(&event.id.as_ulid());
        });
        
        // For now, just propagate the error
        // In the future, we might want to record error metrics here too
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
use obzenflow_core::event::processing_outcome::ProcessingOutcome;

/// MetricRecorder implementation for RED taxonomy
impl MetricRecorder for REDMetrics {
    fn on_event_start(&self, _event: &ChainEvent) {
        // RED doesn't need to track anything at start
        // We'll record rate when the event completes
    }
    
    fn on_event_complete(&self, event: &ChainEvent, _results: &[ChainEvent], duration: Duration) {
        // Check the processing outcome
        match &event.processing_info.outcome {
            ProcessingOutcome::Error(_) => {
                self.record_error(duration);
            }
            _ => {
                self.record_success(duration);
            }
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
        // Mark that we're starting work (affects utilization)
        self.start_work();
    }
    
    fn on_event_complete(&self, event: &ChainEvent, _results: &[ChainEvent], _duration: Duration) {
        // Mark work as complete
        self.end_work();
        
        // Check for errors
        if let ProcessingOutcome::Error(_) = &event.processing_info.outcome {
            self.record_error();
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
        // Golden Signals tracks traffic at completion with duration
    }
    
    fn on_event_complete(&self, event: &ChainEvent, _results: &[ChainEvent], duration: Duration) {
        // Check for errors
        match &event.processing_info.outcome {
            ProcessingOutcome::Error(_) => {
                self.record_error(duration);
            }
            _ => {
                self.record_success(duration);
            }
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
        // SAAFE might track various things at start
        // For now, just track that an event started
    }
    
    fn on_event_complete(&self, event: &ChainEvent, _results: &[ChainEvent], _duration: Duration) {
        // Check outcome for various SAAFE categories
        match &event.processing_info.outcome {
            ProcessingOutcome::Success => {
                // Success - no anomalies to record
            }
            ProcessingOutcome::Error(_) => {
                // This is a failure
                self.record_failure();
                self.record_error();
            }
            ProcessingOutcome::Filtered => {
                // Track as an anomaly - using severity 0.3 for skipped
                self.record_anomaly(0.3);
            }
            ProcessingOutcome::Retry { .. } => {
                // Retry is not a failure yet, just track as anomaly
                self.record_anomaly(0.5);
            }
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