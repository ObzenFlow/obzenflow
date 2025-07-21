//! FSM instrumentation for HandlerSupervised stages

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::RwLock;
use std::time::{Duration, Instant};
use hdrhistogram::Histogram;
use serde::{Serialize, Deserialize};

use super::constants::{
    HISTOGRAM_MIN_MS, 
    HISTOGRAM_MAX_MS, 
    HISTOGRAM_SIGFIGS,
    QUANTILE_P50,
    QUANTILE_P90,
    QUANTILE_P95,
    QUANTILE_P99,
    QUANTILE_P999,
};

/// Runtime context snapshot injected into events
/// Following Honeycomb's wide events philosophy - contains accurate point-in-time metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeContext {
    // Gauge snapshots - current values
    pub in_flight: u32,
    
    // Histogram percentiles - pre-computed for efficiency
    pub recent_p50_ms: u64,
    pub recent_p90_ms: u64,
    pub recent_p95_ms: u64,
    pub recent_p99_ms: u64,
    pub recent_p999_ms: u64,
    
    // Counter snapshots - raw totals (Prometheus computes rates)
    pub events_processed_total: u64,
    pub errors_total: u64,
    pub failures_total: u64,
    
    // FSM state
    pub fsm_state: String,
    pub time_in_state_ms: u64,
    
    // Event loop metrics (cumulative)
    pub event_loops_total: u64,
    pub event_loops_with_work_total: u64,
}

/// Configuration for stage instrumentation
#[derive(Debug, Clone)]
pub struct InstrumentationConfig {
    pub enable_histograms: bool,
    pub enable_utilization: bool,
    pub enable_anomaly_detection: bool,
}

impl Default for InstrumentationConfig {
    fn default() -> Self {
        Self {
            enable_histograms: true,
            enable_utilization: true,
            enable_anomaly_detection: true,
        }
    }
}

/// Stage instrumentation that tracks metrics alongside FSM state
pub struct StageInstrumentation {
    // Gauge metrics - current values
    pub in_flight_count: AtomicU32,
    
    // Counter metrics - monotonic, let Prometheus compute rates
    pub events_processed_total: AtomicU64,
    pub errors_total: AtomicU64,
    pub failures_total: AtomicU64,          // Critical failures
    pub event_loops_total: AtomicU64,       // Total event loop iterations
    pub event_loops_with_work_total: AtomicU64,  // Loops that had work
    pub anomalies_total: AtomicU64,         // Outliers detected
    pub amendments_total: AtomicU64,        // Config changes
    
    // Histogram for processing time (percentiles)
    pub processing_time_histogram: RwLock<Histogram<u64>>,
    
    // FSM state tracking
    pub current_state: RwLock<String>,
    pub state_entered_at: RwLock<Instant>,
    
    // Configuration
    config: InstrumentationConfig,
}

impl StageInstrumentation {
    pub fn new() -> Self {
        Self::new_with_config(InstrumentationConfig::default())
    }
    
    pub fn new_with_config(config: InstrumentationConfig) -> Self {
        Self {
            // Gauges
            in_flight_count: AtomicU32::new(0),
            
            // Counters
            events_processed_total: AtomicU64::new(0),
            errors_total: AtomicU64::new(0),
            failures_total: AtomicU64::new(0),
            event_loops_total: AtomicU64::new(0),
            event_loops_with_work_total: AtomicU64::new(0),
            anomalies_total: AtomicU64::new(0),
            amendments_total: AtomicU64::new(0),
            
            // Histogram
            processing_time_histogram: RwLock::new(
                Histogram::new_with_bounds(HISTOGRAM_MIN_MS, HISTOGRAM_MAX_MS, HISTOGRAM_SIGFIGS)
                    .expect("Failed to create histogram")
            ),
            
            // State
            current_state: RwLock::new("Created".to_string()),
            state_entered_at: RwLock::new(Instant::now()),
            
            config,
        }
    }
    
    /// Create a snapshot for event injection
    pub fn snapshot(&self) -> obzenflow_core::event::runtime_context::RuntimeContext {
        let histogram = self.processing_time_histogram.read().unwrap();
        
        obzenflow_core::event::runtime_context::RuntimeContext {
            // Gauge snapshots
            in_flight: self.in_flight_count.load(Ordering::Relaxed),
            
            // Histogram percentiles
            recent_p50_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P50)
            } else { 0 },
            recent_p90_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P90)
            } else { 0 },
            recent_p95_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P95)
            } else { 0 },
            recent_p99_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P99)
            } else { 0 },
            recent_p999_ms: if self.config.enable_histograms {
                histogram.value_at_quantile(QUANTILE_P999)
            } else { 0 },
            
            // Counter snapshots (totals, not rates!)
            events_processed_total: self.events_processed_total.load(Ordering::Relaxed),
            errors_total: self.errors_total.load(Ordering::Relaxed),
            failures_total: self.failures_total.load(Ordering::Relaxed),
            
            // FSM state
            fsm_state: self.current_state.read().unwrap().clone(),
            time_in_state_ms: self.state_entered_at.read().unwrap().elapsed().as_millis() as u64,
            
            // Event loop metrics
            event_loops_total: self.event_loops_total.load(Ordering::Relaxed),
            event_loops_with_work_total: self.event_loops_with_work_total.load(Ordering::Relaxed),
        }
    }
    
    /// Record processing duration in histogram
    pub fn record_processing_time(&self, duration: Duration) {
        if !self.config.enable_histograms {
            return;
        }
        
        let duration_ms = duration.as_millis() as u64;
        let clamped = duration_ms.max(HISTOGRAM_MIN_MS).min(HISTOGRAM_MAX_MS);
        
        if let Ok(mut histogram) = self.processing_time_histogram.write() {
            histogram.record(clamped)
                .unwrap_or_else(|e| tracing::warn!("Failed to record duration: {:?}", e));
        }
    }
    
    /// Track FSM state transition
    pub fn transition_to_state(&self, new_state: &str) {
        *self.current_state.write().unwrap() = new_state.to_string();
        *self.state_entered_at.write().unwrap() = Instant::now();
    }
    
    /// Check if a duration is an anomaly (outlier)
    pub fn check_anomaly(&self, duration: Duration) -> bool {
        if !self.config.enable_anomaly_detection {
            return false;
        }
        
        let duration_ms = duration.as_millis() as u64;
        let histogram = self.processing_time_histogram.read().unwrap();
        let p99 = histogram.value_at_quantile(QUANTILE_P99);
        
        // Consider it an anomaly if it's more than 3x the p99
        duration_ms > p99 * 3
    }
    
    /// Get utilization percentage (0-100)
    pub fn utilization_percentage(&self) -> f64 {
        if !self.config.enable_utilization {
            return 0.0;
        }
        
        let total_loops = self.event_loops_total.load(Ordering::Relaxed);
        let loops_with_work = self.event_loops_with_work_total.load(Ordering::Relaxed);
        
        if total_loops == 0 {
            0.0
        } else {
            (loops_with_work as f64 / total_loops as f64) * 100.0
        }
    }
}

/// Higher-order function for instrumented event processing
use std::future::Future;
use std::error::Error;
use std::sync::Arc;

pub async fn process_with_instrumentation<T, F, Fut>(
    instrumentation: &Arc<StageInstrumentation>,
    f: F,
) -> Result<T, Box<dyn Error + Send + Sync>>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, Box<dyn Error + Send + Sync>>>,
{
    // Track in-flight
    instrumentation.in_flight_count.fetch_add(1, Ordering::Relaxed);
    
    // Process with timing
    let start = Instant::now();
    let result = f().await;
    let duration = start.elapsed();
    
    // Update metrics
    instrumentation.in_flight_count.fetch_sub(1, Ordering::Relaxed);
    
    // Record processing time
    instrumentation.record_processing_time(duration);
    
    // Check for anomalies
    if instrumentation.check_anomaly(duration) {
        instrumentation.anomalies_total.fetch_add(1, Ordering::Relaxed);
    }
    
    // Track success/error
    match &result {
        Ok(_) => {
            instrumentation.events_processed_total.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            instrumentation.errors_total.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    result
}