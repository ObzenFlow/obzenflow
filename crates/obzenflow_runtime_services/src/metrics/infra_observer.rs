//! Infrastructure metrics observer for journal and stage metrics
//!
//! This observer collects metrics that cannot go through the journal
//! (to avoid the observer paradox) such as journal write latency.

use obzenflow_core::metrics::{InfraMetricsSnapshot, JournalMetricsSnapshot, 
                              StageInfraMetrics, HistogramSnapshot};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use tokio::sync::RwLock;

/// Observer for infrastructure metrics that cannot go through journal
pub struct InfraMetricsObserver {
    /// Shared state for collecting metrics
    state: Arc<RwLock<InfraMetricsState>>,
}

/// Internal state for metrics collection
struct InfraMetricsState {
    /// Journal write metrics
    journal_writes_total: u64,
    journal_write_latencies: Vec<f64>, // microseconds
    journal_bytes_written: u64,
    
    /// Stage infrastructure metrics
    stage_queue_depths: HashMap<String, u64>,
    stage_in_flight: HashMap<String, u64>,
    
    /// Last snapshot time for rate calculations
    last_snapshot_time: Instant,
    last_writes_total: u64,
}

impl InfraMetricsObserver {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(InfraMetricsState {
                journal_writes_total: 0,
                journal_write_latencies: Vec::new(),
                journal_bytes_written: 0,
                stage_queue_depths: HashMap::new(),
                stage_in_flight: HashMap::new(),
                last_snapshot_time: Instant::now(),
                last_writes_total: 0,
            })),
        }
    }
    
    /// Record a journal write operation
    pub async fn record_journal_write(&self, latency: Duration, bytes: usize) {
        let mut state = self.state.write().await;
        state.journal_writes_total += 1;
        state.journal_write_latencies.push(latency.as_micros() as f64);
        state.journal_bytes_written += bytes as u64;
    }
    
    /// Update stage queue depth
    pub async fn update_queue_depth(&self, stage_name: String, depth: u64) {
        let mut state = self.state.write().await;
        state.stage_queue_depths.insert(stage_name, depth);
    }
    
    /// Update stage in-flight count
    pub async fn update_in_flight(&self, stage_name: String, count: u64) {
        let mut state = self.state.write().await;
        state.stage_in_flight.insert(stage_name, count);
    }
    
    /// Collect current snapshot of infrastructure metrics
    pub async fn collect_snapshot(&self) -> InfraMetricsSnapshot {
        let mut state = self.state.write().await;
        let now = Instant::now();
        
        // Calculate throughput
        let elapsed_secs = (now - state.last_snapshot_time).as_secs_f64();
        let writes_delta = state.journal_writes_total - state.last_writes_total;
        let throughput = if elapsed_secs > 0.0 {
            writes_delta as f64 / elapsed_secs
        } else {
            0.0
        };
        
        // Build latency histogram
        let latency_histogram = if !state.journal_write_latencies.is_empty() {
            let mut latencies = state.journal_write_latencies.clone();
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
            
            let count = latencies.len();
            let sum: f64 = latencies.iter().sum();
            
            let mut percentiles = HashMap::new();
            percentiles.insert("0.5".to_string(), latencies[count / 2]);
            percentiles.insert("0.9".to_string(), latencies[count * 9 / 10]);
            percentiles.insert("0.95".to_string(), latencies[count * 95 / 100]);
            percentiles.insert("0.99".to_string(), latencies[count * 99 / 100]);
            
            HistogramSnapshot {
                count: count as u64,
                sum,
                min: *latencies.first().unwrap(),
                max: *latencies.last().unwrap(),
                percentiles,
            }
        } else {
            HistogramSnapshot::default()
        };
        
        // Build stage metrics
        let mut stage_metrics = HashMap::new();
        for (stage_name, &queue_depth) in &state.stage_queue_depths {
            let in_flight = state.stage_in_flight.get(stage_name).copied().unwrap_or(0);
            stage_metrics.insert(
                stage_name.clone(),
                StageInfraMetrics {
                    queue_depth,
                    in_flight,
                }
            );
        }
        
        // Update state for next interval
        state.last_snapshot_time = now;
        state.last_writes_total = state.journal_writes_total;
        state.journal_write_latencies.clear();
        
        InfraMetricsSnapshot {
            timestamp: chrono::Utc::now(),
            journal_metrics: JournalMetricsSnapshot {
                writes_total: state.journal_writes_total,
                write_latency: latency_histogram,
                throughput,
                bytes_written: state.journal_bytes_written,
            },
            stage_metrics,
        }
    }
}