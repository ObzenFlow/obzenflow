// src/step.rs
use serde::{Serialize, Deserialize};
use serde_json::Value;
use ulid::Ulid;
use chrono::{DateTime, Utc};
use async_trait::async_trait;
use tokio::sync::mpsc::{Receiver, Sender};
use std::error::Error;
use std::time::{Duration, Instant};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainEvent {
    pub ulid: String,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub payload: Value,
}

impl ChainEvent {
    pub fn new(event_type: &str, payload: Value) -> Self {
        ChainEvent {
            ulid: Ulid::new().to_string(),
            event_type: event_type.to_string(),
            timestamp: Utc::now(),
            payload,
        }
    }
}

pub type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

/// Core trait for pipeline steps that process events
#[async_trait]
pub trait PipelineStep: Send + Sync {
    /// Legacy batch processing (for backward compatibility)
    fn handle(&self, event: ChainEvent) -> Vec<ChainEvent> {
        vec![event] // Default passthrough
    }

    /// Streaming interface - this is the new primary interface
    async fn process_stream(
        &mut self,
        input: Receiver<ChainEvent>,
        output: Sender<ChainEvent>,
        metrics: StepMetrics,
    ) -> Result<()> {
        // Default implementation uses the batch interface
        let mut input = input;
        while let Some(event) = input.recv().await {
            let start = Instant::now();
            let results = self.handle(event);
            let duration = start.elapsed();

            metrics.record_processing_time(duration);
            metrics.increment_processed(1);

            for result in results {
                if output.send(result).await.is_err() {
                    metrics.increment_errors(1);
                    return Err("Output channel closed".into());
                }
                metrics.increment_emitted(1);
            }
        }
        Ok(())
    }

    /// Called before processing starts
    async fn initialize(&mut self) -> Result<()> {
        Ok(())
    }

    /// Called after processing completes
    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

/// Metrics collected per pipeline step
#[derive(Clone)]
pub struct StepMetrics {
    name: String,
    // We'll use atomic counters for lock-free updates
    processed: Arc<AtomicU64>,
    emitted: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    processing_time_us: Arc<AtomicU64>,

    // For percentile calculations
    latencies: Arc<Mutex<hdrhistogram::Histogram<u64>>>,
}

impl StepMetrics {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            processed: Arc::new(AtomicU64::new(0)),
            emitted: Arc::new(AtomicU64::new(0)),
            errors: Arc::new(AtomicU64::new(0)),
            processing_time_us: Arc::new(AtomicU64::new(0)),
            latencies: Arc::new(Mutex::new(
                hdrhistogram::Histogram::new(3).unwrap()
            )),
        }
    }

    pub fn record_processing_time(&self, duration: Duration) {
        let us = duration.as_micros() as u64;
        self.processing_time_us.fetch_add(us, Ordering::Relaxed);

        if let Ok(mut hist) = self.latencies.lock() {
            let _ = hist.record(us);
        }
    }

    pub fn increment_processed(&self, count: u64) {
        self.processed.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_emitted(&self, count: u64) {
        self.emitted.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_errors(&self, count: u64) {
        self.errors.fetch_add(count, Ordering::Relaxed);
    }

    /// Get a snapshot of current metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        let latencies = self.latencies.lock().unwrap();

        MetricsSnapshot {
            name: self.name.clone(),
            processed: self.processed.load(Ordering::Relaxed),
            emitted: self.emitted.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            total_processing_time_us: self.processing_time_us.load(Ordering::Relaxed),
            p50_latency_us: latencies.value_at_percentile(50.0),
            p95_latency_us: latencies.value_at_percentile(95.0),
            p99_latency_us: latencies.value_at_percentile(99.0),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    pub name: String,
    pub processed: u64,
    pub emitted: u64,
    pub errors: u64,
    pub total_processing_time_us: u64,
    pub p50_latency_us: u64,
    pub p95_latency_us: u64,
    pub p99_latency_us: u64,
}

impl MetricsSnapshot {
    pub fn throughput(&self, duration: Duration) -> f64 {
        self.processed as f64 / duration.as_secs_f64()
    }
}
