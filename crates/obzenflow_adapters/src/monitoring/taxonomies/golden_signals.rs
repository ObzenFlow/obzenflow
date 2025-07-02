use crate::monitoring::{Taxonomy, TaxonomyMetrics, MetricSnapshot, MetricUpdate};
use crate::monitoring::metrics::{RateMetric, ErrorMetric, DurationMetric, SaturationMetric};
use crate::monitoring::metrics::duration::DurationBuckets;
use crate::middleware::{Middleware, MiddlewareFactory};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_topology_services::stages::StageId;
use tokio::sync::broadcast;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Golden Signals (Latency, Traffic, Errors, Saturation) metrics implementation
pub struct GoldenSignalsMetrics {
    stage_name: String,
    stage_id: StageId,
    traffic: Arc<RateMetric>, // All requests (success + error)
    errors: Arc<ErrorMetric>,
    latency: Arc<DurationMetric>,
    saturation: Arc<SaturationMetric>,
    update_tx: broadcast::Sender<MetricUpdate>,
}

impl GoldenSignalsMetrics {
    pub fn new(stage_name: &str, stage_id: StageId) -> Self {
        let traffic = Arc::new(RateMetric::new(format!("{}_traffic", stage_name)));
        let errors = Arc::new(ErrorMetric::new(format!("{}_errors", stage_name)));
        let latency = Arc::new(DurationMetric::with_buckets(
            format!("{}_latency", stage_name),
            DurationBuckets::Milliseconds
        ));
        let saturation = Arc::new(SaturationMetric::new(format!("{}_saturation", stage_name)));
        
        let (tx, _) = broadcast::channel(256);
        
        Self {
            stage_name: stage_name.to_string(),
            stage_id,
            traffic,
            errors,
            latency,
            saturation,
            update_tx: tx,
        }
    }
    
    /// Record a successful request
    pub fn record_success(&self, duration: Duration) {
        self.traffic.record_event();
        self.latency.record_duration(duration);
        self.broadcast_update();
    }
    
    /// Record a failed request
    pub fn record_error(&self, duration: Duration) {
        self.traffic.record_event();
        self.errors.record_error();
        self.latency.record_duration(duration);
        self.broadcast_update();
    }
    
    /// Record saturation (queue depth)
    pub fn record_saturation(&self, queue_depth: usize, max_depth: usize) {
        if max_depth > 0 {
            let ratio = queue_depth as f64 / max_depth as f64;
            self.saturation.set_ratio(ratio);
        }
        
        let _ = self.update_tx.send(MetricUpdate::Saturation {
            value: if max_depth > 0 { queue_depth as f64 / max_depth as f64 } else { 0.0 },
            queue_depth,
            timestamp: Instant::now(),
            stage: self.stage_name.clone(),
        });
    }
    
    /// Start a timer for latency tracking
    pub fn start_timer(&self) -> crate::monitoring::Timer {
        crate::monitoring::Timer::start()
    }
    
    fn broadcast_update(&self) {
        let _ = self.update_tx.send(MetricUpdate::Rate {
            value: 0.0, // Would calculate actual rate
            timestamp: Instant::now(),
            stage: self.stage_name.clone(),
        });
    }
}

impl TaxonomyMetrics for GoldenSignalsMetrics {
    fn current_values(&self) -> MetricSnapshot {
        let total_traffic = self.traffic.total_events();
        let total_errors = self.errors.total_errors();
        let error_rate = if total_traffic > 0 {
            total_errors as f64 / total_traffic as f64
        } else {
            0.0
        };
        
        MetricSnapshot {
            timestamp: Instant::now(),
            traffic_per_sec: total_traffic as f64, // Raw counter - exporters calculate rate
            error_count: total_errors,
            error_rate,
            latency_p99: Duration::from_millis(0), // TODO: Get from latency metric
            saturation: self.saturation.current_ratio(),
            ..Default::default()
        }
    }
    
    fn subscribe_updates(&self) -> broadcast::Receiver<MetricUpdate> {
        self.update_tx.subscribe()
    }
    
    fn export_prometheus(&self) {
        // Metrics auto-register with Prometheus on creation
    }
    
    fn taxonomy_name(&self) -> &'static str {
        GoldenSignals::NAME
    }
}
/// Golden Signals taxonomy definition
pub struct GoldenSignals;

/// Factory for creating GoldenSignals monitoring middleware with stage context
pub struct GoldenSignalsMonitoringFactory;

impl MiddlewareFactory for GoldenSignalsMonitoringFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(crate::middleware::MonitoringMiddleware::<GoldenSignals>::new(
            config.name.clone(),
            config.stage_id,
        ))
    }
    
    fn name(&self) -> &str {
        "GoldenSignals::monitoring"
    }
}

impl GoldenSignals {
    /// Create monitoring middleware factory for this taxonomy
    pub fn monitoring() -> Box<dyn MiddlewareFactory> {
        Box::new(GoldenSignalsMonitoringFactory)
    }
}

impl Taxonomy for GoldenSignals {
    const NAME: &'static str = "GoldenSignals";
    const DESCRIPTION: &'static str = "Latency, Traffic, Errors, Saturation - Google's SRE approach";
    
    type Metrics = GoldenSignalsMetrics;
    
    fn create_metrics(stage_name: &str, stage_id: StageId) -> Self::Metrics {
        GoldenSignalsMetrics::new(stage_name, stage_id)
    }
}