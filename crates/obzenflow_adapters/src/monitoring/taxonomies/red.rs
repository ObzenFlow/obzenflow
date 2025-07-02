use crate::monitoring::{Taxonomy, TaxonomyMetrics, MetricSnapshot, MetricUpdate};
use crate::monitoring::metrics::{RateMetric, ErrorMetric, DurationMetric};
use crate::monitoring::metrics::duration::DurationBuckets;
use crate::middleware::{Middleware, MiddlewareFactory};
use obzenflow_runtime_services::pipeline::config::StageConfig;
use obzenflow_topology_services::stages::StageId;
use tokio::sync::broadcast;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// RED (Rate, Errors, Duration) metrics implementation
pub struct REDMetrics {
    stage_name: String,
    stage_id: StageId,
    rate: Arc<RateMetric>,
    errors: Arc<ErrorMetric>,
    duration: Arc<DurationMetric>,
    update_tx: broadcast::Sender<MetricUpdate>,
}

impl REDMetrics {
    pub fn new(stage_name: &str, stage_id: StageId) -> Self {
        let rate = Arc::new(RateMetric::new(format!("{}_rate", stage_name)));
        let errors = Arc::new(ErrorMetric::new(format!("{}_errors", stage_name)));
        let duration = Arc::new(DurationMetric::with_buckets(
            format!("{}_duration", stage_name),
            DurationBuckets::Milliseconds
        ));
        
        let (tx, _) = broadcast::channel(256);
        
        Self {
            stage_name: stage_name.to_string(),
            stage_id,
            rate,
            errors,
            duration,
            update_tx: tx,
        }
    }
    
    /// Record a successful request
    pub fn record_success(&self, duration: Duration) {
        self.rate.record_event();
        self.duration.record_duration(duration);
        self.broadcast_update();
    }
    
    /// Record a failed request
    pub fn record_error(&self, duration: Duration) {
        self.rate.record_event();
        self.errors.record_error();
        self.duration.record_duration(duration);
        self.broadcast_update();
    }
    
    /// Start a timer for duration tracking
    pub fn start_timer(&self) -> crate::monitoring::Timer {
        crate::monitoring::Timer::start()
    }
    
    fn broadcast_update(&self) {
        let _ = self.update_tx.send(MetricUpdate::Rate {
            value: self.rate.total_events() as f64, // Raw counter value
            timestamp: Instant::now(),
            stage: self.stage_name.clone(),
        });
    }
}

impl TaxonomyMetrics for REDMetrics {
    fn current_values(&self) -> MetricSnapshot {
        let total_events = self.rate.total_events();
        let total_errors = self.errors.total_errors();
        let error_rate = if total_events > 0 {
            total_errors as f64 / total_events as f64
        } else {
            0.0
        };
        
        MetricSnapshot {
            timestamp: Instant::now(),
            rate_per_sec: total_events as f64, // Raw counter - exporters calculate rate
            error_count: total_errors,
            error_rate,
            duration_p50: Duration::from_millis(0), // Duration metric provides these
            duration_p99: Duration::from_millis(0),
            duration_p999: Duration::from_millis(0),
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
        RED::NAME
    }
}
/// RED taxonomy definition
pub struct RED;

/// Factory for creating RED monitoring middleware with stage context
pub struct RedMonitoringFactory;

impl MiddlewareFactory for RedMonitoringFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(crate::middleware::MonitoringMiddleware::<RED>::new(
            config.name.clone(),
            config.stage_id,
        ))
    }
    
    fn name(&self) -> &str {
        "RED::monitoring"
    }
}

impl RED {
    /// Create monitoring middleware factory for this taxonomy
    pub fn monitoring() -> Box<dyn MiddlewareFactory> {
        Box::new(RedMonitoringFactory)
    }
}

impl Taxonomy for RED {
    const NAME: &'static str = "RED";
    const DESCRIPTION: &'static str = "Rate, Errors, Duration - ideal for request/response systems";
    
    type Metrics = REDMetrics;
    
    fn create_metrics(stage_name: &str, stage_id: StageId) -> Self::Metrics {
        REDMetrics::new(stage_name, stage_id)
    }
}