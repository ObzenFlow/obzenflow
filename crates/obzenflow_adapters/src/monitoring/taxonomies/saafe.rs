use crate::monitoring::{Taxonomy, TaxonomyMetrics, MetricSnapshot, MetricUpdate};
use crate::monitoring::metrics::{
    SaturationMetric, AmendmentMetric, AnomalyMetric, FailureMetric, ErrorMetric
};
use tokio::sync::broadcast;
use std::sync::Arc;
use std::time::Instant;

/// SAAFE (Saturation, Amendments, Anomalies, Failures, Errors) metrics implementation
/// 
/// Designed for comprehensive infrastructure monitoring with focus on
/// hardware and system-level metrics.
pub struct SAAFEMetrics {
    stage_name: String,
    saturation: Arc<SaturationMetric>,
    amendments: Arc<AmendmentMetric>,
    anomalies: Arc<AnomalyMetric>,
    failures: Arc<FailureMetric>,
    errors: Arc<ErrorMetric>,
    update_tx: broadcast::Sender<MetricUpdate>,
}

impl SAAFEMetrics {
    pub fn new(stage_name: &str) -> Self {
        let saturation = Arc::new(SaturationMetric::new(format!("{}_saturation", stage_name)));
        let amendments = Arc::new(AmendmentMetric::new(format!("{}_amendments", stage_name)));
        let anomalies = Arc::new(AnomalyMetric::new(format!("{}_anomalies", stage_name)));
        let failures = Arc::new(FailureMetric::new(format!("{}_failures", stage_name)));
        let errors = Arc::new(ErrorMetric::new(format!("{}_errors", stage_name)));
        
        let (tx, _) = broadcast::channel(256);
        
        // Record lifecycle start
        amendments.record_start();
        
        Self {
            stage_name: stage_name.to_string(),
            saturation,
            amendments,
            anomalies,
            failures,
            errors,
            update_tx: tx,
        }
    }
    
    /// Record resource saturation
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
    
    /// Record an amendment (lifecycle event)
    pub fn record_amendment(&self, amendment_type: crate::monitoring::metrics::AmendmentType) {
        self.amendments.record(amendment_type);
    }
    
    /// Record an anomaly
    pub fn record_anomaly(&self, severity: f64) {
        self.anomalies.record_anomaly_with_severity(severity);
    }
    
    /// Record a failure
    pub fn record_failure(&self) {
        self.failures.record_failure();
    }
    
    /// Record an error
    pub fn record_error(&self) {
        self.errors.record_error();
    }
}

impl Drop for SAAFEMetrics {
    fn drop(&mut self) {
        // Record lifecycle stop when metrics are dropped
        self.amendments.record_stop();
    }
}

impl TaxonomyMetrics for SAAFEMetrics {
    fn current_values(&self) -> MetricSnapshot {
        MetricSnapshot {
            timestamp: Instant::now(),
            saturation: 0.0, // TODO: Get from saturation metric
            amendments: 0, // TODO: Get from amendments metric
            anomalies: 0, // TODO: Get from anomalies metric
            failures: 0, // TODO: Get from failures metric
            error_count: 0, // TODO: Get from error metric
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
        SAAFE::NAME
    }
}

/// SAAFE taxonomy definition
pub struct SAAFE;

impl SAAFE {
    /// Create monitoring middleware for this taxonomy
    pub fn monitoring() -> Box<dyn crate::middleware::Middleware> {
        Box::new(crate::middleware::MonitoringMiddleware::<Self>::new(""))
    }
}

impl Taxonomy for SAAFE {
    const NAME: &'static str = "SAAFE";
    const DESCRIPTION: &'static str = "Saturation, Amendments, Anomalies, Failures, Errors - comprehensive infrastructure monitoring";
    
    type Metrics = SAAFEMetrics;
    
    fn create_metrics(stage_name: &str) -> Self::Metrics {
        SAAFEMetrics::new(stage_name)
    }
}