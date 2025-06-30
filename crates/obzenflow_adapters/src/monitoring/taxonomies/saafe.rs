use crate::monitoring::{Taxonomy, TaxonomyMetrics, MetricSnapshot, MetricUpdate};
use crate::monitoring::metrics::{
    SaturationMetric, AmendmentMetric, AnomalyMetric, FailureMetric, ErrorMetric
};
use crate::middleware::{Middleware, MiddlewareFactory};
use obzenflow_runtime_services::control_plane::stages::supervisors::config::StageConfig;
use obzenflow_topology_services::stages::StageId;
use tokio::sync::broadcast;
use std::sync::Arc;
use std::time::Instant;

/// SAAFE (Saturation, Amendments, Anomalies, Failures, Errors) metrics implementation
/// 
/// Designed for comprehensive infrastructure monitoring with focus on
/// hardware and system-level metrics.
pub struct SAAFEMetrics {
    stage_name: String,
    stage_id: StageId,
    saturation: Arc<SaturationMetric>,
    amendments: Arc<AmendmentMetric>,
    anomalies: Arc<AnomalyMetric>,
    failures: Arc<FailureMetric>,
    errors: Arc<ErrorMetric>,
    update_tx: broadcast::Sender<MetricUpdate>,
}

impl SAAFEMetrics {
    pub fn new(stage_name: &str, stage_id: StageId) -> Self {
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
            stage_id,
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
            saturation: self.saturation.current_ratio(),
            amendments: self.amendments.total_amendments(),
            anomalies: self.anomalies.anomaly_count(),
            failures: self.failures.total_failures(),
            error_count: self.errors.total_errors(),
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

/// Factory for creating SAAFE monitoring middleware with stage context
pub struct SaafeMonitoringFactory;

impl MiddlewareFactory for SaafeMonitoringFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(crate::middleware::MonitoringMiddleware::<SAAFE>::new(
            config.stage_name.clone(),
            config.stage_id,
        ))
    }
    
    fn name(&self) -> &str {
        "SAAFE::monitoring"
    }
}

impl SAAFE {
    /// Create monitoring middleware factory for this taxonomy
    pub fn monitoring() -> Box<dyn MiddlewareFactory> {
        Box::new(SaafeMonitoringFactory)
    }
}

impl Taxonomy for SAAFE {
    const NAME: &'static str = "SAAFE";
    const DESCRIPTION: &'static str = "Saturation, Amendments, Anomalies, Failures, Errors - comprehensive infrastructure monitoring";
    
    type Metrics = SAAFEMetrics;
    
    fn create_metrics(stage_name: &str, stage_id: StageId) -> Self::Metrics {
        SAAFEMetrics::new(stage_name, stage_id)
    }
}