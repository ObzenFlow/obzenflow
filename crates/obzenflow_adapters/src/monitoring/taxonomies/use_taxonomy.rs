use crate::monitoring::{Taxonomy, TaxonomyMetrics, MetricSnapshot, MetricUpdate};
use crate::monitoring::metrics::{UtilizationMetric, SaturationMetric, ErrorMetric};
use crate::middleware::{Middleware, MiddlewareFactory};
use obzenflow_runtime_services::control_plane::stages::supervisors::config::StageConfig;
use obzenflow_topology_services::stages::StageId;
use tokio::sync::broadcast;
use std::sync::Arc;
use std::time::Instant;

/// USE (Utilization, Saturation, Errors) metrics implementation
pub struct USEMetrics {
    stage_name: String,
    stage_id: StageId,
    utilization: Arc<UtilizationMetric>,
    saturation: Arc<SaturationMetric>,
    errors: Arc<ErrorMetric>,
    update_tx: broadcast::Sender<MetricUpdate>,
}

impl USEMetrics {
    pub fn new(stage_name: &str, stage_id: StageId) -> Self {
        let utilization = Arc::new(UtilizationMetric::new(format!("{}_utilization", stage_name)));
        let saturation = Arc::new(SaturationMetric::new(format!("{}_saturation", stage_name)));
        let errors = Arc::new(ErrorMetric::new(format!("{}_errors", stage_name)));
        
        let (tx, _) = broadcast::channel(256);
        
        Self {
            stage_name: stage_name.to_string(),
            stage_id,
            utilization,
            saturation,
            errors,
            update_tx: tx,
        }
    }
    
    /// Start recording work (binary utilization = 1.0)
    pub fn start_work(&self) {
        self.utilization.start_work();
        self.broadcast_update();
    }
    
    /// Start recording wait (binary utilization = 0.0)
    pub fn start_waiting(&self) {
        self.utilization.start_waiting();
        self.broadcast_update();
    }
    
    /// End current work and go idle
    pub fn end_work(&self) {
        self.utilization.end_work();
        self.broadcast_update();
    }
    
    /// Get current utilization ratio (0.0 or 1.0 for binary utilization)
    pub fn current_utilization(&self) -> f64 {
        self.utilization.current_utilization()
    }
    
    /// Check if currently working
    pub fn is_working(&self) -> bool {
        self.utilization.is_working()
    }
    
    /// Check if currently waiting
    pub fn is_waiting(&self) -> bool {
        self.utilization.is_waiting()
    }
    
    /// Record queue saturation
    pub fn record_saturation(&self, queue_depth: usize, max_depth: usize) {
        if max_depth > 0 {
            let ratio = queue_depth as f64 / max_depth as f64;
            self.saturation.set_ratio(ratio);
        }
        
        // Broadcast saturation update for potential backpressure
        let _ = self.update_tx.send(MetricUpdate::Saturation {
            value: if max_depth > 0 { queue_depth as f64 / max_depth as f64 } else { 0.0 },
            queue_depth,
            timestamp: Instant::now(),
            stage: self.stage_name.clone(),
        });
    }
    
    /// Record an error
    pub fn record_error(&self) {
        self.errors.record_error();
        self.broadcast_update();
    }
    
    /// Get current saturation level (useful for backpressure decisions)
    pub fn saturation_level(&self) -> f64 {
        self.saturation.current_ratio()
    }
    
    fn broadcast_update(&self) {
        // Broadcast general update
        let _ = self.update_tx.send(MetricUpdate::Utilization {
            value: self.utilization.current_utilization(), // Binary utilization
            timestamp: Instant::now(),
            stage: self.stage_name.clone(),
        });
    }
}

impl TaxonomyMetrics for USEMetrics {
    fn current_values(&self) -> MetricSnapshot {
        let utilization = self.utilization.current_utilization();
        let saturation = self.saturation.current_ratio();
        let error_count = self.errors.total_errors();
        
        MetricSnapshot {
            timestamp: Instant::now(),
            utilization,
            saturation, 
            queue_depth: 0, // TODO: Track actual queue depth if needed
            error_count,
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
        USE::NAME
    }
}
/// USE taxonomy definition
pub struct USE;

/// Factory for creating USE monitoring middleware with stage context
pub struct UseMonitoringFactory;

impl MiddlewareFactory for UseMonitoringFactory {
    fn create(&self, config: &StageConfig) -> Box<dyn Middleware> {
        Box::new(crate::middleware::MonitoringMiddleware::<USE>::new(
            config.stage_name.clone(),
            config.stage_id,
        ))
    }
    
    fn name(&self) -> &str {
        "USE::monitoring"
    }
}

impl USE {
    /// Create monitoring middleware factory for this taxonomy
    pub fn monitoring() -> Box<dyn MiddlewareFactory> {
        Box::new(UseMonitoringFactory)
    }
}

impl Taxonomy for USE {
    const NAME: &'static str = "USE";
    const DESCRIPTION: &'static str = "Utilization, Saturation, Errors - ideal for resource-constrained systems";
    
    type Metrics = USEMetrics;
    
    fn create_metrics(stage_name: &str, stage_id: StageId) -> Self::Metrics {
        USEMetrics::new(stage_name, stage_id)
    }
}