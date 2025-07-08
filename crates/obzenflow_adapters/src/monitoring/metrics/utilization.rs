use super::core::{Metric, MetricValue, MetricUpdate, MetricSnapshot, MetricType};
use super::core::{StatefulMetric};
use super::primitives::Gauge;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::broadcast;

const DEFAULT_METRIC_CHANNEL_CAPACITY: usize = 1024;

/// Binary utilization states
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UtilizationState {
    /// Resource is waiting/idle (0% utilization) - DEFAULT
    Waiting = 0,
    /// Resource is actively working (100% utilization)
    Working = 1,
}

/// Utilization metric - wraps Gauge primitive for binary utilization tracking
///
/// Simplified to wrap a Gauge primitive. Binary utilization: 0.0 (waiting/idle) or 1.0 (working).
/// This follows the FLOWIP-004 specification and uses stopwatch time only.
pub struct UtilizationMetric {
    name: String,
    gauge: Gauge,  // Stores current utilization: 0.0 (waiting) or 1.0 (working)
    current_state: AtomicU64,  // UtilizationState as u64
    last_change: AtomicU64,    // Stopwatch time in nanoseconds since metric creation
    start_time: Instant,       // Stopwatch reference point (not calendar time)
    realtime_tx: broadcast::Sender<MetricUpdate>,
}

impl UtilizationMetric {
    pub fn new(name: impl Into<String>) -> Self {
        let (tx, _) = broadcast::channel(DEFAULT_METRIC_CHANNEL_CAPACITY);
        let start_time = Instant::now();  // Stopwatch time, not calendar time
        
        Self {
            name: name.into(),
            gauge: Gauge::new(),  // Starts at 0.0 (waiting)
            current_state: AtomicU64::new(UtilizationState::Waiting as u64), // Default to waiting
            last_change: AtomicU64::new(0), // Start at 0 nanoseconds since creation
            start_time,
            realtime_tx: tx,
        }
    }
    
    /// Set utilization as a ratio (0.0 to 1.0)
    /// 
    /// This is the FLOWIP-004 compliant interface. The stage calculates its own
    /// utilization ratio and reports it. For binary utilization, typically 0.0 or 1.0.
    pub fn set_utilization(&self, ratio: f64) {
        self.gauge.set(ratio.clamp(0.0, 1.0));
        self.broadcast_update(ratio);
    }
    
    /// Start doing work - sets utilization to 1.0 (100% utilized)
    pub fn start_work(&self) {
        self.transition_to(UtilizationState::Working);
        self.set_utilization(1.0);
    }
    
    /// Start waiting/go idle - sets utilization to 0.0 (0% utilized)
    pub fn start_waiting(&self) {
        self.transition_to(UtilizationState::Waiting);
        self.set_utilization(0.0);
    }
    
    /// End work (go back to waiting) - sets utilization to 0.0
    pub fn end_work(&self) {
        self.start_waiting();
    }
    
    /// Get current utilization ratio (0.0 or 1.0 for binary utilization)
    pub fn current_utilization(&self) -> f64 {
        self.gauge.get()
    }
    
    /// Get current state
    pub fn current_state(&self) -> UtilizationState {
        match self.current_state.load(Ordering::Relaxed) {
            0 => UtilizationState::Waiting,
            1 => UtilizationState::Working,
            _ => UtilizationState::Waiting, // Default fallback
        }
    }
    
    /// Check if currently working
    pub fn is_working(&self) -> bool {
        self.current_state() == UtilizationState::Working
    }
    
    /// Check if currently waiting/idle
    pub fn is_waiting(&self) -> bool {
        self.current_state() == UtilizationState::Waiting
    }
    
    /// Get time since metric creation using stopwatch time
    pub fn uptime_nanos(&self) -> u64 {
        self.start_time.elapsed().as_nanos() as u64
    }
    
    /// Reset utilization to 0.0 and return to waiting state
    pub fn reset(&self) {
        self.gauge.reset();
        self.current_state.store(UtilizationState::Waiting as u64, Ordering::Relaxed);
        self.last_change.store(self.uptime_nanos(), Ordering::Relaxed);
        
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Gauge(0.0),
            timestamp: Instant::now(),
        });
    }
    
    /// Internal: transition to a new state using stopwatch time
    fn transition_to(&self, new_state: UtilizationState) {
        let now_nanos = self.uptime_nanos();
        self.current_state.store(new_state as u64, Ordering::Relaxed);
        self.last_change.store(now_nanos, Ordering::Relaxed);
    }
    
    /// Internal: broadcast utilization update
    fn broadcast_update(&self, utilization: f64) {
        let _ = self.realtime_tx.send(MetricUpdate {
            metric_name: self.name.clone(),
            value: MetricValue::Gauge(utilization),
            timestamp: Instant::now(),
        });
    }
}

impl Metric for UtilizationMetric {
    fn name(&self) -> &str {
        &self.name
    }
    
    fn snapshot(&self) -> MetricSnapshot {
        let mut labels = HashMap::new();
        
        // Add current state to labels for exporters
        let state_str = match self.current_state() {
            UtilizationState::Waiting => "waiting",
            UtilizationState::Working => "working",
        };
        labels.insert("state".to_string(), state_str.to_string());
        
        // Labels provide dimensional metadata for metrics filtering and grouping.
        // Examples: {"service": "api", "state": "working", "region": "us-west-2"}
        // This enables Prometheus queries like: utilization{state="working"}
        // Binary utilization: 0.0 (waiting/idle) or 1.0 (working)
        
        MetricSnapshot {
            name: self.name.clone(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(self.current_utilization()),
            timestamp: Instant::now(),
            labels,
        }
    }
    
    fn update(&self, value: MetricValue) {
        match value {
            MetricValue::Gauge(ratio) => {
                // Accept external utilization ratio updates
                self.set_utilization(ratio);
            }
            _ => {}
        }
    }
    
    fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
        self.realtime_tx.subscribe()
    }
}

// Mark UtilizationMetric as requiring internal state reporting
impl StatefulMetric for UtilizationMetric {}

/// Support trait for stages that can provide utilization metrics
/// 
/// Utilization metrics track binary resource usage: working (1.0) or waiting (0.0).
pub trait UtilizationSupport {
    /// Get the utilization metric instance for this stage
    fn utilization_metric(&self) -> &UtilizationMetric;
    
    /// Report utilization as a ratio (0.0 to 1.0, typically 0.0 or 1.0 for binary)
    fn report_utilization(&self, ratio: f64) {
        self.utilization_metric().set_utilization(ratio);
    }
    
    /// Report that the stage is starting work (sets utilization to 1.0)
    fn start_work(&self) {
        self.utilization_metric().start_work();
    }
    
    /// Report that the stage is waiting/idle (sets utilization to 0.0)
    fn start_waiting(&self) {
        self.utilization_metric().start_waiting();
    }
    
    /// Report that the stage finished work and is waiting (sets utilization to 0.0)
    fn end_work(&self) {
        self.utilization_metric().end_work();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_utilization_binary_states() {
        let metric = UtilizationMetric::new("test_resource");
        
        assert_eq!(metric.name(), "test_resource");
        assert_eq!(metric.current_utilization(), 0.0);
        assert_eq!(metric.current_state(), UtilizationState::Waiting);
        assert!(metric.is_waiting());
        assert!(!metric.is_working());
        
        // Start working
        metric.start_work();
        assert_eq!(metric.current_utilization(), 1.0);
        assert_eq!(metric.current_state(), UtilizationState::Working);
        assert!(!metric.is_waiting());
        assert!(metric.is_working());
        
        // End work (back to waiting)
        metric.end_work();
        assert_eq!(metric.current_utilization(), 0.0);
        assert_eq!(metric.current_state(), UtilizationState::Waiting);
        assert!(metric.is_waiting());
        assert!(!metric.is_working());
    }

    #[tokio::test]
    async fn test_utilization_custom_ratios() {
        let metric = UtilizationMetric::new("custom_test");
        
        // Test partial utilization ratios (for non-binary use cases)
        metric.set_utilization(0.75);
        assert!((metric.current_utilization() - 0.75).abs() < 0.001);
        
        metric.set_utilization(0.25);
        assert!((metric.current_utilization() - 0.25).abs() < 0.001);
        
        // Test clamping
        metric.set_utilization(1.5);
        assert_eq!(metric.current_utilization(), 1.0);
        
        metric.set_utilization(-0.5);
        assert_eq!(metric.current_utilization(), 0.0);
    }

    #[tokio::test]
    async fn test_utilization_snapshot() {
        let metric = UtilizationMetric::new("snapshot_test");
        metric.start_work();
        
        let snapshot = metric.snapshot();
        assert_eq!(snapshot.name, "snapshot_test");
        assert_eq!(snapshot.metric_type, MetricType::Gauge);
        
        if let MetricValue::Gauge(value) = snapshot.value {
            assert_eq!(value, 1.0);
        } else {
            panic!("Expected Gauge value");
        }
        
        assert_eq!(snapshot.labels.get("state"), Some(&"working".to_string()));
        
        // Test waiting state
        metric.start_waiting();
        let snapshot2 = metric.snapshot();
        assert_eq!(snapshot2.labels.get("state"), Some(&"waiting".to_string()));
    }

    #[tokio::test]
    async fn test_utilization_reset() {
        let metric = UtilizationMetric::new("reset_test");
        
        metric.start_work();
        assert_eq!(metric.current_utilization(), 1.0);
        assert_eq!(metric.current_state(), UtilizationState::Working);
        
        metric.reset();
        assert_eq!(metric.current_utilization(), 0.0);
        assert_eq!(metric.current_state(), UtilizationState::Waiting);
    }

    #[tokio::test]
    async fn test_utilization_realtime_updates() {
        let metric = UtilizationMetric::new("realtime_test");
        let mut receiver = metric.subscribe();
        
        metric.start_work();
        
        let update = receiver.recv().await.expect("Should receive update");
        assert_eq!(update.metric_name, "realtime_test");
        
        if let MetricValue::Gauge(value) = update.value {
            assert_eq!(value, 1.0);
        } else {
            panic!("Expected Gauge value in update");
        }
    }

    #[tokio::test]
    async fn test_utilization_support_trait() {
        struct TestStage {
            utilization: UtilizationMetric,
        }
        
        impl UtilizationSupport for TestStage {
            fn utilization_metric(&self) -> &UtilizationMetric {
                &self.utilization
            }
        }
        
        let stage = TestStage {
            utilization: UtilizationMetric::new("stage_test"),
        };
        
        // Test trait methods
        stage.report_utilization(0.6);
        assert!((stage.utilization_metric().current_utilization() - 0.6).abs() < 0.001);
        
        stage.start_work();
        assert_eq!(stage.utilization_metric().current_utilization(), 1.0);
        assert_eq!(stage.utilization_metric().current_state(), UtilizationState::Working);
        
        stage.start_waiting();
        assert_eq!(stage.utilization_metric().current_utilization(), 0.0);
        assert_eq!(stage.utilization_metric().current_state(), UtilizationState::Waiting);
        
        stage.end_work();
        assert_eq!(stage.utilization_metric().current_utilization(), 0.0);
        assert_eq!(stage.utilization_metric().current_state(), UtilizationState::Waiting);
    }

    #[tokio::test]
    async fn test_utilization_stopwatch_time() {
        let metric = UtilizationMetric::new("stopwatch_test");
        
        let start_uptime = metric.uptime_nanos();
        // uptime_nanos() measures elapsed time since metric creation, not zero
        
        // Wait a bit and check that uptime increases
        sleep(Duration::from_millis(1)).await;
        let later_uptime = metric.uptime_nanos();
        assert!(later_uptime > start_uptime);
        assert!(later_uptime > 500_000); // Should be at least 0.5ms in nanos
    }

    #[tokio::test]
    async fn test_binary_utilization_workflow() {
        let metric = UtilizationMetric::new("workflow_test");
        
        // Typical workflow: waiting -> work -> waiting -> work
        assert!(metric.is_waiting()); // Default state
        
        metric.start_work();
        assert!(metric.is_working());
        assert_eq!(metric.current_utilization(), 1.0);
        
        metric.end_work();
        assert!(metric.is_waiting());
        assert_eq!(metric.current_utilization(), 0.0);
        
        metric.start_work();
        assert!(metric.is_working());
        assert_eq!(metric.current_utilization(), 1.0);
        
        metric.start_waiting(); // Direct transition to waiting
        assert!(metric.is_waiting());
        assert_eq!(metric.current_utilization(), 0.0);
    }
}