use flowstate_rs::monitoring::metrics::{
    EventfulMetric, StatefulMetric,
    RateMetric, RateSupport,
    ErrorMetric, ErrorSupport, 
    DurationMetric, DurationSupport,
    UtilizationMetric, UtilizationSupport,
    SaturationMetric, SaturationSupport,
    NewMetric as Metric,
};

/// Integration tests for metric trait system
/// 
/// These tests verify that our compile-time metric contract system works correctly
/// by testing trait implementations and bounds without creating circular dependencies.

#[test]
fn test_eventful_metrics_implement_marker_trait() {
    // Test that eventful metrics implement the EventfulMetric marker trait
    let rate_metric = RateMetric::new("test_rate");
    let error_metric = ErrorMetric::new("test_errors");
    let duration_metric = DurationMetric::new("test_duration");
    
    // These should compile because these metrics implement EventfulMetric
    fn accepts_eventful<T: EventfulMetric>(_metric: &T) {}
    
    accepts_eventful(&rate_metric);
    accepts_eventful(&error_metric);
    accepts_eventful(&duration_metric);
}

#[test]
fn test_stateful_metrics_implement_marker_trait() {
    // Test that stateful metrics implement the StatefulMetric marker trait
    let utilization_metric = UtilizationMetric::new("test_utilization");
    let saturation_metric = SaturationMetric::new("test_saturation");
    
    // These should compile because these metrics implement StatefulMetric
    fn accepts_stateful<T: StatefulMetric>(_metric: &T) {}
    
    accepts_stateful(&utilization_metric);
    accepts_stateful(&saturation_metric);
}

// TODO: Replace with primitive-based tests after refactoring

#[test]
fn test_metric_categorization() {
    // Test that we can write generic functions that work with different metric categories
    
    fn process_eventful_metric<T: EventfulMetric>(metric: &T) -> String {
        format!("Processing eventful metric: {}", metric.name())
    }
    
    fn process_stateful_metric<T: StatefulMetric>(metric: &T) -> String {
        format!("Processing stateful metric: {}", metric.name())
    }
    
    let rate_metric = RateMetric::new("test_rate");
    let utilization_metric = UtilizationMetric::new("test_utilization");
    let saturation_metric = SaturationMetric::new("test_saturation");
    
    let eventful_result = process_eventful_metric(&rate_metric);
    let stateful_result = process_stateful_metric(&utilization_metric);
    let stateful_result2 = process_stateful_metric(&saturation_metric);
    
    assert_eq!(eventful_result, "Processing eventful metric: test_rate");
    assert_eq!(stateful_result, "Processing stateful metric: test_utilization");
    assert_eq!(stateful_result2, "Processing stateful metric: test_saturation");
}


/// Mock stage implementation to test trait requirements
struct MockStage {
    rate_metric: RateMetric,
    error_metric: ErrorMetric,
    duration_metric: DurationMetric,
    utilization_metric: UtilizationMetric,
    saturation_metric: SaturationMetric,
}

impl MockStage {
    fn new() -> Self {
        Self {
            rate_metric: RateMetric::new("mock_rate"),
            error_metric: ErrorMetric::new("mock_errors"),
            duration_metric: DurationMetric::new("mock_duration"),
            utilization_metric: UtilizationMetric::new("mock_utilization"),
            saturation_metric: SaturationMetric::new("mock_saturation"),
        }
    }
}

impl RateSupport for MockStage {
    fn rate_metric(&self) -> &RateMetric {
        &self.rate_metric
    }
}

impl ErrorSupport for MockStage {
    fn error_metric(&self) -> &ErrorMetric {
        &self.error_metric
    }
}

impl DurationSupport for MockStage {
    fn duration_metric(&self) -> &DurationMetric {
        &self.duration_metric
    }
}

impl UtilizationSupport for MockStage {
    fn utilization_metric(&self) -> &UtilizationMetric {
        &self.utilization_metric
    }
}

impl SaturationSupport for MockStage {
    fn saturation_metric(&self) -> &SaturationMetric {
        &self.saturation_metric
    }
}

#[test]
fn test_mock_stage_implements_all_support_traits() {
    let stage = MockStage::new();
    
    // Test that our mock stage can provide all metric types
    assert_eq!(stage.rate_metric().name(), "mock_rate");
    assert_eq!(stage.error_metric().name(), "mock_errors");
    assert_eq!(stage.duration_metric().name(), "mock_duration");
    assert_eq!(stage.utilization_metric().name(), "mock_utilization");
    assert_eq!(stage.saturation_metric().name(), "mock_saturation");
    
    // Test convenience methods work with binary utilization
    stage.start_work();
    stage.end_work();
    stage.start_waiting();
    stage.report_saturation(0.75);
}

#[test]
fn test_compile_time_metric_contracts() {
    // Test that we can write functions that require specific metric support
    
    fn requires_rate_and_errors<T>(stage: &T) 
    where 
        T: RateSupport + ErrorSupport
    {
        // This stage must support both rate and error metrics
        let _rate = stage.rate_metric();
        let _errors = stage.error_metric();
    }
    
    fn requires_all_red_metrics<T>(stage: &T)
    where
        T: RateSupport + ErrorSupport + DurationSupport
    {
        // This stage must support all RED metrics (Rate, Errors, Duration)
        let _rate = stage.rate_metric();
        let _errors = stage.error_metric();  
        let _duration = stage.duration_metric();
    }
    
    fn requires_utilization<T>(stage: &T)
    where
        T: UtilizationSupport
    {
        // This stage must support utilization metrics (for USE taxonomy)
        let _utilization = stage.utilization_metric();
        stage.start_work();
        stage.end_work();
    }
    
    let stage = MockStage::new();
    
    // All of these should compile because MockStage implements all support traits
    requires_rate_and_errors(&stage);
    requires_all_red_metrics(&stage);
    requires_utilization(&stage);
}

#[test]
fn test_advanced_metric_contracts() {
    // Test more complex metric contract combinations
    
    fn requires_use_metrics<T>(stage: &T)
    where
        T: UtilizationSupport + SaturationSupport + ErrorSupport
    {
        // USE taxonomy: Utilization, Saturation, Errors
        let _utilization = stage.utilization_metric();
        let _saturation = stage.saturation_metric();
        let _errors = stage.error_metric();
    }
    
    fn requires_golden_signals<T>(stage: &T)
    where
        T: RateSupport + ErrorSupport + DurationSupport + SaturationSupport
    {
        // Golden Signals: Rate, Errors, Duration, Saturation
        let _rate = stage.rate_metric();
        let _errors = stage.error_metric();
        let _duration = stage.duration_metric();
        let _saturation = stage.saturation_metric();
    }
    
    let stage = MockStage::new();
    
    // These should compile because MockStage supports all metrics
    requires_use_metrics(&stage);
    requires_golden_signals(&stage);
}

/// Example of a simple stage that only supports event-observable metrics
struct SimpleJsonParseStage {
    rate_metric: RateMetric,
    error_metric: ErrorMetric,
    duration_metric: DurationMetric,
}

impl SimpleJsonParseStage {
    fn new() -> Self {
        Self {
            rate_metric: RateMetric::new("json_parse_rate"),
            error_metric: ErrorMetric::new("json_parse_errors"),
            duration_metric: DurationMetric::new("json_parse_duration"),
        }
    }
}

impl RateSupport for SimpleJsonParseStage {
    fn rate_metric(&self) -> &RateMetric {
        &self.rate_metric
    }
}

impl ErrorSupport for SimpleJsonParseStage {
    fn error_metric(&self) -> &ErrorMetric {
        &self.error_metric
    }
}

impl DurationSupport for SimpleJsonParseStage {
    fn duration_metric(&self) -> &DurationMetric {
        &self.duration_metric
    }
}

// Note: SimpleJsonParseStage does NOT implement SaturationSupport or UtilizationSupport
// because it's a pure function with no internal state to saturate

#[test]
fn test_stage_with_limited_metric_support() {
    let simple_stage = SimpleJsonParseStage::new();
    
    // This function requires only RED metrics
    fn requires_red_metrics<T>(stage: &T)
    where
        T: RateSupport + ErrorSupport + DurationSupport
    {
        let _ = stage.rate_metric();
        let _ = stage.error_metric();
        let _ = stage.duration_metric();
    }
    
    // This should compile - SimpleJsonParseStage supports RED
    requires_red_metrics(&simple_stage);
    
    // The following would NOT compile (commented out to prevent compilation errors):
    // requires_use_metrics(&simple_stage);  // Error: SimpleJsonParseStage doesn't implement SaturationSupport
    // requires_golden_signals(&simple_stage); // Error: SimpleJsonParseStage doesn't implement SaturationSupport
}

/// Example of a complex stage that supports state-dependent metrics
struct ComplexQueueStage {
    // Event-observable metrics
    rate_metric: RateMetric,
    error_metric: ErrorMetric,
    duration_metric: DurationMetric,
    
    // State-dependent metrics
    utilization_metric: UtilizationMetric,
    saturation_metric: SaturationMetric,
    
    // Internal state
    queue_size: usize,
    max_queue_size: usize,
}

impl ComplexQueueStage {
    fn new(max_size: usize) -> Self {
        Self {
            rate_metric: RateMetric::new("queue_rate"),
            error_metric: ErrorMetric::new("queue_errors"),
            duration_metric: DurationMetric::new("queue_duration"),
            utilization_metric: UtilizationMetric::new("queue_utilization"),
            saturation_metric: SaturationMetric::new("queue_saturation"),
            queue_size: 0,
            max_queue_size: max_size,
        }
    }
    
    fn add_item(&mut self) {
        self.queue_size += 1;
        
        // Report saturation based on internal state
        let ratio = self.queue_size as f64 / self.max_queue_size as f64;
        self.report_saturation(ratio);
        
        // Track utilization
        self.start_work();
    }
}

// Implement all support traits for ComplexQueueStage
impl RateSupport for ComplexQueueStage {
    fn rate_metric(&self) -> &RateMetric {
        &self.rate_metric
    }
}

impl ErrorSupport for ComplexQueueStage {
    fn error_metric(&self) -> &ErrorMetric {
        &self.error_metric
    }
}

impl DurationSupport for ComplexQueueStage {
    fn duration_metric(&self) -> &DurationMetric {
        &self.duration_metric
    }
}

impl UtilizationSupport for ComplexQueueStage {
    fn utilization_metric(&self) -> &UtilizationMetric {
        &self.utilization_metric
    }
}

impl SaturationSupport for ComplexQueueStage {
    fn saturation_metric(&self) -> &SaturationMetric {
        &self.saturation_metric
    }
}

#[test]
fn test_complex_stage_supports_all_taxonomies() {
    let mut complex_stage = ComplexQueueStage::new(100);
    
    // Function that requires ALL metrics (like SAAFE would)
    fn requires_all_metrics<T>(stage: &T)
    where
        T: RateSupport + ErrorSupport + DurationSupport + UtilizationSupport + SaturationSupport
    {
        let _ = stage.rate_metric();
        let _ = stage.error_metric();
        let _ = stage.duration_metric();
        let _ = stage.utilization_metric();
        let _ = stage.saturation_metric();
    }
    
    // This should compile - ComplexQueueStage supports everything
    requires_all_metrics(&complex_stage);
    
    // Test that the stage can report its internal state
    complex_stage.add_item();
    assert!(complex_stage.saturation_metric().current_ratio() > 0.0);
}

#[test]
fn test_compile_time_enforcement_example() {
    // This test demonstrates the compile-time enforcement of metric contracts
    
    let simple_stage = SimpleJsonParseStage::new();
    let complex_stage = ComplexQueueStage::new(100);
    
    // Generic function that can work with any stage supporting RED metrics
    fn process_with_red<T>(stage: &T) -> String
    where
        T: RateSupport + ErrorSupport + DurationSupport
    {
        format!("Processing with RED metrics: {}", stage.rate_metric().name())
    }
    
    // Both stages can be used with RED taxonomy
    let _result1 = process_with_red(&simple_stage);
    let _result2 = process_with_red(&complex_stage);
    
    // Generic function requiring USE metrics
    fn process_with_use<T>(stage: &T) -> String
    where
        T: UtilizationSupport + SaturationSupport + ErrorSupport
    {
        format!("Processing with USE metrics: {}", stage.utilization_metric().name())
    }
    
    // Only complex stage can be used with USE taxonomy
    let _result3 = process_with_use(&complex_stage);
    // process_with_use(&simple_stage); // Would NOT compile!
}