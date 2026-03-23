// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ExportError, MetricExporter};
use crate::monitoring::metrics::core::{Metric, MetricSnapshot};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Test exporter that captures metric snapshots for assertions
///
/// This exporter is always available (no feature flags) and enables:
/// - Unit testing of metrics without external dependencies
/// - Integration testing of pipelines with metric assertions
/// - Debugging and development without an external metrics backend
/// - Performance testing with captured metrics
pub struct TestExporter {
    captured_metrics: Arc<Mutex<Vec<MetricSnapshot>>>,
    name_index: Arc<Mutex<HashMap<String, usize>>>,
}

impl TestExporter {
    /// Create a new test exporter
    pub fn new() -> Self {
        Self {
            captured_metrics: Arc::new(Mutex::new(Vec::new())),
            name_index: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Get all captured metric snapshots
    pub fn captured_metrics(&self) -> Vec<MetricSnapshot> {
        self.captured_metrics.lock().unwrap().clone()
    }

    /// Get the most recent snapshot for a metric by name
    pub fn get_metric(&self, name: &str) -> Option<MetricSnapshot> {
        let captured = self.captured_metrics.lock().unwrap();
        let index = self.name_index.lock().unwrap();

        if let Some(&idx) = index.get(name) {
            captured.get(idx).cloned()
        } else {
            None
        }
    }

    /// Get all snapshots for a metric by name (in chronological order)
    pub fn get_metric_history(&self, name: &str) -> Vec<MetricSnapshot> {
        let captured = self.captured_metrics.lock().unwrap();
        captured
            .iter()
            .filter(|snapshot| snapshot.name == name)
            .cloned()
            .collect()
    }

    /// Clear all captured metrics
    pub fn clear(&self) {
        self.captured_metrics.lock().unwrap().clear();
        self.name_index.lock().unwrap().clear();
    }

    /// Get count of captured metrics
    pub fn len(&self) -> usize {
        self.captured_metrics.lock().unwrap().len()
    }

    /// Check if any metrics have been captured
    pub fn is_empty(&self) -> bool {
        self.captured_metrics.lock().unwrap().is_empty()
    }

    /// Get all unique metric names that have been captured
    pub fn metric_names(&self) -> Vec<String> {
        let index = self.name_index.lock().unwrap();
        index.keys().cloned().collect()
    }

    /// Assert that a metric with the given name exists
    pub fn assert_metric_exists(&self, name: &str) {
        assert!(
            self.get_metric(name).is_some(),
            "Expected metric '{}' to exist, but it was not found. Available metrics: {:?}",
            name,
            self.metric_names()
        );
    }

    /// Assert that a counter metric has the expected value
    pub fn assert_counter_value(&self, name: &str, expected: u64) {
        let metric = self
            .get_metric(name)
            .unwrap_or_else(|| panic!("Metric '{name}' not found"));

        match metric.value.as_counter() {
            Some(actual) => assert_eq!(
                actual, expected,
                "Counter '{name}' expected {expected}, but was {actual}"
            ),
            None => panic!(
                "Metric '{}' is not a counter, it's a {:?}",
                name,
                metric.value.metric_type()
            ),
        }
    }

    /// Assert that a gauge metric has the expected value (with tolerance for floats)
    pub fn assert_gauge_value(&self, name: &str, expected: f64, tolerance: f64) {
        let metric = self
            .get_metric(name)
            .unwrap_or_else(|| panic!("Metric '{name}' not found"));

        match metric.value.as_gauge() {
            Some(actual) => {
                let diff = (actual - expected).abs();
                assert!(
                    diff <= tolerance,
                    "Gauge '{name}' expected {expected:.3} ± {tolerance:.3}, but was {actual:.3} (diff: {diff:.3})"
                );
            }
            None => panic!(
                "Metric '{}' is not a gauge, it's a {:?}",
                name,
                metric.value.metric_type()
            ),
        }
    }

    /// Assert that a metric has been captured at least N times
    pub fn assert_metric_count(&self, name: &str, min_count: usize) {
        let history = self.get_metric_history(name);
        assert!(
            history.len() >= min_count,
            "Expected metric '{}' to be captured at least {} times, but was captured {} times",
            name,
            min_count,
            history.len()
        );
    }
}

impl MetricExporter<MetricSnapshot> for TestExporter {
    fn export(&self, metric: &dyn Metric) -> Result<MetricSnapshot, ExportError> {
        let snapshot = metric.snapshot();

        // Store the snapshot
        {
            let mut captured = self.captured_metrics.lock().unwrap();
            let mut index = self.name_index.lock().unwrap();

            let position = captured.len();
            captured.push(snapshot.clone());
            index.insert(snapshot.name.clone(), position);
        }

        Ok(snapshot)
    }
}

impl Default for TestExporter {
    fn default() -> Self {
        Self::new()
    }
}

// Convenience methods for common test patterns
impl TestExporter {
    /// Create a new test exporter and capture metrics from the provided metrics
    pub fn capture_metrics(metrics: Vec<&dyn Metric>) -> Self {
        let exporter = Self::new();
        for metric in metrics {
            let _ = exporter.export(metric);
        }
        exporter
    }

    /// Export and return the snapshot in one call (useful for single-metric tests)
    pub fn export_one(&self, metric: &dyn Metric) -> MetricSnapshot {
        self.export(metric).expect("Failed to export metric")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::metrics::core::{MetricType, MetricUpdate, MetricValue};
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::sync::broadcast;

    // Mock metric for testing
    struct MockCounter {
        name: String,
        value: u64,
    }

    impl Metric for MockCounter {
        fn name(&self) -> &str {
            &self.name
        }

        fn snapshot(&self) -> MetricSnapshot {
            MetricSnapshot {
                name: self.name.clone(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(self.value),
                timestamp: Instant::now(),
                labels: HashMap::new(),
            }
        }

        fn update(&self, _value: MetricValue) {
            // Mock implementation
        }

        fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
            let (_, rx) = broadcast::channel(1);
            rx
        }
    }

    #[test]
    fn test_basic_capture() {
        let exporter = TestExporter::new();
        let metric = MockCounter {
            name: "test_counter".to_string(),
            value: 42,
        };

        let snapshot = exporter.export(&metric).unwrap();

        assert_eq!(snapshot.name, "test_counter");
        assert_eq!(snapshot.value.as_counter(), Some(42));

        // Verify it's stored
        let captured = exporter.get_metric("test_counter").unwrap();
        assert_eq!(captured.value.as_counter(), Some(42));
    }

    #[test]
    fn test_multiple_captures() {
        let exporter = TestExporter::new();

        let counter1 = MockCounter {
            name: "counter1".to_string(),
            value: 10,
        };
        let counter2 = MockCounter {
            name: "counter2".to_string(),
            value: 20,
        };

        exporter.export(&counter1).unwrap();
        exporter.export(&counter2).unwrap();

        assert_eq!(exporter.len(), 2);
        assert_eq!(exporter.metric_names().len(), 2);

        exporter.assert_counter_value("counter1", 10);
        exporter.assert_counter_value("counter2", 20);
    }

    #[test]
    fn test_metric_history() {
        let exporter = TestExporter::new();

        // Simulate a counter being incremented
        let counter_v1 = MockCounter {
            name: "requests".to_string(),
            value: 5,
        };
        let counter_v2 = MockCounter {
            name: "requests".to_string(),
            value: 10,
        };
        let counter_v3 = MockCounter {
            name: "requests".to_string(),
            value: 15,
        };

        exporter.export(&counter_v1).unwrap();
        exporter.export(&counter_v2).unwrap();
        exporter.export(&counter_v3).unwrap();

        let history = exporter.get_metric_history("requests");
        assert_eq!(history.len(), 3);

        // Most recent value should be 15
        exporter.assert_counter_value("requests", 15);

        // History should show progression
        assert_eq!(history[0].value.as_counter(), Some(5));
        assert_eq!(history[1].value.as_counter(), Some(10));
        assert_eq!(history[2].value.as_counter(), Some(15));
    }

    #[test]
    fn test_assertions() {
        let exporter = TestExporter::new();
        let metric = MockCounter {
            name: "test_metric".to_string(),
            value: 100,
        };

        exporter.export(&metric).unwrap();

        // Test successful assertions
        exporter.assert_metric_exists("test_metric");
        exporter.assert_counter_value("test_metric", 100);
        exporter.assert_metric_count("test_metric", 1);
    }

    #[test]
    #[should_panic(expected = "Expected metric 'nonexistent' to exist")]
    fn test_assert_metric_exists_failure() {
        let exporter = TestExporter::new();
        exporter.assert_metric_exists("nonexistent");
    }

    #[test]
    #[should_panic(expected = "Counter 'test' expected 50, but was 100")]
    fn test_assert_counter_value_failure() {
        let exporter = TestExporter::new();
        let metric = MockCounter {
            name: "test".to_string(),
            value: 100,
        };

        exporter.export(&metric).unwrap();
        exporter.assert_counter_value("test", 50);
    }

    #[test]
    fn test_convenience_methods() {
        let metric = MockCounter {
            name: "convenience_test".to_string(),
            value: 777,
        };

        // Test capture_metrics convenience method
        let exporter = TestExporter::capture_metrics(vec![&metric]);
        exporter.assert_counter_value("convenience_test", 777);

        // Test export_one convenience method
        let exporter2 = TestExporter::new();
        let snapshot = exporter2.export_one(&metric);
        assert_eq!(snapshot.value.as_counter(), Some(777));
    }
}
