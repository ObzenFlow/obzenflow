use crate::monitoring::metrics::core::Metric;

/// Generic exporter interface for converting metrics to various formats
/// 
/// This trait enables the core principle of separation of concerns:
/// - Metrics focus on collecting and tracking values
/// - Exporters focus on format conversion and transmission
/// 
/// The generic type T represents the output format (e.g., Prometheus MetricFamily,
/// StatsD wire format string, JSON for APIs, test snapshots, etc.)
pub trait MetricExporter<T> {
    /// Export a single metric to the target format
    fn export(&self, metric: &dyn Metric) -> Result<T, ExportError>;
    
    /// Export multiple metrics efficiently (can be overridden for batch optimizations)
    fn export_batch(&self, metrics: Vec<&dyn Metric>) -> Result<Vec<T>, ExportError> {
        metrics.into_iter()
            .map(|metric| self.export(metric))
            .collect()
    }
}

/// Errors that can occur during metric export
#[derive(Debug, thiserror::Error)]
pub enum ExportError {
    #[error("Unsupported metric type: {metric_type} for exporter {exporter_name}")]
    UnsupportedMetricType {
        metric_type: String,
        exporter_name: String,
    },
    
    #[error("Export format error: {message}")]
    FormatError { message: String },
    
    #[error("Network error during export: {message}")]
    NetworkError { message: String },
    
    #[error("Authentication error: {message}")]
    AuthError { message: String },
    
    #[error("Configuration error: {message}")]
    ConfigError { message: String },
}

/// Helper trait for exporters that need to group metrics by labels or other criteria
pub trait BatchingExporter<T>: MetricExporter<T> {
    type BatchKey: Eq + std::hash::Hash;
    
    /// Extract a grouping key from a metric for batching
    fn batch_key(&self, metric: &dyn Metric) -> Self::BatchKey;
    
    /// Export a batch of metrics with the same key
    fn export_batch_grouped(&self, key: Self::BatchKey, metrics: Vec<&dyn Metric>) -> Result<T, ExportError>;
}

/// Registry for managing multiple exporters
pub struct ExporterRegistry {
    exporters: Vec<Box<dyn ExporterAny>>,
}

impl ExporterRegistry {
    pub fn new() -> Self {
        Self {
            exporters: Vec::new(),
        }
    }
    
    pub fn add_exporter<T, E>(mut self, exporter: E) -> Self 
    where
        E: MetricExporter<T> + Send + Sync + 'static,
        T: Send + Sync + 'static,
    {
        self.exporters.push(Box::new(TypeErasedExporter::new(exporter)));
        self
    }
    
    pub fn export_all(&self, metrics: Vec<&dyn Metric>) -> Vec<Result<(), ExportError>> {
        self.exporters.iter()
            .map(|exporter| exporter.export_metrics(metrics.clone()))
            .collect()
    }
}

// Type erasure helpers for the registry
trait ExporterAny: Send + Sync {
    fn export_metrics(&self, metrics: Vec<&dyn Metric>) -> Result<(), ExportError>;
}

struct TypeErasedExporter<T, E> {
    exporter: E,
    _phantom: std::marker::PhantomData<T>,
}

impl<T, E> TypeErasedExporter<T, E> {
    fn new(exporter: E) -> Self {
        Self {
            exporter,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T, E> ExporterAny for TypeErasedExporter<T, E>
where
    E: MetricExporter<T> + Send + Sync,
    T: Send + Sync + 'static,
{
    fn export_metrics(&self, metrics: Vec<&dyn Metric>) -> Result<(), ExportError> {
        // Export but discard the results (registry just triggers exports)
        let _results = self.exporter.export_batch(metrics)?;
        Ok(())
    }
}

impl Default for ExporterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Conditional compilation for different exporters
#[cfg(feature = "metrics-statsd")]
pub mod statsd;

// Always available for testing
pub mod test;

// Clean exporters for FLOWIP-056-666
pub mod prometheus_exporter;

// Re-exports
pub use self::prometheus_exporter::PrometheusExporter;
pub use self::test::TestExporter;

#[cfg(feature = "metrics-statsd")]
pub use self::statsd::StatsDExporter;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::metrics::core::{MetricValue, MetricSnapshot};
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::sync::broadcast;

    // Mock metric for testing
    struct MockMetric {
        name: String,
        value: MetricValue,
        tx: broadcast::Sender<crate::monitoring::metrics::core::MetricUpdate>,
    }

    impl MockMetric {
        fn new(name: String, value: MetricValue) -> Self {
            let (tx, _) = broadcast::channel(10);
            Self { name, value, tx }
        }
    }

    impl Metric for MockMetric {
        fn name(&self) -> &str {
            &self.name
        }

        fn snapshot(&self) -> MetricSnapshot {
            MetricSnapshot {
                name: self.name.clone(),
                metric_type: self.value.metric_type(),
                value: self.value.clone(),
                timestamp: Instant::now(),
                labels: HashMap::new(),
            }
        }

        fn update(&self, _value: MetricValue) {
            // Mock implementation
        }

        fn subscribe(&self) -> broadcast::Receiver<crate::monitoring::metrics::core::MetricUpdate> {
            self.tx.subscribe()
        }
    }

    // Mock exporter for testing
    struct MockExporter;

    impl MetricExporter<String> for MockExporter {
        fn export(&self, metric: &dyn Metric) -> Result<String, ExportError> {
            Ok(format!("exported:{}", metric.name()))
        }
    }

    #[test]
    fn test_mock_exporter() {
        let metric = MockMetric::new(
            "test_counter".to_string(),
            MetricValue::Counter(42),
        );
        
        let exporter = MockExporter;
        let result = exporter.export(&metric).unwrap();
        
        assert_eq!(result, "exported:test_counter");
    }

    #[test]
    fn test_batch_export() {
        let metrics = vec![
            MockMetric::new(
                "counter1".to_string(),
                MetricValue::Counter(10),
            ),
            MockMetric::new(
                "counter2".to_string(),
                MetricValue::Counter(20),
            ),
        ];
        
        let metric_refs: Vec<&dyn Metric> = metrics.iter()
            .map(|m| m as &dyn Metric)
            .collect();
        
        let exporter = MockExporter;
        let results = exporter.export_batch(metric_refs).unwrap();
        
        assert_eq!(results.len(), 2);
        assert_eq!(results[0], "exported:counter1");
        assert_eq!(results[1], "exported:counter2");
    }

    #[test]
    fn test_registry() {
        let registry = ExporterRegistry::new()
            .add_exporter(MockExporter);
        
        let metric = MockMetric::new(
            "test_metric".to_string(),
            MetricValue::Counter(100),
        );
        
        let results = registry.export_all(vec![&metric]);
        assert_eq!(results.len(), 1);
        assert!(results[0].is_ok());
    }
}