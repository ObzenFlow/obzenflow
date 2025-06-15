// Prometheus exporter implementation  
// This module is only compiled when the "metrics-prometheus" feature is enabled
//
// This exporter follows FLOWIP-004 architecture:
// - Reads from Layer 2 (Metrics) via lock-free interfaces
// - Converts to Prometheus text exposition format
// - Stateless operation - no Mutex, no registration
// - Zero blocking operations

use crate::monitoring::metrics::{
    NewMetric as Metric, 
    NewMetricType as MetricType, 
    NewMetricValue as MetricValue,
};
use super::{MetricExporter, ExportError};
use std::fmt::Write;

/// Prometheus exporter that converts metrics to text exposition format
/// 
/// This is a stateless, lock-free exporter that generates Prometheus
/// format on-demand without any internal state or registration.
pub struct PrometheusExporter {
    prefix: String,
}

impl PrometheusExporter {
    /// Create a new Prometheus exporter
    pub fn new() -> Self {
        Self {
            prefix: "flowstate_".to_string(),
        }
    }
    
    /// Set metric name prefix (default: "flowstate_")
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
    
    /// Export a single metric to Prometheus text format
    fn export_single(&self, metric: &dyn Metric) -> Result<String, ExportError> {
        let snapshot = metric.snapshot();  // Lock-free atomic read
        let mut output = String::new();
        
        // Add HELP and TYPE comments
        writeln!(&mut output, "# HELP {}{} {}", 
            self.prefix, 
            snapshot.name,
            self.help_text_for(&snapshot.name)
        ).map_err(|e| ExportError::FormatError { 
            message: format!("Failed to write HELP: {}", e) 
        })?;
        
        match snapshot.metric_type {
            MetricType::Counter => {
                writeln!(&mut output, "# TYPE {}{} counter", self.prefix, snapshot.name)
                    .map_err(|e| ExportError::FormatError { 
                        message: format!("Failed to write TYPE: {}", e) 
                    })?;
                
                if let MetricValue::Counter(value) = snapshot.value {
                    // Prometheus convention: counters end with _total
                    let metric_name = if snapshot.name.ends_with("_total") {
                        format!("{}{}", self.prefix, snapshot.name)
                    } else {
                        format!("{}{}_total", self.prefix, snapshot.name)
                    };
                    
                    write!(&mut output, "{}", metric_name)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write metric name: {}", e) 
                        })?;
                    
                    // Add labels if present
                    if !snapshot.labels.is_empty() {
                        write!(&mut output, "{{{}}}", self.format_labels(&snapshot.labels))
                            .map_err(|e| ExportError::FormatError { 
                                message: format!("Failed to write labels: {}", e) 
                            })?;
                    }
                    
                    writeln!(&mut output, " {}", value)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write value: {}", e) 
                        })?;
                }
            }
            
            MetricType::Gauge => {
                writeln!(&mut output, "# TYPE {}{} gauge", self.prefix, snapshot.name)
                    .map_err(|e| ExportError::FormatError { 
                        message: format!("Failed to write TYPE: {}", e) 
                    })?;
                
                if let MetricValue::Gauge(value) = snapshot.value {
                    write!(&mut output, "{}{}", self.prefix, snapshot.name)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write metric name: {}", e) 
                        })?;
                    
                    if !snapshot.labels.is_empty() {
                        write!(&mut output, "{{{}}}", self.format_labels(&snapshot.labels))
                            .map_err(|e| ExportError::FormatError { 
                                message: format!("Failed to write labels: {}", e) 
                            })?;
                    }
                    
                    writeln!(&mut output, " {}", value)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write value: {}", e) 
                        })?;
                }
            }
            
            MetricType::Histogram => {
                writeln!(&mut output, "# TYPE {}{} histogram", self.prefix, snapshot.name)
                    .map_err(|e| ExportError::FormatError { 
                        message: format!("Failed to write TYPE: {}", e) 
                    })?;
                
                if let MetricValue::Histogram { buckets, sum, count } = snapshot.value {
                    let base_name = format!("{}{}", self.prefix, snapshot.name);
                    
                    // Export bucket counts with cumulative values
                    let mut cumulative = 0u64;
                    for (value, count) in &buckets {
                        cumulative += count;
                        
                        write!(&mut output, "{}_bucket", base_name)
                            .map_err(|e| ExportError::FormatError { 
                                message: format!("Failed to write bucket name: {}", e) 
                            })?;
                        
                        // Add le label for bucket upper bound
                        let mut labels = snapshot.labels.clone();
                        labels.insert("le".to_string(), value.to_string());
                        
                        write!(&mut output, "{{{}}}", self.format_labels(&labels))
                            .map_err(|e| ExportError::FormatError { 
                                message: format!("Failed to write bucket labels: {}", e) 
                            })?;
                        
                        writeln!(&mut output, " {}", cumulative)
                            .map_err(|e| ExportError::FormatError { 
                                message: format!("Failed to write bucket value: {}", e) 
                            })?;
                    }
                    
                    // Export +Inf bucket
                    write!(&mut output, "{}_bucket", base_name)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write +Inf bucket: {}", e) 
                        })?;
                    
                    let mut labels = snapshot.labels.clone();
                    labels.insert("le".to_string(), "+Inf".to_string());
                    
                    writeln!(&mut output, "{{{}}} {}", self.format_labels(&labels), count)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write +Inf value: {}", e) 
                        })?;
                    
                    // Export sum
                    write!(&mut output, "{}_sum", base_name)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write sum name: {}", e) 
                        })?;
                    
                    if !snapshot.labels.is_empty() {
                        write!(&mut output, "{{{}}}", self.format_labels(&snapshot.labels))
                            .map_err(|e| ExportError::FormatError { 
                                message: format!("Failed to write sum labels: {}", e) 
                            })?;
                    }
                    
                    writeln!(&mut output, " {}", sum)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write sum value: {}", e) 
                        })?;
                    
                    // Export count
                    write!(&mut output, "{}_count", base_name)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write count name: {}", e) 
                        })?;
                    
                    if !snapshot.labels.is_empty() {
                        write!(&mut output, "{{{}}}", self.format_labels(&snapshot.labels))
                            .map_err(|e| ExportError::FormatError { 
                                message: format!("Failed to write count labels: {}", e) 
                            })?;
                    }
                    
                    writeln!(&mut output, " {}", count)
                        .map_err(|e| ExportError::FormatError { 
                            message: format!("Failed to write count value: {}", e) 
                        })?;
                }
            }
            
            MetricType::Summary => {
                // For now, treat summaries as gauges
                // In a full implementation, we'd export quantiles
                return Err(ExportError::UnsupportedMetricType {
                    metric_type: "Summary".to_string(),
                    exporter_name: "PrometheusExporter".to_string(),
                });
            }
        }
        
        Ok(output)
    }
    
    /// Format labels into Prometheus format: key="value",key2="value2"
    fn format_labels(&self, labels: &std::collections::HashMap<String, String>) -> String {
        let mut parts: Vec<String> = labels
            .iter()
            .map(|(k, v)| format!(r#"{}="{}""#, k, self.escape_label_value(v)))
            .collect();
        parts.sort(); // Ensure consistent ordering
        parts.join(",")
    }
    
    /// Escape label values according to Prometheus spec
    fn escape_label_value(&self, value: &str) -> String {
        value
            .replace('\\', r"\\")
            .replace('"', r#"\""#)
            .replace('\n', r"\n")
    }
    
    /// Generate help text for a metric
    fn help_text_for(&self, name: &str) -> String {
        // In a real implementation, this could be configured per metric
        match name {
            n if n.contains("rate") => "Number of events processed",
            n if n.contains("error") => "Number of errors encountered",
            n if n.contains("duration") => "Operation duration in milliseconds",
            n if n.contains("utilization") => "Resource utilization ratio (0.0-1.0)",
            n if n.contains("saturation") => "Resource saturation ratio (0.0-1.0)",
            n if n.contains("amendments") => "Number of configuration amendments",
            n if n.contains("anomalies") => "Number of anomalies detected",
            n if n.contains("failures") => "Number of failures encountered",
            _ => "Metric value",
        }.to_string()
    }
}

impl MetricExporter<String> for PrometheusExporter {
    fn export(&self, metric: &dyn Metric) -> Result<String, ExportError> {
        self.export_single(metric)
    }
    
    fn export_batch(&self, metrics: Vec<&dyn Metric>) -> Result<Vec<String>, ExportError> {
        // Export each metric independently - no state needed
        metrics.into_iter()
            .map(|metric| self.export(metric))
            .collect()
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Create HTTP endpoint for Prometheus scraping
#[cfg(feature = "warp")]
pub mod http {
    use super::*;
    use warp::{Filter, Reply};
    use std::sync::Arc;
    
    /// Create a warp filter for the /metrics endpoint
    pub fn metrics_endpoint(
        metrics: Arc<Vec<Box<dyn Metric>>>,
        exporter: Arc<PrometheusExporter>,
    ) -> impl Filter<Extract = impl Reply, Error = warp::Rejection> + Clone {
        warp::path("metrics")
            .and(warp::get())
            .map(move || {
                let mut output = String::new();
                
                // Export all metrics
                for metric in metrics.iter() {
                    match exporter.export(metric.as_ref()) {
                        Ok(formatted) => output.push_str(&formatted),
                        Err(e) => {
                            // Log error but continue with other metrics
                            eprintln!("Failed to export metric: {}", e);
                        }
                    }
                    output.push('\n');
                }
                
                warp::reply::with_header(
                    output,
                    "Content-Type",
                    "text/plain; version=0.0.4; charset=utf-8",
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::metrics::{
        NewMetricSnapshot as MetricSnapshot,
        NewMetricUpdate as MetricUpdate,
    };
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::sync::broadcast;
    
    // Mock metric for testing
    struct MockMetric {
        snapshot: MetricSnapshot,
    }
    
    impl Metric for MockMetric {
        fn name(&self) -> &str {
            &self.snapshot.name
        }
        
        fn snapshot(&self) -> MetricSnapshot {
            self.snapshot.clone()
        }
        
        fn update(&self, _value: MetricValue) {}
        
        fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
            let (_, rx) = broadcast::channel(1);
            rx
        }
    }
    
    #[test]
    fn test_counter_export() {
        let exporter = PrometheusExporter::new();
        
        let metric = MockMetric {
            snapshot: MetricSnapshot {
                name: "requests".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(42),
                timestamp: Instant::now(),
                labels: HashMap::new(),
            },
        };
        
        let result = exporter.export(&metric).unwrap();
        
        assert!(result.contains("# HELP flowstate_requests"));
        assert!(result.contains("# TYPE flowstate_requests counter"));
        assert!(result.contains("flowstate_requests_total 42"));
    }
    
    #[test]
    fn test_gauge_export() {
        let exporter = PrometheusExporter::new();
        
        let metric = MockMetric {
            snapshot: MetricSnapshot {
                name: "cpu_utilization".to_string(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(0.67),
                timestamp: Instant::now(),
                labels: HashMap::new(),
            },
        };
        
        let result = exporter.export(&metric).unwrap();
        
        assert!(result.contains("# TYPE flowstate_cpu_utilization gauge"));
        assert!(result.contains("flowstate_cpu_utilization 0.67"));
    }
    
    #[test]
    fn test_labels_formatting() {
        let exporter = PrometheusExporter::new();
        
        let mut labels = HashMap::new();
        labels.insert("service".to_string(), "api".to_string());
        labels.insert("region".to_string(), "us-west-2".to_string());
        
        let metric = MockMetric {
            snapshot: MetricSnapshot {
                name: "requests".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(100),
                timestamp: Instant::now(),
                labels,
            },
        };
        
        let result = exporter.export(&metric).unwrap();
        
        // Labels should be sorted alphabetically
        assert!(result.contains(r#"flowstate_requests_total{region="us-west-2",service="api"} 100"#));
    }
    
    #[test]
    fn test_label_escaping() {
        let exporter = PrometheusExporter::new();
        
        let escaped = exporter.escape_label_value(r#"test"value\with\special"#);
        assert_eq!(escaped, r#"test\"value\\with\\special"#);
    }
    
    #[test]
    fn test_custom_prefix() {
        let exporter = PrometheusExporter::new()
            .with_prefix("myapp_");
        
        let metric = MockMetric {
            snapshot: MetricSnapshot {
                name: "requests".to_string(),
                metric_type: MetricType::Counter,
                value: MetricValue::Counter(42),
                timestamp: Instant::now(),
                labels: HashMap::new(),
            },
        };
        
        let result = exporter.export(&metric).unwrap();
        
        assert!(result.contains("myapp_requests_total 42"));
    }
}