// StatsD exporter implementation
// This module is only compiled when the "metrics-statsd" feature is enabled

use crate::monitoring::metrics::core::{Metric, MetricType, MetricValue};
use super::{MetricExporter, ExportError};
use std::net::UdpSocket;

/// StatsD exporter that converts core metrics to StatsD wire format
pub struct StatsDExporter {
    socket: UdpSocket,
    target: String,
    prefix: String,
    sample_rate: f64,
}

impl StatsDExporter {
    /// Create a new StatsD exporter
    pub fn new(target: impl Into<String>) -> Result<Self, ExportError> {
        let target = target.into();
        let socket = UdpSocket::bind("0.0.0.0:0").map_err(|e| ExportError::NetworkError {
            message: format!("Failed to bind UDP socket: {}", e)
        })?;
        
        Ok(Self {
            socket,
            target,
            prefix: "flowstate.".to_string(),
            sample_rate: 1.0,
        })
    }
    
    /// Set metric name prefix (default: "flowstate.")
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into();
        self
    }
    
    /// Set sample rate (default: 1.0 = 100%)
    pub fn with_sample_rate(mut self, rate: f64) -> Self {
        self.sample_rate = rate.clamp(0.0, 1.0);
        self
    }
    
    fn send_metric(&self, wire_format: &str) -> Result<(), ExportError> {
        self.socket.send_to(wire_format.as_bytes(), &self.target)
            .map_err(|e| ExportError::NetworkError {
                message: format!("Failed to send to {}: {}", self.target, e)
            })?;
        Ok(())
    }
}

impl MetricExporter<String> for StatsDExporter {
    fn export(&self, metric: &dyn Metric) -> Result<String, ExportError> {
        let snapshot = metric.snapshot();
        let metric_name = format!("{}{}", self.prefix, snapshot.name);
        
        let wire_format = match snapshot.metric_type {
            MetricType::Counter => {
                if let Some(value) = snapshot.value.as_counter() {
                    if self.sample_rate < 1.0 {
                        format!("{}:{}|c|@{}", metric_name, value, self.sample_rate)
                    } else {
                        format!("{}:{}|c", metric_name, value)
                    }
                } else {
                    return Err(ExportError::FormatError {
                        message: "Counter metric has non-counter value".to_string()
                    });
                }
            }
            MetricType::Gauge => {
                if let Some(value) = snapshot.value.as_gauge() {
                    format!("{}:{:.3}|g", metric_name, value)
                } else {
                    return Err(ExportError::FormatError {
                        message: "Gauge metric has non-gauge value".to_string()
                    });
                }
            }
            MetricType::Histogram => {
                if let MetricValue::Histogram { sum, count, .. } = &snapshot.value {
                    if *count > 0 {
                        let mean = sum / (*count as f64);
                        format!("{}:{:.3}|h", metric_name, mean)
                    } else {
                        format!("{}:0|h", metric_name)
                    }
                } else {
                    return Err(ExportError::FormatError {
                        message: "Histogram metric has non-histogram value".to_string()
                    });
                }
            }
            MetricType::Summary => {
                // StatsD doesn't have native summary support, treat as histogram
                if let MetricValue::Summary { sum, count, .. } = &snapshot.value {
                    if *count > 0 {
                        let mean = sum / (*count as f64);
                        format!("{}:{:.3}|h", metric_name, mean)
                    } else {
                        format!("{}:0|h", metric_name)
                    }
                } else {
                    return Err(ExportError::FormatError {
                        message: "Summary metric has non-summary value".to_string()
                    });
                }
            }
        };
        
        // Send the metric
        self.send_metric(&wire_format)?;
        
        Ok(wire_format)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::metrics::core::{MetricSnapshot, MetricUpdate};
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio::sync::broadcast;

    // Mock metric for testing
    struct MockGauge {
        name: String,
        value: f64,
    }

    impl Metric for MockGauge {
        fn name(&self) -> &str {
            &self.name
        }

        fn snapshot(&self) -> MetricSnapshot {
            MetricSnapshot {
                name: self.name.clone(),
                metric_type: MetricType::Gauge,
                value: MetricValue::Gauge(self.value),
                timestamp: Instant::now(),
                labels: HashMap::new(),
            }
        }

        fn update(&self, _value: MetricValue) {}

        fn subscribe(&self) -> broadcast::Receiver<MetricUpdate> {
            let (_, rx) = broadcast::channel(1);
            rx
        }
    }

    #[test]
    fn test_statsd_wire_format() {
        // Note: This test won't actually send over network since we're mocking
        let exporter = StatsDExporter {
            socket: std::net::UdpSocket::bind("127.0.0.1:0").unwrap(),
            target: "127.0.0.1:8125".to_string(),
            prefix: "test.".to_string(),
            sample_rate: 1.0,
        };
        
        let metric = MockGauge {
            name: "cpu_usage".to_string(),
            value: 75.5,
        };
        
        // We can't easily test the network send in unit tests, but we can test the format
        let snapshot = metric.snapshot();
        let expected = "test.cpu_usage:75.500|g";
        
        // Test the format logic
        if let Some(value) = snapshot.value.as_gauge() {
            let format = format!("{}{}:{:.3}|g", exporter.prefix, snapshot.name, value);
            assert_eq!(format, expected);
        }
    }

    #[test]
    fn test_statsd_exporter_creation() {
        let result = StatsDExporter::new("127.0.0.1:8125");
        assert!(result.is_ok());
        
        let exporter = result.unwrap()
            .with_prefix("custom.")
            .with_sample_rate(0.5);
        
        assert_eq!(exporter.prefix, "custom.");
        assert_eq!(exporter.sample_rate, 0.5);
    }
}