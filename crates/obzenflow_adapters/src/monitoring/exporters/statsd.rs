// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

// StatsD exporter implementation
// This module is only compiled when the "metrics-statsd" feature is enabled

use super::{ExportError, MetricExporter};
use crate::monitoring::metrics::core::{Metric, MetricSnapshot, MetricType, MetricValue};
use obzenflow_core::metrics::{
    AppMetricsSnapshot, InfraMetricsSnapshot, MetricsExporter, Percentile,
};
use std::net::UdpSocket;

fn boxed_export_error(e: ExportError) -> Box<dyn std::error::Error + Send + Sync> {
    Box::new(e)
}

fn build_wire_format(
    prefix: &str,
    sample_rate: f64,
    snapshot: &MetricSnapshot,
) -> Result<String, ExportError> {
    let metric_name = format!("{prefix}{}", snapshot.name);

    let wire_format = match snapshot.metric_type {
        MetricType::Counter => {
            if let Some(value) = snapshot.value.as_counter() {
                if sample_rate < 1.0 {
                    format!("{metric_name}:{value}|c|@{sample_rate}")
                } else {
                    format!("{metric_name}:{value}|c")
                }
            } else {
                return Err(ExportError::FormatError {
                    message: "Counter metric has non-counter value".to_string(),
                });
            }
        }
        MetricType::Gauge => {
            if let Some(value) = snapshot.value.as_gauge() {
                format!("{metric_name}:{value:.3}|g")
            } else {
                return Err(ExportError::FormatError {
                    message: "Gauge metric has non-gauge value".to_string(),
                });
            }
        }
        MetricType::Histogram => {
            if let MetricValue::Histogram { sum, count, .. } = &snapshot.value {
                if *count > 0 {
                    let mean = sum / (*count as f64);
                    format!("{metric_name}:{mean:.3}|h")
                } else {
                    format!("{metric_name}:0|h")
                }
            } else {
                return Err(ExportError::FormatError {
                    message: "Histogram metric has non-histogram value".to_string(),
                });
            }
        }
        MetricType::Summary => {
            // StatsD doesn't have native summary support, treat as histogram
            if let MetricValue::Summary { sum, count, .. } = &snapshot.value {
                if *count > 0 {
                    let mean = sum / (*count as f64);
                    format!("{metric_name}:{mean:.3}|h")
                } else {
                    format!("{metric_name}:0|h")
                }
            } else {
                return Err(ExportError::FormatError {
                    message: "Summary metric has non-summary value".to_string(),
                });
            }
        }
    };

    Ok(wire_format)
}

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
            message: format!("Failed to bind UDP socket: {e}"),
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
        self.socket
            .send_to(wire_format.as_bytes(), &self.target)
            .map_err(|e| ExportError::NetworkError {
                message: format!("Failed to send to {}: {}", self.target, e),
            })?;
        Ok(())
    }

    fn send_gauge(&self, name: &str, value: f64) -> Result<(), ExportError> {
        let wire_format = format!("{}{}:{:.3}|g", self.prefix, name, value);
        self.send_metric(&wire_format)
    }
}

impl MetricExporter<String> for StatsDExporter {
    fn export(&self, metric: &dyn Metric) -> Result<String, ExportError> {
        let snapshot = metric.snapshot();
        let wire_format = build_wire_format(&self.prefix, self.sample_rate, &snapshot)?;

        // Send the metric
        self.send_metric(&wire_format)?;

        Ok(wire_format)
    }
}

impl MetricsExporter for StatsDExporter {
    fn update_app_metrics(
        &self,
        snapshot: AppMetricsSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(flow_metrics) = &snapshot.flow_metrics {
            self.send_gauge("flow.events_in", flow_metrics.events_in as f64)
                .map_err(boxed_export_error)?;
            self.send_gauge("flow.events_out", flow_metrics.events_out as f64)
                .map_err(boxed_export_error)?;
            self.send_gauge("flow.errors_total", flow_metrics.errors_total as f64)
                .map_err(boxed_export_error)?;
            self.send_gauge(
                "flow.event_loops_total",
                flow_metrics.event_loops_total as f64,
            )
            .map_err(boxed_export_error)?;
            self.send_gauge(
                "flow.event_loops_with_work_total",
                flow_metrics.event_loops_with_work_total as f64,
            )
            .map_err(boxed_export_error)?;
        }

        for (stage_id, count) in &snapshot.event_counts {
            self.send_gauge(&format!("stage.{stage_id}.events_total"), *count as f64)
                .map_err(boxed_export_error)?;
        }

        for (stage_id, count) in &snapshot.error_counts {
            self.send_gauge(&format!("stage.{stage_id}.errors_total"), *count as f64)
                .map_err(boxed_export_error)?;
        }

        for (stage_id, value) in &snapshot.in_flight {
            self.send_gauge(&format!("stage.{stage_id}.in_flight"), *value)
                .map_err(boxed_export_error)?;
        }

        for (stage_id, hist) in &snapshot.processing_times {
            if hist.count == 0 {
                continue;
            }

            let mean_ms = (hist.sum / hist.count as f64) * 1_000.0;
            self.send_gauge(&format!("stage.{stage_id}.processing_time_ms.avg"), mean_ms)
                .map_err(boxed_export_error)?;

            for percentile in Percentile::all() {
                if let Some(value_seconds) = hist.percentiles.get(percentile) {
                    let value_ms = value_seconds * 1_000.0;
                    self.send_gauge(
                        &format!(
                            "stage.{stage_id}.processing_time_ms.{}",
                            percentile.as_str()
                        ),
                        value_ms,
                    )
                    .map_err(boxed_export_error)?;
                }
            }
        }

        Ok(())
    }

    fn update_infra_metrics(
        &self,
        snapshot: InfraMetricsSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let journal = &snapshot.journal_metrics;
        self.send_gauge("journal.writes_total", journal.writes_total as f64)
            .map_err(boxed_export_error)?;
        self.send_gauge("journal.throughput", journal.throughput)
            .map_err(boxed_export_error)?;
        self.send_gauge("journal.bytes_written", journal.bytes_written as f64)
            .map_err(boxed_export_error)?;
        self.send_gauge(
            "journal.write_latency_us.count",
            journal.write_latency.count as f64,
        )
        .map_err(boxed_export_error)?;
        self.send_gauge("journal.write_latency_us.min", journal.write_latency.min)
            .map_err(boxed_export_error)?;
        self.send_gauge("journal.write_latency_us.max", journal.write_latency.max)
            .map_err(boxed_export_error)?;

        for (stage_id, metrics) in &snapshot.stage_metrics {
            self.send_gauge(
                &format!("stage.{stage_id}.infra.in_flight"),
                metrics.in_flight as f64,
            )
            .map_err(boxed_export_error)?;
        }

        Ok(())
    }

    fn render_metrics(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(format!(
            "# StatsD exporter configured (push-only)\n# target: {}\n# prefix: {}\n",
            self.target, self.prefix
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Instant;

    #[test]
    fn test_statsd_wire_format_gauge() {
        let snapshot = MetricSnapshot {
            name: "cpu_usage".to_string(),
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(75.5),
            timestamp: Instant::now(),
            labels: HashMap::new(),
        };

        let wire_format = build_wire_format("test.", 1.0, &snapshot).unwrap();
        assert_eq!(wire_format, "test.cpu_usage:75.500|g");
    }

    #[test]
    fn test_statsd_wire_format_counter_sampled() {
        let snapshot = MetricSnapshot {
            name: "requests".to_string(),
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(42),
            timestamp: Instant::now(),
            labels: HashMap::new(),
        };

        let wire_format = build_wire_format("test.", 0.5, &snapshot).unwrap();
        assert_eq!(wire_format, "test.requests:42|c|@0.5");
    }

    #[test]
    fn test_statsd_wire_format_histogram_mean() {
        let snapshot = MetricSnapshot {
            name: "latency".to_string(),
            metric_type: MetricType::Histogram,
            value: MetricValue::Histogram {
                buckets: vec![],
                sum: 10.0,
                count: 4,
            },
            timestamp: Instant::now(),
            labels: HashMap::new(),
        };

        let wire_format = build_wire_format("test.", 1.0, &snapshot).unwrap();
        assert_eq!(wire_format, "test.latency:2.500|h");
    }
}
