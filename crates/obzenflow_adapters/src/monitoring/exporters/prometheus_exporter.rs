//! Clean Prometheus exporter implementation for FLOWIP-056-666
//!
//! This module implements the MetricsExporter trait for Prometheus exposition format.
//! It receives snapshots from collectors and renders them as Prometheus text.
//! No collection logic, no dependencies on aggregators - pure export functionality.

use obzenflow_core::metrics::{MetricsExporter, AppMetricsSnapshot, InfraMetricsSnapshot, HistogramSnapshot};
use std::sync::RwLock;
use std::error::Error;
use std::fmt::Write;

/// Prometheus exporter that formats metrics snapshots as Prometheus text
pub struct PrometheusExporter {
    /// Latest application metrics snapshot
    app_snapshot: RwLock<Option<AppMetricsSnapshot>>,
    
    /// Latest infrastructure metrics snapshot  
    infra_snapshot: RwLock<Option<InfraMetricsSnapshot>>,
}

impl PrometheusExporter {
    /// Create a new Prometheus exporter
    pub fn new() -> Self {
        Self {
            app_snapshot: RwLock::new(None),
            infra_snapshot: RwLock::new(None),
        }
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsExporter for PrometheusExporter {
    /// Update application metrics from event stream
    fn update_app_metrics(&self, snapshot: AppMetricsSnapshot) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut app_snapshot = self.app_snapshot.write()
            .map_err(|_| "Failed to acquire app snapshot write lock")?;
        *app_snapshot = Some(snapshot);
        Ok(())
    }
    
    /// Update infrastructure metrics from direct observation
    fn update_infra_metrics(&self, snapshot: InfraMetricsSnapshot) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut infra_snapshot = self.infra_snapshot.write()
            .map_err(|_| "Failed to acquire infra snapshot write lock")?;
        *infra_snapshot = Some(snapshot);
        Ok(())
    }
    
    /// Render all metrics in Prometheus exposition format
    fn render_metrics(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut output = String::with_capacity(4096);
        
        // Header
        writeln!(&mut output, "# ObzenFlow Metrics - FLOWIP-056-666")?;
        writeln!(&mut output, "# HELP obzenflow_build_info ObzenFlow build information")?;
        writeln!(&mut output, "# TYPE obzenflow_build_info gauge")?;
        writeln!(&mut output, "obzenflow_build_info{{version=\"0.1.0\"}} 1")?;
        writeln!(&mut output)?;
        
        // Render application metrics
        if let Ok(app_guard) = self.app_snapshot.read() {
            if let Some(ref snapshot) = *app_guard {
                self.render_app_metrics(&mut output, snapshot)?;
            }
        }
        
        // Render infrastructure metrics
        if let Ok(infra_guard) = self.infra_snapshot.read() {
            if let Some(ref snapshot) = *infra_guard {
                self.render_infra_metrics(&mut output, snapshot)?;
            }
        }
        
        Ok(output)
    }
}

impl PrometheusExporter {
    /// Render application metrics from snapshot
    fn render_app_metrics(&self, output: &mut String, snapshot: &AppMetricsSnapshot) -> Result<(), Box<dyn Error + Send + Sync>> {
        tracing::debug!("render_app_metrics: event_counts={}, error_counts={}, processing_times={}", 
            snapshot.event_counts.len(), snapshot.error_counts.len(), snapshot.processing_times.len());
        
        // Event counts
        if !snapshot.event_counts.is_empty() {
            writeln!(output, "# HELP obzenflow_events_total Total number of events processed")?;
            writeln!(output, "# TYPE obzenflow_events_total counter")?;
            
            for (key, count) in &snapshot.event_counts {
                // Parse key format "flow_name:stage_name"
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(
                        output, 
                        "obzenflow_events_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(parts[0]),
                        escape_label(parts[1]),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }
        
        // Error counts
        if !snapshot.error_counts.is_empty() {
            writeln!(output, "# HELP obzenflow_errors_total Total number of errors by stage")?;
            writeln!(output, "# TYPE obzenflow_errors_total counter")?;
            
            for (key, count) in &snapshot.error_counts {
                // Parse key format "flow_name:stage_name"
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(
                        output,
                        "obzenflow_errors_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(parts[0]),
                        escape_label(parts[1]),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }
        
        // Processing times
        if !snapshot.processing_times.is_empty() {
            writeln!(output, "# HELP obzenflow_processing_time_seconds Event processing duration in seconds")?;
            writeln!(output, "# TYPE obzenflow_processing_time_seconds histogram")?;
            
            for (key, hist) in &snapshot.processing_times {
                // Parse key format "flow_name:stage_name"
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    self.render_histogram(
                        output, 
                        "obzenflow_processing_time_seconds",
                        &[("flow", parts[0]), ("stage", parts[1])],
                        hist,
                        1.0 // Already in seconds - no conversion needed
                    )?;
                }
            }
            writeln!(output)?;
        }
        
        // In-flight events
        if !snapshot.in_flight.is_empty() {
            writeln!(output, "# HELP obzenflow_in_flight_events Number of events currently being processed")?;
            writeln!(output, "# TYPE obzenflow_in_flight_events gauge")?;
            
            for (key, value) in &snapshot.in_flight {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_in_flight_events{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Queue depth
        if !snapshot.queue_depth.is_empty() {
            writeln!(output, "# HELP obzenflow_queue_depth Current queue depth")?;
            writeln!(output, "# TYPE obzenflow_queue_depth gauge")?;
            
            for (key, value) in &snapshot.queue_depth {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_queue_depth{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // CPU usage
        if !snapshot.cpu_usage_ratio.is_empty() {
            writeln!(output, "# HELP obzenflow_cpu_usage_ratio CPU usage ratio (0.0-1.0)")?;
            writeln!(output, "# TYPE obzenflow_cpu_usage_ratio gauge")?;
            
            for (key, value) in &snapshot.cpu_usage_ratio {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_cpu_usage_ratio{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Memory usage
        if !snapshot.memory_bytes.is_empty() {
            writeln!(output, "# HELP obzenflow_memory_bytes Memory usage in bytes")?;
            writeln!(output, "# TYPE obzenflow_memory_bytes gauge")?;
            
            for (key, value) in &snapshot.memory_bytes {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_memory_bytes{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // SAAFE metrics - anomalies
        if !snapshot.anomalies_total.is_empty() {
            writeln!(output, "# HELP obzenflow_anomalies_total Total number of anomalies detected")?;
            writeln!(output, "# TYPE obzenflow_anomalies_total counter")?;
            
            for (key, count) in &snapshot.anomalies_total {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_anomalies_total{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), count)?;
                }
            }
            writeln!(output)?;
        }
        
        // SAAFE metrics - amendments
        if !snapshot.amendments_total.is_empty() {
            writeln!(output, "# HELP obzenflow_amendments_total Total number of amendments (lifecycle changes)")?;
            writeln!(output, "# TYPE obzenflow_amendments_total counter")?;
            
            for (key, count) in &snapshot.amendments_total {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_amendments_total{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), count)?;
                }
            }
            writeln!(output)?;
        }
        
        // SAAFE metrics - saturation
        if !snapshot.saturation_ratio.is_empty() {
            writeln!(output, "# HELP obzenflow_saturation_ratio Saturation ratio (0.0-1.0)")?;
            writeln!(output, "# TYPE obzenflow_saturation_ratio gauge")?;
            
            for (key, value) in &snapshot.saturation_ratio {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_saturation_ratio{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Flow latency histograms
        if !snapshot.flow_latency_seconds.is_empty() {
            writeln!(output, "# HELP obzenflow_flow_latency_seconds End-to-end flow latency in seconds")?;
            writeln!(output, "# TYPE obzenflow_flow_latency_seconds histogram")?;
            
            for (flow_name, histogram) in &snapshot.flow_latency_seconds {
                self.render_histogram(
                    output,
                    "obzenflow_flow_latency_seconds",
                    &[("flow", flow_name)],
                    histogram,
                    1.0, // Already in seconds
                )?;
            }
            writeln!(output)?;
        }
        
        // Dropped events
        if !snapshot.dropped_events.is_empty() {
            writeln!(output, "# HELP obzenflow_dropped_events Number of dropped events per flow")?;
            writeln!(output, "# TYPE obzenflow_dropped_events gauge")?;
            
            for (flow_name, value) in &snapshot.dropped_events {
                writeln!(output, "obzenflow_dropped_events{{flow=\"{}\"}} {}", 
                    escape_label(flow_name), value)?;
            }
            writeln!(output)?;
        }
        
        // Circuit breaker state
        if !snapshot.circuit_breaker_state.is_empty() {
            writeln!(output, "# HELP obzenflow_circuit_breaker_state Circuit breaker state (0=closed, 0.5=half_open, 1=open)")?;
            writeln!(output, "# TYPE obzenflow_circuit_breaker_state gauge")?;
            
            for (key, value) in &snapshot.circuit_breaker_state {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_circuit_breaker_state{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Circuit breaker rejection rate
        if !snapshot.circuit_breaker_rejection_rate.is_empty() {
            writeln!(output, "# HELP obzenflow_circuit_breaker_rejection_rate Circuit breaker rejection rate (0.0-1.0)")?;
            writeln!(output, "# TYPE obzenflow_circuit_breaker_rejection_rate gauge")?;
            
            for (key, value) in &snapshot.circuit_breaker_rejection_rate {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_circuit_breaker_rejection_rate{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Circuit breaker consecutive failures
        if !snapshot.circuit_breaker_consecutive_failures.is_empty() {
            writeln!(output, "# HELP obzenflow_circuit_breaker_consecutive_failures Consecutive failures in circuit breaker")?;
            writeln!(output, "# TYPE obzenflow_circuit_breaker_consecutive_failures gauge")?;
            
            for (key, value) in &snapshot.circuit_breaker_consecutive_failures {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_circuit_breaker_consecutive_failures{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Rate limiter delay rate
        if !snapshot.rate_limiter_delay_rate.is_empty() {
            writeln!(output, "# HELP obzenflow_rate_limiter_delay_rate Rate limiter delay rate (0.0-1.0)")?;
            writeln!(output, "# TYPE obzenflow_rate_limiter_delay_rate gauge")?;
            
            for (key, value) in &snapshot.rate_limiter_delay_rate {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_rate_limiter_delay_rate{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Rate limiter utilization
        if !snapshot.rate_limiter_utilization.is_empty() {
            writeln!(output, "# HELP obzenflow_rate_limiter_utilization Rate limiter utilization (0.0-1.0)")?;
            writeln!(output, "# TYPE obzenflow_rate_limiter_utilization gauge")?;
            
            for (key, value) in &snapshot.rate_limiter_utilization {
                let parts: Vec<&str> = key.splitn(2, ':').collect();
                if parts.len() == 2 {
                    writeln!(output, "obzenflow_rate_limiter_utilization{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(parts[0]), escape_label(parts[1]), value)?;
                }
            }
            writeln!(output)?;
        }
        
        // Flow metrics (if available)
        if let Some(ref flow_metrics) = snapshot.flow_metrics {
            // Journey counts
            writeln!(output, "# HELP obzenflow_journeys_opened_total Number of event journeys started")?;
            writeln!(output, "# TYPE obzenflow_journeys_opened_total counter")?;
            writeln!(output, "obzenflow_journeys_opened_total {}", flow_metrics.journeys_opened)?;
            writeln!(output)?;
            
            writeln!(output, "# HELP obzenflow_journeys_sealed_total Number of event journeys completed")?;
            writeln!(output, "# TYPE obzenflow_journeys_sealed_total counter")?;
            writeln!(output, "obzenflow_journeys_sealed_total {}", flow_metrics.journeys_sealed)?;
            writeln!(output)?;
            
            // Dropped events (opened - sealed)
            let dropped = flow_metrics.journeys_opened.saturating_sub(flow_metrics.journeys_sealed);
            writeln!(output, "# HELP obzenflow_dropped_events_total Number of events that didn't complete their journey")?;
            writeln!(output, "# TYPE obzenflow_dropped_events_total gauge")?;
            writeln!(output, "obzenflow_dropped_events_total {}", dropped)?;
            writeln!(output)?;
        }
        
        Ok(())
    }
    
    /// Render infrastructure metrics from snapshot
    fn render_infra_metrics(&self, output: &mut String, snapshot: &InfraMetricsSnapshot) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Journal metrics
        let journal = &snapshot.journal_metrics;
        
        writeln!(output, "# HELP obzenflow_journal_writes_total Total number of journal write operations")?;
        writeln!(output, "# TYPE obzenflow_journal_writes_total counter")?;
        writeln!(output, "obzenflow_journal_writes_total {}", journal.writes_total)?;
        writeln!(output)?;
        
        writeln!(output, "# HELP obzenflow_journal_bytes_written_total Total bytes written to journal")?;
        writeln!(output, "# TYPE obzenflow_journal_bytes_written_total counter")?;
        writeln!(output, "obzenflow_journal_bytes_written_total {}", journal.bytes_written)?;
        writeln!(output)?;
        
        writeln!(output, "# HELP obzenflow_journal_throughput_events_per_second Current journal throughput")?;
        writeln!(output, "# TYPE obzenflow_journal_throughput_events_per_second gauge")?;
        writeln!(output, "obzenflow_journal_throughput_events_per_second {}", journal.throughput)?;
        writeln!(output)?;
        
        // Journal write latency histogram
        writeln!(output, "# HELP obzenflow_journal_write_duration_seconds Journal write latency in seconds")?;
        writeln!(output, "# TYPE obzenflow_journal_write_duration_seconds histogram")?;
        self.render_histogram(
            output,
            "obzenflow_journal_write_duration_seconds",
            &[],
            &journal.write_latency,
            0.000001 // Convert microseconds to seconds
        )?;
        writeln!(output)?;
        
        // Stage infrastructure metrics
        if !snapshot.stage_metrics.is_empty() {
            writeln!(output, "# HELP obzenflow_stage_queue_depth Current number of events queued per stage")?;
            writeln!(output, "# TYPE obzenflow_stage_queue_depth gauge")?;
            
            for (stage, metrics) in &snapshot.stage_metrics {
                writeln!(
                    output,
                    "obzenflow_stage_queue_depth{{stage=\"{}\"}} {}",
                    escape_label(stage),
                    metrics.queue_depth
                )?;
            }
            writeln!(output)?;
            
            writeln!(output, "# HELP obzenflow_stage_in_flight Current number of events being processed per stage")?;
            writeln!(output, "# TYPE obzenflow_stage_in_flight gauge")?;
            
            for (stage, metrics) in &snapshot.stage_metrics {
                writeln!(
                    output,
                    "obzenflow_stage_in_flight{{stage=\"{}\"}} {}",
                    escape_label(stage),
                    metrics.in_flight
                )?;
            }
            writeln!(output)?;
        }
        
        Ok(())
    }
    
    /// Render a histogram in Prometheus format
    fn render_histogram(
        &self,
        output: &mut String,
        metric_name: &str,
        labels: &[(&str, &str)],
        histogram: &HistogramSnapshot,
        scale_factor: f64,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let label_str = format_labels(labels);
        
        // Standard Prometheus buckets
        let buckets = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0];
        
        // For each bucket, count how many observations fall below it
        for &bucket in &buckets {
            let count = if histogram.count == 0 {
                0
            } else {
                // Estimate bucket count based on percentiles
                // This is approximate but good enough for monitoring
                let bucket_ms = bucket / scale_factor;
                if bucket_ms <= histogram.min {
                    0
                } else if bucket_ms >= histogram.max {
                    histogram.count
                } else {
                    // Linear interpolation between known percentiles
                    estimate_bucket_count(histogram, bucket_ms)
                }
            };
            
            writeln!(
                output,
                "{}_bucket{{{}le=\"{}\"}} {}",
                metric_name,
                label_str,
                bucket,
                count
            )?;
        }
        
        // +Inf bucket
        writeln!(
            output,
            "{}_bucket{{{}le=\"+Inf\"}} {}",
            metric_name,
            label_str,
            histogram.count
        )?;
        
        // Sum and count
        writeln!(
            output,
            "{}_sum{{{}}} {}",
            metric_name,
            label_str,
            histogram.sum * scale_factor
        )?;
        
        writeln!(
            output,
            "{}_count{{{}}} {}",
            metric_name,
            label_str,
            histogram.count
        )?;
        
        Ok(())
    }
}

/// Escape a label value for Prometheus
fn escape_label(value: &str) -> String {
    value.replace('\\', "\\\\")
         .replace('"', "\\\"")
         .replace('\n', "\\n")
}

/// Format label pairs for Prometheus
fn format_labels(labels: &[(&str, &str)]) -> String {
    labels.iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label(v)))
        .collect::<Vec<_>>()
        .join(",")
}

/// Estimate bucket count based on percentiles
fn estimate_bucket_count(histogram: &HistogramSnapshot, bucket_value: f64) -> u64 {
    // Simple linear interpolation based on available percentiles
    if let (Some(p50), Some(p90), Some(p95), Some(p99)) = (
        histogram.percentiles.get("0.5"),
        histogram.percentiles.get("0.9"),
        histogram.percentiles.get("0.95"),
        histogram.percentiles.get("0.99"),
    ) {
        if bucket_value <= *p50 {
            (histogram.count as f64 * 0.5) as u64
        } else if bucket_value <= *p90 {
            (histogram.count as f64 * 0.9) as u64
        } else if bucket_value <= *p95 {
            (histogram.count as f64 * 0.95) as u64
        } else if bucket_value <= *p99 {
            (histogram.count as f64 * 0.99) as u64
        } else {
            histogram.count
        }
    } else {
        // Fallback to simple ratio
        let ratio = (bucket_value - histogram.min) / (histogram.max - histogram.min);
        (histogram.count as f64 * ratio) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    
    #[test]
    fn test_prometheus_format() {
        let exporter = PrometheusExporter::new();
        
        // Create a test app snapshot
        let mut event_counts = HashMap::new();
        event_counts.insert("order_flow:validation".to_string(), 100);
        
        let snapshot = AppMetricsSnapshot {
            timestamp: chrono::Utc::now(),
            event_counts,
            error_counts: HashMap::new(),
            processing_times: HashMap::new(),
            flow_metrics: None,
        };
        
        // Update and render
        exporter.update_app_metrics(snapshot).unwrap();
        let output = exporter.render_metrics().unwrap();
        
        // Check output
        assert!(output.contains("# TYPE obzenflow_events_total counter"));
        assert!(output.contains("obzenflow_events_total{flow=\"order_flow\",stage=\"validation\"} 100"));
    }
}