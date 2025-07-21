//! Prometheus endpoint for MetricsAggregator
//!
//! This module implements the Prometheus exposition format for metrics
//! collected by the MetricsAggregator (FLOWIP-056-666).

use super::MetricsAggregator;
use std::sync::{Arc, Mutex};
use std::fmt::Write;

/// Endpoint that exposes MetricsAggregator metrics in Prometheus format
pub struct MetricsEndpoint {
    aggregator: Arc<Mutex<MetricsAggregator>>,
}

impl MetricsEndpoint {
    /// Create a new metrics endpoint
    pub fn new(aggregator: Arc<Mutex<MetricsAggregator>>) -> Self {
        Self { aggregator }
    }
    
    /// Generate metrics in Prometheus exposition format
    pub fn render_metrics(&self) -> Result<String, std::fmt::Error> {
        // If the mutex is poisoned, we still want to read metrics (read-only operation)
        let aggregator = match self.aggregator.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(), // Recover from poison
        };
        let mut output = String::with_capacity(4096);
        
        // Add header comment
        writeln!(&mut output, "# ObzenFlow Metrics - FLOWIP-056-666 Wide Events")?;
        writeln!(&mut output, "# HELP obzenflow_build_info ObzenFlow build information")?;
        writeln!(&mut output, "# TYPE obzenflow_build_info gauge")?;
        writeln!(&mut output, "obzenflow_build_info{{version=\"0.1.0\"}} 1")?;
        writeln!(&mut output)?;
        
        // Format counters
        self.format_counters(&aggregator, &mut output)?;
        
        // Format gauges
        self.format_gauges(&aggregator, &mut output)?;
        
        // Format histograms
        self.format_histograms(&aggregator, &mut output)?;
        
        Ok(output)
    }
    
    /// Format counter metrics
    fn format_counters(&self, agg: &MetricsAggregator, out: &mut String) -> Result<(), std::fmt::Error> {
        // Events total counter
        if !agg.events_total.is_empty() {
            writeln!(out, "# HELP obzenflow_events_total Total number of events processed by each stage")?;
            writeln!(out, "# TYPE obzenflow_events_total counter")?;
            
            for (key, counter) in &agg.events_total {
                writeln!(
                    out,
                    "obzenflow_events_total{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    counter.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Errors total counter
        if !agg.errors_total.is_empty() {
            writeln!(out, "# HELP obzenflow_errors_total Total number of errors by stage")?;
            writeln!(out, "# TYPE obzenflow_errors_total counter")?;
            
            for (key, counter) in &agg.errors_total {
                writeln!(
                    out,
                    "obzenflow_errors_total{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    counter.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Anomalies total counter
        if !agg.anomalies_total.is_empty() {
            writeln!(out, "# HELP obzenflow_anomalies_total Total number of anomalies detected by stage")?;
            writeln!(out, "# TYPE obzenflow_anomalies_total counter")?;
            
            for (key, counter) in &agg.anomalies_total {
                writeln!(
                    out,
                    "obzenflow_anomalies_total{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    counter.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Amendments total counter
        if !agg.amendments_total.is_empty() {
            writeln!(out, "# HELP obzenflow_amendments_total Total number of lifecycle amendments by stage")?;
            writeln!(out, "# TYPE obzenflow_amendments_total counter")?;
            
            for (key, counter) in &agg.amendments_total {
                writeln!(
                    out,
                    "obzenflow_amendments_total{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    counter.get()
                )?;
            }
            writeln!(out)?;
        }
        
        Ok(())
    }
    
    /// Format gauge metrics
    fn format_gauges(&self, agg: &MetricsAggregator, out: &mut String) -> Result<(), std::fmt::Error> {
        // In-flight events gauge
        if !agg.in_flight.is_empty() {
            writeln!(out, "# HELP obzenflow_in_flight_events Number of events currently being processed")?;
            writeln!(out, "# TYPE obzenflow_in_flight_events gauge")?;
            
            for (key, gauge) in &agg.in_flight {
                writeln!(
                    out,
                    "obzenflow_in_flight_events{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        
        // CPU usage ratio gauge
        if !agg.cpu_usage_ratio.is_empty() {
            writeln!(out, "# HELP obzenflow_cpu_usage_ratio CPU usage ratio (0.0-1.0) for each stage")?;
            writeln!(out, "# TYPE obzenflow_cpu_usage_ratio gauge")?;
            
            for (key, gauge) in &agg.cpu_usage_ratio {
                writeln!(
                    out,
                    "obzenflow_cpu_usage_ratio{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Memory bytes gauge
        if !agg.memory_bytes.is_empty() {
            writeln!(out, "# HELP obzenflow_memory_bytes Memory usage in bytes for each stage")?;
            writeln!(out, "# TYPE obzenflow_memory_bytes gauge")?;
            
            for (key, gauge) in &agg.memory_bytes {
                writeln!(
                    out,
                    "obzenflow_memory_bytes{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Saturation ratio gauge
        if !agg.saturation_ratio.is_empty() {
            writeln!(out, "# HELP obzenflow_saturation_ratio Queue saturation ratio (0.0-1.0) for each stage")?;
            writeln!(out, "# TYPE obzenflow_saturation_ratio gauge")?;
            
            for (key, gauge) in &agg.saturation_ratio {
                writeln!(
                    out,
                    "obzenflow_saturation_ratio{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Circuit breaker state gauge
        if !agg.circuit_breaker_state.is_empty() {
            writeln!(out, "# HELP obzenflow_circuit_breaker_state Circuit breaker state (0=closed, 0.5=half_open, 1=open)")?;
            writeln!(out, "# TYPE obzenflow_circuit_breaker_state gauge")?;
            
            for (key, gauge) in &agg.circuit_breaker_state {
                writeln!(
                    out,
                    "obzenflow_circuit_breaker_state{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Circuit breaker rejection rate gauge
        if !agg.circuit_breaker_rejection_rate.is_empty() {
            writeln!(out, "# HELP obzenflow_circuit_breaker_rejection_rate Ratio of rejected requests by circuit breaker")?;
            writeln!(out, "# TYPE obzenflow_circuit_breaker_rejection_rate gauge")?;
            
            for (key, gauge) in &agg.circuit_breaker_rejection_rate {
                writeln!(
                    out,
                    "obzenflow_circuit_breaker_rejection_rate{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Circuit breaker consecutive failures gauge
        if !agg.circuit_breaker_consecutive_failures.is_empty() {
            writeln!(out, "# HELP obzenflow_circuit_breaker_consecutive_failures Number of consecutive failures in circuit breaker")?;
            writeln!(out, "# TYPE obzenflow_circuit_breaker_consecutive_failures gauge")?;
            
            for (key, gauge) in &agg.circuit_breaker_consecutive_failures {
                writeln!(
                    out,
                    "obzenflow_circuit_breaker_consecutive_failures{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Rate limiter delay rate gauge
        if !agg.rate_limiter_delay_rate.is_empty() {
            writeln!(out, "# HELP obzenflow_rate_limiter_delay_rate Ratio of requests delayed by rate limiter")?;
            writeln!(out, "# TYPE obzenflow_rate_limiter_delay_rate gauge")?;
            
            for (key, gauge) in &agg.rate_limiter_delay_rate {
                writeln!(
                    out,
                    "obzenflow_rate_limiter_delay_rate{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Rate limiter utilization gauge
        if !agg.rate_limiter_utilization.is_empty() {
            writeln!(out, "# HELP obzenflow_rate_limiter_utilization Rate limiter capacity utilization (0.0-1.0)")?;
            writeln!(out, "# TYPE obzenflow_rate_limiter_utilization gauge")?;
            
            for (key, gauge) in &agg.rate_limiter_utilization {
                writeln!(
                    out,
                    "obzenflow_rate_limiter_utilization{{flow=\"{}\",stage=\"{}\"}} {}",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        // Dropped events gauge
        if !agg.dropped_events.is_empty() {
            writeln!(out, "# HELP obzenflow_dropped_events Number of events that entered the flow but didn't reach the sink")?;
            writeln!(out, "# TYPE obzenflow_dropped_events gauge")?;
            
            for (flow_name, gauge) in &agg.dropped_events {
                writeln!(
                    out,
                    "obzenflow_dropped_events{{flow=\"{}\"}} {}",
                    escape_label(flow_name),
                    gauge.get()
                )?;
            }
            writeln!(out)?;
        }
        
        Ok(())
    }
    
    /// Format histogram metrics
    fn format_histograms(&self, agg: &MetricsAggregator, out: &mut String) -> Result<(), std::fmt::Error> {
        if !agg.duration_seconds.is_empty() {
            writeln!(out, "# HELP obzenflow_duration_seconds Event processing duration in seconds")?;
            writeln!(out, "# TYPE obzenflow_duration_seconds histogram")?;
            
            for (key, histogram) in &agg.duration_seconds {
                let labels = format!(
                    "flow=\"{}\",stage=\"{}\"",
                    escape_label(&key.flow_name),
                    escape_label(&key.stage_name)
                );
                
                // Output bucket counts
                let boundaries = histogram.bucket_boundaries();
                let bucket_counts = histogram.bucket_counts();
                let mut cumulative_count = 0u64;
                
                // Note: boundaries includes +Inf, but bucket_counts doesn't
                // So we iterate over bucket_counts length
                for i in 0..bucket_counts.len() {
                    cumulative_count += bucket_counts[i];
                    writeln!(
                        out,
                        "obzenflow_duration_seconds_bucket{{{}le=\"{}\"}} {}",
                        labels,
                        boundaries[i],
                        cumulative_count
                    )?;
                }
                
                // +Inf bucket is already included in the boundaries
                // No need for separate handling
                
                // Sum and count
                writeln!(
                    out,
                    "obzenflow_duration_seconds_sum{{{}}} {}",
                    labels,
                    histogram.sum()
                )?;
                writeln!(
                    out,
                    "obzenflow_duration_seconds_count{{{}}} {}",
                    labels,
                    histogram.count()
                )?;
            }
            writeln!(out)?;
        }
        
        // Flow latency histogram
        if !agg.flow_latency_seconds.is_empty() {
            writeln!(out, "# HELP obzenflow_flow_latency_seconds End-to-end flow processing latency in seconds")?;
            writeln!(out, "# TYPE obzenflow_flow_latency_seconds histogram")?;
            
            for (flow_name, histogram) in &agg.flow_latency_seconds {
                let labels = format!("flow=\"{}\"", escape_label(flow_name));
                
                // Output bucket counts
                let boundaries = histogram.bucket_boundaries();
                let bucket_counts = histogram.bucket_counts();
                let mut cumulative_count = 0u64;
                
                for i in 0..bucket_counts.len() {
                    cumulative_count += bucket_counts[i];
                    writeln!(
                        out,
                        "obzenflow_flow_latency_seconds_bucket{{{}le=\"{}\"}} {}",
                        labels,
                        boundaries[i],
                        cumulative_count
                    )?;
                }
                
                // Sum and count
                writeln!(
                    out,
                    "obzenflow_flow_latency_seconds_sum{{{}}} {}",
                    labels,
                    histogram.sum()
                )?;
                writeln!(
                    out,
                    "obzenflow_flow_latency_seconds_count{{{}}} {}",
                    labels,
                    histogram.count()
                )?;
            }
            writeln!(out)?;
        }
        
        Ok(())
    }
}

// Implement MetricsExporter trait for use in runtime layer
impl obzenflow_core::metrics::MetricsExporter for MetricsEndpoint {
    fn render_metrics(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        self.render_metrics()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }
    
    fn metric_count(&self) -> usize {
        let aggregator = match self.aggregator.lock() {
            Ok(agg) => agg,
            Err(_) => return 0,
        };
        
        aggregator.events_total.len()
            + aggregator.errors_total.len()
            + aggregator.duration_seconds.len()
            + aggregator.in_flight.len()
            + aggregator.cpu_usage_ratio.len()
            + aggregator.memory_bytes.len()
            + aggregator.anomalies_total.len()
            + aggregator.amendments_total.len()
            + aggregator.saturation_ratio.len()
            + aggregator.flow_latency_seconds.len()
            + aggregator.circuit_breaker_state.len()
            + aggregator.circuit_breaker_rejection_rate.len()
            + aggregator.circuit_breaker_consecutive_failures.len()
            + aggregator.rate_limiter_delay_rate.len()
            + aggregator.rate_limiter_utilization.len()
    }
}

/// Escape label values according to Prometheus spec
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::{
        ChainEvent,
        EventId,
        WriterId,
        EventEnvelope,
        event::processing_outcome::ProcessingOutcome,
        event::flow_context::{FlowContext, StageType},
        event::processing_info::ProcessingInfo,
    };
    use serde_json::json;
    
    fn create_test_aggregator() -> Arc<Mutex<MetricsAggregator>> {
        let agg = MetricsAggregator::new();
        Arc::new(Mutex::new(agg))
    }
    
    fn create_test_envelope(flow: &str, stage: &str, processing_time_ms: u64, outcome: ProcessingOutcome) -> EventEnvelope {
        let mut event = ChainEvent::new(
            EventId::new(),
            WriterId::new(),
            "data.test",
            json!({}),
        );
        
        event.flow_context = FlowContext {
            flow_name: flow.to_string(),
            flow_id: "test_flow_id".to_string(),
            stage_name: stage.to_string(),
            stage_type: StageType::Transform,
        };
        
        event.processing_info = ProcessingInfo {
            processing_time_ms,
            outcome,
            ..Default::default()
        };
        
        EventEnvelope::new(WriterId::new(), event)
    }
    
    #[tokio::test]
    async fn test_metrics_endpoint_empty() {
        let agg = create_test_aggregator();
        let endpoint = MetricsEndpoint::new(agg);
        
        let output = endpoint.render_metrics().unwrap();
        
        // Should have header but no metrics
        assert!(output.contains("# ObzenFlow Metrics"));
        assert!(output.contains("obzenflow_build_info"));
        assert!(!output.contains("obzenflow_events_total"));
    }
    
    #[tokio::test]
    async fn test_metrics_endpoint_with_events() {
        let agg = create_test_aggregator();
        
        // Process some test events
        {
            let mut aggregator = agg.lock().unwrap();
            
            // Success event
            let envelope1 = create_test_envelope("test_flow", "transform1", 100, ProcessingOutcome::Success);
            aggregator.process_event(&envelope1);
            
            // Error event
            let envelope2 = create_test_envelope("test_flow", "transform1", 50, ProcessingOutcome::Error("test error".to_string()));
            aggregator.process_event(&envelope2);
            
            // Another stage
            let envelope3 = create_test_envelope("test_flow", "transform2", 200, ProcessingOutcome::Success);
            aggregator.process_event(&envelope3);
        }
        
        let endpoint = MetricsEndpoint::new(agg);
        let output = endpoint.render_metrics().unwrap();
        
        // Check counters
        assert!(output.contains("# TYPE obzenflow_events_total counter"));
        assert!(output.contains("obzenflow_events_total{flow=\"test_flow\",stage=\"transform1\"} 2"));
        assert!(output.contains("obzenflow_events_total{flow=\"test_flow\",stage=\"transform2\"} 1"));
        
        // Check errors
        assert!(output.contains("# TYPE obzenflow_errors_total counter"));
        assert!(output.contains("obzenflow_errors_total{flow=\"test_flow\",stage=\"transform1\"} 1"));
        
        // Check histograms
        assert!(output.contains("# TYPE obzenflow_duration_seconds histogram"));
        assert!(output.contains("obzenflow_duration_seconds_bucket{flow=\"test_flow\",stage=\"transform1\""));
        assert!(output.contains("obzenflow_duration_seconds_sum{flow=\"test_flow\",stage=\"transform1\""));
        assert!(output.contains("obzenflow_duration_seconds_count{flow=\"test_flow\",stage=\"transform1\"} 2"));
    }
    
    #[tokio::test]
    async fn test_metrics_endpoint_with_gauges() {
        let agg = create_test_aggregator();
        
        // Process control events
        {
            let mut aggregator = agg.lock().unwrap();
            
            // State metrics control event
            let state_event = ChainEvent::control(
                ChainEvent::CONTROL_METRICS_STATE,
                json!({
                    "in_flight": 7
                })
            );
            let mut envelope = state_event.clone();
            envelope.flow_context = FlowContext {
                flow_name: "test_flow".to_string(),
                stage_name: "source1".to_string(),
                ..Default::default()
            };
            let wrapped = EventEnvelope::new(WriterId::new(), envelope);
            aggregator.process_event(&wrapped);
            
            // Resource metrics control event
            let resource_event = ChainEvent::control(
                ChainEvent::CONTROL_METRICS_RESOURCE,
                json!({
                    "cpu_usage_ratio": 0.75,
                    "memory_bytes": 1048576
                })
            );
            let mut envelope = resource_event.clone();
            envelope.flow_context = FlowContext {
                flow_name: "test_flow".to_string(),
                stage_name: "source1".to_string(),
                ..Default::default()
            };
            let wrapped = EventEnvelope::new(WriterId::new(), envelope);
            aggregator.process_event(&wrapped);
        }
        
        let endpoint = MetricsEndpoint::new(agg);
        let output = endpoint.render_metrics().unwrap();
        
        // Check gauges
        
        assert!(output.contains("# TYPE obzenflow_in_flight_events gauge"));
        assert!(output.contains("obzenflow_in_flight_events{flow=\"test_flow\",stage=\"source1\"} 7"));
        
        assert!(output.contains("# TYPE obzenflow_cpu_usage_ratio gauge"));
        assert!(output.contains("obzenflow_cpu_usage_ratio{flow=\"test_flow\",stage=\"source1\"} 0.75"));
        
        assert!(output.contains("# TYPE obzenflow_memory_bytes gauge"));
        assert!(output.contains("obzenflow_memory_bytes{flow=\"test_flow\",stage=\"source1\"} 1048576"));
    }
    
    #[test]
    fn test_escape_label() {
        assert_eq!(escape_label("simple"), "simple");
        assert_eq!(escape_label("with\"quotes"), "with\\\"quotes");
        assert_eq!(escape_label("with\\backslash"), "with\\\\backslash");
        assert_eq!(escape_label("with\nnewline"), "with\\nnewline");
    }
    
    #[tokio::test]
    async fn test_flow_latency_metrics_endpoint() {
        use obzenflow_core::event::correlation::{CorrelationId, CorrelationPayload};
        use crate::monitoring::metrics::primitives::Histogram;
        
        let agg = create_test_aggregator();
        
        // Process flow events
        {
            let mut aggregator = agg.lock().unwrap();
            let correlation_id = CorrelationId::new();
            
            // Source event
            let mut source_event = ChainEvent::new(
                EventId::new(),
                WriterId::new(),
                "order_created",
                json!({"order_id": "123"}),
            );
            source_event.correlation_id = Some(correlation_id);
            source_event.correlation_payload = Some(CorrelationPayload::new("order_api", source_event.id));
            source_event.flow_context = FlowContext {
                flow_name: "order_processing".to_string(),
                flow_id: "flow_456".to_string(),
                stage_name: "order_api".to_string(),
                stage_type: StageType::Source,
            };
            source_event.processing_info = ProcessingInfo {
                processing_time_ms: 10,
                outcome: ProcessingOutcome::Success,
                ..Default::default()
            };
            
            let source_envelope = EventEnvelope::new(WriterId::new(), source_event);
            aggregator.process_event(&source_envelope);
            
            // Manually insert completion for testing
            aggregator.flow_latency_seconds
                .entry("order_processing".to_string())
                .or_insert_with(Histogram::new)
                .observe(0.150); // 150ms
        }
        
        let endpoint = MetricsEndpoint::new(agg);
        let output = endpoint.render_metrics().unwrap();
        
        // Check flow latency histogram
        assert!(output.contains("# TYPE obzenflow_flow_latency_seconds histogram"));
        assert!(output.contains("obzenflow_flow_latency_seconds_bucket{flow=\"order_processing\""));
        assert!(output.contains("obzenflow_flow_latency_seconds_sum{flow=\"order_processing\"}"));
        assert!(output.contains("obzenflow_flow_latency_seconds_count{flow=\"order_processing\"} 1"));
    }
}