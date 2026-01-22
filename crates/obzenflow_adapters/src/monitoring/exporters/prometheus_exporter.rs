//! Clean Prometheus exporter implementation for FLOWIP-056-666
//!
//! This module implements the MetricsExporter trait for Prometheus exposition format.
//! It receives snapshots from collectors and renders them as Prometheus text.
//! No collection logic, no dependencies on aggregators - pure export functionality.

use obzenflow_core::event::observability::{HttpPullState, WaitReason};
use obzenflow_core::event::status::processing_status::ErrorKind;
use obzenflow_core::metrics::{
    AppMetricsSnapshot, HistogramSnapshot, InfraMetricsSnapshot, MetricsExporter, StageMetadata,
};
use obzenflow_core::StageId;
use std::error::Error;
use std::fmt::Write;
use std::sync::RwLock;

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
    fn update_app_metrics(
        &self,
        snapshot: AppMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        tracing::info!(
            "update_app_metrics called with {} event counts",
            snapshot.event_counts.len()
        );
        let mut app_snapshot = self
            .app_snapshot
            .write()
            .map_err(|_| "Failed to acquire app snapshot write lock")?;
        *app_snapshot = Some(snapshot);
        Ok(())
    }

    /// Update infrastructure metrics from direct observation
    fn update_infra_metrics(
        &self,
        snapshot: InfraMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut infra_snapshot = self
            .infra_snapshot
            .write()
            .map_err(|_| "Failed to acquire infra snapshot write lock")?;
        *infra_snapshot = Some(snapshot);
        Ok(())
    }

    /// Render all metrics in Prometheus exposition format
    fn render_metrics(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let mut output = String::with_capacity(4096);

        // Header
        writeln!(&mut output, "# ObzenFlow Metrics - FLOWIP-056-666")?;
        writeln!(
            &mut output,
            "# HELP obzenflow_build_info ObzenFlow build information"
        )?;
        writeln!(&mut output, "# TYPE obzenflow_build_info gauge")?;
        writeln!(&mut output, "obzenflow_build_info{{version=\"0.1.0\"}} 1")?;
        writeln!(&mut output)?;

        // Render application metrics
        if let Ok(app_guard) = self.app_snapshot.read() {
            if let Some(ref snapshot) = *app_guard {
                tracing::debug!(
                    "Rendering app metrics snapshot with {} event counts",
                    snapshot.event_counts.len()
                );
                self.render_app_metrics(&mut output, snapshot)?;
            } else {
                tracing::debug!("No app metrics snapshot available");
            }
        }

        // Render infrastructure metrics
        if let Ok(infra_guard) = self.infra_snapshot.read() {
            if let Some(ref snapshot) = *infra_guard {
                tracing::debug!("Rendering infra metrics snapshot");
                self.render_infra_metrics(&mut output, snapshot)?;
            } else {
                tracing::debug!("No infra metrics snapshot available");
            }
        }

        Ok(output)
    }
}

impl PrometheusExporter {
    /// Render application metrics from snapshot
    fn render_app_metrics(
        &self,
        output: &mut String,
        snapshot: &AppMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        tracing::debug!(
            "render_app_metrics: event_counts={}, error_counts={}, processing_times={}",
            snapshot.event_counts.len(),
            snapshot.error_counts.len(),
            snapshot.processing_times.len()
        );

        // Event counts
        if !snapshot.event_counts.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_events_total Total number of events processed"
            )?;
            writeln!(output, "# TYPE obzenflow_events_total counter")?;

            for (stage_id, count) in &snapshot.event_counts {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_events_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Accumulated event counts (stateful/join stages)
        if !snapshot.events_accumulated_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_events_accumulated_total Total number of events accumulated into internal state"
            )?;
            writeln!(output, "# TYPE obzenflow_events_accumulated_total counter")?;

            for (stage_id, count) in &snapshot.events_accumulated_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_events_accumulated_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Emitted event counts (data/delivery outputs)
        if !snapshot.events_emitted_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_events_emitted_total Total number of output events emitted by stage"
            )?;
            writeln!(output, "# TYPE obzenflow_events_emitted_total counter")?;

            for (stage_id, count) in &snapshot.events_emitted_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_events_emitted_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Live join gauge: how many reference events have been processed since the last stream event.
        if !snapshot.join_reference_since_last_stream.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_join_reference_since_last_stream Join live reference backlog gauge"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_join_reference_since_last_stream gauge"
            )?;

            for (stage_id, value) in &snapshot.join_reference_since_last_stream {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_join_reference_since_last_stream{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Error counts by ErrorKind (if available).
        if !snapshot.error_counts_by_kind.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_errors_total Total number of errors by stage and kind"
            )?;
            writeln!(output, "# TYPE obzenflow_errors_total counter")?;

            for (stage_id, by_kind) in &snapshot.error_counts_by_kind {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    for (kind, count) in by_kind {
                        let kind_label = match kind {
                            ErrorKind::Timeout => "timeout",
                            ErrorKind::Remote => "remote",
                            ErrorKind::Deserialization => "deserialization",
                            ErrorKind::Validation => "validation",
                            ErrorKind::Domain => "domain",
                            ErrorKind::Unknown => "unknown",
                        };
                        writeln!(
                            output,
                            "obzenflow_errors_total{{{},error_kind=\"{}\"}} {}",
                            format_stage_labels(stage_id, metadata),
                            kind_label,
                            count
                        )?;
                    }
                }
            }
            writeln!(output)?;
        } else if !snapshot.error_counts.is_empty() {
            // Backward-compatible fallback when per-kind counts are not present.
            writeln!(
                output,
                "# HELP obzenflow_errors_total Total number of errors by stage"
            )?;
            writeln!(output, "# TYPE obzenflow_errors_total counter")?;

            for (stage_id, count) in &snapshot.error_counts {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_errors_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Processing times
        if !snapshot.processing_times.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_processing_time_seconds Event processing duration in seconds"
            )?;
            writeln!(output, "# TYPE obzenflow_processing_time_seconds histogram")?;

            for (stage_id, hist) in &snapshot.processing_times {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    let stage_id_str = stage_id.to_string();
                    let flow_id_str = metadata.flow_id.map(|id| id.to_string());
                    let mut labels: Vec<(&str, &str)> = vec![
                        ("flow", &metadata.flow_name),
                        ("stage", &metadata.name),
                        ("stage_id", &stage_id_str),
                    ];
                    if let Some(flow_id) = flow_id_str.as_deref() {
                        labels.insert(1, ("flow_id", flow_id));
                    }
                    self.render_histogram(
                        output,
                        "obzenflow_processing_time_seconds",
                        &labels,
                        hist,
                        1_000_000_000.0, // Convert nanoseconds to seconds
                    )?;
                }
            }
            writeln!(output)?;
        }

        // In-flight events
        if !snapshot.in_flight.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_in_flight_events Number of events currently being processed"
            )?;
            writeln!(output, "# TYPE obzenflow_in_flight_events gauge")?;

            for (stage_id, value) in &snapshot.in_flight {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_in_flight_events{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // CPU usage
        if !snapshot.cpu_usage_ratio.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_cpu_usage_ratio CPU usage ratio (0.0-1.0)"
            )?;
            writeln!(output, "# TYPE obzenflow_cpu_usage_ratio gauge")?;

            for (stage_id, value) in &snapshot.cpu_usage_ratio {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_cpu_usage_ratio{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Memory usage
        if !snapshot.memory_bytes.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_memory_bytes Memory usage in bytes"
            )?;
            writeln!(output, "# TYPE obzenflow_memory_bytes gauge")?;

            for (stage_id, value) in &snapshot.memory_bytes {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_memory_bytes{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // SAAFE metrics - anomalies
        if !snapshot.anomalies_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_anomalies_total Total number of anomalies detected"
            )?;
            writeln!(output, "# TYPE obzenflow_anomalies_total counter")?;

            for (stage_id, count) in &snapshot.anomalies_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_anomalies_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // SAAFE metrics - amendments
        if !snapshot.amendments_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_amendments_total Total number of amendments (lifecycle changes)"
            )?;
            writeln!(output, "# TYPE obzenflow_amendments_total counter")?;

            for (stage_id, count) in &snapshot.amendments_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_amendments_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // SAAFE metrics - saturation
        if !snapshot.saturation_ratio.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_saturation_ratio Saturation ratio (0.0-1.0)"
            )?;
            writeln!(output, "# TYPE obzenflow_saturation_ratio gauge")?;

            for (stage_id, value) in &snapshot.saturation_ratio {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_saturation_ratio{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // SAAFE metrics - failures
        if !snapshot.failures_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_failures_total Total number of critical failures"
            )?;
            writeln!(output, "# TYPE obzenflow_failures_total counter")?;

            for (stage_id, count) in &snapshot.failures_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_failures_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // USE metrics - event loops total
        if !snapshot.event_loops_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_event_loops_total Total number of event loop iterations"
            )?;
            writeln!(output, "# TYPE obzenflow_event_loops_total counter")?;

            for (stage_id, count) in &snapshot.event_loops_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_event_loops_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // USE metrics - event loops with work
        if !snapshot.event_loops_with_work_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_event_loops_with_work_total Event loop iterations that had work"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_event_loops_with_work_total counter"
            )?;

            for (stage_id, count) in &snapshot.event_loops_with_work_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_event_loops_with_work_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Flow latency histograms
        if !snapshot.flow_latency_seconds.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_flow_latency_seconds End-to-end flow latency in seconds"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_latency_seconds histogram")?;

            for (stage_id, histogram) in &snapshot.flow_latency_seconds {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    let stage_id_str = stage_id.to_string();
                    self.render_histogram(
                        output,
                        "obzenflow_flow_latency_seconds",
                        &[("flow", &metadata.flow_name), ("stage_id", &stage_id_str)],
                        histogram,
                        1.0, // Already in seconds
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Dropped events
        if !snapshot.dropped_events.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_dropped_events Number of dropped events per flow"
            )?;
            writeln!(output, "# TYPE obzenflow_dropped_events gauge")?;

            for (stage_id, value) in &snapshot.dropped_events {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_dropped_events{{flow=\"{}\",stage_id=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&stage_id.to_string()),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker state
        if !snapshot.circuit_breaker_state.is_empty() {
            writeln!(output, "# HELP obzenflow_circuit_breaker_state Circuit breaker state (0=closed, 0.5=half_open, 1=open)")?;
            writeln!(output, "# TYPE obzenflow_circuit_breaker_state gauge")?;

            for (stage_id, value) in &snapshot.circuit_breaker_state {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_state{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker rejection rate
        if !snapshot.circuit_breaker_rejection_rate.is_empty() {
            writeln!(output, "# HELP obzenflow_circuit_breaker_rejection_rate Circuit breaker rejection rate (0.0-1.0)")?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_rejection_rate gauge"
            )?;

            for (stage_id, value) in &snapshot.circuit_breaker_rejection_rate {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_rejection_rate{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker consecutive failures
        if !snapshot.circuit_breaker_consecutive_failures.is_empty() {
            writeln!(output, "# HELP obzenflow_circuit_breaker_consecutive_failures Consecutive failures in circuit breaker")?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_consecutive_failures gauge"
            )?;

            for (stage_id, value) in &snapshot.circuit_breaker_consecutive_failures {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_consecutive_failures{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker requests total
        if !snapshot.circuit_breaker_requests_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_circuit_breaker_requests_total Total requests processed by the circuit breaker"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_requests_total counter"
            )?;

            for (stage_id, count) in &snapshot.circuit_breaker_requests_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_requests_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker rejections total
        if !snapshot.circuit_breaker_rejections_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_circuit_breaker_rejections_total Total requests rejected by the circuit breaker"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_rejections_total counter"
            )?;

            for (stage_id, count) in &snapshot.circuit_breaker_rejections_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_rejections_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker opened total
        if !snapshot.circuit_breaker_opened_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_circuit_breaker_opened_total Total times circuit breaker has opened"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_opened_total counter"
            )?;

            for (stage_id, count) in &snapshot.circuit_breaker_opened_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_opened_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker successes total
        if !snapshot.circuit_breaker_successes_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_circuit_breaker_successes_total Total calls allowed by the breaker that did not count as failures"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_successes_total counter"
            )?;

            for (stage_id, count) in &snapshot.circuit_breaker_successes_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_successes_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker failures total
        if !snapshot.circuit_breaker_failures_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_circuit_breaker_failures_total Total calls allowed by the breaker that counted as failures"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_failures_total counter"
            )?;

            for (stage_id, count) in &snapshot.circuit_breaker_failures_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_failures_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker time in each state (monotonic)
        if !snapshot
            .circuit_breaker_time_in_state_seconds_total
            .is_empty()
        {
            writeln!(
                output,
                "# HELP obzenflow_circuit_breaker_time_in_state_seconds_total Total time spent in each circuit breaker state"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_time_in_state_seconds_total counter"
            )?;

            for ((stage_id, state), seconds_total) in
                &snapshot.circuit_breaker_time_in_state_seconds_total
            {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_time_in_state_seconds_total{{{},state=\"{}\"}} {}",
                        format_stage_labels(stage_id, metadata),
                        escape_label(state),
                        seconds_total
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Circuit breaker state transitions (monotonic)
        if !snapshot.circuit_breaker_state_transitions_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_circuit_breaker_state_transitions_total Total circuit breaker state transitions"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_circuit_breaker_state_transitions_total counter"
            )?;

            for ((stage_id, from_state, to_state), count) in
                &snapshot.circuit_breaker_state_transitions_total
            {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_circuit_breaker_state_transitions_total{{{},from_state=\"{}\",to_state=\"{}\"}} {}",
                        format_stage_labels(stage_id, metadata),
                        escape_label(from_state),
                        escape_label(to_state),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter utilization
        if !snapshot.rate_limiter_utilization.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_utilization Rate limiter utilization (0.0-1.0)"
            )?;
            writeln!(output, "# TYPE obzenflow_rate_limiter_utilization gauge")?;

            for (stage_id, value) in &snapshot.rate_limiter_utilization {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_utilization{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        value
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter events total
        if !snapshot.rate_limiter_events_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_events_total Total events processed by the rate limiter"
            )?;
            writeln!(output, "# TYPE obzenflow_rate_limiter_events_total counter")?;

            for (stage_id, count) in &snapshot.rate_limiter_events_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_events_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter delayed total
        if !snapshot.rate_limiter_delayed_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_delayed_total Total events delayed by the rate limiter"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_rate_limiter_delayed_total counter"
            )?;

            for (stage_id, count) in &snapshot.rate_limiter_delayed_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_delayed_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter tokens consumed total (monotonic)
        if !snapshot.rate_limiter_tokens_consumed_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_tokens_consumed_total Total tokens consumed by the rate limiter"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_rate_limiter_tokens_consumed_total counter"
            )?;

            for (stage_id, tokens) in &snapshot.rate_limiter_tokens_consumed_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_tokens_consumed_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        tokens
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter delay seconds total (monotonic)
        if !snapshot.rate_limiter_delay_seconds_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_delay_seconds_total Total time spent blocked waiting for tokens"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_rate_limiter_delay_seconds_total counter"
            )?;

            for (stage_id, seconds) in &snapshot.rate_limiter_delay_seconds_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_delay_seconds_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        seconds
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter bucket tokens (FLOWIP-059a-3 Issue 3)
        if !snapshot.rate_limiter_bucket_tokens.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_bucket_tokens Current tokens available in the rate limiter bucket"
            )?;
            writeln!(output, "# TYPE obzenflow_rate_limiter_bucket_tokens gauge")?;

            for (stage_id, tokens) in &snapshot.rate_limiter_bucket_tokens {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_bucket_tokens{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        tokens
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter bucket capacity (FLOWIP-059a-3 Issue 3)
        if !snapshot.rate_limiter_bucket_capacity.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_bucket_capacity Maximum capacity of the rate limiter bucket"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_rate_limiter_bucket_capacity gauge"
            )?;

            for (stage_id, capacity) in &snapshot.rate_limiter_bucket_capacity {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_bucket_capacity{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        capacity
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Backpressure metrics (FLOWIP-086k)
        let has_backpressure_metrics = !snapshot.backpressure_window.is_empty()
            || !snapshot.backpressure_in_flight.is_empty()
            || !snapshot.backpressure_credits.is_empty()
            || !snapshot.backpressure_blocked.is_empty()
            || !snapshot.backpressure_min_reader_seq.is_empty()
            || !snapshot.backpressure_writer_seq.is_empty()
            || !snapshot.backpressure_wait_seconds_total.is_empty()
            || snapshot.backpressure_bypass_enabled;

        // Flow-level bypass flag (debug-only)
        if has_backpressure_metrics {
            let flow_name = snapshot
                .stage_metadata
                .values()
                .next()
                .map(|m| m.flow_name.as_str())
                .unwrap_or("unknown");
            let flow_id = snapshot.stage_metadata.values().find_map(|m| m.flow_id);
            let enabled = if snapshot.backpressure_bypass_enabled {
                1
            } else {
                0
            };

            writeln!(
                output,
                "# HELP obzenflow_backpressure_bypass_enabled Whether OBZENFLOW_BACKPRESSURE_DISABLED is active (debug-only)"
            )?;
            writeln!(output, "# TYPE obzenflow_backpressure_bypass_enabled gauge")?;
            if let Some(flow_id) = flow_id {
                writeln!(
                    output,
                    "obzenflow_backpressure_bypass_enabled{{flow=\"{}\",flow_id=\"{}\"}} {}",
                    escape_label(flow_name),
                    escape_label(&flow_id.to_string()),
                    enabled
                )?;
            } else {
                writeln!(
                    output,
                    "obzenflow_backpressure_bypass_enabled{{flow=\"{}\"}} {}",
                    escape_label(flow_name),
                    enabled
                )?;
            }
            writeln!(output)?;
        }

        // Edge-scoped window
        if !snapshot.backpressure_window.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_backpressure_window Backpressure window size per edge (upstream->downstream)"
            )?;
            writeln!(output, "# TYPE obzenflow_backpressure_window gauge")?;

            for ((upstream, downstream), window) in &snapshot.backpressure_window {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                let upstream_name = upstream_meta.map(|m| m.name.as_str()).unwrap_or("unknown");
                let downstream_name = downstream_meta
                    .map(|m| m.name.as_str())
                    .unwrap_or("unknown");

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_backpressure_window{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        window
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_backpressure_window{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        window
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Edge-scoped in-flight
        if !snapshot.backpressure_in_flight.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_backpressure_in_flight In-flight events per edge (upstream effective writer - downstream reader)"
            )?;
            writeln!(output, "# TYPE obzenflow_backpressure_in_flight gauge")?;

            for ((upstream, downstream), in_flight) in &snapshot.backpressure_in_flight {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                let upstream_name = upstream_meta.map(|m| m.name.as_str()).unwrap_or("unknown");
                let downstream_name = downstream_meta
                    .map(|m| m.name.as_str())
                    .unwrap_or("unknown");

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_backpressure_in_flight{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        in_flight
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_backpressure_in_flight{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        in_flight
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Edge-scoped credits
        if !snapshot.backpressure_credits.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_backpressure_credits Remaining credits per edge (window - in_flight)"
            )?;
            writeln!(output, "# TYPE obzenflow_backpressure_credits gauge")?;

            for ((upstream, downstream), credits) in &snapshot.backpressure_credits {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                let upstream_name = upstream_meta.map(|m| m.name.as_str()).unwrap_or("unknown");
                let downstream_name = downstream_meta
                    .map(|m| m.name.as_str())
                    .unwrap_or("unknown");

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_backpressure_credits{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        credits
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_backpressure_credits{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        credits
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Stage-scoped blocked flag
        if !snapshot.backpressure_blocked.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_backpressure_blocked Whether the stage is blocked on downstream credits (0/1)"
            )?;
            writeln!(output, "# TYPE obzenflow_backpressure_blocked gauge")?;

            for (stage_id, blocked) in &snapshot.backpressure_blocked {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_backpressure_blocked{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        blocked
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Stage-scoped min reader sequence
        if !snapshot.backpressure_min_reader_seq.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_backpressure_min_reader_seq Minimum downstream reader sequence observed by the stage"
            )?;
            writeln!(output, "# TYPE obzenflow_backpressure_min_reader_seq gauge")?;

            for (stage_id, seq) in &snapshot.backpressure_min_reader_seq {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_backpressure_min_reader_seq{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        seq
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Stage-scoped writer sequence
        if !snapshot.backpressure_writer_seq.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_backpressure_writer_seq Writer sequence observed by the stage"
            )?;
            writeln!(output, "# TYPE obzenflow_backpressure_writer_seq gauge")?;

            for (stage_id, seq) in &snapshot.backpressure_writer_seq {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_backpressure_writer_seq{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        seq
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Stage-scoped wait seconds total (monotonic)
        if !snapshot.backpressure_wait_seconds_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_backpressure_wait_seconds_total Total time spent blocked waiting for downstream credits"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_backpressure_wait_seconds_total counter"
            )?;

            for (stage_id, seconds) in &snapshot.backpressure_wait_seconds_total {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_backpressure_wait_seconds_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        seconds
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Flow metrics (if available)
        if let Some(ref flow_metrics) = snapshot.flow_metrics {
            // Get flow name from stage metadata (all stages belong to same flow)
            let flow_name = snapshot
                .stage_metadata
                .values()
                .next()
                .map(|m| m.flow_name.as_str())
                .unwrap_or("unknown");
            // Prefer an explicit flow_id observed in stage metadata; fall back to the legacy
            // heuristic of using an arbitrary stage_id when flow_id is not yet available.
            let flow_id_label = snapshot
                .stage_metadata
                .values()
                .find_map(|m| m.flow_id)
                .map(|id| id.to_string())
                .or_else(|| {
                    snapshot
                        .stage_metadata
                        .keys()
                        .next()
                        .map(|id| id.to_string())
                })
                .unwrap_or_else(|| "unknown".to_string());

            // Event counts
            writeln!(
                output,
                "# HELP obzenflow_flow_events_in_total Events entering from sources"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_events_in_total counter")?;
            writeln!(
                output,
                "obzenflow_flow_events_in_total{{flow=\"{}\",flow_id=\"{}\"}} {}",
                flow_name,
                escape_label(&flow_id_label),
                flow_metrics.events_in
            )?;
            writeln!(output)?;

            writeln!(
                output,
                "# HELP obzenflow_flow_events_out_total Events exiting through sinks"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_events_out_total counter")?;
            writeln!(
                output,
                "obzenflow_flow_events_out_total{{flow=\"{}\",flow_id=\"{}\"}} {}",
                flow_name,
                escape_label(&flow_id_label),
                flow_metrics.events_out
            )?;
            writeln!(output)?;

            writeln!(
                output,
                "# HELP obzenflow_flow_errors_total Total errors across all stages"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_errors_total counter")?;
            writeln!(
                output,
                "obzenflow_flow_errors_total{{flow=\"{}\",flow_id=\"{}\"}} {}",
                flow_name,
                escape_label(&flow_id_label),
                flow_metrics.errors_total
            )?;
            writeln!(output)?;

            // Work tracking
            writeln!(
                output,
                "# HELP obzenflow_flow_event_loops_total Total event loops across all stages"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_event_loops_total counter")?;
            writeln!(
                output,
                "obzenflow_flow_event_loops_total{{flow=\"{}\",flow_id=\"{}\"}} {}",
                flow_name,
                escape_label(&flow_id_label),
                flow_metrics.event_loops_total
            )?;
            writeln!(output)?;

            writeln!(output, "# HELP obzenflow_flow_event_loops_with_work_total Event loops with work across all stages")?;
            writeln!(
                output,
                "# TYPE obzenflow_flow_event_loops_with_work_total counter"
            )?;
            writeln!(
                output,
                "obzenflow_flow_event_loops_with_work_total{{flow=\"{}\",flow_id=\"{}\"}} {}",
                flow_name,
                escape_label(&flow_id_label),
                flow_metrics.event_loops_with_work_total
            )?;
            writeln!(output)?;

            // Gauges
            let utilization = if flow_metrics.event_loops_total > 0 {
                flow_metrics.event_loops_with_work_total as f64
                    / flow_metrics.event_loops_total as f64
            } else {
                0.0
            };
            writeln!(
                output,
                "# HELP obzenflow_flow_utilization Flow utilization (0.0-1.0)"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_utilization gauge")?;
            writeln!(
                output,
                "obzenflow_flow_utilization{{flow=\"{}\",flow_id=\"{}\"}} {}",
                flow_name,
                escape_label(&flow_id_label),
                utilization
            )?;
            writeln!(output)?;
        }

        // Contract metrics (FLOWIP-059a)
        if !snapshot.contract_metrics.results_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_contract_results_total Contract verification results per edge"
            )?;
            writeln!(output, "# TYPE obzenflow_contract_results_total counter")?;

            for ((upstream, downstream, contract, status), count) in
                &snapshot.contract_metrics.results_total
            {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                let upstream_name = upstream_meta.map(|m| m.name.as_str()).unwrap_or("unknown");
                let downstream_name = downstream_meta
                    .map(|m| m.name.as_str())
                    .unwrap_or("unknown");

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_contract_results_total{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\",contract=\"{}\",status=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        escape_label(contract),
                        escape_label(status),
                        count
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_contract_results_total{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\",contract=\"{}\",status=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        escape_label(contract),
                        escape_label(status),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        if !snapshot.contract_metrics.violations_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_contract_violations_total Contract violations per edge"
            )?;
            writeln!(output, "# TYPE obzenflow_contract_violations_total counter")?;

            for ((upstream, downstream, contract, cause), count) in
                &snapshot.contract_metrics.violations_total
            {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                let upstream_name = upstream_meta.map(|m| m.name.as_str()).unwrap_or("unknown");
                let downstream_name = downstream_meta
                    .map(|m| m.name.as_str())
                    .unwrap_or("unknown");

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_contract_violations_total{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\",contract=\"{}\",cause=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        escape_label(contract),
                        escape_label(cause),
                        count
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_contract_violations_total{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\",contract=\"{}\",cause=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        escape_label(contract),
                        escape_label(cause),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        if !snapshot.contract_metrics.overrides_total.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_contract_overrides_total Contract decisions overridden by policy"
            )?;
            writeln!(output, "# TYPE obzenflow_contract_overrides_total counter")?;

            for ((upstream, downstream, contract, policy), count) in
                &snapshot.contract_metrics.overrides_total
            {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                let upstream_name = upstream_meta.map(|m| m.name.as_str()).unwrap_or("unknown");
                let downstream_name = downstream_meta
                    .map(|m| m.name.as_str())
                    .unwrap_or("unknown");

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_contract_overrides_total{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\",contract=\"{}\",policy=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        escape_label(contract),
                        escape_label(policy),
                        count
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_contract_overrides_total{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",upstream=\"{}\",downstream=\"{}\",contract=\"{}\",policy=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(upstream_name),
                        escape_label(downstream_name),
                        escape_label(contract),
                        escape_label(policy),
                        count
                    )?;
                }
            }
            writeln!(output)?;
        }

        if !snapshot.contract_metrics.reader_seq.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_contract_reader_seq Latest reader sequence per contract edge"
            )?;
            writeln!(output, "# TYPE obzenflow_contract_reader_seq gauge")?;

            for ((upstream, downstream, contract), seq) in &snapshot.contract_metrics.reader_seq {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_contract_reader_seq{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",contract=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(contract),
                        seq
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_contract_reader_seq{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",contract=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(contract),
                        seq
                    )?;
                }
            }
            writeln!(output)?;
        }

        if !snapshot.contract_metrics.advertised_writer_seq.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_contract_advertised_writer_seq Latest advertised writer sequence per contract edge"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_contract_advertised_writer_seq gauge"
            )?;

            for ((upstream, downstream, contract), seq) in
                &snapshot.contract_metrics.advertised_writer_seq
            {
                let upstream_id = upstream.to_string();
                let downstream_id = downstream.to_string();

                let upstream_meta = snapshot.stage_metadata.get(upstream);
                let downstream_meta = snapshot.stage_metadata.get(downstream);

                let flow_name = upstream_meta
                    .map(|m| m.flow_name.as_str())
                    .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                    .unwrap_or("unknown");
                let flow_id = upstream_meta
                    .and_then(|m| m.flow_id)
                    .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                if let Some(flow_id) = flow_id {
                    writeln!(
                        output,
                        "obzenflow_contract_advertised_writer_seq{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",contract=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&flow_id.to_string()),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(contract),
                        seq
                    )?;
                } else {
                    writeln!(
                        output,
                        "obzenflow_contract_advertised_writer_seq{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",contract=\"{}\"}} {}",
                        escape_label(flow_name),
                        escape_label(&upstream_id),
                        escape_label(&downstream_id),
                        escape_label(contract),
                        seq
                    )?;
                }
            }
            writeln!(output)?;
        }

        // Lag in events per contract edge (best-effort; only when advertised is present).
        if !snapshot.contract_metrics.reader_seq.is_empty()
            && !snapshot.contract_metrics.advertised_writer_seq.is_empty()
        {
            writeln!(
                output,
                "# HELP obzenflow_contract_lag_events Latest observed lag in events per contract edge"
            )?;
            writeln!(output, "# TYPE obzenflow_contract_lag_events gauge")?;

            for ((upstream, downstream, contract), reader_seq) in
                &snapshot.contract_metrics.reader_seq
            {
                if let Some(advertised) = snapshot.contract_metrics.advertised_writer_seq.get(&(
                    *upstream,
                    *downstream,
                    contract.clone(),
                )) {
                    let lag = advertised.saturating_sub(*reader_seq);

                    let upstream_id = upstream.to_string();
                    let downstream_id = downstream.to_string();

                    let upstream_meta = snapshot.stage_metadata.get(upstream);
                    let downstream_meta = snapshot.stage_metadata.get(downstream);

                    let flow_name = upstream_meta
                        .map(|m| m.flow_name.as_str())
                        .or_else(|| downstream_meta.map(|m| m.flow_name.as_str()))
                        .unwrap_or("unknown");
                    let flow_id = upstream_meta
                        .and_then(|m| m.flow_id)
                        .or_else(|| downstream_meta.and_then(|m| m.flow_id));

                    if let Some(flow_id) = flow_id {
                        writeln!(
                            output,
                            "obzenflow_contract_lag_events{{flow=\"{}\",flow_id=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",contract=\"{}\"}} {}",
                            escape_label(flow_name),
                            escape_label(&flow_id.to_string()),
                            escape_label(&upstream_id),
                            escape_label(&downstream_id),
                            escape_label(contract),
                            lag
                        )?;
                    } else {
                        writeln!(
                            output,
                            "obzenflow_contract_lag_events{{flow=\"{}\",upstream_stage_id=\"{}\",downstream_stage_id=\"{}\",contract=\"{}\"}} {}",
                            escape_label(flow_name),
                            escape_label(&upstream_id),
                            escape_label(&downstream_id),
                            escape_label(contract),
                            lag
                        )?;
                    }
                }
            }
            writeln!(output)?;
        }

        // Stage lifecycle state metrics (FLOWIP-059b - essential events only)
        // Shows ALL states each stage has been in
        if !snapshot.stage_lifecycle_states.is_empty() {
            writeln!(output, "# HELP obzenflow_stage_lifecycle_state Stage lifecycle state (1 = stage has been in this state)")?;
            writeln!(output, "# TYPE obzenflow_stage_lifecycle_state gauge")?;

            for ((stage_id, state), &seen) in &snapshot.stage_lifecycle_states {
                if seen {
                    if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                        writeln!(
                            output,
                            "obzenflow_stage_lifecycle_state{{{},state=\"{}\"}} 1",
                            format_stage_labels(stage_id, metadata),
                            escape_label(state)
                        )?;
                    }
                }
            }
            writeln!(output)?;
        }

        // Pipeline state metric (FLOWIP-059b)
        if !snapshot.pipeline_state.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_pipeline_state Pipeline lifecycle state"
            )?;
            writeln!(output, "# TYPE obzenflow_pipeline_state gauge")?;
            writeln!(
                output,
                "obzenflow_pipeline_state{{state=\"{}\"}} 1",
                escape_label(&snapshot.pipeline_state)
            )?;
            writeln!(output)?;
        }

        // Stage vector clock watermarks (FLOWIP-059c)
        if !snapshot.stage_vector_clocks.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_stage_vector_clock Last vector clock sequence observed for this stage"
            )?;
            writeln!(output, "# TYPE obzenflow_stage_vector_clock gauge")?;

            for (stage_id, seq) in &snapshot.stage_vector_clocks {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_stage_vector_clock{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        seq
                    )?;
                }
            }
            writeln!(output)?;
        }

        // HTTP ingestion telemetry (FLOWIP-084d).
        if !snapshot.ingestion_metrics.is_empty() {
            writeln!(
                output,
                "# HELP http_ingestion_requests_total Total HTTP ingestion requests"
            )?;
            writeln!(output, "# TYPE http_ingestion_requests_total counter")?;
            for metrics in snapshot.ingestion_metrics.values() {
                writeln!(
                    output,
                    "http_ingestion_requests_total{{base_path=\"{}\"}} {}",
                    escape_label(&metrics.base_path),
                    metrics.requests_total
                )?;
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_ingestion_events_accepted_total Total accepted events"
            )?;
            writeln!(
                output,
                "# TYPE http_ingestion_events_accepted_total counter"
            )?;
            for metrics in snapshot.ingestion_metrics.values() {
                writeln!(
                    output,
                    "http_ingestion_events_accepted_total{{base_path=\"{}\"}} {}",
                    escape_label(&metrics.base_path),
                    metrics.events_accepted_total
                )?;
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_ingestion_events_rejected_total Total rejected events by reason"
            )?;
            writeln!(
                output,
                "# TYPE http_ingestion_events_rejected_total counter"
            )?;
            for metrics in snapshot.ingestion_metrics.values() {
                let base_path = escape_label(&metrics.base_path);
                writeln!(
                    output,
                    "http_ingestion_events_rejected_total{{base_path=\"{}\",reason=\"auth\"}} {}",
                    base_path, metrics.events_rejected_auth_total
                )?;
                writeln!(
                    output,
                    "http_ingestion_events_rejected_total{{base_path=\"{}\",reason=\"validation\"}} {}",
                    base_path,
                    metrics.events_rejected_validation_total
                )?;
                writeln!(
                    output,
                    "http_ingestion_events_rejected_total{{base_path=\"{}\",reason=\"buffer_full\"}} {}",
                    base_path,
                    metrics.events_rejected_buffer_full_total
                )?;
                writeln!(
                    output,
                    "http_ingestion_events_rejected_total{{base_path=\"{}\",reason=\"not_ready\"}} {}",
                    base_path,
                    metrics.events_rejected_not_ready_total
                )?;
                writeln!(
                    output,
                    "http_ingestion_events_rejected_total{{base_path=\"{}\",reason=\"payload_too_large\"}} {}",
                    base_path,
                    metrics.events_rejected_payload_too_large_total
                )?;
                writeln!(
                    output,
                    "http_ingestion_events_rejected_total{{base_path=\"{}\",reason=\"invalid_json\"}} {}",
                    base_path,
                    metrics.events_rejected_invalid_json_total
                )?;
                writeln!(
                    output,
                    "http_ingestion_events_rejected_total{{base_path=\"{}\",reason=\"channel_closed\"}} {}",
                    base_path,
                    metrics.events_rejected_channel_closed_total
                )?;
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_ingestion_channel_depth Current ingestion channel depth"
            )?;
            writeln!(output, "# TYPE http_ingestion_channel_depth gauge")?;
            for metrics in snapshot.ingestion_metrics.values() {
                writeln!(
                    output,
                    "http_ingestion_channel_depth{{base_path=\"{}\"}} {}",
                    escape_label(&metrics.base_path),
                    metrics.channel_depth
                )?;
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_ingestion_channel_capacity Ingestion channel capacity"
            )?;
            writeln!(output, "# TYPE http_ingestion_channel_capacity gauge")?;
            for metrics in snapshot.ingestion_metrics.values() {
                writeln!(
                    output,
                    "http_ingestion_channel_capacity{{base_path=\"{}\"}} {}",
                    escape_label(&metrics.base_path),
                    metrics.channel_capacity
                )?;
            }
            writeln!(output)?;
        }

        // HTTP pull telemetry (FLOWIP-084e).
        if !snapshot.http_pull_metrics.is_empty() {
            writeln!(
                output,
                "# HELP http_pull_waiting Indicates the source is waiting (1) or not (0), by reason"
            )?;
            writeln!(output, "# TYPE http_pull_waiting gauge")?;
            for (stage_id, metrics) in &snapshot.http_pull_metrics {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    let stage_labels = format_stage_labels(stage_id, metadata);
                    let waiting_reason = if matches!(metrics.state, HttpPullState::Waiting) {
                        metrics.wait_reason.as_ref()
                    } else {
                        None
                    };

                    let rate_limit = if matches!(waiting_reason, Some(WaitReason::RateLimit)) {
                        1
                    } else {
                        0
                    };
                    let poll_interval = if matches!(waiting_reason, Some(WaitReason::PollInterval))
                    {
                        1
                    } else {
                        0
                    };
                    let backoff = if matches!(waiting_reason, Some(WaitReason::Backoff)) {
                        1
                    } else {
                        0
                    };

                    writeln!(
                        output,
                        "http_pull_waiting{{{stage_labels},reason=\"rate_limit\"}} {rate_limit}"
                    )?;
                    writeln!(
                        output,
                        "http_pull_waiting{{{stage_labels},reason=\"poll_interval\"}} {poll_interval}"
                    )?;
                    writeln!(
                        output,
                        "http_pull_waiting{{{stage_labels},reason=\"backoff\"}} {backoff}"
                    )?;
                }
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_pull_next_wake_unix_seconds Unix timestamp when the source will next attempt activity"
            )?;
            writeln!(output, "# TYPE http_pull_next_wake_unix_seconds gauge")?;
            for (stage_id, metrics) in &snapshot.http_pull_metrics {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    if let Some(next_wake) = metrics.next_wake_unix_secs {
                        writeln!(
                            output,
                            "http_pull_next_wake_unix_seconds{{{}}} {}",
                            format_stage_labels(stage_id, metadata),
                            next_wake
                        )?;
                    }
                }
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_pull_last_success_unix_seconds Unix timestamp of the last successful fetch"
            )?;
            writeln!(output, "# TYPE http_pull_last_success_unix_seconds gauge")?;
            for (stage_id, metrics) in &snapshot.http_pull_metrics {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    if let Some(last_success) = metrics.last_success_unix_secs {
                        writeln!(
                            output,
                            "http_pull_last_success_unix_seconds{{{}}} {}",
                            format_stage_labels(stage_id, metadata),
                            last_success
                        )?;
                    }
                }
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_pull_requests_total Total HTTP requests made by the pull source"
            )?;
            writeln!(output, "# TYPE http_pull_requests_total counter")?;
            for (stage_id, metrics) in &snapshot.http_pull_metrics {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "http_pull_requests_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        metrics.requests_total
                    )?;
                }
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_pull_responses_total Total HTTP responses by status class"
            )?;
            writeln!(output, "# TYPE http_pull_responses_total counter")?;
            for (stage_id, metrics) in &snapshot.http_pull_metrics {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    let stage_labels = format_stage_labels(stage_id, metadata);
                    writeln!(
                        output,
                        "http_pull_responses_total{{{},status_class=\"2xx\"}} {}",
                        stage_labels, metrics.responses_2xx
                    )?;
                    writeln!(
                        output,
                        "http_pull_responses_total{{{},status_class=\"4xx\"}} {}",
                        stage_labels, metrics.responses_4xx
                    )?;
                    writeln!(
                        output,
                        "http_pull_responses_total{{{},status_class=\"5xx\"}} {}",
                        stage_labels, metrics.responses_5xx
                    )?;
                }
            }
            writeln!(output)?;

            writeln!(
                output,
                "# HELP http_pull_events_decoded_total Total decoded events emitted by the pull source"
            )?;
            writeln!(output, "# TYPE http_pull_events_decoded_total counter")?;
            for (stage_id, metrics) in &snapshot.http_pull_metrics {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "http_pull_events_decoded_total{{{}}} {}",
                        format_stage_labels(stage_id, metadata),
                        metrics.events_decoded_total
                    )?;
                }
            }
            writeln!(output)?;
        }

        Ok(())
    }

    /// Render infrastructure metrics from snapshot
    fn render_infra_metrics(
        &self,
        output: &mut String,
        snapshot: &InfraMetricsSnapshot,
    ) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Journal metrics
        let journal = &snapshot.journal_metrics;

        writeln!(
            output,
            "# HELP obzenflow_journal_writes_total Total number of journal write operations"
        )?;
        writeln!(output, "# TYPE obzenflow_journal_writes_total counter")?;
        writeln!(
            output,
            "obzenflow_journal_writes_total {}",
            journal.writes_total
        )?;
        writeln!(output)?;

        writeln!(
            output,
            "# HELP obzenflow_journal_bytes_written_total Total bytes written to journal"
        )?;
        writeln!(
            output,
            "# TYPE obzenflow_journal_bytes_written_total counter"
        )?;
        writeln!(
            output,
            "obzenflow_journal_bytes_written_total {}",
            journal.bytes_written
        )?;
        writeln!(output)?;

        writeln!(
            output,
            "# HELP obzenflow_journal_throughput_events_per_second Current journal throughput"
        )?;
        writeln!(
            output,
            "# TYPE obzenflow_journal_throughput_events_per_second gauge"
        )?;
        writeln!(
            output,
            "obzenflow_journal_throughput_events_per_second {}",
            journal.throughput
        )?;
        writeln!(output)?;

        // Journal write latency histogram
        writeln!(
            output,
            "# HELP obzenflow_journal_write_duration_seconds Journal write latency in seconds"
        )?;
        writeln!(
            output,
            "# TYPE obzenflow_journal_write_duration_seconds histogram"
        )?;
        self.render_histogram(
            output,
            "obzenflow_journal_write_duration_seconds",
            &[],
            &journal.write_latency,
            0.000001, // Convert microseconds to seconds
        )?;
        writeln!(output)?;

        // Stage infrastructure metrics
        if !snapshot.stage_metrics.is_empty() {
            writeln!(output, "# HELP obzenflow_stage_in_flight Current number of events being processed per stage")?;
            writeln!(output, "# TYPE obzenflow_stage_in_flight gauge")?;

            for (stage_id, metrics) in &snapshot.stage_metrics {
                writeln!(
                    output,
                    "obzenflow_stage_in_flight{{stage=\"{}\"}} {}",
                    escape_label(&stage_id.to_string()),
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
        let buckets = [
            0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ];

        // For each bucket, count how many observations fall below it
        for &bucket in &buckets {
            let count = if histogram.count == 0 {
                0
            } else {
                // Estimate bucket count based on percentiles
                // This is approximate but good enough for monitoring
                let bucket_ns = bucket * scale_factor; // Convert bucket seconds to nanoseconds
                if bucket_ns <= histogram.min {
                    0
                } else if bucket_ns >= histogram.max {
                    histogram.count
                } else {
                    // Linear interpolation between known percentiles
                    estimate_bucket_count(histogram, bucket_ns)
                }
            };

            writeln!(
                output,
                "{metric_name}_bucket{{{label_str},le=\"{bucket}\"}} {count}"
            )?;
        }

        // +Inf bucket
        writeln!(
            output,
            "{}_bucket{{{},le=\"+Inf\"}} {}",
            metric_name, label_str, histogram.count
        )?;

        // Sum and count
        writeln!(
            output,
            "{}_sum{{{}}} {}",
            metric_name,
            label_str,
            histogram.sum / scale_factor
        )?;

        writeln!(
            output,
            "{}_count{{{}}} {}",
            metric_name, label_str, histogram.count
        )?;

        Ok(())
    }
}

/// Escape a label value for Prometheus
fn escape_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Centralized stage label formatting (FLOWIP-059a-3 Issue 2)
///
/// All stage-scoped metrics MUST use this function to ensure consistent labeling.
/// Returns the standard label set: flow, flow_id (when available), stage, stage_id
fn format_stage_labels(stage_id: &StageId, metadata: &StageMetadata) -> String {
    let stage_id_str = stage_id.to_string();
    let reference_mode = metadata
        .reference_mode
        .as_deref()
        .map(|mode| format!(",reference_mode=\"{}\"", escape_label(mode)))
        .unwrap_or_default();

    if let Some(flow_id) = &metadata.flow_id {
        format!(
            "flow=\"{}\",flow_id=\"{}\",stage=\"{}\",stage_id=\"{}\"{}",
            escape_label(&metadata.flow_name),
            escape_label(&flow_id.to_string()),
            escape_label(&metadata.name),
            escape_label(&stage_id_str),
            reference_mode,
        )
    } else {
        format!(
            "flow=\"{}\",stage=\"{}\",stage_id=\"{}\"{}",
            escape_label(&metadata.flow_name),
            escape_label(&metadata.name),
            escape_label(&stage_id_str),
            reference_mode,
        )
    }
}

/// Format label pairs for Prometheus
fn format_labels(labels: &[(&str, &str)]) -> String {
    labels
        .iter()
        .map(|(k, v)| format!("{}=\"{}\"", k, escape_label(v)))
        .collect::<Vec<_>>()
        .join(",")
}

/// Estimate bucket count based on percentiles
fn estimate_bucket_count(histogram: &HistogramSnapshot, bucket_value: f64) -> u64 {
    // Get percentiles - they're stored as Percentile enum and values are in nanoseconds
    let p50 = histogram
        .percentiles
        .get(&obzenflow_core::metrics::Percentile::P50)
        .copied();
    let p90 = histogram
        .percentiles
        .get(&obzenflow_core::metrics::Percentile::P90)
        .copied();
    let p95 = histogram
        .percentiles
        .get(&obzenflow_core::metrics::Percentile::P95)
        .copied();
    let p99 = histogram
        .percentiles
        .get(&obzenflow_core::metrics::Percentile::P99)
        .copied();
    let p999 = histogram
        .percentiles
        .get(&obzenflow_core::metrics::Percentile::P999)
        .copied();

    // If we have percentiles, use them for accurate bucket estimation
    if let (Some(p50_val), Some(p90_val), Some(p95_val), Some(p99_val)) = (p50, p90, p95, p99) {
        if bucket_value <= p50_val {
            (histogram.count as f64 * 0.5) as u64
        } else if bucket_value <= p90_val {
            (histogram.count as f64 * 0.9) as u64
        } else if bucket_value <= p95_val {
            (histogram.count as f64 * 0.95) as u64
        } else if bucket_value <= p99_val {
            (histogram.count as f64 * 0.99) as u64
        } else if let Some(p999_val) = p999 {
            if bucket_value <= p999_val {
                (histogram.count as f64 * 0.999) as u64
            } else {
                histogram.count
            }
        } else {
            histogram.count
        }
    } else {
        // Fallback to simple ratio if no percentiles available
        if histogram.max > histogram.min {
            let ratio = (bucket_value - histogram.min) / (histogram.max - histogram.min);
            (histogram.count as f64 * ratio.clamp(0.0, 1.0)) as u64
        } else {
            // All values are the same
            if bucket_value >= histogram.max {
                histogram.count
            } else {
                0
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::ingestion::IngestionTelemetrySnapshot;
    use obzenflow_core::event::observability::{HttpPullState, HttpPullTelemetry, WaitReason};
    use std::collections::HashMap;

    #[test]
    fn test_prometheus_format() {
        let exporter = PrometheusExporter::new();

        // Create a test app snapshot
        let mut event_counts = HashMap::new();
        let stage_id = StageId::new();
        event_counts.insert(stage_id, 100);

        // Create stage metadata
        let mut stage_metadata = HashMap::new();
        stage_metadata.insert(
            stage_id,
            StageMetadata {
                name: "validation".to_string(),
                flow_name: "order_flow".to_string(),
                stage_type: obzenflow_core::event::context::StageType::Transform,
                reference_mode: None,
                flow_id: None,
            },
        );

        let snapshot = AppMetricsSnapshot {
            event_counts,
            stage_metadata,
            pipeline_state: "Created".to_string(),
            ..Default::default()
        };

        // Update and render
        exporter.update_app_metrics(snapshot).unwrap();
        let output = exporter.render_metrics().unwrap();

        // Check output
        assert!(output.contains("# TYPE obzenflow_events_total counter"));
        let expected = format!(
            "obzenflow_events_total{{flow=\"order_flow\",stage=\"validation\",stage_id=\"{}\"}} 100",
            escape_label(&stage_id.to_string())
        );
        assert!(output.contains(&expected));
    }

    #[test]
    fn test_http_ingestion_metrics_rendered() {
        let exporter = PrometheusExporter::new();

        let mut snapshot = AppMetricsSnapshot::default();
        snapshot.ingestion_metrics.insert(
            "/api/ingest".to_string(),
            IngestionTelemetrySnapshot {
                base_path: "/api/ingest".to_string(),
                channel_depth: 12,
                channel_capacity: 10_000,
                requests_total: 5,
                events_accepted_total: 4,
                events_rejected_auth_total: 1,
                events_rejected_validation_total: 0,
                events_rejected_buffer_full_total: 0,
                events_rejected_not_ready_total: 0,
                events_rejected_payload_too_large_total: 0,
                events_rejected_invalid_json_total: 0,
                events_rejected_channel_closed_total: 7,
            },
        );

        exporter.update_app_metrics(snapshot).unwrap();
        let output = exporter.render_metrics().unwrap();

        let base_path = escape_label("/api/ingest");
        assert!(output.contains("# TYPE http_ingestion_requests_total counter"));
        assert!(output.contains(&format!(
            "http_ingestion_requests_total{{base_path=\"{base_path}\"}} 5"
        )));
        assert!(output.contains(&format!(
            "http_ingestion_events_rejected_total{{base_path=\"{base_path}\",reason=\"auth\"}} 1"
        )));
        assert!(output.contains(&format!(
            "http_ingestion_events_rejected_total{{base_path=\"{base_path}\",reason=\"channel_closed\"}} 7"
        )));
        assert!(output.contains(&format!(
            "http_ingestion_channel_depth{{base_path=\"{base_path}\"}} 12"
        )));
    }

    #[test]
    fn test_http_pull_metrics_rendered() {
        let exporter = PrometheusExporter::new();

        let stage_id = StageId::new();
        let mut stage_metadata = HashMap::new();
        stage_metadata.insert(
            stage_id,
            StageMetadata {
                name: "http_pull".to_string(),
                flow_name: "order_flow".to_string(),
                stage_type: obzenflow_core::event::context::StageType::FiniteSource,
                reference_mode: None,
                flow_id: None,
            },
        );

        let telemetry = HttpPullTelemetry {
            state: HttpPullState::Waiting,
            wait_reason: Some(WaitReason::RateLimit),
            next_wake_unix_secs: Some(1_700_000_123),
            last_success_unix_secs: Some(1_700_000_000),
            requests_total: 10,
            responses_2xx: 7,
            responses_4xx: 2,
            responses_5xx: 1,
            events_decoded_total: 42,
            ..Default::default()
        };

        let mut http_pull_metrics = HashMap::new();
        http_pull_metrics.insert(stage_id, telemetry);

        let snapshot = AppMetricsSnapshot {
            stage_metadata,
            http_pull_metrics,
            ..Default::default()
        };

        exporter.update_app_metrics(snapshot).unwrap();
        let output = exporter.render_metrics().unwrap();

        let stage_id = escape_label(&stage_id.to_string());
        assert!(output.contains("# TYPE http_pull_waiting gauge"));
        assert!(output.contains(&format!(
            "http_pull_waiting{{flow=\"order_flow\",stage=\"http_pull\",stage_id=\"{stage_id}\",reason=\"rate_limit\"}} 1"
        )));
        assert!(output.contains(&format!(
            "http_pull_next_wake_unix_seconds{{flow=\"order_flow\",stage=\"http_pull\",stage_id=\"{stage_id}\"}} 1700000123"
        )));
        assert!(output.contains(&format!(
            "http_pull_last_success_unix_seconds{{flow=\"order_flow\",stage=\"http_pull\",stage_id=\"{stage_id}\"}} 1700000000"
        )));
        assert!(output.contains(&format!(
            "http_pull_requests_total{{flow=\"order_flow\",stage=\"http_pull\",stage_id=\"{stage_id}\"}} 10"
        )));
        assert!(output.contains(&format!(
            "http_pull_responses_total{{flow=\"order_flow\",stage=\"http_pull\",stage_id=\"{stage_id}\",status_class=\"2xx\"}} 7"
        )));
        assert!(output.contains(&format!(
            "http_pull_events_decoded_total{{flow=\"order_flow\",stage=\"http_pull\",stage_id=\"{stage_id}\"}} 42"
        )));
    }
}
