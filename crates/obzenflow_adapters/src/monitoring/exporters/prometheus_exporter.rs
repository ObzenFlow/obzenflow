//! Clean Prometheus exporter implementation for FLOWIP-056-666
//!
//! This module implements the MetricsExporter trait for Prometheus exposition format.
//! It receives snapshots from collectors and renders them as Prometheus text.
//! No collection logic, no dependencies on aggregators - pure export functionality.

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
                        "obzenflow_events_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
                        count
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
                            "obzenflow_errors_total{{flow=\"{}\",stage=\"{}\",error_kind=\"{}\"}} {}",
                            escape_label(&metadata.flow_name),
                            escape_label(&metadata.name),
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
                        "obzenflow_errors_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                    self.render_histogram(
                        output,
                        "obzenflow_processing_time_seconds",
                        &[("flow", &metadata.flow_name), ("stage", &metadata.name)],
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
                        "obzenflow_in_flight_events{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_cpu_usage_ratio{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_memory_bytes{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_anomalies_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_amendments_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_saturation_ratio{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_failures_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_event_loops_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_event_loops_with_work_total{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                    self.render_histogram(
                        output,
                        "obzenflow_flow_latency_seconds",
                        &[("flow", &metadata.flow_name)],
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
                        "obzenflow_dropped_events{{flow=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
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
                        "obzenflow_circuit_breaker_state{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                        "obzenflow_circuit_breaker_rejection_rate{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
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
                    writeln!(output, "obzenflow_circuit_breaker_consecutive_failures{{flow=\"{}\",stage=\"{}\"}} {}", 
                        escape_label(&metadata.flow_name), escape_label(&metadata.name), value)?;
                }
            }
            writeln!(output)?;
        }

        // Rate limiter delay rate
        if !snapshot.rate_limiter_delay_rate.is_empty() {
            writeln!(
                output,
                "# HELP obzenflow_rate_limiter_delay_rate Rate limiter delay rate (0.0-1.0)"
            )?;
            writeln!(output, "# TYPE obzenflow_rate_limiter_delay_rate gauge")?;

            for (stage_id, value) in &snapshot.rate_limiter_delay_rate {
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    writeln!(
                        output,
                        "obzenflow_rate_limiter_delay_rate{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
                        value
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
                        "obzenflow_rate_limiter_utilization{{flow=\"{}\",stage=\"{}\"}} {}",
                        escape_label(&metadata.flow_name),
                        escape_label(&metadata.name),
                        value
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

            // Journey tracking counters
            writeln!(
                output,
                "# HELP obzenflow_flow_journeys_started_total Number of event journeys started"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_flow_journeys_started_total counter"
            )?;
            writeln!(
                output,
                "obzenflow_flow_journeys_started_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.journeys_opened
            )?;
            writeln!(output)?;

            writeln!(
                output,
                "# HELP obzenflow_flow_journeys_completed_total Number of event journeys completed"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_flow_journeys_completed_total counter"
            )?;
            writeln!(
                output,
                "obzenflow_flow_journeys_completed_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.journeys_sealed
            )?;
            writeln!(output)?;

            writeln!(output, "# HELP obzenflow_flow_journeys_errored_total Number of event journeys that encountered errors")?;
            writeln!(
                output,
                "# TYPE obzenflow_flow_journeys_errored_total counter"
            )?;
            writeln!(
                output,
                "obzenflow_flow_journeys_errored_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.journeys_errored
            )?;
            writeln!(output)?;

            writeln!(
                output,
                "# HELP obzenflow_flow_journeys_abandoned_total Number of event journeys abandoned"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_flow_journeys_abandoned_total counter"
            )?;
            writeln!(
                output,
                "obzenflow_flow_journeys_abandoned_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.journeys_abandoned
            )?;
            writeln!(output)?;

            // Event counts
            writeln!(
                output,
                "# HELP obzenflow_flow_events_in_total Events entering from sources"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_events_in_total counter")?;
            writeln!(
                output,
                "obzenflow_flow_events_in_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.events_in
            )?;
            writeln!(output)?;

            writeln!(
                output,
                "# HELP obzenflow_flow_events_out_total Events exiting through sinks"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_events_out_total counter")?;
            writeln!(
                output,
                "obzenflow_flow_events_out_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.events_out
            )?;
            writeln!(output)?;

            writeln!(
                output,
                "# HELP obzenflow_flow_errors_total Total errors across all stages"
            )?;
            writeln!(output, "# TYPE obzenflow_flow_errors_total counter")?;
            writeln!(
                output,
                "obzenflow_flow_errors_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.errors_total
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
                "obzenflow_flow_event_loops_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.event_loops_total
            )?;
            writeln!(output)?;

            writeln!(output, "# HELP obzenflow_flow_event_loops_with_work_total Event loops with work across all stages")?;
            writeln!(
                output,
                "# TYPE obzenflow_flow_event_loops_with_work_total counter"
            )?;
            writeln!(
                output,
                "obzenflow_flow_event_loops_with_work_total{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.event_loops_with_work_total
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
                "obzenflow_flow_utilization{{flow=\"{}\"}} {}",
                flow_name, utilization
            )?;
            writeln!(output)?;

            writeln!(output, "# HELP obzenflow_flow_saturation_journeys Active journeys (backpressure indicator)")?;
            writeln!(output, "# TYPE obzenflow_flow_saturation_journeys gauge")?;
            writeln!(
                output,
                "obzenflow_flow_saturation_journeys{{flow=\"{}\"}} {}",
                flow_name, flow_metrics.saturation_journeys
            )?;
            writeln!(output)?;

            // Journey duration histogram
            writeln!(
                output,
                "# HELP obzenflow_flow_journey_duration_seconds End-to-end journey duration"
            )?;
            writeln!(
                output,
                "# TYPE obzenflow_flow_journey_duration_seconds histogram"
            )?;
            self.render_histogram(
                output,
                "obzenflow_flow_journey_duration_seconds",
                &[("flow", flow_name)],
                &flow_metrics.e2e_latency,
                1_000_000_000.0, // Convert nanoseconds to seconds
            )?;
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
                            "obzenflow_stage_lifecycle_state{{flow=\"{}\",stage=\"{}\",state=\"{}\"}} 1",
                            escape_label(&metadata.flow_name),
                            escape_label(&metadata.name),
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
                "{}_bucket{{{},le=\"{}\"}} {}",
                metric_name, label_str, bucket, count
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
            (histogram.count as f64 * ratio.min(1.0).max(0.0)) as u64
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
            },
        );

        let snapshot = AppMetricsSnapshot {
            timestamp: chrono::Utc::now(),
            event_counts,
            error_counts: HashMap::new(),
            error_counts_by_kind: HashMap::new(),
            processing_times: HashMap::new(),
            in_flight: HashMap::new(),
            cpu_usage_ratio: HashMap::new(),
            memory_bytes: HashMap::new(),
            anomalies_total: HashMap::new(),
            amendments_total: HashMap::new(),
            saturation_ratio: HashMap::new(),
            failures_total: HashMap::new(),
            event_loops_total: HashMap::new(),
            event_loops_with_work_total: HashMap::new(),
            flow_latency_seconds: HashMap::new(),
            dropped_events: HashMap::new(),
            circuit_breaker_state: HashMap::new(),
            circuit_breaker_rejection_rate: HashMap::new(),
            circuit_breaker_consecutive_failures: HashMap::new(),
            rate_limiter_delay_rate: HashMap::new(),
            rate_limiter_utilization: HashMap::new(),
            flow_metrics: None,
            stage_metadata,
            stage_first_event_time: HashMap::new(),
            stage_last_event_time: HashMap::new(),
            stage_lifecycle_states: HashMap::new(),
            pipeline_state: "Created".to_string(),
        };

        // Update and render
        exporter.update_app_metrics(snapshot).unwrap();
        let output = exporter.render_metrics().unwrap();

        // Check output
        assert!(output.contains("# TYPE obzenflow_events_total counter"));
        assert!(
            output.contains("obzenflow_events_total{flow=\"order_flow\",stage=\"validation\"} 100")
        );
    }
}
