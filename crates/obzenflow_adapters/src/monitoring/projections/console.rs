// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Console text projection over an immutable, independently owned read view.

use crate::monitoring::read_model::MetricsReadView;
use obzenflow_core::event::context::StageType;
use obzenflow_core::id::StageId;
use obzenflow_core::metrics::{Percentile, PercentileExt, StageMetadata};
use std::error::Error;
use std::time::Instant;

/// Deterministic console summary of the latest available observations.
#[derive(Default)]
pub struct ConsoleProjection;

impl ConsoleProjection {
    pub fn new() -> Self {
        Self
    }

    pub fn render(&self, view: &MetricsReadView) -> Result<String, Box<dyn Error + Send + Sync>> {
        self.render_until(view, None)
    }

    /// Render with an optional caller-owned deadline. Oversized reports are
    /// declined before sorting or allocating their text, keeping output work
    /// bounded even when application metadata is unexpectedly large.
    pub fn render_until(
        &self,
        view: &MetricsReadView,
        deadline: Option<Instant>,
    ) -> Result<String, Box<dyn Error + Send + Sync>> {
        let check_budget = || -> Result<(), Box<dyn Error + Send + Sync>> {
            if deadline.is_some_and(|end| Instant::now() >= end) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "console projection deadline expired",
                )
                .into());
            }
            Ok(())
        };
        check_budget()?;
        if let Some(snapshot) = view.app.as_ref() {
            if snapshot.stage_metadata.len() > 4096 {
                return Err(std::io::Error::other("console report exceeds 4096 stages").into());
            }
            for metadata in snapshot.stage_metadata.values() {
                check_budget()?;
                if metadata.name.len() > 4096 || metadata.flow_name.len() > 4096 {
                    return Err(
                        std::io::Error::other("console stage label exceeds 4096 bytes").into(),
                    );
                }
            }
            let mut summary = String::new();

            // Count events by stage type
            let mut source_events = 0u64;
            let mut sink_events = 0u64;
            let mut transform_events = 0u64;
            let mut total_errors = 0u64;
            let mut total_time_ms = 0f64;
            let mut total_event_count = 0u64;

            // Process event counts using stage metadata
            for (stage_id, count) in &snapshot.event_counts {
                check_budget()?;
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    match metadata.stage_type {
                        StageType::FiniteSource | StageType::InfiniteSource => {
                            source_events += count;
                        }
                        StageType::Sink => {
                            sink_events += count;
                        }
                        StageType::Transform | StageType::Stateful | StageType::Join => {
                            transform_events += count;
                        }
                    }
                }
            }

            // Count total errors
            for errors in snapshot.error_counts.values() {
                check_budget()?;
                total_errors += errors;
            }

            // Calculate processing times
            for hist in snapshot.processing_times.values() {
                check_budget()?;
                total_time_ms += hist.sum / 1_000_000.0; // Convert nanoseconds to ms
                total_event_count += hist.count;
            }

            // Calculate total events (should be sum of all stages but avoiding double counting)
            let total_events = source_events.max(sink_events).max(transform_events);

            // Format duration helper
            let format_duration = |ms: f64| -> String {
                if ms < 0.01 {
                    format!("{ms:.3}ms") // Show 3 decimal places for < 0.01ms
                } else if ms < 1.0 {
                    format!("{ms:.2}ms") // Show 2 decimal places for < 1ms
                } else if ms < 10.0 {
                    format!("{ms:.1}ms") // Show 1 decimal place for < 10ms
                } else {
                    format!("{ms:.0}ms") // No decimal places for >= 10ms
                }
            };

            // Flow-level metrics
            summary.push_str("Flow Summary:\n");

            if let Some(flow_metrics) = &snapshot.flow_metrics {
                // Calculate rate from flow metrics
                let duration_secs = flow_metrics.flow_duration.as_secs_f64();
                let rate = if duration_secs > 0.0 && flow_metrics.events_in > 0 {
                    flow_metrics.events_in as f64 / duration_secs
                } else {
                    0.0
                };

                summary.push_str(&format!("  Rate:              {rate:.1} events/sec\n"));
                summary.push_str(&format!(
                    "  Events In:         {} (from sources)\n",
                    flow_metrics.events_in
                ));
                summary.push_str(&format!(
                    "  Events Out:        {} (to sinks)\n",
                    flow_metrics.events_out
                ));
                summary.push_str(&format!(
                    "  Errors:            {} ({:.1}%)\n",
                    flow_metrics.errors_total,
                    if flow_metrics.events_in > 0 {
                        (flow_metrics.errors_total as f64 / flow_metrics.events_in as f64) * 100.0
                    } else {
                        0.0
                    }
                ));

                if total_event_count > 0 {
                    let avg_ms = total_time_ms / total_event_count as f64;
                    summary.push_str(&format!(
                        "  Avg Stage Duration: {}\n",
                        format_duration(avg_ms)
                    ));
                }

                // Utilization
                let utilization = if flow_metrics.event_loops_total > 0 {
                    flow_metrics.event_loops_with_work_total as f64
                        / flow_metrics.event_loops_total as f64
                        * 100.0
                } else {
                    0.0
                };
                summary.push_str(&format!("\nUtilization:     {utilization:.1}%"));
                if utilization > 90.0 {
                    summary.push_str(" (WARNING)");
                }
                summary.push_str("\n\n");
            } else {
                // Fallback to stage-based calculations
                let rate_str = "N/A";
                summary.push_str(&format!("  Rate:        {rate_str} events/sec"));
                summary.push_str(&format!("  Total Events: {total_events}"));
                summary.push_str(&format!(
                    "  Total Errors: {} ({:.1}%)\n\n",
                    total_errors,
                    if total_events > 0 {
                        (total_errors as f64 / total_events as f64) * 100.0
                    } else {
                        0.0
                    }
                ));
            }

            // Per-stage processing summary
            summary.push_str("Metrics by Stage:\n");

            // Collect and sort stages by type and name
            let mut stages: Vec<(&StageId, &StageMetadata)> =
                snapshot.stage_metadata.iter().collect();
            stages.sort_by(|a, b| {
                // Sort by stage type (sources first, then transforms, then sinks)
                let type_order = |t: &StageType| match t {
                    StageType::FiniteSource | StageType::InfiniteSource => 0,
                    StageType::Transform | StageType::Stateful | StageType::Join => 1,
                    StageType::Sink => 2,
                };
                type_order(&a.1.stage_type)
                    .cmp(&type_order(&b.1.stage_type))
                    .then_with(|| a.1.name.cmp(&b.1.name))
            });

            for (stage_id, metadata) in stages {
                check_budget()?;
                let events = snapshot.event_counts.get(stage_id).unwrap_or(&0);
                let errors = snapshot.error_counts.get(stage_id).unwrap_or(&0);

                // Format stage display name
                let stage_display = format!(
                    "{}: {} ({})",
                    metadata.flow_name,
                    metadata.name,
                    metadata.stage_type.as_str()
                );
                summary.push_str(&format!("\n  {stage_display}\n"));

                // Rate (calculate from stage-specific timestamps)
                if let (Some(first_time), Some(last_time)) = (
                    snapshot.stage_first_event_time.get(stage_id),
                    snapshot.stage_last_event_time.get(stage_id),
                ) {
                    let duration = *last_time - *first_time;
                    let duration_secs = duration.num_milliseconds() as f64 / 1000.0;
                    if duration_secs > 0.0 && *events > 0 {
                        let stage_rate = *events as f64 / duration_secs;
                        summary.push_str(&format!("    Rate:     {stage_rate:.1} events/sec\n"));
                    }
                } else {
                    summary.push_str("    Rate:     N/A\n");
                }

                // Errors
                let error_pct = if *events > 0 {
                    (*errors as f64 / *events as f64) * 100.0
                } else {
                    0.0
                };
                summary.push_str(&format!("    Errors:   {errors} ({error_pct:.1}%)\n"));

                // Duration percentiles
                if let Some(hist) = snapshot.processing_times.get(stage_id) {
                    if hist.count > 0 {
                        let p50 = hist.percentiles.get_as_millis(&Percentile::P50);
                        let p90 = hist.percentiles.get_as_millis(&Percentile::P90);
                        let p99 = hist.percentiles.get_as_millis(&Percentile::P99);
                        let p999 = hist.percentiles.get_as_millis(&Percentile::P999);

                        summary.push_str(&format!(
                            "    Duration: p50={}, p90={}, p99={}, p99.9={}",
                            format_duration(p50),
                            format_duration(p90),
                            format_duration(p99),
                            format_duration(p999)
                        ));

                        // Warning for slow stages
                        if p99 > 100.0 {
                            summary.push_str(" (WARNING: slow)");
                        }
                        summary.push('\n');
                    }
                }
            }
            summary.push('\n');

            // Runtime State (from FSM instrumentation)
            let total_in_flight: f64 =
                snapshot.in_flight.values().try_fold(0.0, |sum, value| {
                    check_budget()?;
                    Ok::<_, Box<dyn Error + Send + Sync>>(sum + value)
                })?;
            // events_behind removed - calculate in PromQL instead
            let total_failures: u64 =
                snapshot
                    .failures_total
                    .values()
                    .try_fold(0u64, |sum, value| {
                        check_budget()?;
                        Ok::<_, Box<dyn Error + Send + Sync>>(sum + value)
                    })?;

            summary.push_str("Runtime State:\n");
            summary.push_str(&format!(
                "\n  In Flight:   {} events",
                total_in_flight as u64
            ));
            if total_in_flight > 50.0 {
                summary.push_str(" (WARNING: high)");
            }

            // events_behind removed - calculate in PromQL instead
            // e.g., events_processed_total{stage="transform"} - events_processed_total{stage="sink"}

            // Calculate utilization if event loop data available
            let total_loops: u64 =
                snapshot
                    .event_loops_total
                    .values()
                    .try_fold(0u64, |sum, value| {
                        check_budget()?;
                        Ok::<_, Box<dyn Error + Send + Sync>>(sum + value)
                    })?;
            let loops_with_work: u64 =
                snapshot
                    .event_loops_with_work_total
                    .values()
                    .try_fold(0u64, |sum, value| {
                        check_budget()?;
                        Ok::<_, Box<dyn Error + Send + Sync>>(sum + value)
                    })?;
            if total_loops > 0 {
                let utilization = (loops_with_work as f64 / total_loops as f64) * 100.0;
                summary.push_str(&format!("\n  Utilization: {utilization:.1}%"));
                if utilization > 90.0 {
                    summary.push_str(" (WARNING)");
                }
            }
            summary.push_str("\n\n");

            // Event flow
            summary.push_str("Event Flow:\n");
            summary.push_str(&format!("  Source → {source_events} events\n"));
            if transform_events > 0 {
                summary.push_str(&format!("  Transform → {transform_events} events\n"));
            }
            summary.push_str(&format!("  Sink → {sink_events} events"));

            if sink_events < source_events {
                summary.push_str(&format!(
                    " (WARNING: {} missing)",
                    source_events - sink_events
                ));
            }
            summary.push('\n');

            let has_dropped =
                snapshot
                    .dropped_events
                    .values()
                    .try_fold(false, |found, value| {
                        check_budget()?;
                        Ok::<_, Box<dyn Error + Send + Sync>>(found || *value > 0.0)
                    })?;
            // Errors, failures and drops
            if total_errors > 0 || total_failures > 0 || has_dropped {
                summary.push_str("\nIssues:\n");
                if total_errors > 0 {
                    summary.push_str(&format!("  Errors: {total_errors}\n"));
                }
                if total_failures > 0 {
                    summary.push_str(&format!("  Failures: {total_failures} (critical)\n"));
                }
                let total_dropped: f64 =
                    snapshot
                        .dropped_events
                        .values()
                        .try_fold(0.0, |sum, value| {
                            check_budget()?;
                            Ok::<_, Box<dyn Error + Send + Sync>>(sum + value)
                        })?;
                if total_dropped > 0.0 {
                    summary.push_str(&format!("  Dropped: {}\n", total_dropped as u64));
                }
            }

            summary.push_str(&format!("\n{}\n", "=".repeat(50)));

            check_budget()?;
            Ok(summary)
        } else {
            Ok("No metrics available yet\n".to_string())
        }
    }
}
