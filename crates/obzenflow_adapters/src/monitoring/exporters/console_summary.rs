//! Console summary exporter that provides user-friendly metrics output
//!
//! This exporter stores metrics and provides a formatted summary when requested,
//! making it easy to get flow statistics without parsing Prometheus text.

use obzenflow_core::metrics::{MetricsExporter, AppMetricsSnapshot, InfraMetricsSnapshot, Percentile, PercentileExt, StageMetadata};
use obzenflow_core::id::StageId;
use obzenflow_core::event::context::StageType;
use std::sync::RwLock;
use std::error::Error;
use std::collections::HashMap;

/// Console exporter that stores metrics and can render a summary
pub struct ConsoleSummaryExporter {
    /// Latest application metrics snapshot
    app_snapshot: RwLock<Option<AppMetricsSnapshot>>,
    
    /// Latest infrastructure metrics snapshot  
    infra_snapshot: RwLock<Option<InfraMetricsSnapshot>>,
}

impl ConsoleSummaryExporter {
    pub fn new() -> Self {
        Self {
            app_snapshot: RwLock::new(None),
            infra_snapshot: RwLock::new(None),
        }
    }
    
    /// Get a formatted summary of the metrics
    pub fn summary(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        let app_snapshot = self.app_snapshot.read()
            .map_err(|_| "Failed to acquire read lock")?;
            
        if let Some(snapshot) = app_snapshot.as_ref() {
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
                if let Some(metadata) = snapshot.stage_metadata.get(stage_id) {
                    match metadata.stage_type {
                        StageType::FiniteSource | StageType::InfiniteSource => {
                            source_events += count;
                        }
                        StageType::Sink => {
                            sink_events += count;
                        }
                        StageType::Transform | StageType::Stateful => {
                            transform_events += count;
                        }
                    }
                }
            }
            
            // Count total errors
            for (_stage_id, errors) in &snapshot.error_counts {
                total_errors += errors;
            }
            
            // Calculate processing times
            for (_stage_id, hist) in &snapshot.processing_times {
                total_time_ms += hist.sum / 1_000_000.0; // Convert nanoseconds to ms
                total_event_count += hist.count;
            }
            
            // Calculate total events (should be sum of all stages but avoiding double counting)
            let total_events = source_events.max(sink_events).max(transform_events);
            
            // Format duration helper
            let format_duration = |ms: f64| -> String {
                if ms < 0.01 {
                    format!("{:.3}ms", ms)  // Show 3 decimal places for < 0.01ms
                } else if ms < 1.0 {
                    format!("{:.2}ms", ms)  // Show 2 decimal places for < 1ms
                } else if ms < 10.0 {
                    format!("{:.1}ms", ms)  // Show 1 decimal place for < 10ms
                } else {
                    format!("{:.0}ms", ms)  // No decimal places for >= 10ms
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
                
                summary.push_str(&format!("  Rate:              {:.1} events/sec\n", rate));
                summary.push_str(&format!("  Events In:         {} (from sources)\n", flow_metrics.events_in));
                summary.push_str(&format!("  Events Out:        {} (to sinks)\n", flow_metrics.events_out));
                summary.push_str(&format!("  Errors:            {} ({:.1}%)\n", 
                    flow_metrics.errors_total,
                    if flow_metrics.events_in > 0 {
                        (flow_metrics.errors_total as f64 / flow_metrics.events_in as f64) * 100.0
                    } else {
                        0.0
                    }
                ));
                
                // Journey metrics
                summary.push_str("\nJourney Tracking:\n");
                summary.push_str(&format!("  Started:           {}\n", flow_metrics.journeys_opened));
                summary.push_str(&format!("  Completed:         {}\n", flow_metrics.journeys_sealed));
                summary.push_str(&format!("  Errored:           {}\n", flow_metrics.journeys_errored));
                summary.push_str(&format!("  Abandoned:         {} ({:.1}%)\n", 
                    flow_metrics.journeys_abandoned,
                    if flow_metrics.journeys_opened > 0 {
                        (flow_metrics.journeys_abandoned as f64 / flow_metrics.journeys_opened as f64) * 100.0
                    } else {
                        0.0
                    }
                ));
                summary.push_str(&format!("  Active:            {} (saturation)\n", flow_metrics.saturation_journeys));
                
                // E2E latency percentiles
                if flow_metrics.e2e_latency.count > 0 {
                    summary.push_str("\nE2E Latency:\n");
                    let p50 = flow_metrics.e2e_latency.percentiles.get(&Percentile::P50)
                        .map(|&v| format_duration(v / 1_000_000.0))
                        .unwrap_or_else(|| "N/A".to_string());
                    let p90 = flow_metrics.e2e_latency.percentiles.get(&Percentile::P90)
                        .map(|&v| format_duration(v / 1_000_000.0))
                        .unwrap_or_else(|| "N/A".to_string());
                    let p99 = flow_metrics.e2e_latency.percentiles.get(&Percentile::P99)
                        .map(|&v| format_duration(v / 1_000_000.0))
                        .unwrap_or_else(|| "N/A".to_string());
                    summary.push_str(&format!("  p50: {}, p90: {}, p99: {}\n", p50, p90, p99));
                }
                
                // Utilization
                let utilization = if flow_metrics.event_loops_total > 0 {
                    flow_metrics.event_loops_with_work_total as f64 / flow_metrics.event_loops_total as f64 * 100.0
                } else {
                    0.0
                };
                summary.push_str(&format!("\nUtilization:     {:.1}%", utilization));
                if utilization > 90.0 {
                    summary.push_str(" (WARNING)");
                }
                summary.push_str("\n\n");
            } else {
                // Fallback to stage-based calculations
                let rate_str = "N/A";
                summary.push_str(&format!("  Rate:        {} events/sec", rate_str));
                summary.push_str(&format!("  Total Events: {}", total_events));
                summary.push_str(&format!("  Total Errors: {} ({:.1}%)\n\n", 
                    total_errors,
                    if total_events > 0 {
                        (total_errors as f64 / total_events as f64) * 100.0
                    } else {
                        0.0
                    }
                ));
            }
            
            // Per-stage RED metrics
            summary.push_str("RED Metrics by Stage:\n");
            
            // Collect and sort stages by type and name
            let mut stages: Vec<(&StageId, &StageMetadata)> = snapshot.stage_metadata.iter().collect();
            stages.sort_by(|a, b| {
                // Sort by stage type (sources first, then transforms, then sinks)
                let type_order = |t: &StageType| match t {
                    StageType::FiniteSource | StageType::InfiniteSource => 0,
                    StageType::Transform | StageType::Stateful => 1,
                    StageType::Sink => 2,
                };
                type_order(&a.1.stage_type).cmp(&type_order(&b.1.stage_type))
                    .then_with(|| a.1.name.cmp(&b.1.name))
            });
            
            for (stage_id, metadata) in stages {
                let events = snapshot.event_counts.get(stage_id).unwrap_or(&0);
                let errors = snapshot.error_counts.get(stage_id).unwrap_or(&0);
                
                // Format stage display name
                let stage_display = format!("{}: {} ({})", 
                    metadata.flow_name, 
                    metadata.name, 
                    metadata.stage_type.as_str()
                );
                summary.push_str(&format!("\n  {}\n", stage_display));
                
                // Rate (calculate from stage-specific timestamps)
                if let (Some(first_time), Some(last_time)) = (
                    snapshot.stage_first_event_time.get(stage_id),
                    snapshot.stage_last_event_time.get(stage_id)
                ) {
                    let duration = *last_time - *first_time;
                    let duration_secs = duration.num_milliseconds() as f64 / 1000.0;
                    if duration_secs > 0.0 && *events > 0 {
                        let stage_rate = *events as f64 / duration_secs;
                        summary.push_str(&format!("    Rate:     {:.1} events/sec\n", stage_rate));
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
                summary.push_str(&format!("    Errors:   {} ({:.1}%)\n", errors, error_pct));
                
                // Duration percentiles
                if let Some(hist) = snapshot.processing_times.get(stage_id) {
                    if hist.count > 0 {
                        let p50 = hist.percentiles.get_as_millis(&Percentile::P50);
                        let p90 = hist.percentiles.get_as_millis(&Percentile::P90);
                        let p99 = hist.percentiles.get_as_millis(&Percentile::P99);
                        let p999 = hist.percentiles.get_as_millis(&Percentile::P999);
                        
                        summary.push_str(&format!("    Duration: p50={}, p90={}, p99={}, p99.9={}", 
                            format_duration(p50), 
                            format_duration(p90), 
                            format_duration(p99), 
                            format_duration(p999)
                        ));
                        
                        // Warning for slow stages
                        if p99 > 100.0 {
                            summary.push_str(" (WARNING: slow)");
                        }
                        summary.push_str("\n");
                    }
                }
            }
            summary.push_str("\n");
            
            // Runtime State (from FSM instrumentation)
            let total_in_flight: f64 = snapshot.in_flight.values().sum();
            // events_behind removed - calculate in PromQL instead
            let total_failures: u64 = snapshot.failures_total.values().sum();
            
            summary.push_str("Runtime State:\n");
            summary.push_str(&format!("\n  In Flight:   {} events", total_in_flight as u64));
            if total_in_flight > 50.0 {
                summary.push_str(" (WARNING: high)");
            }
            
            // events_behind removed - calculate in PromQL instead
            // e.g., events_processed_total{stage="transform"} - events_processed_total{stage="sink"}
            
            // Calculate utilization if event loop data available
            let total_loops: u64 = snapshot.event_loops_total.values().sum();
            let loops_with_work: u64 = snapshot.event_loops_with_work_total.values().sum();
            if total_loops > 0 {
                let utilization = (loops_with_work as f64 / total_loops as f64) * 100.0;
                summary.push_str(&format!("\n  Utilization: {:.1}%", utilization));
                if utilization > 90.0 {
                    summary.push_str(" (WARNING)");
                }
            }
            summary.push_str("\n\n");
            
            // Event flow
            summary.push_str("Event Flow:\n");
            summary.push_str(&format!("  Source → {} events\n", source_events));
            if transform_events > 0 {
                summary.push_str(&format!("  Transform → {} events\n", transform_events));
            }
            summary.push_str(&format!("  Sink → {} events", sink_events));

            if sink_events < source_events {
                summary.push_str(&format!(" (WARNING: {} missing)", source_events - sink_events));
            }
            summary.push_str("\n");
            
            // Errors, failures and drops
            if total_errors > 0 || total_failures > 0 || snapshot.dropped_events.values().sum::<f64>() > 0.0 {
                summary.push_str("\nIssues:\n");
                if total_errors > 0 {
                    summary.push_str(&format!("  Errors: {}\n", total_errors));
                }
                if total_failures > 0 {
                    summary.push_str(&format!("  Failures: {} (critical)\n", total_failures));
                }
                let total_dropped: f64 = snapshot.dropped_events.values().sum();
                if total_dropped > 0.0 {
                    summary.push_str(&format!("  Dropped: {}\n", total_dropped as u64));
                }
            }
            
            
            summary.push_str(&format!("\n{}\n", "=".repeat(50)));
            
            Ok(summary)
        } else {
            Ok("No metrics available yet\n".to_string())
        }
    }
}

impl Default for ConsoleSummaryExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsExporter for ConsoleSummaryExporter {
    fn update_app_metrics(&self, snapshot: AppMetricsSnapshot) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut app_snapshot = self.app_snapshot.write()
            .map_err(|_| "Failed to acquire write lock")?;
        *app_snapshot = Some(snapshot);
        Ok(())
    }
    
    fn update_infra_metrics(&self, snapshot: InfraMetricsSnapshot) -> Result<(), Box<dyn Error + Send + Sync>> {
        let mut infra_snapshot = self.infra_snapshot.write()
            .map_err(|_| "Failed to acquire write lock")?;
        *infra_snapshot = Some(snapshot);
        Ok(())
    }
    
    fn render_metrics(&self) -> Result<String, Box<dyn Error + Send + Sync>> {
        // For render_metrics, return the summary
        self.summary()
    }
}