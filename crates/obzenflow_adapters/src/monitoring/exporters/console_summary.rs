//! Console summary exporter that provides user-friendly metrics output
//!
//! This exporter stores metrics and provides a formatted summary when requested,
//! making it easy to get flow statistics without parsing Prometheus text.

use obzenflow_core::metrics::{MetricsExporter, AppMetricsSnapshot, InfraMetricsSnapshot};
use std::sync::RwLock;
use std::error::Error;

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
            
            summary.push_str("\n📊 Flow Summary:\n");
            summary.push_str(&"=".repeat(50));
            summary.push_str("\n\n");
            
            // Count events by stage type
            let mut source_events = 0u64;
            let mut sink_events = 0u64;
            let mut transform_events = 0u64;
            let mut total_errors = 0u64;
            let mut total_time_ms = 0f64;
            let mut total_event_count = 0u64;
            
            // Process event counts
            for (stage_key, count) in &snapshot.event_counts {
                // Parse "flow:stage" format
                if let Some((_flow, stage)) = stage_key.split_once(':') {
                    if stage.contains("source") {
                        source_events += count;
                    } else if stage.contains("sink") {
                        sink_events += count;
                    } else if stage.contains("transform") || stage.contains("processor") {
                        transform_events += count;
                    }
                }
            }
            
            // Count total errors
            for (_stage, errors) in &snapshot.error_counts {
                total_errors += errors;
            }
            
            // Calculate processing times
            for (_stage, hist) in &snapshot.processing_times {
                total_time_ms += hist.sum * 1000.0; // Convert seconds to ms
                total_event_count += hist.count;
            }
            
            // Calculate total events (should be sum of all stages but avoiding double counting)
            let total_events = source_events.max(sink_events).max(transform_events);
            
            // RED Metrics Summary
            summary.push_str("📈 RED Metrics:\n");
            summary.push_str(&format!("  Rate:     {} events/sec", 
                if snapshot.processing_times.values().any(|h| h.sum > 0.0) {
                    let total_duration = snapshot.processing_times.values()
                        .map(|h| h.sum)
                        .max_by(|a, b| a.partial_cmp(b).unwrap())
                        .unwrap_or(1.0);
                    format!("{:.1}", total_events as f64 / total_duration)
                } else {
                    "N/A".to_string()
                }
            ));
            
            summary.push_str(&format!("  Errors:   {} ({:.1}%)\n", 
                total_errors,
                if total_events > 0 {
                    (total_errors as f64 / total_events as f64) * 100.0
                } else {
                    0.0
                }
            ));
            
            // Calculate duration percentiles across all stages
            let mut all_percentiles: Vec<(String, f64)> = Vec::new();
            for (_stage, hist) in &snapshot.processing_times {
                for (percentile, value) in &hist.percentiles {
                    all_percentiles.push((percentile.clone(), *value * 1000.0)); // Convert seconds to ms
                }
            }
            
            // Get p50, p90, p99, p99.9 if available
            let p50 = all_percentiles.iter()
                .find(|(p, _)| p == "p50")
                .map(|(_, v)| *v)
                .unwrap_or(0.0);
            let p90 = all_percentiles.iter()
                .find(|(p, _)| p == "p90")
                .map(|(_, v)| *v)
                .unwrap_or(0.0);
            let p99 = all_percentiles.iter()
                .find(|(p, _)| p == "p99")
                .map(|(_, v)| *v)
                .unwrap_or(0.0);
            let p999 = all_percentiles.iter()
                .find(|(p, _)| p == "p999")
                .map(|(_, v)| *v)
                .unwrap_or(0.0);
            
            summary.push_str(&format!("  Duration: p50={:.0}ms, p90={:.0}ms, p99={:.0}ms, p99.9={:.0}ms", p50, p90, p99, p999));
            
            // Check for slow processing at p99
            if p99 > 100.0 {
                summary.push_str(" ⚠️");
            }
            summary.push_str("\n\n");
            
            // Event flow
            summary.push_str("📊 Event Flow:\n");
            summary.push_str(&format!("  Source → {} events\n", source_events));
            if transform_events > 0 {
                summary.push_str(&format!("  Transform → {} events\n", transform_events));
            }
            summary.push_str(&format!("  Sink → {} events", sink_events));
            
            if sink_events < source_events {
                summary.push_str(&format!(" ⚠️  ({} missing)", source_events - sink_events));
            }
            summary.push_str("\n");
            
            // Errors and drops
            if total_errors > 0 || snapshot.dropped_events.values().sum::<f64>() > 0.0 {
                summary.push_str("\n⚠️  Issues:\n");
                if total_errors > 0 {
                    summary.push_str(&format!("  Errors: {}\n", total_errors));
                }
                let total_dropped: f64 = snapshot.dropped_events.values().sum();
                if total_dropped > 0.0 {
                    summary.push_str(&format!("  Dropped: {}\n", total_dropped as u64));
                }
            }
            
            // Show per-stage breakdown if verbose
            if !snapshot.event_counts.is_empty() {
                summary.push_str("\n📈 Per-stage breakdown:\n");
                let mut stages: Vec<_> = snapshot.event_counts.iter().collect();
                stages.sort_by_key(|&(k, _)| k);
                
                for (stage_key, count) in stages {
                    let errors = snapshot.error_counts.get(stage_key).unwrap_or(&0);
                    summary.push_str(&format!("  {:<30} {} events", stage_key, count));
                    if *errors > 0 {
                        summary.push_str(&format!(" ({} errors)", errors));
                    }
                    summary.push_str("\n");
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