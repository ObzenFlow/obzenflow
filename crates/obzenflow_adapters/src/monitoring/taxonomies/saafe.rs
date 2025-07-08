//! SAAFE (Saturation, Amendments, Anomalies, Failures, Errors) Taxonomy
//!
//! SAAFE is a comprehensive monitoring approach that extends beyond traditional methods:
//! - **Saturation**: How full is the service (queue depth, capacity)
//! - **Amendments**: Data corrections and updates
//! - **Anomalies**: Unusual patterns or outliers
//! - **Failures**: Complete failures to process
//! - **Errors**: Recoverable errors
//!
//! This taxonomy is ideal for data pipelines where data quality and anomaly detection are critical.
//!
//! ## Metrics Available in ObzenFlow
//!
//! With FLOWIP-056-666, SAAFE metrics require a mix of automatic and custom metrics:
//!
//! | Metric | Prometheus Name | Description |
//! |--------|----------------|-------------|
//! | Saturation | `obzenflow_queue_depth` | Queue depth / backlog |
//! | Amendments | *custom metric* | Requires application-specific tracking |
//! | Anomalies | *custom metric* | Requires anomaly detection logic |
//! | Failures | `obzenflow_errors_total` | Total processing failures |
//! | Errors | `obzenflow_errors_total` | Total errors (same as failures) |
//!
//! ## Example Implementation
//!
//! For amendments and anomalies, emit custom control events:
//!
//! ```rust,no_run
//! # use obzenflow_core::event::chain_event::ChainEvent;
//! # use obzenflow_adapters::middleware::MiddlewareContext;
//! # use serde_json::json;
//! # let mut ctx = MiddlewareContext::new();
//! // Emit amendment event
//! ctx.write_control_event(ChainEvent::control(
//!     ChainEvent::CONTROL_METRICS_CUSTOM,
//!     json!({
//!         "metric_type": "amendment",
//!         "reason": "duplicate_removed",
//!         "count": 1
//!     })
//! ));
//!
//! // Emit anomaly event
//! ctx.write_control_event(ChainEvent::control(
//!     ChainEvent::CONTROL_METRICS_CUSTOM,
//!     json!({
//!         "metric_type": "anomaly",
//!         "severity": "high",
//!         "description": "Unexpected spike in values"
//!     })
//! ));
//! ```

/// SAAFE taxonomy definition
///
/// SAAFE provides comprehensive monitoring for data quality and anomalies.
pub struct SAAFE;

impl SAAFE {
    /// Taxonomy name
    pub const NAME: &'static str = "SAAFE";
    
    /// Human-readable description  
    pub const DESCRIPTION: &'static str = "Saturation, Amendments, Anomalies, Failures, Errors - comprehensive data pipeline monitoring";
    
    /// Get Prometheus queries for SAAFE metrics
    pub fn prometheus_queries(flow_name: &str, stage_name: &str) -> Vec<(&'static str, String)> {
        vec![
            (
                "Saturation (Queue Depth)",
                format!(
                    "obzenflow_queue_depth{{flow=\"{}\",stage=\"{}\"}}",
                    flow_name, stage_name
                )
            ),
            (
                "In-Flight Events",
                format!(
                    "obzenflow_in_flight_events{{flow=\"{}\",stage=\"{}\"}}",
                    flow_name, stage_name
                )
            ),
            (
                "Failure Rate",
                format!(
                    "rate(obzenflow_errors_total{{flow=\"{}\",stage=\"{}\"}}[5m])",
                    flow_name, stage_name
                )
            ),
            (
                "Error Percentage",
                format!(
                    "rate(obzenflow_errors_total{{flow=\"{}\",stage=\"{}\"}}[5m]) / rate(obzenflow_events_total{{flow=\"{}\",stage=\"{}\"}}[5m]) * 100",
                    flow_name, stage_name, flow_name, stage_name
                )
            ),
            // Note: Amendments and Anomalies require custom metrics
        ]
    }
    
    /// Get Grafana dashboard JSON for SAAFE metrics
    pub fn grafana_dashboard(flow_name: &str) -> serde_json::Value {
        serde_json::json!({
            "title": format!("SAAFE Metrics - {}", flow_name),
            "panels": [
                {
                    "title": "Saturation",
                    "targets": [
                        {
                            "expr": format!("obzenflow_queue_depth{{flow=\"{}\"}}", flow_name),
                            "legendFormat": "Queue Depth"
                        },
                        {
                            "expr": format!("obzenflow_in_flight_events{{flow=\"{}\"}}", flow_name),
                            "legendFormat": "In-Flight"
                        }
                    ]
                },
                {
                    "title": "Failures & Errors",
                    "targets": [
                        {
                            "expr": format!("rate(obzenflow_errors_total{{flow=\"{}\"}}[5m])", flow_name),
                            "legendFormat": "Error Rate"
                        },
                        {
                            "expr": format!("sum(increase(obzenflow_errors_total{{flow=\"{}\"}}[1h]))", flow_name),
                            "legendFormat": "Errors (1h)"
                        }
                    ]
                },
                {
                    "title": "Data Quality",
                    "description": "Amendments and Anomalies require custom application metrics",
                    "targets": []
                }
            ],
            "annotations": [
                {
                    "description": "To track amendments and anomalies, emit custom control events with metric_type='amendment' or 'anomaly'"
                }
            ]
        })
    }
    
    /// Example code for emitting custom SAAFE metrics
    pub fn custom_metric_examples() -> &'static str {
        r#"
// Track data amendments
ctx.write_control_event(ChainEvent::control(
    ChainEvent::CONTROL_METRICS_CUSTOM,
    json!({
        "metric_type": "saafe.amendment",
        "stage": ctx.stage_name(),
        "flow": ctx.flow_name(),
        "amendment_type": "duplicate_removed",
        "count": duplicate_count
    })
));

// Track anomalies
ctx.write_control_event(ChainEvent::control(
    ChainEvent::CONTROL_METRICS_CUSTOM,
    json!({
        "metric_type": "saafe.anomaly", 
        "stage": ctx.stage_name(),
        "flow": ctx.flow_name(),
        "anomaly_type": "value_spike",
        "severity": "high",
        "details": {
            "expected_range": [0, 100],
            "actual_value": 250
        }
    })
));
"#
    }
}