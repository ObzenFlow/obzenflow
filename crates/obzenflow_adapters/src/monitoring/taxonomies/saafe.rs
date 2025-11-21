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
//! With FLOWIP-056-666, core metrics are emitted automatically from journals; consult
//! obzenflow_adapters metrics documentation for current coverage.

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
                "Saturation (In-Flight Events)",
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

}
