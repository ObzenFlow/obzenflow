//! USE (Utilization, Saturation, Errors) Taxonomy
//!
//! The USE method is a monitoring approach that focuses on:
//! - **Utilization**: The average time a resource was busy servicing work
//! - **Saturation**: The degree to which the resource has extra work it can't service
//! - **Errors**: The count of error events
//!
//! This taxonomy is ideal for resource-oriented monitoring (CPU, memory, queues).
//!
//! ## Metrics Available in ObzenFlow
//!
//! With FLOWIP-056-666, USE metrics are automatically derived from control events:
//!
//! | Metric | Prometheus Name | Description |
//! |--------|----------------|-------------|
//! | Utilization | `obzenflow_cpu_usage_ratio` | CPU usage (0.0-1.0) |
//! | Saturation | `obzenflow_queue_depth` | Queue depth / backlog |
//! | Errors | `obzenflow_errors_total` | Total errors encountered |
//!
//! ## Example Prometheus Queries
//!
//! ```promql
//! # CPU Utilization (percentage)
//! obzenflow_cpu_usage_ratio{flow="data_processing"} * 100
//!
//! # Queue Saturation
//! obzenflow_queue_depth{flow="data_processing"}
//!
//! # Error rate
//! rate(obzenflow_errors_total{flow="data_processing"}[5m])
//! ```

/// USE taxonomy definition
///
/// USE focuses on Utilization, Saturation, and Errors - key metrics
/// for understanding resource health and capacity.
pub struct USE;

impl USE {
    /// Taxonomy name
    pub const NAME: &'static str = "USE";

    /// Human-readable description
    pub const DESCRIPTION: &'static str =
        "Utilization, Saturation, Errors - ideal for resource monitoring";

    /// Get Prometheus queries for USE metrics
    pub fn prometheus_queries(flow_name: &str, stage_name: &str) -> Vec<(&'static str, String)> {
        vec![
            (
                "CPU Utilization (%)",
                format!(
                    "obzenflow_cpu_usage_ratio{{flow=\"{flow_name}\",stage=\"{stage_name}\"}} * 100"
                ),
            ),
            (
                "Memory Usage (MB)",
                format!(
                    "obzenflow_memory_bytes{{flow=\"{flow_name}\",stage=\"{stage_name}\"}} / 1024 / 1024"
                ),
            ),
            (
                "Saturation (In-Flight Events)",
                format!(
                    "obzenflow_in_flight_events{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}"
                ),
            ),
            (
                "Error Rate",
                format!(
                    "rate(obzenflow_errors_total{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m])"
                ),
            ),
        ]
    }

    /// Get Grafana dashboard JSON for USE metrics
    pub fn grafana_dashboard(flow_name: &str) -> serde_json::Value {
        serde_json::json!({
            "title": format!("USE Metrics - {}", flow_name),
            "panels": [
                {
                    "title": "CPU Utilization",
                    "targets": [{
                        "expr": format!("obzenflow_cpu_usage_ratio{{flow=\"{}\"}} * 100", flow_name)
                    }],
                    "unit": "percent"
                },
                {
                    "title": "Memory Usage",
                    "targets": [{
                        "expr": format!("obzenflow_memory_bytes{{flow=\"{}\"}}", flow_name)
                    }],
                    "unit": "bytes"
                },
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
                    "title": "Error Rate",
                    "targets": [{
                        "expr": format!("rate(obzenflow_errors_total{{flow=\"{}\"}}[5m])", flow_name)
                    }]
                }
            ]
        })
    }
}
