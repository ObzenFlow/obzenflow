//! Golden Signals Taxonomy
//!
//! The Four Golden Signals are the most important metrics to monitor:
//! - **Latency**: The time it takes to service a request
//! - **Traffic**: A measure of how much demand is placed on the system
//! - **Errors**: The rate of requests that fail
//! - **Saturation**: How "full" the service is
//!
//! This taxonomy comes from the Google SRE book and is ideal for service monitoring.
//!
//! ## Metrics Available in ObzenFlow
//!
//! With FLOWIP-056-666, Golden Signals are automatically derived from events:
//!
//! | Signal | Prometheus Name | Description |
//! |--------|----------------|-------------|
//! | Latency | `obzenflow_duration_seconds` | Request duration histogram |
//! | Traffic | `obzenflow_events_total` | Total requests (use `rate()`) |
//! | Errors | `obzenflow_errors_total` | Failed requests |
//! | Saturation | `obzenflow_in_flight` | In-flight processing as saturation proxy |
//!
//! ## Example Prometheus Queries
//!
//! ```promql
//! # Traffic (requests per second)
//! rate(obzenflow_events_total{flow="api_gateway"}[5m])
//!
//! # Latency (95th percentile)
//! histogram_quantile(0.95, rate(obzenflow_duration_seconds_bucket[5m]))
//!
//! # Error rate (percentage)
//! rate(obzenflow_errors_total[5m]) / rate(obzenflow_events_total[5m]) * 100
//!
//! # Saturation
//! obzenflow_in_flight{flow="api_gateway"}  # Current processing pressure
//! ```

/// Golden Signals taxonomy definition
///
/// The Four Golden Signals from Google SRE: Latency, Traffic, Errors, and Saturation.
pub struct GoldenSignals;

impl GoldenSignals {
    /// Taxonomy name
    pub const NAME: &'static str = "GoldenSignals";

    /// Human-readable description
    pub const DESCRIPTION: &'static str =
        "Latency, Traffic, Errors, Saturation - Google SRE's four golden signals";

    /// Get Prometheus queries for Golden Signals
    pub fn prometheus_queries(flow_name: &str, stage_name: &str) -> Vec<(&'static str, String)> {
        vec![
            (
                "Traffic (req/s)",
                format!(
                    "rate(obzenflow_events_total{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m])"
                )
            ),
            (
                "Latency P50",
                format!(
                    "histogram_quantile(0.5, rate(obzenflow_duration_seconds_bucket{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]))"
                )
            ),
            (
                "Latency P95",
                format!(
                    "histogram_quantile(0.95, rate(obzenflow_duration_seconds_bucket{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]))"
                )
            ),
            (
                "Latency P99",
                format!(
                    "histogram_quantile(0.99, rate(obzenflow_duration_seconds_bucket{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]))"
                )
            ),
            (
                "Error Rate (%)",
                format!(
                    "rate(obzenflow_errors_total{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]) / rate(obzenflow_events_total{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]) * 100"
                )
            ),
            (
                "Saturation (In-Flight)",
                format!(
                    "obzenflow_in_flight{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}"
                )
            ),
        ]
    }

    /// Get Grafana dashboard JSON for Golden Signals
    pub fn grafana_dashboard(flow_name: &str) -> serde_json::Value {
        serde_json::json!({
            "title": format!("Golden Signals - {}", flow_name),
            "panels": [
                {
                    "title": "Traffic",
                    "targets": [{
                        "expr": format!("rate(obzenflow_events_total{{flow=\"{}\"}}[5m])", flow_name)
                    }],
                    "unit": "reqps"
                },
                {
                    "title": "Latency",
                    "targets": [
                        {
                            "expr": format!("histogram_quantile(0.5, rate(obzenflow_duration_seconds_bucket{{flow=\"{}\"}}[5m]))", flow_name),
                            "legendFormat": "P50"
                        },
                        {
                            "expr": format!("histogram_quantile(0.95, rate(obzenflow_duration_seconds_bucket{{flow=\"{}\"}}[5m]))", flow_name),
                            "legendFormat": "P95"
                        },
                        {
                            "expr": format!("histogram_quantile(0.99, rate(obzenflow_duration_seconds_bucket{{flow=\"{}\"}}[5m]))", flow_name),
                            "legendFormat": "P99"
                        }
                    ],
                    "unit": "s"
                },
                {
                    "title": "Errors",
                    "targets": [
                        {
                            "expr": format!("rate(obzenflow_errors_total{{flow=\"{}\"}}[5m])", flow_name),
                            "legendFormat": "Error Rate"
                        },
                        {
                            "expr": format!("rate(obzenflow_errors_total{{flow=\"{}\"}}[5m]) / rate(obzenflow_events_total{{flow=\"{}\"}}[5m]) * 100", flow_name, flow_name),
                            "legendFormat": "Error %"
                        }
                    ]
                },
                {
                    "title": "Saturation",
                    "targets": [
                        {
                            "expr": format!("obzenflow_in_flight_events{{flow=\"{}\"}}", flow_name),
                            "legendFormat": "In-Flight"
                        }
                    ]
                }
            ]
        })
    }
}
