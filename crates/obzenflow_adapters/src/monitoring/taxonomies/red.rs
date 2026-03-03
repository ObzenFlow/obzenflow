// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! RED (Rate, Errors, Duration) Taxonomy
//!
//! The RED method is a monitoring approach that focuses on:
//! - **Rate**: The number of requests per second
//! - **Errors**: The number of failed requests
//! - **Duration**: The amount of time requests take
//!
//! This taxonomy is ideal for request/response systems and user-facing services.
//!
//! ## Metrics Available in ObzenFlow
//!
//! RED metrics are automatically derived from the event journal:
//!
//! | Metric | Prometheus Name | Description |
//! |--------|----------------|-------------|
//! | Rate | `obzenflow_events_total` | Total events processed (use `rate()` function) |
//! | Errors | `obzenflow_errors_total` | Total errors encountered |
//! | Duration | `obzenflow_duration_seconds` | Processing time histogram |
//!
//! ## Example Prometheus Queries
//!
//! ```promql
//! # Request rate (per second)
//! rate(obzenflow_events_total{flow="payment_processing"}[5m])
//!
//! # Error rate (percentage)
//! rate(obzenflow_errors_total[5m]) / rate(obzenflow_events_total[5m]) * 100
//!
//! # 99th percentile latency
//! histogram_quantile(0.99, rate(obzenflow_duration_seconds_bucket[5m]))
//! ```

/// RED taxonomy definition
///
/// RED focuses on Rate, Errors, and Duration - the three key metrics
/// for understanding request/response systems.
pub struct RED;

impl RED {
    /// Taxonomy name
    pub const NAME: &'static str = "RED";

    /// Human-readable description
    pub const DESCRIPTION: &'static str =
        "Rate, Errors, Duration - ideal for request/response systems";

    /// Get Prometheus queries for RED metrics
    pub fn prometheus_queries(flow_name: &str, stage_name: &str) -> Vec<(&'static str, String)> {
        vec![
            (
                "Request Rate (req/s)",
                format!(
                    "rate(obzenflow_events_total{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m])"
                )
            ),
            (
                "Error Rate (%)",
                format!(
                    "rate(obzenflow_errors_total{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]) / rate(obzenflow_events_total{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]) * 100"
                )
            ),
            (
                "P50 Latency",
                format!(
                    "histogram_quantile(0.5, rate(obzenflow_duration_seconds_bucket{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]))"
                )
            ),
            (
                "P99 Latency", 
                format!(
                    "histogram_quantile(0.99, rate(obzenflow_duration_seconds_bucket{{flow=\"{flow_name}\",stage=\"{stage_name}\"}}[5m]))"
                )
            ),
        ]
    }

    /// Get Grafana dashboard JSON for RED metrics
    pub fn grafana_dashboard(flow_name: &str) -> serde_json::Value {
        serde_json::json!({
            "title": format!("RED Metrics - {}", flow_name),
            "panels": [
                {
                    "title": "Request Rate",
                    "targets": [{
                        "expr": format!("rate(obzenflow_events_total{{flow=\"{}\"}}[5m])", flow_name)
                    }]
                },
                {
                    "title": "Error Rate",
                    "targets": [{
                        "expr": format!("rate(obzenflow_errors_total{{flow=\"{}\"}}[5m]) / rate(obzenflow_events_total{{flow=\"{}\"}}[5m]) * 100", flow_name, flow_name)
                    }]
                },
                {
                    "title": "Latency (P50, P95, P99)",
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
                    ]
                }
            ]
        })
    }
}
