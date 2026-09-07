// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics HTTP endpoint
//!
//! Prometheus delivery over an owned read view. No Warp dependency.

use async_trait::async_trait;
use obzenflow_adapters::monitoring::{projections::PrometheusProjection, MetricsReadModel};
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use std::sync::Arc;

/// HTTP endpoint for Prometheus metrics
pub struct PrometheusMetricsEndpoint {
    model: Arc<MetricsReadModel>,
}

impl PrometheusMetricsEndpoint {
    pub fn new(model: Arc<MetricsReadModel>) -> Self {
        Self { model }
    }
}

#[async_trait]
impl HttpEndpoint for PrometheusMetricsEndpoint {
    fn path(&self) -> &str {
        "/metrics"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
        match PrometheusProjection::new().render(&self.model.snapshot()) {
            Ok(metrics) => {
                let mut response = Response::ok();
                response.headers.insert(
                    "Content-Type".to_string(),
                    "text/plain; version=0.0.4; charset=utf-8".to_string(),
                );
                response.body = metrics.into_bytes();
                Ok(response.into())
            }
            Err(e) => Ok(Response::internal_error()
                .with_text(&format!("Failed to render metrics: {e}"))
                .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::metrics::{AppMetricsSnapshot, MetricsSnapshotExporter};

    #[tokio::test]
    async fn portable_endpoint_uses_current_owned_view_and_exact_media_type() {
        let model = Arc::new(MetricsReadModel::default());
        let endpoint = PrometheusMetricsEndpoint::new(model.clone());
        assert_eq!(endpoint.path(), "/metrics");
        assert_eq!(endpoint.methods(), &[HttpMethod::Get]);
        for state in [None, Some("Running"), Some("quoted\"state\\with\nnewline")] {
            if let Some(state) = state {
                let mut snapshot = AppMetricsSnapshot::default();
                snapshot.pipeline_state = state.into();
                model.publish_app_snapshot(snapshot);
            }
            let ManagedResponse::Unary(response) = endpoint
                .handle(Request::new(HttpMethod::Get, "/metrics".into()))
                .await
                .unwrap()
            else {
                panic!("metrics must be a unary response");
            };
            assert_eq!(response.status, 200);
            assert_eq!(
                response.headers["Content-Type"],
                "text/plain; version=0.0.4; charset=utf-8"
            );
            let text = String::from_utf8(response.body).unwrap();
            assert!(text.contains(&format!(
                "obzenflow_build_info{{version=\"{}\"}} 1",
                env!("CARGO_PKG_VERSION")
            )));
            assert_eq!(text.contains("obzenflow_pipeline_state{"), state.is_some());
            if state.is_some_and(|state| state.starts_with("quoted")) {
                assert!(text.contains("state=\"quoted\\\"state\\\\with\\nnewline\""));
            }
        }
    }
}
