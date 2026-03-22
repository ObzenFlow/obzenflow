// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics HTTP endpoint
//!
//! Re-exports the metrics endpoint from metrics_server.rs

use async_trait::async_trait;
use obzenflow_core::metrics::MetricsExporter;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use std::sync::Arc;

/// HTTP endpoint for Prometheus metrics
pub struct MetricsHttpEndpoint {
    exporter: Arc<dyn MetricsExporter>,
}

impl MetricsHttpEndpoint {
    pub fn new(exporter: Arc<dyn MetricsExporter>) -> Self {
        Self { exporter }
    }
}

#[async_trait]
impl HttpEndpoint for MetricsHttpEndpoint {
    fn path(&self) -> &str {
        "/metrics"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Get]
    }

    async fn handle(&self, _request: Request) -> Result<ManagedResponse, WebError> {
        match self.exporter.render_metrics() {
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
