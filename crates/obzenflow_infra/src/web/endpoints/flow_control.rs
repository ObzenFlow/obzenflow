// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow control HTTP endpoint (Play / Pause / Stop)
//!
//! Provides a small control surface for server-mode flows so that
//! operators (or the UI) can start and stop the pipeline without
//! restarting the process.

use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, ManagedResponse, Request, Response, WebError};
use obzenflow_runtime::pipeline::FlowHandle;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Supported control actions for a flow.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowControlAction {
    Play,
    Pause,
    Stop,
}

/// Stop mode for FlowControlAction::Stop.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowStopMode {
    /// Stop as quickly as possible (Cancel semantics).
    Cancel,
    /// Stop intake and attempt a bounded drain (GracefulStop semantics).
    Graceful,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlRequest {
    pub action: FlowControlAction,
    /// Optional stop mode for `action=stop` (defaults to `cancel`).
    #[serde(default)]
    pub stop_mode: Option<FlowStopMode>,
    /// Optional timeout (seconds) for graceful stop.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowControlStatus {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlResponse {
    pub status: FlowControlStatus,
    pub message: String,
}

/// HTTP endpoint that controls a single flow via FlowHandle.
pub struct FlowControlEndpoint {
    flow_handle: Arc<FlowHandle>,
}

impl FlowControlEndpoint {
    pub fn new(flow_handle: Arc<FlowHandle>) -> Self {
        Self { flow_handle }
    }
}

#[async_trait]
impl HttpEndpoint for FlowControlEndpoint {
    fn path(&self) -> &str {
        "/api/flow/control"
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Post]
    }

    async fn handle(&self, request: Request) -> Result<ManagedResponse, WebError> {
        // Parse JSON body
        let req: FlowControlRequest = match serde_json::from_slice(&request.body) {
            Ok(r) => r,
            Err(e) => {
                let resp = FlowControlResponse {
                    status: FlowControlStatus::Rejected,
                    message: format!("Invalid request body: {e}"),
                };
                return Response::new(400)
                    .with_json(&resp)
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })
                    .map(Into::into);
            }
        };

        fn default_grace_timeout() -> Duration {
            let shutdown_timeout_secs = std::env::var("OBZENFLOW_SHUTDOWN_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(30);
            Duration::from_secs(shutdown_timeout_secs)
        }

        let result = match req.action {
            FlowControlAction::Play => {
                tracing::info!("FlowControlEndpoint: Play requested");
                self.flow_handle.start().await
            }
            FlowControlAction::Pause => {
                // Pause semantics are not yet implemented at the FSM level.
                // For now, explicitly reject with a descriptive message.
                tracing::info!("FlowControlEndpoint: Pause requested but not supported");
                return ok_json_response(FlowControlResponse {
                    status: FlowControlStatus::Rejected,
                    message: "Pause is not yet supported for this flow".to_string(),
                });
            },
            FlowControlAction::Stop => {
                let mode = req.stop_mode.or_else(|| {
                    request.query_params.get("mode").and_then(|s| {
                        match s.to_ascii_lowercase().as_str() {
                            "cancel" => Some(FlowStopMode::Cancel),
                            "graceful" => Some(FlowStopMode::Graceful),
                            _ => None,
                        }
                    })
                });
                let timeout_secs = req.timeout_secs.or_else(|| {
                    request
                        .query_params
                        .get("timeout_secs")
                        .and_then(|s| s.parse::<u64>().ok())
                });

                match mode.unwrap_or(FlowStopMode::Cancel) {
                    FlowStopMode::Cancel => {
                        tracing::info!("FlowControlEndpoint: Stop requested (cancel)");
                        self.flow_handle.stop_cancel().await
                    }
                    FlowStopMode::Graceful => {
                        let timeout = timeout_secs
                            .map(Duration::from_secs)
                            .unwrap_or_else(default_grace_timeout);
                        tracing::info!(
                            timeout_secs = timeout.as_secs(),
                            "FlowControlEndpoint: Stop requested (graceful)"
                        );
                        self.flow_handle.stop_graceful(timeout).await
                    }
                }
            }
        };

        match result {
            Ok(()) => ok_json_response(FlowControlResponse {
                status: FlowControlStatus::Accepted,
                message: format!("Action {:?} accepted", req.action),
            }),
            Err(e) => {
                tracing::error!("Flow control action {:?} failed: {}", req.action, e);
                ok_json_response(FlowControlResponse {
                    status: FlowControlStatus::Rejected,
                    message: format!("Action {:?} failed: {}", req.action, e),
                })
            }
        }
    }
}

fn ok_json_response(body: FlowControlResponse) -> Result<ManagedResponse, WebError> {
    Response::ok()
        .with_json(&body)
        .map_err(|e| WebError::RequestHandlingFailed {
            message: e.to_string(),
            source: None,
        })
        .map(Into::into)
}
