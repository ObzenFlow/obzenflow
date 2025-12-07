//! Flow control HTTP endpoint (Play / Pause / Stop)
//!
//! Provides a small control surface for server-mode flows so that
//! operators (or the UI) can start and stop the pipeline without
//! restarting the process.

use async_trait::async_trait;
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Request, Response, WebError};
use obzenflow_runtime_services::pipeline::FlowHandle;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Supported control actions for a flow.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowControlAction {
    Play,
    Pause,
    Stop,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlRequest {
    pub action: FlowControlAction,
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

    async fn handle(&self, request: Request) -> Result<Response, WebError> {
        // Parse JSON body
        let req: FlowControlRequest = match serde_json::from_slice(&request.body) {
            Ok(r) => r,
            Err(e) => {
                let resp = FlowControlResponse {
                    status: FlowControlStatus::Rejected,
                    message: format!("Invalid request body: {}", e),
                };
                return Response::new(400).with_json(&resp).map_err(|err| WebError::RequestHandlingFailed {
                    message: err.to_string(),
                    source: None,
                });
            }
        };

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
            }
            FlowControlAction::Stop => {
                tracing::info!("FlowControlEndpoint: Stop requested");
                self.flow_handle.shutdown().await
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

fn ok_json_response(body: FlowControlResponse) -> Result<Response, WebError> {
    Response::ok().with_json(&body).map_err(|e| WebError::RequestHandlingFailed {
        message: e.to_string(),
        source: None,
    })
}

