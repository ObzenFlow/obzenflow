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
use obzenflow_runtime::errors::FlowError;
use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlowControlStatus {
    Accepted,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FlowControlResponse {
    pub status: FlowControlStatus,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<String>,
}

/// HTTP endpoint that controls a single flow via FlowHandle.
pub struct FlowControlEndpoint {
    flow_handle: Arc<dyn FlowControlTarget>,
}

impl FlowControlEndpoint {
    pub fn new(flow_handle: Arc<FlowHandle>) -> Self {
        Self { flow_handle }
    }

    #[cfg(test)]
    fn new_for_target(flow_handle: Arc<dyn FlowControlTarget>) -> Self {
        Self { flow_handle }
    }
}

#[async_trait]
trait FlowControlTarget: Send + Sync {
    fn current_state(&self) -> PipelineState;
    async fn start(&self) -> Result<(), FlowError>;
    async fn stop_cancel(&self) -> Result<(), FlowError>;
    async fn stop_graceful(&self, timeout: Duration) -> Result<(), FlowError>;
}

#[async_trait]
impl FlowControlTarget for FlowHandle {
    fn current_state(&self) -> PipelineState {
        self.current_state()
    }

    async fn start(&self) -> Result<(), FlowError> {
        self.start().await
    }

    async fn stop_cancel(&self) -> Result<(), FlowError> {
        self.stop_cancel().await
    }

    async fn stop_graceful(&self, timeout: Duration) -> Result<(), FlowError> {
        self.stop_graceful(timeout).await
    }
}

fn pipeline_state_label(state: &PipelineState) -> String {
    match state {
        PipelineState::Created => "created",
        PipelineState::Materializing => "materializing",
        PipelineState::Materialized => "materialized",
        PipelineState::ReadyForRun => "readyforrun",
        PipelineState::Running => "running",
        PipelineState::SourceCompleted => "sourcecompleted",
        PipelineState::AbortRequested { .. } => "abortrequested",
        PipelineState::Draining => "draining",
        PipelineState::Drained => "drained",
        PipelineState::Failed { .. } => "failed",
    }
    .to_string()
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
                    state: None,
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
            obzenflow_runtime::bootstrap::shutdown_timeout()
        }

        let action = req.action.clone();
        let result = match action {
            FlowControlAction::Play => {
                tracing::info!("FlowControlEndpoint: Play requested");
                let state = self.flow_handle.current_state();
                let state_label = pipeline_state_label(&state);
                // Play is only start-actionable after the runtime has been
                // materialized and every non-source stage has reported running.
                // `Materialized` is still inside that readiness barrier.
                match state {
                    PipelineState::ReadyForRun => self.flow_handle.start().await,
                    PipelineState::Running => {
                        return ok_json_response(FlowControlResponse {
                            status: FlowControlStatus::Accepted,
                            message: "Play accepted: already running".to_string(),
                            state: Some(state_label),
                        });
                    }
                    _ => {
                        return ok_json_response(FlowControlResponse {
                            status: FlowControlStatus::Rejected,
                            message: format!(
                                "Play rejected: pipeline is not ready for run (state={state_label})"
                            ),
                            state: Some(state_label),
                        });
                    }
                }
            }
            FlowControlAction::Pause => {
                // Pause semantics are not yet implemented at the FSM level.
                // For now, explicitly reject with a descriptive message.
                tracing::info!("FlowControlEndpoint: Pause requested but not supported");
                let state = self.flow_handle.current_state();
                return ok_json_response(FlowControlResponse {
                    status: FlowControlStatus::Rejected,
                    message: "Pause is not yet supported for this flow".to_string(),
                    state: Some(pipeline_state_label(&state)),
                });
            }
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
                message: format!("Action {:?} accepted", action),
                state: Some(pipeline_state_label(&self.flow_handle.current_state())),
            }),
            Err(e) => {
                tracing::error!("Flow control action {:?} failed: {}", action, e);
                ok_json_response(FlowControlResponse {
                    status: FlowControlStatus::Rejected,
                    message: format!("Action {:?} failed: {}", action, e),
                    state: Some(pipeline_state_label(&self.flow_handle.current_state())),
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

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::types::ViolationCause;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    struct TestFlowTarget {
        state: Mutex<PipelineState>,
        starts: AtomicUsize,
        stop_cancels: AtomicUsize,
        stop_gracefuls: AtomicUsize,
    }

    impl TestFlowTarget {
        fn new(state: PipelineState) -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(state),
                starts: AtomicUsize::new(0),
                stop_cancels: AtomicUsize::new(0),
                stop_gracefuls: AtomicUsize::new(0),
            })
        }
    }

    #[async_trait]
    impl FlowControlTarget for TestFlowTarget {
        fn current_state(&self) -> PipelineState {
            self.state.lock().expect("state lock poisoned").clone()
        }

        async fn start(&self) -> Result<(), FlowError> {
            self.starts.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn stop_cancel(&self) -> Result<(), FlowError> {
            self.stop_cancels.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }

        async fn stop_graceful(&self, _timeout: Duration) -> Result<(), FlowError> {
            self.stop_gracefuls.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    fn request(action: &str) -> Request {
        Request::new(HttpMethod::Post, "/api/flow/control".to_string())
            .with_body(serde_json::to_vec(&json!({ "action": action })).expect("request json"))
    }

    async fn post_control(
        endpoint: &FlowControlEndpoint,
        action: &str,
    ) -> (u16, FlowControlResponse) {
        match endpoint
            .handle(request(action))
            .await
            .expect("endpoint response")
        {
            ManagedResponse::Unary(response) => {
                let body = serde_json::from_slice(&response.body).expect("response json");
                (response.status, body)
            }
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        }
    }

    #[tokio::test]
    async fn play_rejects_pre_ready_and_terminal_states_with_state_payload() {
        let cases = [
            (PipelineState::Created, "created"),
            (PipelineState::Materializing, "materializing"),
            (PipelineState::Materialized, "materialized"),
            (PipelineState::SourceCompleted, "sourcecompleted"),
            (
                PipelineState::AbortRequested {
                    reason: ViolationCause::Other("abort".to_string()),
                    upstream: None,
                },
                "abortrequested",
            ),
            (PipelineState::Draining, "draining"),
            (PipelineState::Drained, "drained"),
            (
                PipelineState::Failed {
                    reason: "failed".to_string(),
                    failure_cause: None,
                },
                "failed",
            ),
        ];

        for (state, expected_label) in cases {
            let target = TestFlowTarget::new(state);
            let endpoint = FlowControlEndpoint::new_for_target(target.clone());

            let (status, response) = post_control(&endpoint, "play").await;

            assert_eq!(status, 200);
            assert_eq!(response.status, FlowControlStatus::Rejected);
            assert_eq!(response.state.as_deref(), Some(expected_label));
            assert_eq!(target.starts.load(Ordering::Relaxed), 0);
        }
    }

    #[tokio::test]
    async fn play_in_ready_for_run_dispatches_start() {
        let target = TestFlowTarget::new(PipelineState::ReadyForRun);
        let endpoint = FlowControlEndpoint::new_for_target(target.clone());

        let (status, response) = post_control(&endpoint, "play").await;

        assert_eq!(status, 200);
        assert_eq!(response.status, FlowControlStatus::Accepted);
        assert_eq!(target.starts.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn play_in_running_is_idempotently_accepted_without_dispatch() {
        let target = TestFlowTarget::new(PipelineState::Running);
        let endpoint = FlowControlEndpoint::new_for_target(target.clone());

        let (status, response) = post_control(&endpoint, "play").await;

        assert_eq!(status, 200);
        assert_eq!(response.status, FlowControlStatus::Accepted);
        assert_eq!(response.state.as_deref(), Some("running"));
        assert!(response.message.contains("already running"));
        assert_eq!(target.starts.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn pause_rejects_with_current_state_payload() {
        let target = TestFlowTarget::new(PipelineState::Materialized);
        let endpoint = FlowControlEndpoint::new_for_target(target.clone());

        let (status, response) = post_control(&endpoint, "pause").await;

        assert_eq!(status, 200);
        assert_eq!(response.status, FlowControlStatus::Rejected);
        assert_eq!(response.state.as_deref(), Some("materialized"));
        assert_eq!(target.starts.load(Ordering::Relaxed), 0);
    }
}
