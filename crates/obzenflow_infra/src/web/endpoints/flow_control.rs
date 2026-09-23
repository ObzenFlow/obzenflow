// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow control HTTP endpoint (Play / Pause / Stop)
//!
//! Provides a small control surface for server-mode flows so that
//! operators (or the UI) can start and stop the pipeline without
//! restarting the process.

use crate::web::run_control::{
    RunControlTarget, TargetedFlowControlRequest, TargetedFlowControlResponse,
    RUN_CONTROL_PROTOCOL_VERSION,
};
use async_trait::async_trait;
use obzenflow_core::web::{
    EndpointError, HttpEndpoint, HttpMethod, ManagedResponse, Request, Response,
};
use obzenflow_runtime::errors::FlowError;
use obzenflow_runtime::pipeline::{FlowHandle, FlowStartControlOutcome, PipelineState};
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
#[serde(deny_unknown_fields)]
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
    target: Option<RunControlTarget>,
}

impl FlowControlEndpoint {
    pub fn new(flow_handle: Arc<FlowHandle>) -> Self {
        Self {
            flow_handle,
            target: None,
        }
    }

    #[cfg(test)]
    fn new_for_target(flow_handle: Arc<dyn FlowControlTarget>) -> Self {
        Self {
            flow_handle,
            target: None,
        }
    }

    /// Bind conditional control to this host incarnation and its existing handle.
    pub fn with_target(mut self, target: RunControlTarget) -> Self {
        self.target = Some(target);
        self
    }

    fn response(
        &self,
        status: u16,
        versioned: bool,
        result: FlowControlResponse,
    ) -> Result<ManagedResponse, EndpointError> {
        let response = Response::new(status);
        let encoded = match (versioned, &self.target) {
            (true, Some(target)) => response.with_json(&TargetedFlowControlResponse {
                protocol_version: RUN_CONTROL_PROTOCOL_VERSION,
                target: target.clone(),
                result,
            }),
            _ => response.with_json(&result),
        };
        encoded
            .map(Into::into)
            .map_err(|error| EndpointError::with_source("Serialising flow control", error))
    }
}

#[async_trait]
trait FlowControlTarget: Send + Sync {
    fn current_state(&self) -> PipelineState;
    async fn start_if_ready_now(&self) -> Result<FlowStartControlOutcome, FlowError>;
    async fn stop_cancel(&self) -> Result<(), FlowError>;
    async fn stop_graceful(&self, timeout: Duration) -> Result<(), FlowError>;
}

#[async_trait]
impl FlowControlTarget for FlowHandle {
    fn current_state(&self) -> PipelineState {
        self.current_state()
    }

    async fn start_if_ready_now(&self) -> Result<FlowStartControlOutcome, FlowError> {
        FlowHandle::start_if_ready_now(self).await
    }

    async fn stop_cancel(&self) -> Result<(), FlowError> {
        self.stop_cancel().await
    }

    async fn stop_graceful(&self, timeout: Duration) -> Result<(), FlowError> {
        self.stop_graceful(timeout).await
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

    async fn handle(&self, request: Request) -> Result<ManagedResponse, EndpointError> {
        let reject = |message: String| FlowControlResponse {
            status: FlowControlStatus::Rejected,
            message,
            state: None,
        };
        let value: serde_json::Value = match serde_json::from_slice(&request.body) {
            Ok(value) => value,
            Err(error) => {
                return self.response(400, false, reject(format!("Invalid request body: {error}")))
            }
        };
        let versioned = ["protocol_version", "target", "control"]
            .iter()
            .any(|field| value.get(field).is_some());
        let req: FlowControlRequest = if versioned {
            let envelope: TargetedFlowControlRequest = match serde_json::from_value(value) {
                Ok(envelope) => envelope,
                Err(error) => {
                    return self.response(
                        400,
                        true,
                        reject(format!("Invalid conditional control: {error}")),
                    )
                }
            };
            if envelope.protocol_version != RUN_CONTROL_PROTOCOL_VERSION
                || !envelope.target.pipeline_writer_id.is_system()
            {
                return self.response(
                    400,
                    true,
                    reject("Unsupported protocol or invalid pipeline writer".into()),
                );
            }
            if self.target.as_ref() != Some(&envelope.target) {
                tracing::warn!(expected = ?envelope.target, actual = ?self.target, "run_target_mismatch");
                return self.response(
                    409,
                    true,
                    reject("Selected host/run has changed; no control was submitted".into()),
                );
            }
            envelope.control
        } else {
            match serde_json::from_value(value) {
                Ok(control) => control,
                Err(error) => {
                    return self.response(
                        400,
                        false,
                        reject(format!("Invalid request body: {error}")),
                    )
                }
            }
        };
        let ok_json_response = |body| self.response(200, versioned, body);

        fn default_grace_timeout() -> Duration {
            obzenflow_runtime::bootstrap::shutdown_timeout()
        }

        let action = req.action.clone();
        let result = match action {
            FlowControlAction::Play => {
                tracing::info!("FlowControlEndpoint: Play requested");
                match self.flow_handle.start_if_ready_now().await {
                    Ok(FlowStartControlOutcome::Submitted {
                        observed_state: state,
                    }) => {
                        return ok_json_response(FlowControlResponse {
                            status: FlowControlStatus::Accepted,
                            message: "Play accepted".to_string(),
                            state: Some(state_diagnostic_label(&state).to_string()),
                        });
                    }
                    Ok(FlowStartControlOutcome::AlreadyRunning { state }) => {
                        return ok_json_response(FlowControlResponse {
                            status: FlowControlStatus::Accepted,
                            message: "Play accepted: already running".to_string(),
                            state: Some(state_diagnostic_label(&state).to_string()),
                        });
                    }
                    Ok(FlowStartControlOutcome::Rejected { state, reason }) => {
                        let state_label = state_diagnostic_label(&state).to_string();
                        return ok_json_response(FlowControlResponse {
                            status: FlowControlStatus::Rejected,
                            message: format!("Play rejected: {reason} (state={state_label})"),
                            state: Some(state_label),
                        });
                    }
                    Err(e) => {
                        tracing::error!("Flow control action {:?} failed: {}", action, e);
                        return ok_json_response(FlowControlResponse {
                            status: FlowControlStatus::Rejected,
                            message: format!("Action {:?} failed: {}", action, e),
                            state: Some(
                                state_diagnostic_label(&self.flow_handle.current_state())
                                    .to_string(),
                            ),
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
                    state: Some(state_diagnostic_label(&state).to_string()),
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
                state: Some(state_diagnostic_label(&self.flow_handle.current_state()).to_string()),
            }),
            Err(e) => {
                tracing::error!("Flow control action {:?} failed: {}", action, e);
                ok_json_response(FlowControlResponse {
                    status: FlowControlStatus::Rejected,
                    message: format!("Action {:?} failed: {}", action, e),
                    state: Some(
                        state_diagnostic_label(&self.flow_handle.current_state()).to_string(),
                    ),
                })
            }
        }
    }
}

fn state_diagnostic_label(state: &PipelineState) -> &'static str {
    match state {
        PipelineState::Created => "created",
        PipelineState::Materializing => "materializing",
        PipelineState::Materialized => "materialized",
        PipelineState::ReadyForRun => "ready_for_run",
        PipelineState::Running => "running",
        PipelineState::SourceCompleted => "source_completed",
        PipelineState::AbortRequested { .. } => "abort_requested",
        PipelineState::Draining => "draining",
        PipelineState::Drained => "drained",
        PipelineState::Failed { .. } => "failed",
    }
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
        play_outcome: Mutex<FlowStartControlOutcome>,
        play_calls: AtomicUsize,
        stop_cancels: AtomicUsize,
        stop_gracefuls: AtomicUsize,
    }

    impl TestFlowTarget {
        fn new(state: PipelineState) -> Arc<Self> {
            Self::with_play_outcome(
                state.clone(),
                FlowStartControlOutcome::Rejected {
                    state,
                    reason: "pipeline is not ready for run",
                },
            )
        }

        fn with_play_outcome(
            state: PipelineState,
            play_outcome: FlowStartControlOutcome,
        ) -> Arc<Self> {
            Arc::new(Self {
                state: Mutex::new(state),
                play_outcome: Mutex::new(play_outcome),
                play_calls: AtomicUsize::new(0),
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

        async fn start_if_ready_now(&self) -> Result<FlowStartControlOutcome, FlowError> {
            self.play_calls.fetch_add(1, Ordering::Relaxed);
            Ok(self
                .play_outcome
                .lock()
                .expect("play outcome lock poisoned")
                .clone())
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
            (PipelineState::SourceCompleted, "source_completed"),
            (
                PipelineState::AbortRequested {
                    reason: ViolationCause::Other("abort".to_string()),
                    upstream: None,
                },
                "abort_requested",
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
            let target = TestFlowTarget::with_play_outcome(
                state.clone(),
                FlowStartControlOutcome::Rejected {
                    state,
                    reason: "pipeline is not ready for run",
                },
            );
            let endpoint = FlowControlEndpoint::new_for_target(target.clone());

            let (status, response) = post_control(&endpoint, "play").await;

            assert_eq!(status, 200);
            assert_eq!(response.status, FlowControlStatus::Rejected);
            assert_eq!(response.state.as_deref(), Some(expected_label));
            assert_eq!(target.play_calls.load(Ordering::Relaxed), 1);
        }
    }

    #[tokio::test]
    async fn play_maps_submitted_runtime_outcome() {
        let target = TestFlowTarget::with_play_outcome(
            PipelineState::Materialized,
            FlowStartControlOutcome::Submitted {
                observed_state: PipelineState::ReadyForRun,
            },
        );
        let endpoint = FlowControlEndpoint::new_for_target(target.clone());

        let (status, response) = post_control(&endpoint, "play").await;

        assert_eq!(status, 200);
        assert_eq!(response.status, FlowControlStatus::Accepted);
        assert_eq!(response.state.as_deref(), Some("ready_for_run"));
        assert_eq!(response.message, "Play accepted");
        assert_eq!(target.play_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn play_maps_already_running_runtime_outcome() {
        let target = TestFlowTarget::with_play_outcome(
            PipelineState::Materialized,
            FlowStartControlOutcome::AlreadyRunning {
                state: PipelineState::Running,
            },
        );
        let endpoint = FlowControlEndpoint::new_for_target(target.clone());

        let (status, response) = post_control(&endpoint, "play").await;

        assert_eq!(status, 200);
        assert_eq!(response.status, FlowControlStatus::Accepted);
        assert_eq!(response.state.as_deref(), Some("running"));
        assert!(response.message.contains("already running"));
        assert_eq!(target.play_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn pause_rejects_with_current_state_payload() {
        let target = TestFlowTarget::new(PipelineState::Materialized);
        let endpoint = FlowControlEndpoint::new_for_target(target.clone());

        let (status, response) = post_control(&endpoint, "pause").await;

        assert_eq!(status, 200);
        assert_eq!(response.status, FlowControlStatus::Rejected);
        assert_eq!(response.state.as_deref(), Some("materialized"));
        assert_eq!(target.play_calls.load(Ordering::Relaxed), 0);
    }

    fn binding() -> RunControlTarget {
        RunControlTarget {
            runtime_instance_id: crate::web::RuntimeInstanceId::new(),
            pipeline_writer_id: obzenflow_core::SystemId::new().into(),
        }
    }

    fn conditional(target: &RunControlTarget) -> serde_json::Value {
        json!({ "protocol_version": 1, "target": target, "control": { "action": "play" } })
    }

    async fn post_json(endpoint: &FlowControlEndpoint, body: serde_json::Value) -> Response {
        let request = Request::new(HttpMethod::Post, "/api/flow/control".into())
            .with_body(serde_json::to_vec(&body).unwrap());
        match endpoint.handle(request).await.unwrap() {
            ManagedResponse::Unary(response) => response,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        }
    }

    #[tokio::test]
    async fn conditional_control_checks_both_identities_before_any_runtime_call() {
        let runtime = TestFlowTarget::new(PipelineState::ReadyForRun);
        let actual = binding();
        let endpoint =
            FlowControlEndpoint::new_for_target(runtime.clone()).with_target(actual.clone());
        for stale in [
            RunControlTarget {
                runtime_instance_id: binding().runtime_instance_id,
                ..actual.clone()
            },
            RunControlTarget {
                pipeline_writer_id: binding().pipeline_writer_id,
                ..actual.clone()
            },
        ] {
            let response = post_json(&endpoint, conditional(&stale)).await;
            assert_eq!(response.status, 409);
            let response: TargetedFlowControlResponse =
                serde_json::from_slice(&response.body).unwrap();
            assert_eq!(response.target, actual);
            assert_eq!(response.result.status, FlowControlStatus::Rejected);
        }
        assert_eq!(runtime.play_calls.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.stop_cancels.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.stop_gracefuls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn malformed_or_mixed_envelopes_never_fall_back_to_legacy_control() {
        let runtime = TestFlowTarget::new(PipelineState::ReadyForRun);
        let actual = binding();
        let endpoint =
            FlowControlEndpoint::new_for_target(runtime.clone()).with_target(actual.clone());
        let mut mixed = conditional(&actual);
        mixed["action"] = json!("play");
        let mut future = conditional(&actual);
        future["protocol_version"] = json!(2);
        let mut unknown = conditional(&actual);
        unknown["control"]["unexpected"] = json!(true);
        let mut wrong_writer = conditional(&actual);
        wrong_writer["target"]["pipeline_writer_id"] = serde_json::to_value(
            obzenflow_core::WriterId::from(obzenflow_core::StageId::new()),
        )
        .unwrap();
        for body in [
            mixed,
            future,
            unknown,
            wrong_writer,
            json!({"action":"play", "target":actual}),
            json!({"action":"play", "protocol_version":1}),
            json!({"action":"play", "control":null}),
        ] {
            assert_eq!(post_json(&endpoint, body).await.status, 400);
        }
        assert_eq!(runtime.play_calls.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.stop_cancels.load(Ordering::Relaxed), 0);
        assert_eq!(runtime.stop_gracefuls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn conditional_response_echoes_binding_and_preserves_typed_runtime_result() {
        let runtime = TestFlowTarget::with_play_outcome(
            PipelineState::ReadyForRun,
            FlowStartControlOutcome::Submitted {
                observed_state: PipelineState::ReadyForRun,
            },
        );
        let target = binding();
        let endpoint =
            FlowControlEndpoint::new_for_target(runtime.clone()).with_target(target.clone());
        let response = post_json(&endpoint, conditional(&target)).await;
        assert_eq!(response.status, 200);
        let response: TargetedFlowControlResponse = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response.protocol_version, 1);
        assert_eq!(response.target, target);
        assert_eq!(response.result.status, FlowControlStatus::Accepted);
        *runtime.play_outcome.lock().unwrap() = FlowStartControlOutcome::Rejected {
            state: PipelineState::Drained,
            reason: "finished",
        };
        let response = post_json(&endpoint, conditional(&target)).await;
        assert_eq!(response.status, 200);
        let response: TargetedFlowControlResponse = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response.result.status, FlowControlStatus::Rejected);
        assert_eq!(runtime.play_calls.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn conditional_play_is_inert_for_a_legacy_server_decoder() {
        #[derive(serde::Deserialize)]
        struct LegacyRequest {
            #[allow(dead_code)]
            action: FlowControlAction,
        }
        let error = serde_json::from_value::<LegacyRequest>(conditional(&binding()))
            .err()
            .unwrap();
        assert!(error.to_string().contains("missing field `action`"));
    }
}
