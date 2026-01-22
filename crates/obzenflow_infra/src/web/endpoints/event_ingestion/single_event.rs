use super::shared::join_path;
use super::IngestionState;
use super::{authorize_request, validate_submission};
use async_trait::async_trait;
use obzenflow_core::event::ingestion::{
    EventSubmission, IngestionRejectionReason, SubmissionResponse,
};
use obzenflow_core::web::{HttpEndpoint, HttpMethod, Request, Response, WebError};
use serde_json::json;
use tokio::sync::mpsc::error::TrySendError;

pub struct SingleEventEndpoint {
    state: IngestionState,
    path: String,
}

impl SingleEventEndpoint {
    pub fn new(state: IngestionState) -> Self {
        let path = join_path(&state.config.base_path, "events");
        Self { state, path }
    }
}

#[async_trait]
impl HttpEndpoint for SingleEventEndpoint {
    fn path(&self) -> &str {
        &self.path
    }

    fn methods(&self) -> &[HttpMethod] {
        &[HttpMethod::Post]
    }

    async fn handle(&self, request: Request) -> Result<Response, WebError> {
        self.state.telemetry.observe_request();

        if request.body.len() > self.state.config.max_body_size {
            let response = Response::new(413)
                .with_json(&json!({"error": "payload too large"}))
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            self.state
                .telemetry
                .observe_rejected(IngestionRejectionReason::PayloadTooLarge, 1);
            return Ok(response);
        }

        if let Some(ref auth) = self.state.config.auth {
            if let Err(e) = authorize_request(auth, &request) {
                let response = Response::new(401)
                    .with_json(&json!({"error": e.to_string()}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                self.state
                    .telemetry
                    .observe_rejected(IngestionRejectionReason::Auth, 1);
                return Ok(response);
            }
        }

        if !self.state.is_ready() {
            let response = Response::new(503)
                .with_header("Retry-After".to_string(), "1".to_string())
                .with_json(&json!({"error": "not ready"}))
                .map_err(|e| WebError::RequestHandlingFailed {
                    message: e.to_string(),
                    source: None,
                })?;
            self.state
                .telemetry
                .observe_rejected(IngestionRejectionReason::NotReady, 1);
            return Ok(response);
        }

        let submission: EventSubmission = match serde_json::from_slice(&request.body) {
            Ok(s) => s,
            Err(e) => {
                let response = Response::new(400)
                    .with_json(&json!({"error": format!("invalid request body: {e}")}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                self.state
                    .telemetry
                    .observe_rejected(IngestionRejectionReason::InvalidJson, 1);
                return Ok(response);
            }
        };

        if let Some(ref validation) = self.state.config.validation {
            if let Err(e) = validate_submission(&submission, validation) {
                let response = Response::new(400)
                    .with_json(&json!({"error": e.to_message()}))
                    .map_err(|err| WebError::RequestHandlingFailed {
                        message: err.to_string(),
                        source: None,
                    })?;
                self.state
                    .telemetry
                    .observe_rejected(IngestionRejectionReason::Validation, 1);
                return Ok(response);
            }
        }

        match self.state.tx.try_send(submission) {
            Ok(()) => {
                let response = Response::ok()
                    .with_json(&SubmissionResponse {
                        accepted: 1,
                        rejected: 0,
                        errors: Vec::new(),
                    })
                    .map_err(|e| WebError::RequestHandlingFailed {
                        message: e.to_string(),
                        source: None,
                    })?;
                self.state.telemetry.observe_accepted(1);
                Ok(response)
            }
            Err(TrySendError::Full(_)) => {
                let response = Response::new(503)
                    .with_header("Retry-After".to_string(), "1".to_string())
                    .with_json(&json!({"error": "buffer full"}))
                    .map_err(|e| WebError::RequestHandlingFailed {
                        message: e.to_string(),
                        source: None,
                    })?;
                self.state
                    .telemetry
                    .observe_rejected(IngestionRejectionReason::BufferFull, 1);
                Ok(response)
            }
            Err(TrySendError::Closed(_)) => {
                let response = Response::internal_error()
                    .with_json(&json!({"error": "ingestion channel closed"}))
                    .map_err(|e| WebError::RequestHandlingFailed {
                        message: e.to_string(),
                        source: None,
                    })?;
                self.state
                    .telemetry
                    .observe_rejected(IngestionRejectionReason::ChannelClosed, 1);
                Ok(response)
            }
        }
    }
}
