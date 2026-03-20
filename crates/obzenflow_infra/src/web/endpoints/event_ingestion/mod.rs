// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP event ingestion endpoints
//!
//! Provides a small ingestion surface for push-based flows:
//! - `POST {base_path}/events` (single event)
//! - `POST {base_path}/batch`  (batch)
//! - `GET  {base_path}/health` (ingestion readiness/backpressure)

mod auth;
mod batch_event;
mod health;
mod shared;
mod single_event;
mod validation;

pub use auth::{authorize_request, AuthConfig, AuthError};
pub use shared::{IngestionConfig, IngestionState};
pub use validation::{
    validate_submission, SchemaValidator, TypedValidator, ValidationConfig, ValidationError,
};

use obzenflow_core::event::ingestion::EventSubmission;
use obzenflow_core::event::ingestion::IngestionTelemetry;
use obzenflow_core::web::HttpEndpoint;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::application::{WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext};

use batch_event::BatchEventEndpoint;
use health::IngestionHealthEndpoint;
use single_event::SingleEventEndpoint;

/// Create ingestion endpoints along with the receiver for wiring into `HttpSource`.
///
/// Returns `(endpoints, rx, state)`:
/// - `endpoints`: register with `start_web_server(..., extra_endpoints, port)`
/// - `rx`: pass into `obzenflow_adapters::sources::http::HttpSource`
/// - `state`: call `state.watch_pipeline_state(flow_handle.state_receiver())` (optional)
pub fn create_ingestion_endpoints(
    config: IngestionConfig,
) -> (
    Vec<Box<dyn HttpEndpoint>>,
    mpsc::Receiver<EventSubmission>,
    IngestionState,
) {
    let (state, rx) = IngestionState::new(config);

    let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
        Box::new(SingleEventEndpoint::new(state.clone())),
        Box::new(BatchEventEndpoint::new(state.clone())),
        Box::new(IngestionHealthEndpoint::new(state.clone())),
    ];

    (endpoints, rx, state)
}

/// Create a managed ingestion surface attachment plus the receiver for wiring into `HttpSource`.
///
/// This returns `(surface, rx, telemetry)`:
/// - `surface`: register with `FlowApplication::builder().with_web_surface(...)`
/// - `rx`: pass into `obzenflow_adapters::sources::http::HttpSource`
/// - `telemetry`: pass into `HttpSource::with_telemetry(...)`
///
/// # Example
/// ```ignore
/// use obzenflow_infra::application::FlowApplication;
/// use obzenflow_infra::web::endpoints::event_ingestion::{create_ingestion_surface, IngestionConfig};
/// use obzenflow_adapters::sources::http::HttpSource;
///
/// let config = IngestionConfig {
///     base_path: "/api/bank/accounts".to_string(),
///     ..Default::default()
/// };
/// let (surface, rx, telemetry) = create_ingestion_surface(config);
/// let accounts_source = HttpSource::new(rx).with_telemetry(telemetry);
///
/// FlowApplication::builder()
///     .with_web_surface(surface)
///     .run_blocking(flow! { /* ... */ })?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub fn create_ingestion_surface(
    config: IngestionConfig,
) -> (
    WebSurfaceAttachment,
    mpsc::Receiver<EventSubmission>,
    Arc<IngestionTelemetry>,
) {
    let (state, rx) = IngestionState::new(config);
    let telemetry = state.telemetry();
    let base_path = state.config.base_path.clone();

    let state_for_wiring = state.clone();
    let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
        Box::new(SingleEventEndpoint::new(state.clone())),
        Box::new(BatchEventEndpoint::new(state.clone())),
        Box::new(IngestionHealthEndpoint::new(state)),
    ];

    let surface = WebSurfaceAttachment::new(format!("ingestion:{base_path}"), endpoints)
        .with_wiring(move |ctx: WebSurfaceWiringContext| {
            let task = state_for_wiring.watch_pipeline_state(ctx.pipeline_state);
            Ok(WebSurfaceWiring::new(vec![task]))
        });

    (surface, rx, telemetry)
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::web::{HttpMethod, Request};
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;

    fn json_request(path: &str, body: serde_json::Value) -> Request {
        Request::new(HttpMethod::Post, path.to_string())
            .with_body(serde_json::to_vec(&body).unwrap())
    }

    fn event_body(event_type: &str) -> serde_json::Value {
        json!({
            "event_type": event_type,
            "data": { "value": 1 }
        })
    }

    #[tokio::test]
    async fn single_event_rejects_when_not_ready() {
        let (state, mut rx) = IngestionState::new(IngestionConfig::default());
        let telemetry = state.telemetry();
        let endpoint = SingleEventEndpoint::new(state.clone());

        let resp = endpoint
            .handle(json_request(endpoint.path(), event_body("order.created")))
            .await
            .unwrap();

        assert_eq!(resp.status, 503);
        assert_eq!(
            resp.headers.get("Retry-After").map(String::as_str),
            Some("1")
        );
        assert!(rx.try_recv().is_err());

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.events_accepted_total, 0);
        assert_eq!(snapshot.events_rejected_not_ready_total, 1);
    }

    #[tokio::test]
    async fn single_event_accepts_when_ready_and_enqueues() {
        let (state, mut rx) = IngestionState::new(IngestionConfig::default());
        let telemetry = state.telemetry();
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = SingleEventEndpoint::new(state.clone());

        let resp = endpoint
            .handle(json_request(endpoint.path(), event_body("order.created")))
            .await
            .unwrap();

        assert_eq!(resp.status, 200);
        let queued = rx.recv().await.expect("queued event");
        assert_eq!(queued.event_type, "order.created");
        assert_eq!(queued.data, json!({ "value": 1 }));

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.events_accepted_total, 1);
    }

    #[tokio::test]
    async fn single_event_returns_503_when_buffer_full() {
        let config = IngestionConfig {
            buffer_capacity: 1,
            ..Default::default()
        };
        let (state, _rx) = IngestionState::new(config);
        let telemetry = state.telemetry();
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = SingleEventEndpoint::new(state.clone());

        state
            .tx
            .try_send(obzenflow_core::event::ingestion::EventSubmission {
                event_type: "filled".to_string(),
                data: json!({"value": 1}),
                metadata: None,
            })
            .unwrap();

        let resp = endpoint
            .handle(json_request(endpoint.path(), event_body("order.created")))
            .await
            .unwrap();

        assert_eq!(resp.status, 503);
        assert_eq!(
            resp.headers.get("Retry-After").map(String::as_str),
            Some("1")
        );

        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.events_accepted_total, 0);
        assert_eq!(snapshot.events_rejected_buffer_full_total, 1);
    }

    #[tokio::test]
    async fn single_event_returns_500_when_channel_closed() {
        let (state, rx) = IngestionState::new(IngestionConfig::default());
        let telemetry = state.telemetry();
        drop(rx);

        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = SingleEventEndpoint::new(state);

        let resp = endpoint
            .handle(json_request(endpoint.path(), event_body("order.created")))
            .await
            .unwrap();

        assert_eq!(resp.status, 500);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.events_rejected_channel_closed_total, 1);
    }

    #[tokio::test]
    async fn batch_rejects_when_buffer_insufficient() {
        let config = IngestionConfig {
            buffer_capacity: 1,
            ..Default::default()
        };
        let (state, _rx) = IngestionState::new(config);
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = BatchEventEndpoint::new(state);

        let body = json!({
            "events": [
                {"event_type":"a","data":{"value": 1}},
                {"event_type":"b","data":{"value": 2}}
            ]
        });

        let resp = endpoint
            .handle(json_request(endpoint.path(), body))
            .await
            .unwrap();
        assert_eq!(resp.status, 503);
        assert_eq!(
            resp.headers.get("Retry-After").map(String::as_str),
            Some("1")
        );
    }

    #[tokio::test]
    async fn health_reports_depth_and_readiness() {
        let config = IngestionConfig {
            buffer_capacity: 2,
            ..Default::default()
        };
        let (state, _rx) = IngestionState::new(config);
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        state
            .tx
            .try_send(obzenflow_core::event::ingestion::EventSubmission {
                event_type: "one".to_string(),
                data: json!({"value": 1}),
                metadata: None,
            })
            .unwrap();

        let endpoint = IngestionHealthEndpoint::new(state);
        let resp = endpoint
            .handle(Request::new(HttpMethod::Get, endpoint.path().to_string()))
            .await
            .unwrap();

        assert_eq!(resp.status, 200);
        let body: serde_json::Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["pipeline_ready"], true);
        assert_eq!(body["channel_capacity"], 2);
        assert_eq!(body["channel_depth"], 1);
    }

    #[tokio::test]
    async fn single_event_requires_api_key_when_configured() {
        let mut keys = HashSet::new();
        keys.insert("secret".to_string());
        let config = IngestionConfig {
            auth: Some(AuthConfig::ApiKey {
                header: "x-api-key".to_string(),
                keys,
            }),
            ..Default::default()
        };
        let (state, _rx) = IngestionState::new(config);
        let endpoint = SingleEventEndpoint::new(state);

        let resp = endpoint
            .handle(json_request(endpoint.path(), event_body("order.created")))
            .await
            .unwrap();
        assert_eq!(resp.status, 401);
    }

    #[tokio::test]
    async fn batch_partial_accepts_validation_errors() {
        use obzenflow_core::event::schema::TypedPayload;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct TestPayload {
            required: String,
        }

        impl TypedPayload for TestPayload {
            const EVENT_TYPE: &'static str = "test.event";
        }

        let config = IngestionConfig {
            validation: Some(ValidationConfig::Single {
                validator: Arc::new(TypedValidator::<TestPayload>::new()),
            }),
            ..Default::default()
        };
        let (state, mut rx) = IngestionState::new(config);
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = BatchEventEndpoint::new(state);

        let body = json!({
            "events": [
                {"event_type":"test.event","data":{"required":"ok"}},
                {"event_type":"test.event","data":{}}
            ]
        });

        let resp = endpoint
            .handle(json_request(endpoint.path(), body))
            .await
            .unwrap();
        assert_eq!(resp.status, 200);
        let response: obzenflow_core::event::ingestion::SubmissionResponse =
            serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(response.accepted, 1);
        assert_eq!(response.rejected, 1);
        assert_eq!(rx.recv().await.unwrap().event_type, "test.event");
    }

    #[tokio::test]
    async fn registry_rejects_unknown_event_type_when_configured() {
        use obzenflow_core::event::schema::TypedPayload;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct Known {
            required: String,
        }

        impl TypedPayload for Known {
            const EVENT_TYPE: &'static str = "known.event";
        }

        let mut validators: HashMap<String, Arc<dyn SchemaValidator>> = HashMap::new();
        validators.insert(
            Known::EVENT_TYPE.to_string(),
            Arc::new(TypedValidator::<Known>::new()),
        );

        let config = IngestionConfig {
            validation: Some(ValidationConfig::Registry {
                validators,
                reject_unknown: true,
            }),
            ..Default::default()
        };

        let (state, _rx) = IngestionState::new(config);
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = SingleEventEndpoint::new(state);

        let resp = endpoint
            .handle(json_request(endpoint.path(), event_body("unknown.event")))
            .await
            .unwrap();
        assert_eq!(resp.status, 400);
    }

    #[tokio::test]
    async fn single_event_accepts_stripe_style_webhook_signature() {
        use ring::hmac;

        let secret = "secret";
        let config = IngestionConfig {
            auth: Some(AuthConfig::WebhookSignature {
                secret: secret.to_string(),
                header: "stripe-signature".to_string(),
                replay_window_secs: None,
            }),
            ..Default::default()
        };

        let (state, mut rx) = IngestionState::new(config);
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = SingleEventEndpoint::new(state);

        let body_json = event_body("order.created");
        let body_bytes = serde_json::to_vec(&body_json).unwrap();

        let ts = "1700000000";
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let mut signed = Vec::with_capacity(ts.len() + 1 + body_bytes.len());
        signed.extend_from_slice(ts.as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(&body_bytes);
        let sig = hmac::sign(&key, &signed);
        let sig_hex = sig
            .as_ref()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();

        let request = Request::new(HttpMethod::Post, endpoint.path().to_string())
            .with_header(
                "Stripe-Signature".to_string(),
                format!("t={ts},v1={sig_hex}"),
            )
            .with_body(body_bytes);

        let resp = endpoint.handle(request).await.unwrap();
        assert_eq!(resp.status, 200);
        let queued = rx.recv().await.expect("queued event");
        assert_eq!(queued.event_type, "order.created");
    }
}
