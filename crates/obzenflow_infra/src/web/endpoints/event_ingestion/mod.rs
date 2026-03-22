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

use obzenflow_adapters::sources::http::{HttpSource, HttpSourceTyped};
use obzenflow_core::event::ingestion::EventSubmission;
use obzenflow_core::event::ingestion::IngestionTelemetry;
use obzenflow_core::web::HttpEndpoint;
use obzenflow_core::TypedPayload;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::application::{WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext};

use batch_event::BatchEventEndpoint;
use health::IngestionHealthEndpoint;
use single_event::SingleEventEndpoint;

/// Framework-owned HTTP ingress bundle for a single typed payload.
pub struct HttpIngress<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    surface: WebSurfaceAttachment,
    source: HttpSourceTyped<T>,
    telemetry: Arc<IngestionTelemetry>,
}

impl<T> HttpIngress<T>
where
    T: TypedPayload + Send + Sync + 'static,
{
    /// Clone the typed source that feeds accepted events into `flow!`.
    pub fn source(&self) -> HttpSourceTyped<T> {
        self.source.clone()
    }

    /// Consume the bundle and return the hosted surface attachment.
    pub fn into_surface(self) -> WebSurfaceAttachment {
        self.surface
    }

    pub(crate) fn into_surface_and_telemetry(
        self,
    ) -> (WebSurfaceAttachment, Arc<IngestionTelemetry>) {
        (self.surface, self.telemetry)
    }
}

/// Create a zero-wiring HTTP ingress bundle for `T`.
///
/// If `config.validation` is not set, the bundle defaults to a single-type validator
/// for `T`.
pub fn http_ingress<T>(mut config: IngestionConfig) -> HttpIngress<T>
where
    T: TypedPayload + Send + Sync + std::fmt::Debug + 'static,
{
    if config.validation.is_none() {
        config.validation = Some(ValidationConfig::Single {
            validator: Arc::new(TypedValidator::<T>::new()),
        });
    }

    let (surface, rx, telemetry) = create_ingestion_surface(config);
    let source = HttpSource::new(rx).typed::<T>();

    HttpIngress {
        surface,
        source,
        telemetry,
    }
}

/// Create ingestion endpoints along with the receiver for wiring into `HttpSource`.
///
/// Returns `(endpoints, rx, state)`:
/// - `endpoints`: register with `start_web_server(..., extra_endpoints, port)`
/// - `rx`: pass into `obzenflow_adapters::sources::http::HttpSource`
/// - `state`: call `state.watch_pipeline_state(flow_handle.state_receiver())` (optional)
#[allow(dead_code)]
pub(crate) fn create_ingestion_endpoints(
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

pub(crate) fn create_ingestion_surface(
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
    use async_trait::async_trait;
    use obzenflow_core::event::chain_event::ChainEvent;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::web::{HttpMethod, ManagedResponse, Request, Response};
    use obzenflow_dsl::{async_infinite_source, flow, sink};
    use obzenflow_runtime::pipeline::{FlowHandle, PipelineState};
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::{
        source::AsyncInfiniteSourceHandler, SinkHandler,
    };
    use obzenflow_runtime::supervised_base::SupervisorHandle;
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};
    use tokio::sync::Notify;

    use crate::journal::disk_journals;

    fn unwrap_unary(resp: ManagedResponse) -> Response {
        match resp {
            ManagedResponse::Unary(resp) => resp,
            ManagedResponse::Sse(_) => panic!("expected unary response"),
        }
    }

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

    #[derive(Clone, Debug)]
    struct NotifyingSink {
        delivered: Arc<AtomicUsize>,
        notify: Arc<Notify>,
    }

    #[async_trait]
    impl SinkHandler for NotifyingSink {
        async fn consume(
            &mut self,
            _event: ChainEvent,
        ) -> std::result::Result<DeliveryPayload, HandlerError> {
            self.delivered.fetch_add(1, Ordering::AcqRel);
            self.notify.notify_waiters();
            Ok(DeliveryPayload::success(
                "ingress_test_sink",
                DeliveryMethod::Custom("Collect".to_string()),
                None,
            ))
        }
    }

    fn unique_journal_dir(prefix: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_else(|_| Duration::from_millis(0))
            .as_nanos();
        PathBuf::from("target").join(format!("{prefix}_{suffix}"))
    }

    async fn wait_for_running(handle: &FlowHandle) {
        let mut rx = handle.state_receiver();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if matches!(*rx.borrow(), PipelineState::Running) {
                    return;
                }
                rx.changed().await.expect("pipeline state channel closed");
            }
        })
        .await
        .expect("timed out waiting for running");
    }

    fn journal_contains_token(root: &Path, token: &[u8]) -> bool {
        let Ok(entries) = fs::read_dir(root) else {
            return false;
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                if journal_contains_token(&path, token) {
                    return true;
                }
                continue;
            }

            let Ok(bytes) = fs::read(&path) else {
                continue;
            };
            if bytes.windows(token.len()).any(|window| window == token) {
                return true;
            }
        }

        false
    }

    #[tokio::test]
    async fn single_event_rejects_when_not_ready() {
        let (state, mut rx) = IngestionState::new(IngestionConfig::default());
        let telemetry = state.telemetry();
        let endpoint = SingleEventEndpoint::new(state.clone());

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), event_body("order.created")))
                .await
                .unwrap(),
        );

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

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), event_body("order.created")))
                .await
                .unwrap(),
        );

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
                ingress_handoff: None,
            })
            .unwrap();

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), event_body("order.created")))
                .await
                .unwrap(),
        );

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

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), event_body("order.created")))
                .await
                .unwrap(),
        );

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

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), body))
                .await
                .unwrap(),
        );
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
                ingress_handoff: None,
            })
            .unwrap();

        let endpoint = IngestionHealthEndpoint::new(state);
        let resp = unwrap_unary(
            endpoint
                .handle(Request::new(HttpMethod::Get, endpoint.path().to_string()))
                .await
                .unwrap(),
        );

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

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), event_body("order.created")))
                .await
                .unwrap(),
        );
        assert_eq!(resp.status, 401);
    }

    #[tokio::test]
    async fn batch_auth_rejection_counts_events_from_request_body() {
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
        let telemetry = state.telemetry();
        let endpoint = BatchEventEndpoint::new(state);

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(
                    endpoint.path(),
                    json!({
                        "events": [
                            {"event_type":"a","data":{"value": 1}},
                            {"event_type":"b","data":{"value": 2}}
                        ]
                    }),
                ))
                .await
                .unwrap(),
        );

        assert_eq!(resp.status, 401);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.events_rejected_auth_total, 2);
    }

    #[tokio::test]
    async fn batch_not_ready_rejection_counts_events_from_request_body() {
        let (state, _rx) = IngestionState::new(IngestionConfig::default());
        let telemetry = state.telemetry();
        let endpoint = BatchEventEndpoint::new(state);

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(
                    endpoint.path(),
                    json!({
                        "events": [
                            {"event_type":"a","data":{"value": 1}},
                            {"event_type":"b","data":{"value": 2}},
                            {"event_type":"c","data":{"value": 3}}
                        ]
                    }),
                ))
                .await
                .unwrap(),
        );

        assert_eq!(resp.status, 503);
        let snapshot = telemetry.snapshot();
        assert_eq!(snapshot.requests_total, 1);
        assert_eq!(snapshot.events_rejected_not_ready_total, 3);
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

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), body))
                .await
                .unwrap(),
        );
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

        let resp = unwrap_unary(
            endpoint
                .handle(json_request(endpoint.path(), event_body("unknown.event")))
                .await
                .unwrap(),
        );
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

        let resp = unwrap_unary(endpoint.handle(request).await.unwrap());
        assert_eq!(resp.status, 200);
        let queued = rx.recv().await.expect("queued event");
        assert_eq!(queued.event_type, "order.created");
    }

    #[tokio::test]
    async fn http_ingress_bundle_wires_surface_and_source() {
        use obzenflow_core::event::ChainEventContent;
        use obzenflow_core::{StageId, WriterId};
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct TestPayload {
            order_id: String,
        }

        impl TypedPayload for TestPayload {
            const EVENT_TYPE: &'static str = "order.created";
        }

        let ingress = http_ingress::<TestPayload>(IngestionConfig::default());
        let mut source = ingress.source();
        let (surface, _telemetry) = ingress.into_surface_and_telemetry();
        let (_name, endpoints, wiring) = surface.into_parts();

        let wiring = wiring.expect("ingress surface wiring");
        let (_tx, pipeline_state) = tokio::sync::watch::channel(PipelineState::Running);
        let wired = wiring(WebSurfaceWiringContext { pipeline_state }).unwrap();
        tokio::task::yield_now().await;

        let events_endpoint = endpoints
            .into_iter()
            .find(|endpoint| endpoint.path().ends_with("/events"))
            .expect("events endpoint");

        let request = Request::new(HttpMethod::Post, events_endpoint.path().to_string()).with_body(
            serde_json::to_vec(&serde_json::json!({
                "event_type": "order.created",
                "data": { "order_id": "1" }
            }))
            .unwrap(),
        );

        let response = unwrap_unary(events_endpoint.handle(request).await.unwrap());
        assert_eq!(response.status, 200);

        source.bind_writer_id(WriterId::from(StageId::new()));
        let out = source.next().await.unwrap();
        assert_eq!(out.len(), 1);
        match &out[0].content {
            ChainEventContent::Data {
                event_type,
                payload,
            } => {
                assert_eq!(event_type, "order.created");
                assert_eq!(payload, &serde_json::json!({ "order_id": "1" }));
            }
            other => panic!("expected data event, got {other:?}"),
        }
        let ingress_context = out[0].ingress_context.as_ref().expect("ingress context");
        assert_eq!(ingress_context.base_path, "/api/ingest");
        assert_eq!(ingress_context.batch_index, None);

        for task in wired.tasks {
            task.abort();
        }
    }

    #[tokio::test]
    async fn http_ingress_pipeline_journal_never_contains_snapshot_event() {
        use obzenflow_core::TypedPayload;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct TestPayload {
            order_id: String,
        }

        impl TypedPayload for TestPayload {
            const EVENT_TYPE: &'static str = "order.created";
        }

        let ingress = http_ingress::<TestPayload>(IngestionConfig {
            base_path: "/api/journal-check".to_string(),
            ..Default::default()
        });
        let source = ingress.source();
        let (surface, _telemetry) = ingress.into_surface_and_telemetry();
        let (_name, endpoints, wiring) = surface.into_parts();
        let wiring = wiring.expect("ingress surface wiring");
        let events_endpoint = endpoints
            .into_iter()
            .find(|endpoint| endpoint.path().ends_with("/events"))
            .expect("events endpoint");

        let delivered = Arc::new(AtomicUsize::new(0));
        let notify = Arc::new(Notify::new());
        let sink = NotifyingSink {
            delivered: delivered.clone(),
            notify: notify.clone(),
        };

        let journal_root = unique_journal_dir("flowip_084h_ingestion_journal_absence");
        let journal_root_for_flow = journal_root.clone();
        let handle = flow! {
            name: "flowip_084h_ingestion_journal_absence",
            journals: disk_journals(journal_root_for_flow),
            middleware: [],

            stages: {
                source = async_infinite_source!(source);
                sink = sink!(sink);
            },

            topology: {
                source |> sink;
            }
        }
        .await
        .expect("build flow");

        let wired = wiring(WebSurfaceWiringContext {
            pipeline_state: handle.state_receiver(),
        })
        .expect("wire ingress surface");

        handle.start().await.expect("start flow");
        wait_for_running(&handle).await;
        tokio::task::yield_now().await;

        let response = unwrap_unary(
            events_endpoint
                .handle(json_request(
                    events_endpoint.path(),
                    json!({
                        "event_type": "order.created",
                        "data": { "order_id": "1" }
                    }),
                ))
                .await
                .expect("endpoint response"),
        );
        assert_eq!(response.status, 200);

        tokio::time::timeout(Duration::from_secs(5), async {
            while delivered.load(Ordering::Acquire) == 0 {
                notify.notified().await;
            }
        })
        .await
        .expect("timed out waiting for delivered event");

        handle.stop_cancel().await.expect("stop flow");
        tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
            .await
            .expect("timed out waiting for flow completion")
            .expect("flow completion");

        for task in wired.tasks {
            task.abort();
        }

        assert!(
            !journal_contains_token(&journal_root, b"http_ingestion.snapshot"),
            "ingestion journals must not contain http_ingestion.snapshot after 084h cutover"
        );
    }
}
