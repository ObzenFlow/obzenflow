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
use obzenflow_core::ingress::EventSubmission;
use obzenflow_core::ingress::IngestionTelemetry;
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_core::web::HttpEndpoint;
use obzenflow_core::TypedPayload;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::application::{
    ApplicationError, WebSurfaceAttachment, WebSurfaceWiring, WebSurfaceWiringContext,
};

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

    let (surface, rx, telemetry, ingress_slot) = create_ingestion_surface(config);
    let source = HttpSource::new(rx)
        .with_ingress_slot(ingress_slot)
        .typed::<T>();

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
    HostedIngressBindingSlot,
) {
    let (state, rx) = IngestionState::new(config);
    let telemetry = state.telemetry();
    let ingress_slot = state.ingress_slot();
    let base_path = state.config.base_path.clone();

    let state_for_wiring = state.clone();
    let endpoints: Vec<Box<dyn HttpEndpoint>> = vec![
        Box::new(SingleEventEndpoint::new(state.clone())),
        Box::new(BatchEventEndpoint::new(state.clone())),
        Box::new(IngestionHealthEndpoint::new(state)),
    ];

    let surface = WebSurfaceAttachment::new(format!("ingestion:{base_path}"), endpoints)
        .with_ingress_slot(ingress_slot.clone())
        .with_wiring(move |ctx: WebSurfaceWiringContext| {
            // FLOWIP-115d: install the refusal-fact writer from the host system
            // journal. Recording is on by default; a surface that wants it but
            // has no system journal fails startup rather than silently dropping
            // replayable refusal evidence.
            if state_for_wiring.refusal_recording_enabled() {
                match ctx.system_journal {
                    Some(journal) => state_for_wiring.install_refusal_writer(journal),
                    None => {
                        return Err(ApplicationError::FlowBuildFailed(format!(
                            "hosted ingress surface '{}' has refusal recording enabled but no \
                             system journal is available; disable record_ingress_refusals or run \
                             with a system journal",
                            state_for_wiring.config.base_path
                        )));
                    }
                }
            }
            let task = state_for_wiring.watch_pipeline_state(ctx.pipeline_state);
            Ok(WebSurfaceWiring::new(vec![task]))
        });

    (surface, rx, telemetry, ingress_slot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::chain_event::ChainEvent;
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::identity::EventId;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::{JournalEvent, SystemEvent, SystemEventType};
    use obzenflow_core::id::JournalId;
    use obzenflow_core::id::SystemId;
    use obzenflow_core::ingress::{
        FilledHostedIngress, IngressAttemptSeq, IngressBoundaryMiddleware, IngressRefusalReason,
    };
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::journal::run_manifest::RUN_MANIFEST_FILENAME;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::journal::JournalStorageKind;
    use obzenflow_core::web::{HttpMethod, ManagedResponse, Request, Response};
    use obzenflow_core::StageId;

    use crate::journal::MemoryJournal;
    use obzenflow_dsl::{async_infinite_source, flow, sink};
    use obzenflow_fsm::FsmAction;
    use obzenflow_runtime::metrics::{
        MetricsAggregatorAction, MetricsAggregatorContext, MetricsStore,
    };
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

    /// FLOWIP-115d: a hosted-ingress boundary that always rejects (as a rate
    /// limiter whose bucket is exhausted would), for endpoint-admission tests.
    struct AlwaysRejectIngress;

    impl obzenflow_core::ingress::IngressBoundaryMiddleware for AlwaysRejectIngress {
        fn label(&self) -> &'static str {
            "always_reject"
        }

        fn on_ingress(
            &self,
            _attempt: &obzenflow_core::ingress::IngressAttemptContext,
        ) -> obzenflow_core::ingress::IngressAdmissionDecision {
            obzenflow_core::ingress::IngressAdmissionDecision::Reject {
                retry_after: Some(Duration::from_secs(2)),
            }
        }

        fn observe(
            &self,
            _attempt: &obzenflow_core::ingress::IngressAttemptContext,
            _outcome: obzenflow_core::ingress::IngressAdmissionOutcome,
        ) {
        }
    }

    /// FLOWIP-115d: the single-event endpoint runs ingress admission after
    /// reserving capacity, and maps a rate-limited rejection to `429` plus
    /// `Retry-After`, recording it as a rate-limited refusal.
    /// FLOWIP-115d: build an ingestion state with an in-memory refusal-fact
    /// journal installed and the binding slot filled, so endpoint refusals append
    /// `IngressRefusal` facts the test can read back. `boundary` is the ingress
    /// admission boundary (None skips boundary admission).
    fn state_with_refusal_journal(
        config: IngestionConfig,
        boundary: Option<Arc<dyn IngressBoundaryMiddleware>>,
    ) -> (
        IngestionState,
        mpsc::Receiver<EventSubmission>,
        Arc<MemoryJournal<SystemEvent>>,
    ) {
        let (state, rx) = IngestionState::new(config);
        state
            .ingress_slot()
            .fill(FilledHostedIngress {
                stage_id: StageId::new(),
                stage_key: "test".to_string(),
                boundary,
            })
            .expect("slot fill succeeds once");
        let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(
            SystemId::new(),
        )));
        state.install_refusal_writer(journal.clone());
        (state, rx, journal)
    }

    #[derive(Clone)]
    struct FailingAppendJournal<T: JournalEvent> {
        inner: MemoryJournal<T>,
    }

    impl<T: JournalEvent> FailingAppendJournal<T> {
        fn with_owner(owner: JournalOwner) -> Self {
            Self {
                inner: MemoryJournal::with_owner(owner),
            }
        }
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> Journal<T> for FailingAppendJournal<T> {
        fn storage_kind(&self) -> JournalStorageKind {
            self.inner.storage_kind()
        }

        fn id(&self) -> &JournalId {
            self.inner.id()
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.inner.owner()
        }

        async fn append(
            &self,
            _event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            Err(JournalError::Implementation {
                message: "forced append failure".to_string(),
                source: "forced append failure".into(),
            })
        }

        async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            self.inner.read_causally_ordered().await
        }

        async fn read_causally_after(
            &self,
            after_event_id: &EventId,
        ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            self.inner.read_causally_after(after_event_id).await
        }

        async fn read_event(
            &self,
            event_id: &EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            self.inner.read_event(event_id).await
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            self.inner.reader().await
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            self.inner.reader_from(position).await
        }

        async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            self.inner.read_last_n(count).await
        }
    }

    fn state_with_failing_refusal_journal(
        config: IngestionConfig,
        boundary: Option<Arc<dyn IngressBoundaryMiddleware>>,
    ) -> (IngestionState, mpsc::Receiver<EventSubmission>) {
        let (state, rx) = IngestionState::new(config);
        state
            .ingress_slot()
            .fill(FilledHostedIngress {
                stage_id: StageId::new(),
                stage_key: "test".to_string(),
                boundary,
            })
            .expect("slot fill succeeds once");
        state.install_refusal_writer(Arc::new(FailingAppendJournal::with_owner(
            JournalOwner::system(SystemId::new()),
        )));
        (state, rx)
    }

    /// Read the `(reason, event_count)` of every `IngressRefusal` fact, in order.
    async fn refusal_facts(
        journal: &MemoryJournal<SystemEvent>,
    ) -> Vec<(IngressRefusalReason, u64)> {
        journal
            .read_causally_ordered()
            .await
            .expect("read system journal")
            .into_iter()
            .filter_map(|env| match env.event.event {
                SystemEventType::IngressRefusal {
                    reason,
                    event_count,
                    ..
                } => Some((reason, event_count)),
                _ => None,
            })
            .collect()
    }

    async fn refusal_fact_details(
        journal: &MemoryJournal<SystemEvent>,
    ) -> Vec<(IngressRefusalReason, u64, IngressAttemptSeq)> {
        journal
            .read_causally_ordered()
            .await
            .expect("read system journal")
            .into_iter()
            .filter_map(|env| match env.event.event {
                SystemEventType::IngressRefusal {
                    reason,
                    event_count,
                    attempt_seq,
                    ..
                } => Some((reason, event_count, attempt_seq)),
                _ => None,
            })
            .collect()
    }

    fn run_manifest_paths(root: &Path) -> Vec<PathBuf> {
        let flows_dir = root.join("flows");
        let Ok(entries) = fs::read_dir(flows_dir) else {
            return Vec::new();
        };
        entries
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path().join(RUN_MANIFEST_FILENAME))
            .filter(|path| path.exists())
            .collect()
    }

    async fn replay_system_journal_through_refusal_aggregator(
        system_journal: Arc<dyn Journal<SystemEvent>>,
    ) -> HashMap<(String, String), u64> {
        let events = system_journal
            .read_causally_ordered()
            .await
            .expect("read archived system journal");
        let mut ctx = MetricsAggregatorContext {
            system_journal,
            stage_data_journals: HashMap::new(),
            stage_error_journals: HashMap::new(),
            backpressure_registry: None,
            include_error_journals: true,
            exporter: None,
            metrics_store: MetricsStore::default(),
            export_interval_secs: 60,
            system_id: SystemId::new(),
            stage_metadata: HashMap::new(),
        };

        for envelope in events {
            MetricsAggregatorAction::ProcessSystemEvent {
                envelope: Box::new(envelope),
            }
            .execute(&mut ctx)
            .await
            .expect("replay archived system event through metrics aggregator");
        }

        ctx.metrics_store.ingestion_refusals_total
    }

    #[tokio::test]
    async fn single_event_ingress_admission_maps_reject_to_429() {
        let config = IngestionConfig {
            base_path: "/api/test".to_string(),
            ..IngestionConfig::default()
        };
        let (state, _rx, journal) =
            state_with_refusal_journal(config, Some(Arc::new(AlwaysRejectIngress)));
        state.ready.store(true, Ordering::Release);

        let endpoint = SingleEventEndpoint::new(state.clone());
        let request = json_request("/api/test/events", event_body("order.created"));
        let response = unwrap_unary(endpoint.handle(request).await.unwrap());

        assert_eq!(response.status, 429, "ingress rejection maps to 429");
        // The refusal is a durable fact, not an in-memory counter.
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::RateLimited, 1)],
            "a rate-limited rejection appends one RateLimited refusal fact"
        );
    }

    #[tokio::test]
    async fn single_event_refusal_append_failure_returns_listener_unavailable() {
        let config = IngestionConfig {
            base_path: "/api/test".to_string(),
            ..IngestionConfig::default()
        };
        let (state, mut rx) =
            state_with_failing_refusal_journal(config, Some(Arc::new(AlwaysRejectIngress)));
        state.ready.store(true, Ordering::Release);

        let endpoint = SingleEventEndpoint::new(state);
        let response = unwrap_unary(
            endpoint
                .handle(json_request(
                    "/api/test/events",
                    event_body("order.created"),
                ))
                .await
                .unwrap(),
        );

        assert_eq!(
            response.status, 503,
            "evidence failure fails closed instead of returning a clean 429"
        );
        assert_eq!(
            response.headers.get("Retry-After").map(String::as_str),
            Some("1")
        );
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"], "listener unavailable");
        assert!(rx.try_recv().is_err());
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
        let (state, mut rx, journal) = state_with_refusal_journal(IngestionConfig::default(), None);
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
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::NotReady, 1)]
        );
    }

    #[tokio::test]
    async fn single_event_accepts_when_ready_and_enqueues() {
        let (state, mut rx) = IngestionState::new(IngestionConfig::default());
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
        // FLOWIP-115d: the accepted submission carries its attempt sequence, the
        // cross-journal merge key with refusal facts.
        assert!(queued.ingress_handoff.is_some());
    }

    #[tokio::test]
    async fn single_event_returns_503_when_buffer_full() {
        let config = IngestionConfig {
            buffer_capacity: 1,
            ..Default::default()
        };
        let (state, _rx, journal) = state_with_refusal_journal(config, None);
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = SingleEventEndpoint::new(state.clone());

        // Fill the single channel slot so the endpoint's reserve fails.
        state
            .tx
            .try_send(EventSubmission {
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
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::BufferFull, 1)]
        );
    }

    #[tokio::test]
    async fn single_event_returns_500_when_channel_closed() {
        let (state, rx, journal) = state_with_refusal_journal(IngestionConfig::default(), None);
        drop(rx);

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

        assert_eq!(resp.status, 500);
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::ChannelClosed, 1)]
        );
    }

    #[tokio::test]
    async fn batch_rejects_when_buffer_insufficient() {
        let config = IngestionConfig {
            buffer_capacity: 1,
            ..Default::default()
        };
        let (state, _rx, journal) = state_with_refusal_journal(config, None);
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
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::BufferFull, 2)]
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
            .try_send(obzenflow_core::ingress::EventSubmission {
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
    async fn batch_auth_rejection_returns_401() {
        // FLOWIP-115d: auth is a protocol 4xx reject (Class B), not journalled; it
        // always returns a 401 the HTTP-surface metrics record.
        let mut keys = HashSet::new();
        keys.insert("secret".to_string());
        let config = IngestionConfig {
            auth: Some(AuthConfig::ApiKey {
                header: "x-api-key".to_string(),
                keys,
            }),
            ..Default::default()
        };
        let (state, _rx, journal) = state_with_refusal_journal(config, None);
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
        assert!(
            refusal_facts(&journal).await.is_empty(),
            "auth rejections are not journalled refusal facts"
        );
    }

    #[tokio::test]
    async fn batch_not_ready_rejection_records_event_count() {
        let (state, _rx, journal) = state_with_refusal_journal(IngestionConfig::default(), None);
        let endpoint = BatchEventEndpoint::new(state.clone());

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
        // Not-ready sheds the whole batch; the single fact carries the event count.
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::NotReady, 3)]
        );
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
        let (state, mut rx, journal) = state_with_refusal_journal(config, None);
        state
            .ready
            .store(true, std::sync::atomic::Ordering::Release);
        let endpoint = BatchEventEndpoint::new(state.clone());

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
        let response: obzenflow_core::ingress::SubmissionResponse =
            serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(response.accepted, 1);
        assert_eq!(response.rejected, 1);
        assert_eq!(rx.recv().await.unwrap().event_type, "test.event");
        // FLOWIP-115d: per-event validation rejection is journalled even though the
        // partial batch returns 200 (so it is invisible to the 4xx surface metric).
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::Validation, 1)]
        );
    }

    #[tokio::test]
    async fn batch_validation_plus_rate_limit_records_two_facts_for_one_attempt() {
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
        let (state, mut rx, journal) =
            state_with_refusal_journal(config, Some(Arc::new(AlwaysRejectIngress)));
        state.ready.store(true, Ordering::Release);
        let endpoint = BatchEventEndpoint::new(state);

        let response = unwrap_unary(
            endpoint
                .handle(json_request(
                    endpoint.path(),
                    json!({
                        "events": [
                            {"event_type":"test.event","data":{"required":"ok-1"}},
                            {"event_type":"test.event","data":{}},
                            {"event_type":"test.event","data":{"required":"ok-2"}}
                        ]
                    }),
                ))
                .await
                .unwrap(),
        );

        assert_eq!(response.status, 429);
        let response_body: obzenflow_core::ingress::SubmissionResponse =
            serde_json::from_slice(&response.body).unwrap();
        assert_eq!(response_body.accepted, 0);
        assert_eq!(response_body.rejected, 3);
        assert!(
            response_body
                .errors
                .iter()
                .any(|error| error.contains("rate limited")),
            "response should include the admission refusal alongside validation errors"
        );
        assert!(rx.try_recv().is_err());

        let facts = refusal_fact_details(&journal).await;
        assert_eq!(facts.len(), 2);
        assert_eq!(facts[0].0, IngressRefusalReason::Validation);
        assert_eq!(facts[0].1, 1);
        assert_eq!(facts[1].0, IngressRefusalReason::RateLimited);
        assert_eq!(facts[1].1, 2);
        assert_eq!(
            facts[0].2, facts[1].2,
            "mixed batch facts share the external submission attempt sequence"
        );
    }

    #[tokio::test]
    async fn batch_validation_refusal_append_failure_returns_listener_unavailable() {
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
        let (state, mut rx) = state_with_failing_refusal_journal(config, None);
        state.ready.store(true, Ordering::Release);
        let endpoint = BatchEventEndpoint::new(state);

        let response = unwrap_unary(
            endpoint
                .handle(json_request(
                    endpoint.path(),
                    json!({
                        "events": [
                            {"event_type":"test.event","data":{"required":"ok"}},
                            {"event_type":"test.event","data":{}}
                        ]
                    }),
                ))
                .await
                .unwrap(),
        );

        assert_eq!(
            response.status, 503,
            "missing validation-refusal evidence must not be hidden behind a partial 200"
        );
        let body: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
        assert_eq!(body["error"], "listener unavailable");
        assert!(rx.try_recv().is_err());
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

        let (state, _rx, journal) = state_with_refusal_journal(config, None);
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
        assert_eq!(
            refusal_facts(&journal).await,
            vec![(IngressRefusalReason::Validation, 1)]
        );
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

        // This test wires the surface/source, not refusal recording, so it runs
        // without a system journal; disable recording to avoid the startup check.
        let ingress = http_ingress::<TestPayload>(IngestionConfig {
            record_ingress_refusals: false,
            ..IngestionConfig::default()
        });
        let mut source = ingress.source();
        let (surface, _telemetry) = ingress.into_surface_and_telemetry();
        let (_name, endpoints, wiring, _ingress_slot) = surface.into_parts();

        let wiring = wiring.expect("ingress surface wiring");
        let (_tx, pipeline_state) = tokio::sync::watch::channel(PipelineState::Running);
        let wired = wiring(WebSurfaceWiringContext {
            pipeline_state,
            system_journal: None,
        })
        .unwrap();
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
            record_ingress_refusals: false,
            ..Default::default()
        });
        let source = ingress.source();
        let (surface, _telemetry) = ingress.into_surface_and_telemetry();
        let (_name, endpoints, wiring, _ingress_slot) = surface.into_parts();
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
                source = async_infinite_source!(TestPayload => source);
                sink = sink!(TestPayload => sink);
            },

            topology: {
                source |> sink;
            }
        }
        .await
        .expect("build flow");

        let wired = wiring(WebSurfaceWiringContext {
            pipeline_state: handle.state_receiver(),
            system_journal: None,
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

    /// FLOWIP-115d Phase 7: end-to-end composition proof. A rate limiter attached
    /// to a hosted-ingress source binds to the `Ingress` surface (AC42), the
    /// `FlowApplication` wiring installs it from the real system journal, and a
    /// rate-limited POST appends a durable `IngressRefusal` fact that the metrics
    /// aggregator would project. This exercises the whole vertical in a running
    /// flow, not just the unit seams.
    #[tokio::test]
    async fn http_ingress_rate_limiter_writes_refusal_fact_through_running_flow() {
        use obzenflow_adapters::middleware::rate_limit_with_burst;
        use obzenflow_core::TypedPayload;
        use serde::{Deserialize, Serialize};

        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct OrderPayload {
            order_id: String,
        }
        impl TypedPayload for OrderPayload {
            const EVENT_TYPE: &'static str = "order.created";
        }

        let ingress = http_ingress::<OrderPayload>(IngestionConfig {
            base_path: "/api/orders".to_string(),
            ..Default::default()
        });
        let source = ingress.source();
        let (surface, _telemetry) = ingress.into_surface_and_telemetry();
        let (_name, endpoints, wiring, _slot) = surface.into_parts();
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

        let journal_root = unique_journal_dir("flowip_115d_ingress_refusal_fact");
        let journal_root_for_flow = journal_root.clone();
        let handle = flow! {
            name: "flowip_115d_ingress_refusal_fact",
            journals: disk_journals(journal_root_for_flow),
            middleware: [],

            stages: {
                // Capacity 1 with a negligible refill: the first POST consumes the
                // single burst token, the second is fail-fast rate-limited. The
                // limiter routes to Ingress (AC42), not the internal source poll.
                source = async_infinite_source!(OrderPayload => source, [
                    rate_limit_with_burst(0.001, 1.0)
                ]);
                sink = sink!(OrderPayload => sink);
            },

            topology: {
                source |> sink;
            }
        }
        .await
        .expect("build flow");

        let system_journal = handle.system_journal().expect("flow has a system journal");
        let wired = wiring(WebSurfaceWiringContext {
            pipeline_state: handle.state_receiver(),
            system_journal: Some(system_journal.clone()),
        })
        .expect("wire ingress surface");

        handle.start().await.expect("start flow");
        wait_for_running(&handle).await;
        tokio::task::yield_now().await;

        let order_body = |id: &str| {
            json!({
                "event_type": "order.created",
                "data": { "order_id": id }
            })
        };

        // First POST consumes the burst token and is admitted.
        let first = unwrap_unary(
            events_endpoint
                .handle(json_request(events_endpoint.path(), order_body("1")))
                .await
                .expect("first response"),
        );
        assert_eq!(first.status, 200, "first POST is admitted");

        // Wait for the accepted event to drain to the sink.
        tokio::time::timeout(Duration::from_secs(5), async {
            while delivered.load(Ordering::Acquire) == 0 {
                notify.notified().await;
            }
        })
        .await
        .expect("timed out waiting for delivered event");

        // Second POST finds the bucket empty and is rate-limited fail-fast.
        let second = unwrap_unary(
            events_endpoint
                .handle(json_request(events_endpoint.path(), order_body("2")))
                .await
                .expect("second response"),
        );
        assert_eq!(second.status, 429, "second POST is rate-limited");

        handle.stop_cancel().await.expect("stop flow");
        tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
            .await
            .expect("timed out waiting for flow completion")
            .expect("flow completion");
        for task in wired.tasks {
            task.abort();
        }

        // The rate-limited refusal is a durable fact on the real system journal.
        let refusals: Vec<_> = system_journal
            .read_causally_ordered()
            .await
            .expect("read system journal")
            .into_iter()
            .filter_map(|env| match env.event.event {
                SystemEventType::IngressRefusal {
                    reason,
                    event_count,
                    base_path,
                    ..
                } => Some((reason, event_count, base_path)),
                _ => None,
            })
            .collect();

        assert_eq!(
            refusals,
            vec![(
                IngressRefusalReason::RateLimited,
                1,
                "/api/orders".to_string()
            )],
            "exactly one rate-limited refusal fact for the second POST"
        );
        // Exactly one event was admitted, so the sink delivered exactly one.
        assert_eq!(delivered.load(Ordering::Acquire), 1);

        let manifests = run_manifest_paths(&journal_root);
        assert_eq!(
            manifests.len(),
            1,
            "disk-backed live run writes one replay archive manifest"
        );
        let replayed_refusals =
            replay_system_journal_through_refusal_aggregator(system_journal.clone()).await;
        assert_eq!(
            replayed_refusals.get(&("/api/orders".to_string(), "rate_limited".to_string())),
            Some(&1),
            "strict replay over the archived system facts projects the same refusal total"
        );
    }
}
