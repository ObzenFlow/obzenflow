// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-084l: the durable admission contract at the hosted edge and through
//! a running flow.

use super::*;
use crate::journal::{disk_journals, memory_journals, MemoryJournal};
use async_trait::async_trait;
use obzenflow_core::event::payloads::delivery_payload::DeliveryMethod;
use obzenflow_core::event::{JournalEvent, SourceDiagnosticReason, SystemEvent, SystemPayload};
use obzenflow_core::id::{JournalId, SystemId};
use obzenflow_core::ingress::{
    FilledHostedIngress, IngressAdmissionDecision, IngressAdmissionOutcome,
    IngressBoundaryMiddleware, IngressRefusalReason, IngressTiming,
};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::web::{HttpMethod, ManagedResponse, Request, Response};
use obzenflow_core::{ChainEvent, EventId, JournalRecord, StageId};
use obzenflow_dsl::{async_infinite_source, flow, sink, FlowDefinition};
use obzenflow_runtime::pipeline::ingress::IngressEvidenceWriter;
use obzenflow_runtime::pipeline::PipelineState;
use obzenflow_runtime::stages::common::handlers::{InlineSink, SinkDescription, SinkWriteFailure};
use obzenflow_runtime::stages::HostedSubmission;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, SystemTime};
use tokio::sync::mpsc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Order {
    order_id: String,
    #[serde(default)]
    amount_cents: u64,
}

impl TypedPayload for Order {
    const EVENT_TYPE: &'static str = "order.created";
}

#[derive(Debug)]
struct TestIngress<T>(PhantomData<fn() -> T>);

impl<T> Clone for TestIngress<T> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<T: TypedPayload + Send + Sync + 'static> IngressDecoder for TestIngress<T> {
    type Output = T;
}

fn decoder<T>() -> TestIngress<T> {
    TestIngress(PhantomData)
}

fn unary(response: ManagedResponse) -> Response {
    match response {
        ManagedResponse::Unary(response) => response,
        ManagedResponse::Sse(_) => panic!("expected unary response"),
    }
}

fn body(response: &Response) -> Value {
    serde_json::from_slice(&response.body).expect("JSON response body")
}

fn order(id: &str, amount_cents: u64) -> Value {
    json!({"event_type": "order.created", "data": {"order_id": id, "amount_cents": amount_cents}})
}

fn post(path: &str, payload: Value, key: Option<&str>) -> Request {
    let request = Request::new(HttpMethod::Post, path.to_string())
        .with_body(serde_json::to_vec(&payload).unwrap());
    match key {
        Some(key) => request.with_header("Idempotency-Key".to_string(), format!("\"{key}\"")),
        None => request,
    }
}

/// The readiness watcher keeps the last state after its sender is gone.
async fn mark_running<T>(state: &IngestionState<T>) {
    let (_, running) = tokio::sync::watch::channel(PipelineState::Running);
    state.watch_pipeline_state(running);
    tokio::task::yield_now().await;
}

fn evidence_journal() -> Arc<MemoryJournal<SystemEvent>> {
    Arc::new(MemoryJournal::with_owner(JournalOwner::system(
        SystemId::new(),
    )))
}

async fn ingress_facts(journal: &dyn Journal<SystemEvent>) -> Vec<SystemPayload> {
    journal
        .read_causally_ordered()
        .await
        .expect("read system journal")
        .into_iter()
        .map(|record| record.into_parts().1)
        .filter(|payload| {
            matches!(
                payload,
                SystemPayload::IngressRefusal { .. }
                    | SystemPayload::IngressDuplicateRecognized { .. }
            )
        })
        .collect()
}

fn refusal_reasons(facts: &[SystemPayload]) -> Vec<(IngressRefusalReason, u64)> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            SystemPayload::IngressRefusal {
                reason,
                event_count,
                ..
            } => Some((*reason, *event_count)),
            _ => None,
        })
        .collect()
}

/// A hosted edge with no running source: admission and refusals only.
struct Edge {
    state: IngestionState<Order>,
    rx: Option<mpsc::Receiver<HostedSubmission<Order>>>,
    journal: Arc<MemoryJournal<SystemEvent>>,
}

impl Edge {
    fn new(config: IngestionConfig, boundary: Option<Arc<dyn IngressBoundaryMiddleware>>) -> Self {
        let (state, rx) = IngestionState::new(config);
        state
            .ingress_slot()
            .fill(FilledHostedIngress {
                stage_id: StageId::new(),
                stage_key: "orders".into(),
                boundary,
            })
            .expect("slot fill succeeds once");
        let journal = evidence_journal();
        state.install_evidence_writer(IngressEvidenceWriter::for_test(journal.clone()));
        Self {
            state,
            rx: Some(rx),
            journal,
        }
    }

    fn plain() -> Self {
        Self::new(IngestionConfig::default(), None)
    }

    /// Mark the pipeline running through the real readiness watcher.
    async fn ready(&self) {
        mark_running(&self.state).await;
    }

    fn single(&self) -> SingleEventEndpoint<TestIngress<Order>> {
        SingleEventEndpoint::new(self.state.clone(), decoder())
    }

    fn batch(&self) -> BatchEventEndpoint<TestIngress<Order>> {
        BatchEventEndpoint::new(self.state.clone(), decoder())
    }

    async fn facts(&self) -> Vec<SystemPayload> {
        ingress_facts(self.journal.as_ref()).await
    }
}

struct AlwaysReject;

impl IngressBoundaryMiddleware for AlwaysReject {
    fn label(&self) -> &'static str {
        "always_reject"
    }

    fn on_ingress(&self, _attempt: &IngressAttemptContext) -> IngressAdmissionDecision {
        IngressAdmissionDecision::Reject {
            retry_after: Some(Duration::from_secs(2)),
        }
    }

    fn observe(&self, _attempt: &IngressAttemptContext, _outcome: IngressAdmissionOutcome) {}
}

#[tokio::test]
async fn request_level_refusals_use_the_error_envelope() {
    let edge = Edge::new(
        IngestionConfig {
            max_body_size: 16,
            ..IngestionConfig::default()
        },
        None,
    );
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("1", 1), None))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 413);
    assert_eq!(
        body(&response),
        json!({"error": "request body exceeds 16 bytes", "code": "payload_too_large", "retryable": false})
    );

    let edge = Edge::plain();
    edge.ready().await;
    let invalid_key = post("/api/ingest/events", order("1", 1), None)
        .with_header("Idempotency-Key".to_string(), "tx 1".to_string());
    let response = unary(edge.single().handle(invalid_key).await.unwrap());
    assert_eq!(response.status, 400);
    assert_eq!(body(&response)["code"], "invalid_idempotency_key");

    let response = unary(
        edge.single()
            .handle(
                Request::new(HttpMethod::Post, "/api/ingest/events".to_string())
                    .with_body(b"{".to_vec()),
            )
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 400);
    assert_eq!(body(&response)["code"], "invalid_body");

    let oversized = json!({"events": vec![order("1", 1); 3]});
    let edge = Edge::new(
        IngestionConfig {
            max_batch_size: 2,
            ..IngestionConfig::default()
        },
        None,
    );
    edge.ready().await;
    let response = unary(
        edge.batch()
            .handle(post("/api/ingest/batch", oversized, None))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 413);
    assert_eq!(body(&response)["error"], "batch size 3 exceeds maximum 2");
    assert!(
        edge.facts().await.is_empty(),
        "protocol rejects are not journalled"
    );
}

#[tokio::test]
async fn unauthorized_requests_are_refused_before_admission() {
    let edge = Edge::new(
        IngestionConfig {
            auth: Some(AuthConfig::ApiKey {
                header: "x-api-key".to_string(),
                keys: ["secret".to_string()].into_iter().collect(),
            }),
            ..IngestionConfig::default()
        },
        None,
    );
    edge.ready().await;
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("1", 1), None))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 401);
    assert_eq!(body(&response)["code"], "unauthorized");
    assert_eq!(body(&response)["retryable"], false);
}

#[tokio::test]
async fn not_ready_refusals_carry_the_request_id() {
    let edge = Edge::plain();
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("1", 1), Some("tx-1")))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 503);
    assert_eq!(
        response.headers.get("Retry-After").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        body(&response),
        json!({"error": "ingress is not ready; retry once the pipeline is live", "code": "not_ready", "retryable": true})
    );
    let facts = edge.facts().await;
    assert_eq!(
        refusal_reasons(&facts),
        vec![(IngressRefusalReason::NotReady, 1)]
    );
    assert!(matches!(
        &facts[0],
        SystemPayload::IngressRefusal { request_id: Some(id), .. } if id.as_str() == "tx-1"
    ));
}

#[tokio::test]
async fn ingress_stays_unavailable_until_the_source_is_live() {
    let edge = Edge::plain();
    edge.ready().await;
    edge.state.ingress_slot().hold_until_live();
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("1", 1), None))
            .await
            .unwrap(),
    );
    assert_eq!(body(&response)["code"], "not_ready");

    edge.state.ingress_slot().mark_resume_live();
    let health = IngestionHealthEndpoint::new(edge.state.clone());
    let response = unary(
        health
            .handle(Request::new(HttpMethod::Get, "/api/ingest/health".into()))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 200);
    let health = body(&response);
    assert_eq!(health["pipeline_ready"], true);
    assert_eq!(health["idempotency_entries"], 0);
    assert_eq!(health["idempotency_window_secs"], 86_400);
}

#[tokio::test]
async fn rate_limited_requests_answer_429_with_one_fact() {
    let edge = Edge::new(IngestionConfig::default(), Some(Arc::new(AlwaysReject)));
    edge.ready().await;
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("1", 1), Some("tx-1")))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 429);
    assert_eq!(
        response.headers.get("Retry-After").map(String::as_str),
        Some("2")
    );
    assert_eq!(body(&response)["code"], "rate_limited");
    assert_eq!(
        refusal_reasons(&edge.facts().await),
        vec![(IngressRefusalReason::RateLimited, 1)]
    );
}

#[tokio::test]
async fn a_closed_queue_answers_shutting_down() {
    let mut edge = Edge::plain();
    edge.ready().await;
    drop(edge.rx.take());
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("1", 1), None))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 503);
    assert_eq!(body(&response)["code"], "shutting_down");
    assert_eq!(
        refusal_reasons(&edge.facts().await),
        vec![(IngressRefusalReason::ChannelClosed, 1)]
    );
}

#[tokio::test]
async fn a_full_queue_answers_buffer_full() {
    let edge = Edge::new(
        IngestionConfig {
            buffer_capacity: 1,
            ..IngestionConfig::default()
        },
        None,
    );
    edge.ready().await;
    let waiting = {
        let endpoint = edge.single();
        tokio::spawn(async move {
            endpoint
                .handle(post("/api/ingest/events", order("1", 1), None))
                .await
        })
    };
    while edge.state.channel_depth() == 0 {
        tokio::task::yield_now().await;
    }
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("2", 1), None))
            .await
            .unwrap(),
    );
    waiting.abort();
    assert_eq!(response.status, 503);
    assert_eq!(body(&response)["code"], "buffer_full");
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Tagged {
    name: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tags: Vec<String>,
}

impl TypedPayload for Tagged {
    const EVENT_TYPE: &'static str = "order.created";
}

#[tokio::test]
async fn admission_refuses_each_bad_member_with_one_fact() {
    let edge = Edge::plain();
    edge.ready().await;
    let batch = json!({"events": [
        {"event_type": "order.created", "data": {"amount_cents": 1}},
        {"event_type": "order.other", "data": {"order_id": "2"}},
    ]});
    let response = unary(
        edge.batch()
            .handle(post("/api/ingest/batch", batch, Some("batch-1")))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 400);
    assert_eq!(
        body(&response),
        json!({
            "committed": 0, "duplicates": 0, "rejected": 2, "unknown": 0,
            "results": [
                {"index": 0, "outcome": "rejected", "code": "validation_failed",
                 "error": "missing field `order_id`"},
                {"index": 1, "outcome": "rejected", "code": "validation_failed",
                 "error": "expected event_type 'order.created', got 'order.other'"}
            ]
        })
    );
    let facts = edge.facts().await;
    assert_eq!(
        refusal_reasons(&facts),
        vec![(IngressRefusalReason::Validation, 2)]
    );
    let SystemPayload::IngressRefusal { diagnostics, .. } = &facts[0] else {
        panic!("validation refusal fact");
    };
    assert_eq!(
        diagnostics
            .iter()
            .map(|diagnostic| diagnostic.location().record_index())
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1)]
    );
}

#[tokio::test]
async fn a_custom_validator_adds_to_the_decoder_and_never_replaces_it() {
    #[derive(Debug)]
    struct AlwaysValid;

    impl SchemaValidator for AlwaysValid {
        fn event_type(&self) -> obzenflow_core::EventType {
            "order.created".into()
        }

        fn validate(
            &self,
            _payload: &Value,
        ) -> Result<(), obzenflow_core::event::SourceDiagnostic> {
            Ok(())
        }
    }

    let edge = Edge::new(
        IngestionConfig {
            validation: Some(ValidationConfig::Single {
                validator: Arc::new(AlwaysValid),
            }),
            ..IngestionConfig::default()
        },
        None,
    );
    edge.ready().await;
    let response = unary(
        edge.single()
            .handle(post(
                "/api/ingest/events",
                json!({"event_type": "order.created", "data": {}}),
                None,
            ))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 400);
    assert_eq!(body(&response)["results"][0]["code"], "validation_failed");
}

#[tokio::test]
async fn unrepresentable_values_are_refused_at_the_failing_field() {
    let (state, _) = IngestionState::<Tagged>::new(IngestionConfig::default());
    state
        .ingress_slot()
        .fill(FilledHostedIngress {
            stage_id: StageId::new(),
            stage_key: "tagged".into(),
            boundary: None,
        })
        .expect("slot fill succeeds once");
    let journal = evidence_journal();
    state.install_evidence_writer(IngressEvidenceWriter::for_test(journal.clone()));
    mark_running(&state).await;

    let endpoint = SingleEventEndpoint::new(state, decoder::<Tagged>());
    let response = unary(
        endpoint
            .handle(post(
                "/api/ingest/events",
                json!({"event_type": "order.created", "data": {"name": "a", "tags": []}}),
                None,
            ))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 400);
    assert_eq!(
        body(&response)["results"][0]["error"],
        "the decoded value cannot be recorded faithfully at `tags`"
    );
    let facts = ingress_facts(journal.as_ref()).await;
    let SystemPayload::IngressRefusal { diagnostics, .. } = &facts[0] else {
        panic!("validation refusal fact");
    };
    assert_eq!(
        diagnostics[0].reason(),
        SourceDiagnosticReason::UnrepresentableValue
    );
}

#[tokio::test]
async fn an_unanswered_member_is_unknown_never_success() {
    let mut edge = Edge::plain();
    edge.ready().await;
    let mut rx = edge.rx.take().expect("source queue");
    let keyed = {
        let endpoint = edge.single();
        tokio::spawn(async move {
            endpoint
                .handle(post("/api/ingest/events", order("1", 1), Some("tx-1")))
                .await
        })
    };
    let keyless = {
        let endpoint = edge.single();
        tokio::spawn(async move {
            endpoint
                .handle(post("/api/ingest/events", order("2", 1), None))
                .await
        })
    };
    // Both members reach the queue; dropping it drops their receipts.
    for _ in 0..2 {
        drop(rx.recv().await.expect("queued member"));
    }
    let keyed = unary(keyed.await.unwrap().unwrap());
    let keyless = unary(keyless.await.unwrap().unwrap());
    for response in [&keyed, &keyless] {
        assert_eq!(response.status, 503);
        assert_eq!(
            response.headers.get("Retry-After").map(String::as_str),
            Some("1")
        );
        assert_eq!(body(response)["unknown"], 1);
    }
    assert_eq!(
        body(&keyed)["results"][0]["error"],
        "commit outcome unknown; retry with the same Idempotency-Key"
    );
    assert_eq!(
        body(&keyless)["results"][0]["error"],
        "commit outcome unknown; without an Idempotency-Key a retry may be admitted twice"
    );
}

#[tokio::test]
async fn the_acknowledgement_deadline_answers_unknown() {
    let edge = Edge::plain();
    edge.state
        .ingress_slot()
        .install_timing(
            IngressTiming::new(Duration::from_secs(1), Duration::from_secs(5)).expect("timing"),
        )
        .expect("first install");
    edge.ready().await;
    let started = tokio::time::Instant::now();
    let response = unary(
        edge.single()
            .handle(post("/api/ingest/events", order("1", 1), Some("tx-1")))
            .await
            .unwrap(),
    );
    assert!(started.elapsed() >= Duration::from_secs(1));
    assert_eq!(response.status, 503);
    assert_eq!(body(&response)["results"][0]["code"], "outcome_unknown");
}

struct CountEnqueuedBeforeObservation {
    receiver: std::sync::Mutex<mpsc::Receiver<HostedSubmission<Order>>>,
    observed: Arc<AtomicUsize>,
}

impl IngressBoundaryMiddleware for CountEnqueuedBeforeObservation {
    fn label(&self) -> &'static str {
        "count_enqueued_before_observation"
    }

    fn on_ingress(&self, _attempt: &IngressAttemptContext) -> IngressAdmissionDecision {
        IngressAdmissionDecision::Accept
    }

    fn observe(&self, attempt: &IngressAttemptContext, outcome: IngressAdmissionOutcome) {
        if outcome != IngressAdmissionOutcome::AcceptedForEnqueue {
            return;
        }
        let mut receiver = self.receiver.lock().expect("receiver lock");
        let mut enqueued = 0;
        while receiver.try_recv().is_ok() {
            enqueued += 1;
        }
        assert_eq!(enqueued, attempt.event_count as usize);
        self.observed.store(enqueued, Ordering::Release);
    }
}

/// FLOWIP-115s A3: `AcceptedForEnqueue` follows the send of every member.
#[tokio::test]
async fn accepted_observation_follows_enqueue_on_single_batch_and_handle_paths() {
    async fn observed_after(
        submit: impl FnOnce(IngestionState<Order>) -> futures::future::BoxFuture<'static, ()>,
    ) -> usize {
        let (state, rx) = IngestionState::<Order>::new(IngestionConfig {
            record_ingress_evidence: false,
            ..IngestionConfig::default()
        });
        let observed = Arc::new(AtomicUsize::new(0));
        state
            .ingress_slot()
            .fill(FilledHostedIngress {
                stage_id: StageId::new(),
                stage_key: "observed".into(),
                boundary: Some(Arc::new(CountEnqueuedBeforeObservation {
                    receiver: std::sync::Mutex::new(rx),
                    observed: Arc::clone(&observed),
                })),
            })
            .expect("slot fill succeeds once");
        mark_running(&state).await;
        submit(state).await;
        observed.load(Ordering::Acquire)
    }

    let single = observed_after(|state| {
        Box::pin(async move {
            SingleEventEndpoint::new(state, decoder::<Order>())
                .handle(post("/events", order("1", 1), None))
                .await
                .expect("single response");
        })
    })
    .await;
    assert_eq!(single, 1);

    let batch = observed_after(|state| {
        Box::pin(async move {
            BatchEventEndpoint::new(state, decoder::<Order>())
                .handle(post(
                    "/batch",
                    json!({"events": [order("1", 1), order("2", 1)]}),
                    None,
                ))
                .await
                .expect("batch response");
        })
    })
    .await;
    assert_eq!(batch, 2);

    let handle = observed_after(|state| {
        Box::pin(async move {
            let outcome = IngressHandle { state }
                .submit(Order {
                    order_id: "1".into(),
                    amount_cents: 1,
                })
                .await;
            assert!(matches!(outcome, IngressOutcome::Unknown { .. }));
        })
    })
    .await;
    assert_eq!(handle, 1);
}

#[derive(Clone)]
struct FailingAppendJournal<T: JournalEvent> {
    inner: MemoryJournal<T>,
}

#[async_trait]
impl<T: JournalEvent + 'static> obzenflow_core::journal::JournalStorage<T>
    for FailingAppendJournal<T>
{
    fn storage_id(&self) -> &JournalId {
        self.inner.id()
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }

    async fn storage_append(
        &self,
        _event: T,
        _options: obzenflow_core::journal::AppendOptions<T>,
    ) -> Result<JournalRecord<T::Payload>, JournalError> {
        Err(JournalError::Implementation {
            message: "forced append failure".to_string(),
            source: "forced append failure".into(),
        })
    }

    async fn storage_read_all_unordered(
        &self,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.inner.read_all_unordered().await
    }

    async fn storage_read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
        self.inner.read_event(event_id).await
    }

    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        self.inner.reader_from(position).await
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
        self.inner.read_last_n(count).await
    }
}

#[tokio::test]
async fn evidence_failure_answers_listener_unavailable() {
    let (state, _) = IngestionState::<Order>::new(IngestionConfig::default());
    state
        .ingress_slot()
        .fill(FilledHostedIngress {
            stage_id: StageId::new(),
            stage_key: "orders".into(),
            boundary: Some(Arc::new(AlwaysReject)),
        })
        .expect("slot fill succeeds once");
    state.install_evidence_writer(IngressEvidenceWriter::for_test(Arc::new(
        FailingAppendJournal {
            inner: MemoryJournal::with_owner(JournalOwner::system(SystemId::new())),
        },
    )));
    mark_running(&state).await;

    let response = unary(
        SingleEventEndpoint::new(state, decoder::<Order>())
            .handle(post("/api/ingest/events", order("1", 1), None))
            .await
            .unwrap(),
    );
    assert_eq!(response.status, 503);
    assert_eq!(
        response.headers.get("Retry-After").map(String::as_str),
        Some("1")
    );
    assert_eq!(
        body(&response),
        json!({"error": "ingress listener unavailable", "code": "listener_unavailable", "retryable": true})
    );
}

#[tokio::test]
async fn a_valid_webhook_signature_passes_authentication() {
    use ring::hmac;

    let secret = "secret";
    let edge = Edge::new(
        IngestionConfig {
            auth: Some(AuthConfig::WebhookSignature {
                secret: secret.to_string(),
                header: "stripe-signature".to_string(),
                replay_window_secs: None,
            }),
            ..IngestionConfig::default()
        },
        None,
    );
    let body_bytes = serde_json::to_vec(&order("1", 1)).unwrap();
    let ts = "1700000000";
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
    let mut signed = Vec::with_capacity(ts.len() + 1 + body_bytes.len());
    signed.extend_from_slice(ts.as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(&body_bytes);
    let signature: String = hmac::sign(&key, &signed)
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let request = Request::new(HttpMethod::Post, "/api/ingest/events".to_string())
        .with_header(
            "Stripe-Signature".to_string(),
            format!("t={ts},v1={signature}"),
        )
        .with_body(body_bytes);

    // Authentication passes, so the request reaches the readiness check.
    let response = unary(edge.single().handle(request).await.unwrap());
    assert_eq!(body(&response)["code"], "not_ready");
}

/// Acknowledgement follows the source commit, never delivery, so tests read
/// admissions from the source journal and the sink only completes the graph.
#[derive(Debug, Clone, Copy)]
struct DiscardSink;

#[async_trait]
impl InlineSink for DiscardSink {
    type Input = Order;

    fn describe(&self) -> SinkDescription {
        SinkDescription::method(DeliveryMethod::Custom("Discard".to_string()))
            .with_redelivery_safety(obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat)
    }

    async fn write(&mut self, _event: Order) -> Result<(), SinkWriteFailure> {
        Ok(())
    }
}

/// The source's committed `Order` rows, read from its own journal.
async fn source_rows(handle: &FlowHandle, slot: &HostedIngressBindingSlot) -> Vec<ChainEvent> {
    let stage = slot.filled().expect("bound source").stage_id;
    let (_, journal) = handle
        .stage_journals()
        .into_iter()
        .find(|(id, _)| *id == stage)
        .expect("source journal");
    journal
        .read_causally_ordered()
        .await
        .expect("read source journal")
        .into_iter()
        .filter(|record| record.event_type_name() == Order::EVENT_TYPE)
        .map(|record| record.authored())
        .collect()
}

fn unique_journal_dir(prefix: &str) -> PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
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

/// The hosted source feeding a discarding sink, on disk when `journals_root`
/// is set so a later run can replay it.
async fn ingress_flow(
    source: HostedIngressSource<TestIngress<Order>>,
    journals_root: Option<PathBuf>,
) -> FlowHandle {
    let sink = DiscardSink;
    FlowDefinition::materialize(move |_runtime_config| {
        Ok(match journals_root {
            Some(root) => flow! {
                name: "flowip_084l_running_ingress",
                journals: disk_journals(root),
                stages: {
                    source = async_infinite_source!(Order => source);
                    sink = sink!(Order => sink);
                },
                topology: {
                    source |> sink;
                }
            },
            None => flow! {
                name: "flowip_084l_running_ingress",
                journals: memory_journals(),
                stages: {
                    source = async_infinite_source!(Order => source);
                    sink = sink!(Order => sink);
                },
                topology: {
                    source |> sink;
                }
            },
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("build flow")
}

/// A hosted HTTP ingress source wired into a running flow.
struct RunningIngress {
    handle: FlowHandle,
    events: Box<dyn HttpEndpoint>,
    batch: Box<dyn HttpEndpoint>,
    submit: IngressHandle<Order>,
    tasks: Vec<JoinHandle<()>>,
}

/// What a stopped run committed: its ingress evidence and the source's rows.
struct Stopped {
    facts: Vec<SystemPayload>,
    rows: Vec<ChainEvent>,
}

impl RunningIngress {
    async fn start(name: &'static str, journals_root: Option<PathBuf>) -> Self {
        let ingress = http_ingress(
            decoder::<Order>(),
            IngestionConfig {
                base_path: format!("/api/{name}"),
                ..IngestionConfig::default()
            },
        );
        let submit = ingress.handle();
        let (source, http) = ingress.into_parts();
        let (_, mut endpoints, wiring, _) = http.into_surface().into_parts();
        let take = |suffix: &str, endpoints: &mut Vec<Box<dyn HttpEndpoint>>| {
            let position = endpoints
                .iter()
                .position(|endpoint| endpoint.path().ends_with(suffix))
                .expect("endpoint");
            endpoints.remove(position)
        };
        let events = take("/events", &mut endpoints);
        let batch = take("/batch", &mut endpoints);

        let handle = ingress_flow(source, journals_root).await;
        let wired = wiring.expect("ingress surface wiring")(WebSurfaceWiringContext {
            pipeline_state: handle.state_receiver(),
            ingress_evidence: handle.ingress_evidence(),
        })
        .expect("wire ingress surface");
        handle.start().await.expect("start flow");
        wait_for_running(&handle).await;
        tokio::task::yield_now().await;
        Self {
            handle,
            events,
            batch,
            submit,
            tasks: wired.tasks,
        }
    }

    async fn post_events(&self, payload: Value, key: Option<&str>) -> Response {
        unary(
            self.events
                .handle(post(self.events.path(), payload, key))
                .await
                .expect("events response"),
        )
    }

    async fn post_batch(&self, payload: Value, key: Option<&str>) -> Response {
        unary(
            self.batch
                .handle(post(self.batch.path(), payload, key))
                .await
                .expect("batch response"),
        )
    }

    async fn stop(self) -> Stopped {
        let system = self.handle.system_journal().expect("system journal");
        self.handle.stop_cancel().await.expect("stop flow");
        tokio::time::timeout(Duration::from_secs(5), self.handle.wait_for_completion())
            .await
            .expect("timed out waiting for flow completion")
            .expect("flow completion");
        for task in self.tasks {
            task.abort();
        }
        Stopped {
            facts: ingress_facts(system.as_ref()).await,
            rows: source_rows(&self.handle, &self.submit.ingress_slot()).await,
        }
    }
}

#[tokio::test]
async fn retries_resolve_to_the_committed_row_through_a_running_flow() {
    let running = RunningIngress::start("orders", None).await;

    let first = running.post_events(order("1", 250), Some("tx-1")).await;
    assert_eq!(first.status, 202);
    let first = body(&first);
    assert_eq!(first["committed"], 1);
    assert_eq!(first["results"][0]["outcome"], "committed");
    let event_id = first["results"][0]["event_id"].clone();

    let retry = running.post_events(order("1", 250), Some("tx-1")).await;
    assert_eq!(retry.status, 202);
    assert_eq!(
        body(&retry),
        json!({"committed": 0, "duplicates": 1, "rejected": 0, "unknown": 0,
               "results": [{"index": 0, "outcome": "duplicate", "event_id": event_id}]})
    );

    let conflict = running.post_events(order("1", 999), Some("tx-1")).await;
    assert_eq!(conflict.status, 422);
    assert_eq!(
        body(&conflict)["results"][0],
        json!({"index": 0, "outcome": "rejected", "code": "idempotency_key_conflict",
               "error": "Idempotency-Key \"tx-1\" was already used with different content"})
    );

    let in_process = running
        .submit
        .submit_identified(
            IngressRequestId::parse("tx-2").unwrap(),
            Order {
                order_id: "2".into(),
                amount_cents: 1,
            },
        )
        .await;
    let IngressOutcome::Committed { event_id: second } = in_process else {
        panic!("expected a committed in-process submission, got {in_process:?}");
    };
    let again = running
        .submit
        .submit_identified(
            IngressRequestId::parse("tx-2").unwrap(),
            Order {
                order_id: "2".into(),
                amount_cents: 1,
            },
        )
        .await;
    assert_eq!(again, IngressOutcome::Duplicate { event_id: second });

    let Stopped { facts, rows } = running.stop().await;
    // One committed row per request, each carrying its request identity.
    assert_eq!(
        rows.iter()
            .map(|row| {
                let request = row
                    .ingress_context
                    .as_ref()
                    .and_then(|context| context.request.as_ref())
                    .expect("identified row");
                (row.id.to_string(), request.id.as_str().to_string())
            })
            .collect::<Vec<_>>(),
        vec![
            (event_id.as_str().unwrap().to_string(), "tx-1".to_string()),
            (second.to_string(), "tx-2".to_string()),
        ]
    );
    let duplicates: Vec<_> = facts
        .iter()
        .filter_map(|fact| match fact {
            SystemPayload::IngressDuplicateRecognized {
                request_id,
                originals,
                ..
            } => Some((request_id.as_str().to_string(), originals.len())),
            _ => None,
        })
        .collect();
    assert_eq!(
        duplicates,
        vec![("tx-1".to_string(), 1), ("tx-2".to_string(), 1)]
    );
    assert_eq!(
        refusal_reasons(&facts),
        vec![(IngressRefusalReason::IdempotencyKeyConflict, 1)]
    );
}

#[tokio::test]
async fn a_retried_partial_batch_admits_only_uncommitted_members() {
    let running = RunningIngress::start("batch", None).await;
    let batch = |third: bool| {
        let mut events = vec![
            order("1", 250),
            json!({"event_type": "order.created", "data": {"amount_cents": 1}}),
        ];
        if third {
            events.push(order("3", 100));
        }
        json!({ "events": events })
    };

    let first = running.post_batch(batch(false), Some("batch-1")).await;
    assert_eq!(first.status, 202);
    let first = body(&first);
    assert_eq!(
        (first["committed"].clone(), first["rejected"].clone()),
        (json!(1), json!(1))
    );

    let retry = running.post_batch(batch(true), Some("batch-1")).await;
    assert_eq!(retry.status, 202);
    let retry = body(&retry);
    assert_eq!(retry["results"][0]["outcome"], "duplicate");
    assert_eq!(
        retry["results"][0]["event_id"],
        first["results"][0]["event_id"]
    );
    assert_eq!(retry["results"][1]["code"], "validation_failed");
    assert_eq!(retry["results"][2]["outcome"], "committed");

    let Stopped { facts, rows } = running.stop().await;
    assert_eq!(
        rows.iter()
            .map(|row| row
                .ingress_context
                .as_ref()
                .and_then(|context| context.batch_index))
            .collect::<Vec<_>>(),
        vec![Some(0), Some(2)]
    );
    assert!(matches!(
        facts.iter().find_map(|fact| match fact {
            SystemPayload::IngressDuplicateRecognized { originals, .. } => Some(originals.clone()),
            _ => None,
        }).as_deref(),
        Some([original]) if original.batch_index == Some(0)
    ));
}

/// A live request mid-replay would race an ungated replay, so the refusal is
/// proven here rather than in the contract test's strict replay.
#[tokio::test(flavor = "multi_thread")]
async fn a_strict_replay_refuses_ingress_throughout() {
    use crate::journal::disk::replay_archive::DiskReplayArchive;
    use obzenflow_runtime::bootstrap::{
        install_bootstrap_config, BootstrapConfig, ReplayBootstrap, ReplayVerb,
    };

    let root = unique_journal_dir("flowip_084l_strict_replay");
    let recorded = RunningIngress::start("recorded", Some(root.clone())).await;
    assert_eq!(
        recorded
            .post_events(order("1", 250), Some("tx-1"))
            .await
            .status,
        202
    );
    let recorded = recorded.stop().await;
    let archive = fs::read_dir(root.join("flows"))
        .expect("the recorded run")
        .map(|entry| entry.expect("a run directory").path())
        .find(|run| run.join("run_manifest.json").exists())
        .expect("a recorded archive");

    let _bootstrap = install_bootstrap_config(BootstrapConfig {
        replay_archive: Some(Arc::new(
            DiskReplayArchive::open(archive.clone(), true)
                .await
                .expect("open the archive"),
        )),
        replay: Some(ReplayBootstrap {
            archive_path: archive,
            allow_incomplete_archive: true,
            allow_duplicate_sink_delivery: false,
            verb: ReplayVerb::Replay,
        }),
        ..BootstrapConfig::default()
    });
    let ingress = http_ingress(
        decoder::<Order>(),
        IngestionConfig {
            base_path: "/api/replayed".to_string(),
            ..IngestionConfig::default()
        },
    );
    let slot = ingress.handle().ingress_slot();
    let (source, http) = ingress.into_parts();
    let (_, endpoints, wiring, _) = http.into_surface().into_parts();
    let events = endpoints
        .into_iter()
        .find(|endpoint| endpoint.path().ends_with("/events"))
        .expect("events endpoint");
    let handle = ingress_flow(source, Some(root)).await;
    let wired = wiring.expect("ingress surface wiring")(WebSurfaceWiringContext {
        pipeline_state: handle.state_receiver(),
        ingress_evidence: handle.ingress_evidence(),
    })
    .expect("wire ingress surface");
    let refuse = |id: &'static str, code: &'static str| {
        let request = post(events.path(), order(id, 1), Some(id));
        let events = &events;
        async move {
            let response = unary(events.handle(request).await.expect("events response"));
            assert_eq!(response.status, 503, "{id}");
            assert_eq!(body(&response)["code"], code, "{id}");
        }
    };

    handle.start().await.expect("start the replay");
    refuse("during", "not_ready").await;
    let system = handle.system_journal().expect("system journal");
    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .expect("timed out waiting for the replay")
        .expect("replay completion");
    assert!(!slot.is_resume_live(), "a strict replay never goes live");
    // The completed run's system journal is closed, so the refusal fails closed.
    refuse("after", "listener_unavailable").await;
    for task in wired.tasks {
        task.abort();
    }

    // The replay reproduces the recorded row and admits nothing new.
    let replayed: Vec<EventId> = source_rows(&handle, &slot)
        .await
        .iter()
        .map(|row| row.id)
        .collect();
    let rows: Vec<EventId> = recorded.rows.iter().map(|row| row.id).collect();
    assert_eq!(replayed, rows);
    assert_eq!(
        refusal_reasons(&ingress_facts(system.as_ref()).await),
        vec![(IngressRefusalReason::NotReady, 1)]
    );
}

#[tokio::test]
async fn ingress_journals_never_contain_the_snapshot_event() {
    let root = unique_journal_dir("flowip_084l_ingress_journal");
    let running = RunningIngress::start("journal", Some(root.clone())).await;
    let response = running.post_events(order("1", 1), None).await;
    assert_eq!(response.status, 202);
    running.stop().await;
    assert!(!journal_contains_token(&root, b"http_ingestion.snapshot"));
}

fn journal_contains_token(root: &Path, token: &[u8]) -> bool {
    let Ok(entries) = fs::read_dir(root) else {
        return false;
    };
    entries.flatten().any(|entry| {
        let path = entry.path();
        if path.is_dir() {
            journal_contains_token(&path, token)
        } else {
            fs::read(&path)
                .map(|bytes| bytes.windows(token.len()).any(|window| window == token))
                .unwrap_or(false)
        }
    })
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct PlacementPayload;

impl TypedPayload for PlacementPayload {
    const EVENT_TYPE: &'static str = "flowip_115s.placement";
}

#[tokio::test]
async fn duplicate_limiters_resolving_to_hosted_ingress_are_rejected() {
    use obzenflow_adapters::middleware::rate_limit;

    let ingress = http_ingress(
        decoder::<PlacementPayload>(),
        IngestionConfig {
            base_path: "/api/placement-duplicate".to_string(),
            record_ingress_evidence: false,
            ..Default::default()
        },
    );
    let (hosted_source, _) = ingress.into_parts();
    let built = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "flowip_115s_duplicate_positions",
            journals: memory_journals(),

            stages: {
                source = async_infinite_source!(
                    PlacementPayload => hosted_source with { rate_limit(10.0), rate_limit(10.0) }
                );
                sink = sink!(PlacementPayload => placeholder!());
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let failure = built
        .err()
        .expect("duplicate rate-limiter positions must fail");
    assert!(
        failure.run.is_none(),
        "validation must precede substrate selection"
    );
    assert!(matches!(
        failure.error,
        obzenflow_dsl::dsl::FlowBuildError::MiddlewarePlan(_)
    ));
    let message = failure.error.to_string();
    assert!(message.contains("rate_limiter"), "{message}");
    assert!(message.contains("duplicate"), "{message}");
}

#[tokio::test]
async fn a_hosted_rate_limiter_refuses_through_a_running_flow() {
    use obzenflow_adapters::middleware::rate_limit;

    let ingress = http_ingress(
        decoder::<Order>(),
        IngestionConfig {
            base_path: "/api/limited".to_string(),
            ..Default::default()
        },
    );
    let slot = ingress.handle().ingress_slot();
    let (source, http) = ingress.into_parts();
    let (_, endpoints, wiring, _) = http.into_surface().into_parts();
    let events = endpoints
        .into_iter()
        .find(|endpoint| endpoint.path().ends_with("/events"))
        .expect("events endpoint");
    let sink = DiscardSink;
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "flowip_084l_limited_ingress",
            journals: memory_journals(),

            stages: {
                // One burst token: the first POST is admitted, the second is
                // rate-limited fail-fast at the hosted edge.
                source = async_infinite_source!(
                    Order => source with rate_limit(0.001).burst_capacity(1.0)
                );
                sink = sink!(Order => sink);
            },

            topology: {
                source |> sink;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("build flow");
    let wired = wiring.expect("ingress surface wiring")(WebSurfaceWiringContext {
        pipeline_state: handle.state_receiver(),
        ingress_evidence: handle.ingress_evidence(),
    })
    .expect("wire ingress surface");
    handle.start().await.expect("start flow");
    wait_for_running(&handle).await;
    tokio::task::yield_now().await;

    let first = unary(
        events
            .handle(post(events.path(), order("1", 1), None))
            .await
            .unwrap(),
    );
    assert_eq!(first.status, 202);
    let second = unary(
        events
            .handle(post(events.path(), order("2", 1), None))
            .await
            .unwrap(),
    );
    assert_eq!(second.status, 429);

    let system = handle.system_journal().expect("system journal");
    handle.stop_cancel().await.expect("stop flow");
    tokio::time::timeout(Duration::from_secs(5), handle.wait_for_completion())
        .await
        .expect("timed out waiting for flow completion")
        .expect("flow completion");
    for task in wired.tasks {
        task.abort();
    }
    let facts = ingress_facts(system.as_ref()).await;
    assert_eq!(
        refusal_reasons(&facts),
        vec![(IngressRefusalReason::RateLimited, 1)]
    );
    assert!(matches!(
        &facts[0],
        SystemPayload::IngressRefusal { ingress_key, .. } if ingress_key.as_str() == "/api/limited"
    ));
    assert_eq!(source_rows(&handle, &slot).await.len(), 1);
}
