// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::*;
use crate::journal::MemoryJournal;
use futures::StreamExt;
use obzenflow_adapters::studio::ContractBoundaryAliases;
use obzenflow_core::composite::CompositeDefinition;
use obzenflow_core::event::journal_record::JournalRecord;
use obzenflow_core::event::journal_record::SystemJournalRecord;
use obzenflow_core::event::{PipelineLifecycleEvent, StageLifecycleEvent, SystemPayload, WriterId};
use obzenflow_core::id::{CompositeId, JournalId, RoleId, SystemId};
use obzenflow_core::journal::{JournalError, JournalReader};
use obzenflow_core::JournalOwner;
use obzenflow_core::{web::SseFrame, EventId, StageId};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

fn definition(left: StageId, right: StageId) -> Vec<CompositeDefinition> {
    vec![CompositeDefinition::new(
        CompositeId::new("test:pair"),
        vec![(left, RoleId::new("left")), (right, RoleId::new("right"))],
    )]
}

async fn append(
    journal: &dyn Journal<SystemEvent>,
    writer: WriterId,
    event: SystemPayload,
) -> SystemJournalRecord {
    journal
        .append(SystemEvent::new(writer, event), None)
        .await
        .expect("test event appends")
}

fn endpoint(
    journal: Arc<dyn Journal<SystemEvent>>,
    definitions: Vec<CompositeDefinition>,
) -> (StudioUpdatesEndpoint, watch::Sender<bool>) {
    let (closing, receiver) = watch::channel(false);
    (
        StudioUpdatesEndpoint::new(
            journal,
            StudioProjection::new(definitions, ContractBoundaryAliases::default()).unwrap(),
            Some(RuntimeInstanceId::new()),
            receiver,
        ),
        closing,
    )
}

async fn open(endpoint: &StudioUpdatesEndpoint, cursor: Option<&str>) -> SseBody {
    let mut request = Request::new(HttpMethod::Get, endpoint.path().into());
    if let Some(cursor) = cursor {
        request
            .headers
            .insert("Last-Event-ID".into(), cursor.into());
    }
    match endpoint.handle(request).await.unwrap() {
        ManagedResponse::Sse(body) => body,
        other => panic!("expected SSE, got {other:?}"),
    }
}

pub(super) async fn collect_closing(
    endpoint: &StudioUpdatesEndpoint,
    closing: watch::Sender<bool>,
    cursor: Option<&str>,
) -> Vec<SseFrame> {
    let body = open(endpoint, cursor).await;
    // Set shutdown after opening; an endpoint already closing would return HTTP 204.
    closing.send(true).unwrap();
    tokio::time::timeout(Duration::from_secs(2), body.collect())
        .await
        .expect("SSE response closes after terminal shutdown")
}

async fn request_body(
    journal: Arc<dyn Journal<SystemEvent>>,
    definitions: Vec<CompositeDefinition>,
    last_event_id: Option<EventId>,
) -> Vec<SseFrame> {
    let (endpoint, closing) = endpoint(journal, definitions);
    collect_closing(
        &endpoint,
        closing,
        last_event_id.map(|id| id.to_string()).as_deref(),
    )
    .await
}

pub(super) fn frames<'a>(body: &'a [SseFrame], event_name: &str) -> Vec<&'a SseFrame> {
    body.iter()
        .filter(|frame| frame.event.as_deref() == Some(event_name))
        .collect()
}

pub(super) fn frame_payload(frame: &SseFrame) -> serde_json::Value {
    serde_json::from_str(&frame.data).expect("SSE data is JSON")
}

async fn completed_tape() -> (
    Arc<MemoryJournal<SystemEvent>>,
    Vec<CompositeDefinition>,
    EventId,
) {
    let system_id = SystemId::new();
    let writer = WriterId::from(system_id);
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let left = StageId::new();
    let right = StageId::new();

    append(
        journal.as_ref(),
        writer,
        SystemPayload::StageLifecycle {
            stage_id: left,
            event: StageLifecycleEvent::Running,
        },
    )
    .await;
    append(
        journal.as_ref(),
        writer,
        SystemPayload::StageLifecycle {
            stage_id: left,
            event: StageLifecycleEvent::Completed { accounting: None },
        },
    )
    .await;
    let terminal = append(
        journal.as_ref(),
        writer,
        SystemPayload::StageLifecycle {
            stage_id: right,
            event: StageLifecycleEvent::Drained,
        },
    )
    .await;
    append(
        journal.as_ref(),
        writer,
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
    )
    .await;

    (
        journal,
        definition(left, right),
        terminal.envelope.provenance.event.id,
    )
}

#[tokio::test]
async fn fresh_valid_resume_and_missing_cursor_converge_on_terminal_snapshot() {
    let (journal, definitions, terminal_id) = completed_tape().await;

    let fresh = request_body(journal.clone(), definitions.clone(), None).await;
    let fresh_status = frames(&fresh, "composite_status");
    assert_eq!(fresh_status.len(), 1);
    assert_eq!(frame_payload(fresh_status[0])["status"], "completed");
    assert!(
        fresh_status[0].id.is_none(),
        "projected status must not mint a resume cursor"
    );

    let resumed = request_body(journal.clone(), definitions.clone(), Some(terminal_id)).await;
    let resumed_status = frames(&resumed, "composite_status");
    assert_eq!(resumed_status.len(), 1);
    let resumed_payload = frame_payload(resumed_status[0]);
    assert_eq!(resumed_payload["status"], "completed");
    assert_eq!(resumed_payload["as_of_event_id"], terminal_id.to_string());
    assert!(resumed_status[0].id.is_none());

    let missing = request_body(journal, definitions, Some(EventId::new())).await;
    let errors = frames(&missing, "error");
    assert!(errors
        .iter()
        .any(|frame| { frame_payload(frame)["error_type"] == "journal_resume_not_found" }));
    let missing_status = frames(&missing, "composite_status");
    assert_eq!(missing_status.len(), 1);
    assert_eq!(frame_payload(missing_status[0])["status"], "completed");
    assert_eq!(frames(&missing, "bootstrap").len(), 1);
}

#[tokio::test]
async fn reconnect_after_fact_recovers_measurements_only_after_factual_catch_up() {
    use obzenflow_core::event::observation::*;
    use obzenflow_core::event::payloads::execution_payload::{
        CircuitBreakerFact, CircuitState, MiddlewareFact,
    };
    use obzenflow_core::event::system_event::MiddlewareEventOrigin;
    use obzenflow_core::event::types::SeqNo;
    use obzenflow_runtime::metrics::observations::ObservationHub;

    let system = SystemId::new();
    let stage = StageId::new();
    let writer = WriterId::from(system);
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system)));
    let prefix = append(
        journal.as_ref(),
        writer,
        SystemPayload::StageLifecycle {
            stage_id: stage,
            event: StageLifecycleEvent::Running,
        },
    )
    .await;
    let scope = CaptureScope {
        flow_id: obzenflow_core::FlowId::new(),
        resume_generation: Default::default(),
    };
    let source = Arc::new(ObservationHub::default());
    source.activate_scope(scope);
    let sample = |seq| {
        let mut packet = ObservabilityContext::new(CaptureStamp {
            capture_scope: scope,
            observer: stage.into(),
            capture_seq: CaptureSeq(seq),
            capture_reason: CaptureReason::Periodic,
            observed_at_ms: seq,
        });
        packet
            .records
            .push(ObservationRecord::CircuitBreakerSummary {
                effect_type: None,
                window_duration_s: 1,
                requests_processed: seq,
                requests_rejected: 0,
                observed_state: CircuitState::Closed,
                consecutive_failures: 0,
                rejection_rate: 0.0,
                successes_total: seq,
                failures_total: 0,
                opened_total: 0,
                time_in_closed_seconds: 1.0,
                time_in_open_seconds: 0.0,
                time_in_half_open_seconds: 0.0,
            });
        packet
    };
    let mut opened = SystemEvent::new(
        writer,
        SystemPayload::MiddlewareLifecycle {
            stage_id: stage,
            stage_name: Some("worker".into()),
            flow_id: None,
            flow_name: None,
            origin: MiddlewareEventOrigin {
                event_id: EventId::new(),
                writer_key: stage.to_string(),
                seq: SeqNo(420),
            },
            middleware: MiddlewareFact::CircuitBreaker(CircuitBreakerFact::StateChanged {
                from_state: CircuitState::Closed,
                to_state: CircuitState::Open,
                timestamp: 420,
            }),
        },
    );
    opened.envelope.observability = Some(sample(3));
    let opened = journal.append(opened, None).await.unwrap();
    source.offer(sample(11));
    let (closing, receiver) = watch::channel(false);
    let endpoint = StudioUpdatesEndpoint::new(
        journal.clone(),
        StudioProjection::new(vec![], ContractBoundaryAliases::default())
            .unwrap()
            .with_observations(source),
        Some(RuntimeInstanceId::new()),
        receiver,
    );
    let mut first = open(&endpoint, Some(&prefix.id().to_string())).await;
    let fact = tokio::time::timeout(Duration::from_secs(2), first.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(fact.id, Some(opened.id().to_string()));
    assert_eq!(frame_payload(&fact)["revision"], 420);
    drop(first); // Disconnect before the optional frame belonging to this fact.

    let terminal = append(
        journal.as_ref(),
        writer,
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
    )
    .await;
    let resumed = collect_closing(&endpoint, closing, Some(&opened.id().to_string())).await;
    let terminal_index = resumed
        .iter()
        .position(|frame| frame.id == Some(terminal.id().to_string()))
        .unwrap();
    let measurement_index = resumed
        .iter()
        .position(|frame| frame_payload(frame).get("capture").is_some())
        .unwrap();
    assert!(
        terminal_index < measurement_index,
        "live samples follow all committed catch-up facts"
    );
    let measurement = &resumed[measurement_index];
    assert!(measurement.id.is_none());
    let body = frame_payload(measurement);
    assert_eq!(body["capture"]["capture_seq"], 11);
    assert_eq!(body["timestamp_ms"], 11);
    assert!(body.get("revision").is_none() && body.get("origin").is_none());
    assert!(body.get("state").is_none() && body.get("state_to").is_none());
    assert_eq!(
        resumed.last().unwrap().event.as_deref(),
        Some("server_shutdown")
    );
}

async fn terminal_projection_body(
    events: Vec<(StageId, StageLifecycleEvent)>,
    definitions: Vec<CompositeDefinition>,
) -> Vec<SseFrame> {
    let system_id = SystemId::new();
    let writer = WriterId::from(system_id);
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    for (stage_id, event) in events {
        append(
            journal.as_ref(),
            writer,
            SystemPayload::StageLifecycle { stage_id, event },
        )
        .await;
    }
    append(
        journal.as_ref(),
        writer,
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
    )
    .await;
    request_body(journal, definitions, None).await
}

#[tokio::test]
async fn clean_cancellation_and_contradictory_history_surface_at_the_route() {
    let left = StageId::new();
    let right = StageId::new();
    let cancelled = terminal_projection_body(
        vec![
            (
                left,
                StageLifecycleEvent::Cancelled {
                    reason: "operator stop".to_string(),
                    accounting: None,
                },
            ),
            (
                right,
                StageLifecycleEvent::Cancelled {
                    reason: "sibling stop".to_string(),
                    accounting: None,
                },
            ),
        ],
        definition(left, right),
    )
    .await;
    let cancelled_payload = frame_payload(frames(&cancelled, "composite_status")[0]);
    assert_eq!(cancelled_payload["status"], "cancelled");
    assert_eq!(cancelled_payload["reason"], "operator stop");

    let left = StageId::new();
    let right = StageId::new();
    let contradictory = terminal_projection_body(
        vec![
            (left, StageLifecycleEvent::Completed { accounting: None }),
            (
                left,
                StageLifecycleEvent::Cancelled {
                    reason: "late contradiction".to_string(),
                    accounting: None,
                },
            ),
        ],
        definition(left, right),
    )
    .await;
    let invalid_payload = frame_payload(frames(&contradictory, "composite_status")[0]);
    assert_eq!(invalid_payload["status"], "invalid");
    assert!(invalid_payload["error"]
        .as_str()
        .is_some_and(|error| error.contains("conflicting terminal")));
}

struct ScriptedReader {
    pending_read: Option<Arc<PendingOpen>>,
    reads: Arc<AtomicUsize>,
    events: Vec<SystemJournalRecord>,
    position: usize,
    fail_at: Option<usize>,
    dropped: Arc<AtomicBool>,
}

impl Drop for ScriptedReader {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl JournalReader<SystemEvent> for ScriptedReader {
    async fn next(&mut self) -> Result<Option<SystemJournalRecord>, JournalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(probe) = &self.pending_read {
            let _guard = PendingOpenGuard(probe.clone());
            probe.entered.notify_one();
            std::future::pending::<()>().await;
        }
        if self.fail_at == Some(self.position) {
            return Err(JournalError::SubscriptionClosed);
        }
        let next = self.events.get(self.position).cloned();
        if next.is_some() {
            self.position += 1;
        }
        Ok(next)
    }

    fn position(&self) -> u64 {
        self.position as u64
    }
}

#[derive(Default)]
struct PendingOpen {
    entered: tokio::sync::Notify,
    dropped: tokio::sync::Notify,
}

struct PendingOpenGuard(Arc<PendingOpen>);
impl Drop for PendingOpenGuard {
    fn drop(&mut self) {
        self.0.dropped.notify_one();
    }
}

#[tokio::test]
async fn dropping_studio_response_cancels_pending_journal_open() {
    let mut journal = ScriptedJournal::new(SystemId::new());
    let probe = Arc::new(PendingOpen::default());
    journal.pending_open = Some(probe.clone());
    let (endpoint, _closing) = endpoint(Arc::new(journal), vec![]);
    let mut body = open(&endpoint, None).await;
    tokio::select! {
        _ = body.next() => panic!("pending open cannot produce a frame"),
        _ = probe.entered.notified() => {}
    }
    drop(body);
    tokio::time::timeout(Duration::from_secs(1), probe.dropped.notified())
        .await
        .expect("response drop must cancel an opening reader");
}

pub(crate) struct ScriptedJournal {
    pending_read: Option<Arc<PendingOpen>>,
    pub(crate) reads: Arc<AtomicUsize>,
    pub(crate) opens: Arc<AtomicUsize>,
    inner: MemoryJournal<SystemEvent>,
    fail_at: Option<usize>,
    fail_open: bool,
    pending_open: Option<Arc<PendingOpen>>,
    reader_dropped: Arc<AtomicBool>,
    reader_opened: tokio::sync::Notify,
}

impl ScriptedJournal {
    pub(crate) fn new(system_id: SystemId) -> Self {
        Self {
            reads: Arc::new(AtomicUsize::new(0)),
            opens: Arc::new(AtomicUsize::new(0)),
            pending_read: None,
            inner: MemoryJournal::with_owner(JournalOwner::system(system_id)),
            fail_at: None,
            fail_open: false,
            pending_open: None,
            reader_dropped: Arc::new(AtomicBool::new(false)),
            reader_opened: tokio::sync::Notify::new(),
        }
    }
}

#[async_trait]
impl Journal<SystemEvent> for ScriptedJournal {
    fn id(&self) -> &JournalId {
        self.inner.id()
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }

    async fn append(
        &self,
        event: SystemEvent,
        parent: Option<&JournalRecord<obzenflow_core::event::SystemPayload>>,
    ) -> Result<SystemJournalRecord, JournalError> {
        self.inner.append(event, parent).await
    }

    async fn read_all_unordered(&self) -> Result<Vec<SystemJournalRecord>, JournalError> {
        self.inner.read_all_unordered().await
    }

    async fn read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<SystemJournalRecord>, JournalError> {
        self.inner.read_event(event_id).await
    }

    async fn reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<SystemEvent>>, JournalError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        if let Some(probe) = &self.pending_open {
            let _guard = PendingOpenGuard(probe.clone());
            probe.entered.notify_one();
            std::future::pending::<()>().await;
        }
        if self.fail_open {
            return Err(JournalError::SubscriptionClosed);
        }
        let reader = Box::new(ScriptedReader {
            pending_read: self.pending_read.clone(),
            reads: self.reads.clone(),
            events: self.inner.read_all_unordered().await?,
            position: position as usize,
            fail_at: self.fail_at,
            dropped: self.reader_dropped.clone(),
        });
        self.reader_opened.notify_one();
        Ok(reader)
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<SystemJournalRecord>, JournalError> {
        self.inner.read_last_n(count).await
    }
}

#[tokio::test]
async fn journal_open_and_read_failures_are_typed_route_errors_only() {
    let system_id = SystemId::new();
    let writer = WriterId::from(system_id);
    let left = StageId::new();
    let right = StageId::new();
    let mut read_failure = ScriptedJournal::new(system_id);
    append(
        &read_failure,
        writer,
        SystemPayload::StageLifecycle {
            stage_id: left,
            event: StageLifecycleEvent::Running,
        },
    )
    .await;
    read_failure.fail_at = Some(1);
    let read_failure = Arc::new(read_failure);
    let body = request_body(read_failure.clone(), definition(left, right), None).await;
    assert_eq!(
        frame_payload(frames(&body, "error")[0])["error_type"],
        "journal_read_error"
    );
    assert_eq!(read_failure.read_all_unordered().await.unwrap().len(), 1);

    let system_id = SystemId::new();
    let mut open_failure = ScriptedJournal::new(system_id);
    open_failure.fail_open = true;
    let body = request_body(
        Arc::new(open_failure),
        definition(StageId::new(), StageId::new()),
        None,
    )
    .await;
    assert_eq!(
        frame_payload(frames(&body, "error")[0])["error_type"],
        "journal_open_error"
    );
}

#[tokio::test]
async fn dropping_the_sse_body_drops_its_reader() {
    let system_id = SystemId::new();
    let journal = Arc::new(ScriptedJournal::new(system_id));
    let reader_dropped = journal.reader_dropped.clone();
    let (endpoint, _closing) = endpoint(journal, vec![]);
    let mut body = open(&endpoint, None).await;
    assert_eq!(
        body.next().await.unwrap().event.as_deref(),
        Some("bootstrap")
    );
    drop(body);
    assert!(
        reader_dropped.load(Ordering::SeqCst),
        "response owns and drops its reader synchronously"
    );
}

#[tokio::test]
async fn empty_bootstrap_and_closing_admission_do_not_fabricate_cursors() {
    let journal = Arc::new(ScriptedJournal::new(SystemId::new()));
    let (endpoint, closing) = endpoint(journal.clone(), vec![]);
    assert_eq!(endpoint.methods(), &[HttpMethod::Get]);
    assert!(endpoint.managed_route().is_none());

    // Creating the response leaves the reader unopened until the body is polled.
    drop(open(&endpoint, None).await);
    assert_eq!(journal.opens.load(Ordering::SeqCst), 0);
    let mut body = open(&endpoint, None).await;
    let bootstrap = body.next().await.unwrap();
    assert_eq!(bootstrap.event.as_deref(), Some("bootstrap"));
    assert!(bootstrap.id.is_none());
    assert!(frame_payload(&bootstrap)["checkpoint_event_id"].is_null());
    assert_eq!(
        frame_payload(&bootstrap)["runtime_instance_id"],
        endpoint.runtime_instance_id.as_ref().unwrap().as_str()
    );
    drop(body);

    closing.send(true).unwrap();
    let response = endpoint
        .handle(Request::new(HttpMethod::Get, endpoint.path().into()))
        .await
        .unwrap();
    let ManagedResponse::Unary(response) = response else {
        panic!("closing admission must be unary");
    };
    assert_eq!(response.status, 204);
    assert!(response.body.is_empty());
    assert_eq!(journal.opens.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn malformed_and_unknown_cursors_preserve_error_payloads_and_fresh_fallback() {
    let (journal, definitions, _) = completed_tape().await;
    let last_id = journal.read_last_n(1).await.unwrap()[0]
        .envelope
        .provenance
        .event
        .id
        .to_string();
    for cursor in [
        String::new(),
        "malformed".into(),
        EventId::new().to_string(),
    ] {
        let (endpoint, closing) = endpoint(journal.clone(), definitions.clone());
        let body = collect_closing(&endpoint, closing, Some(&cursor)).await;
        let error = frame_payload(&body[0]);
        match EventId::from_string(&cursor) {
            Err(parse_error) => assert_eq!(
                error,
                serde_json::json!({
                    "error_type": "invalid_last_event_id", "message": parse_error.to_string(), "recoverable": false
                })
            ),
            Ok(_) => assert_eq!(
                error,
                serde_json::json!({
                    "error_type": "journal_resume_not_found",
                    "message": "Last-Event-ID was not found in the system journal; resuming from live tail",
                    "recoverable": true
                })
            ),
        }
        assert!(body[0].id.is_none());
        let bootstrap = frames(&body, "bootstrap")[0];
        assert_eq!(bootstrap.id.as_deref(), Some(last_id.as_str()));
        assert_eq!(frame_payload(bootstrap)["checkpoint_event_id"], last_id);
        assert_eq!(frames(&body, "stage_lifecycle").len(), 2);
        assert_eq!(
            body.last().unwrap().event.as_deref(),
            Some("server_shutdown")
        );
        assert!(body.last().unwrap().id.is_none());
    }
}

#[tokio::test]
async fn pending_reads_and_catch_up_are_owned_and_cancellable_by_the_body() {
    let mut journal = ScriptedJournal::new(SystemId::new());
    let probe = Arc::new(PendingOpen::default());
    journal.pending_read = Some(probe.clone());
    let reader_dropped = journal.reader_dropped.clone();
    let (endpoint, _closing) = endpoint(Arc::new(journal), vec![]);
    let mut body = open(&endpoint, None).await;
    tokio::select! {
        _ = body.next() => panic!("pending read cannot produce a frame"),
        _ = probe.entered.notified() => {}
    }
    drop(body);
    assert!(reader_dropped.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(1), probe.dropped.notified())
        .await
        .unwrap();

    let system = SystemId::new();
    let journal = Arc::new(ScriptedJournal::new(system));
    let stage = StageId::new();
    for _ in 0..256 {
        append(
            journal.as_ref(),
            WriterId::from(system),
            SystemPayload::StageLifecycle {
                stage_id: stage,
                event: StageLifecycleEvent::Running,
            },
        )
        .await;
    }
    let (endpoint, _closing) = self::endpoint(journal.clone(), vec![]);
    let mut body = open(&endpoint, None).await;
    assert!(
        futures::poll!(body.next()).is_pending(),
        "ready history must yield before exhausting the tape"
    );
    let reads = journal.reads.load(Ordering::SeqCst);
    assert!(reads > 0 && reads < 256);
    drop(body);
    assert!(journal.reader_dropped.load(Ordering::SeqCst));
    tokio::task::yield_now().await;
    assert_eq!(journal.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn differently_paced_clients_keep_independent_cursors_and_repair_derived_frame_disconnects() {
    let system = SystemId::new();
    let writer = WriterId::from(system);
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system)));
    let left = StageId::new();
    let right = StageId::new();
    let (endpoint, closing) = endpoint(journal.clone(), definition(left, right));
    let mut slow = open(&endpoint, None).await;
    let mut fast = open(&endpoint, None).await;
    for body in [&mut slow, &mut fast] {
        assert_eq!(
            body.next().await.unwrap().event.as_deref(),
            Some("composite_status")
        );
        assert_eq!(
            body.next().await.unwrap().event.as_deref(),
            Some("bootstrap")
        );
    }

    let running = append(
        journal.as_ref(),
        writer,
        SystemPayload::StageLifecycle {
            stage_id: left,
            event: StageLifecycleEvent::Running,
        },
    )
    .await;
    let fact = fast.next().await.unwrap();
    assert_eq!(
        fact.id.as_deref(),
        Some(running.envelope.provenance.event.id.to_string().as_str())
    );
    // Disconnect after receiving the stage message and its reconnect ID, but
    // before receiving the accompanying composite status.
    drop(fast);
    let mut resumed = open(
        &endpoint,
        Some(&running.envelope.provenance.event.id.to_string()),
    )
    .await;
    let repaired = resumed.next().await.unwrap();
    assert_eq!(frame_payload(&repaired)["status"], "running");
    assert!(repaired.id.is_none());
    assert_eq!(
        frame_payload(&repaired)["as_of_event_id"],
        running.envelope.provenance.event.id.to_string()
    );

    append(
        journal.as_ref(),
        writer,
        SystemPayload::StageLifecycle {
            stage_id: left,
            event: StageLifecycleEvent::Completed { accounting: None },
        },
    )
    .await;
    append(
        journal.as_ref(),
        writer,
        SystemPayload::StageLifecycle {
            stage_id: right,
            event: StageLifecycleEvent::Drained,
        },
    )
    .await;
    append(
        journal.as_ref(),
        writer,
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
    )
    .await;
    closing.send(true).unwrap();
    let resumed: Vec<_> = resumed.collect().await;
    let slow: Vec<_> = slow.collect().await;
    assert_eq!(frames(&resumed, "stage_lifecycle").len(), 2);
    assert_eq!(frames(&slow, "stage_lifecycle").len(), 3);
    for body in [&resumed, &slow] {
        assert_eq!(
            frame_payload(frames(body, "composite_status").last().unwrap())["status"],
            "completed"
        );
        assert_eq!(
            body.last().unwrap().event.as_deref(),
            Some("server_shutdown")
        );
    }
    assert_eq!(
        journal.read_all_unordered().await.unwrap().len(),
        4,
        "readers create no facts"
    );
}

#[tokio::test]
async fn terminal_flow_totals_reach_sse_independently_of_metrics_reporting() {
    use obzenflow_adapters::monitoring::MetricsReadModel;
    use obzenflow_core::event::{PipelineLifecycleEvent, SystemPayload};
    use obzenflow_dsl::{flow, sink, source, FlowDefinition};
    use obzenflow_runtime::run_context::FlowBuildContext;
    use obzenflow_runtime::stages::sink::SinkTyped;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Item(u64);
    impl obzenflow_core::TypedPayload for Item {
        const EVENT_TYPE: &'static str = "metrics_reporting.terminal_proof";
    }

    let enabled_modes = if cfg!(feature = "prometheus") {
        vec![false, true]
    } else {
        vec![false]
    };
    for enabled in enabled_modes {
        let model = enabled.then(|| Arc::new(MetricsReadModel::default()));
        let definition = FlowDefinition::materialize(move |_| {
            let input = obzenflow_adapters::sources::finite(vec![Item(1), Item(2), Item(3)]);
            let output = SinkTyped::new(|_: Item| async {}).idempotent();
            Ok(flow! {
                name: "terminal_reporting_proof",
                journals: crate::journal::memory_journals(),
                stages: {
                    input = source!(Item => input);
                    output = sink!(Item => output);
                },
                topology: { input |> output; }
            })
        });
        let mut context = FlowBuildContext::for_tests();
        if let Some(model) = model {
            context = context.with_metrics_exporter(model);
        }
        let handle = definition.build(context).await.unwrap();
        let journal = handle.system_journal().unwrap();
        handle.run().await.unwrap();

        let mut reader = journal.reader_from(0).await.unwrap();
        let cursor = reader
            .next()
            .await
            .unwrap()
            .unwrap()
            .envelope
            .provenance
            .event
            .id;
        let mut duration = None;
        while let Some(envelope) = reader.next().await.unwrap() {
            if let SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Completed {
                duration_ms,
                metrics,
            }) = &envelope.payload
            {
                assert_eq!(metrics.events_in_total, 3);
                assert_eq!(metrics.events_out_total, 3);
                assert_eq!(metrics.errors_total, 0);
                duration = Some(serde_json::to_value(duration_ms).unwrap());
            }
        }
        let duration = duration.expect("terminal lifecycle fact must exist without a reporter");

        // Connect after the run finishes, using its first entry as `Last-Event-ID`.
        // The completion message must include totals even without a metrics scrape.
        let body = request_body(journal, vec![], Some(cursor)).await;
        let payload = frames(&body, "flow_lifecycle")
            .into_iter()
            .map(frame_payload)
            .find(|payload| payload["event_type"] == "flow_completed")
            .expect("SSE must carry the journaled terminal event");
        assert_eq!(payload["duration_ms"], duration);
        assert_eq!(
            payload["metrics"],
            serde_json::json!({
                "events_in_total": 3,
                "events_out_total": 3,
                "errors_total": 0,
            })
        );
    }
}
