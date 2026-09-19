// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::*;
use crate::journal::MemoryJournal;
use futures::StreamExt;
use obzenflow_adapters::studio::ContractBoundaryAliases;
use obzenflow_core::composite::CompositeDefinition;
use obzenflow_core::event::journal_record::SystemJournalRecord;
use obzenflow_core::event::{PipelineLifecycleEvent, StageLifecycleEvent, SystemPayload, WriterId};
use obzenflow_core::id::{CompositeId, JournalId, RoleId, SystemId};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::journal::{JournalError, JournalReader};
use obzenflow_core::{web::SseFrame, EventId, FlowId, JournalOwner, StageId};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

fn definition(left: StageId, right: StageId) -> Vec<CompositeDefinition> {
    vec![CompositeDefinition::new(
        CompositeId::new("test:pair"),
        vec![(left, RoleId::new("left")), (right, RoleId::new("right"))],
    )]
}

#[tokio::test]
async fn initial_prefix_includes_every_atomic_member_and_stays_fixed_while_tailing() {
    let directory = tempfile::tempdir().unwrap();
    let system = SystemId::new();
    let stage = StageId::new();
    let journals: Vec<Arc<dyn Journal<SystemEvent>>> = vec![
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system))),
        Arc::new(
            crate::journal::disk::DiskJournal::with_owner(
                directory.path().join("prefix.log"),
                JournalOwner::system(system),
            )
            .unwrap(),
        ),
    ];
    for journal in journals {
        let empty = journal.reader().await.unwrap();
        assert!(empty.initial_prefix_complete().unwrap());
        let event = || {
            SystemEvent::new(
                system.into(),
                SystemPayload::StageLifecycle {
                    stage_id: stage,
                    event: StageLifecycleEvent::Running,
                },
            )
        };
        let group = journal
            .append_group("initial", vec![event(), event()], Default::default())
            .await
            .unwrap();
        let mut reader = journal.reader().await.unwrap();
        let later = journal.append(event(), Default::default()).await.unwrap();
        assert!(!reader.initial_prefix_complete().unwrap());
        assert_eq!(reader.next().await.unwrap().unwrap().id(), group[0].id());
        assert!(
            !reader.initial_prefix_complete().unwrap(),
            "a parsed group still has a buffered member"
        );
        assert_eq!(reader.next().await.unwrap().unwrap().id(), group[1].id());
        assert!(reader.initial_prefix_complete().unwrap());
        assert!(
            !reader.is_at_end(),
            "the initial cut cannot certify physical end"
        );
        assert_eq!(reader.next().await.unwrap().unwrap().id(), later.id());
        assert!(reader.initial_prefix_complete().unwrap());
        assert!(reader.next().await.unwrap().is_none());
        assert!(reader.is_at_end());
        let appended = journal.append(event(), Default::default()).await.unwrap();
        assert_eq!(reader.next().await.unwrap().unwrap().id(), appended.id());
        assert!(reader.initial_prefix_complete().unwrap());
        assert!(empty.initial_prefix_complete().unwrap());
    }
}

#[tokio::test(start_paused = true)]
async fn two_connections_coalesce_live_and_attached_observations_on_one_deadline_under_busy_facts()
{
    use obzenflow_adapters::monitoring::MetricsReadModel;
    use obzenflow_core::event::observability::*;
    use obzenflow_core::metrics::{
        AppMetricsSnapshot, MetricsSnapshotExporter, ThroughputMeasurement,
    };
    use obzenflow_core::time::MetricsDuration;
    use obzenflow_runtime::metrics::observations::LatestObservationMap;

    for interval_ms in [250, 500] {
        let system = SystemId::new();
        let stage = StageId::new();
        let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system)));
        let model = Arc::new(MetricsReadModel::default());
        let observations = Arc::new(LatestObservationMap::default());
        let scope = CaptureScope {
            flow_id: FlowId::new(),
            resume_generation: Default::default(),
        };
        observations.activate_scope(scope);
        let stamp = |sequence| CaptureStamp {
            capture_scope: scope,
            observer: system.into(),
            capture_seq: CaptureSeq(sequence),
            capture_reason: CaptureReason::Periodic,
            observed_at_ms: sequence,
        };
        let publish = |sequence| {
            let mut snapshot = AppMetricsSnapshot::default();
            snapshot.throughput.stages.insert(
                stage,
                ThroughputMeasurement {
                    capture: stamp(sequence),
                    event_delta: 21,
                    elapsed: MetricsDuration::from_millis(500),
                    events_per_second: 42.0,
                },
            );
            model.publish_app_snapshot(snapshot);
        };
        let edge = |sequence| {
            let mut packet = ObservabilityContext::new(stamp(sequence));
            packet.records.push(ObservationRecord::EdgeLiveness {
                upstream: stage,
                reader: stage,
                state: EdgeLivenessState::Healthy,
                idle_ms: obzenflow_core::event::types::DurationMs(0),
                last_reader_seq: None,
                last_event_id: None,
            });
            packet
        };
        publish(7);
        let (closing, receiver) = watch::channel(false);
        let endpoint = StudioUpdatesEndpoint::new(
            journal.clone(),
            StudioProjection::new(vec![], ContractBoundaryAliases::default())
                .unwrap()
                .with_observations(observations.clone())
                .with_throughput(model.clone()),
            None,
            receiver,
        )
        .with_observation_interval(Duration::from_millis(interval_ms));
        let mut fast = open(&endpoint, None).await;
        let mut slow = open(&endpoint, None).await;
        for client in [&mut fast, &mut slow] {
            assert_eq!(
                client.next().await.unwrap().event.as_deref(),
                Some("bootstrap")
            );
            let initial = client.next().await.unwrap();
            assert_eq!(
                frame_payload(&initial)["stages"][0]["measurement"]["capture"]["capture_seq"],
                7
            );
            assert!(initial.id.is_none());
            assert!(frame_payload(&initial).get("capture").is_none());
        }
        publish(8);
        observations.offer(edge(8));
        tokio::time::advance(Duration::from_millis(interval_ms - 1)).await;
        let fact = append(
            journal.as_ref(),
            system.into(),
            SystemPayload::StageLifecycle {
                stage_id: stage,
                event: StageLifecycleEvent::Running,
            },
        )
        .await;
        assert_eq!(fast.next().await.unwrap().id, Some(fact.id().to_string()));
        assert!(
            futures::poll!(fast.next()).is_pending(),
            "live observations cannot bypass the observation deadline"
        );
        publish(9);
        observations.offer(edge(9));
        tokio::time::advance(Duration::from_millis(1)).await;
        for client in [&mut fast, &mut slow] {
            loop {
                let frame = client.next().await.unwrap();
                if frame.event.as_deref() == Some("throughput_update") {
                    assert_eq!(
                        frame_payload(&frame)["stages"][0]["measurement"]["capture"]["capture_seq"],
                        9
                    );
                    assert!(frame.id.is_none());
                    break;
                }
                if frame.event.as_deref() == Some("edge_liveness") {
                    assert_eq!(frame_payload(&frame)["capture"]["capture_seq"], 9);
                    assert!(frame.id.is_none());
                }
            }
        }
        let mut attached = SystemEvent::new(
            system.into(),
            SystemPayload::StageLifecycle {
                stage_id: stage,
                event: StageLifecycleEvent::Running,
            },
        );
        attached.envelope.observability = Some(edge(10));
        let attached = journal.append(attached, Default::default()).await.unwrap();
        assert_eq!(
            fast.next().await.unwrap().id,
            Some(attached.id().to_string())
        );
        assert!(
            futures::poll!(fast.next()).is_pending(),
            "journal attachments cannot bypass the same deadline"
        );
        tokio::time::advance(Duration::from_millis(interval_ms)).await;
        let attached = fast.next().await.unwrap();
        assert_eq!(attached.event.as_deref(), Some("edge_liveness"));
        assert_eq!(frame_payload(&attached)["capture"]["capture_seq"], 10);
        assert!(attached.id.is_none());
        // A ready prefix of records that produce no public frame must not
        // starve a due measurement or force the reader to reach physical EOF.
        for sequence in 0..128 {
            append(
                journal.as_ref(),
                system.into(),
                SystemPayload::IngressRefusal {
                    ingress_key: obzenflow_core::ingress::IngressKey("busy".into()),
                    stage_id: stage,
                    stage_key: "busy".into(),
                    reason: obzenflow_core::ingress::IngressRefusalReason::NotReady,
                    attempt_seq: obzenflow_core::ingress::IngressAttemptSeq(sequence),
                    request_count: 1,
                    event_count: 1,
                    batch_count: 0,
                    http_status: 503,
                    retry_after_ms_bucket: None,
                },
            )
            .await;
        }
        publish(11);
        tokio::time::advance(Duration::from_millis(interval_ms)).await;
        let frame = fast.next().await.unwrap();
        assert_eq!(frame.event.as_deref(), Some("throughput_update"));
        assert_eq!(
            frame_payload(&frame)["stages"][0]["measurement"]["capture"]["capture_seq"],
            11
        );
        append(
            journal.as_ref(),
            system.into(),
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
        )
        .await;
        closing.send(true).unwrap();
        let rest: Vec<_> = fast.collect().await;
        assert_eq!(
            rest.last().unwrap().event.as_deref(),
            Some("server_shutdown")
        );
    }
}

async fn append(
    journal: &dyn Journal<SystemEvent>,
    writer: WriterId,
    event: SystemPayload,
) -> SystemJournalRecord {
    journal
        .append(SystemEvent::new(writer, event), Default::default())
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
    use obzenflow_core::event::observability::*;
    use obzenflow_core::event::payloads::execution_payload::{
        CircuitBreakerFact, CircuitBreakerOpenTrigger, CircuitState, MiddlewareFact,
    };
    use obzenflow_core::event::payloads::system_payload::MiddlewareEventOrigin;
    use obzenflow_core::event::types::SeqNo;
    use obzenflow_runtime::metrics::observations::LatestObservationMap;

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
        flow_id: FlowId::new(),
        resume_generation: Default::default(),
    };
    let source = Arc::new(LatestObservationMap::default());
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
            middleware: MiddlewareFact::CircuitBreaker(CircuitBreakerFact::Opened {
                cooldown_ms: 5_000,
                error_rate: 1.0,
                failure_count: 3,
                trigger: CircuitBreakerOpenTrigger::ConsecutiveFailures,
                observed_calls: 3,
                slow_call_rate: None,
                slow_call_count: None,
                last_error: None,
            }),
        },
    );
    opened.envelope.observability = Some(sample(3));
    let opened = journal.append(opened, Default::default()).await.unwrap();
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
    assert_eq!(frame_payload(&fact)["context"]["cooldown_ms"], 5_000);
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

    fn initial_prefix_complete(&self) -> Result<bool, JournalError> {
        Ok(self.position >= self.events.len())
    }

    fn is_at_end(&self) -> bool {
        self.position >= self.events.len()
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
        options: AppendOptions<'_, SystemEvent>,
    ) -> Result<SystemJournalRecord, JournalError> {
        self.inner.append(event, options).await
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

#[tokio::test(start_paused = true)]
async fn bootstrap_fallback_restores_middleware_before_post_cut_facts() {
    use obzenflow_core::event::payloads::execution_payload::{
        CircuitBreakerFact, CircuitBreakerOpenTrigger, CircuitState, MiddlewareFact,
        RateLimiterFact, RateLimiterMode,
    };
    use obzenflow_core::event::payloads::system_payload::MiddlewareEventOrigin;
    use obzenflow_core::event::types::SeqNo;

    for disk in [false, true] {
        for cursor_kind in ["unknown", "fresh", "malformed", "known"] {
            let directory = tempfile::tempdir().unwrap();
            let system = SystemId::new();
            let breaker = StageId::new();
            let limiter = StageId::new();
            let journal: Arc<dyn Journal<SystemEvent>> = if disk {
                Arc::new(
                    crate::journal::disk::DiskJournal::with_owner(
                        directory.path().join("fallback.log"),
                        JournalOwner::system(system),
                    )
                    .unwrap(),
                )
            } else {
                Arc::new(MemoryJournal::with_owner(JournalOwner::system(system)))
            };
            let middleware = |stage: StageId, revision, fact| SystemPayload::MiddlewareLifecycle {
                stage_id: stage,
                stage_name: None,
                flow_id: None,
                flow_name: None,
                origin: MiddlewareEventOrigin {
                    event_id: EventId::new(),
                    writer_key: stage.to_string(),
                    seq: SeqNo(revision),
                },
                middleware: fact,
            };
            let opened = append(
                journal.as_ref(),
                system.into(),
                middleware(
                    breaker,
                    420,
                    MiddlewareFact::CircuitBreaker(CircuitBreakerFact::Opened {
                        cooldown_ms: 5_000,
                        error_rate: 1.0,
                        failure_count: 3,
                        trigger: CircuitBreakerOpenTrigger::ConsecutiveFailures,
                        observed_calls: 3,
                        slow_call_rate: None,
                        slow_call_count: None,
                        last_error: None,
                    }),
                ),
            )
            .await;
            let prefix = append(
                journal.as_ref(),
                system.into(),
                middleware(
                    limiter,
                    7,
                    MiddlewareFact::RateLimiter(RateLimiterFact::ModeChange {
                        mode_from: RateLimiterMode::Normal,
                        mode_to: RateLimiterMode::Limiting,
                        limit_rate: 10.0,
                    }),
                ),
            )
            .await;
            let cursor = match cursor_kind {
                "unknown" => Some(EventId::new().to_string()),
                "fresh" => None,
                "malformed" => Some("malformed".to_string()),
                "known" => Some(prefix.id().to_string()),
                _ => unreachable!(),
            };
            let (endpoint, closing) = endpoint(journal.clone(), vec![]);
            let mut stream = open(&endpoint, cursor.as_deref()).await;
            let mut body = Vec::new();
            if cursor_kind != "known" {
                loop {
                    let frame = stream.next().await.unwrap();
                    let bootstrap = frame.event.as_deref() == Some("bootstrap");
                    body.push(frame);
                    if bootstrap {
                        break;
                    }
                }
            }
            if cursor_kind == "unknown" {
                // Receiving the bootstrap cursor must already establish factual
                // middleware state, even if the client disconnects immediately.
                drop(stream);
                stream = open(&endpoint, Some(&prefix.id().to_string())).await;
            }
            // Later facts must not leak into the initial snapshot.
            let closed = append(
                journal.as_ref(),
                system.into(),
                middleware(
                    breaker,
                    421,
                    MiddlewareFact::CircuitBreaker(CircuitBreakerFact::StateChanged {
                        from_state: CircuitState::Open,
                        to_state: CircuitState::Closed,
                        timestamp: 421,
                    }),
                ),
            )
            .await;
            let terminal = append(
                journal.as_ref(),
                system.into(),
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
            )
            .await;
            closing.send(true).unwrap();
            body.extend(
                tokio::time::timeout(Duration::from_secs(2), stream.collect::<Vec<_>>())
                    .await
                    .expect("post-cut facts drain before shutdown"),
            );
            let snapshots = frames(&body, "middleware_state_snapshot");
            assert_eq!(
                snapshots.len(),
                usize::from(cursor_kind != "known"),
                "disk={disk}, cursor={cursor_kind}"
            );
            if let Some(snapshot) = snapshots.first() {
                assert!(snapshot.id.is_none());
                let payload = frame_payload(snapshot);
                let members = payload["middleware"].as_array().unwrap();
                assert_eq!(members.len(), 2);
                let cb = members
                    .iter()
                    .find(|entry| entry["stage_id"] == breaker.to_string())
                    .unwrap();
                let rl = members
                    .iter()
                    .find(|entry| entry["stage_id"] == limiter.to_string())
                    .unwrap();
                assert_eq!(cb["circuit_breaker"]["state"], "open");
                assert_eq!(cb["circuit_breaker"]["revision"], 420);
                assert_eq!(
                    cb["circuit_breaker"]["state_updated_at_ms"],
                    opened.envelope.provenance.event.timestamp
                );
                assert_eq!(rl["rate_limiter"]["mode"], "limiting");
                assert_eq!(rl["rate_limiter"]["revision"], 7);
                assert_eq!(
                    payload["vector_clock"],
                    serde_json::to_value(&prefix.envelope.provenance.journal.vector_clock).unwrap()
                );
                let snapshot_index = body
                    .iter()
                    .position(|frame| frame.event.as_deref() == Some("middleware_state_snapshot"))
                    .unwrap();
                let closed_index = body
                    .iter()
                    .position(|frame| frame.id == Some(closed.id().to_string()))
                    .unwrap();
                let bootstrap_index = body
                    .iter()
                    .position(|frame| frame.event.as_deref() == Some("bootstrap"))
                    .unwrap();
                assert!(snapshot_index < bootstrap_index);
                assert!(snapshot_index < closed_index);
            }
            let facts = frames(&body, "middleware_lifecycle");
            assert_eq!(facts.len(), 1);
            assert_eq!(frame_payload(facts[0])["state_to"], "closed");
            assert_eq!(frame_payload(facts[0])["revision"], 421);
            let mut expected_ids = Vec::new();
            if cursor_kind != "known" {
                expected_ids.push(prefix.id().to_string());
            }
            expected_ids.extend([closed.id().to_string(), terminal.id().to_string()]);
            assert_eq!(
                body.iter()
                    .filter_map(|frame| frame.id.clone())
                    .collect::<Vec<_>>(),
                expected_ids
            );
            let errors = frames(&body, "error");
            match cursor_kind {
                "unknown" => assert_eq!(
                    frame_payload(errors[0])["error_type"],
                    "journal_resume_not_found"
                ),
                "malformed" => assert_eq!(
                    frame_payload(errors[0])["error_type"],
                    "invalid_last_event_id"
                ),
                _ => assert!(errors.is_empty()),
            }
            assert_eq!(
                body.last().unwrap().event.as_deref(),
                Some("server_shutdown")
            );
        }
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
    assert_eq!(
        body.next().await.unwrap().event.as_deref(),
        Some("bootstrap")
    );
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
async fn source_middleware_transitions_survive_unread_stream_and_reconnect() {
    use obzenflow_adapters::middleware::{rate_limit_with_burst, CircuitBreaker};
    use obzenflow_core::TypedPayload;
    use obzenflow_dsl::{flow, sink, source, FlowDefinition};
    use obzenflow_runtime::run_context::FlowBuildContext;
    use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
    use obzenflow_runtime::stages::sink::SinkTyped;
    use obzenflow_runtime::stages::SourceError;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Item(usize);
    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "studio.source_middleware";
    }

    #[derive(Clone, Debug)]
    struct RecoveringSource(usize);
    impl TypedFiniteSourceHandler for RecoveringSource {
        type Output = Item;

        fn next(&mut self) -> Result<Option<Vec<Item>>, SourceError> {
            let attempt = self.0;
            self.0 += 1;
            match attempt {
                1 => Err(SourceError::Timeout("open the source breaker".into())),
                0..=1000 => Ok(Some(vec![Item(attempt)])),
                _ => Ok(None),
            }
        }
    }

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().to_path_buf();
    let definition = FlowDefinition::materialize(move |_| {
        let input_handler = RecoveringSource(0);
        let output_handler = SinkTyped::new(|_: Item| async {}).idempotent();
        Ok(flow! {
            name: "source_middleware_studio",
            journals: crate::journal::disk_journals(path.clone()),
            stages: {
                input = source!(Item => input_handler with [
                    CircuitBreaker::builder()
                        .count_window(2)
                        .minimum_calls(2)
                        .failure_rate_threshold(0.5)
                        .open_for(Duration::from_millis(1))
                        .build()
                        .unwrap(),
                    // The initial burst funds all 1,000 admissions. Their
                    // utilisation produces a mode change without a long wait.
                    rate_limit_with_burst(1.0, 2000.0)
                ]);
                output = sink!(Item => output_handler);
            },
            topology: { input |> output; }
        })
    });
    // No metrics exporter or observation source participates in delivery.
    let handle = definition
        .build(FlowBuildContext::for_tests())
        .await
        .unwrap();
    let topology = handle.topology().unwrap();
    let input = topology
        .stages()
        .find(|stage| stage.name == "input")
        .unwrap();
    let config = input
        .middleware
        .as_ref()
        .unwrap()
        .circuit_breaker
        .as_ref()
        .unwrap();
    assert_eq!(
        config.cooldown_ms, 1,
        "the real factory snapshot reaches topology"
    );
    let journal = handle.system_journal().unwrap();
    let (endpoint, closing) = endpoint(journal.clone(), vec![]);
    let endpoint = endpoint.with_observation_interval(Duration::from_secs(3600));
    let mut stream = open(&endpoint, None).await;
    while stream.next().await.unwrap().event.as_deref() != Some("bootstrap") {}

    // Do not read Studio again until the breaker has opened and recovered.
    // All transitions must survive independently of measurement deadlines.
    tokio::time::timeout(Duration::from_secs(15), handle.run())
        .await
        .expect("source finishes while Studio is unread")
        .unwrap();
    closing.send(true).unwrap();
    let body: Vec<_> = tokio::time::timeout(Duration::from_secs(2), stream.collect())
        .await
        .unwrap();
    let changes = frames(&body, "middleware_lifecycle");
    let payloads: Vec<_> = changes.iter().map(|frame| frame_payload(frame)).collect();
    let breaker_states: Vec<_> = payloads
        .iter()
        .filter_map(|payload| payload["state_to"].as_str())
        .collect();
    assert_eq!(breaker_states, ["open", "half_open", "closed"]);
    let opened = payloads
        .iter()
        .find(|payload| payload["state_to"] == "open")
        .unwrap();
    assert_eq!(
        opened["context"]["cooldown_ms"], 1,
        "the effective cooldown survives the source journal, system mirror, and SSE"
    );
    let limiter = payloads
        .iter()
        .find(|payload| payload["middleware"] == "rate_limiter")
        .expect("source limiter mode change reaches Studio");
    assert_eq!(limiter["mode_from"], "normal");
    assert_eq!(limiter["mode_to"], "limiting");
    assert_eq!(changes.len(), 4, "each transition is delivered once");
    assert!(payloads.iter().all(|payload| {
        payload["revision"].is_u64()
            && payload["origin"].is_object()
            && payload.get("capture").is_none()
    }));
    assert!(payloads.windows(2).all(|pair| {
        pair[0]["revision"].as_u64().unwrap() < pair[1]["revision"].as_u64().unwrap()
    }));

    let recorded: Vec<_> = journal
        .read_all_unordered()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| matches!(record.payload, SystemPayload::MiddlewareLifecycle { .. }))
        .collect();
    let ids: Vec<_> = changes.iter().map(|frame| frame.id.clone()).collect();
    assert_eq!(
        ids,
        recorded
            .iter()
            .map(|record| Some(record.id().to_string()))
            .collect::<Vec<_>>()
    );

    // A reconnect after Opened must still receive HalfOpen and Closed.
    let cursor = EventId::from_string(changes[0].id.as_deref().unwrap()).unwrap();
    let resumed = request_body(journal.clone(), vec![], Some(cursor)).await;
    assert_eq!(
        frames(&resumed, "middleware_lifecycle")
            .iter()
            .map(|frame| frame.id.clone())
            .collect::<Vec<_>>(),
        ids[1..]
    );

    // A new viewer receives the current factual state without replaying it.
    let fresh = request_body(journal, vec![], None).await;
    let snapshot = frame_payload(frames(&fresh, "middleware_state_snapshot")[0]);
    let middleware = snapshot["middleware"].as_array().unwrap();
    assert_eq!(middleware.len(), 1);
    assert_eq!(middleware[0]["circuit_breaker"]["state"], "closed");
    assert_eq!(middleware[0]["rate_limiter"]["mode"], "limiting");
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
