// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::super::*;
use crate::journal::MemoryJournal;
use futures::StreamExt;
use obzenflow_adapters::studio::ContractBoundaryAliases;
use obzenflow_core::composite::CompositeDefinition;
use obzenflow_core::event::journal_record::{ChainJournalRecord, SystemJournalRecord};
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, StageLifecycleFact};
use obzenflow_core::event::payloads::system_payload::{
    MetricsCoordinationEvent as MetricsFact, SystemPayload as SystemFact,
};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEvent, ChainEventFactory, ChainPayload};
use obzenflow_core::event::{PipelineLifecycleEvent, SystemPayload, WriterId};
use obzenflow_core::id::{CompositeId, JournalId, RoleId, SystemId};
use obzenflow_runtime::stages::sink::{InlineSink, SinkDescription, SinkWriteFailure};

use obzenflow_core::journal::AppendOptions;
use obzenflow_core::journal::{JournalError, JournalReader};
use obzenflow_core::{web::SseFrame, EventId, FlowId, JournalOwner, StageId};
use std::sync::atomic::{AtomicUsize, Ordering};
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
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
                    stage_count: Some(1),
                }),
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
        // Leave one full reader polling interval before the observation deadline.
        tokio::time::advance(Duration::from_millis(interval_ms - 20)).await;
        let fact = append(
            journal.as_ref(),
            system.into(),
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Starting),
        )
        .await;
        assert_eq!(fast.next().await.unwrap().id, Some(cursor(&fact)));
        assert!(
            futures::poll!(fast.next()).is_pending(),
            "live observations cannot bypass the observation deadline"
        );
        publish(9);
        observations.offer(edge(9));
        tokio::time::advance(Duration::from_millis(20)).await;
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
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Starting),
        );
        attached.envelope.observability = Some(edge(10));
        let attached = journal.append(attached, Default::default()).await.unwrap();
        assert_eq!(fast.next().await.unwrap().id, Some(cursor(&attached)));
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

async fn append_stage(
    journal: &dyn Journal<ChainEvent>,
    stage: StageId,
    event: ExecutionPayload,
) -> ChainJournalRecord {
    journal
        .append(
            ChainEventFactory::execution_event(stage.into(), event)
                .with_flow_context(FlowContext::new(stage.to_string(), stage)),
            Default::default(),
        )
        .await
        .unwrap()
}

#[derive(Clone)]
struct TestJournals {
    system: Arc<dyn Journal<SystemEvent>>,
    stages: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
}
impl<T: Journal<SystemEvent> + 'static> From<Arc<T>> for TestJournals {
    fn from(system: Arc<T>) -> Self {
        Self {
            system,
            stages: Vec::new(),
        }
    }
}
impl From<Arc<dyn Journal<SystemEvent>>> for TestJournals {
    fn from(system: Arc<dyn Journal<SystemEvent>>) -> Self {
        Self {
            system,
            stages: Vec::new(),
        }
    }
}
impl TestJournals {
    fn new(system: SystemId, stages: &[StageId]) -> Self {
        Self {
            system: Arc::new(MemoryJournal::with_owner(JournalOwner::system(system))),
            stages: stages
                .iter()
                .map(|stage| {
                    (
                        *stage,
                        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(*stage)))
                            as Arc<dyn Journal<ChainEvent>>,
                    )
                })
                .collect(),
        }
    }
    fn stage(&self, id: StageId) -> &dyn Journal<ChainEvent> {
        self.stages
            .iter()
            .find(|(stage, _)| *stage == id)
            .unwrap()
            .1
            .as_ref()
    }
    async fn checkpoint(&self) -> String {
        let mut positions = std::collections::BTreeMap::new();
        positions.insert(
            *self.system.id(),
            self.system.committed_position().await.unwrap(),
        );
        for (_, stage) in &self.stages {
            positions.insert(*stage.id(), stage.committed_position().await.unwrap());
        }
        positions.retain(|_, position| *position != 0);
        format!("jr1:{}", serde_json::to_string(&positions).unwrap())
    }
    async fn cursor_for(&self, id: EventId) -> Option<String> {
        if let Some(record) = self.system.read_event(&id).await.unwrap() {
            return Some(cursor(&record));
        }
        for (_, stage) in &self.stages {
            if stage.read_event(&id).await.unwrap().is_some() {
                return Some(self.checkpoint().await);
            }
        }
        None
    }
}

fn endpoint(
    journal: impl Into<TestJournals>,
    definitions: Vec<CompositeDefinition>,
) -> (StudioUpdatesEndpoint, watch::Sender<bool>) {
    let journal = journal.into();
    let (closing, receiver) = watch::channel(false);
    (
        StudioUpdatesEndpoint::new(
            journal.system.clone(),
            StudioProjection::new(definitions, ContractBoundaryAliases::default()).unwrap(),
            Some(RuntimeInstanceId::new()),
            receiver,
        )
        .with_live_journals(journal.stages, vec![journal.system]),
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
    // Correctness is delivery through the committed terminal prefix. Nextest's
    // test watchdog bounds hangs; disk catch-up has no two-second contract.
    body.collect().await
}

pub(super) fn cursor<P: obzenflow_core::event::payloads::JournalPayload>(
    row: &obzenflow_core::JournalRecord<P>,
) -> String {
    format!(
        "jr1:{}",
        serde_json::to_string(&std::collections::BTreeMap::from([(
            *row.envelope
                .provenance
                .journal
                .journal_writer_id
                .as_journal_id(),
            row.local_sequence()
        )]))
        .unwrap()
    )
}

async fn request_body(
    journal: impl Into<TestJournals>,
    definitions: Vec<CompositeDefinition>,
    last_event_id: Option<EventId>,
) -> Vec<SseFrame> {
    let journal = journal.into();
    let checkpoint = match last_event_id {
        Some(id) => Some(match journal.cursor_for(id).await {
            Some(cursor) => cursor,
            None => format!(
                "jr1:{}",
                serde_json::json!({obzenflow_core::JournalId::new().to_string(): 1})
            ),
        }),
        None => None,
    };
    let (endpoint, closing) = endpoint(journal, definitions);
    collect_closing(&endpoint, closing, checkpoint.as_deref()).await
}

pub(super) fn frames<'a>(body: &'a [SseFrame], event_name: &str) -> Vec<&'a SseFrame> {
    body.iter()
        .filter(|frame| frame.event.as_deref() == Some(event_name))
        .collect()
}

pub(super) fn frame_payload(frame: &SseFrame) -> serde_json::Value {
    serde_json::from_str(&frame.data).expect("SSE data is JSON")
}

async fn completed_tape() -> (TestJournals, Vec<CompositeDefinition>, EventId) {
    let system = SystemId::new();
    let left = StageId::new();
    let right = StageId::new();
    let journals = TestJournals::new(system, &[left, right]);
    append_stage(
        journals.stage(left),
        left,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: left }),
    )
    .await;
    append_stage(
        journals.stage(left),
        left,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Completed {
            stage_id: left,
            accounting: None,
        }),
    )
    .await;
    let terminal = append_stage(
        journals.stage(right),
        right,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Drained {
            stage_id: right,
            events_processed: None,
        }),
    )
    .await;
    append(
        journals.system.as_ref(),
        system.into(),
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
    )
    .await;
    (journals, definition(left, right), *terminal.id())
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
    assert_eq!(
        resumed_payload["as_of_event_id"],
        frame_payload(fresh_status[0])["as_of_event_id"]
    );
    assert!(resumed_status[0].id.is_none());

    let missing = request_body(journal, definitions, Some(EventId::new())).await;
    let errors = frames(&missing, "error");
    assert!(errors
        .iter()
        .any(|frame| { frame_payload(frame)["error_type"] == "invalid_last_event_id" }));
    let missing_status = frames(&missing, "composite_status");
    assert_eq!(missing_status.len(), 1);
    assert_eq!(frame_payload(missing_status[0])["status"], "completed");
    assert_eq!(frames(&missing, "bootstrap").len(), 1);
}

#[tokio::test]
async fn reconnect_after_fact_recovers_measurements_only_after_factual_catch_up() {
    use obzenflow_core::event::observability::*;
    use obzenflow_core::event::payloads::execution_payload::{
        CircuitBreakerFact, CircuitBreakerOpenTrigger, CircuitState,
    };
    use obzenflow_runtime::metrics::observations::LatestObservationMap;

    let system = SystemId::new();
    let stage = StageId::new();
    let writer = WriterId::from(system);
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system)));
    let stage_journal = Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage)));
    let prefix = append_stage(
        stage_journal.as_ref(),
        stage,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: stage }),
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
    let mut opened = ChainEventFactory::execution_event(
        stage.into(),
        ExecutionPayload::CircuitBreaker(CircuitBreakerFact::Opened {
            cooldown_ms: 5_000,
            error_rate: 1.0,
            failure_count: 3,
            trigger: CircuitBreakerOpenTrigger::ConsecutiveFailures,
            observed_calls: 3,
            slow_call_rate: None,
            slow_call_count: None,
            last_error: None,
        }),
    )
    .with_flow_context(FlowContext::new("worker", stage));
    opened.envelope.observability = Some(sample(3));
    let opened = stage_journal
        .append(opened, Default::default())
        .await
        .unwrap();
    source.offer(sample(11));
    let (closing, receiver) = watch::channel(false);
    let endpoint = StudioUpdatesEndpoint::new(
        journal.clone(),
        StudioProjection::new(vec![], ContractBoundaryAliases::default())
            .unwrap()
            .with_observations(source),
        Some(RuntimeInstanceId::new()),
        receiver,
    )
    .with_live_journals(vec![(stage, stage_journal)], vec![journal.clone()]);
    let mut first = open(&endpoint, Some(&cursor(&prefix))).await;
    let fact = tokio::time::timeout(Duration::from_secs(2), first.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        frame_payload(&fact)["commitment"]["event_id"],
        opened.id().to_string()
    );
    assert_eq!(frame_payload(&fact)["revision"], opened.local_sequence());
    assert_eq!(frame_payload(&fact)["context"]["cooldown_ms"], 5_000);
    drop(first); // Disconnect before the optional frame belonging to this fact.

    let terminal = append(
        journal.as_ref(),
        writer,
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
    )
    .await;
    let resumed = collect_closing(&endpoint, closing, Some(&cursor(&opened))).await;
    let terminal_index = resumed
        .iter()
        .position(|frame| {
            frame_payload(frame)["commitment"]["event_id"] == terminal.id().to_string()
        })
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
    events: Vec<StageLifecycleFact>,
    definitions: Vec<CompositeDefinition>,
) -> Vec<SseFrame> {
    let system = SystemId::new();
    let stages: std::collections::BTreeSet<_> =
        events.iter().map(StageLifecycleFact::stage_id).collect();
    let journals = TestJournals::new(system, &stages.into_iter().collect::<Vec<_>>());
    for event in events {
        append_stage(
            journals.stage(event.stage_id()),
            event.stage_id(),
            ExecutionPayload::StageLifecycle(event),
        )
        .await;
    }
    append(
        journals.system.as_ref(),
        system.into(),
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
    )
    .await;
    request_body(journals, definitions, None).await
}

#[tokio::test]
async fn clean_cancellation_and_contradictory_history_surface_at_the_route() {
    let left = StageId::new();
    let right = StageId::new();
    // Independent child journals have no shared append order. This shutdown
    // has one cause; Core separately tests retaining the first applied reason.
    let cancelled = terminal_projection_body(
        vec![
            StageLifecycleFact::Cancelled {
                stage_id: left,
                reason: "operator stop".to_string(),
                accounting: None,
            },
            StageLifecycleFact::Cancelled {
                stage_id: right,
                reason: "operator stop".to_string(),
                accounting: None,
            },
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
            StageLifecycleFact::Completed {
                stage_id: left,
                accounting: None,
            },
            StageLifecycleFact::Cancelled {
                stage_id: left,
                reason: "late contradiction".to_string(),
                accounting: None,
            },
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
    pending_read_from: usize,
    reads: Arc<AtomicUsize>,
    events: Vec<SystemJournalRecord>,
    position: usize,
    fail_at: Option<usize>,
    dropped: Arc<tokio::sync::Notify>,
}

impl Drop for ScriptedReader {
    fn drop(&mut self) {
        self.dropped.notify_one();
    }
}

#[async_trait]
impl obzenflow_core::journal::JournalStorageReader<SystemEvent> for ScriptedReader {
    async fn storage_next(&mut self) -> Result<Option<SystemJournalRecord>, JournalError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Some(probe) = self
            .pending_read
            .as_ref()
            .filter(|_| self.position >= self.pending_read_from)
        {
            let _guard = PendingOpenGuard(probe.clone());
            probe.entered.notify_one();
            probe.release.notified().await;
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

    fn storage_position(&self) -> u64 {
        self.position as u64
    }

    fn storage_initial_prefix_complete(&self) -> Result<bool, JournalError> {
        Ok(self.position >= self.events.len())
    }

    fn storage_is_at_end(&self) -> bool {
        self.position >= self.events.len()
    }
}

#[derive(Default)]
struct PendingOpen {
    entered: tokio::sync::Notify,
    dropped: tokio::sync::Notify,
    release: tokio::sync::Notify,
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
    probe.dropped.notified().await;
}

pub(crate) struct ScriptedJournal {
    pending_read: Option<Arc<PendingOpen>>,
    pending_read_from: usize,
    pub(crate) reads: Arc<AtomicUsize>,
    pub(crate) opens: Arc<AtomicUsize>,
    inner: MemoryJournal<SystemEvent>,
    fail_at: Option<usize>,
    fail_open: bool,
    pending_open: Option<Arc<PendingOpen>>,
    reader_dropped: Arc<tokio::sync::Notify>,
    reader_opened: tokio::sync::Notify,
}

impl ScriptedJournal {
    pub(crate) fn new(system_id: SystemId) -> Self {
        Self {
            reads: Arc::new(AtomicUsize::new(0)),
            opens: Arc::new(AtomicUsize::new(0)),
            pending_read: None,
            pending_read_from: 0,
            inner: MemoryJournal::with_owner(JournalOwner::system(system_id)),
            fail_at: None,
            fail_open: false,
            pending_open: None,
            reader_dropped: Arc::new(tokio::sync::Notify::new()),
            reader_opened: tokio::sync::Notify::new(),
        }
    }

    async fn open_reader(&self, position: u64) -> Result<Box<ScriptedReader>, JournalError> {
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
            pending_read_from: self.pending_read_from,
            reads: self.reads.clone(),
            events: self.inner.read_all_unordered().await?,
            position: position as usize,
            fail_at: self.fail_at,
            dropped: self.reader_dropped.clone(),
        });
        self.reader_opened.notify_one();
        Ok(reader)
    }
}

#[async_trait]
impl obzenflow_core::journal::JournalStorage<SystemEvent> for ScriptedJournal {
    fn storage_id(&self) -> &JournalId {
        self.inner.id()
    }

    fn storage_owner(&self) -> Option<&JournalOwner> {
        self.inner.owner()
    }

    async fn storage_append(
        &self,
        event: SystemEvent,
        options: AppendOptions<SystemEvent>,
    ) -> Result<SystemJournalRecord, JournalError> {
        self.inner.append(event, options).await
    }

    async fn storage_read_all_unordered(&self) -> Result<Vec<SystemJournalRecord>, JournalError> {
        self.inner.read_all_unordered().await
    }

    async fn storage_read_event(
        &self,
        event_id: &EventId,
    ) -> Result<Option<SystemJournalRecord>, JournalError> {
        self.inner.read_event(event_id).await
    }

    async fn storage_reader_from(
        &self,
        position: u64,
    ) -> Result<Box<dyn JournalReader<SystemEvent>>, JournalError> {
        Ok(self.open_reader(position).await?)
    }

    async fn storage_read_last_n(
        &self,
        count: usize,
    ) -> Result<Vec<SystemJournalRecord>, JournalError> {
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
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
            stage_count: Some(1),
        }),
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
    reader_dropped.notified().await;
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
    let last_id = journal.checkpoint().await;
    for cursor in [
        String::new(),
        "malformed".into(),
        EventId::new().to_string(),
        format!(
            "jr1:{}",
            serde_json::to_string(&std::collections::BTreeMap::from([(
                *journal.system.id(),
                u64::MAX
            )]))
            .unwrap()
        ),
        format!(
            "jr1:{}",
            serde_json::to_string(&std::collections::BTreeMap::from([(
                JournalId::new(),
                1u64
            )]))
            .unwrap()
        ),
    ] {
        let (endpoint, closing) = endpoint(journal.clone(), definitions.clone());
        let body = collect_closing(&endpoint, closing, Some(&cursor)).await;
        let error = frame_payload(&body[0]);
        assert_eq!(error["error_type"], "invalid_last_event_id");
        assert_eq!(error["recoverable"], false);
        assert!(body[0].id.is_none());
        let bootstrap = frames(&body, "bootstrap")[0];
        assert_eq!(bootstrap.id.as_deref(), Some(last_id.as_str()));
        assert!(frame_payload(bootstrap)["checkpoint_event_id"].is_null());
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
        CircuitBreakerFact, CircuitBreakerOpenTrigger, CircuitState, RateLimiterFact,
        RateLimiterMode,
    };

    for disk in [false, true] {
        for cursor_kind in ["unknown", "fresh", "malformed", "known"] {
            let directory = tempfile::tempdir().unwrap();
            let system = SystemId::new();
            let breaker = StageId::new();
            let limiter = StageId::new();
            let stage_journal = |stage: StageId, name: &str| -> Arc<dyn Journal<ChainEvent>> {
                if disk {
                    Arc::new(
                        crate::journal::disk::DiskJournal::with_owner(
                            directory.path().join(name),
                            JournalOwner::stage(stage),
                        )
                        .unwrap(),
                    )
                } else {
                    Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage)))
                }
            };
            let journals = TestJournals {
                system: Arc::new(MemoryJournal::with_owner(JournalOwner::system(system))),
                stages: vec![
                    (breaker, stage_journal(breaker, "breaker.log")),
                    (limiter, stage_journal(limiter, "limiter.log")),
                ],
            };
            let opened = append_stage(
                journals.stage(breaker),
                breaker,
                ExecutionPayload::CircuitBreaker(CircuitBreakerFact::Opened {
                    cooldown_ms: 5_000,
                    error_rate: 1.0,
                    failure_count: 3,
                    trigger: CircuitBreakerOpenTrigger::ConsecutiveFailures,
                    observed_calls: 3,
                    slow_call_rate: None,
                    slow_call_count: None,
                    last_error: None,
                }),
            )
            .await;
            let prefix = append_stage(
                journals.stage(limiter),
                limiter,
                ExecutionPayload::RateLimiter(RateLimiterFact::ModeChange {
                    mode_from: RateLimiterMode::Normal,
                    mode_to: RateLimiterMode::Limiting,
                    limit_rate: 10.0,
                }),
            )
            .await;
            let prefix_cursor = journals.checkpoint().await;
            let resume_cursor = match cursor_kind {
                "unknown" => Some(EventId::new().to_string()),
                "fresh" => None,
                "malformed" => Some("malformed".to_string()),
                "known" => Some(prefix_cursor.clone()),
                _ => unreachable!(),
            };
            let (endpoint, closing) = endpoint(journals.clone(), vec![]);
            let mut stream = open(&endpoint, resume_cursor.as_deref()).await;
            let mut body = Vec::new();
            if cursor_kind != "known" {
                loop {
                    let frame = stream.next().await.unwrap_or_else(|| {
                        panic!("stream ended before bootstrap: disk={disk}, cursor={cursor_kind}")
                    });
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
                stream = open(&endpoint, Some(&prefix_cursor)).await;
            }
            // Later facts must not leak into the initial snapshot.
            let closed = append_stage(
                journals.stage(breaker),
                breaker,
                ExecutionPayload::CircuitBreaker(CircuitBreakerFact::StateChanged {
                    from_state: CircuitState::Open,
                    to_state: CircuitState::Closed,
                    timestamp: 421,
                }),
            )
            .await;
            let terminal = append(
                journals.system.as_ref(),
                system.into(),
                SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
            )
            .await;
            closing.send(true).unwrap();
            body.extend(stream.collect::<Vec<_>>().await);
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
                assert_eq!(cb["circuit_breaker"]["revision"], opened.local_sequence());
                assert_eq!(
                    cb["circuit_breaker"]["state_updated_at_ms"],
                    opened.envelope.provenance.event.processing.event_time
                );
                assert_eq!(rl["rate_limiter"]["mode"], "limiting");
                assert_eq!(rl["rate_limiter"]["revision"], prefix.local_sequence());
                assert!([
                    serde_json::to_value(&prefix.envelope.provenance.journal.vector_clock).unwrap(),
                    serde_json::to_value(&opened.envelope.provenance.journal.vector_clock).unwrap(),
                ].contains(&payload["vector_clock"]), "snapshot carries an observed journal clock; independent readers need no total order");
                let snapshot_index = body
                    .iter()
                    .position(|frame| frame.event.as_deref() == Some("middleware_state_snapshot"))
                    .unwrap();
                let closed_index = body
                    .iter()
                    .position(|frame| {
                        frame_payload(frame)["commitment"]["event_id"] == closed.id().to_string()
                    })
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
            assert_eq!(frame_payload(facts[0])["revision"], closed.local_sequence());
            let ids: Vec<_> = body
                .iter()
                .filter_map(|frame| frame.id.as_deref())
                .collect();
            assert_eq!(ids.len(), 2 + usize::from(cursor_kind != "known"));
            if cursor_kind != "known" {
                assert_eq!(ids[0], prefix_cursor);
            }
            let mut previous: std::collections::BTreeMap<String, u64> =
                serde_json::from_str(prefix_cursor.strip_prefix("jr1:").unwrap()).unwrap();
            for id in ids {
                let current: std::collections::BTreeMap<String, u64> =
                    serde_json::from_str(id.strip_prefix("jr1:").unwrap()).unwrap();
                assert!(previous.iter().all(|(journal, position)| current
                    .get(journal)
                    .is_some_and(|next| next >= position)));
                previous = current;
            }
            assert_eq!(
                format!("jr1:{}", serde_json::to_string(&previous).unwrap()),
                journals.checkpoint().await
            );
            assert!(body
                .iter()
                .any(|frame| frame_payload(frame)["commitment"]["event_id"]
                    == terminal.id().to_string()));
            let errors = frames(&body, "error");
            match cursor_kind {
                "unknown" => assert_eq!(
                    frame_payload(errors[0])["error_type"],
                    "invalid_last_event_id"
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
    probe.dropped.notified().await;
    reader_dropped.notified().await;

    let system = SystemId::new();
    let mut journal = ScriptedJournal::new(system);
    let catch_up = Arc::new(PendingOpen::default());
    journal.pending_read = Some(catch_up.clone());
    journal.pending_read_from = 64;
    let journal = Arc::new(journal);
    for _ in 0..256 {
        append(
            journal.as_ref(),
            WriterId::from(system),
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
                stage_count: Some(1),
            }),
        )
        .await;
    }
    let (endpoint, _closing) = self::endpoint(journal.clone(), vec![]);
    let mut body = open(&endpoint, None).await;
    // Cancel at a witnessed read inside the initial prefix, independent of how
    // many polls or worker turns were needed to reach it.
    tokio::select! {
        _ = body.next() => panic!("bootstrap cannot pass the held prefix read"),
        _ = catch_up.entered.notified() => {}
    }
    let reads = journal.reads.load(Ordering::SeqCst);
    assert_eq!(reads, 65);
    assert!(reads < 256);
    drop(body);
    catch_up.dropped.notified().await;
    journal.reader_dropped.notified().await;
    assert_eq!(journal.reads.load(Ordering::SeqCst), reads);
}

#[tokio::test]
async fn differently_paced_clients_keep_independent_cursors_and_repair_derived_frame_disconnects() {
    let system = SystemId::new();
    let writer = WriterId::from(system);
    let left = StageId::new();
    let right = StageId::new();
    let journal = TestJournals::new(system, &[left, right]);
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

    let running = append_stage(
        journal.stage(left),
        left,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: left }),
    )
    .await;
    let fact = fast.next().await.unwrap();
    assert_eq!(fact.id.as_deref(), None);
    // Disconnect after receiving the stage message and its reconnect ID, but
    // before receiving the accompanying composite status.
    drop(fast);
    let mut resumed = open(&endpoint, Some(&cursor(&running))).await;
    let repaired = resumed.next().await.unwrap();
    assert_eq!(frame_payload(&repaired)["status"], "running");
    assert!(repaired.id.is_none());
    assert_eq!(
        frame_payload(&repaired)["as_of_event_id"],
        running.envelope.provenance.event.id.to_string()
    );

    append_stage(
        journal.stage(left),
        left,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Completed {
            stage_id: left,
            accounting: None,
        }),
    )
    .await;
    append_stage(
        journal.stage(right),
        right,
        ExecutionPayload::StageLifecycle(StageLifecycleFact::Drained {
            stage_id: right,
            events_processed: None,
        }),
    )
    .await;
    append(
        journal.system.as_ref(),
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
        journal.system.read_all_unordered().await.unwrap().len()
            + journal
                .stage(left)
                .read_all_unordered()
                .await
                .unwrap()
                .len()
            + journal
                .stage(right)
                .read_all_unordered()
                .await
                .unwrap()
                .len(),
        4,
        "readers create no facts"
    );
}

#[tokio::test]
async fn source_middleware_transitions_survive_unread_stream_and_reconnect() {
    use obzenflow_adapters::middleware::{circuit_breaker, rate_limit};
    use obzenflow_core::TypedPayload;
    use obzenflow_dsl::{flow, sink, source, FlowDefinition};
    use obzenflow_runtime::run_context::FlowBuildContext;
    use obzenflow_runtime::stages::common::handlers::TypedFiniteSourceHandler;
    use obzenflow_runtime::stages::SourceError;

    let started = std::time::Instant::now();

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
        #[derive(Clone, Debug)]
        struct DiscardOutputHandler {}
        #[async_trait::async_trait]
        impl InlineSink for DiscardOutputHandler {
            type Input = Item;
            fn describe(&self) -> SinkDescription {
                SinkDescription::method(
                    obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                )
                .with_redelivery_safety(
                    obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat,
                )
            }
            async fn write(&mut self, _input: Item) -> Result<(), SinkWriteFailure> {
                Ok(())
            }
        }
        let output_handler = DiscardOutputHandler {};
        Ok(flow! {
            name: "source_middleware_studio",
            journals: crate::journal::disk_journals(path.clone()),
            stages: {
                input = source!(Item => input_handler with {
                    circuit_breaker()
                        .count_window(2)
                        .minimum_calls(2)
                        .failure_rate_threshold(0.5)
                        .open_for(Duration::from_millis(1)),
                    // The initial burst funds all 1,000 admissions. Their
                    // utilisation produces a mode change without a long wait.
                    rate_limit(1.0).burst_capacity(2000.0)
                });
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
        .attachments
        .iter()
        .find(|attachment| {
            attachment.family == obzenflow_topology::MiddlewareFamily::CircuitBreaker
        })
        .unwrap();
    assert_eq!(
        config.configuration["open_for_ms"], 1,
        "the real factory snapshot reaches topology"
    );
    let journal = handle.system_journal().unwrap();
    let stage_journals = handle.stage_journals();
    let system_journals = handle.system_journals();
    let source_journal = stage_journals
        .iter()
        .find(|(id, _)| *id == StageId::from_ulid(input.id.ulid()))
        .unwrap()
        .1
        .clone();
    let (endpoint, closing) = endpoint(journal.clone(), vec![]);
    let endpoint = endpoint
        .with_live_journals(stage_journals.clone(), system_journals.clone())
        .with_observation_interval(Duration::from_secs(3600));
    let mut stream = open(&endpoint, None).await;
    while stream.next().await.unwrap().event.as_deref() != Some("bootstrap") {}

    // Do not read Studio again until the breaker has opened and recovered.
    // All transitions must survive independently of measurement deadlines.
    handle.run().await.unwrap();
    println!(
        "Studio middleware proof: flow settled after {:?}",
        started.elapsed()
    );
    closing.send(true).unwrap();
    let body: Vec<_> = stream.collect().await;
    let changes = frames(&body, "middleware_lifecycle");
    println!(
        "Studio middleware proof: unread stream drained after {:?}",
        started.elapsed()
    );
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
        "the effective cooldown survives the source journal and SSE"
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

    let recorded: Vec<_> = source_journal
        .read_all_unordered()
        .await
        .unwrap()
        .into_iter()
        .filter(|record| {
            matches!(
                record.payload,
                ChainPayload::Execution(
                    ExecutionPayload::CircuitBreaker(_) | ExecutionPayload::RateLimiter(_)
                )
            )
        })
        .map(|record| record.id().to_string())
        .collect();
    let ids: Vec<_> = changes
        .iter()
        .map(|frame| {
            frame_payload(frame)["commitment"]["event_id"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(ids, recorded);
    println!(
        "Studio middleware proof: journal transitions checked after {:?}",
        started.elapsed()
    );
    assert!(
        journal
            .read_all_unordered()
            .await
            .unwrap()
            .iter()
            .all(|record| record.writer_id().as_system().is_some()),
        "pipeline history contains only owner facts"
    );

    // Resume independently in each physical history after the first transition.
    let (resumed_endpoint, resumed_closing) = self::endpoint(journal.clone(), vec![]);
    let resumed_endpoint =
        resumed_endpoint.with_live_journals(stage_journals.clone(), system_journals.clone());
    let resumed =
        collect_closing(&resumed_endpoint, resumed_closing, changes[0].id.as_deref()).await;
    println!(
        "Studio middleware proof: reconnect drained after {:?}",
        started.elapsed()
    );
    assert_eq!(
        frames(&resumed, "middleware_lifecycle")
            .iter()
            .filter(|frame| frame_payload(frame).get("commitment").is_some())
            .map(|frame| frame_payload(frame)["commitment"]["event_id"]
                .as_str()
                .unwrap()
                .to_owned())
            .collect::<Vec<_>>(),
        ids[1..]
    );

    let (fresh_endpoint, fresh_closing) = self::endpoint(journal, vec![]);
    let fresh_endpoint = fresh_endpoint.with_live_journals(stage_journals, system_journals);
    let fresh = collect_closing(&fresh_endpoint, fresh_closing, None).await;
    for (phase, frames) in [("unread", &body), ("resumed", &resumed), ("fresh", &fresh)] {
        assert!(
            self::frames(frames, "error").is_empty(),
            "{phase}: {frames:?}"
        );
        assert_eq!(
            frames.last().unwrap().event.as_deref(),
            Some("server_shutdown"),
            "{phase}: terminal history must drain before the response closes"
        );
    }
    let snapshot = frame_payload(frames(&fresh, "middleware_state_snapshot")[0]);
    let middleware = snapshot["middleware"].as_array().unwrap();
    assert_eq!(middleware.len(), 1);
    assert_eq!(middleware[0]["circuit_breaker"]["state"], "closed");
    assert_eq!(middleware[0]["rate_limiter"]["mode"], "limiting");
    println!(
        "Studio middleware proof: fresh snapshot checked after {:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn terminal_flow_totals_reach_sse_independently_of_metrics_reporting() {
    use obzenflow_adapters::monitoring::MetricsReadModel;
    use obzenflow_core::event::{PipelineLifecycleEvent, SystemPayload};
    use obzenflow_dsl::{flow, sink, source, FlowDefinition};
    use obzenflow_runtime::run_context::FlowBuildContext;

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
            let input =
                obzenflow_adapters::sources::ValuesSource::new(vec![Item(1), Item(2), Item(3)]);
            #[derive(Clone, Debug)]
            struct DiscardOutput {}
            #[async_trait::async_trait]
            impl InlineSink for DiscardOutput {
                type Input = Item;
                fn describe(&self) -> SinkDescription {
                    SinkDescription::method(
                        obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Noop,
                    )
                    .with_redelivery_safety(
                        obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat,
                    )
                }
                async fn write(&mut self, _input: Item) -> Result<(), SinkWriteFailure> {
                    Ok(())
                }
            }
            let output = DiscardOutput {};
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

struct ComposedEvidence(Option<tempfile::TempDir>);
impl ComposedEvidence {
    fn new() -> Self {
        let base = std::env::var_os("OBZENFLOW_TEST_ARTIFACTS")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::path::PathBuf::from("target/studio-test-evidence"));
        std::fs::create_dir_all(&base).unwrap();
        let directory = tempfile::Builder::new()
            .prefix("composed-studio-")
            .tempdir_in(base)
            .unwrap();
        eprintln!("composed artifacts={}", directory.path().display());
        Self(Some(directory))
    }
    fn path(&self) -> &std::path::Path {
        self.0.as_ref().unwrap().path()
    }
}
impl Drop for ComposedEvidence {
    fn drop(&mut self) {
        if std::thread::panicking() {
            let retained = self.0.take().unwrap().keep();
            eprintln!(
                "composed: retained failure journals at {}",
                retained.display()
            );
        }
    }
}

#[derive(Debug)]
struct ComposedProgress {
    disk: bool,
    stages: usize,
    sources: Vec<std::sync::atomic::AtomicU8>,
    sink: AtomicUsize,
    frames: AtomicUsize,
    cursor: std::sync::Mutex<String>,
    waiting: tokio::sync::Notify,
}
impl ComposedProgress {
    fn new(disk: bool, stages: usize) -> Self {
        Self {
            disk,
            stages,
            sources: (0..stages - 1)
                .map(|_| std::sync::atomic::AtomicU8::new(0))
                .collect(),
            sink: AtomicUsize::new(0),
            frames: AtomicUsize::new(0),
            cursor: Default::default(),
            waiting: Default::default(),
        }
    }
    fn snapshot(
        &self,
        state: &tokio::sync::watch::Receiver<obzenflow_runtime::pipeline::PipelineState>,
    ) -> String {
        let mut counts = [0usize; 4];
        let mut unfinished = Vec::new();
        for (index, value) in self.sources.iter().enumerate() {
            let phase = value.load(Ordering::Relaxed) as usize;
            counts[phase] += 1;
            if phase != 3 && unfinished.len() < 16 {
                unfinished.push((index, ["unpolled", "waiting", "emitted", "eof"][phase]));
            }
        }
        let cursor = self
            .cursor
            .try_lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| "<busy>".into());
        format!("pipeline={:?}, source_unpolled={}, source_waiting={}, source_emitted={}, source_eof={}, unfinished_sources_first_16={unfinished:?}, sink_callbacks={}, studio_frames={}, last_cursor_prefix={cursor}",
            *state.borrow(), counts[0], counts[1], counts[2], counts[3],
            self.sink.load(Ordering::Relaxed), self.frames.load(Ordering::Relaxed))
    }
}

async fn composed_wait<F: std::future::Future>(
    phase: &str,
    budget: Duration,
    progress: &ComposedProgress,
    state: &tokio::sync::watch::Receiver<obzenflow_runtime::pipeline::PipelineState>,
    future: F,
) -> Result<F::Output, String> {
    let started = std::time::Instant::now();
    let deadline = tokio::time::sleep(budget);
    let period = Duration::from_secs(5);
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(future, deadline);
    let case = format!(
        "composed backend={}, stages={}, phase={phase}",
        if progress.disk { "disk" } else { "memory" },
        progress.stages
    );
    eprintln!("{case}, budget={budget:?}");
    loop {
        tokio::select! {
            result = &mut future => return Ok(result),
            _ = &mut deadline => return Err(format!("{case} exceeded {budget:?}; elapsed={:?}, {}", started.elapsed(), progress.snapshot(state))),
            _ = heartbeat.tick() => eprintln!("{case}, elapsed={:?}, {}", started.elapsed(), progress.snapshot(state)),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_stage_pipeline_metrics_and_studio_settle_owned_journals() {
    for (disk, stages) in [(false, 2), (false, 10), (false, 100), (true, 10)] {
        composed_case(disk, stages, false).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composed_stall_reports_source_progress_before_recovery() {
    composed_case(false, 2, true).await;
}

async fn composed_case(disk: bool, stages: usize, control_stall: bool) {
    use obzenflow_adapters::monitoring::MetricsReadModel;
    use obzenflow_core::event::CausalCoordinate;
    use obzenflow_dsl::dsl::{composition::IntoFlowMember, topology::AuthoredConnection};
    use obzenflow_dsl::{async_source, sink};
    use obzenflow_runtime::run_context::FlowBuildContext;
    use obzenflow_runtime::stages::common::handlers::TypedAsyncFiniteSourceHandler;
    use obzenflow_runtime::stages::SourceError;
    use std::collections::HashMap;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct Item;
    impl obzenflow_core::TypedPayload for Item {
        const EVENT_TYPE: &'static str = "supervision.scale";
    }
    #[derive(Clone, Debug)]
    struct GatedSource {
        gate: Arc<tokio::sync::Semaphore>,
        emitted: bool,
        index: usize,
        progress: Arc<ComposedProgress>,
    }
    #[async_trait::async_trait]
    impl TypedAsyncFiniteSourceHandler for GatedSource {
        type Output = Item;
        async fn next(&mut self) -> Result<Option<Vec<Item>>, SourceError> {
            if self.emitted {
                self.progress.sources[self.index].store(3, Ordering::Relaxed);
                return Ok(None);
            }
            self.progress.sources[self.index].store(1, Ordering::Relaxed);
            self.progress.waiting.notify_one();
            self.gate.acquire().await.unwrap().forget();
            self.emitted = true;
            self.progress.sources[self.index].store(2, Ordering::Relaxed);
            Ok(Some(vec![Item]))
        }
    }

    // Exercise the ordinary lowering/materialisation path with a generated
    // fan-in, retaining both memory and disk cases and their journal oracles.
    {
        let progress = Arc::new(ComposedProgress::new(disk, stages));
        eprintln!("composed backend={disk}, stages={stages}, phase=build");
        let directory = ComposedEvidence::new();
        let gate = Arc::new(tokio::sync::Semaphore::new(0));
        let mut members = HashMap::new();
        let mut connections = Vec::new();
        for index in 0..stages - 1 {
            let name = format!("source_{index:03}");
            let handler = GatedSource {
                gate: gate.clone(),
                emitted: false,
                index,
                progress: progress.clone(),
            };
            let mut descriptor = async_source!(Item => handler);
            descriptor.set_name(name.clone());
            members.insert(name.clone(), descriptor.into_flow_member());
            connections.push(AuthoredConnection::edge(
                name,
                "output",
                obzenflow_topology::EdgeKind::Forward,
            ));
        }
        let sink_progress = progress.clone();
        #[derive(Clone)]
        struct CountComposedDeliveries {
            progress: Arc<ComposedProgress>,
        }
        impl std::fmt::Debug for CountComposedDeliveries {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("CountComposedDeliveries")
            }
        }
        #[async_trait::async_trait]
        impl InlineSink for CountComposedDeliveries {
            type Input = Item;
            fn describe(&self) -> SinkDescription {
                SinkDescription::method(
                    obzenflow_core::event::payloads::delivery_payload::DeliveryMethod::Custom(
                        "test_observer".into(),
                    ),
                )
                .with_redelivery_safety(
                    obzenflow_runtime::effects::SinkRedeliverySafety::SafeToRepeat,
                )
            }
            async fn write(&mut self, _input: Item) -> Result<(), SinkWriteFailure> {
                self.progress.sink.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
        }
        let output_handler = CountComposedDeliveries {
            progress: sink_progress,
        };
        let mut output = sink!(Item => output_handler);
        output.set_name("output".into());
        members.insert("output".into(), output.into_flow_member());
        let lowered =
            obzenflow_dsl::dsl::composites::lower_composites(members, connections).unwrap();
        let model = Arc::new(MetricsReadModel::default());
        let context = FlowBuildContext::for_tests().with_metrics_exporter(model);
        let built = if disk {
            obzenflow_dsl::dsl::flow_builder::build_flow(
                "supervision_scale",
                crate::journal::disk_journals(directory.path().to_owned()),
                lowered,
                context,
                None,
            )
            .await
        } else {
            obzenflow_dsl::dsl::flow_builder::build_flow(
                "supervision_scale",
                crate::journal::memory_journals(),
                lowered,
                context,
                None,
            )
            .await
        }
        .unwrap();
        let handle = built.into_handle();
        let state = handle.state_receiver();
        let pipeline = handle.system_journal().unwrap();
        let journals = handle.stage_journals();
        let metrics = handle.metrics_journals().unwrap();
        let (endpoint, closing) = endpoint(pipeline.clone(), vec![]);
        let endpoint = endpoint.with_live_journals(journals.clone(), handle.system_journals());
        let mut stream = composed_wait(
            "open-studio",
            Duration::from_secs(5),
            &progress,
            &state,
            open(&endpoint, None),
        )
        .await
        .unwrap_or_else(|diagnostic| panic!("{diagnostic}"));
        composed_wait(
            "bootstrap",
            Duration::from_secs(5),
            &progress,
            &state,
            async { while stream.next().await.unwrap().event.as_deref() != Some("bootstrap") {} },
        )
        .await
        .unwrap_or_else(|diagnostic| panic!("{diagnostic}"));
        let started = std::time::Instant::now();
        let delivery_progress = progress.clone();
        let delivery = tokio::spawn(async move {
            stream
                .inspect(|frame| {
                    delivery_progress.frames.fetch_add(1, Ordering::Relaxed);
                    if let Some(cursor) = &frame.id {
                        // This fixture-owned lock is never held over I/O. Failure
                        // capture uses try_lock and never waits for the consumer.
                        *delivery_progress.cursor.lock().unwrap() =
                            cursor.chars().take(2048).collect();
                    }
                })
                .collect::<Vec<_>>()
                .await
        });
        // Existing hang budgets remain; performance/capacity acceptance belongs
        // to 145i's separate measured workloads, not this elapsed-time guard.
        let run_budget = Duration::from_secs(if stages == 100 { 60 } else { 15 });
        let run = handle.run();
        tokio::pin!(run);
        if control_stall {
            tokio::time::timeout(Duration::from_secs(5), async {
                tokio::select! {
                    _ = progress.waiting.notified() => {},
                    result = &mut run => panic!("flow settled before controlled source wait: {result:?}"),
                }
            }).await.expect("the source must reach its real gate");
            let diagnostic = composed_wait(
                "run",
                Duration::from_millis(20),
                &progress,
                &state,
                &mut run,
            )
            .await
            .expect_err("an unreleased source cannot settle");
            assert!(diagnostic.contains("phase=run"), "{diagnostic}");
            assert!(diagnostic.contains("source_waiting=1"), "{diagnostic}");
            assert!(diagnostic.contains("sink_callbacks=0"), "{diagnostic}");
            assert!(diagnostic.contains("pipeline="), "{diagnostic}");
            eprintln!("controlled stall: {diagnostic}");
        }
        gate.add_permits(stages - 1);
        composed_wait("run", run_budget, &progress, &state, &mut run)
            .await
            .unwrap_or_else(|diagnostic| panic!("{diagnostic}"))
            .unwrap();
        let settled = started.elapsed();
        closing.send(true).unwrap();
        eprintln!("composed backend={disk}, stages={stages}, phase=studio, settlement={settled:?}");
        let frames = composed_wait(
            "studio",
            Duration::from_secs(5),
            &progress,
            &state,
            delivery,
        )
        .await
        .unwrap_or_else(|diagnostic| panic!("{diagnostic}"))
        .unwrap();
        assert_eq!(progress.sink.load(Ordering::Relaxed), stages - 1);
        assert_eq!(
            frames.last().unwrap().event.as_deref(),
            Some("server_shutdown")
        );
        let history = pipeline.read_all_unordered().await.unwrap();
        let owner = match pipeline.owner().unwrap() {
            JournalOwner::System { system_id } => WriterId::from(*system_id),
            _ => panic!("pipeline owner"),
        };
        assert!(
            history.iter().all(|row| *row.writer_id() == owner),
            "children cannot write system.log"
        );
        let terminal = history.last().unwrap();
        assert!(matches!(
            &terminal.payload,
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained)
        ));
        let delivered: std::collections::HashSet<_> = frames
            .iter()
            .filter_map(|frame| {
                let payload = frame_payload(frame);
                payload["commitment"]["event_id"]
                    .as_str()
                    .map(str::to_owned)
            })
            .collect();
        let mut completion_latencies = Vec::new();
        for (stage, journal) in &journals {
            let rows = journal.read_all_unordered().await.unwrap();
            let completed = rows
                .iter()
                .filter(|row| row.writer_id().as_stage() == Some(stage))
                .find(|row| {
                    matches!(
                        row.payload,
                        ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                            StageLifecycleFact::Completed { .. }
                        ))
                    )
                })
                .unwrap();
            assert!(
                delivered.contains(&completed.id().to_string()),
                "Studio must deliver every stage's committed completion"
            );
            let position = journal.committed_position().await.unwrap();
            assert!(
                terminal
                    .envelope
                    .provenance
                    .journal
                    .vector_clock
                    .get(&CausalCoordinate::new((*journal.id()).into()))
                    >= position,
                "pipeline drained inherits the joined child completion frontier"
            );
            completion_latencies.push(
                (terminal.envelope.provenance.journal.timestamp
                    - completed.envelope.provenance.journal.timestamp)
                    .num_microseconds()
                    .unwrap(),
            );
        }
        assert_eq!(completion_latencies.len(), stages);
        completion_latencies.sort_unstable();
        let coord = metrics.coordination.read_all_unordered().await.unwrap();
        let exports = metrics.export.read_all_unordered().await.unwrap();
        assert!(exports.iter().any(|row| matches!(
            &row.payload,
            SystemFact::MetricsCoordination(MetricsFact::Exported { .. })
        )));
        assert!(coord.iter().all(|row| !matches!(
            &row.payload,
            SystemFact::MetricsCoordination(MetricsFact::Exported { .. })
        )));
        eprintln!("composed backend={disk}, stages={stages}, settlement={settled:?}, Studio={:?}, stage-completion-to-pipeline-drained-us p50={} p95={} p99={} max={}, exports={}, coordination={}", started.elapsed(), completion_latencies[stages / 2], completion_latencies[(stages - 1) * 95 / 100], completion_latencies[(stages - 1) * 99 / 100], completion_latencies[stages - 1], exports.len(), coord.len());
    }
}

#[tokio::test]
async fn forwarded_stage_facts_advance_the_cursor_without_becoming_owner_observations() {
    for disk in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let run = FlowId::new();
        let foreign = StageId::new();
        let owner = StageId::new();
        let system = SystemId::new();
        let source = MemoryJournal::with_owner_in_run(JournalOwner::stage(foreign), run);
        let original = append_stage(
            &source,
            foreign,
            ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: foreign }),
        )
        .await;
        let journal: Arc<dyn Journal<ChainEvent>> = if disk {
            Arc::new(
                crate::journal::disk::DiskJournal::with_owner_in_run(
                    directory.path().join("owner.log"),
                    JournalOwner::stage(owner),
                    run,
                )
                .unwrap(),
            )
        } else {
            Arc::new(MemoryJournal::with_owner_in_run(
                JournalOwner::stage(owner),
                run,
            ))
        };
        journal
            .append(
                original.authored(),
                AppendOptions::from_record(Some(&original)).unwrap(),
            )
            .await
            .unwrap();
        let owned = append_stage(
            journal.as_ref(),
            owner,
            ExecutionPayload::StageLifecycle(StageLifecycleFact::Running { stage_id: owner }),
        )
        .await;
        let journals = TestJournals {
            system: Arc::new(MemoryJournal::with_owner_in_run(
                JournalOwner::system(system),
                run,
            )),
            stages: vec![(owner, journal.clone())],
        };
        append(
            journals.system.as_ref(),
            system.into(),
            SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Drained),
        )
        .await;
        let (endpoint, closing) = endpoint(journals, vec![]);
        let stream = open(&endpoint, None).await;
        closing.send(true).unwrap();
        let body = stream.collect::<Vec<_>>().await;
        let stages = frames(&body, "stage_lifecycle");
        assert_eq!(stages.len(), 1);
        assert_eq!(frame_payload(stages[0])["stage_id"], owner.to_string());
        let bootstrap = frames(&body, "bootstrap");
        let cursor = bootstrap[0].id.as_ref().unwrap();
        let positions: std::collections::BTreeMap<JournalId, u64> =
            serde_json::from_str(cursor.strip_prefix("jr1:").unwrap()).unwrap();
        assert_eq!(positions[journal.id()], owned.local_sequence());
        assert_eq!(
            owned.local_sequence(),
            2,
            "forwarded records retain ordinary journal positions"
        );
    }
}

#[tokio::test]
async fn an_issued_read_settles_while_the_client_stops_polling() {
    let probe = Arc::new(PendingOpen::default());
    let mut journal = ScriptedJournal::new(SystemId::new());
    journal.pending_read = Some(probe.clone());
    let reader_dropped = journal.reader_dropped.clone();
    let (endpoint, _closing) = endpoint(Arc::new(journal), vec![]);
    let mut body = open(&endpoint, None).await;
    assert_eq!(
        body.next().await.unwrap().event.as_deref(),
        Some("bootstrap")
    );
    tokio::select! {
        _ = body.next() => panic!("the held read cannot produce a frame"),
        _ = probe.entered.notified() => {}
    }
    // Finishing the underlying I/O must release its operation guard even though
    // this connection does not poll the returned read result again.
    probe.release.notify_one();
    probe.dropped.notified().await;
    drop(body);
    reader_dropped.notified().await;
}

#[tokio::test]
async fn ready_records_are_delivered_before_a_later_pending_read_settles() {
    let system = SystemId::new();
    let mut journal = ScriptedJournal::new(system);
    let probe = Arc::new(PendingOpen::default());
    journal.pending_read = Some(probe.clone());
    journal.pending_read_from = 1;
    let record = append(
        &journal,
        system.into(),
        SystemPayload::PipelineLifecycle(PipelineLifecycleEvent::Running {
            stage_count: Some(1),
        }),
    )
    .await;
    let reader_dropped = journal.reader_dropped.clone();
    let (endpoint, _closing) = endpoint(Arc::new(journal), vec![]);
    let mut body = open(&endpoint, Some("jr1:{}")).await;
    let frame = body.next().await.unwrap();
    assert_eq!(frame.event.as_deref(), Some("flow_lifecycle"));
    assert_eq!(frame.id, Some(cursor(&record)));
    tokio::select! {
        _ = body.next() => panic!("the pending tail cannot produce another frame"),
        _ = probe.entered.notified() => {}
    }
    drop(body);
    probe.dropped.notified().await;
    reader_dropped.notified().await;
}
