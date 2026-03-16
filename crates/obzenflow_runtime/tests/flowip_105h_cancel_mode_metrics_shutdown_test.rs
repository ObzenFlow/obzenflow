use async_trait::async_trait;
use obzenflow_core::event::{ChainEvent, JournalEvent, JournalWriterId, SystemEvent, SystemEventType};
use obzenflow_core::event::MetricsCoordinationEvent;
use obzenflow_core::id::{FlowId, JournalId, SystemId};
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::{MetricsExporter, NoOpMetricsExporter};
use obzenflow_core::{EventEnvelope, StageId};
use obzenflow_fsm::FsmAction;
use obzenflow_runtime::message_bus::FsmMessageBus;
use obzenflow_runtime::pipeline::fsm::{FlowStopMode, PipelineContext};
use obzenflow_runtime::pipeline::PipelineAction;
use obzenflow_topology::TopologyBuilder;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Minimal in-memory journal with a live reader (sees newly appended events).
struct MemoryJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
}

impl<T: JournalEvent> MemoryJournal<T> {
    fn with_owner(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

struct MemoryJournalReader<T: JournalEvent> {
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    pos: usize,
}

#[async_trait]
impl<T> JournalReader<T> for MemoryJournalReader<T>
where
    T: JournalEvent,
{
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournalReader: poisoned lock");
        if self.pos >= guard.len() {
            return Ok(None);
        }
        let envelope = guard[self.pos].clone();
        drop(guard);
        self.pos += 1;
        Ok(Some(envelope))
    }

    async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
        let guard = self.events.lock().expect("MemoryJournalReader: poisoned lock");
        let len = guard.len();
        drop(guard);
        let before = self.pos;
        self.pos = std::cmp::min(len, self.pos.saturating_add(n as usize));
        Ok((self.pos - before) as u64)
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }
}

#[async_trait]
impl<T> Journal<T> for MemoryJournal<T>
where
    T: JournalEvent + 'static,
{
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        _parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        let envelope = EventEnvelope::new(JournalWriterId::from(self.id), event);
        let mut guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        guard.push(envelope.clone());
        Ok(envelope)
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.clone())
    }

    async fn read_causally_after(
        &self,
        after_event_id: &obzenflow_core::EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        if let Some(idx) = guard.iter().position(|e| e.event.id() == after_event_id) {
            return Ok(guard.iter().skip(idx + 1).cloned().collect());
        }
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        event_id: &obzenflow_core::EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        Ok(guard.iter().find(|e| e.event.id() == event_id).cloned())
    }

    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader {
            events: Arc::clone(&self.events),
            pos: 0,
        }))
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(MemoryJournalReader {
            events: Arc::clone(&self.events),
            pos: position as usize,
        }))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().expect("MemoryJournal: poisoned lock");
        let len = guard.len();
        let start = len.saturating_sub(count);
        // Return most recent first, matching Journal contract.
        Ok(guard[start..].iter().rev().cloned().collect())
    }
}

fn make_topology() -> Arc<obzenflow_topology::Topology> {
    let mut builder = TopologyBuilder::new();
    builder.add_stage(Some("stage1".to_string()));
    builder.add_stage(Some("stage2".to_string()));
    Arc::new(builder.build_unchecked().expect("build topology"))
}

fn make_context(
    system_id: SystemId,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    stage_data_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    metrics_exporter: Option<Arc<dyn MetricsExporter>>,
) -> PipelineContext {
    PipelineContext {
        system_id,
        bus: Arc::new(FsmMessageBus::new()),
        topology: make_topology(),
        flow_name: "test_flow".to_string(),
        flow_id: FlowId::new(),
        system_journal,
        stage_supervisors: HashMap::new(),
        source_supervisors: HashMap::new(),
        completed_stages: Vec::new(),
        running_stages: HashSet::new(),
        completion_subscription: None,
        metrics_exporter,
        metrics_handle: None,
        stage_data_journals,
        stage_error_journals: Vec::new(),
        backpressure_registry: None,
        contract_status: HashMap::new(),
        contract_pairs: HashMap::new(),
        expected_contract_pairs: HashSet::new(),
        expected_sources: Vec::new(),
        stage_lifecycle_metrics: HashMap::new(),
        flow_start_time: None,
        last_system_event_id_seen: None,
        stop_intent: Default::default(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_metrics_skips_when_metrics_not_started() {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));

    let mut ctx = make_context(
        system_id,
        system_journal.clone(),
        Vec::new(),
        Some(Arc::new(NoOpMetricsExporter)),
    );

    PipelineAction::DrainMetrics.execute(&mut ctx).await.unwrap();

    let events = system_journal.read_causally_ordered().await.unwrap();
    assert!(
        events.is_empty(),
        "expected no system events when DrainMetrics is gated off"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_mode_drains_and_shuts_down_metrics_aggregator() {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));

    let stage_id = StageId::new();
    let stage_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(MemoryJournal::with_owner(JournalOwner::stage(stage_id)));

    let mut ctx = make_context(
        system_id,
        system_journal.clone(),
        vec![(stage_id, stage_journal)],
        Some(Arc::new(NoOpMetricsExporter)),
    );

    PipelineAction::StartMetricsAggregator
        .execute(&mut ctx)
        .await
        .unwrap();
    assert!(
        ctx.metrics_handle
            .as_ref()
            .map(|h| h.is_running())
            .unwrap_or(false),
        "expected metrics handle to be stored and running"
    );

    PipelineAction::WritePipelineStopRequested {
        mode: FlowStopMode::Cancel,
    }
    .execute(&mut ctx)
    .await
    .unwrap();

    PipelineAction::DrainMetrics.execute(&mut ctx).await.unwrap();
    PipelineAction::Cleanup.execute(&mut ctx).await.unwrap();

    assert!(
        ctx.metrics_handle.is_none(),
        "expected Cleanup to consume metrics handle"
    );

    // Verify the metrics supervisor drained and wrote its shutdown marker.
    let events = system_journal.read_causally_ordered().await.unwrap();

    let drained = events.iter().any(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
        )
    });

    let shutdown = events.iter().any(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Shutdown)
        )
    });

    assert!(drained, "expected MetricsCoordination::Drained system event");
    assert!(shutdown, "expected MetricsCoordination::Shutdown system event");
}
