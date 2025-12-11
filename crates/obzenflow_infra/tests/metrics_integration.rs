use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{ChainEventFactory, SystemEvent, SystemEventType, WriterId};
use obzenflow_core::id::{StageId, SystemId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::metrics::{
    AppMetricsSnapshot, MetricsExporter, StageMetadata,
};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime_services::metrics::{
    MetricsAggregatorAction, MetricsAggregatorContext, MetricsAggregatorEvent,
    MetricsAggregatorState, MetricsStore, StageMetrics,
};
use obzenflow_runtime_services::metrics::fsm::build_metrics_aggregator_fsm;
use obzenflow_fsm::FsmAction;
use obzenflow_core::event::JournalWriterId;
use obzenflow_core::EventEnvelope;

/// Simple in-memory exporter that records AppMetricsSnapshot values.
#[derive(Default)]
struct RecordingExporter {
    snapshots: Mutex<Vec<AppMetricsSnapshot>>,
}

impl MetricsExporter for RecordingExporter {
    fn update_app_metrics(
        &self,
        snapshot: AppMetricsSnapshot,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.snapshots.lock().unwrap().push(snapshot);
        Ok(())
    }

    fn render_metrics(&self) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
        Ok(String::new())
    }

    fn metric_count(&self) -> usize {
        self.snapshots.lock().unwrap().len()
    }
}

fn make_system_journal(system_id: SystemId) -> Arc<MemoryJournal<SystemEvent>> {
    Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)))
}

fn single_stage_metadata(stage_id: StageId) -> HashMap<StageId, StageMetadata> {
    let mut map = HashMap::new();
    map.insert(
        stage_id,
        StageMetadata {
            name: "test_stage".to_string(),
            stage_type: StageType::Transform,
            flow_name: "test_flow".to_string(),
        },
    );
    map
}

fn make_empty_context(
    system_id: SystemId,
    system_journal: Arc<MemoryJournal<SystemEvent>>,
    exporter: Arc<RecordingExporter>,
    stage_id: StageId,
) -> MetricsAggregatorContext {
    MetricsAggregatorContext {
        system_journal,
        // No upstream journals in this test; tail-read will simply see None.
        stage_data_journals: std::collections::HashMap::new(),
        stage_error_journals: std::collections::HashMap::new(),
        data_subscription: None,
        error_subscription: None,
        system_subscription: None,
        include_error_journals: true,
        exporter: Some(exporter),
        metrics_store: MetricsStore::default(),
        export_interval_secs: 60,
        system_id,
        export_timer: None,
        stage_metadata: single_stage_metadata(stage_id),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn export_snapshot_sanity_from_metrics_store() {
    let stage_id = StageId::new();
    let system_id = SystemId::new();
    let system_journal = make_system_journal(system_id);
    let exporter = Arc::new(RecordingExporter::default());

    let mut ctx = make_empty_context(system_id, system_journal, exporter.clone(), stage_id);

    // Pre-populate the metrics store as if UpdateMetrics had run.
    let mut stage_metrics = StageMetrics::default();
    // Wide-event snapshot counters that ExportMetrics and build_app_metrics_snapshot use.
    stage_metrics.latest_events_processed_total = Some(10);
    stage_metrics.latest_errors_total = Some(2);
    stage_metrics.event_loops_total = 5;
    stage_metrics.event_loops_with_work_total = 5;
    ctx.metrics_store.stage_metrics.insert(stage_id, stage_metrics);

    // Manually execute the ExportMetrics action.
    MetricsAggregatorAction::ExportMetrics
        .execute(&mut ctx)
        .await
        .unwrap();

    let snapshots = exporter.snapshots.lock().unwrap();
    assert!(
        !snapshots.is_empty(),
        "expected at least one exported snapshot"
    );
    let last = snapshots.last().unwrap();

    // Basic sanity: counts and processing metadata made it into the snapshot.
    assert_eq!(
        last.event_counts.get(&stage_id).copied().unwrap_or(0),
        10
    );
    assert_eq!(
        last.error_counts.get(&stage_id).copied().unwrap_or(0),
        2
    );
    assert!(
        last.stage_metadata.get(&stage_id).is_some(),
        "expected stage metadata for stage"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn publish_drain_complete_writes_drained_event() {
    let stage_id = StageId::new();
    let system_id = SystemId::new();
    let system_journal = make_system_journal(system_id);
    let exporter = Arc::new(RecordingExporter::default());

    let mut ctx = make_empty_context(system_id, system_journal.clone(), exporter, stage_id);

    // Execute the PublishDrainComplete action with a synthetic last_event_id.
    let last_event_id = ChainEventFactory::data_event(
        WriterId::from(stage_id),
        "test.event",
        serde_json::json!({"value": 1}),
    )
    .id;

    MetricsAggregatorAction::PublishDrainComplete {
        last_event_id: Some(last_event_id.clone()),
    }
    .execute(&mut ctx)
    .await
    .unwrap();

    // Verify that a MetricsCoordination::Drained event was written to system journal.
    let events = system_journal
        .read_causally_ordered()
        .await
        .expect("read system journal");

    let drained = events.iter().any(|envelope| {
        matches!(
            envelope.event.event,
            SystemEventType::MetricsCoordination(
                obzenflow_core::event::MetricsCoordinationEvent::Drained
            )
        )
    });

    assert!(
        drained,
        "expected MetricsCoordination::Drained event in system journal"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn running_state_process_batch_transitions() {
    let stage_id = StageId::new();
    let system_id = SystemId::new();
    let system_journal = make_system_journal(system_id);
    let exporter = Arc::new(RecordingExporter::default());

    let mut ctx = make_empty_context(system_id, system_journal, exporter, stage_id);
    let mut fsm = build_metrics_aggregator_fsm();

    // Move FSM to Running.
    let actions = fsm
        .handle(
            MetricsAggregatorEvent::StartRunning,
            &mut ctx,
        )
        .await
        .unwrap();
    for action in actions {
        action.execute(&mut ctx).await.unwrap();
    }
    assert!(matches!(
        fsm.state(),
        MetricsAggregatorState::Running
    ));

    // Build a synthetic data event; ProcessBatch should keep us in Running.
    let writer = WriterId::from(stage_id);
    let event = ChainEventFactory::data_event(
        writer,
        "test.event",
        serde_json::json!({"value": 1}),
    );
    let envelope = EventEnvelope::new(JournalWriterId::new(), event);

    let actions = fsm
        .handle(
            MetricsAggregatorEvent::ProcessBatch {
                events: vec![envelope],
            },
            &mut ctx,
        )
        .await
        .unwrap();
    for action in actions {
        action.execute(&mut ctx).await.unwrap();
    }

    assert!(
        matches!(fsm.state(), MetricsAggregatorState::Running),
        "expected FSM to remain in Running after ProcessBatch"
    );
}
