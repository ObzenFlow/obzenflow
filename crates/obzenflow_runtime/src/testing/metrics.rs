// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Metrics journal scenarios supplied with real journals by outer tests.

use crate::journal::FlowJournalFactory;
use obzenflow_core::event::context::StageType;
use obzenflow_core::journal::journal_name::JournalName;
use obzenflow_core::{ChainEvent, Journal, JournalOwner, StageId};
use obzenflow_fsm::FsmAction;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn metrics_tail_refresh_keeps_counts_current_without_advancing_input_coverage(
    make_journals: fn() -> Box<dyn FlowJournalFactory>,
) {
    use obzenflow_core::event::{MetricsCoordinationEvent, SystemEvent, SystemEventType};
    use obzenflow_core::metrics::{
        AppMetricsSnapshot, InfraMetricsSnapshot, MetricsSnapshotExporter,
    };
    use obzenflow_core::SystemId;

    #[derive(Default)]
    struct RecordingSnapshots(std::sync::Mutex<Vec<AppMetricsSnapshot>>);
    impl MetricsSnapshotExporter for RecordingSnapshots {
        fn publish_app_snapshot(&self, value: AppMetricsSnapshot) {
            self.0.lock().unwrap().push(value);
        }
        fn publish_infra_snapshot(&self, _value: InfraMetricsSnapshot) {}
    }

    use crate::metrics::fsm::{
        MetricsAggregatorAction, MetricsAggregatorContext, MetricsJournalKind,
    };
    use crate::metrics::MetricsInputs;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{context::RuntimeContext, ChainEventFactory};

    let mut journals = make_journals();
    let system_id = SystemId::new();
    let system: Arc<dyn Journal<SystemEvent>> = journals
        .create_system_journal(JournalName::System, JournalOwner::system(system_id))
        .unwrap();
    let stage = StageId::new();
    let data: Arc<dyn Journal<ChainEvent>> = journals
        .create_chain_journal(
            JournalName::Stage {
                id: stage,
                stage_type: StageType::Transform,
                name: "data".into(),
            },
            JournalOwner::stage(stage),
        )
        .unwrap();
    let errors: Arc<dyn Journal<ChainEvent>> = journals
        .create_chain_journal(
            JournalName::Stage {
                id: stage,
                stage_type: StageType::Transform,
                name: "errors".into(),
            },
            JournalOwner::stage(stage),
        )
        .unwrap();
    let mut rows = Vec::new();
    for (count, target, kind) in [
        (1, &data, MetricsJournalKind::Data),
        (2, &errors, MetricsJournalKind::Error),
    ] {
        let mut event =
            ChainEventFactory::data_event(stage.into(), "test.fact", serde_json::json!({}));
        event.flow_context.stage_id = stage;
        event = event
            .with_runtime_context(RuntimeContext {
                events_processed_total: count,
                errors_total: count,
                errors_by_kind: HashMap::from([(ErrorKind::Unknown, count)]),
                ..crate::metrics::instrumentation::StageInstrumentation::new()
                    .snapshot_with_control()
            })
            .mark_as_error("expected", ErrorKind::Unknown);
        rows.push((kind, target.append(event, None).await.unwrap()));
    }
    let exporter = Arc::new(RecordingSnapshots::default());
    let (mut context, mut io) = MetricsAggregatorContext::new(
        MetricsInputs::new(vec![(stage, data)], vec![(stage, errors)]),
        system.clone(),
        exporter.clone(),
        1,
        system_id,
        HashMap::new(),
        Vec::new(),
    )
    .await
    .unwrap();

    MetricsAggregatorAction::ExportMetrics
        .execute(&mut context)
        .await
        .unwrap();
    {
        let snapshots = exporter.0.lock().unwrap();
        let snapshot = snapshots.last().unwrap();
        assert_eq!(snapshot.event_counts[&stage], 2);
        assert_eq!(snapshot.error_counts[&stage], 2);
        assert_eq!(
            snapshot.error_counts_by_kind[&stage][&ErrorKind::Unknown],
            2
        );
        assert!(snapshot.stage_vector_clocks.is_empty());
    }
    assert!(context.metrics_store.last_event_id.is_none());
    assert!(matches!(
        io.data_subscription.poll_next().await,
        crate::messaging::PollResult::Event(row) if row.event.id == rows[0].1.event.id
    ));
    assert!(!system
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .any(|row| {
            matches!(
                row.event.event,
                SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
            )
        }));

    // Catch-up can revisit older cumulative snapshots and error-marked rows.
    // Neither observation order nor repeated folding may inflate the counts.
    for _ in 0..2 {
        for (kind, row) in &rows {
            MetricsAggregatorAction::UpdateMetrics {
                envelope: Box::new(row.clone()),
                journal_kind: *kind,
                journal_stage: stage,
            }
            .execute(&mut context)
            .await
            .unwrap();
        }
    }
    let metrics = &context.metrics_store.stage_metrics[&stage];
    assert_eq!(metrics.latest_errors_total, Some(2));
    assert_eq!(metrics.errors_by_kind[&ErrorKind::Unknown], 2);
}
