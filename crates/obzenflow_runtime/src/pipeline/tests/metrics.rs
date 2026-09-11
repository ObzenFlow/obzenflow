// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Pipeline metrics preparation, topology metadata and child lifetime coordination.

use crate::id_conversions::StageIdExt;
use crate::pipeline::fsm::{PipelineAction, PipelineDeadline, PipelineFsmEvent, PipelineFsmState};
use crate::pipeline::metrics::composite_boundaries_from_topology;
use crate::pipeline::tests::support::{
    make_context, make_fsm_context, owned_test_stage, source_sink_topology_with_source,
    test_context, DiscardSnapshots, MemoryJournal, StartGate, TerminalAppendGate,
};
use crate::pipeline::PipelineState;
use crate::supervised_base::{ChannelBuilder, SupervisorHandle};
use crate::testing::memory_journal::MemoryJournal as LiveJournal;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::{
    ChainEvent, JournalEvent, MetricsCoordinationEvent, SystemEvent, SystemEventFactory,
    SystemEventType,
};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::metrics::{AppMetricsSnapshot, InfraMetricsSnapshot, MetricsSnapshotExporter};
use obzenflow_core::{FlowId, StageId, SystemId};
use obzenflow_fsm::FsmAction;
use obzenflow_topology::{
    BoundaryPortSpec, CompositePortRef, DirectedEdge, EdgeKind, PortDirection, StageInfo,
    StageType as TopologyStageType, SubgraphInternalEdge, Topology, TopologySubgraphInfo,
};
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;

#[derive(Default)]
struct RecordingSnapshots(std::sync::Mutex<Vec<AppMetricsSnapshot>>);
impl MetricsSnapshotExporter for RecordingSnapshots {
    fn publish_app_snapshot(&self, value: AppMetricsSnapshot) {
        self.0.lock().unwrap().push(value);
    }
    fn publish_infra_snapshot(&self, _value: InfraMetricsSnapshot) {}
}

#[test]
fn runtime_boundary_is_the_named_multi_port_cut_even_when_not_collapsible() {
    let ids: Vec<_> = (1_u128..=6)
        .map(|value| obzenflow_topology::StageId::from_bytes(value.to_be_bytes()))
        .collect();
    let (producer, entry, completed, failed, ok_sink, err_sink) =
        (ids[0], ids[1], ids[2], ids[3], ids[4], ids[5]);
    let stages = vec![
        StageInfo::new(producer, "producer", TopologyStageType::FiniteSource),
        StageInfo::new(entry, "entry", TopologyStageType::Transform),
        StageInfo::new(completed, "completed", TopologyStageType::Transform),
        StageInfo::new(failed, "failed", TopologyStageType::Transform),
        StageInfo::new(ok_sink, "ok", TopologyStageType::Sink),
        StageInfo::new(err_sink, "err", TopologyStageType::Sink),
    ];
    let subgraph_id = "saga:checkout";
    let edges = vec![
        DirectedEdge::new(producer, entry, EdgeKind::Forward)
            .with_composite_ports(vec![CompositePortRef::new(subgraph_id, "commands")]),
        DirectedEdge::new(entry, completed, EdgeKind::Forward),
        DirectedEdge::new(entry, failed, EdgeKind::Forward),
        DirectedEdge::new(completed, ok_sink, EdgeKind::Forward)
            .with_composite_ports(vec![CompositePortRef::new(subgraph_id, "completed")]),
        DirectedEdge::new(failed, err_sink, EdgeKind::Forward)
            .with_composite_ports(vec![CompositePortRef::new(subgraph_id, "failed")]),
    ];
    let subgraph = TopologySubgraphInfo::new(
        subgraph_id,
        "saga",
        "checkout",
        "checkout",
        vec![entry, completed, failed],
        vec![
            SubgraphInternalEdge::new(entry, completed, "terminal"),
            SubgraphInternalEdge::new(entry, failed, "terminal"),
        ],
        vec![entry],
        vec![completed, failed],
        false,
    )
    .with_boundary_ports(vec![
        BoundaryPortSpec::new(
            "commands",
            PortDirection::Input,
            entry,
            vec!["checkout.command.v1".into()],
            true,
        ),
        BoundaryPortSpec::new(
            "completed",
            PortDirection::Output,
            completed,
            vec!["checkout.completed.v1".into()],
            true,
        ),
        BoundaryPortSpec::new(
            "failed",
            PortDirection::Output,
            failed,
            vec!["checkout.failed.v1".into()],
            false,
        ),
    ]);
    let topology = Topology::new_unvalidated(stages, edges)
        .unwrap()
        .with_subgraphs(vec![subgraph]);

    let boundaries = composite_boundaries_from_topology(&topology);
    assert_eq!(boundaries.len(), 1);
    let boundary = &boundaries[0];
    assert_eq!(boundary.ports.len(), 3);
    assert_eq!(boundary.edges.len(), 3);
    assert!(boundary.edges.iter().any(|edge| {
        edge.port == "completed"
            && edge.member == obzenflow_core::StageId::from_topology_id(completed)
    }));
    assert!(boundary.edges.iter().any(|edge| {
        edge.port == "failed" && edge.member == obzenflow_core::StageId::from_topology_id(failed)
    }));
}

#[tokio::test]
async fn dropping_pipeline_context_cancels_its_metrics_supervisor() {
    use crate::metrics::fsm::{MetricsAggregatorEvent, MetricsAggregatorState};
    use crate::supervised_base::HandleBuilder;

    let system_id = SystemId::new();
    let journal = Arc::new(MemoryJournal::with_owner(JournalOwner::system(system_id)));
    let (topology, _, _) = source_sink_topology_with_source();
    let context = test_context(topology, system_id, journal, None);
    let (sender, _receiver, watcher) =
        ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
            .build(MetricsAggregatorState::Running);
    let (started_tx, started_rx) = oneshot::channel();
    let (terminated_tx, terminated_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let _termination = terminated_tx;
        started_tx.send(()).unwrap();
        std::future::pending::<Result<(), Box<dyn std::error::Error + Send + Sync>>>().await
    });
    context.resources.metrics.install_for_test(
        HandleBuilder::new()
            .with_event_sender(sender)
            .with_state_watcher(watcher)
            .with_supervisor_task(task)
            .build_standard()
            .unwrap(),
    );
    started_rx.await.unwrap();
    drop(context);
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), terminated_rx)
            .await
            .expect("metrics task must be cancelled when its pipeline disappears")
            .is_err()
    );
}

#[tokio::test]
async fn parent_panic_retains_metrics_publication_until_repeated_flow_joins_finish() {
    use crate::__private::lifecycle;
    let system_id = SystemId::new();
    let metrics_gate = Arc::new(TerminalAppendGate {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        fail: false,
    });
    let mut journal = MemoryJournal::with_owner(JournalOwner::system(system_id));
    journal.metrics_ready_append = Some(metrics_gate.clone());
    let journal = Arc::new(journal);
    let (topology, source, sink) = source_sink_topology_with_source();
    let (entered, start_entered) = oneshot::channel();
    let (release, start_release) = oneshot::channel();
    let mut stage = owned_test_stage(sink, StageType::Sink, None);
    stage.panic_on_start = true;
    stage.start_gate = Some(StartGate {
        entered: Mutex::new(Some(entered)),
        release: tokio::sync::Mutex::new(Some(start_release)),
        count: Arc::new(AtomicUsize::new(0)),
    });
    let flow = crate::pipeline::PipelineBuilder::new(topology, journal.clone(), FlowId::new())
        .with_sources(vec![Box::new(owned_test_stage(
            source,
            StageType::FiniteSource,
            None,
        ))])
        .with_stages(vec![Box::new(stage)])
        .with_metrics_exporter(Arc::new(DiscardSnapshots))
        .build()
        .await
        .unwrap();
    let guard = lifecycle::guard_execution(&flow);
    tokio::time::timeout(Duration::from_secs(2), async {
        start_entered.await.unwrap();
        metrics_gate.entered.notified().await;
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while flow.is_running() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut abandoned = Box::pin(lifecycle::wait(&flow));
    assert!(
        futures::poll!(&mut abandoned).is_pending(),
        "accepted metrics publication still owns its join"
    );
    drop(abandoned);
    metrics_gate.release.notify_one();
    for _ in 0..2 {
        let error = tokio::time::timeout(Duration::from_secs(2), lifecycle::wait(&flow))
            .await
            .unwrap()
            .unwrap_err();
        assert!(std::error::Error::source(&error)
            .unwrap()
            .to_string()
            .contains("panicked"));
    }
    guard.disarm();
    let rows = journal.read_all_unordered().await.unwrap();
    assert_eq!(
        rows.iter()
            .filter(|row| row.event.event_type_name() == "system.metrics.ready")
            .count(),
        1
    );
    assert!(!rows
        .iter()
        .any(|row| row.event.event_type_name() == "system.pipeline.drained"));
}

#[tokio::test]
async fn metrics_preparation_is_passive_and_cancellation_prevents_late_installation() {
    let mut ctx = make_fsm_context();
    ctx.metrics_exporter = Some(Arc::new(RecordingSnapshots::default()));
    let prepared = crate::pipeline::metrics::prepare_metrics(&ctx)
        .await
        .unwrap()
        .unwrap();
    assert!(ctx
        .system_journal
        .read_all_unordered()
        .await
        .unwrap()
        .is_empty());
    ctx.resources.metrics.request_abort();
    assert!(ctx.resources.metrics.start(prepared).is_err());
    assert!(ctx.resources.metrics.handle().is_none());
    assert!(ctx
        .system_journal
        .read_all_unordered()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn original_terminal_acknowledgement_expires_metrics_before_delayed_journal_consumption() {
    use crate::metrics::{MetricsAggregatorEvent, MetricsAggregatorState};
    use crate::pipeline::supervisor::PipelineSupervisor;
    use crate::supervised_base::{ChannelBuilder, HandleBuilder, SelfSupervised};
    let mut ctx = make_fsm_context();
    ctx.metrics_drain_timeout_ms = 1;
    let (sender, _receiver, watcher) =
        ChannelBuilder::<MetricsAggregatorEvent, MetricsAggregatorState>::new()
            .build(MetricsAggregatorState::Running);
    let task = tokio::spawn(std::future::pending::<
        Result<(), Box<dyn std::error::Error + Send + Sync>>,
    >());
    let metrics = HandleBuilder::new()
        .with_event_sender(sender)
        .with_state_watcher(watcher)
        .with_supervisor_task(task)
        .build_standard()
        .unwrap();
    ctx.resources.metrics.install_for_test(metrics);
    let ack = std::time::Instant::now() - std::time::Duration::from_secs(1);
    ctx.resources.terminal_ack.set(ack).unwrap();
    let (_sender, receiver, watcher) = ChannelBuilder::new().build(PipelineState::Draining);
    let mut supervisor = PipelineSupervisor::new(
        ctx.system_id,
        receiver,
        watcher,
        ctx.resources.failure.clone(),
    );
    assert!(matches!(
        supervisor
            .dispatch_state(&PipelineFsmState::PublishingTerminal, &mut ctx)
            .await
            .unwrap(),
        crate::supervised_base::EventLoopDirective::Transition(PipelineFsmEvent::Deadline(
            PipelineDeadline::Metrics
        ))
    ));
    assert_eq!(ctx.resources.terminal_ack.get(), Some(&ack));
    ctx.resources.metrics.abort_and_join().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn drain_metrics_skips_when_metrics_not_started() {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(LiveJournal::with_owner(JournalOwner::system(system_id)));

    let mut ctx = make_context(
        system_id,
        system_journal.clone(),
        Vec::new(),
        Some(Arc::new(RecordingSnapshots::default())),
    );

    PipelineAction::DrainMetrics
        .execute(&mut ctx)
        .await
        .unwrap();

    let events = system_journal.read_causally_ordered().await.unwrap();
    assert!(
        events.is_empty(),
        "expected no system events when DrainMetrics is gated off"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn late_metrics_bootstrap_reads_all_physical_inputs_without_stage_eof() {
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::{context::RuntimeContext, ChainEventFactory};
    let system_id = SystemId::new();
    let journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(LiveJournal::with_owner(JournalOwner::system(system_id)));
    let data_stage = StageId::new();
    let error_stage = StageId::new();
    let data: Arc<dyn Journal<ChainEvent>> =
        Arc::new(LiveJournal::with_owner(JournalOwner::stage(data_stage)));
    let errors: Arc<dyn Journal<ChainEvent>> =
        Arc::new(LiveJournal::with_owner(JournalOwner::stage(error_stage)));
    for (stage, rows, target, failed) in [
        (data_stage, 50, &data, false),
        (error_stage, 7, &errors, true),
    ] {
        for count in 1..=rows {
            let mut event = ChainEventFactory::data_event(
                stage.into(),
                "test.fact",
                serde_json::json!({"n":count}),
            );
            event.flow_context.stage_id = stage;
            event = event.with_runtime_context(RuntimeContext {
                events_emitted_total: count,
                errors_total: if failed { count } else { 0 },
                errors_by_kind: if failed {
                    HashMap::from([(ErrorKind::Unknown, count)])
                } else {
                    HashMap::new()
                },
                ..crate::metrics::instrumentation::StageInstrumentation::new()
                    .snapshot_with_control()
            });
            if failed {
                event = event.mark_as_error("expected", ErrorKind::Unknown);
            }
            target.append(event, None).await.unwrap();
        }
    }
    // The observer starts after publication, with no stage terminal or EOF.
    // Another pipeline writer and a later same-writer fact cannot extend its
    // system endpoint beyond the selected current-writer terminal.
    journal
        .append(
            SystemEventFactory::new(SystemId::new()).pipeline_not_started(),
            None,
        )
        .await
        .unwrap();
    let terminal = SystemEventFactory::new(system_id).pipeline_cancelled(
        "test".into(),
        obzenflow_core::event::types::DurationMs(0),
        None,
        None,
    );
    journal.append(terminal, None).await.unwrap();
    journal
        .append(
            SystemEventFactory::new(system_id).pipeline_failed(
                "outside fixed endpoint".into(),
                obzenflow_core::event::types::DurationMs(0),
                None,
                None,
            ),
            None,
        )
        .await
        .unwrap();
    let exporter = Arc::new(RecordingSnapshots::default());
    let mut ctx = make_context(
        system_id,
        journal.clone(),
        vec![(data_stage, data)],
        Some(exporter.clone()),
    );
    ctx.stage_error_journals.push((error_stage, errors));
    ctx.resources.prepared_metrics = crate::pipeline::metrics::prepare_metrics(&ctx)
        .await
        .unwrap();
    PipelineAction::StartMetricsAggregator
        .execute(&mut ctx)
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .unwrap()
            .wait_for_completion(),
    )
    .await
    .unwrap()
    .unwrap();
    {
        let snapshots = exporter.0.lock().unwrap();
        let snapshot = snapshots.last().unwrap();
        assert_eq!(snapshot.pipeline_state, "cancelled");
        assert_eq!(snapshot.events_emitted_total[&data_stage], 50);
        assert_eq!(snapshot.events_emitted_total[&error_stage], 7);
        assert_eq!(snapshot.error_counts[&error_stage], 7);
        assert_eq!(
            snapshot.error_counts_by_kind[&error_stage][&ErrorKind::Unknown],
            7
        );
    }
    assert!(journal
        .read_all_unordered()
        .await
        .unwrap()
        .iter()
        .any(|row| row.event.event_type_name() == "system.metrics.drained"));
}

#[tokio::test(flavor = "multi_thread")]
async fn stage_cleanup_keeps_metrics_alive_until_the_terminal_fact() {
    let system_id = SystemId::new();
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(LiveJournal::with_owner(JournalOwner::system(system_id)));

    let stage_id = StageId::new();
    let stage_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(LiveJournal::with_owner(JournalOwner::stage(stage_id)));

    let mut ctx = make_context(
        system_id,
        system_journal.clone(),
        vec![(stage_id, stage_journal)],
        Some(Arc::new(RecordingSnapshots::default())),
    );

    ctx.resources.prepared_metrics = crate::pipeline::metrics::prepare_metrics(&ctx)
        .await
        .unwrap();
    PipelineAction::StartMetricsAggregator
        .execute(&mut ctx)
        .await
        .unwrap();
    assert!(
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .map(|h| h.is_running())
            .unwrap_or(false),
        "expected metrics handle to be stored and running"
    );

    PipelineAction::CancelStages {
        contract_abort: false,
    }
    .execute(&mut ctx)
    .await
    .unwrap();

    for _ in 0..128 {
        PipelineAction::DrainMetrics
            .execute(&mut ctx)
            .await
            .unwrap();
    }
    ctx.resources.publications.observe_accepted().await.unwrap();

    assert!(
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .unwrap()
            .is_running(),
        "stage cleanup must retain metrics for terminal catch-up"
    );

    let events = system_journal.read_causally_ordered().await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|envelope| matches!(
                &envelope.event.event,
                SystemEventType::MetricsCoordination(MetricsCoordinationEvent::DrainRequested)
            ))
            .count(),
        1,
        "repeated failure cleanup must retain one drain admission"
    );
    assert!(!events.iter().any(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
        )
    }));
    system_journal
        .append(
            SystemEventFactory::new(system_id).pipeline_not_started(),
            None,
        )
        .await
        .unwrap();
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        ctx.resources
            .metrics
            .handle()
            .as_ref()
            .unwrap()
            .wait_for_completion(),
    )
    .await
    .unwrap()
    .unwrap();
    let events = system_journal.read_causally_ordered().await.unwrap();
    assert!(events.iter().any(|envelope| {
        matches!(
            &envelope.event.event,
            SystemEventType::MetricsCoordination(MetricsCoordinationEvent::Drained)
        )
    }));
}
