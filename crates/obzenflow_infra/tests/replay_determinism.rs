use std::sync::Arc;

use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::{ChainEvent, ChainEventContent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::Ulid;
use obzenflow_core::{FlowId, StageId, SystemId, TypedPayload, WriterId};
use obzenflow_infra::journal::disk::disk_journal::DiskJournal;
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime_services::id_conversions::StageIdExt;
use obzenflow_runtime_services::stages::common::control_strategies::JonestownStrategy;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::StatefulHandler;
use obzenflow_runtime_services::stages::join::handle::JoinHandleExt;
use obzenflow_runtime_services::stages::join::{
    JoinBuilder, JoinConfig, StrictJoinBuilder, TypedJoinState,
};
use obzenflow_runtime_services::stages::resources_builder::StageResourcesBuilder;
use obzenflow_runtime_services::stages::stateful::{
    StatefulBuilder, StatefulConfig, StatefulHandleExt, StatefulState,
};
use obzenflow_runtime_services::stages::JoinHandler;
use obzenflow_runtime_services::supervised_base::{SupervisorBuilder, SupervisorHandle};
use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};
use serde::{Deserialize, Serialize};
use serde_json::json;

/// Helper: create a FlowControl EOF event with advertised writer_seq.
fn make_eof_event(writer: WriterId, seq: u64) -> ChainEvent {
    let mut eof = ChainEventFactory::eof_event(writer.clone(), true);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        ref mut writer_id,
        ref mut writer_seq,
        ..
    }) = eof.content
    {
        *writer_id = Some(writer.clone());
        *writer_seq = Some(SeqNo(seq));
    }
    eof
}

// =========================
// Stateful replay harness
// =========================

#[derive(Clone, Debug)]
struct CountingHandler {
    writer_id: WriterId,
}

#[async_trait::async_trait]
trait SimpleStateful {
    type State;

    fn initial_state(&self) -> Self::State;
    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent);
    async fn drain(&self, state: &Self::State) -> Vec<ChainEvent>;
}

#[async_trait::async_trait]
impl SimpleStateful for CountingHandler {
    type State = u64;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if matches!(
            event.content,
            ChainEventContent::Data {
                event_type: _,
                payload: _
            }
        ) {
            *state += 1;
        }
    }

    async fn drain(&self, state: &Self::State) -> Vec<ChainEvent> {
        if *state == 0 {
            Vec::new()
        } else {
            vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                "test.stateful.count",
                json!({ "count": *state }),
            )]
        }
    }
}

async fn run_stateful_fold_once(
    upstream_stage: StageId,
    upstream_journal: Arc<MemoryJournal<ChainEvent>>,
) -> Vec<serde_json::Value> {
    let writer_id = WriterId::from(StageId::new());
    let mut handler = CountingHandler { writer_id };
    let mut state = handler.initial_state();

    let events = upstream_journal
        .read_causally_ordered()
        .await
        .expect("read upstream journal");

    for envelope in events {
        match envelope.event.content {
            ChainEventContent::Data { .. } => {
                handler.accumulate(&mut state, envelope.event.clone());
            }
            ChainEventContent::FlowControl(FlowControlPayload::Eof { .. }) => {
                break;
            }
            _ => {}
        }
    }

    handler
        .drain(&state)
        .await
        .into_iter()
        .map(|ev| ev.payload().clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn stateful_replay_produces_identical_aggregates() {
    let upstream_stage = StageId::new();
    let upstream_writer = WriterId::from(upstream_stage);

    let upstream_journal: Arc<MemoryJournal<ChainEvent>> = Arc::new(MemoryJournal::with_owner(
        JournalOwner::stage(upstream_stage),
    ));

    // Write a small deterministic input stream: 3 data events + EOF.
    for i in 0..3 {
        let event = ChainEventFactory::data_event(
            upstream_writer.clone(),
            "test.input",
            json!({ "seq": i }),
        );
        upstream_journal
            .append(event, None)
            .await
            .expect("append data");
    }
    upstream_journal
        .append(make_eof_event(upstream_writer.clone(), 3), None)
        .await
        .expect("append eof");

    let aggregates_run1 = run_stateful_fold_once(upstream_stage, upstream_journal.clone()).await;
    let aggregates_run2 = run_stateful_fold_once(upstream_stage, upstream_journal.clone()).await;

    assert_eq!(
        aggregates_run1, aggregates_run2,
        "stateful replay should produce identical aggregates"
    );
    assert_eq!(aggregates_run1.len(), 1);
    assert_eq!(aggregates_run1[0]["count"], json!(3));
}

// =========================
// Join replay harness
// =========================

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CatalogRow {
    key: String,
    value: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct StreamRow {
    key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct JoinedRow {
    source: String,
    stream: String,
}

impl TypedPayload for CatalogRow {
    const EVENT_TYPE: &'static str = "test.catalog";
}

impl TypedPayload for StreamRow {
    const EVENT_TYPE: &'static str = "test.stream";
}

impl TypedPayload for JoinedRow {
    const EVENT_TYPE: &'static str = "test.joined";
}

async fn run_strict_join_once() -> Vec<JoinedRow> {
    let handler = StrictJoinBuilder::<CatalogRow, StreamRow, JoinedRow>::new()
        .catalog_key(|c| c.key.clone())
        .stream_key(|s| s.key.clone())
        .build(|catalog, stream| JoinedRow {
            source: catalog.value,
            stream: stream.key,
        });

    let mut state = handler.initial_state();
    let writer = WriterId::from(StageId::new());

    // Hydrate reference catalog.
    let ref_rows = vec![
        CatalogRow {
            key: "k1".into(),
            value: "v1".into(),
        },
        CatalogRow {
            key: "k2".into(),
            value: "v2".into(),
        },
    ];
    for row in &ref_rows {
        let event = row.clone().to_event(writer.clone());
        let _ = handler
            .process_event(&mut state, event, StageId::new(), writer.clone())
            .expect("Reference catalog hydration should not fail");
    }

    // Ensure catalog hydration worked as expected.
    let typed_state: TypedJoinState<CatalogRow, StreamRow, String> = state.clone();
    assert_eq!(
        typed_state.reference_catalog.len(),
        2,
        "reference catalog should contain two rows"
    );

    // Stream side: one hit, one miss.
    let stream_rows = vec![
        StreamRow { key: "k1".into() },
        StreamRow {
            key: "missing".into(),
        },
    ];

    let mut joined = Vec::new();
    for (idx, row) in stream_rows.iter().enumerate() {
        let event = row.clone().to_event(writer.clone());
        let out = handler
            .process_event(&mut state, event, StageId::new(), writer.clone())
            .expect("Join handler in replay_determinism test should not fail");
        if idx == 0 {
            assert!(
                !out.is_empty(),
                "expected at least one joined event for hit stream row"
            );
        }
        for ev in out {
            if let Some(j) = JoinedRow::from_event(&ev) {
                joined.push(j);
            }
        }
    }

    joined
}

#[tokio::test(flavor = "multi_thread")]
async fn join_replay_produces_identical_joins() {
    let joined_run1 = run_strict_join_once().await;
    let joined_run2 = run_strict_join_once().await;

    // Strict join: only k1 should produce a joined row.
    assert_eq!(joined_run1.len(), 1);
    assert_eq!(joined_run1, joined_run2);
    assert_eq!(
        joined_run1[0],
        JoinedRow {
            source: "v1".into(),
            stream: "k1".into()
        }
    );
}

// =========================
// Supervisor-driven replay harnesses
// =========================

#[derive(Clone, Debug)]
struct SupervisorCountingHandler {
    writer_id: WriterId,
}

#[async_trait::async_trait]
impl StatefulHandler for SupervisorCountingHandler {
    type State = u64;

    fn accumulate(&mut self, state: &mut Self::State, event: ChainEvent) {
        if matches!(
            event.content,
            ChainEventContent::Data {
                event_type: _,
                payload: _
            }
        ) {
            *state += 1;
        }
    }

    fn initial_state(&self) -> Self::State {
        0
    }

    fn create_events(
        &self,
        state: &Self::State,
    ) -> std::result::Result<Vec<ChainEvent>, HandlerError> {
        if *state == 0 {
            Ok(Vec::new())
        } else {
            Ok(vec![ChainEventFactory::data_event(
                self.writer_id.clone(),
                "test.stateful.count.supervisor",
                json!({ "count": *state }),
            )])
        }
    }
}

async fn run_stateful_supervisor_once() -> Vec<serde_json::Value> {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let src = StageId::new();
    let stateful_stage = StageId::new();

    // Topology: src -> stateful
    let mut topo_builder = TopologyBuilder::new();
    topo_builder.add_stage_with_id(
        src.to_topology_id(),
        Some("src".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        stateful_stage.to_topology_id(),
        Some("stateful".to_string()),
        TopologyStageType::Stateful,
    );
    topo_builder.reset_current();
    topo_builder.add_edge(src.to_topology_id(), stateful_stage.to_topology_id());
    let topology = Arc::new(
        topo_builder
            .build_unchecked()
            .expect("topology should build structurally"),
    );

    // Journals: one per stage (on-disk so subscriptions can obtain readers)
    let base = std::env::temp_dir().join(format!("stateful_supervisor_replay_{}", Ulid::new()));
    std::fs::create_dir_all(&base).expect("create temp dir for stateful supervisor replay");
    let src_path = base.join("src.log");
    let stateful_path = base.join("stateful.log");

    let src_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(src_path, JournalOwner::stage(src))
            .expect("create src disk journal"),
    );
    let stateful_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(stateful_path, JournalOwner::stage(stateful_stage))
            .expect("create stateful disk journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(src, src_journal.clone() as Arc<dyn Journal<ChainEvent>>);
    stage_journals.insert(
        stateful_stage,
        stateful_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let mut error_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    for (id, journal) in stage_journals.iter() {
        error_journals.insert(*id, journal.clone());
    }

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            base.join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Write deterministic upstream events into src journal.
    let upstream_writer = WriterId::from(src);
    for i in 0..3 {
        let event = ChainEventFactory::data_event(
            upstream_writer.clone(),
            "test.input",
            json!({ "seq": i }),
        );
        src_journal.append(event, None).await.expect("append data");
    }
    src_journal
        .append(make_eof_event(upstream_writer.clone(), 3), None)
        .await
        .expect("append eof");

    // Build stage resources and extract stateful resources.
    let mut resources_set = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("stage resources should build");

    let stateful_resources = resources_set
        .take_stage_resources(stateful_stage)
        .expect("stateful resources exist");

    // Build and run the stateful stage.
    let handler = SupervisorCountingHandler {
        writer_id: WriterId::from(stateful_stage),
    };
    let config = StatefulConfig {
        stage_id: stateful_stage,
        stage_name: "stateful_supervisor_replay".to_string(),
        flow_name: "replay_flow".to_string(),
        upstream_stages: vec![src],
        emit_interval: None,
        control_strategy: None,
    };

    let handle = StatefulBuilder::new(handler.clone(), config, stateful_resources)
        .build()
        .await
        .expect("build stateful stage");

    handle.initialize().await.expect("initialize stateful");
    handle.ready().await.expect("ready stateful");

    for _ in 0..200 {
        if handle.is_terminal() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        handle.is_terminal(),
        "stateful supervisor did not terminate; final state = {:?}",
        handle.current_state()
    );

    let events = stateful_journal
        .read_causally_ordered()
        .await
        .expect("read stateful journal");
    events
        .iter()
        .filter(|env| env.event.event_type() == "test.stateful.count.supervisor")
        .map(|env| env.event.payload().clone())
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn stateful_supervisor_replay_produces_identical_aggregates() {
    let aggregates_run1 = run_stateful_supervisor_once().await;
    let aggregates_run2 = run_stateful_supervisor_once().await;

    assert_eq!(
        aggregates_run1, aggregates_run2,
        "stateful supervisor replay should produce identical aggregates"
    );
    assert_eq!(aggregates_run1.len(), 1);
    assert_eq!(aggregates_run1[0]["count"], json!(3));
}

async fn run_join_supervisor_once() -> Vec<JoinedRow> {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    // Topology: reference -> join, stream -> join
    let mut topo_builder = TopologyBuilder::new();
    topo_builder.add_stage_with_id(
        reference_stage.to_topology_id(),
        Some("reference".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        stream_stage.to_topology_id(),
        Some("stream".to_string()),
        TopologyStageType::FiniteSource,
    );
    topo_builder.reset_current();
    topo_builder.add_stage_with_id(
        join_stage.to_topology_id(),
        Some("join".to_string()),
        TopologyStageType::Join,
    );
    topo_builder.reset_current();
    topo_builder.add_edge(
        reference_stage.to_topology_id(),
        join_stage.to_topology_id(),
    );
    topo_builder.add_edge(stream_stage.to_topology_id(), join_stage.to_topology_id());
    let topology = Arc::new(
        topo_builder
            .build_unchecked()
            .expect("topology should build structurally"),
    );

    // Journals per stage (on-disk so subscriptions can obtain readers)
    let base = std::env::temp_dir().join(format!("join_supervisor_replay_{}", Ulid::new()));
    std::fs::create_dir_all(&base).expect("create temp dir for join supervisor replay");
    let reference_path = base.join("reference.log");
    let stream_path = base.join("stream.log");
    let join_path = base.join("join.log");

    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(reference_path, JournalOwner::stage(reference_stage))
            .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(stream_path, JournalOwner::stage(stream_stage))
            .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(join_path, JournalOwner::stage(join_stage))
            .expect("create join disk journal"),
    );

    let mut stage_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    stage_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    stage_journals.insert(
        join_stage,
        join_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let mut error_journals: std::collections::HashMap<StageId, Arc<dyn Journal<ChainEvent>>> =
        std::collections::HashMap::new();
    for (id, journal) in stage_journals.iter() {
        error_journals.insert(*id, journal.clone());
    }

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            base.join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create join system disk journal"),
    );

    // Reference side: two keys.
    let reference_writer = WriterId::from(reference_stage);
    let ref_rows = vec![
        CatalogRow {
            key: "k1".into(),
            value: "v1".into(),
        },
        CatalogRow {
            key: "k2".into(),
            value: "v2".into(),
        },
    ];
    for row in &ref_rows {
        reference_journal
            .append(row.clone().to_event(reference_writer.clone()), None)
            .await
            .expect("append reference");
    }
    reference_journal
        .append(
            make_eof_event(reference_writer.clone(), ref_rows.len() as u64),
            None,
        )
        .await
        .expect("append reference eof");

    // Stream side: one hit, one miss.
    let stream_writer = WriterId::from(stream_stage);
    let stream_rows = vec![
        StreamRow { key: "k1".into() },
        StreamRow {
            key: "missing".into(),
        },
    ];
    for row in &stream_rows {
        stream_journal
            .append(row.clone().to_event(stream_writer.clone()), None)
            .await
            .expect("append stream");
    }
    stream_journal
        .append(
            make_eof_event(stream_writer.clone(), stream_rows.len() as u64),
            None,
        )
        .await
        .expect("append stream eof");

    // Build stage resources and extract join resources.
    let mut resources_set = StageResourcesBuilder::new(
        flow_id,
        system_id,
        topology,
        system_journal,
        stage_journals,
        error_journals,
    )
    .build()
    .await
    .expect("stage resources should build");

    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    // Build join handler via StrictJoinBuilder.
    let handler = StrictJoinBuilder::<CatalogRow, StreamRow, JoinedRow>::new()
        .catalog_key(|c| c.key.clone())
        .stream_key(|s| s.key.clone())
        .build(|catalog, stream| JoinedRow {
            source: catalog.value,
            stream: stream.key,
        });

    let mut join_config = JoinConfig::new(
        join_stage,
        "join_supervisor_replay",
        "replay_flow",
        reference_stage,
        stream_stage,
    );
    join_config.control_strategy = Some(Arc::new(JonestownStrategy));

    let handle = JoinBuilder::new(
        handler,
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        Arc::new(JonestownStrategy),
    )
    .expect("build join builder")
    .build()
    .await
    .expect("build join supervisor");

    handle.initialize().await.expect("initialize join");
    handle.ready().await.expect("ready join");

    for _ in 0..200 {
        if handle.is_terminal() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(
        handle.is_terminal(),
        "join supervisor did not terminate; final state = {:?}",
        handle.current_state()
    );

    let events = join_journal
        .read_causally_ordered()
        .await
        .expect("read join journal");

    events
        .iter()
        .filter_map(|env| JoinedRow::from_event(&env.event))
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn join_supervisor_replay_produces_identical_joins() {
    let joined_run1 = run_join_supervisor_once().await;
    let joined_run2 = run_join_supervisor_once().await;

    assert_eq!(joined_run1.len(), 1);
    assert_eq!(joined_run1, joined_run2);
    assert_eq!(
        joined_run1[0],
        JoinedRow {
            source: "v1".into(),
            stream: "k1".into()
        }
    );
}
