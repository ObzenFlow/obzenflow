use std::sync::Arc;

use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::ChainEventContent;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::{ChainEvent, FlowId, StageId, SystemId, TypedPayload, WriterId};
use obzenflow_infra::journal::disk::disk_journal::DiskJournal;
use obzenflow_runtime_services::id_conversions::StageIdExt;
use obzenflow_runtime_services::stages::common::control_strategies::JonestownStrategy;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::join::handle::JoinHandleExt;
use obzenflow_runtime_services::stages::join::{JoinBuilder, JoinConfig, JoinReferenceMode};
use obzenflow_runtime_services::stages::resources_builder::StageResourcesBuilder;
use obzenflow_runtime_services::stages::JoinHandler;
use obzenflow_runtime_services::supervised_base::SupervisorBuilder;
use obzenflow_runtime_services::supervised_base::SupervisorHandle;
use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};
use serde::{Deserialize, Serialize};

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct CatalogRow {
    key: String,
    value: String,
}

impl TypedPayload for CatalogRow {
    const EVENT_TYPE: &'static str = "test.catalog";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamRow {
    key: String,
}

impl TypedPayload for StreamRow {
    const EVENT_TYPE: &'static str = "test.stream";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct JoinedRow {
    value: String,
    key: String,
}

impl TypedPayload for JoinedRow {
    const EVENT_TYPE: &'static str = "test.joined";
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_processes_stream_without_reference_eof() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

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
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_path = tmp.path().join("reference.log");
    let stream_path = tmp.path().join("stream.log");
    let join_path = tmp.path().join("join.log");

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
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits data but never EOF.
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(
            CatalogRow {
                key: "k1".into(),
                value: "v1".into(),
            }
            .to_event(reference_writer.clone()),
            None,
        )
        .await
        .expect("append reference data");

    // Stream emits one matching record and EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(
            StreamRow { key: "k1".into() }.to_event(stream_writer.clone()),
            None,
        )
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer.clone(), 1), None)
        .await
        .expect("append stream eof");

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
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let handler = obzenflow_runtime_services::stages::join::InnerJoinBuilder::<
        CatalogRow,
        StreamRow,
        JoinedRow,
    >::new()
    .catalog_key(|c| c.key.clone())
    .stream_key(|s| s.key.clone())
    .build(|catalog, stream| JoinedRow {
        value: catalog.value,
        key: stream.key,
    });

    let control = Arc::new(JonestownStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_no_ref_eof",
        "flowip_084g",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        handler,
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
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
    let joined: Vec<JoinedRow> = events
        .iter()
        .filter_map(|env| JoinedRow::from_event(&env.event))
        .collect();

    assert_eq!(
        joined,
        vec![JoinedRow {
            value: "v1".into(),
            key: "k1".into()
        }]
    );
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefEvent {
    id: u64,
}

impl TypedPayload for RefEvent {
    const EVENT_TYPE: &'static str = "test.ref";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamEvent {
    id: u64,
}

impl TypedPayload for StreamEvent {
    const EVENT_TYPE: &'static str = "test.stream_event";
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct StreamObservedRefs {
    refs_seen: usize,
}

impl TypedPayload for StreamObservedRefs {
    const EVENT_TYPE: &'static str = "test.stream_observed_refs";
}

#[derive(Clone, Debug)]
struct CountReferenceEventsJoin;

#[async_trait::async_trait]
impl JoinHandler for CountReferenceEventsJoin {
    type State = usize;

    fn initial_state(&self) -> Self::State {
        0
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if RefEvent::from_event(&event).is_some() {
            *state += 1;
            return Ok(Vec::new());
        }

        if StreamEvent::from_event(&event).is_some() {
            return Ok(vec![
                StreamObservedRefs { refs_seen: *state }.to_event(writer_id)
            ]);
        }

        Ok(Vec::new())
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_reference_batch_cap_prevents_stream_starvation() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

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
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_path = tmp.path().join("reference.log");
    let stream_path = tmp.path().join("stream.log");
    let join_path = tmp.path().join("join.log");

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
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits many records, never EOF.
    let reference_writer = WriterId::from(reference_stage);
    for i in 0..10u64 {
        reference_journal
            .append(RefEvent { id: i }.to_event(reference_writer.clone()), None)
            .await
            .expect("append reference data");
    }

    // Stream emits one record + EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamEvent { id: 1 }.to_event(stream_writer.clone()), None)
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer.clone(), 1), None)
        .await
        .expect("append stream eof");

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
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let control = Arc::new(JonestownStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_batch_cap",
        "flowip_084g",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.reference_batch_cap = Some(3);
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        CountReferenceEventsJoin,
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
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
    let observed: Vec<StreamObservedRefs> = events
        .iter()
        .filter_map(|env| StreamObservedRefs::from_event(&env.event))
        .collect();

    assert_eq!(observed, vec![StreamObservedRefs { refs_seen: 3 }]);
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_forwards_reference_eof() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

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
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("reference.log"),
            JournalOwner::stage(reference_stage),
        )
        .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("stream.log"),
            JournalOwner::stage(stream_stage),
        )
        .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(tmp.path().join("join.log"), JournalOwner::stage(join_stage))
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
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits data + EOF (in Live mode EOF should be forwarded but not drive completion).
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(
            CatalogRow {
                key: "k1".into(),
                value: "v1".into(),
            }
            .to_event(reference_writer.clone()),
            None,
        )
        .await
        .expect("append reference data");
    reference_journal
        .append(make_eof_event(reference_writer.clone(), 1), None)
        .await
        .expect("append reference eof");

    // Stream emits one matching record and EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(
            StreamRow { key: "k1".into() }.to_event(stream_writer.clone()),
            None,
        )
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer.clone(), 1), None)
        .await
        .expect("append stream eof");

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
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let handler = obzenflow_runtime_services::stages::join::InnerJoinBuilder::<
        CatalogRow,
        StreamRow,
        JoinedRow,
    >::new()
    .catalog_key(|c| c.key.clone())
    .stream_key(|s| s.key.clone())
    .build(|catalog, stream| JoinedRow {
        value: catalog.value,
        key: stream.key,
    });

    let control = Arc::new(JonestownStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_reference_eof_forwarded",
        "flowip_084g",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        handler,
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
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

    let joined: Vec<JoinedRow> = events
        .iter()
        .filter_map(|env| JoinedRow::from_event(&env.event))
        .collect();
    assert_eq!(
        joined,
        vec![JoinedRow {
            value: "v1".into(),
            key: "k1".into()
        }]
    );

    let saw_reference_eof = events.iter().any(|env| {
        env.event.writer_id == reference_writer
            && matches!(
                &env.event.content,
                ChainEventContent::FlowControl(FlowControlPayload::Eof { .. })
            )
    });
    assert!(saw_reference_eof, "expected reference EOF to be forwarded");
}

#[derive(Clone, Debug)]
struct ReferenceErrorJoin;

#[async_trait::async_trait]
impl JoinHandler for ReferenceErrorJoin {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn process_event(
        &self,
        _state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if CatalogRow::from_event(&event).is_some() {
            return Err(HandlerError::Remote("boom".to_string()));
        }

        if let Some(stream) = StreamRow::from_event(&event) {
            return Ok(vec![JoinedRow {
                value: "ok".into(),
                key: stream.key,
            }
            .to_event(writer_id)]);
        }

        Ok(Vec::new())
    }

    fn on_source_eof(
        &self,
        _state: &mut Self::State,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn live_join_reference_errors_are_per_record() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

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
    let topology = Arc::new(topo_builder.build_unchecked().expect("build topology"));

    let tmp = tempfile::tempdir().expect("tempdir");
    let reference_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("reference.log"),
            JournalOwner::stage(reference_stage),
        )
        .expect("create reference disk journal"),
    );
    let stream_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("stream.log"),
            JournalOwner::stage(stream_stage),
        )
        .expect("create stream disk journal"),
    );
    let join_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(tmp.path().join("join.log"), JournalOwner::stage(join_stage))
            .expect("create join disk journal"),
    );
    let join_error_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(
            tmp.path().join("join_error.log"),
            JournalOwner::stage(join_stage),
        )
        .expect("create join error disk journal"),
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
    error_journals.insert(
        reference_stage,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    error_journals.insert(
        stream_stage,
        stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );
    error_journals.insert(
        join_stage,
        join_error_journal.clone() as Arc<dyn Journal<ChainEvent>>,
    );

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        DiskJournal::<SystemEvent>::with_owner(
            tmp.path().join("system.log"),
            JournalOwner::system(system_id),
        )
        .expect("create system disk journal"),
    );

    // Reference emits one record that will fail the handler.
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(
            CatalogRow {
                key: "k1".into(),
                value: "v1".into(),
            }
            .to_event(reference_writer.clone()),
            None,
        )
        .await
        .expect("append reference data");

    // Stream emits one record + EOF (must still be processed).
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(
            StreamRow { key: "k1".into() }.to_event(stream_writer.clone()),
            None,
        )
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer.clone(), 1), None)
        .await
        .expect("append stream eof");

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
    .expect("build stage resources");
    let join_resources = resources_set
        .take_stage_resources(join_stage)
        .expect("join resources exist");

    let control = Arc::new(JonestownStrategy);
    let mut join_config = JoinConfig::new(
        join_stage,
        "live_join_reference_error_per_record",
        "flowip_084g",
        reference_stage,
        stream_stage,
    );
    join_config.reference_mode = JoinReferenceMode::Live;
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        ReferenceErrorJoin,
        join_config,
        join_resources,
        reference_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        vec![(
            stream_stage,
            stream_journal.clone() as Arc<dyn Journal<ChainEvent>>,
        )],
        control,
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

    let output_events = join_journal
        .read_causally_ordered()
        .await
        .expect("read join journal");
    let joined: Vec<JoinedRow> = output_events
        .iter()
        .filter_map(|env| JoinedRow::from_event(&env.event))
        .collect();
    assert_eq!(
        joined,
        vec![JoinedRow {
            value: "ok".into(),
            key: "k1".into()
        }]
    );

    let error_events = join_error_journal
        .read_causally_ordered()
        .await
        .expect("read join error journal");
    let saw_reference_error = error_events.iter().any(|env| {
        env.event.writer_id == reference_writer
            && CatalogRow::from_event(&env.event).is_some()
            && matches!(
                env.event.processing_info.status,
                ProcessingStatus::Error { .. }
            )
    });
    assert!(
        saw_reference_error,
        "expected reference error event in error journal"
    );
}
