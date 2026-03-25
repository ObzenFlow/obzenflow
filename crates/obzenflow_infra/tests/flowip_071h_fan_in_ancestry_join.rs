// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::HashSet;
use std::sync::Arc;

use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::event::{ChainEvent, ChainEventContent};
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{FlowId, StageId, SystemId, TypedPayload, WriterId};
use obzenflow_infra::journal::disk::disk_journal::DiskJournal;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::stages::common::control_strategies::JonestownStrategy;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::join::handle::JoinHandleExt;
use obzenflow_runtime::stages::join::{JoinBuilder, JoinConfig};
use obzenflow_runtime::stages::resources_builder::StageResourcesBuilder;
use obzenflow_runtime::stages::JoinHandler;
use obzenflow_runtime::supervised_base::SupervisorBuilder;
use obzenflow_runtime::supervised_base::SupervisorHandle;
use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};
use serde::{Deserialize, Serialize};

/// Helper: create a FlowControl EOF event with advertised writer_seq.
fn make_eof_event(writer: WriterId, seq: u64) -> ChainEvent {
    let mut eof = ChainEventFactory::eof_event(writer, true);
    if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
        ref mut writer_id,
        ref mut writer_seq,
        ..
    }) = eof.content
    {
        *writer_id = Some(writer);
        *writer_seq = Some(SeqNo(seq));
    }
    eof
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefRow {
    key: String,
}

impl TypedPayload for RefRow {
    const EVENT_TYPE: &'static str = "test.flowip_071h.ref_row";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StreamRow {
    key: String,
}

impl TypedPayload for StreamRow {
    const EVENT_TYPE: &'static str = "test.flowip_071h.stream_row";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct DrainOnlyOutput {
    note: String,
}

impl TypedPayload for DrainOnlyOutput {
    const EVENT_TYPE: &'static str = "test.flowip_071h.drain_only_output";
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct MatchOutput {
    key: String,
}

impl TypedPayload for MatchOutput {
    const EVENT_TYPE: &'static str = "test.flowip_071h.match_output";
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct FanOutOutput {
    idx: u8,
}

impl TypedPayload for FanOutOutput {
    const EVENT_TYPE: &'static str = "test.flowip_071h.fan_out_output";
}

#[derive(Clone, Debug)]
struct DrainOnlyJoin {
    writer_id: WriterId,
}

#[async_trait::async_trait]
impl JoinHandler for DrainOnlyJoin {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn process_event(
        &self,
        _state: &mut Self::State,
        _event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        // Intentionally emit no outputs during normal processing so that the only
        // output is produced during drain-time (FLOWIP-071h zero-output edge case).
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

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![DrainOnlyOutput {
            note: "drain".to_string(),
        }
        .to_event(self.writer_id)])
    }
}

#[derive(Clone, Debug)]
struct KeyMatchJoin;

#[async_trait::async_trait]
impl JoinHandler for KeyMatchJoin {
    type State = HashSet<String>;

    fn initial_state(&self) -> Self::State {
        HashSet::new()
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(reference) = RefRow::from_event(&event) {
            state.insert(reference.key);
            return Ok(Vec::new());
        }

        if let Some(stream) = StreamRow::from_event(&event) {
            if state.contains(&stream.key) {
                return Ok(vec![MatchOutput { key: stream.key }.to_event(writer_id)]);
            }
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

#[derive(Clone, Debug)]
struct FanOutJoin {
    count: u8,
}

#[async_trait::async_trait]
impl JoinHandler for FanOutJoin {
    type State = HashSet<String>;

    fn initial_state(&self) -> Self::State {
        HashSet::new()
    }

    fn process_event(
        &self,
        state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if let Some(reference) = RefRow::from_event(&event) {
            state.insert(reference.key);
            return Ok(Vec::new());
        }

        if let Some(stream) = StreamRow::from_event(&event) {
            if state.contains(&stream.key) {
                return Ok((0..self.count)
                    .map(|idx| FanOutOutput { idx }.to_event(writer_id))
                    .collect());
            }
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

#[derive(Clone, Debug)]
struct ErrorOnStreamJoin;

#[async_trait::async_trait]
impl JoinHandler for ErrorOnStreamJoin {
    type State = ();

    fn initial_state(&self) -> Self::State {}

    fn process_event(
        &self,
        _state: &mut Self::State,
        event: ChainEvent,
        _source_id: StageId,
        _writer_id: WriterId,
    ) -> Result<Vec<ChainEvent>, HandlerError> {
        if StreamRow::from_event(&event).is_some() {
            return Err(HandlerError::other("boom"));
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
async fn drain_only_output_inherits_reference_and_stream_ancestry_even_if_no_outputs_before_draining(
) {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    // Topology: reference -> join, stream -> join.
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

    // Reference side: one row + EOF so the supervisor hydrates and observes reference ancestry.
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(RefRow { key: "k1".into() }.to_event(reference_writer), None)
        .await
        .expect("append reference data");
    reference_journal
        .append(make_eof_event(reference_writer, 1), None)
        .await
        .expect("append reference eof");

    // Stream side: one row + EOF. No outputs will be emitted prior to draining.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(
            StreamRow {
                key: "missing".into(),
            }
            .to_event(stream_writer),
            None,
        )
        .await
        .expect("append stream data");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
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
        "flowip_071h_drain_only",
        "flowip_071h",
        reference_stage,
        stream_stage,
    );
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        DrainOnlyJoin {
            writer_id: WriterId::from(join_stage),
        },
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
    let drain_env = events
        .iter()
        .find(|env| DrainOnlyOutput::from_event(&env.event).is_some())
        .expect("expected a drain-only output event");

    // FLOWIP-071h: drain-time outputs must still preserve ancestry from both contributors,
    // including the zero-output edge case (no prior output to "carry" reference ancestry).
    let reference_key = reference_writer.to_string();
    let stream_key = stream_writer.to_string();
    assert_ne!(
        drain_env.vector_clock.get(&reference_key),
        0,
        "drain-only output vector clock must include reference writer ancestry"
    );
    assert_ne!(
        drain_env.vector_clock.get(&stream_key),
        0,
        "drain-only output vector clock must include stream writer ancestry"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn conservative_reference_ancestry_overclaims_distinct_reference_writers() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    // Topology: reference -> join, stream -> join.
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

    // Reference side: two records from distinct writers.
    //
    // Conservative interim (FLOWIP-071h): the join supervisor merges reference-side ancestry
    // into a single high-water clock. This intentionally over-claims ancestry for a match
    // against only one reference row.
    let reference_writer_a = WriterId::from(reference_stage);
    let reference_writer_b = WriterId::from(StageId::new());
    reference_journal
        .append(
            RefRow { key: "k1".into() }.to_event(reference_writer_a),
            None,
        )
        .await
        .expect("append reference a");
    reference_journal
        .append(
            RefRow { key: "k2".into() }.to_event(reference_writer_b),
            None,
        )
        .await
        .expect("append reference b");
    reference_journal
        .append(make_eof_event(reference_writer_a, 1), None)
        .await
        .expect("append reference eof");

    // Stream side: match only the first reference key.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamRow { key: "k1".into() }.to_event(stream_writer), None)
        .await
        .expect("append stream");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
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
        "flowip_071h_overclaim",
        "flowip_071h",
        reference_stage,
        stream_stage,
    );
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        KeyMatchJoin,
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
    let matched_env = events
        .iter()
        .find(|env| MatchOutput::from_event(&env.event).is_some())
        .expect("expected a matched output event");
    let matched = MatchOutput::from_event(&matched_env.event).expect("parse matched payload");

    assert_eq!(matched, MatchOutput { key: "k1".into() });

    // Over-claim is intentional under the conservative interim: output depends on k1 only,
    // but merges the reference high-water clock, which includes writer_b.
    let key_a = reference_writer_a.to_string();
    let key_b = reference_writer_b.to_string();
    let key_stream = stream_writer.to_string();
    assert_ne!(
        matched_env.vector_clock.get(&key_a),
        0,
        "matched output must include reference writer a ancestry"
    );
    assert_ne!(
        matched_env.vector_clock.get(&key_stream),
        0,
        "matched output must include stream writer ancestry"
    );
    assert_ne!(
        matched_env.vector_clock.get(&key_b),
        0,
        "conservative interim must over-claim by including reference writer b ancestry"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn fan_out_outputs_all_carry_merged_ancestry_from_both_sides() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    // Topology: reference -> join, stream -> join.
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

    // Reference side: one key + EOF.
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(RefRow { key: "k1".into() }.to_event(reference_writer), None)
        .await
        .expect("append reference");
    reference_journal
        .append(make_eof_event(reference_writer, 1), None)
        .await
        .expect("append reference eof");

    // Stream side: one hit + EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamRow { key: "k1".into() }.to_event(stream_writer), None)
        .await
        .expect("append stream");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
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
        "flowip_071h_fan_out",
        "flowip_071h",
        reference_stage,
        stream_stage,
    );
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        FanOutJoin { count: 3 },
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
    let out_envs: Vec<_> = events
        .iter()
        .filter(|env| FanOutOutput::from_event(&env.event).is_some())
        .collect();
    assert_eq!(out_envs.len(), 3, "expected 3 fan-out outputs");

    let reference_key = reference_writer.to_string();
    let stream_key = stream_writer.to_string();
    for env in out_envs {
        assert_ne!(
            env.vector_clock.get(&reference_key),
            0,
            "fan-out output must include reference ancestry"
        );
        assert_ne!(
            env.vector_clock.get(&stream_key),
            0,
            "fan-out output must include stream ancestry"
        );
    }

    let payloads: Vec<FanOutOutput> = events
        .iter()
        .filter_map(|env| FanOutOutput::from_event(&env.event))
        .collect();
    assert_eq!(
        payloads,
        vec![
            FanOutOutput { idx: 0 },
            FanOutOutput { idx: 1 },
            FanOutOutput { idx: 2 },
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn error_journal_entries_carry_merged_parent_ancestry() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let reference_stage = StageId::new();
    let stream_stage = StageId::new();
    let join_stage = StageId::new();

    // Topology: reference -> join, stream -> join.
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
    let join_error_path = tmp.path().join("join_error.log");

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
    let join_error_journal: Arc<DiskJournal<ChainEvent>> = Arc::new(
        DiskJournal::with_owner(join_error_path, JournalOwner::stage(join_stage))
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

    // Reference side: one key + EOF.
    let reference_writer = WriterId::from(reference_stage);
    reference_journal
        .append(RefRow { key: "k1".into() }.to_event(reference_writer), None)
        .await
        .expect("append reference");
    reference_journal
        .append(make_eof_event(reference_writer, 1), None)
        .await
        .expect("append reference eof");

    // Stream side: one record triggers a handler error, then EOF.
    let stream_writer = WriterId::from(stream_stage);
    stream_journal
        .append(StreamRow { key: "k1".into() }.to_event(stream_writer), None)
        .await
        .expect("append stream");
    stream_journal
        .append(make_eof_event(stream_writer, 1), None)
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
        "flowip_071h_error_journal_ancestry",
        "flowip_071h",
        reference_stage,
        stream_stage,
    );
    join_config.control_strategy = Some(control.clone());

    let handle = JoinBuilder::new(
        ErrorOnStreamJoin,
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

    let errors = join_error_journal
        .read_causally_ordered()
        .await
        .expect("read join error journal");
    let error_env = errors
        .iter()
        .find(|env| {
            matches!(
                env.event.processing_info.status,
                ProcessingStatus::Error { .. }
            )
        })
        .expect("expected an error-journal entry");

    // FLOWIP-071h: error journal writes derived from an input must use the same merged parent.
    let reference_key = reference_writer.to_string();
    let stream_key = stream_writer.to_string();
    assert_ne!(
        error_env.vector_clock.get(&reference_key),
        0,
        "error-journal entry must include reference ancestry"
    );
    assert_ne!(
        error_env.vector_clock.get(&stream_key),
        0,
        "error-journal entry must include stream ancestry"
    );
}
