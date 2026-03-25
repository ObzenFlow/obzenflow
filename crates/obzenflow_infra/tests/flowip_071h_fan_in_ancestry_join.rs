// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::sync::Arc;

use obzenflow_core::event::chain_event::ChainEventFactory;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
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
