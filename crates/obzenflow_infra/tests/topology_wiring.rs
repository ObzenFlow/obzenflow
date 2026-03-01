// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, StageId, SystemId};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::stages::resources_builder::StageResourcesBuilder;
use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};

#[tokio::test]
async fn sink_wires_to_reducer_in_simple_chain() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let src = StageId::new();
    let mapper = StageId::new();
    let reducer = StageId::new();
    let sink = StageId::new();

    let mut builder = TopologyBuilder::new();
    builder.add_stage_with_id(
        src.to_topology_id(),
        Some("src".to_string()),
        TopologyStageType::FiniteSource,
    );
    builder.reset_current();
    builder.add_stage_with_id(
        mapper.to_topology_id(),
        Some("mapper".to_string()),
        TopologyStageType::Transform,
    );
    builder.reset_current();
    builder.add_stage_with_id(
        reducer.to_topology_id(),
        Some("reducer".to_string()),
        TopologyStageType::Transform,
    );
    builder.reset_current();
    builder.add_stage_with_id(
        sink.to_topology_id(),
        Some("sink".to_string()),
        TopologyStageType::Sink,
    );
    builder.reset_current();
    builder.add_edge(src.to_topology_id(), mapper.to_topology_id());
    builder.add_edge(mapper.to_topology_id(), reducer.to_topology_id());
    builder.add_edge(reducer.to_topology_id(), sink.to_topology_id());

    let topology = Arc::new(builder.build().expect("topology should build"));

    let mut stage_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>> = HashMap::new();
    let mut error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>> = HashMap::new();

    for id in [src, mapper, reducer, sink] {
        let chain = Arc::new(MemoryJournal::<ChainEvent>::with_owner(
            JournalOwner::stage(id),
        )) as Arc<dyn Journal<ChainEvent>>;
        stage_journals.insert(id, chain.clone());
        error_journals.insert(id, chain);
    }

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        MemoryJournal::<SystemEvent>::with_owner(JournalOwner::system(system_id)),
    );

    let resources = StageResourcesBuilder::new(
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

    let sink_resources = resources
        .get_stage_resources(sink)
        .expect("sink resources exist");
    let upstream: HashSet<StageId> = sink_resources
        .upstream_journals
        .iter()
        .map(|(id, _)| *id)
        .collect();

    assert_eq!(
        upstream,
        HashSet::from([reducer]),
        "sink should subscribe to reducer journal only"
    );
}
