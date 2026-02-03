// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, StageId, SystemId, WriterId};
use obzenflow_infra::journal::MemoryJournal;
use obzenflow_runtime_services::id_conversions::StageIdExt;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;
use obzenflow_runtime_services::stages::common::handlers::StatefulHandler;
use obzenflow_runtime_services::stages::resources_builder::StageResourcesBuilder;
use obzenflow_runtime_services::stages::stateful::{
    StatefulBuilder, StatefulConfig, StatefulHandleExt,
};
use obzenflow_runtime_services::supervised_base::SupervisorBuilder;
use obzenflow_topology::{StageType as TopologyStageType, TopologyBuilder};
use serde_json::json;

#[derive(Clone, Debug)]
struct TimerEmitHandler {
    emit_after: Duration,
    writer_id: WriterId,
}

#[derive(Clone, Debug)]
struct TimerEmitState {
    first_seen: Option<Instant>,
    buffered: usize,
}

#[async_trait::async_trait]
impl StatefulHandler for TimerEmitHandler {
    type State = TimerEmitState;

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        if state.first_seen.is_none() {
            state.first_seen = Some(Instant::now());
        }
        state.buffered += 1;
    }

    fn initial_state(&self) -> Self::State {
        TimerEmitState {
            first_seen: None,
            buffered: 0,
        }
    }

    fn should_emit(&self, state: &Self::State) -> bool {
        state
            .first_seen
            .is_some_and(|first_seen| first_seen.elapsed() >= self.emit_after)
    }

    fn create_events(&self, state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![ChainEventFactory::data_event(
            self.writer_id,
            "test.timer.emit",
            json!({ "buffered": state.buffered }),
        )])
    }

    fn emit(&self, state: &mut Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        let events = self.create_events(state)?;
        state.first_seen = None;
        state.buffered = 0;
        Ok(events)
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn stateful_emit_interval_emits_while_idle() {
    let flow_id = FlowId::new();
    let system_id = SystemId::new();

    let src = StageId::new();
    let stateful_stage = StageId::new();
    let sink = StageId::new();

    let mut topology = TopologyBuilder::new();
    topology.add_stage_with_id(
        src.to_topology_id(),
        Some("src".to_string()),
        TopologyStageType::FiniteSource,
    );
    topology.reset_current();
    topology.add_stage_with_id(
        stateful_stage.to_topology_id(),
        Some("stateful".to_string()),
        TopologyStageType::Stateful,
    );
    topology.reset_current();
    topology.add_stage_with_id(
        sink.to_topology_id(),
        Some("sink".to_string()),
        TopologyStageType::Sink,
    );
    topology.reset_current();
    topology.add_edge(src.to_topology_id(), stateful_stage.to_topology_id());
    topology.add_edge(stateful_stage.to_topology_id(), sink.to_topology_id());

    let topology = Arc::new(topology.build().expect("topology should build"));

    let src_journal = Arc::new(MemoryJournal::<ChainEvent>::with_owner(
        JournalOwner::stage(src),
    ));
    let stateful_journal = Arc::new(MemoryJournal::<ChainEvent>::with_owner(
        JournalOwner::stage(stateful_stage),
    ));
    let sink_journal = Arc::new(MemoryJournal::<ChainEvent>::with_owner(
        JournalOwner::stage(sink),
    ));

    let mut stage_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>> = HashMap::new();
    stage_journals.insert(src, src_journal.clone());
    stage_journals.insert(stateful_stage, stateful_journal.clone());
    stage_journals.insert(sink, sink_journal.clone());

    let mut error_journals: HashMap<StageId, Arc<dyn Journal<ChainEvent>>> = HashMap::new();
    error_journals.insert(src, src_journal.clone());
    error_journals.insert(stateful_stage, stateful_journal.clone());
    error_journals.insert(sink, sink_journal.clone());

    let system_journal: Arc<dyn Journal<SystemEvent>> = Arc::new(
        MemoryJournal::<SystemEvent>::with_owner(JournalOwner::system(system_id)),
    );

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

    // Seed one upstream data event to establish the baseline.
    let upstream_writer = WriterId::from(src);
    let input_event =
        ChainEventFactory::data_event(upstream_writer, "test.input", json!({ "seq": 1 }));
    src_journal
        .append(input_event, None)
        .await
        .expect("append upstream event");

    let stateful_resources = resources_set
        .take_stage_resources(stateful_stage)
        .expect("stateful resources exist");

    let emit_interval = Duration::from_millis(100);

    let handler = TimerEmitHandler {
        emit_after: Duration::from_millis(50),
        writer_id: WriterId::from(stateful_stage),
    };

    let config = StatefulConfig {
        stage_id: stateful_stage,
        stage_name: "timer_stateful".to_string(),
        flow_name: "timer_flow".to_string(),
        upstream_stages: vec![src],
        emit_interval: Some(emit_interval),
        control_strategy: None,
    };

    let handle = StatefulBuilder::new(handler, config, stateful_resources)
        .build()
        .await
        .expect("build stateful stage");

    handle.initialize().await.expect("initialize stateful");
    handle.ready().await.expect("ready stateful");

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let events = stateful_journal
            .read_causally_ordered()
            .await
            .expect("read stateful journal");
        if events
            .iter()
            .any(|env| env.event.event_type() == "test.timer.emit")
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for timer-driven emission; observed {} events in stateful journal",
            events.len()
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    // NOTE: We do not currently attempt to terminate the stage here; this test only asserts
    // that timer-driven emission happens while the stage is otherwise idle.
}
