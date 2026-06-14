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
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use obzenflow_runtime::stages::common::handlers::StatefulHandler;
use obzenflow_runtime::stages::resources_builder::StageResourcesBuilder;
use obzenflow_runtime::stages::stateful::{StatefulBuilder, StatefulConfig, StatefulHandleExt};
use obzenflow_runtime::supervised_base::SupervisorBuilder;
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

    fn should_emit(&self, state: &mut Self::State) -> bool {
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

/// FLOWIP-114o: a stateful handler whose `should_emit` depends only on buffered
/// state, not on any wall clock. The only timing gate is the supervisor's
/// `emit_interval` baseline (`last_data_event_time`), so this handler isolates
/// that baseline for the paused-time test below.
#[derive(Clone, Debug)]
struct AlwaysEmitHandler {
    writer_id: WriterId,
}

#[derive(Clone, Debug)]
struct AlwaysEmitState {
    buffered: usize,
}

#[async_trait::async_trait]
impl StatefulHandler for AlwaysEmitHandler {
    type State = AlwaysEmitState;

    fn accumulate(&mut self, state: &mut Self::State, _event: ChainEvent) {
        state.buffered += 1;
    }

    fn initial_state(&self) -> Self::State {
        AlwaysEmitState { buffered: 0 }
    }

    fn should_emit(&self, state: &mut Self::State) -> bool {
        state.buffered > 0
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
        state.buffered = 0;
        Ok(events)
    }

    async fn drain(&self, _state: &Self::State) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }
}

/// FLOWIP-114o: the supervisor-driven `emit_interval` baseline reads
/// `tokio::time::Instant`, so Tokio paused time advances it. This test seeds one
/// event to establish the baseline, advances virtual time past the interval, and
/// asserts the timer-driven emission fires. It fails on the pre-FLOWIP-114o
/// `std::time::Instant` baseline, which paused time cannot advance, so
/// `baseline.elapsed()` never crosses the interval and no emission occurs.
#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn stateful_emit_interval_advances_under_paused_time() {
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
    let handler = AlwaysEmitHandler {
        writer_id: WriterId::from(stateful_stage),
    };

    let config = StatefulConfig {
        stage_id: stateful_stage,
        stage_name: "paused_timer_stateful".to_string(),
        flow_name: "paused_timer_flow".to_string(),
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

    // Advance virtual time in steps, yielding so the spawned supervisor task runs:
    // it processes the seeded event (setting the tokio-time baseline), then on a
    // later idle poll observes `baseline.elapsed() >= emit_interval` and emits.
    // No wall-clock sleeps are used.
    let mut emitted = false;
    for _ in 0..40 {
        tokio::time::advance(Duration::from_millis(50)).await;
        tokio::task::yield_now().await;
        let events = stateful_journal
            .read_causally_ordered()
            .await
            .expect("read stateful journal");
        if events
            .iter()
            .any(|env| env.event.event_type() == "test.timer.emit")
        {
            emitted = true;
            break;
        }
    }

    assert!(
        emitted,
        "emit-interval did not fire under paused time; the supervisor baseline \
         (last_data_event_time) must read tokio::time::Instant (FLOWIP-114o)"
    );
}
