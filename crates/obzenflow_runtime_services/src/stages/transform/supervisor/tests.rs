// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform supervisor tests (moved verbatim from supervisor.rs, FLOWIP-051m).

use super::*;
use crate::backpressure::{BackpressurePlan, BackpressureRegistry};
use crate::id_conversions::StageIdExt;
use crate::pipeline::config::CycleGuardConfig;
use crate::stages::common::control_strategies::JonestownStrategy;
use crate::stages::common::cycle_guard::CycleGuard;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::TransformHandler;
use crate::stages::resources_builder::SubscriptionFactory;
use crate::supervised_base::HandlerSupervised;
use async_trait::async_trait;
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::identity::JournalWriterId;
use obzenflow_core::event::journal_event::JournalEvent;
use obzenflow_core::event::vector_clock::CausalOrderingService;
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::id::JournalId;
use obzenflow_core::journal::journal_error::JournalError;
use obzenflow_core::journal::journal_owner::JournalOwner;
use obzenflow_core::journal::journal_reader::JournalReader;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, StageId, WriterId};
use obzenflow_fsm::FsmAction;
use obzenflow_topology::TopologyBuilder;
use serde_json::json;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio_test::{assert_pending, assert_ready};

async fn build_cycle_entry_harness<
    H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
    F: FnOnce(StageId) -> H,
>(
    handler_factory: F,
) -> (
    TransformSupervisor<H>,
    TransformContext<H>,
    BackpressureRegistry,
    StageId,
    StageId,
    StageId,
    Arc<dyn Journal<ChainEvent>>,
) {
    let mut builder = TopologyBuilder::new();
    let s_top = builder.add_stage(Some("s".to_string())); // external upstream
    let t_top = builder.add_stage(Some("t".to_string())); // entry point
    let u_top = builder.add_stage(Some("u".to_string())); // cycle peer
    builder.add_backward_edge(u_top, t_top); // u -> t (backflow)
    let topology = builder.build_unchecked().expect("topology");

    let s = StageId::from_topology_id(s_top);
    let t = StageId::from_topology_id(t_top);
    let u = StageId::from_topology_id(u_top);

    let plan = BackpressurePlan::disabled()
        .track_only_edge(t, u)
        .track_only_edge(u, t);
    let registry = BackpressureRegistry::new(&topology, &plan);

    let upstream_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(s)));
    let internal_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(u)));
    let data_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(t)));
    let error_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(t)));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(t)));

    let mut stage_names = HashMap::new();
    stage_names.insert(s, "s".to_string());
    stage_names.insert(t, "t".to_string());
    stage_names.insert(u, "u".to_string());
    let subscription_factory = SubscriptionFactory::new(stage_names);
    let mut upstream_subscription_factory =
        subscription_factory.bind(&[(s, upstream_journal.clone()), (u, internal_journal.clone())]);
    upstream_subscription_factory.owner_label = "t".to_string();

    let instrumentation = Arc::new(crate::metrics::instrumentation::StageInstrumentation::new());
    let control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy> =
        Arc::new(JonestownStrategy);

    let mut backpressure_readers = HashMap::new();
    backpressure_readers.insert(s, registry.reader(s, t));
    backpressure_readers.insert(u, registry.reader(u, t));

    let cycle_guard_config = CycleGuardConfig {
        max_iterations: crate::pipeline::MaxIterations::new(30),
        scc_id: obzenflow_core::SccId::from_ulid(obzenflow_core::Ulid::from(0u128)),
        external_upstreams: [s].into_iter().collect(),
        internal_upstreams: [u].into_iter().collect(),
        is_entry_point: true,
        scc_internal_edges: vec![(t, u), (u, t)],
    };

    let handler = handler_factory(t);
    let mut ctx = TransformContext {
        handler,
        stage_id: t,
        stage_name: "t".to_string(),
        flow_name: "cycle_test_flow".to_string(),
        flow_id: FlowId::new(),
        data_journal: data_journal.clone(),
        error_journal,
        system_journal: system_journal.clone(),
        writer_id: None,
        subscription: None,
        contract_state: Vec::new(),
        control_strategy,
        buffered_eof: None,
        instrumentation,
        upstream_subscription_factory,
        backpressure_writer: registry.writer(t),
        backpressure_readers,
        pending_outputs: std::collections::VecDeque::new(),
        pending_parent: None,
        pending_ack_upstream: None,
        backpressure_pulse:
            crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
        backpressure_backoff:
            crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                std::time::Duration::from_millis(1),
                std::time::Duration::from_millis(50),
            ),
        backpressure_registry: std::sync::Arc::new(registry.clone()),
        cycle_guard_config: Some(cycle_guard_config),
        external_eofs_received: std::collections::HashSet::new(),
        drain_received: false,
        buffered_terminal_envelope: None,
    };

    TransformAction::AllocateResources
        .execute(&mut ctx)
        .await
        .expect("allocate resources");

    let supervisor = TransformSupervisor::<H> {
        name: "transform_test".to_string(),
        data_journal: data_journal.clone(),
        system_journal,
        stage_id: t,
        subscription: None,
        cycle_guard: Some(CycleGuard::new(
            crate::pipeline::MaxIterations::new(30),
            obzenflow_core::SccId::from_ulid(obzenflow_core::Ulid::from(0u128)),
            true,
            "t".to_string(),
        )),
        _marker: std::marker::PhantomData,
    };

    (supervisor, ctx, registry, s, t, u, upstream_journal)
}

struct TestJournal<T: JournalEvent> {
    id: JournalId,
    owner: Option<JournalOwner>,
    seq: AtomicU64,
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
}

impl<T: JournalEvent> TestJournal<T> {
    fn new(owner: JournalOwner) -> Self {
        Self {
            id: JournalId::new(),
            owner: Some(owner),
            seq: AtomicU64::new(0),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed).saturating_add(1)
    }
}

struct TestJournalReader<T: JournalEvent> {
    events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    pos: usize,
}

#[async_trait]
impl<T: JournalEvent + 'static> Journal<T> for TestJournal<T> {
    fn id(&self) -> &JournalId {
        &self.id
    }

    fn owner(&self) -> Option<&JournalOwner> {
        self.owner.as_ref()
    }

    async fn append(
        &self,
        event: T,
        parent: Option<&EventEnvelope<T>>,
    ) -> Result<EventEnvelope<T>, JournalError> {
        let mut env = EventEnvelope::new(JournalWriterId::from(self.id), event);

        if let Some(parent) = parent {
            CausalOrderingService::update_with_parent(&mut env.vector_clock, &parent.vector_clock);
        }

        let writer_key = env.event.writer_id().to_string();
        let seq = self.next_seq();
        env.vector_clock.clocks.insert(writer_key, seq);

        let mut guard = self.events.lock().unwrap();
        guard.push(env.clone());
        Ok(env)
    }

    async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        Ok(guard.clone())
    }

    async fn read_causally_after(
        &self,
        _after_event_id: &obzenflow_core::EventId,
    ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        Ok(Vec::new())
    }

    async fn read_event(
        &self,
        _event_id: &obzenflow_core::EventId,
    ) -> Result<Option<EventEnvelope<T>>, JournalError> {
        Ok(None)
    }

    async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(TestJournalReader {
            events: self.events.clone(),
            pos: 0,
        }))
    }

    async fn reader_from(&self, position: u64) -> Result<Box<dyn JournalReader<T>>, JournalError> {
        Ok(Box::new(TestJournalReader {
            events: self.events.clone(),
            pos: position as usize,
        }))
    }

    async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        let len = guard.len();
        let start = len.saturating_sub(count);
        Ok(guard[start..].iter().rev().cloned().collect())
    }
}

#[async_trait]
impl<T: JournalEvent + 'static> JournalReader<T> for TestJournalReader<T> {
    async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
        let guard = self.events.lock().unwrap();
        if self.pos >= guard.len() {
            Ok(None)
        } else {
            let env = guard.get(self.pos).cloned();
            self.pos += 1;
            Ok(env)
        }
    }

    async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
        let start = self.pos as u64;
        self.pos = (self.pos as u64 + n) as usize;
        Ok((self.pos as u64).saturating_sub(start))
    }

    fn position(&self) -> u64 {
        self.pos as u64
    }

    fn is_at_end(&self) -> bool {
        let guard = self.events.lock().unwrap();
        self.pos >= guard.len()
    }
}

#[derive(Clone, Debug)]
struct ExpandHandler {
    writer_id: WriterId,
}

#[async_trait]
impl TransformHandler for ExpandHandler {
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(vec![
            ChainEventFactory::data_event(self.writer_id, "bp_test.expand_out", json!({ "n": 1 })),
            ChainEventFactory::data_event(self.writer_id, "bp_test.expand_out", json!({ "n": 2 })),
        ])
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FilterHandler;

#[async_trait]
impl TransformHandler for FilterHandler {
    fn process(&self, _event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        Ok(Vec::new())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }
}

async fn build_transform_harness<
    H: TransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
    F: FnOnce(StageId) -> H,
>(
    handler_factory: F,
    upstream_window: u64,
    transform_window: u64,
) -> (
    TransformSupervisor<H>,
    TransformContext<H>,
    BackpressureRegistry,
    StageId,
    StageId,
    StageId,
    Arc<dyn Journal<ChainEvent>>,
    Arc<dyn Journal<ChainEvent>>,
) {
    let mut builder = TopologyBuilder::new();
    let s_top = builder.add_stage(Some("s".to_string()));
    let t_top = builder.add_stage(Some("t".to_string()));
    let k_top = builder.add_stage(Some("k".to_string()));
    let topology = builder.build_unchecked().expect("topology");

    let s = StageId::from_topology_id(s_top);
    let t = StageId::from_topology_id(t_top);
    let k = StageId::from_topology_id(k_top);

    let plan = BackpressurePlan::disabled()
        .with_stage_window(
            s,
            NonZeroU64::new(upstream_window).expect("upstream_window"),
        )
        .with_stage_window(
            t,
            NonZeroU64::new(transform_window).expect("transform_window"),
        );
    let registry = BackpressureRegistry::new(&topology, &plan);

    let upstream_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(s)));
    let data_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(t)));
    let error_journal: Arc<dyn Journal<ChainEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(t)));
    let system_journal: Arc<dyn Journal<SystemEvent>> =
        Arc::new(TestJournal::new(JournalOwner::stage(t)));

    let mut stage_names = HashMap::new();
    stage_names.insert(s, "s".to_string());
    stage_names.insert(t, "t".to_string());
    stage_names.insert(k, "k".to_string());
    let subscription_factory = SubscriptionFactory::new(stage_names);
    let mut upstream_subscription_factory =
        subscription_factory.bind(&[(s, upstream_journal.clone())]);
    upstream_subscription_factory.owner_label = "t".to_string();

    let instrumentation = Arc::new(crate::metrics::instrumentation::StageInstrumentation::new());
    let control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy> =
        Arc::new(JonestownStrategy);

    let mut backpressure_readers = HashMap::new();
    backpressure_readers.insert(s, registry.reader(s, t));

    let handler = handler_factory(t);
    let mut ctx = TransformContext {
        handler,
        stage_id: t,
        stage_name: "t".to_string(),
        flow_name: "bp_test_flow".to_string(),
        flow_id: FlowId::new(),
        data_journal: data_journal.clone(),
        error_journal,
        system_journal: system_journal.clone(),
        writer_id: None,
        subscription: None,
        contract_state: Vec::new(),
        control_strategy,
        buffered_eof: None,
        instrumentation,
        upstream_subscription_factory,
        backpressure_writer: registry.writer(t),
        backpressure_readers,
        pending_outputs: std::collections::VecDeque::new(),
        pending_parent: None,
        pending_ack_upstream: None,
        backpressure_pulse:
            crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse::new(),
        backpressure_backoff:
            crate::supervised_base::idle_backoff::IdleBackoff::exponential_with_cap(
                std::time::Duration::from_millis(1),
                std::time::Duration::from_millis(50),
            ),
        backpressure_registry: std::sync::Arc::new(registry.clone()),
        cycle_guard_config: None,
        external_eofs_received: std::collections::HashSet::new(),
        drain_received: false,
        buffered_terminal_envelope: None,
    };

    TransformAction::AllocateResources
        .execute(&mut ctx)
        .await
        .expect("allocate resources");

    let supervisor = TransformSupervisor::<H> {
        name: "transform_test".to_string(),
        data_journal: data_journal.clone(),
        system_journal,
        stage_id: t,
        subscription: None,
        cycle_guard: None,
        _marker: std::marker::PhantomData,
    };

    (
        supervisor,
        ctx,
        registry,
        s,
        t,
        k,
        upstream_journal,
        data_journal,
    )
}

#[tokio::test]
async fn expand_transform_defers_upstream_ack_until_all_outputs_written() {
    let (mut supervisor, mut ctx, registry, s, t, k, upstream_journal, data_journal) =
        build_transform_harness(
            |t| ExpandHandler {
                writer_id: WriterId::from(t),
            },
            1,
            1,
        )
        .await;

    // Seed upstream writer_seq to make ack effects observable via credit changes.
    let upstream_writer = registry.writer(s);
    upstream_writer.reserve(1).expect("seed reserve").commit(1);

    let input = ChainEventFactory::data_event(WriterId::from(s), "bp_test.in", json!({}));
    upstream_journal
        .append(input, None)
        .await
        .expect("append input");

    let state = TransformState::<ExpandHandler>::Running;
    let directive = supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch");
    assert!(matches!(directive, EventLoopDirective::Continue));

    // One output written, one pending due to window=1.
    let events = data_journal
        .read_causally_ordered()
        .await
        .expect("read outputs");
    let outputs_written = events
        .iter()
        .filter(|env| env.event.is_data() && env.event.event_type() == "bp_test.expand_out")
        .count();
    assert_eq!(outputs_written, 1);
    assert_eq!(ctx.pending_outputs.len(), 1);
    assert_eq!(
        upstream_writer.min_downstream_credit(),
        0,
        "upstream should not be acked yet"
    );

    // Unblock downstream and drain pending output; this should trigger the deferred upstream ack.
    registry.reader(t, k).ack_consumed(1);
    supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch drain");

    let events = data_journal
        .read_causally_ordered()
        .await
        .expect("read outputs");
    let outputs_written = events
        .iter()
        .filter(|env| env.event.is_data() && env.event.event_type() == "bp_test.expand_out")
        .count();
    assert_eq!(outputs_written, 2);
    assert!(ctx.pending_outputs.is_empty());
    assert_eq!(
        upstream_writer.min_downstream_credit(),
        1,
        "upstream ack should be observed"
    );
}

#[tokio::test]
async fn filter_transform_acks_upstream_even_with_zero_outputs() {
    let (mut supervisor, mut ctx, registry, s, _t, _k, upstream_journal, _data_journal) =
        build_transform_harness(|_| FilterHandler, 1, 1).await;

    let upstream_writer = registry.writer(s);
    upstream_writer.reserve(1).expect("seed reserve").commit(1);

    let input = ChainEventFactory::data_event(WriterId::from(s), "bp_test.in", json!({}));
    upstream_journal
        .append(input, None)
        .await
        .expect("append input");

    let state = TransformState::<FilterHandler>::Running;
    supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch");

    assert_eq!(
        upstream_writer.min_downstream_credit(),
        1,
        "filter must ack upstream even when it emits 0 outputs"
    );
}

#[tokio::test]
async fn backpressure_ack_uses_subscription_upstream_stage_not_event_writer_id() {
    let (mut supervisor, mut ctx, registry, s, t, _k, upstream_journal, _data_journal) =
        build_transform_harness(|_| FilterHandler, 1, 1).await;

    let upstream_writer = registry.writer(s);
    upstream_writer.reserve(1).expect("seed reserve").commit(1);

    // WriterId is not required to match the topology upstream stage; it can be preserved
    // across stages for attribution. Backpressure MUST still ack the edge based on which
    // upstream journal produced the event.
    let input = ChainEventFactory::data_event(WriterId::from(t), "bp_test.in", json!({}));
    upstream_journal
        .append(input, None)
        .await
        .expect("append input");

    let state = TransformState::<FilterHandler>::Running;
    supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch");

    assert_eq!(
        upstream_writer.min_downstream_credit(),
        1,
        "ack should be based on subscription upstream stage, not event.writer_id"
    );
}

#[tokio::test]
async fn downstream_stall_blocks_with_sleep_no_hot_loop() {
    tokio::time::pause();

    let (mut supervisor, mut ctx, registry, _s, t, _k, _upstream_journal, _data_journal) =
        build_transform_harness(
            |t| ExpandHandler {
                writer_id: WriterId::from(t),
            },
            1,
            1,
        )
        .await;

    // Exhaust downstream credits for this stage so the next reserve will block.
    ctx.backpressure_writer
        .reserve(1)
        .expect("seed reserve")
        .commit(1);
    ctx.pending_outputs.push_back(ChainEventFactory::data_event(
        WriterId::from(t),
        "bp_test.pending",
        json!({}),
    ));

    let state = TransformState::<ExpandHandler>::Running;
    let mut task =
        tokio_test::task::spawn(async { supervisor.dispatch_state(&state, &mut ctx).await });

    assert_pending!(task.poll());

    let waited = registry
        .metrics_snapshot()
        .stage_wait_nanos_total
        .get(&t)
        .copied()
        .unwrap_or(0);
    assert_eq!(waited, 1_000_000, "1ms initial backoff expected");

    assert_pending!(task.poll());

    // Complete the 1ms sleep and ensure the supervisor returns (no hot loop).
    tokio::time::advance(std::time::Duration::from_millis(2)).await;
    let result = assert_ready!(task.poll());
    assert!(result.is_ok());
    drop(task);

    // Still blocked on the same pending output: the next dispatch should back off to 2ms.
    let mut task =
        tokio_test::task::spawn(async { supervisor.dispatch_state(&state, &mut ctx).await });
    assert_pending!(task.poll());
    let waited = registry
        .metrics_snapshot()
        .stage_wait_nanos_total
        .get(&t)
        .copied()
        .unwrap_or(0);
    assert_eq!(waited, 3_000_000, "1ms + 2ms backoff expected");
}

#[tokio::test]
async fn entry_point_buffers_external_eof_until_scc_quiescent() {
    let (mut supervisor, mut ctx, registry, s, t, u, upstream_journal) =
        build_cycle_entry_harness(|_| FilterHandler).await;

    // Simulate in-flight cycle work on u -> t so the buffered EOF cannot be released immediately.
    let u_writer = registry.writer(u);
    u_writer.reserve(1).expect("reserve").commit(1);

    upstream_journal
        .append(ChainEventFactory::eof_event(WriterId::from(s), true), None)
        .await
        .expect("append eof");

    let state = TransformState::<FilterHandler>::Running;
    let directive = supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch eof");
    assert!(
        matches!(directive, EventLoopDirective::Continue),
        "expected entry point to buffer EOF and continue"
    );
    assert!(ctx.buffered_terminal_envelope.is_some());
    assert!(ctx.external_eofs_received.contains(&s));

    let forwarded = ctx
        .data_journal
        .read_causally_ordered()
        .await
        .expect("read data journal")
        .into_iter()
        .any(|env| env.event.is_eof());
    assert!(
        !forwarded,
        "expected EOF not to be forwarded while in-flight"
    );

    // Once the SCC is quiescent, the next dispatch releases the buffered EOF.
    registry.reader(u, t).ack_consumed(1);
    let directive = supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch release");
    assert!(matches!(
        directive,
        EventLoopDirective::Transition(TransformEvent::ReceivedEOF)
    ));

    let forwarded = ctx
        .data_journal
        .read_causally_ordered()
        .await
        .expect("read data journal")
        .into_iter()
        .any(|env| env.event.is_eof());
    assert!(forwarded, "expected EOF to be forwarded after quiescence");
}

#[tokio::test]
async fn entry_point_buffers_drain_until_scc_quiescent() {
    let (mut supervisor, mut ctx, registry, s, t, u, upstream_journal) =
        build_cycle_entry_harness(|_| FilterHandler).await;

    // Simulate in-flight cycle work on u -> t so the buffered Drain cannot be released immediately.
    let u_writer = registry.writer(u);
    u_writer.reserve(1).expect("reserve").commit(1);

    upstream_journal
        .append(ChainEventFactory::drain_event(WriterId::from(s)), None)
        .await
        .expect("append drain");

    let state = TransformState::<FilterHandler>::Running;
    let directive = supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch drain");
    assert!(
        matches!(directive, EventLoopDirective::Continue),
        "expected entry point to buffer drain and continue"
    );
    assert!(ctx.buffered_terminal_envelope.is_some());
    assert!(ctx.drain_received);

    let forwarded = ctx
        .data_journal
        .read_causally_ordered()
        .await
        .expect("read data journal")
        .into_iter()
        .any(|env| {
            matches!(
                env.event.content,
                obzenflow_core::event::ChainEventContent::FlowControl(
                    obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::Drain
                )
            )
        });
    assert!(
        !forwarded,
        "expected drain not to be forwarded while in-flight"
    );

    // Once the SCC is quiescent, the next dispatch releases the buffered Drain.
    registry.reader(u, t).ack_consumed(1);
    let directive = supervisor
        .dispatch_state(&state, &mut ctx)
        .await
        .expect("dispatch release");
    assert!(matches!(
        directive,
        EventLoopDirective::Transition(TransformEvent::ReceivedEOF)
    ));

    let forwarded = ctx
        .data_journal
        .read_causally_ordered()
        .await
        .expect("read data journal")
        .into_iter()
        .any(|env| {
            matches!(
                env.event.content,
                obzenflow_core::event::ChainEventContent::FlowControl(
                    obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload::Drain
                )
            )
        });
    assert!(forwarded, "expected drain to be forwarded after quiescence");
}
