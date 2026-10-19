// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;

use obzenflow_core::event::payloads::execution_payload::JoinPhaseFact;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::Journal;
use obzenflow_core::{
    ChainEvent, FlowId, JournalWriterId, ReaderGeneration, StageId, StageKey, TypedPayload,
    WriterId,
};
use obzenflow_fsm::{EventVariant, FsmAction, FsmError, StateMachine, StateVariant};
use serde::{Deserialize, Serialize};

use super::config::{JoinReferenceMode, LiveStream, PhaseWitness, PhaseWitnesses};
use super::fsm::{
    DrainStep, JoinAction, JoinContext, JoinDrain, JoinEvent, JoinPlan, JoinReading, JoinResources,
    JoinState, JoinSubscriptionSide, LiveReads,
};
use super::supervisor::JoinSupervisor;
use crate::backpressure::BackpressureWriter;
use crate::execution::RuntimeExecution;
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::ReaderFrontier;
use crate::messaging::DeliveredRecord;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::JonestownSignalStrategy;
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::join::TypedJoinHandlerAdapter;
use crate::stages::common::handlers::{JoinReferenceView, TypedJoinHandler, UnifiedJoinHandler};
use crate::stages::common::input_facts::{EofRead, WatermarkRead};
use crate::stages::common::supervision::catch_up::test_support::{
    caught_up_through, context, open_at, RecordingJournal,
};
use crate::stages::common::supervision::catch_up::{CatchUpAction, CatchUpState};
use crate::stages::resources_builder::SubscriptionFactory;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::testing::causal_fixture::{committed_input, coordinate};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Price {
    key: u32,
}

impl TypedPayload for Price {
    const EVENT_TYPE: &'static str = "join.machine.price";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Order {
    key: u32,
}

impl TypedPayload for Order {
    const EVENT_TYPE: &'static str = "join.machine.order";
}

#[derive(Clone, Debug)]
struct Pricing;

impl TypedJoinHandler for Pricing {
    type State = ();
    type ReferenceKey = u32;
    type Reference = Price;
    type Stream = Order;
    type Output = Price;

    fn initial_state(&self) {}

    fn admit_reference(&self, reference: &Price) -> Result<u32, HandlerError> {
        Ok(reference.key)
    }

    fn process_stream(
        &self,
        _state: &mut Self::State,
        _references: &mut JoinReferenceView<'_, u32, Price>,
        _stream: Order,
    ) -> Result<Vec<Price>, HandlerError> {
        Ok(Vec::new())
    }
}

type Handler = TypedJoinHandlerAdapter<Pricing>;

struct Join {
    machine: StateMachine<JoinState, JoinEvent, JoinContext<Handler>, JoinAction<Handler>>,
    ctx: JoinContext<Handler>,
    journal: Arc<RecordingJournal>,
}

impl Join {
    fn new(
        state: JoinState,
        witnesses: Option<PhaseWitnesses>,
        archived_phases: Vec<JoinPhaseFact>,
    ) -> Self {
        let stage_id = StageId::new();
        let journal = RecordingJournal::new();
        let data_journal: Arc<dyn Journal<ChainEvent>> = journal.clone();
        let execution = RuntimeExecution::resume_for_test(ReaderGeneration(2));
        let handler = TypedJoinHandlerAdapter::new(Pricing);
        let empty = SubscriptionFactory::new(HashMap::new()).bind(&[]);
        let resources = JoinResources {
            handler_state: handler.initial_state(),
            handler: Some(Arc::new(handler)),
            stage_id,
            stage_name: "posted".to_string(),
            observers: Default::default(),
            runtime_execution: execution.clone(),
            flow_name: "flow".to_string(),
            flow_id: FlowId::new(),
            reference_stage_id: StageId::new(),
            data_journal: data_journal.clone(),
            error_journal: data_journal,
            bus: Arc::new(crate::message_bus::FsmMessageBus::new()),
            writer_id: None,
            lineage_policy: Default::default(),
            reference_subscription: None,
            stream_subscription: None,
            reference_contract_state: Vec::new(),
            reference_last_contract_check: None,
            stream_contract_state: Vec::new(),
            stream_last_contract_check: None,
            reference_high_water_clock: VectorClock::new(),
            instrumentation: Arc::new(StageInstrumentation::new()),
            control_strategy: Arc::new(JonestownSignalStrategy),
            processing_context: Default::default(),
            reference_subscription_factory: empty.clone(),
            stream_subscription_factory: empty,
            witness_keys: super::phase::witness_keys(witnesses.as_ref(), None),
            phase_witnesses: witnesses,
            archived_phases,
            reference_batch_cap: None,
            reference_since_last_stream: 0,
            events_since_last_heartbeat: 0,
            heartbeat_interval: 0,
            backpressure_writer: BackpressureWriter::disabled(),
            output_contract: StageOutputContract::empty(),
            backpressure_readers: HashMap::new(),
            pending_outputs: VecDeque::new(),
            pending_parent: None,
            pending_subscription_ack: None,
            backpressure_pulse: BackpressureActivityPulse::new(),
            backpressure_stall: Default::default(),
            heartbeat: None,
            recorded: None,
            recorded_order: None,
            catch_up: context(stage_id, "posted", execution, Some(journal.clone())),
        };
        let supervisor = JoinSupervisor::<Handler> {
            name: "posted".to_string(),
            stage_id,
            reference_subscription: None,
            stream_subscription: None,
            _marker: PhantomData,
        };
        Self {
            machine: supervisor.build_state_machine(state),
            ctx: JoinContext::new(resources),
            journal,
        }
    }

    async fn send(&mut self, event: JoinEvent) -> Vec<String> {
        let actions = self
            .machine
            .handle(event, &mut self.ctx)
            .await
            .expect("the join machine handles the fact");
        actions.iter().map(shape).collect()
    }

    async fn send_and_run_catch_up(&mut self, event: JoinEvent) {
        let actions = self
            .machine
            .handle(event, &mut self.ctx)
            .await
            .expect("the join machine handles the fact");
        for action in actions {
            if let JoinAction::CatchUp(action) = action {
                let resources = self.ctx.resources_mut().expect("join resources");
                action
                    .execute(&mut resources.catch_up)
                    .await
                    .expect("the catch-up action runs");
            }
        }
    }

    fn state(&self) -> &JoinState {
        self.machine.state()
    }
}

fn shape(action: &JoinAction<Handler>) -> String {
    match action {
        JoinAction::CatchUp(CatchUpAction::AuthorWatermarks { through, .. }) => {
            format!("watermarks through {}", through.0)
        }
        JoinAction::CatchUp(CatchUpAction::RecordBoundary { generation }) => {
            format!("boundary {}", generation.0)
        }
        JoinAction::PromoteHeld { side, generation } => {
            format!("promote {side:?} {}", generation.0)
        }
        JoinAction::ForwardControl(_) => "forward".to_string(),
        JoinAction::EmitHydrationHeartbeat => "heartbeat".to_string(),
        JoinAction::RecordPhase(JoinPhaseFact::EnrichingEntered { generation, .. }) => {
            format!("enriching_entered {generation}")
        }
        JoinAction::RecordPhase(JoinPhaseFact::LiveEntered { generation, .. }) => {
            format!("live_entered {generation}")
        }
        JoinAction::AuthorEof { kind, .. } => format!("author_eof {kind:?}"),
        JoinAction::SendCompletion => "completion".to_string(),
        JoinAction::SendFailure { .. } => "failure".to_string(),
        JoinAction::Cleanup => "cleanup".to_string(),
        JoinAction::Host(SupervisorAction::CloseMailbox) => "close_mailbox".to_string(),
        JoinAction::Host(SupervisorAction::SettlePublications) => "settle".to_string(),
        JoinAction::Host(SupervisorAction::Emit(event)) => format!("emit {}", event.variant_name()),
        other => format!("{other:?}"),
    }
}

fn witness(stage_key: &str) -> PhaseWitness {
    PhaseWitness {
        stage_key: StageKey::from(stage_key),
        stage_id: StageId::new(),
    }
}

fn then_live(
    catalog: Vec<PhaseWitness>,
    batch_stream: Vec<PhaseWitness>,
    live_stream: LiveStream,
) -> PhaseWitnesses {
    PhaseWitnesses {
        catalog,
        batch_stream,
        live_stream,
    }
}

fn plan(mode: JoinReferenceMode, witnesses: Option<&PhaseWitnesses>) -> JoinPlan {
    JoinPlan::new(mode, witnesses, LiveReads::Preference)
}

fn reading(plan: JoinPlan) -> JoinReading {
    JoinReading::new(plan)
}

fn clock_of(author: StageId) -> VectorClock {
    let mut clock = VectorClock::new();
    clock.clocks.insert(coordinate(&author.to_string()), 7);
    clock
}

fn eof(author: StageId, kind: EofKind, ends_reads: bool, frontier: ReaderFrontier) -> EofRead {
    let mut event = ChainEventFactory::eof_event_with_kind(WriterId::from(author), kind);
    if let ChainPayload::FlowControl(FlowControlPayload::Eof { vector_clock, .. }) =
        &mut event.payload
    {
        *vector_clock = Some(clock_of(author));
    }
    let record = committed_input(JournalWriterId::new(), event);
    EofRead {
        envelope: Arc::new(DeliveredRecord::from(record)),
        ends_reads,
        side_worst: Some(kind),
        frontier,
        generation: frontier.highest(),
        ended_by: Some(StageKey::from("upstream")),
    }
}

fn not_crossing() -> ReaderFrontier {
    open_at(0, 0)
}

fn watermark(generation: u64) -> JoinEvent {
    JoinEvent::WatermarkRead(WatermarkRead {
        generation: ReaderGeneration(generation),
        frontier: caught_up_through(generation),
    })
}

fn draining(state: &JoinState) -> &JoinDrain {
    match state {
        JoinState::Draining(drain) => drain,
        other => panic!("expected the join to drain, found {other:?}"),
    }
}

fn reading_of(state: &JoinState) -> &JoinReading {
    match state {
        JoinState::Hydrating(reading)
        | JoinState::Enriching(reading)
        | JoinState::Live(reading) => reading,
        other => panic!("expected a reading state, found {other:?}"),
    }
}

#[tokio::test]
async fn each_plan_opens_on_its_first_phase() {
    let batch = then_live(
        vec![witness("prices")],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let live_only = then_live(vec![witness("prices")], Vec::new(), LiveStream::Present);
    let batch_only = then_live(
        vec![witness("prices")],
        vec![witness("orders")],
        LiveStream::Absent,
    );
    for (plan, opens) in [
        (plan(JoinReferenceMode::Live, None), "Live"),
        (plan(JoinReferenceMode::FiniteEof, None), "Hydrating"),
        (
            plan(JoinReferenceMode::FiniteThenLive, Some(&batch)),
            "Hydrating",
        ),
        (
            plan(JoinReferenceMode::FiniteThenLive, Some(&live_only)),
            "Hydrating",
        ),
        (
            plan(JoinReferenceMode::FiniteThenLive, Some(&batch_only)),
            "Hydrating",
        ),
    ] {
        let mut join = Join::new(JoinState::Starting(plan), None, Vec::new());
        assert!(join.send(JoinEvent::ActivationCompleted).await.is_empty());
        assert_eq!(join.state().variant_name(), opens, "{plan:?}");
        assert_eq!(reading_of(join.state()).plan, plan);
    }
}

#[tokio::test]
async fn hydration_completes_when_every_catalog_witness_ends_naturally_in_either_order() {
    let (a, b) = (witness("prices_a"), witness("prices_b"));
    let witnesses = then_live(
        vec![a.clone(), b.clone()],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));
    for (first, last) in [(&a, &b), (&b, &a)] {
        let mut join = Join::new(
            JoinState::Hydrating(reading(plan)),
            Some(witnesses.clone()),
            Vec::new(),
        );
        let held = join
            .send(JoinEvent::EofRead(eof(
                first.stage_id,
                EofKind::Natural,
                false,
                not_crossing(),
            )))
            .await;
        assert_eq!(held, ["forward"]);
        assert_eq!(join.state().variant_name(), "Hydrating");
        assert_eq!(
            reading_of(join.state())
                .witnessed
                .get(&first.stage_key)
                .copied(),
            Some(EofKind::Natural)
        );

        let completed = join
            .send(JoinEvent::EofRead(eof(
                last.stage_id,
                EofKind::Natural,
                true,
                not_crossing(),
            )))
            .await;
        assert_eq!(completed, ["forward", "heartbeat", "enriching_entered 0"]);
        assert_eq!(join.state().variant_name(), "Enriching");
        assert!(reading_of(join.state()).witnessed.is_empty());
    }
}

#[tokio::test]
async fn a_plan_with_no_batch_stream_goes_from_hydrating_straight_to_live() {
    let prices = witness("prices");
    let witnesses = then_live(vec![prices.clone()], Vec::new(), LiveStream::Present);
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));
    let mut join = Join::new(
        JoinState::Hydrating(reading(plan)),
        Some(witnesses),
        Vec::new(),
    );
    let actions = join
        .send(JoinEvent::EofRead(eof(
            prices.stage_id,
            EofKind::Natural,
            true,
            not_crossing(),
        )))
        .await;
    assert_eq!(actions, ["forward", "heartbeat", "live_entered 0"]);
    assert_eq!(join.state().variant_name(), "Live");
}

#[tokio::test]
async fn a_hydration_left_open_drains_at_flush_with_no_clock() {
    let prices = witness("prices");
    let witnesses = then_live(
        vec![prices.clone()],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));
    let mut join = Join::new(
        JoinState::Hydrating(reading(plan)),
        Some(witnesses),
        Vec::new(),
    );
    let actions = join
        .send(JoinEvent::EofRead(eof(
            prices.stage_id,
            EofKind::Poison,
            true,
            not_crossing(),
        )))
        .await;
    assert_eq!(actions, ["forward", "heartbeat"]);
    let drain = draining(join.state());
    assert_eq!(drain.step, DrainStep::Flush);
    assert_eq!(drain.worst_eof, Some(EofKind::Poison));
    assert_eq!(drain.clock, None);
}

#[tokio::test]
async fn a_holding_batch_goes_live_on_its_witnesses_and_drains_when_left_open() {
    let orders = witness("orders");
    let witnesses = then_live(
        vec![witness("prices")],
        vec![orders.clone()],
        LiveStream::Present,
    );
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));

    let mut join = Join::new(
        JoinState::Enriching(reading(plan)),
        Some(witnesses.clone()),
        Vec::new(),
    );
    let actions = join
        .send(JoinEvent::EofRead(eof(
            orders.stage_id,
            EofKind::Natural,
            false,
            not_crossing(),
        )))
        .await;
    assert_eq!(actions, ["forward", "live_entered 0"]);
    assert_eq!(join.state().variant_name(), "Live");

    let mut join = Join::new(
        JoinState::Enriching(reading(plan)),
        Some(witnesses),
        Vec::new(),
    );
    let actions = join
        .send(JoinEvent::EofRead(eof(
            orders.stage_id,
            EofKind::Poison,
            true,
            not_crossing(),
        )))
        .await;
    assert_eq!(actions, ["forward"]);
    let drain = draining(join.state());
    assert_eq!(drain.step, DrainStep::Flush);
    assert_eq!(drain.clock, Some(clock_of(orders.stage_id)));
}

#[tokio::test]
async fn a_batch_with_no_live_phase_drains_at_finalize_when_its_stream_ends() {
    let orders = witness("orders");
    let witnesses = then_live(
        vec![witness("prices")],
        vec![orders.clone()],
        LiveStream::Absent,
    );
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));

    let mut join = Join::new(
        JoinState::Enriching(reading(plan)),
        Some(witnesses.clone()),
        Vec::new(),
    );
    let ended = eof(orders.stage_id, EofKind::Natural, true, not_crossing());
    let final_eof = ended.envelope.clone();
    let actions = join.send(JoinEvent::EofRead(ended)).await;
    assert_eq!(actions, ["forward"]);
    let drain = draining(join.state());
    assert_eq!(drain.step, DrainStep::Finalize { final_eof });
    assert_eq!(drain.worst_eof, Some(EofKind::Natural));
    assert_eq!(drain.clock, Some(clock_of(orders.stage_id)));

    let mut join = Join::new(
        JoinState::Enriching(reading(plan)),
        Some(witnesses),
        Vec::new(),
    );
    let actions = join
        .send(JoinEvent::EofRead(eof(
            orders.stage_id,
            EofKind::Natural,
            false,
            not_crossing(),
        )))
        .await;
    assert_eq!(actions[0], "failure");
    match join.state() {
        JoinState::Failing(cause) => assert_eq!(
            cause,
            "join 'posted' completed its batch on an EOF that did not end its stream"
        ),
        other => panic!("the join must fail closed, found {other:?}"),
    }
}

#[tokio::test]
async fn a_finite_eof_batch_reads_its_stream_to_the_end_then_checks_its_inputs() {
    let plan = plan(JoinReferenceMode::FiniteEof, None);
    let orders = StageId::new();
    let mut join = Join::new(JoinState::Enriching(reading(plan)), None, Vec::new());

    let actions = join
        .send(JoinEvent::EofRead(eof(
            orders,
            EofKind::Natural,
            false,
            not_crossing(),
        )))
        .await;
    assert_eq!(actions, ["forward"]);
    assert_eq!(join.state().variant_name(), "Enriching");

    let ended = eof(orders, EofKind::Natural, true, not_crossing());
    let final_eof = ended.envelope.clone();
    assert_eq!(join.send(JoinEvent::EofRead(ended)).await, ["forward"]);
    let drain = draining(join.state());
    assert_eq!(drain.step, DrainStep::CheckInputs { final_eof });
    assert_eq!(drain.clock, Some(clock_of(orders)));
}

#[tokio::test]
async fn live_ends_only_on_the_stream_eof_that_ends_its_reads() {
    let plan = plan(JoinReferenceMode::Live, None);
    let (prices, orders) = (StageId::new(), StageId::new());
    let mut join = Join::new(JoinState::Live(reading(plan)), None, Vec::new());

    for (author, kind) in [(prices, EofKind::Poison), (orders, EofKind::Natural)] {
        let actions = join
            .send(JoinEvent::EofRead(eof(author, kind, false, not_crossing())))
            .await;
        assert_eq!(actions, ["forward"]);
        assert_eq!(join.state().variant_name(), "Live");
    }
    assert_eq!(reading_of(join.state()).worst_eof, Some(EofKind::Poison));

    let ended = eof(orders, EofKind::Natural, true, not_crossing());
    let final_eof = ended.envelope.clone();
    assert_eq!(join.send(JoinEvent::EofRead(ended)).await, ["forward"]);
    let drain = draining(join.state());
    assert_eq!(drain.step, DrainStep::Finalize { final_eof });
    assert_eq!(drain.worst_eof, Some(EofKind::Poison));
    assert_eq!(drain.clock, Some(clock_of(orders)));
}

#[tokio::test]
async fn the_drain_runs_its_steps_in_order() {
    let orders = StageId::new();
    let final_eof = eof(orders, EofKind::Natural, true, not_crossing()).envelope;
    let drain = JoinDrain {
        step: DrainStep::CheckInputs {
            final_eof: final_eof.clone(),
        },
        worst_eof: Some(EofKind::Poison),
        clock: Some(clock_of(orders)),
    };
    let mut join = Join::new(JoinState::Draining(drain), None, Vec::new());

    assert!(join.send(JoinEvent::DrainStepEnded).await.is_empty());
    assert_eq!(
        draining(join.state()).step,
        DrainStep::Finalize {
            final_eof: final_eof.clone()
        }
    );
    assert!(join.send(JoinEvent::DrainStepEnded).await.is_empty());
    assert_eq!(draining(join.state()).step, DrainStep::Flush);

    let actions = join.send(JoinEvent::DrainComplete).await;
    assert_eq!(
        actions,
        [
            "author_eof Poison",
            "completion",
            "cleanup",
            "close_mailbox",
            "settle",
            "emit FinalizationCompleted",
        ]
    );
    assert_eq!(join.state().variant_name(), "Finalizing");

    let early = JoinDrain {
        step: DrainStep::Finalize { final_eof },
        worst_eof: None,
        clock: None,
    };
    let mut join = Join::new(JoinState::Draining(early), None, Vec::new());
    assert_eq!(join.send(JoinEvent::DrainComplete).await[0], "failure");
    assert_eq!(join.state().variant_name(), "Failing");

    let flushed = JoinDrain {
        step: DrainStep::Flush,
        worst_eof: None,
        clock: None,
    };
    let mut join = Join::new(JoinState::Draining(flushed), None, Vec::new());
    assert_eq!(join.send(JoinEvent::DrainStepEnded).await[0], "failure");
    assert_eq!(join.state().variant_name(), "Failing");
}

#[tokio::test]
async fn the_join_authors_its_eof_with_the_clock_of_the_eof_that_ended_its_reading() {
    let plan = plan(JoinReferenceMode::Live, None);
    let orders = StageId::new();
    let mut join = Join::new(JoinState::Live(reading(plan)), None, Vec::new());
    join.send(JoinEvent::EofRead(eof(
        orders,
        EofKind::Natural,
        true,
        not_crossing(),
    )))
    .await;
    join.send(JoinEvent::DrainStepEnded).await;
    let actions = join
        .machine
        .handle(JoinEvent::DrainComplete, &mut join.ctx)
        .await
        .expect("the join drains");
    match &actions[0] {
        JoinAction::AuthorEof { kind, clock } => {
            assert_eq!(*kind, EofKind::Natural);
            assert_eq!(clock.as_ref(), Some(&clock_of(orders)));
        }
        other => panic!("the join authors its EOF first, found {other:?}"),
    }
}

#[tokio::test]
async fn each_crossing_promotes_only_what_its_state_holds() {
    let witnesses = then_live(
        vec![witness("prices")],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let then_live_plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));
    for (state, promoted) in [
        (
            JoinState::Hydrating(reading(then_live_plan)),
            Some("promote Stream 1"),
        ),
        (
            JoinState::Enriching(reading(then_live_plan)),
            Some("promote Reference 1"),
        ),
        (
            JoinState::Enriching(reading(plan(JoinReferenceMode::FiniteEof, None))),
            None,
        ),
        (JoinState::Live(reading(then_live_plan)), None),
    ] {
        let name = state.variant_name().to_string();
        let mut join = Join::new(state, Some(witnesses.clone()), Vec::new());
        let mut expected = vec!["watermarks through 1", "boundary 1"];
        expected.extend(promoted);
        assert_eq!(join.send(watermark(1)).await, expected, "{name}");
        assert_eq!(join.state().variant_name(), name);
        assert_eq!(
            reading_of(join.state()).catch_up,
            CatchUpState::Rebuilding {
                crossed: ReaderGeneration(1)
            }
        );
        assert!(join.send(watermark(1)).await.is_empty(), "{name}");
    }
}

#[tokio::test]
async fn an_unreached_frontier_crosses_nothing() {
    let mut join = Join::new(
        JoinState::Hydrating(reading(plan(JoinReferenceMode::FiniteEof, None))),
        None,
        Vec::new(),
    );
    let actions = join
        .send(JoinEvent::WatermarkRead(WatermarkRead {
            generation: ReaderGeneration(1),
            frontier: open_at(0, 1),
        }))
        .await;
    assert!(actions.is_empty());
    assert_eq!(reading_of(join.state()).catch_up, CatchUpState::default());
}

#[tokio::test]
async fn a_stream_eof_that_completes_a_crossing_forwards_after_it_and_drains() {
    let plan = plan(JoinReferenceMode::Live, None);
    let orders = StageId::new();
    let mut join = Join::new(JoinState::Live(reading(plan)), None, Vec::new());
    let actions = join
        .send(JoinEvent::EofRead(eof(
            orders,
            EofKind::Natural,
            true,
            caught_up_through(1),
        )))
        .await;
    assert_eq!(actions, ["watermarks through 1", "boundary 1", "forward"]);
    assert_eq!(join.state().variant_name(), "Draining");
}

#[tokio::test]
async fn a_hold_eof_that_completes_a_crossing_promotes_before_the_forward() {
    let prices = witness("prices");
    let witnesses = then_live(
        vec![prices.clone()],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));
    let mut join = Join::new(
        JoinState::Hydrating(reading(plan)),
        Some(witnesses),
        Vec::new(),
    );
    let actions = join
        .send(JoinEvent::EofRead(eof(
            prices.stage_id,
            EofKind::Natural,
            true,
            caught_up_through(1),
        )))
        .await;
    assert_eq!(
        actions,
        [
            "watermarks through 1",
            "boundary 1",
            "promote Stream 1",
            "forward",
            "heartbeat",
            "enriching_entered 1",
        ]
    );
}

#[tokio::test]
async fn a_join_crossing_fails_when_its_archive_entered_a_later_phase() {
    let witnesses = then_live(
        vec![witness("prices")],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));
    let archived = vec![JoinPhaseFact::LiveEntered {
        generation: 0,
        ended_by: Vec::new(),
    }];

    let mut join = Join::new(
        JoinState::Enriching(reading(plan)),
        Some(witnesses.clone()),
        archived.clone(),
    );
    let actions = join.send(watermark(1)).await;
    assert!(
        actions
            .iter()
            .all(|action| !action.starts_with("watermarks")),
        "{actions:?}"
    );
    match join.state() {
        JoinState::Failing(cause) => {
            assert!(
                cause.contains("its archive had already entered live"),
                "{cause}"
            );
        }
        other => panic!("a join behind its archived phase must fail: {other:?}"),
    }
    assert!(join.journal.markers().is_empty());

    let mut caught_up = Join::new(JoinState::Live(reading(plan)), Some(witnesses), archived);
    caught_up.send_and_run_catch_up(watermark(1)).await;
    assert_eq!(caught_up.state().variant_name(), "Live");
    assert_eq!(caught_up.journal.markers(), vec![1]);
}

#[tokio::test]
async fn ready_leaves_each_reading_state_as_it_was() {
    let prices = witness("prices");
    let witnesses = then_live(
        vec![prices.clone()],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let plan = plan(JoinReferenceMode::FiniteThenLive, Some(&witnesses));
    let held = JoinReading {
        plan,
        catch_up: CatchUpState::Rebuilding {
            crossed: ReaderGeneration(1),
        },
        worst_eof: Some(EofKind::Poison),
        witnessed: Arc::new(BTreeMap::from([(prices.stage_key, EofKind::Natural)])),
    };
    for state in [
        JoinState::Hydrating(held.clone()),
        JoinState::Enriching(held.clone()),
        JoinState::Live(held.clone()),
    ] {
        let mut join = Join::new(state.clone(), Some(witnesses.clone()), Vec::new());
        assert!(join.send(JoinEvent::Ready).await.is_empty());
        assert_eq!(join.state(), &state);
    }
}

#[tokio::test]
async fn a_promotion_without_its_held_subscription_fails() {
    let plan = plan(JoinReferenceMode::FiniteEof, None);
    for (side, input) in [
        (JoinSubscriptionSide::Reference, "catalog"),
        (JoinSubscriptionSide::Stream, "stream"),
    ] {
        let mut join = Join::new(JoinState::Hydrating(reading(plan)), None, Vec::new());
        let error = JoinAction::<Handler>::PromoteHeld {
            side,
            generation: ReaderGeneration(1),
        }
        .execute(&mut join.ctx)
        .await
        .unwrap_err();
        let expected = format!("join 'posted' has no {input} subscription to promote");
        assert!(
            matches!(&error, FsmError::HandlerError(detail) if *detail == expected),
            "{error:?}"
        );
    }
}

#[test]
fn every_join_state_keeps_its_name() {
    let plan = plan(JoinReferenceMode::FiniteEof, None);
    let drain = JoinDrain {
        step: DrainStep::Flush,
        worst_eof: None,
        clock: None,
    };
    let names: Vec<_> = [
        JoinState::Created(plan),
        JoinState::Initializing(plan),
        JoinState::Initialized(plan),
        JoinState::Starting(plan),
        JoinState::Hydrating(reading(plan)),
        JoinState::Enriching(reading(plan)),
        JoinState::Live(reading(plan)),
        JoinState::Draining(drain),
        JoinState::Finalizing,
        JoinState::Drained,
        JoinState::Failing(String::new()),
        JoinState::Failed(String::new()),
        JoinState::Cancelling(String::new()),
        JoinState::Cancelled(String::new()),
    ]
    .iter()
    .map(|state| state.variant_name().to_string())
    .collect();
    assert_eq!(
        names,
        [
            "Created",
            "Initializing",
            "Initialized",
            "Starting",
            "Hydrating",
            "Enriching",
            "Live",
            "Draining",
            "Finalizing",
            "Drained",
            "Failing",
            "Failed",
            "Cancelling",
            "Cancelled",
        ]
    );
}

#[test]
fn a_plan_without_a_live_phase_has_no_live_read_policy() {
    let batch = then_live(
        vec![witness("prices")],
        vec![witness("orders")],
        LiveStream::Present,
    );
    let live_only = then_live(vec![witness("prices")], Vec::new(), LiveStream::Present);
    let batch_only = then_live(
        vec![witness("prices")],
        vec![witness("orders")],
        LiveStream::Absent,
    );
    for (plan, reads) in [
        (
            plan(JoinReferenceMode::FiniteThenLive, Some(&batch)),
            Some(LiveReads::Preference),
        ),
        (
            plan(JoinReferenceMode::FiniteThenLive, Some(&live_only)),
            Some(LiveReads::Preference),
        ),
        (
            plan(JoinReferenceMode::FiniteThenLive, Some(&batch_only)),
            None,
        ),
        (plan(JoinReferenceMode::FiniteEof, None), None),
        (
            JoinPlan::new(JoinReferenceMode::Live, None, LiveReads::Recorded),
            Some(LiveReads::Recorded),
        ),
    ] {
        assert_eq!(plan.live_reads(), reads, "{plan:?}");
    }
    assert!(plan(JoinReferenceMode::FiniteThenLive, Some(&batch_only)).enriching_holds_catalog());
    assert!(!plan(JoinReferenceMode::FiniteEof, None).enriching_holds_catalog());
}
