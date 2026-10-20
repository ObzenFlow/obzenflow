// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::HashMap;
use std::sync::Arc;

use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::Journal;
use obzenflow_core::{
    ChainEvent, FlowId, JournalWriterId, ReaderGeneration, StageId, StageKey, WriterId,
};
use obzenflow_fsm::{EventVariant, FsmAction, FsmError, StateMachine, StateVariant};

use super::fsm::{MergeAction, MergeContext, MergeEvent, MergeInputs, MergeResources, MergeState};
use super::supervisor::MergeSupervisor;
use crate::backpressure::BackpressureWriter;
use crate::execution::{RuntimeExecution, RuntimeMode};
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::ReaderFrontier;
use crate::messaging::DeliveredRecord;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::JonestownSignalStrategy;
use crate::stages::common::input_facts::{EofRead, WatermarkRead};
use crate::stages::common::supervision::catch_up::test_support::{
    caught_up_through, context, open_at, RecordingJournal,
};
use crate::stages::common::supervision::catch_up::CatchUpAction;
use crate::stages::merges::MergeStrategyKind;
use crate::stages::resources_builder::SubscriptionFactory;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::testing::causal_fixture::{committed_input, coordinate};

struct Merge {
    machine: StateMachine<MergeState, MergeEvent, MergeContext, MergeAction>,
    ctx: MergeContext,
}

impl Merge {
    fn new(state: MergeState, execution: RuntimeExecution) -> Self {
        let stage_id = StageId::new();
        let journal = RecordingJournal::new();
        let data_journal: Arc<dyn Journal<ChainEvent>> = journal.clone();
        let empty = SubscriptionFactory::new(HashMap::new()).bind(&[]);
        let ctx = MergeContext::new(MergeResources {
            stage_id,
            stage_name: "trades".to_string(),
            flow_name: "flow".to_string(),
            flow_id: FlowId::new(),
            strategy: MergeStrategyKind::ThenLive,
            writer_id: None,
            runtime_execution: execution.clone(),
            lineage_policy: Default::default(),
            data_journal,
            instrumentation: Arc::new(StageInstrumentation::new()),
            control_strategy: Arc::new(JonestownSignalStrategy),
            processing_context: Default::default(),
            first_phase_subscription_factory: empty.clone(),
            held_subscription_factory: empty,
            first_phase: Default::default(),
            held: Default::default(),
            held_producers: Vec::new(),
            backpressure_writer: BackpressureWriter::disabled(),
            backpressure_readers: HashMap::new(),
            output_contract: StageOutputContract::empty(),
            pending: None,
            backpressure_pulse: BackpressureActivityPulse::new(),
            backpressure_stall: Default::default(),
            heartbeat: None,
            catch_up: context(stage_id, "trades", execution, Some(journal)),
        });
        let supervisor = MergeSupervisor::new("trades".to_string(), stage_id);
        Self {
            machine: supervisor.build_state_machine(state),
            ctx,
        }
    }

    fn resumed(state: MergeState) -> Self {
        Self::new(
            state,
            RuntimeExecution::resume_for_test(ReaderGeneration(2)),
        )
    }

    async fn send(&mut self, event: MergeEvent) -> Vec<String> {
        let actions = self
            .machine
            .handle(event, &mut self.ctx)
            .await
            .expect("the merge machine handles the fact");
        actions.iter().map(shape).collect()
    }

    async fn author_eof(&mut self) -> (EofKind, Option<VectorClock>) {
        let actions = self
            .machine
            .handle(MergeEvent::DrainComplete, &mut self.ctx)
            .await
            .expect("the merge drains");
        match actions.into_iter().next() {
            Some(MergeAction::AuthorEof { kind, clock }) => (kind, clock),
            other => panic!("the merge authors its EOF first, found {other:?}"),
        }
    }

    fn state(&self) -> &MergeState {
        self.machine.state()
    }
}

fn shape(action: &MergeAction) -> String {
    match action {
        MergeAction::CatchUp(CatchUpAction::AuthorWatermarks { through, .. }) => {
            format!("watermarks through {}", through.0)
        }
        MergeAction::CatchUp(CatchUpAction::RecordBoundary { generation }) => {
            format!("boundary {}", generation.0)
        }
        MergeAction::PromoteHeld(generation) => format!("promote {}", generation.0),
        MergeAction::ForwardControl(_) => "forward".to_string(),
        MergeAction::ReleaseInputs => "release".to_string(),
        MergeAction::AuthorEof { kind, .. } => format!("author_eof {kind:?}"),
        MergeAction::SendCompletion => "completion".to_string(),
        MergeAction::SendFailure { .. } => "failure".to_string(),
        MergeAction::Cleanup => "cleanup".to_string(),
        MergeAction::Host(SupervisorAction::CloseMailbox) => "close_mailbox".to_string(),
        MergeAction::Host(SupervisorAction::SettlePublications) => "settle".to_string(),
        MergeAction::Host(SupervisorAction::Emit(event)) => {
            format!("emit {}", event.variant_name())
        }
        other => format!("{other:?}"),
    }
}

fn first_phase() -> MergeState {
    MergeState::FirstPhase(MergeInputs::default())
}

fn clock_of(author: StageId) -> VectorClock {
    let mut clock = VectorClock::new();
    clock.clocks.insert(coordinate(&author.to_string()), 3);
    clock
}

fn eof(author: StageId, kind: EofKind, ends_reads: bool, frontier: ReaderFrontier) -> MergeEvent {
    let mut event = ChainEventFactory::eof_event_with_kind(WriterId::from(author), kind);
    if let ChainPayload::FlowControl(FlowControlPayload::Eof { vector_clock, .. }) =
        &mut event.payload
    {
        *vector_clock = Some(clock_of(author));
    }
    let record = committed_input(JournalWriterId::new(), event);
    MergeEvent::EofRead(EofRead {
        envelope: Arc::new(DeliveredRecord::from(record)),
        ends_reads,
        side_worst: Some(kind),
        frontier,
        generation: frontier.highest(),
        ended_by: Some(StageKey::from("producer")),
    })
}

fn not_crossing() -> ReaderFrontier {
    open_at(0, 0)
}

fn watermark(generation: u64, frontier: ReaderFrontier) -> MergeEvent {
    MergeEvent::WatermarkRead(WatermarkRead {
        generation: ReaderGeneration(generation),
        frontier,
    })
}

#[tokio::test]
async fn a_first_phase_crossing_promotes_the_held_producers_once() {
    let mut merge = Merge::resumed(first_phase());
    assert_eq!(
        merge.send(watermark(1, caught_up_through(1))).await,
        ["watermarks through 1", "boundary 1", "promote 1"]
    );
    assert_eq!(merge.state().variant_name(), "FirstPhase");
    assert!(merge
        .send(watermark(1, caught_up_through(1)))
        .await
        .is_empty());
}

#[tokio::test]
async fn an_unreached_frontier_changes_nothing() {
    let mut merge = Merge::resumed(first_phase());
    assert!(merge.send(watermark(1, open_at(0, 1))).await.is_empty());
    assert_eq!(merge.state(), &first_phase());
}

#[tokio::test]
async fn an_eof_that_completes_the_frontier_crosses_without_ending_the_first_phase() {
    let mut merge = Merge::resumed(first_phase());
    let actions = merge
        .send(eof(
            StageId::new(),
            EofKind::Natural,
            false,
            caught_up_through(1),
        ))
        .await;
    assert_eq!(
        actions,
        ["watermarks through 1", "boundary 1", "promote 1", "forward"]
    );
    assert_eq!(merge.state().variant_name(), "FirstPhase");
}

#[tokio::test]
async fn a_first_phase_that_ends_naturally_reads_the_held_producers() {
    let mut merge = Merge::resumed(first_phase());
    let actions = merge
        .send(eof(StageId::new(), EofKind::Natural, true, not_crossing()))
        .await;
    assert_eq!(actions, ["forward"]);
    assert_eq!(merge.state().variant_name(), "LastPhase");
}

#[tokio::test]
async fn a_last_phase_crossing_promotes_nothing() {
    let mut merge = Merge::resumed(MergeState::LastPhase(MergeInputs::default()));
    assert_eq!(
        merge.send(watermark(1, caught_up_through(1))).await,
        ["watermarks through 1", "boundary 1"]
    );
    assert_eq!(merge.state().variant_name(), "LastPhase");
}

#[tokio::test]
async fn a_replay_crossing_records_no_boundary() {
    let mut merge = Merge::new(
        first_phase(),
        RuntimeExecution::new(RuntimeMode::Replay, None),
    );
    assert_eq!(
        merge.send(watermark(1, caught_up_through(1))).await,
        ["watermarks through 1", "promote 1"]
    );
}

#[tokio::test]
async fn a_merge_drains_with_the_clock_of_the_eof_that_ended_its_reading() {
    let batch = StageId::new();
    let mut left_open = Merge::resumed(first_phase());
    left_open
        .send(eof(batch, EofKind::Poison, true, not_crossing()))
        .await;
    assert_eq!(left_open.state().variant_name(), "Draining");
    assert_eq!(
        left_open.author_eof().await,
        (EofKind::Poison, Some(clock_of(batch)))
    );

    let live = StageId::new();
    let mut ended = Merge::resumed(first_phase());
    ended
        .send(eof(batch, EofKind::Natural, true, not_crossing()))
        .await;
    ended
        .send(eof(live, EofKind::Natural, false, not_crossing()))
        .await;
    assert_eq!(ended.state().variant_name(), "LastPhase");
    ended
        .send(eof(live, EofKind::Natural, true, not_crossing()))
        .await;
    assert_eq!(
        ended.author_eof().await,
        (EofKind::Natural, Some(clock_of(live)))
    );
    assert_eq!(ended.state().variant_name(), "Finalizing");
}

#[tokio::test]
async fn each_way_into_draining_releases_the_merge_inputs() {
    let mut left_open = Merge::resumed(first_phase());
    assert_eq!(
        left_open
            .send(eof(StageId::new(), EofKind::Poison, true, not_crossing()))
            .await,
        ["forward", "release"]
    );
    assert_eq!(left_open.state().variant_name(), "Draining");

    let mut ended = Merge::resumed(MergeState::LastPhase(MergeInputs::default()));
    assert_eq!(
        ended
            .send(eof(StageId::new(), EofKind::Natural, true, not_crossing()))
            .await,
        ["forward", "release"]
    );
    assert_eq!(ended.state().variant_name(), "Draining");
}

#[tokio::test]
async fn the_drain_authors_the_merge_eof_then_completes() {
    let mut merge = Merge::resumed(first_phase());
    merge
        .send(eof(
            StageId::new(),
            EofKind::Truncated,
            true,
            not_crossing(),
        ))
        .await;
    assert_eq!(
        merge.send(MergeEvent::DrainComplete).await,
        [
            "author_eof Truncated",
            "completion",
            "cleanup",
            "close_mailbox",
            "settle",
            "emit FinalizationCompleted",
        ]
    );
}

#[tokio::test]
async fn ready_leaves_each_reading_state_as_it_was() {
    let mut merge = Merge::resumed(first_phase());
    merge.send(watermark(1, caught_up_through(1))).await;
    merge
        .send(eof(StageId::new(), EofKind::Poison, false, not_crossing()))
        .await;
    let first = merge.state().clone();
    assert_ne!(first, first_phase());
    assert!(merge.send(MergeEvent::Ready).await.is_empty());
    assert_eq!(merge.state(), &first);

    merge
        .send(eof(StageId::new(), EofKind::Natural, true, not_crossing()))
        .await;
    let last = merge.state().clone();
    assert_eq!(last.variant_name(), "LastPhase");
    assert!(merge.send(MergeEvent::Ready).await.is_empty());
    assert_eq!(merge.state(), &last);
}

#[tokio::test]
async fn a_promotion_without_its_held_subscription_fails() {
    let mut merge = Merge::resumed(first_phase());
    let error = MergeAction::PromoteHeld(ReaderGeneration(1))
        .execute(&mut merge.ctx)
        .await
        .unwrap_err();
    assert!(
        matches!(&error, FsmError::HandlerError(detail)
            if detail == "merge 'trades' has no held subscription to promote"),
        "{error:?}"
    );
}

#[tokio::test]
async fn every_merge_state_keeps_its_name() {
    let mut merge = Merge::resumed(first_phase());
    merge
        .send(eof(StageId::new(), EofKind::Poison, true, not_crossing()))
        .await;
    let draining = merge.state().clone();
    let names: Vec<_> = [
        MergeState::Created,
        MergeState::Initializing,
        MergeState::Initialized,
        MergeState::Starting,
        first_phase(),
        MergeState::LastPhase(MergeInputs::default()),
        draining,
        MergeState::Finalizing,
        MergeState::Drained,
        MergeState::Failing(String::new()),
        MergeState::Failed(String::new()),
        MergeState::Cancelling(String::new()),
        MergeState::Cancelled(String::new()),
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
            "FirstPhase",
            "LastPhase",
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
