// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::{ActionDeparture, EffectHistory};
use crate::execution::RuntimeExecution;
use crate::messaging::upstream_subscription::ReaderFrontier;
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handler_error::StageFatal;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEventFactory, StageFatalCode, StageFatalReason};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, ReaderGeneration, StageId, StageKey, WriterId};
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, FsmError, StateMachine, StateVariant, Transition,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum CatchUpDisposition {
    Consumed,
    Crossed,
    Failed(StageFatal),
    Refused(StageFatal),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CatchUpState {
    Rebuilding { crossed: ReaderGeneration },
    Announcing { crossed: ReaderGeneration },
    Live { crossed: ReaderGeneration },
    Refused { fatal: StageFatal },
    Failed { fatal: StageFatal },
}

impl Default for CatchUpState {
    fn default() -> Self {
        Self::Rebuilding {
            crossed: ReaderGeneration(0),
        }
    }
}

impl CatchUpState {
    fn crossed(&self) -> Option<ReaderGeneration> {
        match self {
            Self::Rebuilding { crossed }
            | Self::Announcing { crossed }
            | Self::Live { crossed } => Some(*crossed),
            Self::Refused { .. } | Self::Failed { .. } => None,
        }
    }
}

impl StateVariant for CatchUpState {
    fn variant_name(&self) -> &str {
        match self {
            Self::Rebuilding { .. } => "Rebuilding",
            Self::Announcing { .. } => "Announcing",
            Self::Live { .. } => "Live",
            Self::Refused { .. } => "Refused",
            Self::Failed { .. } => "Failed",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum CatchUpEvent {
    FrontierReached {
        generation: ReaderGeneration,
        frontier: ReaderFrontier,
    },
    InputsEnded,
    RecordedCrossingReached {
        generation: ReaderGeneration,
    },
    RecordedActionsExhausted,
    UnsettledDelivery {
        edges_redelivering: usize,
    },
    RecordedInputEnded {
        recorded: ReaderGeneration,
        generation: ReaderGeneration,
    },
    HandoffAnnounced,
}

impl EventVariant for CatchUpEvent {
    fn variant_name(&self) -> &str {
        match self {
            Self::FrontierReached { .. } => "FrontierReached",
            Self::InputsEnded => "InputsEnded",
            Self::RecordedCrossingReached { .. } => "RecordedCrossingReached",
            Self::RecordedActionsExhausted => "RecordedActionsExhausted",
            Self::UnsettledDelivery { .. } => "UnsettledDelivery",
            Self::RecordedInputEnded { .. } => "RecordedInputEnded",
            Self::HandoffAnnounced => "HandoffAnnounced",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CatchUpAction {
    AuthorWatermarks {
        after: ReaderGeneration,
        through: ReaderGeneration,
    },
    RecordBoundary {
        generation: ReaderGeneration,
    },
}

pub(crate) struct MarkerWriter {
    pub(crate) writer_id: WriterId,
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    pub(crate) instrumentation: Arc<StageInstrumentation>,
    pub(crate) flow_context: FlowContext,
}

enum Recorded {
    Nothing,
    Actions(Arc<EffectHistory>),
}

pub(crate) struct CatchUpContext {
    stage_id: StageId,
    stage_key: StageKey,
    writer: Option<MarkerWriter>,
    execution: RuntimeExecution,
    recorded: Recorded,
}

impl FsmContext for CatchUpContext {}

pub(crate) struct FrontierStep {
    pub(crate) next: CatchUpState,
    pub(crate) actions: Vec<CatchUpAction>,
    pub(crate) crossed_into: Option<ReaderGeneration>,
}

pub(crate) fn frontier_step(
    state: &CatchUpState,
    generation: ReaderGeneration,
    frontier: ReaderFrontier,
    ctx: &CatchUpContext,
) -> FrontierStep {
    match state {
        CatchUpState::Rebuilding { crossed }
            if generation > *crossed && frontier.caught_up(generation) =>
        {
            let Transition {
                next_state,
                actions,
            } = ctx.frontier(*crossed, generation);
            let crossed_into = next_state.crossed().filter(|reached| reached > crossed);
            FrontierStep {
                next: next_state,
                actions,
                crossed_into,
            }
        }
        _ => FrontierStep {
            next: state.clone(),
            actions: Vec::new(),
            crossed_into: None,
        },
    }
}

impl CatchUpContext {
    pub(crate) fn producing(
        flow_context: FlowContext,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        instrumentation: Arc<StageInstrumentation>,
        execution: RuntimeExecution,
    ) -> Self {
        let stage_id = flow_context.stage_id;
        let stage_key = StageKey::from(flow_context.stage_name.clone());
        let writer = MarkerWriter {
            writer_id: WriterId::from(stage_id),
            data_journal,
            instrumentation,
            flow_context,
        };
        Self::new(stage_id, stage_key, Some(writer), execution)
    }

    fn new(
        stage_id: StageId,
        stage_key: StageKey,
        writer: Option<MarkerWriter>,
        execution: RuntimeExecution,
    ) -> Self {
        Self {
            stage_id,
            stage_key,
            writer,
            execution,
            recorded: Recorded::Nothing,
        }
    }

    fn resume_generation(&self) -> Option<ReaderGeneration> {
        self.execution
            .resume_control()
            .map(|control| control.resume_generation())
    }

    fn unreproduced(&self) -> Option<StageFatal> {
        let Recorded::Actions(history) = &self.recorded else {
            return None;
        };
        let expected = history.actions().next_unreproduced()?;
        Some(ActionDeparture::skipped(self.stage_key.clone(), None, expected).stage_fatal())
    }

    fn failure(&self, error: FsmError) -> StageFatal {
        match error {
            FsmError::UnhandledEvent { state, event } => StageFatal::new(
                StageFatalCode::Coordination,
                StageFatalReason::CoordinationFailure,
                format!(
                    "stage '{}' received catch-up event {event} while {state}",
                    self.stage_key.as_str()
                ),
            ),
            FsmError::HandlerError(detail) => StageFatal::new(
                StageFatalCode::Journal,
                StageFatalReason::JournalFailure,
                detail,
            ),
            other => StageFatal::new(
                StageFatalCode::Coordination,
                StageFatalReason::CoordinationFailure,
                other.to_string(),
            ),
        }
    }

    fn crossing(
        &self,
        crossed: ReaderGeneration,
        generation: ReaderGeneration,
        live: bool,
    ) -> Transition<CatchUpState, CatchUpAction> {
        let mut actions = Vec::new();
        if self.writer.is_some() {
            actions.push(CatchUpAction::AuthorWatermarks {
                after: crossed,
                through: generation,
            });
        }
        if self.execution.resume_control().is_some() {
            actions.push(CatchUpAction::RecordBoundary { generation });
        }
        let next_state = if live {
            CatchUpState::Live {
                crossed: generation,
            }
        } else {
            CatchUpState::Rebuilding {
                crossed: generation,
            }
        };
        Transition {
            next_state,
            actions,
        }
    }

    fn going_live(
        &self,
        crossed: ReaderGeneration,
        generation: ReaderGeneration,
    ) -> Transition<CatchUpState, CatchUpAction> {
        match self.unreproduced() {
            Some(fatal) => to(CatchUpState::Refused { fatal }),
            None => self.crossing(crossed, generation, true),
        }
    }

    fn frontier(
        &self,
        crossed: ReaderGeneration,
        generation: ReaderGeneration,
    ) -> Transition<CatchUpState, CatchUpAction> {
        if self
            .resume_generation()
            .is_some_and(|resume| generation >= resume)
        {
            self.going_live(crossed, generation)
        } else {
            self.crossing(crossed, generation, false)
        }
    }

    fn recorded_crossing(
        &self,
        crossed: ReaderGeneration,
        generation: ReaderGeneration,
    ) -> Transition<CatchUpState, CatchUpAction> {
        match self.resume_generation() {
            Some(resume) if generation >= resume => to(CatchUpState::Failed {
                fatal: StageFatal::new(
                    StageFatalCode::Replay,
                    StageFatalReason::ReplayDivergence,
                    format!(
                        "stage '{}' reached a recorded crossing into generation {} at or above \
                         the resume generation {}",
                        self.stage_key.as_str(),
                        generation.0,
                        resume.0
                    ),
                ),
            }),
            _ if generation > crossed => self.crossing(crossed, generation, false),
            _ => to(CatchUpState::Rebuilding { crossed }),
        }
    }
}

fn to(next_state: CatchUpState) -> Transition<CatchUpState, CatchUpAction> {
    Transition {
        next_state,
        actions: Vec::new(),
    }
}

fn step(
    state: &CatchUpState,
    event: &CatchUpEvent,
    ctx: &CatchUpContext,
) -> Transition<CatchUpState, CatchUpAction> {
    let resume = ctx.resume_generation();
    match (state, event) {
        (
            _,
            CatchUpEvent::FrontierReached {
                generation,
                frontier,
            },
        ) => {
            let FrontierStep { next, actions, .. } =
                frontier_step(state, *generation, *frontier, ctx);
            Transition {
                next_state: next,
                actions,
            }
        }
        (
            CatchUpState::Rebuilding { crossed },
            CatchUpEvent::RecordedCrossingReached { generation },
        ) => ctx.recorded_crossing(*crossed, *generation),
        (CatchUpState::Rebuilding { crossed }, CatchUpEvent::RecordedActionsExhausted) => {
            match resume {
                Some(generation) if generation > *crossed => ctx.going_live(*crossed, generation),
                _ => to(state.clone()),
            }
        }
        (
            CatchUpState::Rebuilding { crossed },
            CatchUpEvent::UnsettledDelivery {
                edges_redelivering: 0,
            },
        ) => match resume {
            Some(generation) if generation > *crossed => ctx.going_live(*crossed, generation),
            _ => to(state.clone()),
        },
        (CatchUpState::Rebuilding { .. }, CatchUpEvent::InputsEnded) if resume.is_some() => {
            match ctx.unreproduced() {
                Some(fatal) => to(CatchUpState::Refused { fatal }),
                None => to(state.clone()),
            }
        }
        (
            CatchUpState::Rebuilding { crossed },
            CatchUpEvent::RecordedInputEnded {
                recorded,
                generation,
            },
        ) => Transition {
            next_state: CatchUpState::Announcing {
                crossed: *generation,
            },
            actions: ctx
                .writer
                .is_some()
                .then_some(CatchUpAction::AuthorWatermarks {
                    after: (*crossed).max(*recorded),
                    through: *generation,
                })
                .into_iter()
                .collect(),
        },
        (CatchUpState::Announcing { crossed }, CatchUpEvent::HandoffAnnounced) => Transition {
            next_state: CatchUpState::Live { crossed: *crossed },
            actions: resume
                .map(|_| CatchUpAction::RecordBoundary {
                    generation: *crossed,
                })
                .into_iter()
                .collect(),
        },
        _ => to(state.clone()),
    }
}

fn transition<'a>(
    state: &'a CatchUpState,
    event: &'a CatchUpEvent,
    ctx: &'a mut CatchUpContext,
) -> futures::future::BoxFuture<'a, Result<Transition<CatchUpState, CatchUpAction>, FsmError>> {
    let next = step(state, event, ctx);
    Box::pin(async move { Ok(next) })
}

fn refuse_unhandled<'a>(
    state: &'a CatchUpState,
    event: &'a CatchUpEvent,
    _ctx: &'a mut CatchUpContext,
) -> futures::future::BoxFuture<'a, Result<(), FsmError>> {
    let outcome = match state {
        CatchUpState::Refused { .. } | CatchUpState::Failed { .. } => Ok(()),
        _ => Err(FsmError::UnhandledEvent {
            state: state.variant_name().to_string(),
            event: event.variant_name().to_string(),
        }),
    };
    Box::pin(async move { outcome })
}

#[async_trait::async_trait]
impl FsmAction for CatchUpAction {
    type Context = CatchUpContext;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), FsmError> {
        match self {
            Self::AuthorWatermarks { after, through } => {
                let Some(writer) = ctx.writer.as_ref() else {
                    return Ok(());
                };
                for generation in after.0.saturating_add(1)..=through.0 {
                    let mut marker = ChainEventFactory::catch_up_complete_event(
                        writer.writer_id,
                        ReaderGeneration(generation),
                        ctx.stage_key.clone(),
                    );
                    marker.flow_context = writer.flow_context.clone();
                    super::output_committer::commit_control_output(
                        &writer.data_journal,
                        &writer.instrumentation,
                        marker,
                    )
                    .await
                    .map_err(|error| {
                        FsmError::HandlerError(format!(
                            "stage '{}' failed to author its catch-up watermark: {error}",
                            ctx.stage_key.as_str()
                        ))
                    })?;
                }
                tracing::info!(
                    stage_name = %ctx.stage_key.as_str(),
                    generation = through.0,
                    "stage crossed its catch-up boundary and authored its own watermark"
                );
                Ok(())
            }
            Self::RecordBoundary { generation } => {
                if let Some(control) = ctx.execution.resume_control() {
                    control.record_generation_boundary(ctx.stage_id, *generation);
                }
                Ok(())
            }
        }
    }
}

type CatchUpMachine = StateMachine<CatchUpState, CatchUpEvent, CatchUpContext, CatchUpAction>;

fn machine(initial: CatchUpState) -> CatchUpMachine {
    fsm! {
        state: CatchUpState;
        event: CatchUpEvent;
        context: CatchUpContext;
        action: CatchUpAction;
        initial: initial;
        unhandled => refuse_unhandled;
        state CatchUpState::Rebuilding {
            on CatchUpEvent::FrontierReached => transition;
            on CatchUpEvent::InputsEnded => transition;
            on CatchUpEvent::RecordedCrossingReached => transition;
            on CatchUpEvent::RecordedActionsExhausted => transition;
            on CatchUpEvent::UnsettledDelivery => transition;
            on CatchUpEvent::RecordedInputEnded => transition;
        }
        state CatchUpState::Announcing {
            on CatchUpEvent::HandoffAnnounced => transition;
        }
        state CatchUpState::Live {
            on CatchUpEvent::FrontierReached => transition;
            on CatchUpEvent::InputsEnded => transition;
            on CatchUpEvent::RecordedActionsExhausted => transition;
            on CatchUpEvent::UnsettledDelivery => transition;
        }
    }
}

struct CatchUp {
    machine: CatchUpMachine,
    context: CatchUpContext,
}

#[derive(Clone)]
pub struct CatchUpHandle(Arc<tokio::sync::Mutex<CatchUp>>);

impl CatchUpHandle {
    pub(crate) fn producing(
        flow_context: FlowContext,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        instrumentation: Arc<StageInstrumentation>,
        execution: RuntimeExecution,
    ) -> Self {
        Self::with_context(CatchUpContext::producing(
            flow_context,
            data_journal,
            instrumentation,
            execution,
        ))
    }

    pub(crate) fn terminal(
        stage_id: StageId,
        stage_key: StageKey,
        execution: RuntimeExecution,
    ) -> Self {
        Self::with_context(CatchUpContext::new(stage_id, stage_key, None, execution))
    }

    fn with_context(context: CatchUpContext) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(CatchUp {
            machine: machine(CatchUpState::default()),
            context,
        })))
    }

    pub(crate) async fn install_history(&self, history: Arc<EffectHistory>) {
        self.0.lock().await.context.recorded = Recorded::Actions(history);
    }

    pub(crate) async fn send(&self, event: CatchUpEvent) -> CatchUpDisposition {
        let mut catch_up = self.0.lock().await;
        let CatchUp { machine, context } = &mut *catch_up;
        let before = machine.state().crossed();
        let outcome = match machine.handle(event, context).await {
            Ok(actions) => machine.execute_actions(actions, context).await,
            Err(error) => Err(error),
        };
        if let Err(error) = outcome {
            return CatchUpDisposition::Failed(context.failure(error));
        }
        match machine.state() {
            CatchUpState::Refused { fatal } => CatchUpDisposition::Refused(fatal.clone()),
            CatchUpState::Failed { fatal } => CatchUpDisposition::Failed(fatal.clone()),
            state if state.crossed() != before => CatchUpDisposition::Crossed,
            _ => CatchUpDisposition::Consumed,
        }
    }

    #[cfg(test)]
    pub(crate) async fn crossed(&self) -> Option<ReaderGeneration> {
        self.0.lock().await.machine.state().crossed()
    }

    pub(crate) async fn watermark_delivered(
        &self,
        subscription: &UpstreamSubscription<ChainEvent>,
        generation: ReaderGeneration,
    ) -> CatchUpDisposition {
        self.send(CatchUpEvent::FrontierReached {
            generation,
            frontier: subscription.reader_frontier(),
        })
        .await
    }

    pub(crate) async fn eof_delivered(
        &self,
        subscription: &UpstreamSubscription<ChainEvent>,
    ) -> CatchUpDisposition {
        let frontier = subscription.reader_frontier();
        self.send(CatchUpEvent::FrontierReached {
            generation: frontier.highest(),
            frontier,
        })
        .await
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::journal::JournalReader;
    use obzenflow_core::{EventId, JournalError, JournalId, JournalRecord};
    use std::sync::Mutex;

    struct EmptyReader {
        position: u64,
    }

    #[async_trait]
    impl obzenflow_core::journal::JournalStorageReader<ChainEvent> for EmptyReader {
        async fn storage_next(
            &mut self,
        ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
            Ok(None)
        }

        fn storage_position(&self) -> u64 {
            self.position
        }
    }

    pub(crate) struct RecordingJournal {
        id: JournalId,
        events: Mutex<Vec<JournalRecord<ChainPayload>>>,
    }

    impl RecordingJournal {
        pub(crate) fn new() -> Arc<Self> {
            Arc::new(Self {
                id: JournalId::new(),
                events: Mutex::new(Vec::new()),
            })
        }

        pub(crate) fn markers(&self) -> Vec<u64> {
            self.events
                .lock()
                .expect("recording journal lock")
                .iter()
                .filter_map(|record| match &record.authored().payload {
                    ChainPayload::FlowControl(FlowControlPayload::CatchUpComplete {
                        generation,
                        ..
                    }) => Some(generation.0),
                    _ => None,
                })
                .collect()
        }
    }

    #[async_trait]
    impl obzenflow_core::journal::JournalStorage<ChainEvent> for RecordingJournal {
        fn storage_id(&self) -> &JournalId {
            &self.id
        }

        fn storage_owner(&self) -> Option<&obzenflow_core::JournalOwner> {
            None
        }

        async fn storage_append(
            &self,
            event: ChainEvent,
            mut options: obzenflow_core::journal::AppendOptions<ChainEvent>,
        ) -> Result<JournalRecord<ChainPayload>, JournalError> {
            let event = options.capture.prepare(0, event);
            let mut records = self.events.lock().expect("recording journal lock");
            let record =
                crate::testing::causal_fixture::commit(self.id, event, &options, &records)?;
            records.push(record.clone());
            Ok(record)
        }

        async fn storage_read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            Ok(self.events.lock().expect("recording journal lock").clone())
        }

        async fn storage_read_event(
            &self,
            _event_id: &EventId,
        ) -> Result<Option<JournalRecord<ChainPayload>>, JournalError> {
            Ok(None)
        }

        async fn storage_reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<ChainEvent>>, JournalError> {
            Ok(Box::new(EmptyReader { position }))
        }

        async fn storage_read_last_n(
            &self,
            _count: usize,
        ) -> Result<Vec<JournalRecord<ChainPayload>>, JournalError> {
            Ok(Vec::new())
        }
    }

    pub(crate) fn context(
        stage_id: StageId,
        stage_key: &str,
        execution: RuntimeExecution,
        journal: Option<Arc<RecordingJournal>>,
    ) -> CatchUpContext {
        let writer = journal.map(|journal| {
            let data_journal: Arc<dyn Journal<ChainEvent>> = journal;
            MarkerWriter {
                writer_id: WriterId::from(stage_id),
                data_journal,
                instrumentation: Arc::new(StageInstrumentation::new()),
                flow_context: super::super::flow_context_factory::make_flow_context(
                    "flow",
                    "flow_k",
                    stage_key,
                    stage_id,
                    obzenflow_core::event::context::StageType::Transform,
                ),
            }
        });
        CatchUpContext::new(stage_id, StageKey::from(stage_key), writer, execution)
    }

    pub(crate) fn caught_up_through(generation: u64) -> ReaderFrontier {
        ReaderFrontier::for_tests(None, ReaderGeneration(generation))
    }

    pub(crate) fn open_at(least_open: u64, highest: u64) -> ReaderFrontier {
        ReaderFrontier::for_tests(
            Some(ReaderGeneration(least_open)),
            ReaderGeneration(highest),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{caught_up_through, context, open_at, RecordingJournal};
    use super::*;
    use crate::effects::{ActionRow, RecordedAction};
    use obzenflow_core::event::payloads::effect_payload::{
        EffectCursor, EffectOrdinal, EffectType,
    };
    use obzenflow_core::event::StageFatalReason;
    use std::collections::HashMap;

    struct Stage {
        id: StageId,
        journal: Arc<RecordingJournal>,
        execution: RuntimeExecution,
        catch_up: CatchUpHandle,
    }

    fn stage(execution: RuntimeExecution, authors_markers: bool) -> Stage {
        let id = StageId::new();
        let journal = RecordingJournal::new();
        let catch_up = CatchUpHandle::with_context(context(
            id,
            "allocate_stock",
            execution.clone(),
            authors_markers.then(|| journal.clone()),
        ));
        Stage {
            id,
            journal,
            execution,
            catch_up,
        }
    }

    fn resumed(resume_generation: u64) -> RuntimeExecution {
        RuntimeExecution::resume_for_test(ReaderGeneration(resume_generation))
    }

    fn live(stage: &Stage) -> bool {
        stage
            .execution
            .resume_control()
            .is_some_and(|control| control.stage_is_live(stage.id))
    }

    fn effect(input: u64) -> RecordedAction {
        RecordedAction::Effect {
            cursor: EffectCursor::new("flow_k", "allocate_stock", input, EffectOrdinal::new(0)),
            effect_type: EffectType::from("allocation.reserve_stock"),
            in_doubt: false,
        }
    }

    async fn with_actions(stage: &Stage, actions: Vec<RecordedAction>) {
        stage
            .catch_up
            .install_history(Arc::new(EffectHistory::with_actions_for_test(
                "flow_k",
                actions,
                HashMap::<ActionRow, ReaderGeneration>::new(),
            )))
            .await;
    }

    fn reached(generation: u64, frontier: ReaderFrontier) -> CatchUpEvent {
        CatchUpEvent::FrontierReached {
            generation: ReaderGeneration(generation),
            frontier,
        }
    }

    fn unsettled(edges_redelivering: usize) -> CatchUpEvent {
        CatchUpEvent::UnsettledDelivery { edges_redelivering }
    }

    #[tokio::test]
    async fn a_frontier_crossing_authors_each_generation_once_and_goes_live() {
        let stage = stage(resumed(3), true);
        assert!(matches!(
            stage.catch_up.send(reached(1, open_at(0, 1))).await,
            CatchUpDisposition::Consumed
        ));
        let first = stage.catch_up.send(reached(1, caught_up_through(1))).await;
        assert!(matches!(first, CatchUpDisposition::Crossed), "{first:?}");
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(1)));
        assert!(!live(&stage));
        assert!(matches!(
            stage.catch_up.send(reached(3, caught_up_through(3))).await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(3)));
        assert!(matches!(
            stage.catch_up.send(reached(3, caught_up_through(3))).await,
            CatchUpDisposition::Consumed
        ));
        assert_eq!(stage.journal.markers(), vec![1, 2, 3]);
        assert!(live(&stage));
    }

    #[tokio::test]
    async fn an_unreached_frontier_holds_the_stage_and_writes_nothing() {
        let stage = stage(resumed(2), true);
        for event in [reached(1, open_at(0, 1)), reached(2, open_at(1, 2))] {
            assert!(matches!(
                stage.catch_up.send(event).await,
                CatchUpDisposition::Consumed
            ));
        }
        assert!(stage.journal.markers().is_empty());
        assert!(!live(&stage));
    }

    #[tokio::test]
    async fn a_live_stage_ignores_later_frontiers() {
        let stage = stage(resumed(1), true);
        assert!(matches!(
            stage.catch_up.send(reached(1, caught_up_through(1))).await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(1)));
        assert!(live(&stage));
        assert!(matches!(
            stage.catch_up.send(reached(2, caught_up_through(2))).await,
            CatchUpDisposition::Consumed
        ));
        assert_eq!(stage.journal.markers(), vec![1]);
    }

    #[test]
    fn a_stage_transition_and_the_machine_cross_alike() {
        let journal = RecordingJournal::new();
        let ctx = context(StageId::new(), "posted", resumed(2), Some(journal));
        let rebuilding = CatchUpState::default();

        let unreached = frontier_step(&rebuilding, ReaderGeneration(1), open_at(0, 1), &ctx);
        assert_eq!(unreached.next, rebuilding);
        assert!(unreached.actions.is_empty());
        assert_eq!(unreached.crossed_into, None);

        let stepped = frontier_step(&rebuilding, ReaderGeneration(2), caught_up_through(2), &ctx);
        let machine = step(&rebuilding, &reached(2, caught_up_through(2)), &ctx);
        assert_eq!(stepped.next, machine.next_state);
        assert_eq!(stepped.actions, machine.actions);
        assert_eq!(stepped.crossed_into, Some(ReaderGeneration(2)));
        assert_eq!(
            stepped.next,
            CatchUpState::Live {
                crossed: ReaderGeneration(2)
            }
        );
        assert_eq!(
            stepped.actions,
            vec![
                CatchUpAction::AuthorWatermarks {
                    after: ReaderGeneration(0),
                    through: ReaderGeneration(2),
                },
                CatchUpAction::RecordBoundary {
                    generation: ReaderGeneration(2),
                },
            ]
        );

        let repeated = frontier_step(
            &stepped.next,
            ReaderGeneration(2),
            caught_up_through(2),
            &ctx,
        );
        assert_eq!(repeated.next, stepped.next);
        assert!(repeated.actions.is_empty());
        assert_eq!(repeated.crossed_into, None);
    }

    #[tokio::test]
    async fn a_crossing_below_the_resume_generation_skips_the_archived_action_check() {
        let stage = stage(resumed(3), true);
        with_actions(&stage, vec![effect(4)]).await;
        assert!(matches!(
            stage.catch_up.send(reached(2, caught_up_through(2))).await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(2)));
        assert_eq!(stage.journal.markers(), vec![1, 2]);
        assert!(!live(&stage));
    }

    #[tokio::test]
    async fn every_crossing_into_live_work_refuses_while_archived_actions_remain() {
        for event in [
            reached(1, caught_up_through(1)),
            CatchUpEvent::RecordedActionsExhausted,
            unsettled(0),
        ] {
            let stage = stage(resumed(1), true);
            with_actions(&stage, vec![effect(4)]).await;
            match stage.catch_up.send(event).await {
                CatchUpDisposition::Refused(fatal) => {
                    assert_eq!(fatal.reason, StageFatalReason::UnusedEffectHistory);
                    assert_eq!(
                        fatal.detail,
                        "stage 'allocate_stock' skipped a ReserveStock effect its recording holds"
                    );
                    assert!(fatal
                        .departure
                        .is_some_and(|departure| departure.offered.is_none()));
                }
                other => panic!("a crossing with archived actions left must refuse: {other:?}"),
            }
            assert!(stage.journal.markers().is_empty());
            assert!(!live(&stage));
        }

        let ended = self::stage(resumed(1), true);
        with_actions(&ended, vec![effect(4)]).await;
        assert!(matches!(
            ended.catch_up.send(CatchUpEvent::InputsEnded).await,
            CatchUpDisposition::Refused(_)
        ));
    }

    #[tokio::test]
    async fn the_effect_runtime_crosses_through_the_same_machine() {
        let stage = stage(resumed(2), true);
        assert!(matches!(
            stage
                .catch_up
                .send(CatchUpEvent::RecordedCrossingReached {
                    generation: ReaderGeneration(1),
                })
                .await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(1)));
        assert!(!live(&stage));
        assert!(matches!(
            stage
                .catch_up
                .send(CatchUpEvent::RecordedActionsExhausted)
                .await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(2)));
        assert!(live(&stage));
        for repeat in [
            reached(2, caught_up_through(2)),
            CatchUpEvent::RecordedActionsExhausted,
            CatchUpEvent::InputsEnded,
        ] {
            assert!(matches!(
                stage.catch_up.send(repeat).await,
                CatchUpDisposition::Consumed
            ));
        }
        assert_eq!(stage.journal.markers(), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_sink_goes_live_at_an_unsettled_delivery_once_its_edges_redelivered() {
        let stage = stage(resumed(1), false);
        assert!(matches!(
            stage.catch_up.send(unsettled(1)).await,
            CatchUpDisposition::Consumed
        ));
        assert!(!live(&stage));
        assert!(matches!(
            stage.catch_up.send(unsettled(0)).await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(1)));
        assert!(live(&stage));
        assert!(matches!(
            stage.catch_up.send(unsettled(0)).await,
            CatchUpDisposition::Consumed
        ));
        assert!(stage.journal.markers().is_empty());
    }

    #[tokio::test]
    async fn an_event_no_stage_sends_in_its_state_fails_the_stage() {
        let rebuilding = stage(resumed(1), true);
        match rebuilding
            .catch_up
            .send(CatchUpEvent::HandoffAnnounced)
            .await
        {
            CatchUpDisposition::Failed(fatal) => {
                assert_eq!(fatal.code, StageFatalCode::Coordination);
                assert_eq!(fatal.reason, StageFatalReason::CoordinationFailure);
                assert_eq!(
                    fatal.detail,
                    "stage 'allocate_stock' received catch-up event HandoffAnnounced while \
                     Rebuilding"
                );
            }
            other => panic!("an announcement outside a handoff must fail: {other:?}"),
        }
        assert!(!live(&rebuilding));

        let crossed = self::stage(resumed(1), true);
        assert!(matches!(
            crossed
                .catch_up
                .send(CatchUpEvent::RecordedActionsExhausted)
                .await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(crossed.catch_up.crossed().await, Some(ReaderGeneration(1)));
        assert!(matches!(
            crossed
                .catch_up
                .send(CatchUpEvent::RecordedCrossingReached {
                    generation: ReaderGeneration(1),
                })
                .await,
            CatchUpDisposition::Failed(_)
        ));
        assert_eq!(crossed.journal.markers(), vec![1]);
    }

    #[tokio::test]
    async fn a_recorded_crossing_at_the_resume_generation_fails_without_crossing() {
        let stage = stage(resumed(2), true);
        match stage
            .catch_up
            .send(CatchUpEvent::RecordedCrossingReached {
                generation: ReaderGeneration(2),
            })
            .await
        {
            CatchUpDisposition::Failed(fatal) => {
                assert_eq!(fatal.reason, StageFatalReason::ReplayDivergence);
            }
            other => panic!("a recorded crossing into the resume generation must fail: {other:?}"),
        }
        assert!(stage.journal.markers().is_empty());
        assert!(!live(&stage));
    }

    #[tokio::test]
    async fn a_refused_stage_keeps_reporting_its_refusal() {
        let stage = stage(resumed(1), true);
        with_actions(&stage, vec![effect(4)]).await;
        assert!(matches!(
            stage.catch_up.send(CatchUpEvent::InputsEnded).await,
            CatchUpDisposition::Refused(_)
        ));
        assert!(matches!(
            stage.catch_up.send(CatchUpEvent::HandoffAnnounced).await,
            CatchUpDisposition::Refused(_)
        ));
    }

    #[tokio::test]
    async fn a_source_writes_its_markers_and_goes_live_only_once_announced() {
        let stage = stage(resumed(3), true);
        assert!(matches!(
            stage
                .catch_up
                .send(CatchUpEvent::RecordedInputEnded {
                    recorded: ReaderGeneration(1),
                    generation: ReaderGeneration(3),
                })
                .await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(3)));
        assert_eq!(stage.journal.markers(), vec![2, 3]);
        assert!(!live(&stage));
        assert!(matches!(
            stage.catch_up.send(CatchUpEvent::HandoffAnnounced).await,
            CatchUpDisposition::Consumed
        ));
        assert!(live(&stage));
    }

    #[tokio::test]
    async fn a_replay_reauthors_markers_without_recording_a_boundary() {
        let stage = stage(
            RuntimeExecution::new(crate::execution::RuntimeMode::Replay, None),
            true,
        );
        assert!(matches!(
            stage.catch_up.send(reached(2, caught_up_through(2))).await,
            CatchUpDisposition::Crossed
        ));
        assert_eq!(stage.catch_up.crossed().await, Some(ReaderGeneration(2)));
        assert!(matches!(
            stage
                .catch_up
                .send(CatchUpEvent::RecordedActionsExhausted)
                .await,
            CatchUpDisposition::Consumed
        ));
        assert_eq!(stage.journal.markers(), vec![1, 2]);
    }
}
