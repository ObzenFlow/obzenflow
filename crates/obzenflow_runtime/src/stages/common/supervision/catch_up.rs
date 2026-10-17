// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::{ActionDeparture, EffectHistory};
use crate::execution::RuntimeExecution;
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handler_error::StageFatal;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, ReaderGeneration, StageId, StageKey, WriterId};
use obzenflow_fsm::{
    fsm, EventVariant, FsmAction, FsmContext, FsmError, StateMachine, StateVariant, Transition,
};
use std::sync::Arc;

#[derive(Debug)]
pub(crate) enum CatchUpDisposition {
    Consumed,
    Crossed(ReaderGeneration),
    Failed(String),
    Refused(StageFatal),
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CatchUpState {
    Rebuilding { crossed: ReaderGeneration },
    Announcing { crossed: ReaderGeneration },
    Live { crossed: ReaderGeneration },
    Refused { fatal: StageFatal },
    Failed { message: String },
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
        delivered: u64,
    },
    InputsEnded,
    RecordedCrossingReached {
        generation: ReaderGeneration,
    },
    RecordedActionsExhausted {
        delivered: u64,
    },
    SettledDeliveriesExhausted,
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
            Self::RecordedActionsExhausted { .. } => "RecordedActionsExhausted",
            Self::SettledDeliveriesExhausted => "SettledDeliveriesExhausted",
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

pub(crate) struct CatchUpContext {
    stage_id: StageId,
    stage_key: StageKey,
    writer: Option<MarkerWriter>,
    execution: RuntimeExecution,
    history: Option<Arc<EffectHistory>>,
}

impl FsmContext for CatchUpContext {}

impl CatchUpContext {
    fn resume_generation(&self) -> Option<ReaderGeneration> {
        self.execution
            .resume_control()
            .map(|control| control.resume_generation())
    }

    fn unreproduced(&self) -> Option<StageFatal> {
        let expected = self.history.as_ref()?.actions().next_unreproduced()?;
        Some(ActionDeparture::skipped(self.stage_key.clone(), None, expected).stage_fatal())
    }

    fn short_redelivery(&self, delivered: u64) -> Option<String> {
        let max = self
            .execution
            .resume_control()?
            .recorded_delivered_high_water(self.stage_id)?;
        (delivered < max).then(|| {
            format!(
                "stage '{}' crossed its catch-up boundary after {delivered} delivered data events \
                 but the archive records {max}; torn or short re-delivery (FLOWIP-120n F15)",
                self.stage_key.as_str()
            )
        })
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
        delivered: u64,
        crossed: ReaderGeneration,
        generation: ReaderGeneration,
    ) -> Transition<CatchUpState, CatchUpAction> {
        if let Some(fatal) = self.unreproduced() {
            return to(CatchUpState::Refused { fatal });
        }
        if let Some(message) = self.short_redelivery(delivered) {
            return to(CatchUpState::Failed { message });
        }
        self.crossing(crossed, generation, true)
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
            CatchUpState::Rebuilding { crossed },
            CatchUpEvent::FrontierReached {
                generation,
                delivered,
            },
        ) if generation > crossed => {
            if resume.is_some_and(|resume| *generation >= resume) {
                ctx.going_live(*delivered, *crossed, *generation)
            } else {
                ctx.crossing(*crossed, *generation, false)
            }
        }
        (
            CatchUpState::Rebuilding { crossed },
            CatchUpEvent::RecordedCrossingReached { generation },
        ) if generation > crossed => ctx.crossing(*crossed, *generation, false),
        (
            CatchUpState::Rebuilding { crossed },
            CatchUpEvent::RecordedActionsExhausted { delivered },
        ) => match resume {
            Some(generation) if generation > *crossed => {
                ctx.going_live(*delivered, *crossed, generation)
            }
            _ => to(state.clone()),
        },
        (CatchUpState::Rebuilding { crossed }, CatchUpEvent::SettledDeliveriesExhausted) => {
            match resume {
                Some(generation) if generation > *crossed => {
                    ctx.crossing(*crossed, generation, true)
                }
                _ => to(state.clone()),
            }
        }
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
        unhandled => |_state: &CatchUpState, _event: &CatchUpEvent, _ctx: &mut CatchUpContext| {
            Box::pin(async { Ok(()) })
        };
        state CatchUpState::Rebuilding {
            on CatchUpEvent::FrontierReached => transition;
            on CatchUpEvent::InputsEnded => transition;
            on CatchUpEvent::RecordedCrossingReached => transition;
            on CatchUpEvent::RecordedActionsExhausted => transition;
            on CatchUpEvent::SettledDeliveriesExhausted => transition;
            on CatchUpEvent::RecordedInputEnded => transition;
        }
        state CatchUpState::Announcing {
            on CatchUpEvent::HandoffAnnounced => transition;
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

    pub(crate) fn terminal(
        stage_id: StageId,
        stage_key: StageKey,
        execution: RuntimeExecution,
    ) -> Self {
        Self::new(stage_id, stage_key, None, execution)
    }

    fn new(
        stage_id: StageId,
        stage_key: StageKey,
        writer: Option<MarkerWriter>,
        execution: RuntimeExecution,
    ) -> Self {
        Self(Arc::new(tokio::sync::Mutex::new(CatchUp {
            machine: machine(CatchUpState::Rebuilding {
                crossed: ReaderGeneration(0),
            }),
            context: CatchUpContext {
                stage_id,
                stage_key,
                writer,
                execution,
                history: None,
            },
        })))
    }

    pub(crate) async fn install_history(&self, history: Arc<EffectHistory>) {
        self.0.lock().await.context.history = Some(history);
    }

    pub(crate) async fn has_crossed(&self, generation: ReaderGeneration) -> bool {
        self.0
            .lock()
            .await
            .machine
            .state()
            .crossed()
            .is_some_and(|crossed| crossed >= generation)
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
            return CatchUpDisposition::Failed(match error {
                FsmError::HandlerError(message) => message,
                other => other.to_string(),
            });
        }
        match machine.state() {
            CatchUpState::Refused { fatal } => CatchUpDisposition::Refused(fatal.clone()),
            CatchUpState::Failed { message } => CatchUpDisposition::Failed(message.clone()),
            state => match state.crossed() {
                Some(crossed) if Some(crossed) != before => CatchUpDisposition::Crossed(crossed),
                _ => CatchUpDisposition::Consumed,
            },
        }
    }

    pub(crate) async fn frontier_reached(
        &self,
        generation: ReaderGeneration,
        all_inputs_caught_up: bool,
        delivered: u64,
    ) -> CatchUpDisposition {
        if !all_inputs_caught_up {
            return CatchUpDisposition::Consumed;
        }
        self.send(CatchUpEvent::FrontierReached {
            generation,
            delivered,
        })
        .await
    }

    pub(crate) async fn watermark_delivered(
        &self,
        subscription: &UpstreamSubscription<ChainEvent>,
        generation: ReaderGeneration,
    ) -> CatchUpDisposition {
        self.frontier_reached(
            generation,
            subscription.all_readers_caught_up(generation),
            subscription.delivered_data_count(),
        )
        .await
    }

    pub(crate) async fn eof_delivered(
        &self,
        subscription: &UpstreamSubscription<ChainEvent>,
    ) -> CatchUpDisposition {
        let generation = subscription.max_reader_generation();
        if generation == ReaderGeneration(0) {
            return CatchUpDisposition::Consumed;
        }
        self.frontier_reached(
            generation,
            subscription.all_readers_caught_up(generation),
            subscription.delivered_data_count(),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::effects::{ActionRow, RecordedAction};
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::effect_payload::{
        EffectCursor, EffectOrdinal, EffectType,
    };
    use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
    use obzenflow_core::event::{ChainPayload, StageFatalReason};
    use obzenflow_core::journal::JournalReader;
    use obzenflow_core::{EventId, JournalError, JournalId, JournalRecord};
    use std::collections::HashMap;
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

    struct RecordingJournal {
        id: JournalId,
        events: Mutex<Vec<JournalRecord<ChainPayload>>>,
    }

    impl RecordingJournal {
        fn markers(&self) -> Vec<u64> {
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

    struct Stage {
        id: StageId,
        journal: Arc<RecordingJournal>,
        execution: RuntimeExecution,
        catch_up: CatchUpHandle,
    }

    fn stage(execution: RuntimeExecution, authors_markers: bool) -> Stage {
        let id = StageId::new();
        let journal = Arc::new(RecordingJournal {
            id: JournalId::new(),
            events: Mutex::new(Vec::new()),
        });
        let data_journal: Arc<dyn Journal<ChainEvent>> = journal.clone();
        let writer = authors_markers.then(|| MarkerWriter {
            writer_id: WriterId::from(id),
            data_journal,
            instrumentation: Arc::new(StageInstrumentation::new()),
            flow_context: super::super::flow_context_factory::make_flow_context(
                "flash_sale",
                "flow_k",
                "allocate_stock",
                id,
                obzenflow_core::event::context::StageType::Transform,
            ),
        });
        let catch_up = CatchUpHandle::new(
            id,
            StageKey::from("allocate_stock"),
            writer,
            execution.clone(),
        );
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

    #[tokio::test]
    async fn a_frontier_crossing_authors_each_generation_once_and_goes_live() {
        let stage = stage(resumed(3), true);
        assert!(matches!(
            stage
                .catch_up
                .frontier_reached(ReaderGeneration(1), false, 0)
                .await,
            CatchUpDisposition::Consumed
        ));
        let first = stage
            .catch_up
            .frontier_reached(ReaderGeneration(1), true, 0)
            .await;
        assert!(
            matches!(first, CatchUpDisposition::Crossed(ReaderGeneration(1))),
            "{first:?}"
        );
        assert!(!live(&stage));
        assert!(matches!(
            stage
                .catch_up
                .frontier_reached(ReaderGeneration(3), true, 0)
                .await,
            CatchUpDisposition::Crossed(ReaderGeneration(3))
        ));
        assert!(matches!(
            stage
                .catch_up
                .frontier_reached(ReaderGeneration(3), true, 0)
                .await,
            CatchUpDisposition::Consumed
        ));
        assert_eq!(stage.journal.markers(), vec![1, 2, 3]);
        assert!(live(&stage));
        assert!(stage.catch_up.has_crossed(ReaderGeneration(3)).await);
    }

    #[tokio::test]
    async fn a_crossing_below_the_resume_generation_checks_neither_guard() {
        let stage = stage(resumed(3), true);
        with_actions(&stage, vec![effect(4)]).await;
        stage
            .execution
            .resume_control()
            .expect("resumed")
            .record_delivered_high_water(stage.id, 3);
        assert!(matches!(
            stage
                .catch_up
                .frontier_reached(ReaderGeneration(2), true, 0)
                .await,
            CatchUpDisposition::Crossed(ReaderGeneration(2))
        ));
        assert_eq!(stage.journal.markers(), vec![1, 2]);
        assert!(!live(&stage));
    }

    #[tokio::test]
    async fn every_crossing_into_live_work_refuses_while_archived_actions_remain() {
        let stage = stage(resumed(1), true);
        with_actions(&stage, vec![effect(4)]).await;
        match stage
            .catch_up
            .frontier_reached(ReaderGeneration(1), true, 0)
            .await
        {
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
            _ => panic!("a crossing with archived actions left must refuse"),
        }
        assert!(stage.journal.markers().is_empty());
        assert!(!live(&stage));

        let ended = self::stage(resumed(1), true);
        with_actions(&ended, vec![effect(4)]).await;
        assert!(matches!(
            ended.catch_up.send(CatchUpEvent::InputsEnded).await,
            CatchUpDisposition::Refused(_)
        ));
    }

    #[tokio::test]
    async fn a_short_redelivery_fails_whichever_event_crosses() {
        for event in [
            CatchUpEvent::FrontierReached {
                generation: ReaderGeneration(1),
                delivered: 2,
            },
            CatchUpEvent::RecordedActionsExhausted { delivered: 2 },
        ] {
            let stage = stage(resumed(1), true);
            stage
                .execution
                .resume_control()
                .expect("resumed")
                .record_delivered_high_water(stage.id, 3);
            match stage.catch_up.send(event).await {
                CatchUpDisposition::Failed(message) => {
                    assert!(
                        message.contains("after 2 delivered data events"),
                        "{message}"
                    )
                }
                _ => panic!("a short re-delivery must fail the crossing"),
            }
            assert!(stage.journal.markers().is_empty());
        }
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
            CatchUpDisposition::Crossed(ReaderGeneration(1))
        ));
        assert!(!live(&stage));
        assert!(matches!(
            stage
                .catch_up
                .send(CatchUpEvent::RecordedActionsExhausted { delivered: 0 })
                .await,
            CatchUpDisposition::Crossed(ReaderGeneration(2))
        ));
        assert!(live(&stage));
        assert!(matches!(
            stage
                .catch_up
                .frontier_reached(ReaderGeneration(2), true, 0)
                .await,
            CatchUpDisposition::Consumed
        ));
        assert_eq!(stage.journal.markers(), vec![1, 2]);
    }

    #[tokio::test]
    async fn a_terminal_stage_records_its_boundary_without_a_marker() {
        let stage = stage(resumed(1), false);
        assert!(matches!(
            stage
                .catch_up
                .send(CatchUpEvent::SettledDeliveriesExhausted)
                .await,
            CatchUpDisposition::Crossed(ReaderGeneration(1))
        ));
        assert!(live(&stage));
        assert!(stage.journal.markers().is_empty());
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
            CatchUpDisposition::Crossed(ReaderGeneration(3))
        ));
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
            stage
                .catch_up
                .frontier_reached(ReaderGeneration(2), true, 0)
                .await,
            CatchUpDisposition::Crossed(ReaderGeneration(2))
        ));
        assert!(matches!(
            stage
                .catch_up
                .send(CatchUpEvent::RecordedActionsExhausted { delivered: 0 })
                .await,
            CatchUpDisposition::Consumed
        ));
        assert_eq!(stage.journal.markers(), vec![1, 2]);
    }
}
