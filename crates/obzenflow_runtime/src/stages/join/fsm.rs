// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FSM for join stages
//!
//! Join stages have a dedicated FSM that handles per-source EOF semantics.
//! Unlike transforms which have single upstream, joins track two distinct upstreams
//! (reference and stream) with different behaviors.

use crate::messaging::DeliveredRecord;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::observer::StageLifecyclePhase;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::execution_payload::{ExecutionPayload, JoinPhaseFact};
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::vector_clock::VectorClock;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, ReaderGeneration, StageId, StageKey, WriterId};
use obzenflow_fsm::{FsmAction, FsmContext, Transition};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::config::{JoinReferenceMode, LiveStream, PhaseWitnesses};
use super::phase::{check_crossing_phase, observe_hold, CrossingPhase, HoldOutcome};
use crate::backpressure::{BackpressureReader, BackpressureWriter};
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::{
    ContractConfig, ContractsWiring, EofOutcome, ReaderFrontier, ReaderProgress,
};
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::handlers::UnifiedJoinHandler;
use crate::stages::common::heartbeat::HeartbeatHandle;
use crate::stages::common::supervision::catch_up::{
    frontier_step, CatchUpAction, CatchUpContext, CatchUpState,
};
use crate::stages::common::supervision::forward_control_event::forward_control_event;
use crate::stages::common::supervision::lifecycle_actions;
use crate::stages::observer::dispatch::run_stage_lifecycle_observers;
use crate::stages::resources_builder::BoundSubscriptionFactory;
use crate::supervised_base::handler_supervised::SupervisorAction;

// ============================================================================
// FSM States
// ============================================================================

/// The phases a join runs and how its `Live` phase reads, fixed when it is built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinPlan {
    shape: PlanShape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PlanShape {
    Live(LiveReads),
    FiniteEof,
    ThenLive(ThenLivePhases),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ThenLivePhases {
    BatchThenLive(LiveReads),
    LiveOnly(LiveReads),
    BatchOnly,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LiveReads {
    Recorded,
    Canonical,
    Preference,
}

impl JoinPlan {
    pub(crate) fn new(
        mode: JoinReferenceMode,
        witnesses: Option<&PhaseWitnesses>,
        reads: LiveReads,
    ) -> Self {
        let shape = match mode {
            JoinReferenceMode::Live => PlanShape::Live(reads),
            JoinReferenceMode::FiniteEof => PlanShape::FiniteEof,
            JoinReferenceMode::FiniteThenLive => {
                let batch_empty =
                    witnesses.is_some_and(|witnesses| witnesses.batch_stream.is_empty());
                let live_empty =
                    witnesses.is_some_and(|witnesses| witnesses.live_stream == LiveStream::Absent);
                PlanShape::ThenLive(match (batch_empty, live_empty) {
                    (true, _) => ThenLivePhases::LiveOnly(reads),
                    (false, true) => ThenLivePhases::BatchOnly,
                    (false, false) => ThenLivePhases::BatchThenLive(reads),
                })
            }
        };
        Self { shape }
    }

    pub(crate) fn shape(self) -> PlanShape {
        self.shape
    }

    pub(crate) fn live_reads(self) -> Option<LiveReads> {
        match self.shape {
            PlanShape::Live(reads)
            | PlanShape::ThenLive(
                ThenLivePhases::BatchThenLive(reads) | ThenLivePhases::LiveOnly(reads),
            ) => Some(reads),
            PlanShape::FiniteEof | PlanShape::ThenLive(ThenLivePhases::BatchOnly) => None,
        }
    }
}

/// A reading join's plan, its catch-up state and the worst kind its inputs
/// ended with.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinReading {
    pub(crate) plan: JoinPlan,
    pub(crate) catch_up: CatchUpState,
    pub(crate) worst_eof: Option<EofKind>,
}

impl JoinReading {
    pub(crate) fn new(plan: JoinPlan) -> Self {
        Self {
            plan,
            catch_up: CatchUpState::default(),
            worst_eof: None,
        }
    }

    pub(crate) fn with_worst_of(self, eof: &EofRead) -> Self {
        Self {
            worst_eof: match (self.worst_eof, eof.side_worst) {
                (Some(current), Some(kind)) => Some(current.worst(kind)),
                (current, kind) => current.or(kind),
            },
            ..self
        }
    }
}

/// A holding join's reading and the witness EOFs it has folded.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinHold {
    pub(crate) reading: JoinReading,
    pub(crate) witnessed: Arc<BTreeMap<StageKey, EofKind>>,
}

impl JoinHold {
    pub(crate) fn new(reading: JoinReading) -> Self {
        Self {
            reading,
            witnessed: Arc::default(),
        }
    }
}

/// How an enriching join reads its stream.
#[derive(Clone, Debug, PartialEq)]
pub enum JoinEnrichment {
    /// Holds the catalog until the batch stream's witnesses end.
    Holding(JoinHold),
    /// Reads the stream to its end against a finished catalog.
    ToEnd(JoinReading),
}

/// A live join's reading and the policy it reads both inputs by.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinLive {
    pub(crate) reading: JoinReading,
    pub(crate) reads: LiveReads,
}

/// A draining join's step, the worst kind its inputs ended with and the clock
/// its own EOF carries.
#[derive(Clone, Debug, PartialEq)]
pub struct JoinDrain {
    pub(crate) step: DrainStep,
    pub(crate) worst_eof: Option<EofKind>,
    pub(crate) clock: Option<VectorClock>,
}

#[derive(Clone, Debug)]
pub(crate) enum DrainStep {
    Finalize {
        final_eof: Arc<DeliveredRecord<ChainPayload>>,
    },
    Flush,
}

impl PartialEq for DrainStep {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Finalize { final_eof: a }, Self::Finalize { final_eof: b }) => Arc::ptr_eq(a, b),
            (Self::Flush, Self::Flush) => true,
            _ => false,
        }
    }
}

/// FSM states for join stages
#[derive(Clone, Debug, PartialEq, obzenflow_fsm::StateVariant)]
pub enum JoinState {
    /// Initial state - join stage created but not initialized
    Created(JoinPlan),

    Initializing(JoinPlan),
    Starting(JoinPlan),
    Finalizing,
    Failing(String),
    Cancelling(String),
    Cancelled(String),

    /// Resources allocated, ready to start processing
    Initialized(JoinPlan),

    /// Hydrating: Building reference catalog from reference events
    /// During this state:
    /// - ONLY reads from reference subscription
    /// - Stream events queue in journal/subscription (natural backpressure)
    /// - Transitions to Enriching when reference EOF received
    Hydrating(JoinHold),

    /// Live: Reference side remains active while stream events are processed continuously.
    ///
    /// During this state:
    /// - Reads from both reference and stream subscriptions
    /// - Reference updates can arrive at any time and update catalogs
    /// - Stream events are enriched against the current catalog snapshot
    Live(JoinLive),

    /// Enriching: Reference catalog complete, actively enriching stream events
    /// During this state:
    /// - ONLY reads from stream subscription
    /// - Reference catalog is complete and ready for lookups
    /// - Transitions to Draining when stream EOF received
    Enriching(JoinEnrichment),

    /// All upstreams reached EOF, finishing processing
    Draining(JoinDrain),

    /// All events processed, ready to complete
    Drained,

    /// Unrecoverable error occurred
    Failed(String),
}

impl JoinState {
    pub(crate) fn failure(cause: String) -> Self {
        use crate::stages::common::stage_handle::{
            FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
        };
        match cause.as_str() {
            FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP | STOP_REASON_TIMEOUT => {
                Self::Cancelling(cause)
            }
            _ => Self::Failing(cause),
        }
    }

    pub(crate) fn lifecycle_phase(&self) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        use crate::stages::common::stage_lifecycle::LifecyclePhase as Phase;
        match self {
            Self::Initializing(_) => Phase::Initializing,
            Self::Initialized(_) => Phase::Initialized,
            Self::Hydrating(_) | Self::Live(_) | Self::Enriching(_) => Phase::Active,
            Self::Finalizing => Phase::Finalizing,
            Self::Failing(cause) => Phase::Failing(cause.clone()),
            Self::Cancelling(reason) => Phase::Cancelling(reason.clone()),
            Self::Drained => Phase::Completed,
            Self::Failed(cause) => Phase::Failed(cause.clone()),
            Self::Cancelled(reason) => Phase::Cancelled(reason.clone()),
            _ => Phase::Other,
        }
    }

    pub(crate) fn reading(&self) -> Option<&JoinReading> {
        match self {
            Self::Hydrating(hold) | Self::Enriching(JoinEnrichment::Holding(hold)) => {
                Some(&hold.reading)
            }
            Self::Enriching(JoinEnrichment::ToEnd(reading)) => Some(reading),
            Self::Live(live) => Some(&live.reading),
            _ => None,
        }
    }

    pub(crate) fn crossing_phase(&self) -> Option<CrossingPhase> {
        match self {
            Self::Hydrating(_) => Some(CrossingPhase::Hydrating),
            Self::Enriching(_) => Some(CrossingPhase::Enriching),
            Self::Live(_) => Some(CrossingPhase::Live),
            _ => None,
        }
    }

    pub(crate) fn held_input(&self) -> Option<JoinSubscriptionSide> {
        match self {
            Self::Hydrating(_) => Some(JoinSubscriptionSide::Stream),
            Self::Enriching(JoinEnrichment::Holding(_)) => Some(JoinSubscriptionSide::Reference),
            _ => None,
        }
    }
}

// ============================================================================
// FSM Events
// ============================================================================

/// A catch-up watermark the join read, with the frontier of the inputs it reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WatermarkRead {
    pub(crate) generation: ReaderGeneration,
    pub(crate) frontier: ReaderFrontier,
}

/// An EOF the join read, with what its subscription knew once it arrived.
#[derive(Clone, Debug)]
pub struct EofRead {
    pub(crate) envelope: Arc<DeliveredRecord<ChainPayload>>,
    pub(crate) ends_reads: bool,
    pub(crate) side_worst: Option<EofKind>,
    pub(crate) frontier: ReaderFrontier,
    pub(crate) generation: ReaderGeneration,
    pub(crate) ended_by: Option<StageKey>,
}

impl EofRead {
    pub(crate) fn new(
        envelope: DeliveredRecord<ChainPayload>,
        outcome: Option<EofOutcome>,
        generation: Option<ReaderGeneration>,
        frontier: ReaderFrontier,
    ) -> Self {
        Self {
            envelope: Arc::new(envelope),
            ends_reads: outcome.as_ref().is_some_and(|outcome| outcome.is_final),
            side_worst: outcome.as_ref().and_then(|outcome| outcome.worst_kind),
            frontier,
            generation: generation.unwrap_or_default(),
            ended_by: outcome.map(|outcome| StageKey::from(outcome.stage_name)),
        }
    }

    pub(crate) fn clock(&self) -> Option<VectorClock> {
        match &self.envelope.payload {
            ChainPayload::FlowControl(FlowControlPayload::Eof { vector_clock, .. }) => {
                vector_clock.clone()
            }
            _ => None,
        }
    }

    pub(crate) fn author(&self) -> Option<(StageId, EofKind)> {
        let event = self.envelope.authored();
        let ChainPayload::FlowControl(FlowControlPayload::Eof {
            kind, writer_id, ..
        }) = &event.payload
        else {
            return None;
        };
        match writer_id.unwrap_or(event.writer_id) {
            WriterId::Stage(stage) => Some((stage, *kind)),
            _ => None,
        }
    }
}

/// Events that trigger join state transitions
#[derive(Clone, Debug, obzenflow_fsm::EventVariant)]
pub enum JoinEvent {
    /// Initialize the join stage
    Initialize,
    InitializationCompleted,
    ActivationCompleted,
    FinalizationCompleted,
    TerminationSettled,

    /// Ready to start processing
    Ready,

    /// The reading state's dispatch read a catch-up watermark.
    WatermarkRead(WatermarkRead),

    /// The reading state's dispatch read an EOF on its reference subscription.
    ReferenceEofRead(EofRead),

    /// The reading state's dispatch read an EOF on its stream subscription.
    StreamEofRead(EofRead),

    /// The current draining step finished its work.
    DrainStepEnded,

    /// Draining complete
    DrainComplete,

    /// Unrecoverable error occurred
    Error(String),
}

impl crate::supervised_base::with_external_events::ExternalControlEvent for JoinEvent {
    fn discard_details(
        &self,
    ) -> (
        obzenflow_core::event::CommandDiscardDisposition,
        Option<String>,
    ) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Error(message) => Some(message.as_str()),
            _ => None,
        })
    }
}

/// The generation of the delivery that ended a holding state, and the stages
/// whose natural EOFs ended it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HoldCompletion {
    pub generation: u64,
    pub ended_by: Vec<StageKey>,
}

impl HoldCompletion {
    pub(crate) fn enriching_entered(self) -> JoinPhaseFact {
        JoinPhaseFact::EnrichingEntered {
            generation: self.generation,
            ended_by: self.ended_by,
        }
    }

    pub(crate) fn live_entered(self) -> JoinPhaseFact {
        JoinPhaseFact::LiveEntered {
            generation: self.generation,
            ended_by: self.ended_by,
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

#[derive(Clone)]
pub(crate) enum JoinAction<H> {
    Host(SupervisorAction<JoinEvent>),
    AllocateResources,
    PublishRunning,
    EmitHydrationHeartbeat,
    RecordPhase(JoinPhaseFact),
    CatchUp(CatchUpAction),
    PromoteHeld {
        side: JoinSubscriptionSide,
        generation: ReaderGeneration,
    },
    ForwardControl(Arc<DeliveredRecord<ChainPayload>>),
    ReleaseInputs,
    AuthorEof {
        kind: EofKind,
        clock: Option<VectorClock>,
    },
    SendCompletion,
    SendFailure {
        message: String,
    },
    Cleanup,
    _Phantom(PhantomData<H>),
}

impl<H> std::fmt::Debug for JoinAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Host(action) => action.fmt(f),
            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::EmitHydrationHeartbeat => write!(f, "EmitHydrationHeartbeat"),
            Self::RecordPhase(fact) => write!(f, "RecordPhase({fact:?})"),
            Self::CatchUp(action) => write!(f, "CatchUp({action:?})"),
            Self::PromoteHeld { side, generation } => {
                write!(f, "PromoteHeld({side:?}, {generation:?})")
            }
            Self::ForwardControl(envelope) => {
                write!(f, "ForwardControl({})", envelope.event_type())
            }
            Self::ReleaseInputs => write!(f, "ReleaseInputs"),
            Self::AuthorEof { kind, .. } => write!(f, "AuthorEof({kind:?})"),
            Self::SendCompletion => write!(f, "SendCompletion"),
            Self::SendFailure { message } => write!(f, "SendFailure({message:?})"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

// ============================================================================
// FSM Context
// ============================================================================

/// Structural subscription branch that owns a deferred consumption ack.
/// This is deliberately independent of event authorship and upstream ID.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum JoinSubscriptionSide {
    Reference,
    Stream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PendingSubscriptionAck {
    pub side: JoinSubscriptionSide,
    pub upstream: StageId,
}

/// Context for join handlers - contains everything actions need
pub struct JoinResources<H: UnifiedJoinHandler> {
    /// The handler instance (immutable, wrapped in Arc like StatefulContext)
    pub handler: Option<Arc<H>>,

    /// Handler state (catalogs, buffers)
    pub handler_state: H::State,

    /// This join's stage ID
    pub stage_id: StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Runtime observer bundle attached to this stage.
    pub observers: crate::stages::observer::StageObserverBundle,

    /// Runtime execution strategy (FLOWIP-120r).
    pub runtime_execution: crate::execution::RuntimeExecution,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Reference stage ID (set during construction via with_reference())
    /// Join stages need to know this to differentiate reference from stream
    pub reference_stage_id: StageId,

    /// Data journal for writing joined events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this join (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// Build-resolved lineage policy used for runtime-authored fatal evidence.
    pub lineage_policy: obzenflow_core::config::LineagePolicy,

    /// Subscription to reference journal ONLY (used during Hydrating)
    pub reference_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Subscription to stream journals ONLY (used during Enriching)
    pub stream_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// FSM-owned contract state for reference side (aligned with reference_subscription readers)
    pub reference_contract_state: Vec<ReaderProgress>,

    /// Last supervisor-driven contract check instant for the reference subscription (FLOWIP-080r).
    pub(crate) reference_last_contract_check: Option<tokio::time::Instant>,

    /// FSM-owned contract state for stream side (aligned with stream_subscription readers)
    pub stream_contract_state: Vec<ReaderProgress>,

    /// Last supervisor-driven contract check instant for the stream subscription (FLOWIP-080r).
    pub(crate) stream_last_contract_check: Option<tokio::time::Instant>,

    /// Conservative high-water clock for the reference side (FLOWIP-071h interim).
    ///
    /// This accumulates ancestry from reference envelopes as they are consumed
    /// and is merged into stream-triggered output parents.
    pub(crate) reference_high_water_clock: VectorClock,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Control event handling strategy
    pub control_strategy: Arc<dyn crate::stages::common::control_strategies::SignalGate>,

    /// Durable per-stage signal-strategy scratch (FLOWIP-115c).
    pub processing_context: crate::stages::common::control_strategies::ProcessingContext,

    /// Bound factories for reference and stream subscriptions
    pub reference_subscription_factory: BoundSubscriptionFactory,
    pub stream_subscription_factory: BoundSubscriptionFactory,

    pub(crate) phase_witnesses: Option<PhaseWitnesses>,

    pub(crate) witness_keys: HashMap<StageId, StageKey>,

    pub(crate) archived_phases: Vec<JoinPhaseFact>,

    /// Live-mode fairness cap (see `JoinConfig.reference_batch_cap`).
    pub reference_batch_cap: Option<usize>,

    /// Live-mode counter: reference events processed since the last stream event.
    pub reference_since_last_stream: usize,

    /// Counter of reference-side events processed since the last heartbeat
    /// (used during Hydrating for observability snapshots).
    pub events_since_last_heartbeat: u64,

    /// FLOWIP-010: build-resolved `runtime.heartbeat_interval` (events
    /// between heartbeats; 0 disables), from stage resources.
    pub heartbeat_interval: u64,

    /// Backpressure writer handle for this stage's journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Declared stage output contract used by the shared output commit path.
    pub output_contract: StageOutputContract,

    /// Backpressure readers keyed by upstream stage ID (FLOWIP-086k).
    pub backpressure_readers: HashMap<StageId, BackpressureReader>,

    /// Pending data outputs blocked on downstream credits (Phase 1: bounded to one input).
    pub(crate) pending_outputs:
        VecDeque<crate::stages::common::supervision::backpressure_drain::PendingOutput>,

    /// Parent envelope for pending outputs (input that produced them).
    pub(crate) pending_parent: Option<DeliveredRecord<ChainPayload>>,

    /// Structural subscription branch and upstream awaiting a consumption ack
    /// once pending outputs are drained.
    pub(crate) pending_subscription_ack: Option<PendingSubscriptionAck>,

    /// Backpressure activity pulse accumulator (Hz UI animation driver).
    pub(crate) backpressure_pulse: BackpressureActivityPulse,

    /// Start of the current backpressure stall episode; anchored at the
    /// first credit miss, cleared on successful reserve (FLOWIP-115e).
    pub(crate) backpressure_stall: crate::backpressure::StallEpisode,

    /// Optional per-stage heartbeat task (FLOWIP-063e).
    pub(crate) heartbeat: Option<HeartbeatHandle>,

    pub(crate) recorded: Option<crate::stages::resources_builder::RecordedSubscription>,

    pub(crate) recorded_order: Option<crate::messaging::upstream_subscription::RecordedOrder>,

    pub(crate) catch_up: CatchUpContext,
}

pub struct JoinContext<H: UnifiedJoinHandler> {
    pub(crate) resources: Option<JoinResources<H>>,
    pub(crate) instrumentation: Arc<StageInstrumentation>,
}
impl<H: UnifiedJoinHandler> JoinContext<H> {
    pub(crate) fn new(resources: JoinResources<H>) -> Self {
        Self {
            instrumentation: resources.instrumentation.clone(),
            resources: Some(resources),
        }
    }
    pub(crate) fn resources_mut(
        &mut self,
    ) -> Result<&mut JoinResources<H>, obzenflow_fsm::FsmError> {
        self.resources.as_mut().ok_or_else(|| {
            obzenflow_fsm::FsmError::HandlerError(
                "join resources belong to a pending operation".into(),
            )
        })
    }

    fn present(&self) -> &JoinResources<H> {
        self.resources
            .as_ref()
            .expect("a join's resources are back before its machine handles a fact")
    }

    pub(crate) fn stage_name(&self) -> &str {
        &self.present().stage_name
    }

    pub(crate) fn cross(
        &self,
        state: &JoinState,
        generation: ReaderGeneration,
        frontier: ReaderFrontier,
    ) -> Result<(JoinReading, Vec<JoinAction<H>>), String> {
        let (Some(reading), Some(phase)) = (state.reading(), state.crossing_phase()) else {
            unreachable!()
        };
        let resources = self.present();
        let step = frontier_step(&reading.catch_up, generation, frontier, &resources.catch_up)
            .into_result()
            .map_err(|fatal| fatal.detail)?;
        let mut actions: Vec<_> = step.actions.into_iter().map(JoinAction::CatchUp).collect();
        if let Some(crossed) = step.crossed_into {
            check_crossing_phase(
                &resources.stage_name,
                &resources.archived_phases,
                crossed,
                phase,
            )?;
            actions.extend(state.held_input().map(|side| JoinAction::PromoteHeld {
                side,
                generation: crossed,
            }));
        }
        Ok((
            JoinReading {
                catch_up: step.next,
                ..reading.clone()
            },
            actions,
        ))
    }

    pub(crate) fn fold_catalog_witness(
        &self,
        witnessed: &Arc<BTreeMap<StageKey, EofKind>>,
        eof: &EofRead,
    ) -> (Arc<BTreeMap<StageKey, EofKind>>, HoldOutcome) {
        let resources = self.present();
        let catalog = resources
            .phase_witnesses
            .as_ref()
            .map(|witnesses| witnesses.catalog.as_slice());
        observe_hold(witnessed, catalog, &resources.witness_keys, eof)
    }

    pub(crate) fn fold_batch_witness(
        &self,
        witnessed: &Arc<BTreeMap<StageKey, EofKind>>,
        eof: &EofRead,
    ) -> (Arc<BTreeMap<StageKey, EofKind>>, HoldOutcome) {
        let resources = self.present();
        let batch = resources
            .phase_witnesses
            .as_ref()
            .map(|witnesses| witnesses.batch_stream.as_slice());
        observe_hold(witnessed, batch, &resources.witness_keys, eof)
    }

    pub(crate) fn fail(&self, cause: String) -> Transition<JoinState, JoinAction<H>> {
        self.instrumentation
            .failures_total
            .fetch_add(1, Ordering::Relaxed);
        self.fail_uncounted(cause)
    }

    pub(crate) fn fail_uncounted(&self, cause: String) -> Transition<JoinState, JoinAction<H>> {
        Transition {
            next_state: JoinState::failure(cause.clone()),
            actions: vec![
                JoinAction::SendFailure { message: cause },
                JoinAction::Cleanup,
                JoinAction::Host(SupervisorAction::CloseMailbox),
                JoinAction::Host(SupervisorAction::SettlePublications),
                JoinAction::Host(SupervisorAction::Emit(JoinEvent::TerminationSettled)),
            ],
        }
    }
}
impl<H: UnifiedJoinHandler + 'static> FsmContext for JoinContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: UnifiedJoinHandler + Clone + Send + Sync + 'static> FsmAction for JoinAction<H> {
    type Context = JoinContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        self.execute_resources(ctx.resources_mut()?).await
    }
}

impl<H: UnifiedJoinHandler + Clone + Send + Sync + 'static> JoinAction<H> {
    pub(crate) async fn execute_resources(
        &self,
        ctx: &mut JoinResources<H>,
    ) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            JoinAction::Host(_) => Err(obzenflow_fsm::FsmError::HandlerError(
                "host action requires the supervised runner".into(),
            )),

            JoinAction::EmitHydrationHeartbeat => {
                if ctx.events_since_last_heartbeat > 0 {
                    super::supervisor::common::emit_join_heartbeat_if_due(ctx, ctx.stage_id)
                        .await
                        .map_err(|error| {
                            obzenflow_fsm::FsmError::HandlerError(error.to_string())
                        })?;
                }
                Ok(())
            }
            JoinAction::RecordPhase(fact) => {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to journal the join's phase".to_string(),
                    )
                })?;
                let event = ChainEventFactory::execution_event(
                    writer_id,
                    ExecutionPayload::JoinPhase(fact.clone()),
                )
                .with_flow_context(make_flow_context(
                    &ctx.flow_name,
                    &ctx.flow_id.to_string(),
                    &ctx.stage_name,
                    ctx.stage_id,
                    StageType::Join,
                ));
                crate::supervised_base::publication::append(
                    &ctx.data_journal,
                    event,
                    Default::default(),
                )
                .await
                .map_err(|error| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to journal the join's phase: {error}"
                    ))
                })?;
                Ok(())
            }
            JoinAction::CatchUp(action) => action.execute(&mut ctx.catch_up).await,
            JoinAction::PromoteHeld { side, generation } => {
                let (held, input) = match side {
                    JoinSubscriptionSide::Reference => {
                        (ctx.reference_subscription.as_mut(), "catalog")
                    }
                    JoinSubscriptionSide::Stream => (ctx.stream_subscription.as_mut(), "stream"),
                };
                let held = held.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "join '{}' has no {input} subscription to promote",
                        ctx.stage_name
                    ))
                })?;
                held.promote_held_readers(*generation);
                Ok(())
            }
            JoinAction::ForwardControl(envelope) => {
                forward_control_event(
                    envelope,
                    ctx.stage_id,
                    &ctx.stage_name,
                    StageType::Join,
                    &ctx.data_journal,
                )
                .await
                .map_err(|error| obzenflow_fsm::FsmError::HandlerError(error.to_string()))?;
                Ok(())
            }
            JoinAction::ReleaseInputs => {
                for reader in ctx.backpressure_readers.values() {
                    reader.release();
                }
                Ok(())
            }
            JoinAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    reference_stage_id = ?ctx.reference_stage_id,
                    stream_journal_count = ctx.stream_subscription_factory.upstream_stage_ids().len(),
                    "Join: Creating subscriptions to upstream journals"
                );

                // Initialize FSM-owned contract state for reference and stream sides
                let ref_ids = ctx.reference_subscription_factory.upstream_stage_ids();
                ctx.reference_contract_state =
                    ref_ids.into_iter().map(ReaderProgress::new).collect();

                let stream_ids = ctx.stream_subscription_factory.upstream_stage_ids();
                ctx.stream_contract_state =
                    stream_ids.into_iter().map(ReaderProgress::new).collect();

                // 1) Subscribe to reference journal ONLY
                let ref_subscription = ctx
                    .reference_subscription_factory
                    .build_with_contracts(ContractsWiring {
                        writer_id,
                        contract_journal: ctx.data_journal.clone(),
                        config: ContractConfig::default(),
                        reader_stage: Some(ctx.stage_id),
                        control_plane: ctx.instrumentation.control_plane().clone(),
                        include_delivery_contract: false,
                        cycle_guard_config: None,
                    })
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to create reference subscription: {e}"
                        ))
                    })?
                    .with_contract_flow_context(make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        StageType::Join,
                    ));

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join: Created reference subscription successfully"
                );

                ctx.reference_subscription = Some(ref_subscription);

                // 2) Subscribe to stream journals ONLY (NOT reference)
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    stream_journal_ids = ?ctx.stream_subscription_factory.upstream_stage_ids(),
                    "Join: Creating stream subscription"
                );

                let stream_subscription = ctx
                    .stream_subscription_factory
                    .build_with_contracts(ContractsWiring {
                        writer_id,
                        contract_journal: ctx.data_journal.clone(),
                        config: ContractConfig::default(),
                        reader_stage: Some(ctx.stage_id),
                        control_plane: ctx.instrumentation.control_plane().clone(),
                        include_delivery_contract: false,
                        cycle_guard_config: None,
                    })
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to create stream subscription: {e}"
                        ))
                    })?
                    .with_contract_flow_context(make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        StageType::Join,
                    ));

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join: Created stream subscription successfully"
                );

                ctx.stream_subscription = Some(stream_subscription);

                if let Some(archive) = ctx.runtime_execution.archive_for_io().cloned() {
                    ctx.archived_phases =
                        super::phase::load_archived_phases(&archive, &ctx.stage_name)
                            .await
                            .map_err(obzenflow_fsm::FsmError::HandlerError)?;
                }

                if let Some(recorded) = &ctx.recorded {
                    ctx.recorded_order = Some(recorded.order().await.map_err(|error| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to read the join's recorded selections: {error}"
                        ))
                    })?);
                }

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join allocated resources"
                );
                Ok(())
            }

            JoinAction::PublishRunning => {
                lifecycle_actions::publish_running(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::Join,
                    ),
                )
                .await?;
                let scope = ctx
                    .runtime_execution
                    .dispatch_scope(ctx.stage_id, None, None);
                run_stage_lifecycle_observers(
                    &ctx.observers,
                    scope,
                    StageLifecyclePhase::Running,
                    || {
                        (
                            ctx.flow_id,
                            FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: ctx.stage_id,
                                stage_type: StageType::Join,
                            },
                        )
                    },
                );
                Ok(())
            }

            JoinAction::AuthorEof { kind, clock } => {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to forward EOF".to_string(),
                    )
                })?;

                let runtime_context = ctx.instrumentation.snapshot();
                let (authored_writer_seq, writer_seq_by_event_type, authored_last_event_id) =
                    ctx.instrumentation.authored_data_frontier();

                let mut eof_event = ChainEventFactory::eof_event_with_kind(writer_id, *kind);

                if let ChainPayload::FlowControl(FlowControlPayload::Eof {
                    writer_id: ref mut eof_writer,
                    writer_seq,
                    writer_seq_by_event_type: eof_writer_seq_by_event_type,
                    writer_seq_by_event_type_complete,
                    vector_clock,
                    last_event_id,
                    ..
                }) = &mut eof_event.payload
                {
                    *eof_writer = Some(writer_id);
                    *writer_seq = Some(authored_writer_seq);
                    *eof_writer_seq_by_event_type = writer_seq_by_event_type.clone();
                    *writer_seq_by_event_type_complete = true;
                    if let Some(clock) = clock {
                        *vector_clock = Some(clock.clone());
                    }
                    *last_event_id = authored_last_event_id;
                }

                // Attach flow/runtime context for downstream contract tracking
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::Join,
                };
                eof_event.runtime = Some(runtime_context);

                crate::stages::common::supervision::output_committer::commit_control_output(
                    &ctx.data_journal,
                    &ctx.instrumentation,
                    eof_event,
                )
                .await
                .map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!("Failed to forward EOF: {e}"))
                })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join forwarded EOF downstream"
                );
                Ok(())
            }

            JoinAction::SendCompletion => {
                if let Some(heartbeat) = &ctx.heartbeat {
                    heartbeat.state.mark_completed();
                }

                lifecycle_actions::send_completion(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::Join,
                    ),
                    ctx.instrumentation.as_ref(),
                )
                .await?;
                let scope = ctx
                    .runtime_execution
                    .dispatch_scope(ctx.stage_id, None, None);
                run_stage_lifecycle_observers(
                    &ctx.observers,
                    scope,
                    StageLifecyclePhase::Completed,
                    || {
                        (
                            ctx.flow_id,
                            FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: ctx.stage_id,
                                stage_type: StageType::Join,
                            },
                        )
                    },
                );
                Ok(())
            }

            JoinAction::SendFailure { message } => {
                lifecycle_actions::send_failure(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::Join,
                    ),
                    message,
                    ctx.instrumentation.as_ref(),
                    None,
                )
                .await?;
                let scope = ctx
                    .runtime_execution
                    .dispatch_scope(ctx.stage_id, None, None);
                run_stage_lifecycle_observers(
                    &ctx.observers,
                    scope,
                    StageLifecyclePhase::Failed,
                    || {
                        (
                            ctx.flow_id,
                            FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: ctx.stage_id,
                                stage_type: StageType::Join,
                            },
                        )
                    },
                );
                Ok(())
            }

            JoinAction::Cleanup => {
                ctx.handler.take();
                ctx.reference_subscription.take();
                ctx.stream_subscription.take();
                if let Some(heartbeat) = ctx.heartbeat.take() {
                    heartbeat.cancel();
                }

                let stage_name = ctx.stage_name.clone();
                lifecycle_actions::cleanup_best_effort("Join", &stage_name, || async {
                    ctx.reference_subscription = None;
                    ctx.stream_subscription = None;
                    Ok::<(), ()>(())
                })
                .await;
                Ok(())
            }

            JoinAction::_Phantom(_) => unreachable!("PhantomData action"),
        }
    }
}
