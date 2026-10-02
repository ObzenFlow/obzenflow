// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful stage FSM types and state machine definition
//!
//! Stateful stages maintain state across events, enabling aggregations,
//! windowing operations, and session tracking.

use crate::messaging::DeliveredRecord;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::observer::StageLifecyclePhase;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, StageId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::backpressure::{BackpressureReader, BackpressureWriter};
use crate::effects::{EffectDeclaration, EffectHistory, EffectPortRegistry};
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::{
    ContractConfig, ContractsWiring, ReaderProgress, StageInputPosition,
};
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::SignalGate;
use crate::stages::common::handlers::UnifiedStatefulHandler;
use crate::stages::common::heartbeat::HeartbeatHandle;
use crate::stages::common::supervision::lifecycle_actions;
use crate::stages::observer::dispatch::run_stage_lifecycle_observers;
use crate::stages::resources_builder::BoundSubscriptionFactory;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for stateful stages
#[derive(Serialize, Deserialize)]
pub enum StatefulState<H> {
    /// Initial state - stateful stage has been created but not initialized
    Created,

    Initializing,
    Starting,
    Finalising,
    ValidatingTerminal,
    ForwardingTerminal,
    ProducingFinalOutput,
    DrainingFinalOutput,

    Failing(String),
    Cancelling(String),
    Cancelled(String),

    /// Resources allocated, ready to start processing
    Initialized,

    /// Actively accumulating state from upstream events
    Accumulating,

    /// Emitting accumulated results (optional state for future emission strategies)
    Emitting,

    /// Settling an emission accepted before a drain command.
    EmittingDuringDrain,

    /// Received EOF, draining final accumulated state
    Draining,

    /// All events processed, final state emitted, EOF forwarded downstream
    Drained,

    /// Unrecoverable error occurred
    Failed(String),

    #[serde(skip)]
    _Phantom(PhantomData<H>),
}

// Manual implementations that don't require H to implement these traits
impl<H> Clone for StatefulState<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Created => Self::Created,
            Self::Initializing => Self::Initializing,
            Self::Starting => Self::Starting,
            Self::Finalising => Self::Finalising,
            Self::ValidatingTerminal => Self::ValidatingTerminal,
            Self::ForwardingTerminal => Self::ForwardingTerminal,
            Self::ProducingFinalOutput => Self::ProducingFinalOutput,
            Self::DrainingFinalOutput => Self::DrainingFinalOutput,

            Self::Failing(cause) => Self::Failing(cause.clone()),
            Self::Cancelling(cause) => Self::Cancelling(cause.clone()),
            Self::Cancelled(cause) => Self::Cancelled(cause.clone()),

            Self::Initialized => Self::Initialized,
            Self::Accumulating => Self::Accumulating,
            Self::Emitting => Self::Emitting,
            Self::EmittingDuringDrain => Self::EmittingDuringDrain,
            Self::Draining => Self::Draining,
            Self::Drained => Self::Drained,
            Self::Failed(msg) => Self::Failed(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for StatefulState<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Initializing => write!(f, "Initializing"),
            Self::Starting => write!(f, "Starting"),
            Self::Finalising => write!(f, "Finalising"),
            Self::ValidatingTerminal => write!(f, "ValidatingTerminal"),
            Self::ForwardingTerminal => write!(f, "ForwardingTerminal"),
            Self::ProducingFinalOutput => write!(f, "ProducingFinalOutput"),
            Self::DrainingFinalOutput => write!(f, "DrainingFinalOutput"),

            Self::Failing(cause) => write!(f, "Failing({cause:?})"),
            Self::Cancelling(cause) => write!(f, "Cancelling({cause:?})"),
            Self::Cancelled(cause) => write!(f, "Cancelled({cause:?})"),

            Self::Initialized => write!(f, "Initialized"),
            Self::Accumulating => write!(f, "Accumulating"),
            Self::Emitting => write!(f, "Emitting"),
            Self::EmittingDuringDrain => write!(f, "EmittingDuringDrain"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({msg:?})"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync> PartialEq for StatefulState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StatefulState::Created, StatefulState::Created) => true,
            (Self::Initializing, Self::Initializing) => true,
            (Self::Starting, Self::Starting) => true,
            (Self::Finalising, Self::Finalising) => true,
            (Self::ValidatingTerminal, Self::ValidatingTerminal) => true,
            (Self::ForwardingTerminal, Self::ForwardingTerminal) => true,
            (Self::ProducingFinalOutput, Self::ProducingFinalOutput) => true,
            (Self::DrainingFinalOutput, Self::DrainingFinalOutput) => true,

            (Self::Failing(a), Self::Failing(b)) => a == b,
            (Self::Cancelling(a), Self::Cancelling(b)) => a == b,
            (Self::Cancelled(a), Self::Cancelled(b)) => a == b,

            (StatefulState::Initialized, StatefulState::Initialized) => true,
            (StatefulState::Accumulating, StatefulState::Accumulating) => true,
            (StatefulState::Emitting, StatefulState::Emitting) => true,
            (Self::EmittingDuringDrain, Self::EmittingDuringDrain) => true,
            (StatefulState::Draining, StatefulState::Draining) => true,
            (StatefulState::Drained, StatefulState::Drained) => true,
            (StatefulState::Failed(a), StatefulState::Failed(b)) => a == b,
            _ => false,
        }
    }
}

impl<H: Send + Sync + 'static> StateVariant for StatefulState<H> {
    fn variant_name(&self) -> &str {
        match self {
            StatefulState::Created => "Created",
            Self::Initializing => "Initializing",
            Self::Starting => "Starting",
            Self::Finalising => "Finalising",
            Self::ValidatingTerminal => "ValidatingTerminal",
            Self::ForwardingTerminal => "ForwardingTerminal",
            Self::ProducingFinalOutput => "ProducingFinalOutput",
            Self::DrainingFinalOutput => "DrainingFinalOutput",

            Self::Failing(..) => "Failing",
            Self::Cancelling(..) => "Cancelling",
            Self::Cancelled(..) => "Cancelled",

            StatefulState::Initialized => "Initialized",
            StatefulState::Accumulating => "Accumulating",
            StatefulState::Emitting => "Emitting",
            Self::EmittingDuringDrain => "EmittingDuringDrain",
            StatefulState::Draining => "Draining",
            StatefulState::Drained => "Drained",
            StatefulState::Failed(_) => "Failed",
            StatefulState::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

impl<H> StatefulState<H> {
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
            Self::Initializing => Phase::Initializing,
            Self::Initialized => Phase::Initialized,
            Self::Accumulating | Self::Emitting => Phase::Active,
            Self::Finalising => Phase::Finalising,
            Self::Failing(cause) => Phase::Failing(cause.clone()),
            Self::Cancelling(reason) => Phase::Cancelling(reason.clone()),
            Self::Drained => Phase::Completed,
            Self::Failed(cause) => Phase::Failed(cause.clone()),
            Self::Cancelled(reason) => Phase::Cancelled(reason.clone()),
            _ => Phase::Other,
        }
    }
}

// ============================================================================
// FSM Events
// ============================================================================

/// Events that can trigger stateful state transitions
pub enum StatefulEvent<H> {
    /// Initialize the stateful stage
    Initialize,
    InitializationCompleted,
    DrainInputsCompleted,
    TerminalValidated,
    TerminalForwarded,
    FinalOutputsPrepared,
    FinalOutputPending,

    ActivationCompleted,
    FinalisationCompleted,
    TerminationSettled,

    /// Ready to start processing (stateful stages start immediately)
    Ready,

    /// Received data event during accumulation
    ReceivedData,

    /// Should emit accumulated results (future: emission strategies)
    ShouldEmit,

    /// Emission complete, return to accumulating
    EmitComplete,

    /// Received EOF from upstream
    ReceivedEOF,

    /// Begin draining process
    BeginDrain,

    /// Draining complete
    DrainComplete,

    /// Unrecoverable error occurred
    Error(String),

    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for StatefulEvent
impl<H> Clone for StatefulEvent<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Initialize => Self::Initialize,
            Self::InitializationCompleted => Self::InitializationCompleted,
            Self::DrainInputsCompleted => Self::DrainInputsCompleted,
            Self::TerminalValidated => Self::TerminalValidated,
            Self::TerminalForwarded => Self::TerminalForwarded,
            Self::FinalOutputsPrepared => Self::FinalOutputsPrepared,
            Self::FinalOutputPending => Self::FinalOutputPending,

            Self::ActivationCompleted => Self::ActivationCompleted,
            Self::FinalisationCompleted => Self::FinalisationCompleted,
            Self::TerminationSettled => Self::TerminationSettled,

            Self::Ready => Self::Ready,
            Self::ReceivedData => Self::ReceivedData,
            Self::ShouldEmit => Self::ShouldEmit,
            Self::EmitComplete => Self::EmitComplete,
            Self::ReceivedEOF => Self::ReceivedEOF,
            Self::BeginDrain => Self::BeginDrain,
            Self::DrainComplete => Self::DrainComplete,
            Self::Error(msg) => Self::Error(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for StatefulEvent<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialize => write!(f, "Initialize"),
            Self::InitializationCompleted => write!(f, "InitializationCompleted"),
            Self::DrainInputsCompleted => write!(f, "DrainInputsCompleted"),
            Self::TerminalValidated => write!(f, "TerminalValidated"),
            Self::TerminalForwarded => write!(f, "TerminalForwarded"),
            Self::FinalOutputsPrepared => write!(f, "FinalOutputsPrepared"),
            Self::FinalOutputPending => write!(f, "FinalOutputPending"),

            Self::ActivationCompleted => write!(f, "ActivationCompleted"),
            Self::FinalisationCompleted => write!(f, "FinalisationCompleted"),
            Self::TerminationSettled => write!(f, "TerminationSettled"),

            Self::Ready => write!(f, "Ready"),
            Self::ReceivedData => write!(f, "ReceivedData"),
            Self::ShouldEmit => write!(f, "ShouldEmit"),
            Self::EmitComplete => write!(f, "EmitComplete"),
            Self::ReceivedEOF => write!(f, "ReceivedEOF"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::DrainComplete => write!(f, "DrainComplete"),
            Self::Error(msg) => write!(f, "Error({msg:?})"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> crate::supervised_base::with_external_events::ExternalControlEvent
    for StatefulEvent<H>
{
    fn discard_details(
        &self,
    ) -> (
        obzenflow_core::event::CommandDiscardDisposition,
        Option<String>,
    ) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Error(message) => Some(message.as_str()),
            Self::InitializationCompleted
            | Self::ActivationCompleted
            | Self::FinalisationCompleted
            | Self::TerminationSettled
            | Self::DrainInputsCompleted
            | Self::TerminalValidated
            | Self::TerminalForwarded
            | Self::FinalOutputsPrepared
            | Self::FinalOutputPending => None,
            Self::Initialize
            | Self::Ready
            | Self::ReceivedData
            | Self::ShouldEmit
            | Self::EmitComplete
            | Self::ReceivedEOF
            | Self::BeginDrain
            | Self::DrainComplete => None,
            Self::_Phantom(_) => unreachable!("PhantomData variant"),
        })
    }
}

impl<H: Send + Sync + 'static> EventVariant for StatefulEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            StatefulEvent::Initialize => "Initialize",
            Self::InitializationCompleted => "InitializationCompleted",
            Self::DrainInputsCompleted => "DrainInputsCompleted",
            Self::TerminalValidated => "TerminalValidated",
            Self::TerminalForwarded => "TerminalForwarded",
            Self::FinalOutputsPrepared => "FinalOutputsPrepared",
            Self::FinalOutputPending => "FinalOutputPending",

            Self::ActivationCompleted => "ActivationCompleted",
            Self::FinalisationCompleted => "FinalisationCompleted",
            Self::TerminationSettled => "TerminationSettled",

            StatefulEvent::Ready => "Ready",
            StatefulEvent::ReceivedData => "ReceivedData",
            StatefulEvent::ShouldEmit => "ShouldEmit",
            StatefulEvent::EmitComplete => "EmitComplete",
            StatefulEvent::ReceivedEOF => "ReceivedEOF",
            StatefulEvent::BeginDrain => "BeginDrain",
            StatefulEvent::DrainComplete => "DrainComplete",
            StatefulEvent::Error(_) => "Error",
            StatefulEvent::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

/// Actions that stateful FSM transitions can emit
pub enum StatefulAction<H> {
    ValidateTerminal {
        drain_requested: bool,
    },
    ForwardTerminal,
    ProduceFinalOutput,
    DrainFinalOutput,
    Host(crate::supervised_base::handler_supervised::SupervisorAction<StatefulEvent<H>>),
    /// Allocate resources (writer ID, subscriptions)
    AllocateResources,

    /// Initialize handler state (call handler.initial_state())
    InitializeState,

    /// Publish running event to journal
    PublishRunning,

    /// Accumulate event into state (call handler.process())
    AccumulateEvent,

    /// Emit accumulated results (future: emission strategies)
    EmitResults,

    /// Forward EOF event downstream
    ForwardEOF,

    /// Send completion event to journal
    SendCompletion,

    /// Send failure event to journal with metrics
    SendFailure {
        message: String,
    },

    /// Clean up all resources
    Cleanup,

    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for StatefulAction
impl<H> Clone for StatefulAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::ValidateTerminal { drain_requested } => Self::ValidateTerminal {
                drain_requested: *drain_requested,
            },
            Self::ForwardTerminal => Self::ForwardTerminal,
            Self::ProduceFinalOutput => Self::ProduceFinalOutput,
            Self::DrainFinalOutput => Self::DrainFinalOutput,
            Self::Host(action) => Self::Host(action.clone()),

            Self::AllocateResources => Self::AllocateResources,
            Self::InitializeState => Self::InitializeState,
            Self::PublishRunning => Self::PublishRunning,
            Self::AccumulateEvent => Self::AccumulateEvent,
            Self::EmitResults => Self::EmitResults,
            Self::ForwardEOF => Self::ForwardEOF,
            Self::SendCompletion => Self::SendCompletion,
            Self::SendFailure { message } => Self::SendFailure {
                message: message.clone(),
            },
            Self::Cleanup => Self::Cleanup,
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for StatefulAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ValidateTerminal { drain_requested } => f
                .debug_struct("ValidateTerminal")
                .field("drain_requested", drain_requested)
                .finish(),
            Self::ForwardTerminal => f.write_str("ForwardTerminal"),
            Self::ProduceFinalOutput => f.write_str("ProduceFinalOutput"),
            Self::DrainFinalOutput => f.write_str("DrainFinalOutput"),
            Self::Host(action) => action.fmt(f),

            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::InitializeState => write!(f, "InitializeState"),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::AccumulateEvent => write!(f, "AccumulateEvent"),
            Self::EmitResults => write!(f, "EmitResults"),
            Self::ForwardEOF => write!(f, "ForwardEOF"),
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PendingTransition {
    EmitComplete,
}

/// Context for stateful handlers - contains everything actions need
pub struct StatefulResources<H: UnifiedStatefulHandler> {
    /// The handler instance (immutable, so wrapped in Arc)
    pub handler: Option<Arc<H>>,

    /// This stateful stage's stage ID
    pub stage_id: obzenflow_core::StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Runtime observer bundle attached to this stage.
    pub observers: crate::stages::observer::StageObserverBundle,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Current accumulated state (FSM-owned, mutated via supervisor)
    pub current_state: H::State,

    /// Data journal for writing chain events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Recorded effect outcomes for replay suppression.
    pub effect_history: Option<Arc<EffectHistory>>,

    /// Runtime execution strategy (FLOWIP-120r).
    pub runtime_execution: crate::execution::RuntimeExecution,

    /// Flow-scoped typed ports available to replay-safe effects.
    pub effect_ports: EffectPortRegistry,

    /// Descriptor-owned effect declarations for replay-safe effect invocation.
    pub effect_declarations: Vec<EffectDeclaration>,

    /// Last delivered data-input position for deterministic typed stateful emissions.
    pub last_input_position: Option<StageInputPosition>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this stateful stage (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// FLOWIP-010 §7: build-resolved lineage policy from stage resources.
    pub lineage_policy: obzenflow_core::config::LineagePolicy,

    /// Subscription to upstream events
    pub subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// FSM-owned contract state for each upstream reader (aligned with subscription readers)
    pub contract_state: Vec<ReaderProgress>,

    /// Supervisor-driven contract-check tick (FLOWIP-080r).
    ///
    /// This avoids starvation under sustained load by allowing the supervisor
    /// to schedule contract checks from the active `PollResult::Event` path.
    pub(crate) last_contract_check: Option<tokio::time::Instant>,

    /// Control event handling strategy
    pub control_strategy: Arc<dyn SignalGate>,

    /// Durable per-stage signal-strategy scratch (FLOWIP-115c).
    pub processing_context: crate::stages::common::control_strategies::ProcessingContext,

    /// EOF event to forward when draining completes
    pub buffered_eof: Option<ChainEvent>,

    /// Original terminal control envelope retained until terminal validation
    /// has succeeded. This lets protocol-aware stateful handlers reject an
    /// incomplete drain before the terminal signal becomes visible
    /// downstream.
    pub terminal_envelope: Option<DeliveredRecord<ChainPayload>>,

    /// Whether the current drain was requested through the stage handle rather
    /// than by an upstream terminal control row.

    /// Worst-wins join over the inputs' terminal EOF kinds (FLOWIP-095k).
    pub terminal_eof_kind: Option<EofKind>,

    /// Last upstream envelope consumed by this stage (with a vector-clock merged across all
    /// consumed inputs).
    ///
    /// Used as the parent for emitted aggregate events so their journal envelopes preserve
    /// happened-before relationships via vector clock propagation, even when upstream events are
    /// concurrent.
    pub last_consumed_envelope: Option<DeliveredRecord<ChainPayload>>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Bound subscription factory for this stage's upstream journals
    pub upstream_subscription_factory: BoundSubscriptionFactory,

    /// Counter of accumulated events since the last observability heartbeat
    /// (FLOWIP-059 Phase 6.4 - accumulator heartbeats).
    pub events_since_last_heartbeat: u64,

    /// FLOWIP-010: build-resolved `runtime.heartbeat_interval` (events
    /// between heartbeats; 0 disables), from stage resources.
    pub heartbeat_interval: u64,

    /// Baseline for supervisor-driven `emit_interval` timing (FLOWIP-086h).
    ///
    /// FLOWIP-114o: reads `tokio::time::Instant` (matching the sibling
    /// `last_contract_check`) so Tokio paused time advances it in deterministic
    /// tests. Latency measurements elsewhere keep `std::time::Instant`.
    pub last_data_event_time: Option<tokio::time::Instant>,

    /// Optional supervisor-driven emit interval for timer-driven emission while idle (FLOWIP-086h).
    pub emit_interval: Option<Duration>,

    /// Backpressure writer handle for this stage's journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Declared stage output contract used by the shared output commit path.
    pub output_contract: StageOutputContract,

    /// Backpressure readers keyed by upstream stage ID (FLOWIP-086k).
    pub backpressure_readers: HashMap<StageId, BackpressureReader>,

    /// Pending data outputs blocked on downstream credits (Phase 1: bounded to one input).
    pub(crate) pending_outputs:
        VecDeque<crate::stages::common::supervision::backpressure_drain::PendingOutput>,

    /// Pending state transition once blocked outputs are fully written.
    pub(crate) pending_transition: Option<PendingTransition>,

    /// Upstream stage awaiting a consumption ack once pending outputs are drained.
    pub(crate) pending_ack_upstream: Option<StageId>,

    /// Backpressure activity pulse accumulator (Hz UI animation driver).
    pub(crate) backpressure_pulse: BackpressureActivityPulse,

    /// Start of the current backpressure stall episode; anchored at the
    /// first credit miss, cleared on successful reserve (FLOWIP-115e).
    pub(crate) backpressure_stall: Option<tokio::time::Instant>,

    /// Optional per-stage heartbeat task (FLOWIP-063e).
    pub(crate) heartbeat: Option<HeartbeatHandle>,

    /// Catch-up flip latch (FLOWIP-120n): the last generation this stage
    /// flipped at, making the flip idempotent per generation across both
    /// triggers (watermark and authored EOF).
    pub(crate) catch_up_flip: Option<obzenflow_core::ReaderGeneration>,
}

pub struct StatefulContext<H: UnifiedStatefulHandler> {
    pub(crate) resources: Option<StatefulResources<H>>,
    pub(crate) instrumentation: Arc<StageInstrumentation>,
    pub(crate) drain_requested_by_handle: bool,
}
impl<H: UnifiedStatefulHandler> StatefulContext<H> {
    pub(crate) fn new(resources: StatefulResources<H>) -> Self {
        Self {
            drain_requested_by_handle: false,
            instrumentation: resources.instrumentation.clone(),
            resources: Some(resources),
        }
    }
    pub(crate) fn resources_mut(
        &mut self,
    ) -> Result<&mut StatefulResources<H>, obzenflow_fsm::FsmError> {
        self.resources.as_mut().ok_or_else(|| {
            obzenflow_fsm::FsmError::HandlerError(
                "stateful resources belong to a pending operation".into(),
            )
        })
    }
}
impl<H: UnifiedStatefulHandler + 'static> FsmContext for StatefulContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: UnifiedStatefulHandler + Clone + Send + Sync + 'static> FsmAction for StatefulAction<H> {
    type Context = StatefulContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        self.execute_resources(ctx.resources_mut()?).await
    }
}

impl<H: UnifiedStatefulHandler + Clone + Send + Sync + 'static> StatefulAction<H> {
    pub(crate) async fn execute_resources(
        &self,
        ctx: &mut StatefulResources<H>,
    ) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            StatefulAction::ValidateTerminal { .. }
            | StatefulAction::ForwardTerminal
            | StatefulAction::ProduceFinalOutput
            | StatefulAction::DrainFinalOutput
            | StatefulAction::Host(_) => Err(obzenflow_fsm::FsmError::HandlerError(
                "host action requires the supervised runner".into(),
            )),

            StatefulAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);

                // Initialize FSM-owned contract state for each upstream reader
                let upstream_ids = ctx.upstream_subscription_factory.upstream_stage_ids();
                ctx.contract_state = upstream_ids.into_iter().map(ReaderProgress::new).collect();

                // Create subscription using bound factory with contracts
                if !ctx.upstream_subscription_factory.is_empty() {
                    let subscription = ctx
                        .upstream_subscription_factory
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
                                "Failed to create subscription: {e}"
                            ))
                        })?
                        .with_contract_flow_context(make_flow_context(
                            &ctx.flow_name,
                            &ctx.flow_id.to_string(),
                            &ctx.stage_name,
                            ctx.stage_id,
                            StageType::Stateful,
                        ));

                    ctx.subscription = Some(subscription);

                    // archive-io: recorded effect history is genuine I/O, not a phase decision (FLOWIP-120r).
                    let recorded_history = ctx.runtime_execution.archive_for_io();
                    if let Some(archive) = recorded_history {
                        let history = EffectHistory::load(archive, &ctx.stage_name)
                            .await
                            .map_err(|e| {
                                obzenflow_fsm::FsmError::HandlerError(format!(
                                    "Failed to load effect history for '{}': {e}",
                                    ctx.stage_name
                                ))
                            })?;
                        // FLOWIP-120n F7: register the recorded effect mark so a
                        // prefix cursor miss fails loud and a live-tail miss runs.
                        if let Some(control) = ctx.runtime_execution.resume_control() {
                            if let Some(max) = history.max_recorded_input_seq() {
                                control.record_effect_high_water(
                                    ctx.stage_id,
                                    crate::messaging::upstream_subscription::StageInputPosition(
                                        max,
                                    ),
                                );
                            }
                        }
                        ctx.effect_history = Some(Arc::new(history));
                    }

                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        upstream_count = ctx.upstream_subscription_factory.upstream_stage_ids().len(),
                        "Created subscription using bound factory"
                    );
                } else {
                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        "No upstream journals - skipping subscription creation"
                    );
                }

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage allocated resources"
                );
                Ok(())
            }

            StatefulAction::InitializeState => {
                // State is initialized when building StatefulContext.
                // This action is a placeholder for future checkpoint/resume functionality
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage initialized state"
                );
                Ok(())
            }

            StatefulAction::PublishRunning => {
                lifecycle_actions::publish_running(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::Stateful,
                    ),
                )
                .await?;
                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
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
                                stage_type: StageType::Stateful,
                            },
                        )
                    },
                );
                Ok(())
            }

            StatefulAction::AccumulateEvent => {
                // This is handled in dispatch_state for the Accumulating state
                // This action is a placeholder for consistency
                Ok(())
            }

            StatefulAction::EmitResults => {
                // Future: emission strategies (FLOWIP-080c)
                // For now, this is a no-op
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage emission complete"
                );
                Ok(())
            }

            StatefulAction::ForwardEOF => {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to forward EOF".to_string(),
                    )
                })?;

                // Always emit an EOF authored by this stage, preserving the
                // upstream vector clock while sealing only this stage's Data
                // frontier.
                let buffered = ctx.buffered_eof.take();
                // FLOWIP-095k: the authored kind is the worst-wins join over the
                // inputs' terminal kinds; Natural covers the drain-terminated
                // path where no EOF was received.
                let eof_kind = ctx.terminal_eof_kind.unwrap_or(EofKind::Natural);
                let mut upstream_vector_clock = None;
                let runtime_context = ctx.instrumentation.snapshot();
                let (authored_writer_seq, writer_seq_by_event_type, authored_last_event_id) =
                    ctx.instrumentation.authored_data_frontier();

                if let Some(buffered_event) = buffered {
                    if let ChainPayload::FlowControl(FlowControlPayload::Eof {
                        writer_seq: _,
                        vector_clock,
                        ..
                    }) = buffered_event.payload.clone()
                    {
                        upstream_vector_clock = vector_clock;
                        // We intentionally ignore the upstream writer_seq and
                        // last_event_id and advertise our own position below.
                    }
                }

                let mut eof_event = ChainEventFactory::eof_event_with_kind(writer_id, eof_kind);

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
                    if let Some(vc) = upstream_vector_clock {
                        *vector_clock = Some(vc);
                    }
                    *last_event_id = authored_last_event_id;
                }

                // Attach flow/runtime context for downstream contract tracking
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::Stateful,
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
                    "Stateful stage forwarded EOF downstream"
                );
                Ok(())
            }

            StatefulAction::SendCompletion => {
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
                        obzenflow_core::event::context::StageType::Stateful,
                    ),
                    ctx.instrumentation.as_ref(),
                )
                .await?;
                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
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
                                stage_type: StageType::Stateful,
                            },
                        )
                    },
                );
                Ok(())
            }

            StatefulAction::SendFailure { message } => {
                lifecycle_actions::send_failure(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::Stateful,
                    ),
                    message,
                    ctx.instrumentation.as_ref(),
                    None,
                )
                .await?;
                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
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
                                stage_type: StageType::Stateful,
                            },
                        )
                    },
                );
                Ok(())
            }

            StatefulAction::Cleanup => {
                ctx.handler.take();
                ctx.subscription.take();
                if let Some(heartbeat) = ctx.heartbeat.take() {
                    heartbeat.cancel();
                }

                lifecycle_actions::cleanup_best_effort("Stateful", &ctx.stage_name, || async {
                    Ok::<(), ()>(())
                })
                .await;
                Ok(())
            }

            StatefulAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}
