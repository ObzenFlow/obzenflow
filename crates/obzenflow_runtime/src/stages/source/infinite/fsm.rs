// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Infinite source stage FSM types and state machine definition
//!
//! Infinite sources never complete naturally (Kafka, WebSocket, etc).
//! They have a unique "WaitingForGun" state that ensures they don't
//! start emitting events until the pipeline is ready.

use crate::stages::common::stage_handle::{
    FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};

use crate::stages::observer::StageLifecyclePhase;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::types::Count;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateMachine, StateVariant};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::backpressure::BackpressureWriter;
use crate::feed_plan::StageOutputContract;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::observer::dispatch::run_stage_lifecycle_observers;
use crate::stages::source::strategies::{CompletionContext, CompletionGate};

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for infinite source stages
#[derive(Serialize, Deserialize)]
pub enum InfiniteSourceState<H> {
    /// Initial state - source has been created but not initialized
    Created,

    Initializing,
    Starting,
    Finalising,
    AcquiringInput,
    Failing(String),
    Cancelling(String),
    Cancelled(String),

    /// Resources allocated, subscriptions created, ready to wait for start signal
    Initialized,

    /// UNIQUE TO SOURCES: Waiting for explicit start command from pipeline
    /// This prevents sources from emitting events before the pipeline is ready
    WaitingForGun,

    /// Actively producing events - the source is now allowed to emit
    Running,

    /// Shutting down gracefully, finishing any pending work
    Draining,

    /// All work complete, EOF sent downstream
    Drained,

    /// Unrecoverable error occurred
    Failed(String),

    #[serde(skip)]
    _Phantom(std::marker::PhantomData<H>),
}

// Manual implementations that don't require H to implement these traits
impl<H> Clone for InfiniteSourceState<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Created => Self::Created,
            Self::Initializing => Self::Initializing,
            Self::Starting => Self::Starting,
            Self::Finalising => Self::Finalising,
            Self::AcquiringInput => Self::AcquiringInput,
            Self::Failing(cause) => Self::Failing(cause.clone()),
            Self::Cancelling(cause) => Self::Cancelling(cause.clone()),
            Self::Cancelled(cause) => Self::Cancelled(cause.clone()),

            Self::Initialized => Self::Initialized,
            Self::WaitingForGun => Self::WaitingForGun,
            Self::Running => Self::Running,
            Self::Draining => Self::Draining,
            Self::Drained => Self::Drained,
            Self::Failed(msg) => Self::Failed(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(std::marker::PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for InfiniteSourceState<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Initializing => write!(f, "Initializing"),
            Self::Starting => write!(f, "Starting"),
            Self::Finalising => write!(f, "Finalising"),
            Self::AcquiringInput => write!(f, "AcquiringInput"),
            Self::Failing(cause) => write!(f, "Failing({cause:?})"),
            Self::Cancelling(cause) => write!(f, "Cancelling({cause:?})"),
            Self::Cancelled(cause) => write!(f, "Cancelled({cause:?})"),

            Self::Initialized => write!(f, "Initialized"),
            Self::WaitingForGun => write!(f, "WaitingForGun"),
            Self::Running => write!(f, "Running"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({msg:?})"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync> PartialEq for InfiniteSourceState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (InfiniteSourceState::Created, InfiniteSourceState::Created) => true,
            (Self::Initializing, Self::Initializing) => true,
            (Self::Starting, Self::Starting) => true,
            (Self::Finalising, Self::Finalising) => true,
            (Self::AcquiringInput, Self::AcquiringInput) => true,
            (Self::Failing(a), Self::Failing(b)) => a == b,
            (Self::Cancelling(a), Self::Cancelling(b)) => a == b,
            (Self::Cancelled(a), Self::Cancelled(b)) => a == b,

            (InfiniteSourceState::Initialized, InfiniteSourceState::Initialized) => true,
            (InfiniteSourceState::WaitingForGun, InfiniteSourceState::WaitingForGun) => true,
            (InfiniteSourceState::Running, InfiniteSourceState::Running) => true,
            (InfiniteSourceState::Draining, InfiniteSourceState::Draining) => true,
            (InfiniteSourceState::Drained, InfiniteSourceState::Drained) => true,
            (InfiniteSourceState::Failed(a), InfiniteSourceState::Failed(b)) => a == b,
            _ => false,
        }
    }
}

impl<H: Send + Sync + 'static> StateVariant for InfiniteSourceState<H> {
    fn variant_name(&self) -> &str {
        match self {
            InfiniteSourceState::Created => "Created",
            Self::Initializing => "Initializing",
            Self::Starting => "Starting",
            Self::Finalising => "Finalising",
            Self::AcquiringInput => "AcquiringInput",
            Self::Failing(..) => "Failing",
            Self::Cancelling(..) => "Cancelling",
            Self::Cancelled(..) => "Cancelled",

            InfiniteSourceState::Initialized => "Initialized",
            InfiniteSourceState::WaitingForGun => "WaitingForGun",
            InfiniteSourceState::Running => "Running",
            InfiniteSourceState::Draining => "Draining",
            InfiniteSourceState::Drained => "Drained",
            InfiniteSourceState::Failed(_) => "Failed",
            InfiniteSourceState::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

impl<H> InfiniteSourceState<H> {
    pub(crate) fn defer_external_event(state: &Self, event: &InfiniteSourceEvent<H>) -> bool {
        match state {
            Self::Initializing => matches!(
                event,
                InfiniteSourceEvent::Ready
                    | InfiniteSourceEvent::Start
                    | InfiniteSourceEvent::BeginDrain
            ),
            Self::AcquiringInput | Self::Starting | Self::Running => !matches!(
                event,
                InfiniteSourceEvent::Error(_) | InfiniteSourceEvent::BeginDrain
            ),
            _ => false,
        }
    }

    pub(crate) fn failure(cause: String) -> Self {
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
            Self::WaitingForGun => Phase::Ready,
            Self::Running => Phase::Active,
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

/// Events that can trigger infinite source state transitions
pub enum InfiniteSourceEvent<H> {
    /// Initialize the source - allocate resources, create writer ID
    Initialize,
    InitializationCompleted,
    ActivationCompleted,
    FinalisationCompleted,
    TerminationSettled,
    InputAcquired,
    ResumeLiveInput,

    /// Source is ready - transition to WaitingForGun state
    Ready,

    /// Start event production - the "gun" has been fired!
    /// Only sources receive this event
    Start,

    /// Begin graceful shutdown - stop producing new events
    BeginDrain,

    /// Drain completed (after shutdown requested)
    Completed,

    /// Unrecoverable error occurred
    Error(String),

    #[doc(hidden)]
    _Phantom(std::marker::PhantomData<H>),
}

// Manual implementations for InfiniteSourceEvent
impl<H> Clone for InfiniteSourceEvent<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Initialize => Self::Initialize,
            Self::InitializationCompleted => Self::InitializationCompleted,
            Self::ActivationCompleted => Self::ActivationCompleted,
            Self::FinalisationCompleted => Self::FinalisationCompleted,
            Self::TerminationSettled => Self::TerminationSettled,
            Self::InputAcquired => Self::InputAcquired,
            Self::ResumeLiveInput => Self::ResumeLiveInput,

            Self::Ready => Self::Ready,
            Self::Start => Self::Start,
            Self::BeginDrain => Self::BeginDrain,
            Self::Completed => Self::Completed,
            Self::Error(msg) => Self::Error(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(std::marker::PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for InfiniteSourceEvent<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialize => write!(f, "Initialize"),
            Self::InitializationCompleted => write!(f, "InitializationCompleted"),
            Self::ActivationCompleted => write!(f, "ActivationCompleted"),
            Self::FinalisationCompleted => write!(f, "FinalisationCompleted"),
            Self::TerminationSettled => write!(f, "TerminationSettled"),
            Self::InputAcquired => write!(f, "InputAcquired"),
            Self::ResumeLiveInput => write!(f, "ResumeLiveInput"),

            Self::Ready => write!(f, "Ready"),
            Self::Start => write!(f, "Start"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::Completed => write!(f, "Completed"),
            Self::Error(msg) => write!(f, "Error({msg:?})"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> crate::supervised_base::with_external_events::ExternalControlEvent
    for InfiniteSourceEvent<H>
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
            | Self::InputAcquired
            | Self::ResumeLiveInput => None,
            Self::Initialize | Self::Ready | Self::Start | Self::BeginDrain | Self::Completed => {
                None
            }
            Self::_Phantom(_) => unreachable!("PhantomData variant"),
        })
    }
}

impl<H: Send + Sync + 'static> EventVariant for InfiniteSourceEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            InfiniteSourceEvent::Initialize => "Initialize",
            Self::InitializationCompleted => "InitializationCompleted",
            Self::ActivationCompleted => "ActivationCompleted",
            Self::FinalisationCompleted => "FinalisationCompleted",
            Self::TerminationSettled => "TerminationSettled",
            Self::InputAcquired => "InputAcquired",
            Self::ResumeLiveInput => "ResumeLiveInput",

            InfiniteSourceEvent::Ready => "Ready",
            InfiniteSourceEvent::Start => "Start",
            InfiniteSourceEvent::BeginDrain => "BeginDrain",
            InfiniteSourceEvent::Completed => "Completed",
            InfiniteSourceEvent::Error(_) => "Error",
            InfiniteSourceEvent::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

/// Actions that infinite source FSM transitions can emit
pub enum InfiniteSourceAction<H> {
    Host(crate::supervised_base::handler_supervised::SupervisorAction<InfiniteSourceEvent<H>>),
    /// Allocate resources needed by the source
    /// - Register writer ID with journal
    /// - Open connections, etc.
    AllocateResources,

    /// Send EOF event downstream to signal completion
    SendEOF,

    /// Send error event to journal for diagnostics
    SendError {
        message: String,
    },

    /// Publish running lifecycle event
    PublishRunning,

    /// Write stage completed event
    WriteStageCompleted,

    /// Clean up all resources
    Cleanup,

    #[doc(hidden)]
    _Phantom(std::marker::PhantomData<H>),
}

// ============================================================================
// FSM Context
// ============================================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InfiniteSourceCompletionReason {
    ExternalDrain,
    /// The live handler finished or its boundary was rejected.
    LiveEof,
    /// Replay exhaustion reproduces the archive's recorded kind (FLOWIP-095k);
    /// `None`: the archive committed no EOF.
    ReplayExhausted {
        recorded_kind: Option<EofKind>,
    },
}

/// Context for infinite source handlers - contains everything actions need
pub struct InfiniteSourceResources<H> {
    /// This source's stage ID
    pub stage_id: obzenflow_core::StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Runtime observer bundle attached to this source boundary.
    pub observers: crate::stages::observer::StageObserverBundle,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Data journal for writing generated events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events

    /// Runtime execution strategy (FLOWIP-120r).
    pub runtime_execution: crate::execution::RuntimeExecution,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this source (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// Why the source is shutting down (affects EOF semantics).
    pub completion_reason: InfiniteSourceCompletionReason,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Source control strategy for this stage
    pub control_strategy: Arc<dyn CompletionGate>,

    /// Mutable context for the source control strategy
    pub control_context: CompletionContext,

    /// Backpressure writer handle for this stage's journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Declared stage output contract used by the shared output commit path.
    pub output_contract: StageOutputContract,

    /// Pending stage outputs blocked on downstream credits (FLOWIP-086k).
    pub(crate) pending_outputs:
        VecDeque<crate::stages::common::supervision::backpressure_drain::PendingOutput>,

    /// Backpressure activity pulse accumulator (Hz UI animation driver).
    pub(crate) backpressure_pulse: BackpressureActivityPulse,

    /// Start of the current backpressure stall episode; anchored at the
    /// first credit miss, cleared on successful reserve (FLOWIP-115e).
    pub(crate) backpressure_stall: Option<tokio::time::Instant>,

    /// Committed source diagnostic the lifecycle failure links to (084n B2).
    pub(crate) failure_causal_event_id: Option<obzenflow_core::EventId>,

    /// Phantom to keep the handler type in the context's type parameters
    _marker: PhantomData<H>,
}

pub struct InfiniteSourceContextInit {
    pub stage_id: obzenflow_core::StageId,
    pub stage_name: String,
    pub observers: crate::stages::observer::StageObserverBundle,
    pub flow_name: String,
    pub flow_id: FlowId,
    pub data_journal: Arc<dyn Journal<ChainEvent>>,
    pub error_journal: Arc<dyn Journal<ChainEvent>>,
    pub runtime_execution: crate::execution::RuntimeExecution,
    pub bus: Arc<crate::message_bus::FsmMessageBus>,
    pub instrumentation: Arc<StageInstrumentation>,
    pub control_strategy: Arc<dyn CompletionGate>,
    pub backpressure_writer: BackpressureWriter,
    pub output_contract: StageOutputContract,
}

impl<H> InfiniteSourceResources<H> {
    pub fn new(init: InfiniteSourceContextInit) -> Self {
        Self {
            stage_id: init.stage_id,
            stage_name: init.stage_name,
            observers: init.observers,
            flow_name: init.flow_name,
            flow_id: init.flow_id,
            data_journal: init.data_journal,
            error_journal: init.error_journal,
            runtime_execution: init.runtime_execution,
            bus: init.bus,
            writer_id: None,
            completion_reason: InfiniteSourceCompletionReason::ExternalDrain,
            instrumentation: init.instrumentation,
            control_strategy: init.control_strategy,
            control_context: CompletionContext::new(),
            backpressure_writer: init.backpressure_writer,
            output_contract: init.output_contract,
            pending_outputs: VecDeque::new(),
            backpressure_pulse: BackpressureActivityPulse::new(),
            backpressure_stall: None,
            failure_causal_event_id: None,
            _marker: PhantomData,
        }
    }
}

/// FSM observation data remains available while an owned operation uses resources.
pub struct InfiniteSourceContext<H> {
    pub(crate) resources: Option<InfiniteSourceResources<H>>,
    pub instrumentation: Arc<StageInstrumentation>,
}

impl<H> InfiniteSourceContext<H> {
    pub fn new(init: InfiniteSourceContextInit) -> Self {
        let instrumentation = init.instrumentation.clone();
        Self {
            resources: Some(InfiniteSourceResources::new(init)),
            instrumentation,
        }
    }

    pub(crate) fn resources_mut(
        &mut self,
    ) -> Result<&mut InfiniteSourceResources<H>, obzenflow_fsm::FsmError> {
        self.resources.as_mut().ok_or_else(|| {
            obzenflow_fsm::FsmError::HandlerError("source operation owns resources".into())
        })
    }
}

impl<H: Send + Sync + 'static> FsmContext for InfiniteSourceContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

// Manual implementations for InfiniteSourceAction
impl<H> Clone for InfiniteSourceAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Host(action) => Self::Host(action.clone()),

            Self::AllocateResources => Self::AllocateResources,
            Self::SendEOF => Self::SendEOF,
            Self::SendError { message } => Self::SendError {
                message: message.clone(),
            },
            Self::PublishRunning => Self::PublishRunning,
            Self::WriteStageCompleted => Self::WriteStageCompleted,
            Self::Cleanup => Self::Cleanup,
            Self::_Phantom(_) => Self::_Phantom(std::marker::PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for InfiniteSourceAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Host(action) => action.fmt(f),

            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::SendEOF => write!(f, "SendEOF"),
            Self::SendError { message } => write!(f, "SendError({message:?})"),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::WriteStageCompleted => write!(f, "WriteStageCompleted"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

#[async_trait::async_trait]
impl<H: Send + Sync + 'static> FsmAction for InfiniteSourceAction<H> {
    type Context = InfiniteSourceContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        self.execute_resources(ctx.resources_mut()?).await
    }
}

impl<H: Send + Sync + 'static> InfiniteSourceAction<H> {
    pub(crate) async fn execute_resources(
        &self,
        ctx: &mut InfiniteSourceResources<H>,
    ) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            InfiniteSourceAction::Host(_) => Err(obzenflow_fsm::FsmError::HandlerError(
                "host action requires the supervised runner".into(),
            )),

            InfiniteSourceAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    writer_id = %writer_id,
                    "Infinite source allocated resources and registered writer"
                );
                Ok(())
            }

            InfiniteSourceAction::SendEOF => {
                let emitted = ctx
                    .instrumentation
                    .events_processed_total
                    .load(std::sync::atomic::Ordering::Relaxed);

                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to send EOF".to_string(),
                    )
                })?;

                // FLOWIP-095k: replay exhaustion reproduces the archive's
                // recorded kind and never consults the live control strategy.
                let eof_kind = match ctx.completion_reason {
                    InfiniteSourceCompletionReason::ExternalDrain => {
                        let _ = ctx
                            .control_strategy
                            .on_begin_drain(&mut ctx.control_context);
                        EofKind::Poison
                    }
                    InfiniteSourceCompletionReason::LiveEof => {
                        let decision = ctx
                            .control_strategy
                            .on_natural_completion(&mut ctx.control_context);
                        if matches!(
                            decision,
                            crate::stages::source::strategies::CompletionDecision::PoisonEof
                        ) {
                            EofKind::Poison
                        } else {
                            EofKind::Natural
                        }
                    }
                    InfiniteSourceCompletionReason::ReplayExhausted { recorded_kind } => {
                        recorded_kind.unwrap_or(EofKind::Truncated)
                    }
                };

                // Take a final runtime snapshot for wide-event semantics
                let runtime_context = ctx.instrumentation.capture_accounting();
                let (authored_writer_seq, writer_seq_by_event_type, authored_last_event_id) =
                    ctx.instrumentation.authored_data_frontier();

                let mut eof_event = ChainEventFactory::eof_event_with_kind(writer_id, eof_kind);
                if let ChainPayload::FlowControl(FlowControlPayload::Eof {
                    writer_id: writer_id_field,
                    writer_seq,
                    writer_seq_by_event_type: eof_writer_seq_by_event_type,
                    writer_seq_by_event_type_complete,
                    last_event_id,
                    ..
                }) = &mut eof_event.payload
                {
                    *writer_id_field = Some(writer_id);
                    *writer_seq = Some(authored_writer_seq);
                    *eof_writer_seq_by_event_type = writer_seq_by_event_type.clone();
                    *writer_seq_by_event_type_complete = true;
                    *last_event_id = authored_last_event_id;
                }

                // Attach flow/runtime context so the final journal record is a wide snapshot
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::InfiniteSource,
                };
                eof_event = runtime_context.attach_to(eof_event);

                crate::supervised_base::publication::append(
                    &ctx.data_journal,
                    eof_event,
                    AppendOptions::default()
                        .with_capture(ctx.instrumentation.journal_capture(None, vec![(0, false)])),
                )
                .await
                .map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!("Failed to send EOF: {e}"))
                })?;

                let mut final_event = ChainEventFactory::flow_signal_event(
                    writer_id,
                    FlowControlPayload::ProductionFinal {
                        produced_count: Count(authored_writer_seq.0),
                        produced_by_event_type: writer_seq_by_event_type,
                        end_kind: eof_kind,
                        last_event_id: authored_last_event_id,
                    },
                );

                final_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::InfiniteSource,
                };
                final_event = ctx
                    .instrumentation
                    .capture_accounting()
                    .attach_to(final_event);

                crate::supervised_base::publication::append(
                    &ctx.data_journal,
                    final_event,
                    AppendOptions::default()
                        .with_capture(ctx.instrumentation.journal_capture(None, vec![(0, false)])),
                )
                .await
                .map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to send source production_finalized: {e}"
                    ))
                })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    emitted,
                    eof_kind = ?eof_kind,
                    reason = ?ctx.completion_reason,
                    "Infinite source sent EOF and production_finalized"
                );
                Ok(())
            }

            InfiniteSourceAction::SendError { message } => {
                crate::stages::common::supervision::lifecycle_actions::send_failure(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::InfiniteSource,
                    ),
                    message,
                    ctx.instrumentation.as_ref(),
                    ctx.failure_causal_event_id,
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
                                stage_type: StageType::InfiniteSource,
                            },
                        )
                    },
                );
                Ok(())
            }

            InfiniteSourceAction::PublishRunning => {
                // Write running event to system journal
                crate::stages::common::supervision::lifecycle_actions::publish_running(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::InfiniteSource,
                    ),
                )
                .await?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Infinite source published running event"
                );
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
                                stage_type: StageType::InfiniteSource,
                            },
                        )
                    },
                );
                Ok(())
            }

            InfiniteSourceAction::WriteStageCompleted => {
                // Write completion event to system journal with tail-read metrics.
                //
                // Some stages may legitimately complete without emitting any runtime-context
                // bearing events (e.g. zero input). In that case, fall back to a best-effort
                // snapshot from instrumentation instead of failing completion.
                crate::stages::common::supervision::lifecycle_actions::send_completion(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::InfiniteSource,
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
                                stage_type: StageType::InfiniteSource,
                            },
                        )
                    },
                );
                Ok(())
            }

            InfiniteSourceAction::Cleanup => {
                // Handler-specific cleanup would go here
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Infinite source cleaned up resources"
                );
                Ok(())
            }

            InfiniteSourceAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// Type Alias for FSM
// ============================================================================

/// Type alias for infinite source FSM
pub type InfiniteSourceFsm<H> = StateMachine<
    InfiniteSourceState<H>,
    InfiniteSourceEvent<H>,
    InfiniteSourceContext<H>,
    InfiniteSourceAction<H>,
>;
