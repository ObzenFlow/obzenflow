// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Finite source stage FSM types and state machine definition
//!
//! Finite sources eventually complete (files, bounded collections).
//! They have a unique "WaitingForGun" state that ensures they don't
//! start emitting events until the pipeline is ready.

use crate::stages::common::stage_handle::{
    FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};

use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::observer::StageLifecyclePhase;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::types::{Count, JournalIndex, JournalPath};
use obzenflow_core::event::{ChainEventFactory, ChainPayload, SourceContractEventParams};
use obzenflow_core::journal::AppendOptions;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
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

/// FSM states for finite source stages
#[derive(Serialize, Deserialize)]
pub enum FiniteSourceState<H> {
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
    _Phantom(PhantomData<H>),
}

// Manual implementations that don't require H to implement these traits
impl<H> Clone for FiniteSourceState<H> {
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
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for FiniteSourceState<H> {
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

impl<H: Send + Sync> PartialEq for FiniteSourceState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FiniteSourceState::Created, FiniteSourceState::Created) => true,
            (Self::Initializing, Self::Initializing) => true,
            (Self::Starting, Self::Starting) => true,
            (Self::Finalising, Self::Finalising) => true,
            (Self::AcquiringInput, Self::AcquiringInput) => true,
            (Self::Failing(a), Self::Failing(b)) => a == b,
            (Self::Cancelling(a), Self::Cancelling(b)) => a == b,
            (Self::Cancelled(a), Self::Cancelled(b)) => a == b,

            (FiniteSourceState::Initialized, FiniteSourceState::Initialized) => true,
            (FiniteSourceState::WaitingForGun, FiniteSourceState::WaitingForGun) => true,
            (FiniteSourceState::Running, FiniteSourceState::Running) => true,
            (FiniteSourceState::Draining, FiniteSourceState::Draining) => true,
            (FiniteSourceState::Drained, FiniteSourceState::Drained) => true,
            (FiniteSourceState::Failed(a), FiniteSourceState::Failed(b)) => a == b,
            _ => false,
        }
    }
}

impl<H: Send + Sync + 'static> StateVariant for FiniteSourceState<H> {
    fn variant_name(&self) -> &str {
        match self {
            FiniteSourceState::Created => "Created",
            Self::Initializing => "Initializing",
            Self::Starting => "Starting",
            Self::Finalising => "Finalising",
            Self::AcquiringInput => "AcquiringInput",
            Self::Failing(..) => "Failing",
            Self::Cancelling(..) => "Cancelling",
            Self::Cancelled(..) => "Cancelled",

            FiniteSourceState::Initialized => "Initialized",
            FiniteSourceState::WaitingForGun => "WaitingForGun",
            FiniteSourceState::Running => "Running",
            FiniteSourceState::Draining => "Draining",
            FiniteSourceState::Drained => "Drained",
            FiniteSourceState::Failed(_) => "Failed",
            FiniteSourceState::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

impl<H> FiniteSourceState<H> {
    pub(crate) fn defer_external_event(state: &Self, event: &FiniteSourceEvent<H>) -> bool {
        match state {
            Self::Initializing => matches!(
                event,
                FiniteSourceEvent::Ready | FiniteSourceEvent::Start | FiniteSourceEvent::BeginDrain
            ),
            Self::AcquiringInput | Self::Starting | Self::Running => !matches!(
                event,
                FiniteSourceEvent::Error(_) | FiniteSourceEvent::BeginDrain
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

/// Events that can trigger finite source state transitions
pub enum FiniteSourceEvent<H> {
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

    /// Source completed naturally (finite sources only)
    /// This is triggered when is_complete() returns true
    Completed,

    /// Unrecoverable error occurred
    Error(String),

    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for FiniteSourceEvent
impl<H> Clone for FiniteSourceEvent<H> {
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
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for FiniteSourceEvent<H> {
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
    for FiniteSourceEvent<H>
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

impl<H: Send + Sync + 'static> EventVariant for FiniteSourceEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            FiniteSourceEvent::Initialize => "Initialize",
            Self::InitializationCompleted => "InitializationCompleted",
            Self::ActivationCompleted => "ActivationCompleted",
            Self::FinalisationCompleted => "FinalisationCompleted",
            Self::TerminationSettled => "TerminationSettled",
            Self::InputAcquired => "InputAcquired",
            Self::ResumeLiveInput => "ResumeLiveInput",

            FiniteSourceEvent::Ready => "Ready",
            FiniteSourceEvent::Start => "Start",
            FiniteSourceEvent::BeginDrain => "BeginDrain",
            FiniteSourceEvent::Completed => "Completed",
            FiniteSourceEvent::Error(_) => "Error",
            FiniteSourceEvent::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

/// Actions that finite source FSM transitions can emit
pub enum FiniteSourceAction<H> {
    Host(crate::supervised_base::handler_supervised::SupervisorAction<FiniteSourceEvent<H>>),
    /// Allocate resources needed by the source
    /// - Register writer ID with journal
    /// - Open file handles, network connections, etc.
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
    _Phantom(PhantomData<H>),
}

// ============================================================================
// FSM Context
// ============================================================================

/// Why the source is completing (FLOWIP-095k). Replay exhaustion reproduces
/// the archive's recorded kind; live completion consults the control strategy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SourceCompletionOrigin {
    #[default]
    Live,
    ReplayExhausted {
        /// The archive's recorded completion kind; `None`: no committed EOF.
        recorded_kind: Option<EofKind>,
    },
}

/// Context for finite source handlers - contains everything actions need
pub struct FiniteSourceResources<H> {
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

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Source control strategy for this stage
    pub control_strategy: Arc<dyn CompletionGate>,

    /// Mutable context for the source control strategy
    pub control_context: CompletionContext,

    /// Why the source is completing (affects EOF kind, FLOWIP-095k).
    pub completion_origin: SourceCompletionOrigin,

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

    /// Phantom to keep the handler type in the context's type parameters
    _marker: PhantomData<H>,
}

pub struct FiniteSourceContextInit {
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

impl<H> FiniteSourceResources<H> {
    pub fn new(init: FiniteSourceContextInit) -> Self {
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
            instrumentation: init.instrumentation,
            control_strategy: init.control_strategy,
            control_context: CompletionContext::new(),
            completion_origin: SourceCompletionOrigin::Live,
            backpressure_writer: init.backpressure_writer,
            output_contract: init.output_contract,
            pending_outputs: VecDeque::new(),
            backpressure_pulse: BackpressureActivityPulse::new(),
            backpressure_stall: None,
            _marker: PhantomData,
        }
    }
}

/// FSM observation data remains available while an owned operation uses resources.
pub struct FiniteSourceContext<H> {
    pub(crate) resources: Option<FiniteSourceResources<H>>,
    pub instrumentation: Arc<StageInstrumentation>,
}

impl<H> FiniteSourceContext<H> {
    pub fn new(init: FiniteSourceContextInit) -> Self {
        let instrumentation = init.instrumentation.clone();
        Self {
            resources: Some(FiniteSourceResources::new(init)),
            instrumentation,
        }
    }

    pub(crate) fn resources_mut(
        &mut self,
    ) -> Result<&mut FiniteSourceResources<H>, obzenflow_fsm::FsmError> {
        self.resources.as_mut().ok_or_else(|| {
            obzenflow_fsm::FsmError::HandlerError("source operation owns resources".into())
        })
    }
}

impl<H: Send + Sync + 'static> FsmContext for FiniteSourceContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

// Manual implementations for FiniteSourceAction
impl<H> Clone for FiniteSourceAction<H> {
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
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for FiniteSourceAction<H> {
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
impl<H: Send + Sync + 'static> FsmAction for FiniteSourceAction<H> {
    type Context = FiniteSourceContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        self.execute_resources(ctx.resources_mut()?).await
    }
}

impl<H: Send + Sync + 'static> FiniteSourceAction<H> {
    pub(crate) async fn execute_resources(
        &self,
        ctx: &mut FiniteSourceResources<H>,
    ) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            FiniteSourceAction::Host(_) => Err(obzenflow_fsm::FsmError::HandlerError(
                "host action requires the supervised runner".into(),
            )),

            FiniteSourceAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    writer_id = %writer_id,
                    "Finite source allocated resources and registered writer"
                );
                Ok(())
            }

            FiniteSourceAction::SendEOF => {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to send EOF".to_string(),
                    )
                })?;

                // Snapshot how many events this source emitted
                let emitted = ctx
                    .instrumentation
                    .events_processed_total
                    .load(std::sync::atomic::Ordering::Relaxed);

                // FLOWIP-095k: replay exhaustion reproduces the archive's
                // recorded kind and never consults the live control strategy.
                let eof_kind = match ctx.completion_origin {
                    SourceCompletionOrigin::Live => {
                        // Consult the source control strategy (FLOWIP-081a / 051b).
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
                    SourceCompletionOrigin::ReplayExhausted { recorded_kind } => {
                        recorded_kind.unwrap_or(EofKind::Truncated)
                    }
                };

                // Take a final runtime snapshot for wide-event semantics
                let runtime_context = ctx.instrumentation.capture_accounting();
                let (authored_writer_seq, writer_seq_by_event_type, authored_last_event_id) =
                    ctx.instrumentation.authored_data_frontier();

                // Emit EOF with writer positions populated
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

                // Attach flow/runtime context for downstream consumers
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::FiniteSource,
                };
                eof_event = runtime_context.clone().attach_to(eof_event);

                // Publish the source's own production totals at its EOF frontier.
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
                    stage_type: StageType::FiniteSource,
                };
                final_event = runtime_context.attach_to(final_event);

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
                    "Finite source sent EOF and production_finalized"
                );
                Ok(())
            }

            FiniteSourceAction::SendError { message } => {
                crate::stages::common::supervision::lifecycle_actions::send_failure(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::FiniteSource,
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
                                stage_type: StageType::FiniteSource,
                            },
                        )
                    },
                );
                Ok(())
            }

            FiniteSourceAction::PublishRunning => {
                let Some(writer_id) = ctx.writer_id else {
                    tracing::warn!(
                        stage_name = %ctx.stage_name,
                        "No writer ID available to publish running event; skipping running event and source_contract"
                    );
                    return Ok(());
                };

                // Write running event to system journal
                crate::stages::common::supervision::lifecycle_actions::publish_running(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::FiniteSource,
                    ),
                )
                .await?;

                // Emit writer-side source contract with runtime defaults (expected_count unknown)
                let contract = ChainEventFactory::source_contract_event(
                    writer_id,
                    SourceContractEventParams {
                        expected_count: None, // unknown until 010 config plumbing
                        source_id: ctx.stage_id,
                        route: None, // not available here
                        journal_path: JournalPath(ctx.stage_id.to_string()),
                        journal_index: JournalIndex(0),
                        writer_seq: None,   // unknown at start
                        vector_clock: None, // not captured at start
                    },
                )
                .with_flow_context(make_flow_context(
                    &ctx.flow_name,
                    &ctx.flow_id.to_string(),
                    &ctx.stage_name,
                    ctx.stage_id,
                    StageType::FiniteSource,
                ));

                crate::supervised_base::publication::append(
                    &ctx.data_journal,
                    contract,
                    Default::default(),
                )
                .await
                .map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to append source_contract: {e}"
                    ))
                })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Finite source published running event and source_contract"
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
                                stage_type: StageType::FiniteSource,
                            },
                        )
                    },
                );
                Ok(())
            }

            FiniteSourceAction::WriteStageCompleted => {
                // Write completion with protected accounting from the stage owner.
                //
                // Zero-input stages need no journal lookup or optional final capture.
                crate::stages::common::supervision::lifecycle_actions::send_completion(
                    &ctx.data_journal,
                    crate::stages::common::supervision::flow_context_factory::make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        obzenflow_core::event::context::StageType::FiniteSource,
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
                                stage_type: StageType::FiniteSource,
                            },
                        )
                    },
                );
                Ok(())
            }

            FiniteSourceAction::Cleanup => {
                // Handler-specific cleanup would go here
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Finite source cleaned up resources"
                );
                Ok(())
            }

            FiniteSourceAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::message_bus::FsmMessageBus;
    use crate::metrics::instrumentation::StageInstrumentation;
    use async_trait::async_trait;
    use obzenflow_core::event::journal_event::JournalEvent;
    use obzenflow_core::event::journal_record::JournalRecord;
    use obzenflow_core::event::types::SeqNo;
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::reader::JournalReader;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::StageId as CoreStageId;
    use serde_json::json;
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex};

    use crate::stages::common::handlers::source::traits::FiniteSourceHandler as TestFiniteSourceHandler;
    use crate::stages::source::strategies::{
        CompletionContext, CompletionDecision, CompletionGate,
    };

    /// Test stand-in for a degraded-mode source strategy: emits a poison EOF
    /// when the shared flag is non-zero. The breaker-backed implementation
    /// lives in `obzenflow_adapters`; this FSM test only exercises the
    /// policy-neutral `CompletionDecision::PoisonEof` path.
    #[derive(Debug)]
    struct FlagPoisonStrategy {
        state: Arc<AtomicU8>,
    }

    impl CompletionGate for FlagPoisonStrategy {
        fn on_natural_completion(&self, _ctx: &mut CompletionContext) -> CompletionDecision {
            if self.state.load(std::sync::atomic::Ordering::SeqCst) != 0 {
                CompletionDecision::PoisonEof
            } else {
                CompletionDecision::DefaultEof
            }
        }

        fn on_begin_drain(&self, ctx: &mut CompletionContext) -> CompletionDecision {
            self.on_natural_completion(ctx)
        }
    }

    type AppendGate<T> = (
        Box<dyn Fn(&T) -> bool + Send + Sync>,
        Arc<tokio::sync::Notify>,
        Arc<tokio::sync::Notify>,
    );

    /// Minimal in-memory journal for tests
    pub(crate) struct TestJournal<T: JournalEvent> {
        id: JournalId,
        fail_appends: Arc<std::sync::atomic::AtomicBool>,
        append_gate: Mutex<Option<AppendGate<T>>>,
        owner: Option<JournalOwner>,
        events: Arc<Mutex<Vec<JournalRecord<T::Payload>>>>,
    }

    impl<T: JournalEvent> TestJournal<T> {
        pub(crate) fn block_next_append(
            &self,
        ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
            self.block_matching_append(|_| true)
        }

        pub(crate) fn block_matching_append(
            &self,
            matches: impl Fn(&T) -> bool + Send + Sync + 'static,
        ) -> (Arc<tokio::sync::Notify>, Arc<tokio::sync::Notify>) {
            let gate = (
                Arc::new(tokio::sync::Notify::new()),
                Arc::new(tokio::sync::Notify::new()),
            );
            *self.append_gate.lock().unwrap() =
                Some((Box::new(matches), gate.0.clone(), gate.1.clone()));
            gate
        }

        pub(crate) fn with_append_failure(
            mut self,
            fail: Arc<std::sync::atomic::AtomicBool>,
        ) -> Self {
            self.fail_appends = fail;
            self
        }

        pub(crate) fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                fail_appends: Arc::default(),
                append_gate: Mutex::new(None),
                owner: Some(owner),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    struct TestJournalReader<T: JournalEvent> {
        events: Vec<JournalRecord<T::Payload>>,
        pos: usize,
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> obzenflow_core::journal::JournalStorage<T> for TestJournal<T> {
        fn storage_id(&self) -> &JournalId {
            &self.id
        }

        fn storage_owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn storage_append(
            &self,
            event: T,
            mut options: obzenflow_core::journal::AppendOptions<T>,
        ) -> Result<JournalRecord<T::Payload>, JournalError> {
            let gate = {
                let mut guard = self.append_gate.lock().unwrap();
                if guard
                    .as_ref()
                    .is_some_and(|(matches, _, _)| matches(&event))
                {
                    guard.take()
                } else {
                    None
                }
            };
            if let Some((_, entered, release)) = gate {
                entered.notify_one();
                release.notified().await;
            }
            if self.fail_appends.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(JournalError::Full);
            }
            let event = options.capture.prepare(0, event);
            let mut guard = self.events.lock().unwrap();
            let env = crate::testing::causal_fixture::commit(self.id, event, &options, &guard)?;
            guard.push(env.clone());
            Ok(env)
        }

        async fn storage_read_all_unordered(
            &self,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(guard.clone())
        }

        async fn storage_read_event(
            &self,
            _event_id: &obzenflow_core::EventId,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            // Not needed for this test
            Ok(None)
        }

        async fn storage_reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(Box::new(TestJournalReader {
                events: guard.clone(),
                pos: position as usize,
            }))
        }

        async fn storage_read_last_n(
            &self,
            count: usize,
        ) -> Result<Vec<JournalRecord<T::Payload>>, JournalError> {
            let guard = self.events.lock().unwrap();
            let len = guard.len();
            let start = len.saturating_sub(count);
            // Return most recent first to match Journal contract.
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> obzenflow_core::journal::JournalStorageReader<T>
        for TestJournalReader<T>
    {
        async fn storage_next(
            &mut self,
        ) -> Result<Option<JournalRecord<T::Payload>>, JournalError> {
            if self.pos >= self.events.len() {
                Ok(None)
            } else {
                let env = self.events.get(self.pos).cloned();
                self.pos += 1;
                Ok(env)
            }
        }

        fn storage_position(&self) -> u64 {
            self.pos as u64
        }

        fn storage_is_at_end(&self) -> bool {
            self.pos >= self.events.len()
        }
    }

    #[derive(Clone, Debug)]
    struct DummySource;

    impl TestFiniteSourceHandler for DummySource {
        fn next(
            &mut self,
        ) -> Result<
            Option<Vec<ChainEvent>>,
            crate::stages::common::handlers::source::traits::SourceError,
        > {
            // This test source never emits data; it's only used to drive EOF behaviour
            // in combination with the control strategy and breaker state.
            Ok(None)
        }
    }

    #[async_trait]
    impl crate::stages::common::handlers::source::traits::AsyncFiniteSourceHandler for DummySource {
        async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, crate::stages::SourceError> {
            Ok(None)
        }
    }

    impl crate::stages::common::handlers::source::traits::InfiniteSourceHandler for DummySource {
        fn next(&mut self) -> Result<Vec<ChainEvent>, crate::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl crate::stages::common::handlers::source::traits::AsyncInfiniteSourceHandler for DummySource {
        async fn next(&mut self) -> Result<Vec<ChainEvent>, crate::stages::SourceError> {
            Ok(Vec::new())
        }
    }

    // Exercise both physical supervisors for each source family using the same
    // existing journal fixture. FSM acceptance is checked before any action runs.
    macro_rules! source_state_projection_test {
        ($test:ident, $context:ident, $init:ident, $state:ident, $event:ident,
         $sync:ident, $async:ident, $finite:literal, {$($extra:ident: $value:expr),* $(,)?}) => {
            #[tokio::test]
            async fn $test() {
                use crate::supervised_base::base::Supervisor;
                use crate::supervised_base::{
                    ChannelBuilder, idle_backoff::IdleBackoff, HandlerSupervised,
                };
                use std::time::{Duration, Instant};

                for asynchronous in [false, true] {
                    let stage_id = CoreStageId::new();
                    let data_journal: Arc<dyn Journal<ChainEvent>> =
                        Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));
                    let build_async = |external_events, state_watcher| $async {
                        name: "source_projection".into(),
                        handler: Some(DummySource),
                        data_journal: data_journal.clone(),
                        flow_context: FlowContext::new("source_projection", stage_id),
                        stage_id,
                        idle_backoff: IdleBackoff::exponential_with_cap(
                            Duration::from_millis(1), Duration::from_millis(10)),
                        pending_idle_delay: None,
                        external_events,
                        state_watcher,
                        last_state: None,
                        replay_driver: None,
                        replay_started_at: None,
                        replay_completion: Default::default(),
                        source_boundary: None,
                        pending_boundary_error: None,
                        reader_acquired: false,
                        $($extra: $value,)*
                    };
                    let build_sync = || $sync {
                        name: "source_projection".into(),
                        handler: Some(DummySource),
                        data_journal: data_journal.clone(),
                        flow_context: FlowContext::new("source_projection", stage_id),
                        stage_id,
                        idle_backoff: IdleBackoff::exponential_with_cap(
                            Duration::from_millis(1), Duration::from_millis(10)),
                        pending_idle_delay: None,
                        replay_driver: None,
                        replay_started_at: None,
                        replay_completion: Default::default(),
                        source_boundary: None,
                        pending_boundary_error: None,
                        $($extra: $value,)*
                    };
                    let build_fsm = |initial_state| {
                        if asynchronous {
                            let (_, external_events, state_watcher) =
                                ChannelBuilder::new().build($state::<DummySource>::Created);
                            build_async(external_events.into(), state_watcher).build_state_machine(initial_state)
                        } else {
                            build_sync().build_state_machine(initial_state)
                        }
                    };
                    let mut ctx = $context::<DummySource>::new($init {
                        stage_id,
                        stage_name: "source_projection".into(),
                        observers: Default::default(),
                        flow_name: "projection_flow".into(),
                        flow_id: FlowId::new(),
                        data_journal: Arc::new(TestJournal::new(JournalOwner::stage(stage_id))),
                        error_journal: Arc::new(TestJournal::new(JournalOwner::stage(stage_id))),
                        runtime_execution: crate::execution::RuntimeExecution::new(
                            crate::execution::RuntimeMode::Live, None),
                        bus: Arc::new(FsmMessageBus::new()),
                        instrumentation: Arc::new(StageInstrumentation::new()),
                        control_strategy: Arc::new(crate::stages::source::strategies::JonestownSourceStrategy),
                        backpressure_writer: crate::backpressure::BackpressureWriter::disabled(),
                        output_contract: StageOutputContract::empty(),
                    });
                    ctx.instrumentation.bind_observations(ctx.resources.as_ref().unwrap().flow_id, WriterId::from(stage_id), &ctx.resources.as_ref().unwrap().runtime_execution);
                    assert_eq!(*ctx.instrumentation.current_state.read().unwrap(), "Created");
                    let mut fsm = build_fsm($state::Created);
                    for (event, destination) in [
                        ($event::Initialize, "Initializing"),
                        ($event::InitializationCompleted, "Initialized"),
                        ($event::Ready, "WaitingForGun"),
                        ($event::Start, "AcquiringInput"),
                        ($event::InputAcquired, "Starting"),
                        ($event::ActivationCompleted, "Running"),
                        ($event::BeginDrain, "Draining"),
                        ($event::Completed, "Finalising"),
                        ($event::FinalisationCompleted, "Drained"),
                    ] {
                        let old_entry = Instant::now() - Duration::from_secs(60);
                        *ctx.instrumentation.state_entered_at.write().unwrap() = old_entry;
                        let actions = fsm.handle(event, &mut ctx).await.unwrap();
                        if asynchronous {
                            let (_, receiver, watcher) = ChannelBuilder::new().build(fsm.state().clone());
                            build_async(receiver.into(), watcher).after_transition(fsm.state(), &ctx);
                        } else {
                            build_sync().after_transition(fsm.state(), &ctx);
                        }
                        assert_eq!(fsm.state().variant_name(), destination);
                        assert_eq!(*ctx.instrumentation.current_state.read().unwrap(), destination);
                        assert!(*ctx.instrumentation.state_entered_at.read().unwrap() > old_entry);
                        for action in actions {
                            if build_sync().supervisor_action(&action).is_none() {
                                action.execute(&mut ctx).await.unwrap();
                            }
                        }
                    }
                    let events = ctx.resources_mut().unwrap().data_journal.read_causally_ordered().await.unwrap();
                    assert_eq!(events.iter().filter(|env| matches!(env.payload,
                        ChainPayload::FlowControl(FlowControlPayload::SourceContract { .. }))).count(),
                        usize::from($finite));
                    let eof = events.iter().find(|env| env.is_eof()).expect("authored EOF");
                    assert_eq!(eof.envelope.observability.as_ref().unwrap().runtime_snapshot.as_ref().unwrap().fsm_state, "Finalising");
                    for env in &events {
                        if matches!(env.payload, ChainPayload::FlowControl(
                            FlowControlPayload::SourceContract { .. })) {
                            assert_eq!(env.envelope.provenance.event.writer_id, WriterId::from(stage_id));
                            assert_eq!(env.envelope.provenance.event.flow_context.stage_id, stage_id);
                            assert_eq!(env.envelope.provenance.event.flow_context.stage_name, ctx.resources_mut().unwrap().stage_name);
                            assert_eq!(env.envelope.provenance.event.flow_context.stage_type, StageType::FiniteSource);
                            assert_eq!(env.envelope.provenance.event.flow_context.flow_name, ctx.resources_mut().unwrap().flow_name);
                            assert_eq!(env.envelope.provenance.event.flow_context.flow_id, ctx.resources_mut().unwrap().flow_id.to_string());
                        }
                    }

                    // Failure is visible in the pending state before settlement actions.
                    for initial in [$state::Created, $state::Initializing, $state::Initialized,
                        $state::WaitingForGun, $state::AcquiringInput, $state::Starting,
                        $state::Running, $state::Draining, $state::Finalising,
                        $state::Failing("first".into())] {
                        ctx.instrumentation.transition_to_state(initial.variant_name());
                        let old_entry = Instant::now() - Duration::from_secs(60);
                        *ctx.instrumentation.state_entered_at.write().unwrap() = old_entry;
                        let repeated = matches!(initial, $state::Failing(_));
                        let mut fsm = build_fsm(initial);
                        let actions = fsm.handle($event::Error("failure".into()), &mut ctx).await.unwrap();
                        if asynchronous {
                            let (_, receiver, watcher) = ChannelBuilder::new().build(fsm.state().clone());
                            build_async(receiver.into(), watcher).after_transition(fsm.state(), &ctx);
                        } else {
                            build_sync().after_transition(fsm.state(), &ctx);
                        }
                        let expected_reason = if repeated { "first" } else { "failure" };
                        assert_eq!(fsm.state(), &$state::Failing(expected_reason.into()));
                        assert_eq!(actions.is_empty(), repeated);
                        assert_eq!(*ctx.instrumentation.current_state.read().unwrap(), "Failing");
                        assert_eq!(*ctx.instrumentation.state_entered_at.read().unwrap() == old_entry, repeated);
                    }
                    if $finite {
                        ctx.instrumentation.transition_to_state("Running");
                        let mut fsm = build_fsm($state::Running);
                        let actions = fsm.handle($event::Completed, &mut ctx).await.unwrap();
                        build_sync().after_transition(fsm.state(), &ctx);
                        assert_eq!(*ctx.instrumentation.current_state.read().unwrap(), "Draining");
                        assert!(actions.is_empty());
                    }
                }
            }
        };
    }

    use crate::stages::source::finite::{
        async_supervisor::AsyncFiniteSourceSupervisor, supervisor::FiniteSourceSupervisor,
    };
    use crate::stages::source::infinite::{
        async_supervisor::AsyncInfiniteSourceSupervisor,
        fsm::{
            InfiniteSourceContext, InfiniteSourceContextInit, InfiniteSourceEvent,
            InfiniteSourceState,
        },
        supervisor::InfiniteSourceSupervisor,
    };
    use obzenflow_fsm::StateVariant;

    source_state_projection_test!(finite_source_states_precede_actions,
        FiniteSourceContext, FiniteSourceContextInit, FiniteSourceState, FiniteSourceEvent,
        FiniteSourceSupervisor, AsyncFiniteSourceSupervisor, true,
        { pending_boundary_eof: false, pending_boundary_rejected: false });
    source_state_projection_test!(infinite_source_states_precede_actions,
        InfiniteSourceContext, InfiniteSourceContextInit, InfiniteSourceState, InfiniteSourceEvent,
        InfiniteSourceSupervisor, AsyncInfiniteSourceSupervisor, false,
        { pending_boundary_begin_drain: false });

    #[tokio::test]
    async fn send_eof_uses_poison_flag_when_breaker_open() {
        // Shared setup
        let stage_id = CoreStageId::new();
        let flow_id = FlowId::new();
        let flow_name = "test_flow".to_string();
        let stage_name = "finite_source".to_string();

        let data_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));
        let error_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));

        let bus = Arc::new(FsmMessageBus::new());
        // Helper to build a fresh context with a given control strategy
        let build_ctx = |control_strategy: Arc<dyn CompletionGate>| {
            let instrumentation = Arc::new(StageInstrumentation::new());
            FiniteSourceContext::<DummySource>::new(FiniteSourceContextInit {
                stage_id,
                stage_name: stage_name.clone(),
                observers: crate::stages::observer::StageObserverBundle::default(),
                flow_name: flow_name.clone(),
                flow_id,
                data_journal: data_journal.clone(),
                error_journal: error_journal.clone(),
                runtime_execution: crate::execution::RuntimeExecution::new(
                    crate::execution::RuntimeMode::Live,
                    None,
                ),
                bus: bus.clone(),
                instrumentation: instrumentation.clone(),
                control_strategy,
                backpressure_writer: crate::backpressure::BackpressureWriter::disabled(),
                output_contract: StageOutputContract::empty(),
            })
        };

        // Case 1: breaker closed -> natural EOF
        let state_closed = Arc::new(AtomicU8::new(0)); // Closed
        let mut ctx = build_ctx(Arc::new(FlagPoisonStrategy {
            state: state_closed.clone(),
        }));

        // Allocate resources to set writer_id
        FiniteSourceAction::<DummySource>::AllocateResources
            .execute(&mut ctx)
            .await
            .unwrap();

        // Pretend we emitted some data events
        ctx.instrumentation
            .events_processed_total
            .store(3, std::sync::atomic::Ordering::Relaxed);
        let source_writer_id = ctx
            .resources_mut()
            .unwrap()
            .writer_id
            .expect("source should have a writer id");
        ctx.instrumentation
            .record_output_event(&ChainEventFactory::data_event(
                source_writer_id,
                "test.a",
                std::num::NonZeroU32::MIN,
                json!({}),
            ));
        ctx.instrumentation
            .record_output_event(&ChainEventFactory::data_event(
                source_writer_id,
                "test.a",
                std::num::NonZeroU32::MIN,
                json!({}),
            ));
        ctx.instrumentation
            .record_output_event(&ChainEventFactory::data_event(
                source_writer_id,
                "test.b",
                std::num::NonZeroU32::MIN,
                json!({}),
            ));

        // Send EOF with breaker closed
        FiniteSourceAction::<DummySource>::SendEOF
            .execute(&mut ctx)
            .await
            .unwrap();

        let events_closed = data_journal.read_causally_ordered().await.unwrap();
        let eof_natural_closed = events_closed.iter().any(|env| {
            matches!(
                env.payload,
                ChainPayload::FlowControl(FlowControlPayload::Eof { kind, .. })
                    if kind.is_natural()
            )
        });
        assert!(
            eof_natural_closed,
            "Expected natural EOF when breaker is closed"
        );
        let eof_writer_seq_by_event_type = events_closed
            .iter()
            .find_map(|env| match &env.payload {
                ChainPayload::FlowControl(FlowControlPayload::Eof {
                    writer_seq_by_event_type,
                    ..
                }) => Some(writer_seq_by_event_type),
                _ => None,
            })
            .expect("expected EOF writer seq map");
        assert_eq!(
            eof_writer_seq_by_event_type.get(&crate::testing::causal_fixture::fact_descriptor(
                "test.a", 1
            )),
            Some(&SeqNo(2))
        );
        assert_eq!(
            eof_writer_seq_by_event_type.get(&crate::testing::causal_fixture::fact_descriptor(
                "test.b", 1
            )),
            Some(&SeqNo(1))
        );

        // Case 2: breaker open -> poison EOF
        let state_open = Arc::new(AtomicU8::new(1)); // Open
        let mut ctx_open = build_ctx(Arc::new(FlagPoisonStrategy {
            state: state_open.clone(),
        }));

        FiniteSourceAction::<DummySource>::AllocateResources
            .execute(&mut ctx_open)
            .await
            .unwrap();

        ctx_open
            .instrumentation
            .events_processed_total
            .store(10, std::sync::atomic::Ordering::Relaxed);

        FiniteSourceAction::<DummySource>::SendEOF
            .execute(&mut ctx_open)
            .await
            .unwrap();

        let events_open = data_journal.read_causally_ordered().await.unwrap();
        let eof_poison = events_open.iter().any(|env| {
            matches!(
                env.payload,
                ChainPayload::FlowControl(FlowControlPayload::Eof { kind, .. })
                    if kind.is_poison()
            )
        });
        assert!(
            eof_poison,
            "Expected poison EOF (natural = false) when breaker is open"
        );
    }

    #[tokio::test]
    async fn send_eof_replay_exhaustion_reproduces_recorded_kind_without_strategy_consult() {
        // FLOWIP-095k: the strategy flag is held open (PoisonEof) throughout;
        // a ReplayExhausted origin must ignore it and reproduce the recorded
        // kind, or Truncated when the archive committed no EOF.
        let cases = [
            (Some(EofKind::Natural), EofKind::Natural),
            (Some(EofKind::Poison), EofKind::Poison),
            (None, EofKind::Truncated),
        ];

        for (recorded_kind, expected) in cases {
            let stage_id = CoreStageId::new();
            let data_journal: Arc<dyn Journal<ChainEvent>> =
                Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));
            let mut ctx = FiniteSourceContext::<DummySource>::new(FiniteSourceContextInit {
                stage_id,
                stage_name: "finite_source".to_string(),
                observers: crate::stages::observer::StageObserverBundle::default(),
                flow_name: "test_flow".to_string(),
                flow_id: FlowId::new(),
                data_journal: data_journal.clone(),
                error_journal: Arc::new(TestJournal::new(JournalOwner::stage(stage_id))),
                runtime_execution: crate::execution::RuntimeExecution::new(
                    crate::execution::RuntimeMode::Live,
                    None,
                ),
                bus: Arc::new(FsmMessageBus::new()),
                instrumentation: Arc::new(StageInstrumentation::new()),
                control_strategy: Arc::new(FlagPoisonStrategy {
                    state: Arc::new(AtomicU8::new(1)), // open: would poison if consulted
                }),
                backpressure_writer: crate::backpressure::BackpressureWriter::disabled(),
                output_contract: StageOutputContract::empty(),
            });

            FiniteSourceAction::<DummySource>::AllocateResources
                .execute(&mut ctx)
                .await
                .unwrap();
            ctx.resources_mut().unwrap().completion_origin =
                SourceCompletionOrigin::ReplayExhausted { recorded_kind };

            FiniteSourceAction::<DummySource>::SendEOF
                .execute(&mut ctx)
                .await
                .unwrap();

            let kinds: Vec<EofKind> = data_journal
                .read_causally_ordered()
                .await
                .unwrap()
                .iter()
                .filter_map(|env| match &env.payload {
                    ChainPayload::FlowControl(FlowControlPayload::Eof { kind, .. }) => Some(*kind),
                    _ => None,
                })
                .collect();
            assert_eq!(
                kinds,
                vec![expected],
                "recorded {recorded_kind:?} must synthesize {expected:?}"
            );
        }
    }
}
