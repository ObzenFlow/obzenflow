//! Infinite source stage FSM types and state machine definition
//!
//! Infinite sources never complete naturally (Kafka, WebSocket, etc).
//! They have a unique "WaitingForGun" state that ensures they don't
//! start emitting events until the pipeline is ready.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::{Count, SeqNo};
use obzenflow_core::event::{
    ChainEventContent, ChainEventFactory, ConsumptionFinalEventParams, SystemEvent,
};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateMachine, StateVariant};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use crate::backpressure::BackpressureWriter;
use crate::metrics::instrumentation::{snapshot_stage_metrics, StageInstrumentation};
use crate::metrics::tail_read;
use crate::replay::ReplayArchive;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::stage_handle::{
    FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};
use crate::stages::source::strategies::{SourceControlContext, SourceControlStrategy};
use crate::supervised_base::idle_backoff::IdleBackoff;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for infinite source stages
#[derive(Serialize, Deserialize)]
pub enum InfiniteSourceState<H> {
    /// Initial state - source has been created but not initialized
    Created,

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

// ============================================================================
// FSM Events
// ============================================================================

/// Events that can trigger infinite source state transitions
pub enum InfiniteSourceEvent<H> {
    /// Initialize the source - allocate resources, create writer ID
    Initialize,

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
            Self::Ready => write!(f, "Ready"),
            Self::Start => write!(f, "Start"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::Completed => write!(f, "Completed"),
            Self::Error(msg) => write!(f, "Error({msg:?})"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> EventVariant for InfiniteSourceEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            InfiniteSourceEvent::Initialize => "Initialize",
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
    /// Allocate resources needed by the source
    /// - Register writer ID with journal
    /// - Open connections, etc.
    AllocateResources,

    /// Send EOF event downstream to signal completion
    SendEOF,

    /// Send error event to journal for diagnostics
    SendError { message: String },

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
    ArchiveExhausted,
}

/// Context for infinite source handlers - contains everything actions need
#[derive(Clone)]
pub struct InfiniteSourceContext<H> {
    /// This source's stage ID
    pub stage_id: obzenflow_core::StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Data journal for writing generated events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Optional replay archive injection (FLOWIP-095a).
    pub replay_archive: Option<Arc<dyn ReplayArchive>>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this source (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// Flag indicating if source can emit (set to true after Start event)
    pub can_emit: bool,

    /// Flag to track if shutdown was requested
    pub shutdown_requested: bool,

    /// Why the source is shutting down (affects EOF semantics).
    pub completion_reason: InfiniteSourceCompletionReason,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Source control strategy for this stage
    pub control_strategy: Arc<dyn SourceControlStrategy>,

    /// Mutable context for the source control strategy
    pub control_context: SourceControlContext,

    /// Backpressure writer handle for this stage's journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Pending stage outputs blocked on downstream credits (FLOWIP-086k).
    pub(crate) pending_outputs: VecDeque<ChainEvent>,

    /// Backpressure activity pulse accumulator (Hz UI animation driver).
    pub(crate) backpressure_pulse: BackpressureActivityPulse,

    /// Backoff for blocked output writes (1ms → … → 50ms cap).
    pub(crate) backpressure_backoff: IdleBackoff,

    /// Phantom to keep the handler type in the context's type parameters
    _marker: PhantomData<H>,
}

pub struct InfiniteSourceContextInit {
    pub stage_id: obzenflow_core::StageId,
    pub stage_name: String,
    pub flow_name: String,
    pub flow_id: FlowId,
    pub data_journal: Arc<dyn Journal<ChainEvent>>,
    pub error_journal: Arc<dyn Journal<ChainEvent>>,
    pub system_journal: Arc<dyn Journal<SystemEvent>>,
    pub replay_archive: Option<Arc<dyn ReplayArchive>>,
    pub bus: Arc<crate::message_bus::FsmMessageBus>,
    pub instrumentation: Arc<StageInstrumentation>,
    pub control_strategy: Arc<dyn SourceControlStrategy>,
    pub backpressure_writer: BackpressureWriter,
}

impl<H> InfiniteSourceContext<H> {
    pub fn new(init: InfiniteSourceContextInit) -> Self {
        Self {
            stage_id: init.stage_id,
            stage_name: init.stage_name,
            flow_name: init.flow_name,
            flow_id: init.flow_id,
            data_journal: init.data_journal,
            error_journal: init.error_journal,
            system_journal: init.system_journal,
            replay_archive: init.replay_archive,
            bus: init.bus,
            writer_id: None,
            can_emit: false,
            shutdown_requested: false,
            completion_reason: InfiniteSourceCompletionReason::ExternalDrain,
            instrumentation: init.instrumentation,
            control_strategy: init.control_strategy,
            control_context: SourceControlContext::new(),
            backpressure_writer: init.backpressure_writer,
            pending_outputs: VecDeque::new(),
            backpressure_pulse: BackpressureActivityPulse::new(),
            backpressure_backoff: IdleBackoff::exponential_with_cap(
                Duration::from_millis(1),
                Duration::from_millis(50),
            ),
            _marker: PhantomData,
        }
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
        match self {
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

                let decision = match ctx.completion_reason {
                    InfiniteSourceCompletionReason::ArchiveExhausted => ctx
                        .control_strategy
                        .on_natural_completion(&mut ctx.control_context),
                    InfiniteSourceCompletionReason::ExternalDrain => ctx
                        .control_strategy
                        .on_begin_drain(&mut ctx.control_context),
                };

                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to send EOF".to_string(),
                    )
                })?;

                let natural = match ctx.completion_reason {
                    InfiniteSourceCompletionReason::ExternalDrain => false,
                    InfiniteSourceCompletionReason::ArchiveExhausted => !matches!(
                        decision,
                        crate::stages::source::strategies::SourceShutdownDecision::PoisonEof
                    ),
                };

                // Take a final runtime snapshot for wide-event semantics
                let runtime_context = ctx.instrumentation.snapshot_with_control();

                let mut eof_event = ChainEventFactory::eof_event(writer_id, natural);
                if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
                    writer_id: writer_id_field,
                    writer_seq,
                    ..
                }) = &mut eof_event.content
                {
                    *writer_id_field = Some(writer_id);
                    *writer_seq = Some(SeqNo(emitted));
                }

                // Attach flow/runtime context so the final journal record is a wide snapshot
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::InfiniteSource,
                };
                eof_event.runtime_context = Some(runtime_context);

                ctx.data_journal
                    .append(eof_event, None)
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!("Failed to send EOF: {e}"))
                    })?;

                let mut final_event = ChainEventFactory::consumption_final_event(
                    writer_id,
                    ConsumptionFinalEventParams {
                        pass: true,
                        consumed_count: Count(emitted),
                        expected_count: None,
                        eof_seen: true,
                        last_event_id: None,
                        reader_seq: SeqNo(emitted),
                        advertised_writer_seq: Some(SeqNo(emitted)),
                        advertised_vector_clock: None,
                        failure_reason: None,
                    },
                );
                final_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::InfiniteSource,
                };
                final_event.runtime_context = Some(ctx.instrumentation.snapshot_with_control());

                ctx.data_journal
                    .append(final_event, None)
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to send source consumption_final: {e}"
                        ))
                    })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    emitted,
                    natural,
                    reason = ?ctx.completion_reason,
                    "Infinite source sent EOF and consumption_final"
                );
                Ok(())
            }

            InfiniteSourceAction::SendError { message } => {
                // Tail-read metrics for failure; if no runtime_context is present
                // in the journals, fall back to a best-effort snapshot from
                // instrumentation rather than failing the failure path.
                let metrics = match tail_read::read_stage_metrics_from_tail(
                    &ctx.data_journal,
                    Some(&ctx.error_journal),
                    ctx.stage_id,
                )
                .await
                {
                    Some(metrics) => metrics,
                    None => snapshot_stage_metrics(ctx.instrumentation.as_ref()),
                };
                let cancel_reason = match message.as_str() {
                    FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP => Some(STOP_REASON_USER_STOP),
                    STOP_REASON_TIMEOUT => Some(STOP_REASON_TIMEOUT),
                    _ => None,
                };

                let system_event = if let Some(reason) = cancel_reason {
                    obzenflow_core::event::SystemEvent::stage_cancelled_with_metrics(
                        ctx.stage_id,
                        reason.to_string(),
                        metrics,
                    )
                } else {
                    obzenflow_core::event::SystemEvent::stage_failed_with_metrics(
                        ctx.stage_id,
                        message.clone(),
                        false, // not recoverable
                        metrics,
                    )
                };

                match ctx.system_journal.append(system_event, None).await {
                    Ok(_) => {
                        if let Some(reason) = cancel_reason {
                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                reason = %reason,
                                "Infinite source cancelled"
                            );
                        } else {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = %message,
                                "Infinite source encountered error"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            stage_name = %ctx.stage_name,
                            error = %message,
                            journal_error = %e,
                            "Infinite source encountered error but failed to write error event"
                        );
                    }
                }
                Ok(())
            }

            InfiniteSourceAction::PublishRunning => {
                // Write running event to system journal
                let running_event = SystemEvent::stage_running(ctx.stage_id);

                if let Err(e) = ctx.system_journal.append(running_event, None).await {
                    tracing::error!(
                        stage_name = %ctx.stage_name,
                        journal_error = %e,
                        "Failed to publish running event; continuing without system journal entry"
                    );
                }

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Infinite source published running event"
                );
                Ok(())
            }

            InfiniteSourceAction::WriteStageCompleted => {
                // Write completion event to system journal with tail-read metrics.
                //
                // Some stages may legitimately complete without emitting any runtime-context
                // bearing events (e.g. zero input). In that case, fall back to a best-effort
                // snapshot from instrumentation instead of failing completion.
                let metrics = match tail_read::read_stage_metrics_from_tail(
                    &ctx.data_journal,
                    Some(&ctx.error_journal),
                    ctx.stage_id,
                )
                .await
                {
                    Some(metrics) => metrics,
                    None => snapshot_stage_metrics(ctx.instrumentation.as_ref()),
                };
                let completion_event =
                    SystemEvent::stage_completed_with_metrics(ctx.stage_id, metrics);

                if let Err(e) = ctx.system_journal.append(completion_event, None).await {
                    tracing::error!(
                        stage_name = %ctx.stage_name,
                        journal_error = %e,
                        "Failed to write completion event; continuing without system journal entry"
                    );
                }

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Infinite source sent completion event"
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
