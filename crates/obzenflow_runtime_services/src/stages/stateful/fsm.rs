//! Stateful stage FSM types and state machine definition
//!
//! Stateful stages maintain state across events, enabling aggregations,
//! windowing operations, and session tracking.

use obzenflow_fsm::{StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{ChainEvent, EventId, WriterId, FlowId};
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::StageId;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::marker::PhantomData;
use tokio::sync::RwLock;

use crate::stages::common::handlers::StatefulHandler;
use crate::stages::common::control_strategies::ControlEventStrategy;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::messaging::UpstreamSubscription;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for stateful stages
#[derive(Serialize, Deserialize)]
pub enum StatefulState<H> {
    /// Initial state - stateful stage has been created but not initialized
    Created,

    /// Resources allocated, ready to start processing
    Initialized,

    /// Actively accumulating state from upstream events
    Accumulating,

    /// Emitting accumulated results (optional state for future emission strategies)
    Emitting,

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
            Self::Initialized => Self::Initialized,
            Self::Accumulating => Self::Accumulating,
            Self::Emitting => Self::Emitting,
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
            Self::Initialized => write!(f, "Initialized"),
            Self::Accumulating => write!(f, "Accumulating"),
            Self::Emitting => write!(f, "Emitting"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync> PartialEq for StatefulState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (StatefulState::Created, StatefulState::Created) => true,
            (StatefulState::Initialized, StatefulState::Initialized) => true,
            (StatefulState::Accumulating, StatefulState::Accumulating) => true,
            (StatefulState::Emitting, StatefulState::Emitting) => true,
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
            StatefulState::Initialized => "Initialized",
            StatefulState::Accumulating => "Accumulating",
            StatefulState::Emitting => "Emitting",
            StatefulState::Draining => "Draining",
            StatefulState::Drained => "Drained",
            StatefulState::Failed(_) => "Failed",
            StatefulState::_Phantom(_) => unreachable!("PhantomData variant"),
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
            Self::Ready => write!(f, "Ready"),
            Self::ReceivedData => write!(f, "ReceivedData"),
            Self::ShouldEmit => write!(f, "ShouldEmit"),
            Self::EmitComplete => write!(f, "EmitComplete"),
            Self::ReceivedEOF => write!(f, "ReceivedEOF"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::DrainComplete => write!(f, "DrainComplete"),
            Self::Error(msg) => write!(f, "Error({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> EventVariant for StatefulEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            StatefulEvent::Initialize => "Initialize",
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

    /// Clean up all resources
    Cleanup,

    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for StatefulAction
impl<H> Clone for StatefulAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::AllocateResources => Self::AllocateResources,
            Self::InitializeState => Self::InitializeState,
            Self::PublishRunning => Self::PublishRunning,
            Self::AccumulateEvent => Self::AccumulateEvent,
            Self::EmitResults => Self::EmitResults,
            Self::ForwardEOF => Self::ForwardEOF,
            Self::SendCompletion => Self::SendCompletion,
            Self::Cleanup => Self::Cleanup,
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for StatefulAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::InitializeState => write!(f, "InitializeState"),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::AccumulateEvent => write!(f, "AccumulateEvent"),
            Self::EmitResults => write!(f, "EmitResults"),
            Self::ForwardEOF => write!(f, "ForwardEOF"),
            Self::SendCompletion => write!(f, "SendCompletion"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

// ============================================================================
// FSM Context
// ============================================================================

/// Context for stateful handlers - contains everything actions need
#[derive(Clone)]
pub struct StatefulContext<H: StatefulHandler> {
    /// The handler instance (immutable, so wrapped in Arc)
    pub handler: Arc<H>,

    /// This stateful stage's stage ID
    pub stage_id: obzenflow_core::StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Current accumulated state (NEW: managed by supervisor)
    pub current_state: Arc<RwLock<H::State>>,

    /// Data journal for writing chain events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Upstream journals for reading events
    pub upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this stateful stage (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,

    /// Subscription to upstream events
    pub subscription: Arc<RwLock<Option<UpstreamSubscription<ChainEvent>>>>,

    /// Processing task handle
    pub processing_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,

    /// Upstream stage IDs
    pub upstream_stages: Vec<obzenflow_core::StageId>,

    /// Control event handling strategy
    pub control_strategy: Arc<dyn ControlEventStrategy>,

    /// EOF event to forward when draining completes
    pub buffered_eof: Arc<RwLock<Option<ChainEvent>>>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,
}

impl<H: StatefulHandler> StatefulContext<H> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handler: H,
        stage_id: obzenflow_core::StageId,
        stage_name: String,
        flow_name: String,
        flow_id: FlowId,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        upstream_stages: Vec<StageId>,
        control_strategy: Arc<dyn ControlEventStrategy>,
        instrumentation: Arc<StageInstrumentation>,
    ) -> Self {
        let initial_state = handler.initial_state();
        Self {
            handler: Arc::new(handler),
            stage_id,
            stage_name,
            flow_name,
            flow_id,
            current_state: Arc::new(RwLock::new(initial_state)),
            data_journal,
            error_journal,
            system_journal,
            upstream_journals,
            bus,
            writer_id: Arc::new(RwLock::new(None)),
            subscription: Arc::new(RwLock::new(None)),
            processing_task: Arc::new(RwLock::new(None)),
            upstream_stages,
            control_strategy,
            buffered_eof: Arc::new(RwLock::new(None)),
            instrumentation,
        }
    }
}

impl<H: StatefulHandler + 'static> FsmContext for StatefulContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: StatefulHandler + Send + Sync + 'static> FsmAction for StatefulAction<H> {
    type Context = StatefulContext<H>;

    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            StatefulAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id.clone());
                *ctx.writer_id.write().await = Some(writer_id);

                // Create subscription to upstream journals
                let subscription = UpstreamSubscription::new(&ctx.upstream_journals).await
                    .map_err(|e| format!("Failed to create subscription: {:?}", e))?;

                *ctx.subscription.write().await = Some(subscription);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage allocated resources"
                );
                Ok(())
            }

            StatefulAction::InitializeState => {
                // State already initialized in StatefulContext::new()
                // This action is a placeholder for future checkpoint/resume functionality
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage initialized state"
                );
                Ok(())
            }

            StatefulAction::PublishRunning => {
                let writer_id_guard = ctx.writer_id.read().await;
                let _writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to publish running event".to_string())?;

                // Write lifecycle event to system journal
                let running_event = SystemEvent::stage_running(
                    ctx.stage_id
                );

                ctx.system_journal
                    .append(running_event, None)
                    .await
                    .map_err(|e| format!("Failed to publish running event: {}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage published running event"
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
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to forward EOF".to_string())?;

                // Get the buffered EOF event or create a new one
                let eof_event = if let Some(buffered) = ctx.buffered_eof.write().await.take() {
                    buffered
                } else {
                    ChainEventFactory::eof_event(
                        writer_id.clone(),
                        true, // natural EOF
                    )
                };

                ctx.data_journal
                    .append(eof_event, None)
                    .await
                    .map_err(|e| format!("Failed to forward EOF: {}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage forwarded EOF downstream"
                );
                Ok(())
            }

            StatefulAction::SendCompletion => {
                let writer_id_guard = ctx.writer_id.read().await;
                let _writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send completion".to_string())?;

                // Write completion event to system journal
                let completion_event = SystemEvent::stage_completed(
                    ctx.stage_id
                );

                ctx.system_journal
                    .append(completion_event, None)
                    .await
                    .map_err(|e| format!("Failed to write completion event: {}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage sent completion event"
                );
                Ok(())
            }

            StatefulAction::Cleanup => {
                // Stop the processing task if any
                if let Some(task) = ctx.processing_task.write().await.take() {
                    task.abort();
                }

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage cleaned up resources"
                );
                Ok(())
            }

            StatefulAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}
