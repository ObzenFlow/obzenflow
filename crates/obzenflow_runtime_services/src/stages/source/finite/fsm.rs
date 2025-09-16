//! Finite source stage FSM types and state machine definition
//!
//! Finite sources eventually complete (files, bounded collections).
//! They have a unique "WaitingForGun" state that ensures they don't
//! start emitting events until the pipeline is ready.

use obzenflow_fsm::{StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{ChainEvent, EventId, WriterId, StageId, FlowId};
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::marker::PhantomData;
use tokio::sync::RwLock;

use crate::stages::common::handlers::FiniteSourceHandler;
use crate::metrics::instrumentation::StageInstrumentation;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for finite source stages
#[derive(Serialize, Deserialize)]
pub enum FiniteSourceState<H> {
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
    _Phantom(PhantomData<H>),
}

// Manual implementations that don't require H to implement these traits
impl<H> Clone for FiniteSourceState<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Created => Self::Created,
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
            Self::Initialized => write!(f, "Initialized"),
            Self::WaitingForGun => write!(f, "WaitingForGun"),
            Self::Running => write!(f, "Running"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync> PartialEq for FiniteSourceState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (FiniteSourceState::Created, FiniteSourceState::Created) => true,
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

// ============================================================================
// FSM Events
// ============================================================================

/// Events that can trigger finite source state transitions
pub enum FiniteSourceEvent<H> {
    /// Initialize the source - allocate resources, create writer ID
    Initialize,
    
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
            Self::Ready => write!(f, "Ready"),
            Self::Start => write!(f, "Start"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::Completed => write!(f, "Completed"),
            Self::Error(msg) => write!(f, "Error({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> EventVariant for FiniteSourceEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            FiniteSourceEvent::Initialize => "Initialize",
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
    /// Allocate resources needed by the source
    /// - Register writer ID with journal
    /// - Open file handles, network connections, etc.
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
    _Phantom(PhantomData<H>),
}

// ============================================================================
// FSM Context
// ============================================================================

/// Context for finite source handlers - contains everything actions need
#[derive(Clone)]
pub struct FiniteSourceContext<H: FiniteSourceHandler> {
    /// The handler instance that implements source logic
    pub handler: Arc<RwLock<H>>,
    
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
    
    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,
    
    /// Writer ID for this source (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
    
    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,
}

impl<H: FiniteSourceHandler> FiniteSourceContext<H> {
    pub fn new(
        handler: H,
        stage_id: obzenflow_core::StageId,
        stage_name: String,
        flow_name: String,
        flow_id: FlowId,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        instrumentation: Arc<StageInstrumentation>,
    ) -> Self {
        Self {
            handler: Arc::new(RwLock::new(handler)),
            stage_id,
            stage_name,
            flow_name,
            flow_id,
            data_journal,
            error_journal,
            system_journal,
            bus,
            writer_id: Arc::new(RwLock::new(None)),
            instrumentation,
        }
    }
}

impl<H: FiniteSourceHandler + 'static> FsmContext for FiniteSourceContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

// Manual implementations for FiniteSourceAction
impl<H> Clone for FiniteSourceAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::AllocateResources => Self::AllocateResources,
            Self::SendEOF => Self::SendEOF,
            Self::SendError { message } => Self::SendError { message: message.clone() },
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
            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::SendEOF => write!(f, "SendEOF"),
            Self::SendError { message } => write!(f, "SendError({:?})", message),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::WriteStageCompleted => write!(f, "WriteStageCompleted"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

#[async_trait::async_trait]
impl<H: FiniteSourceHandler + Send + Sync + 'static> FsmAction for FiniteSourceAction<H> {
    type Context = FiniteSourceContext<H>;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            FiniteSourceAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id.clone());
                *ctx.writer_id.write().await = Some(writer_id.clone());
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    writer_id = %writer_id,
                    "Finite source allocated resources and registered writer"
                );
                Ok(())
            }
            
            FiniteSourceAction::SendEOF => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send EOF".to_string())?;
                
                let eof_event = ChainEventFactory::eof_event(
                    writer_id.clone(),
                    true, // natural EOF for finite sources
                );
                
                ctx.data_journal
                    .append(eof_event, None)
                    .await
                    .map_err(|e| format!("Failed to send EOF: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Finite source sent EOF event"
                );
                Ok(())
            }
            
            FiniteSourceAction::SendError { message } => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id_guard = ctx.writer_id.read().await;
                let _writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send error".to_string())?;
                
                let error_event = SystemEvent::stage_failed(
                    ctx.stage_id,
                    message.clone(),
                    false // not recoverable
                );
                
                ctx.system_journal
                    .append(error_event, None)
                    .await
                    .map_err(|e| format!("Failed to send error event: {}", e))?;
                
                tracing::error!(
                    stage_name = %ctx.stage_name,
                    error = %message,
                    "Finite source encountered error"
                );
                Ok(())
            }

            FiniteSourceAction::PublishRunning => {
                let writer_id_guard = ctx.writer_id.read().await;
                let _writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to publish running event".to_string())?;

                // Write running event to system journal
                let running_event = SystemEvent::stage_running(ctx.stage_id);

                ctx.system_journal
                    .append(running_event, None)
                    .await
                    .map_err(|e| format!("Failed to publish running event: {}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Finite source published running event"
                );
                Ok(())
            }

            FiniteSourceAction::WriteStageCompleted => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id_guard = ctx.writer_id.read().await;
                let _writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send completion event".to_string())?;
                
                // Write completion event to system journal
                let completion_event = SystemEvent::stage_completed(
                    ctx.stage_id
                );
                
                ctx.system_journal
                    .append(completion_event, None)
                    .await
                    .map_err(|e| format!("Failed to send completion event: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Finite source sent completion event"
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

