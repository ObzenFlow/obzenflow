//! Infinite source stage FSM types and state machine definition
//!
//! Infinite sources never complete naturally (Kafka, WebSocket, etc).
//! They have a unique "WaitingForGun" state that ensures they don't
//! start emitting events until the pipeline is ready.

use obzenflow_fsm::{StateMachine, StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{ChainEvent, EventId, WriterId, Journal};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::stages::common::handlers::InfiniteSourceHandler;

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
            Self::Failed(msg) => write!(f, "Failed({:?})", msg),
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
            Self::Error(msg) => write!(f, "Error({:?})", msg),
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
    
    /// Clean up all resources
    Cleanup,
    
    #[doc(hidden)]
    _Phantom(std::marker::PhantomData<H>),
}

// ============================================================================
// FSM Context
// ============================================================================

/// Context for infinite source handlers - contains everything actions need
#[derive(Clone)]
pub struct InfiniteSourceContext<H: InfiniteSourceHandler> {
    /// The handler instance that implements source logic
    pub handler: Arc<RwLock<H>>,
    
    /// This source's stage ID
    pub stage_id: obzenflow_core::StageId,
    
    /// Human-readable stage name for logging
    pub stage_name: String,
    
    /// Flow name for flow context
    pub flow_name: String,
    
    /// Journal for writing events
    pub journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
    
    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,
    
    /// Writer ID for this source (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
    
    /// Flag indicating if source can emit (set to true after Start event)
    pub can_emit: Arc<RwLock<bool>>,
    
    /// Flag to track if shutdown was requested
    pub shutdown_requested: Arc<RwLock<bool>>,
}

impl<H: InfiniteSourceHandler> InfiniteSourceContext<H> {
    pub fn new(
        handler: H,
        stage_id: obzenflow_core::StageId,
        stage_name: String,
        flow_name: String,
        journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
    ) -> Self {
        Self {
            handler: Arc::new(RwLock::new(handler)),
            stage_id,
            stage_name,
            flow_name,
            journal,
            bus,
            writer_id: Arc::new(RwLock::new(None)),
            can_emit: Arc::new(RwLock::new(false)),
            shutdown_requested: Arc::new(RwLock::new(false)),
        }
    }
}

impl<H: InfiniteSourceHandler + 'static> FsmContext for InfiniteSourceContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

// Manual implementations for InfiniteSourceAction
impl<H> Clone for InfiniteSourceAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::AllocateResources => Self::AllocateResources,
            Self::SendEOF => Self::SendEOF,
            Self::SendError { message } => Self::SendError { message: message.clone() },
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
            Self::SendError { message } => write!(f, "SendError({:?})", message),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

#[async_trait::async_trait]
impl<H: InfiniteSourceHandler + Send + Sync + 'static> FsmAction for InfiniteSourceAction<H> {
    type Context = InfiniteSourceContext<H>;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            InfiniteSourceAction::AllocateResources => {
                // In the new architecture, ReactiveJournal already has its writer_id
                // Just get it from the journal
                let writer_id = ctx.journal.writer_id.clone();
                *ctx.writer_id.write().await = Some(writer_id);
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    writer_id = %writer_id,
                    "Infinite source allocated resources and registered writer"
                );
                Ok(())
            }
            
            InfiniteSourceAction::SendEOF => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send EOF".to_string())?;
                
                let eof_event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    "control.eof",
                    serde_json::json!({
                        "stage_id": ctx.stage_id.to_string(),
                        "stage_name": ctx.stage_name,
                    }),
                );
                
                ctx.journal
                    .write(eof_event, None)
                    .await
                    .map_err(|e| format!("Failed to send EOF: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Infinite source sent EOF event"
                );
                Ok(())
            }
            
            InfiniteSourceAction::SendError { message } => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send error".to_string())?;
                
                let error_event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_ERROR,
                    serde_json::json!({
                        "stage_id": ctx.stage_id.to_string(),
                        "stage_name": ctx.stage_name,
                        "error": message,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    }),
                );
                
                ctx.journal
                    .write_control_event(error_event)
                    .await
                    .map_err(|e| format!("Failed to send error event: {}", e))?;
                
                tracing::error!(
                    stage_name = %ctx.stage_name,
                    error = %message,
                    "Infinite source encountered error"
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

