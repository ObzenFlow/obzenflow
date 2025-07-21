//! Finite source stage FSM types and state machine definition
//!
//! Finite sources eventually complete (files, bounded collections).
//! They have a unique "WaitingForGun" state that ensures they don't
//! start emitting events until the pipeline is ready.

use obzenflow_fsm::{StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{ChainEvent, EventId, WriterId, Journal};
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
    
    /// Journal for writing events
    pub journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
    
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
        journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        instrumentation: Arc<StageInstrumentation>,
    ) -> Self {
        Self {
            handler: Arc::new(RwLock::new(handler)),
            stage_id,
            stage_name,
            flow_name,
            journal,
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
                // In the new architecture, ReactiveJournal already has its writer_id
                // Just get it from the journal
                let writer_id = ctx.journal.writer_id.clone();
                *ctx.writer_id.write().await = Some(writer_id);
                
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
                    "Finite source sent EOF event"
                );
                Ok(())
            }
            
            FiniteSourceAction::SendError { message } => {
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
                    "Finite source encountered error"
                );
                Ok(())
            }
            
            FiniteSourceAction::WriteStageCompleted => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send completion event".to_string())?;
                
                let mut completion_event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_STAGE_COMPLETED,
                    serde_json::json!({
                        "stage_id": ctx.stage_id.to_string(),
                        "stage_name": ctx.stage_name,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    }),
                );
                
                // Set flow context
                completion_event.flow_context = obzenflow_core::event::flow_context::FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: format!("{}-{}", ctx.flow_name, ctx.stage_id),
                    stage_name: ctx.stage_name.clone(),
                    stage_type: obzenflow_core::event::flow_context::StageType::Source,
                };
                
                ctx.journal
                    .write_control_event(completion_event)
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

