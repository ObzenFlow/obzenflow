//! Transform stage FSM types and state machine definition
//!
//! Transforms process events from upstream stages and emit transformed events.
//! They start processing immediately without waiting for a start signal.

use obzenflow_fsm::{StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{ChainEvent, EventId, WriterId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::marker::PhantomData;
use tokio::sync::RwLock;

use crate::stages::common::handlers::TransformHandler;
use crate::stages::common::control_strategies::ControlEventStrategy;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for transform stages
#[derive(Serialize, Deserialize)]
pub enum TransformState<H> {
    /// Initial state - transform has been created but not initialized
    Created,
    
    /// Resources allocated, ready to start processing
    Initialized,
    
    /// Actively processing events from upstream stages
    Running,
    
    /// Received EOF, finishing processing remaining events
    Draining,
    
    /// All events processed, EOF forwarded downstream
    Drained,
    
    /// Unrecoverable error occurred
    Failed(String),
    
    #[serde(skip)]
    _Phantom(PhantomData<H>),
}

// Manual implementations that don't require H to implement these traits
impl<H> Clone for TransformState<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Created => Self::Created,
            Self::Initialized => Self::Initialized,
            Self::Running => Self::Running,
            Self::Draining => Self::Draining,
            Self::Drained => Self::Drained,
            Self::Failed(msg) => Self::Failed(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for TransformState<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Initialized => write!(f, "Initialized"),
            Self::Running => write!(f, "Running"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync> PartialEq for TransformState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (TransformState::Created, TransformState::Created) => true,
            (TransformState::Initialized, TransformState::Initialized) => true,
            (TransformState::Running, TransformState::Running) => true,
            (TransformState::Draining, TransformState::Draining) => true,
            (TransformState::Drained, TransformState::Drained) => true,
            (TransformState::Failed(a), TransformState::Failed(b)) => a == b,
            _ => false,
        }
    }
}

impl<H: Send + Sync + 'static> StateVariant for TransformState<H> {
    fn variant_name(&self) -> &str {
        match self {
            TransformState::Created => "Created",
            TransformState::Initialized => "Initialized",
            TransformState::Running => "Running",
            TransformState::Draining => "Draining",
            TransformState::Drained => "Drained",
            TransformState::Failed(_) => "Failed",
            TransformState::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Events
// ============================================================================

/// Events that can trigger transform state transitions
pub enum TransformEvent<H> {
    /// Initialize the transform
    Initialize,
    
    /// Ready to start processing (transforms start immediately)
    Ready,
    
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

// Manual implementations for TransformEvent
impl<H> Clone for TransformEvent<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Initialize => Self::Initialize,
            Self::Ready => Self::Ready,
            Self::ReceivedEOF => Self::ReceivedEOF,
            Self::BeginDrain => Self::BeginDrain,
            Self::DrainComplete => Self::DrainComplete,
            Self::Error(msg) => Self::Error(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for TransformEvent<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialize => write!(f, "Initialize"),
            Self::Ready => write!(f, "Ready"),
            Self::ReceivedEOF => write!(f, "ReceivedEOF"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::DrainComplete => write!(f, "DrainComplete"),
            Self::Error(msg) => write!(f, "Error({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> EventVariant for TransformEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            TransformEvent::Initialize => "Initialize",
            TransformEvent::Ready => "Ready",
            TransformEvent::ReceivedEOF => "ReceivedEOF",
            TransformEvent::BeginDrain => "BeginDrain",
            TransformEvent::DrainComplete => "DrainComplete",
            TransformEvent::Error(_) => "Error",
            TransformEvent::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

/// Actions that transform FSM transitions can emit
pub enum TransformAction<H> {
    /// Allocate resources (writer ID, subscriptions)
    AllocateResources,
    
    /// Publish running event to journal
    PublishRunning,
    
    /// Forward EOF event downstream
    ForwardEOF,
    
    /// Send completion event to journal
    SendCompletion,
    
    /// Clean up all resources
    Cleanup,
    
    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for TransformAction
impl<H> Clone for TransformAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::AllocateResources => Self::AllocateResources,
            Self::PublishRunning => Self::PublishRunning,
            Self::ForwardEOF => Self::ForwardEOF,
            Self::SendCompletion => Self::SendCompletion,
            Self::Cleanup => Self::Cleanup,
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for TransformAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::PublishRunning => write!(f, "PublishRunning"),
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

/// Context for transform handlers - contains everything actions need
#[derive(Clone)]
pub struct TransformContext<H: TransformHandler> {
    /// The handler instance (stateless, so no RwLock needed)
    pub handler: Arc<H>,
    
    /// This transform's stage ID
    pub stage_id: obzenflow_topology_services::stages::StageId,
    
    /// Human-readable stage name for logging
    pub stage_name: String,
    
    /// Flow name for flow context
    pub flow_name: String,
    
    /// Journal for reading/writing events
    pub journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
    
    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,
    
    /// Writer ID for this transform (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
    
    /// Subscription to upstream events
    pub subscription: Arc<RwLock<Option<crate::messaging::reactive_journal::JournalSubscription>>>,
    
    /// Processing task handle (moved from supervisor to follow FSM patterns)
    pub processing_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    
    /// Upstream stage IDs
    pub upstream_stages: Vec<obzenflow_topology_services::stages::StageId>,
    
    /// Control event handling strategy
    pub control_strategy: Arc<dyn ControlEventStrategy>,
    
    /// EOF event to forward when draining completes
    pub buffered_eof: Arc<RwLock<Option<ChainEvent>>>,
}

impl<H: TransformHandler> TransformContext<H> {
    pub fn new(
        handler: H,
        stage_id: obzenflow_topology_services::stages::StageId,
        stage_name: String,
        flow_name: String,
        journal: Arc<crate::messaging::reactive_journal::ReactiveJournal>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        upstream_stages: Vec<obzenflow_topology_services::stages::StageId>,
        control_strategy: Arc<dyn ControlEventStrategy>,
    ) -> Self {
        Self {
            handler: Arc::new(handler),
            stage_id,
            stage_name,
            flow_name,
            journal,
            bus,
            writer_id: Arc::new(RwLock::new(None)),
            subscription: Arc::new(RwLock::new(None)),
            processing_task: Arc::new(RwLock::new(None)),
            upstream_stages,
            control_strategy,
            buffered_eof: Arc::new(RwLock::new(None)),
        }
    }
}

impl<H: TransformHandler + 'static> FsmContext for TransformContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: TransformHandler + Send + Sync + 'static> FsmAction for TransformAction<H> {
    type Context = TransformContext<H>;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            TransformAction::AllocateResources => {
                // Register writer ID with journal
                let writer_id = ctx.journal
                    .register_writer(ctx.stage_id, None)
                    .await
                    .map_err(|e| format!("Failed to register writer: {}", e))?;
                
                *ctx.writer_id.write().await = Some(writer_id);
                
                // Create subscription to upstreams
                if !ctx.upstream_stages.is_empty() {
                    let filter = crate::messaging::reactive_journal::SubscriptionFilter::UpstreamStages {
                        stages: ctx.upstream_stages.clone(),
                    };
                    
                    let subscription = ctx.journal.subscribe(filter).await
                        .map_err(|e| format!("Failed to create subscription: {:?}", e))?;
                    
                    *ctx.subscription.write().await = Some(subscription);
                }
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Transform allocated resources"
                );
                Ok(())
            }
            
            TransformAction::PublishRunning => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to publish running event".to_string())?;
                
                let running_event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_STAGE_RUNNING,
                    serde_json::json!({
                        "stage_id": ctx.stage_id.to_string(),
                        "stage_name": ctx.stage_name,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    })
                );
                
                ctx.journal
                    .write(writer_id, running_event, None)
                    .await
                    .map_err(|e| format!("Failed to publish running event: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Transform published running event"
                );
                Ok(())
            }
            
            TransformAction::ForwardEOF => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to forward EOF".to_string())?;
                
                // Get the buffered EOF event or create a new one
                let eof_event = if let Some(buffered) = ctx.buffered_eof.write().await.take() {
                    buffered
                } else {
                    ChainEvent::eof(
                        EventId::new(),
                        writer_id.clone(),
                        true, // natural EOF
                    )
                };
                
                ctx.journal
                    .write(writer_id, eof_event, None)
                    .await
                    .map_err(|e| format!("Failed to forward EOF: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Transform forwarded EOF downstream"
                );
                Ok(())
            }
            
            TransformAction::SendCompletion => {
                let writer_id_guard = ctx.writer_id.read().await;
                let writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send completion".to_string())?;
                
                let mut completion_event = ChainEvent::new(
                    EventId::new(),
                    writer_id.clone(),
                    ChainEvent::SYSTEM_STAGE_COMPLETED,
                    serde_json::json!({
                        "stage_id": ctx.stage_id.to_string(),
                        "stage_name": ctx.stage_name,
                        "timestamp": chrono::Utc::now().to_rfc3339(),
                    })
                );
                
                // Populate flow context for completion
                completion_event.flow_context = obzenflow_core::event::flow_context::FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: "default".to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_type: obzenflow_core::event::flow_context::StageType::Transform,
                };
                
                ctx.journal
                    .write(writer_id, completion_event, None)
                    .await
                    .map_err(|e| format!("Failed to write completion event: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Transform sent completion event"
                );
                Ok(())
            }
            
            TransformAction::Cleanup => {
                // Stop the processing task if any
                if let Some(task) = ctx.processing_task.write().await.take() {
                    task.abort();
                }
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Transform cleaned up resources"
                );
                Ok(())
            }
            
            TransformAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}