//! Sink stage FSM types and state machine definition
//!
//! Sinks consume events and write to external destinations.
//! They have a unique "Flushing" state that ensures all buffered
//! data is written before shutdown.

use obzenflow_fsm::{StateVariant, EventVariant, FsmContext, FsmAction};
use obzenflow_core::{ChainEvent, EventId, WriterId, StageId, FlowId};
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::marker::PhantomData;
use futures::TryFutureExt;
use tokio::sync::RwLock;
use obzenflow_core::event::context::{FlowContext, StageType};
use crate::stages::common::handlers::SinkHandler;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::messaging::UpstreamSubscription;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for sink stages
#[derive(Serialize, Deserialize)]
pub enum SinkState<H> {
    /// Initial state - sink has been created but not initialized
    Created,
    
    /// Resources allocated (DB connections, file handles, etc.)
    Initialized,
    
    /// Actively consuming events and writing to destination
    Running,
    
    /// UNIQUE TO SINKS: Flushing any buffered data before drain
    /// This ensures no data loss during shutdown
    Flushing,
    
    /// Flushing complete, waiting for remaining events
    Draining,
    
    /// All events consumed, resources cleaned up
    Drained,
    
    /// Unrecoverable error occurred
    Failed(String),
    
    #[serde(skip)]
    _Phantom(PhantomData<H>),
}

// Manual implementations that don't require H to implement these traits
impl<H> Clone for SinkState<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Created => Self::Created,
            Self::Initialized => Self::Initialized,
            Self::Running => Self::Running,
            Self::Flushing => Self::Flushing,
            Self::Draining => Self::Draining,
            Self::Drained => Self::Drained,
            Self::Failed(msg) => Self::Failed(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for SinkState<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Initialized => write!(f, "Initialized"),
            Self::Running => write!(f, "Running"),
            Self::Flushing => write!(f, "Flushing"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync> PartialEq for SinkState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (SinkState::Created, SinkState::Created) => true,
            (SinkState::Initialized, SinkState::Initialized) => true,
            (SinkState::Running, SinkState::Running) => true,
            (SinkState::Flushing, SinkState::Flushing) => true,
            (SinkState::Draining, SinkState::Draining) => true,
            (SinkState::Drained, SinkState::Drained) => true,
            (SinkState::Failed(a), SinkState::Failed(b)) => a == b,
            _ => false,
        }
    }
}

impl<H: Send + Sync + 'static> StateVariant for SinkState<H> {
    fn variant_name(&self) -> &str {
        match self {
            SinkState::Created => "Created",
            SinkState::Initialized => "Initialized",
            SinkState::Running => "Running",
            SinkState::Flushing => "Flushing",  // Unique to sinks!
            SinkState::Draining => "Draining",
            SinkState::Drained => "Drained",
            SinkState::Failed(_) => "Failed",
            SinkState::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Events
// ============================================================================

/// Events that can trigger sink state transitions
pub enum SinkEvent<H> {
    /// Initialize the sink - open connections, create output files, etc.
    Initialize,
    
    /// Ready to consume events
    Ready,
    
    /// Received EOF from all upstream stages
    ReceivedEOF,
    
    /// Begin flush operation - write any buffered data
    /// UNIQUE TO SINKS: Ensures no data loss
    BeginFlush,
    
    /// Flush operation completed successfully
    FlushComplete,
    
    /// Begin graceful shutdown (after flush)
    BeginDrain,
    
    /// Unrecoverable error occurred
    Error(String),
    
    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for SinkEvent
impl<H> Clone for SinkEvent<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Initialize => Self::Initialize,
            Self::Ready => Self::Ready,
            Self::ReceivedEOF => Self::ReceivedEOF,
            Self::BeginFlush => Self::BeginFlush,
            Self::FlushComplete => Self::FlushComplete,
            Self::BeginDrain => Self::BeginDrain,
            Self::Error(msg) => Self::Error(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for SinkEvent<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialize => write!(f, "Initialize"),
            Self::Ready => write!(f, "Ready"),
            Self::ReceivedEOF => write!(f, "ReceivedEOF"),
            Self::BeginFlush => write!(f, "BeginFlush"),
            Self::FlushComplete => write!(f, "FlushComplete"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::Error(msg) => write!(f, "Error({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> EventVariant for SinkEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            SinkEvent::Initialize => "Initialize",
            SinkEvent::Ready => "Ready",
            SinkEvent::ReceivedEOF => "ReceivedEOF",
            SinkEvent::BeginFlush => "BeginFlush",      // Sink-specific!
            SinkEvent::FlushComplete => "FlushComplete", // Sink-specific!
            SinkEvent::BeginDrain => "BeginDrain",
            SinkEvent::Error(_) => "Error",
            SinkEvent::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

/// Actions that sink FSM transitions can emit
pub enum SinkAction<H> {
    /// Allocate resources needed by the sink
    /// - Register writer ID with journal
    /// - Create subscription to upstream stages
    AllocateResources,
    
    /// Publish running event to journal
    PublishRunning,
    
    /// Send completion event to journal
    SendCompletion,
    
    /// Flush any buffered data to ensure durability
    FlushBuffers,
    
    /// Clean up all resources
    Cleanup,
    
    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for SinkAction
impl<H> Clone for SinkAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::AllocateResources => Self::AllocateResources,
            Self::PublishRunning => Self::PublishRunning,
            Self::SendCompletion => Self::SendCompletion,
            Self::FlushBuffers => Self::FlushBuffers,
            Self::Cleanup => Self::Cleanup,
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for SinkAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::SendCompletion => write!(f, "SendCompletion"),
            Self::FlushBuffers => write!(f, "FlushBuffers"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

// ============================================================================
// FSM Context
// ============================================================================

/// Context for sink handlers - contains everything actions need
#[derive(Clone)]
pub struct SinkContext<H: SinkHandler> {
    /// The handler instance that implements sink logic
    pub handler: Arc<RwLock<H>>,
    
    /// This sink's stage ID
    pub stage_id: obzenflow_core::StageId,
    
    /// Human-readable stage name for logging
    pub stage_name: String,
    
    /// Flow name for flow context
    pub flow_name: String,
    
    /// Flow ID from pipeline
    pub flow_id: FlowId,
    
    /// Data journal for writing delivery events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,
    
    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,
    
    /// Upstream journals for reading events
    pub upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
    
    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,
    
    /// Writer ID for this sink (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,
    
    /// Subscription to upstream events
    pub subscription: Arc<RwLock<Option<UpstreamSubscription<ChainEvent>>>>,
    
    /// Processing task handle (moved from supervisor to follow FSM patterns)
    pub processing_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
    
    /// Upstream stage IDs
    pub upstream_stages: Vec<obzenflow_core::StageId>,
    
    /// Track if we're currently flushing
    pub is_flushing: Arc<RwLock<bool>>,
    
    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,
}

impl<H: SinkHandler> SinkContext<H> {
    pub fn new(
        handler: H,
        stage_id: obzenflow_core::StageId,
        stage_name: String,
        flow_name: String,
        flow_id: FlowId,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        upstream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        upstream_stages: Vec<StageId>,
        instrumentation: Arc<StageInstrumentation>,
    ) -> Self {
        Self {
            handler: Arc::new(RwLock::new(handler)),
            stage_id,
            stage_name,
            flow_name,
            flow_id,
            data_journal,
            system_journal,
            upstream_journals,
            bus,
            writer_id: Arc::new(RwLock::new(None)),
            subscription: Arc::new(RwLock::new(None)),
            processing_task: Arc::new(RwLock::new(None)),
            upstream_stages,
            is_flushing: Arc::new(RwLock::new(false)),
            instrumentation,
        }
    }
}

impl<H: SinkHandler + 'static> FsmContext for SinkContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: SinkHandler + Send + Sync + 'static> FsmAction for SinkAction<H> {
    type Context = SinkContext<H>;
    
    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            SinkAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id.clone());
                *ctx.writer_id.write().await = Some(writer_id);
                
                // Create subscription from upstream journals
                let subscription = UpstreamSubscription::new(&ctx.upstream_journals).await
                    .map_err(|e| format!("Failed to create subscription: {:?}", e))?;
                
                *ctx.subscription.write().await = Some(subscription);
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Sink allocated resources and created subscription"
                );
                Ok(())
            }
            
            SinkAction::PublishRunning => {
                let running_event = SystemEvent::stage_running(ctx.stage_id);
                
                ctx.system_journal
                    .append(running_event, None)
                    .await
                    .map_err(|e| format!("Failed to publish running event: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Sink published running event"
                );
                Ok(())
            }
            
            SinkAction::SendCompletion => {
                // Write completion event to system journal
                let completion_event = SystemEvent::stage_completed(ctx.stage_id);
                
                ctx.system_journal
                    .append(completion_event, None)
                    .await
                    .map_err(|e| format!("Failed to write completion event: {}", e))?;
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Sink sent completion event"
                );
                Ok(())
            }

            SinkAction::FlushBuffers => {
                *ctx.is_flushing.write().await = true;

                let mut handler = ctx.handler.write().await;

                match handler.flush().await {
                    Ok(Some(payload)) => {
                        // grab a copy of the WriterId or crash; this should never be None after init
                        let writer_id = ctx
                            .writer_id
                            .read()
                            .await
                            .as_ref()
                            .expect("writer_id not initialised")
                            .clone();


                        let flow_ctx = FlowContext {
                            flow_name:  ctx.flow_name.clone(),
                            flow_id:    ctx.flow_id.to_string(),
                            stage_name: ctx.stage_name.clone(),
                            stage_type: StageType::Sink,   // or whatever enum case
                        };

                        let evt = ChainEventFactory::delivery_event(writer_id, payload)
                            .with_flow_context(flow_ctx)
                            .with_runtime_context(ctx.instrumentation.snapshot());

                        ctx.data_journal.append(evt, None)
                            .await
                            .map_err(|e| format!("Failed to write delivery receipt: {e}"))?;

                    }
                    Ok(None) => { /* flush succeeded, nothing to record */ }
                    Err(e) => return Err(format!("Failed to flush: {e:?}").into()),
                }

                *ctx.is_flushing.write().await = false;

                tracing::info!(stage_name=%ctx.stage_name, "Sink flushed buffers");
                Ok(())
            }
            
            SinkAction::Cleanup => {
                // Call handler drain before stopping tasks
                let mut handler = ctx.handler.write().await;
                handler.drain().await
                    .map_err(|e| format!("Failed to drain handler: {:?}", e))?;
                drop(handler);
                
                // Stop the processing task
                if let Some(task) = ctx.processing_task.write().await.take() {
                    task.abort();
                }
                
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Sink cleaned up resources"
                );
                Ok(())
            }
            
            SinkAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}