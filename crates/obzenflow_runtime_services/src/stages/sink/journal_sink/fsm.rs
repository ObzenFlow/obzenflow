//! Journal sink stage FSM types and state machine definition
//!
//! Journal sinks consume events and write to external destinations.
//! They have a unique "Flushing" state that ensures all buffered
//! data is written before shutdown.

use crate::messaging::upstream_subscription::ContractConfig;
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::ControlEventStrategy;
use crate::stages::common::handlers::SinkHandler;
use crate::stages::resources_builder::BoundSubscriptionFactory;
use futures::TryFutureExt;
use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, EventId, FlowId, StageId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for journal sink stages
#[derive(Serialize, Deserialize)]
pub enum JournalSinkState<H> {
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
impl<H> Clone for JournalSinkState<H> {
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

impl<H> std::fmt::Debug for JournalSinkState<H> {
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

impl<H: Send + Sync> PartialEq for JournalSinkState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (JournalSinkState::Created, JournalSinkState::Created) => true,
            (JournalSinkState::Initialized, JournalSinkState::Initialized) => true,
            (JournalSinkState::Running, JournalSinkState::Running) => true,
            (JournalSinkState::Flushing, JournalSinkState::Flushing) => true,
            (JournalSinkState::Draining, JournalSinkState::Draining) => true,
            (JournalSinkState::Drained, JournalSinkState::Drained) => true,
            (JournalSinkState::Failed(a), JournalSinkState::Failed(b)) => a == b,
            _ => false,
        }
    }
}

impl<H: Send + Sync + 'static> StateVariant for JournalSinkState<H> {
    fn variant_name(&self) -> &str {
        match self {
            JournalSinkState::Created => "Created",
            JournalSinkState::Initialized => "Initialized",
            JournalSinkState::Running => "Running",
            JournalSinkState::Flushing => "Flushing", // Unique to sinks!
            JournalSinkState::Draining => "Draining",
            JournalSinkState::Drained => "Drained",
            JournalSinkState::Failed(_) => "Failed",
            JournalSinkState::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Events
// ============================================================================

/// Events that can trigger journal sink state transitions
pub enum JournalSinkEvent<H> {
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

// Manual implementations for JournalSinkEvent
impl<H> Clone for JournalSinkEvent<H> {
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

impl<H> std::fmt::Debug for JournalSinkEvent<H> {
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

impl<H: Send + Sync + 'static> EventVariant for JournalSinkEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            JournalSinkEvent::Initialize => "Initialize",
            JournalSinkEvent::Ready => "Ready",
            JournalSinkEvent::ReceivedEOF => "ReceivedEOF",
            JournalSinkEvent::BeginFlush => "BeginFlush", // Sink-specific!
            JournalSinkEvent::FlushComplete => "FlushComplete", // Sink-specific!
            JournalSinkEvent::BeginDrain => "BeginDrain",
            JournalSinkEvent::Error(_) => "Error",
            JournalSinkEvent::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

/// Actions that journal sink FSM transitions can emit
pub enum JournalSinkAction<H> {
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

// Manual implementations for JournalSinkAction
impl<H> Clone for JournalSinkAction<H> {
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

impl<H> std::fmt::Debug for JournalSinkAction<H> {
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

/// Context for journal sink handlers - contains everything actions need
#[derive(Clone)]
pub struct JournalSinkContext<H: SinkHandler> {
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

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this sink (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,

    /// Subscription to upstream events
    pub subscription: Arc<RwLock<Option<UpstreamSubscription<ChainEvent>>>>,

    /// Processing task handle (moved from supervisor to follow FSM patterns)
    pub processing_task: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,

    /// Track if we're currently flushing
    pub is_flushing: Arc<RwLock<bool>>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Bound subscription factory for upstream journals
    pub upstream_subscription_factory: BoundSubscriptionFactory,

    /// Control strategy for FlowControl events
    pub control_strategy: Arc<dyn ControlEventStrategy>,
}

impl<H: SinkHandler> JournalSinkContext<H> {
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
        control_strategy: Arc<dyn ControlEventStrategy>,
        instrumentation: Arc<StageInstrumentation>,
        upstream_subscription_factory: BoundSubscriptionFactory,
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
            subscription: Arc::new(RwLock::new(None)),
            processing_task: Arc::new(RwLock::new(None)),
            is_flushing: Arc::new(RwLock::new(false)),
            upstream_subscription_factory,
            control_strategy,
            instrumentation,
        }
    }
}

impl<H: SinkHandler + 'static> FsmContext for JournalSinkContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: SinkHandler + Send + Sync + 'static> FsmAction for JournalSinkAction<H> {
    type Context = JournalSinkContext<H>;

    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            JournalSinkAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id.clone());
                *ctx.writer_id.write().await = Some(writer_id.clone());

                // Build subscription using bound factory with contracts
                let subscription = ctx
                    .upstream_subscription_factory
                    .build_with_contracts(
                        writer_id,
                        ctx.data_journal.clone(),
                        ContractConfig::default(),
                        Some(ctx.system_journal.clone()),
                        Some(ctx.stage_id),
                    )
                    .await
                    .map_err(|e| format!("Failed to create subscription: {}", e))?;

                *ctx.subscription.write().await = Some(subscription);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    upstream_count = ctx.upstream_subscription_factory.upstream_stage_ids().len(),
                    "Sink allocated resources and created subscription"
                );
                Ok(())
            }

            JournalSinkAction::PublishRunning => {
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

            JournalSinkAction::SendCompletion => {
                // Write completion event to system journal
                let completion_event = SystemEvent::stage_completed(ctx.stage_id);

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    stage_id = ?ctx.stage_id,
                    fsm_state = "Draining",
                    "sink: writing stage_completed to system.log"
                );

                ctx.system_journal
                    .append(completion_event, None)
                    .await
                    .map_err(|e| format!("Failed to write completion event: {}", e))?;

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    stage_id = ?ctx.stage_id,
                    fsm_state = "Draining",
                    "sink: stage_completed append succeeded"
                );

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Sink sent completion event"
                );
                Ok(())
            }

            JournalSinkAction::FlushBuffers => {
                *ctx.is_flushing.write().await = true;

                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: FlushBuffers starting"
                );

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
                            flow_name: ctx.flow_name.clone(),
                            flow_id: ctx.flow_id.to_string(),
                            stage_name: ctx.stage_name.clone(),
                            stage_id: ctx.stage_id.clone(),
                            stage_type: StageType::Sink, // or whatever enum case
                        };

                        let evt = ChainEventFactory::delivery_event(writer_id, payload)
                            .with_flow_context(flow_ctx)
                            .with_runtime_context(ctx.instrumentation.snapshot());

                        ctx.data_journal
                            .append(evt, None)
                            .await
                            .map_err(|e| format!("Failed to write delivery receipt: {e}"))?;
                    }
                    Ok(None) => { /* flush succeeded, nothing to record */ }
                    Err(e) => return Err(format!("Failed to flush: {e:?}").into()),
                }

                // After flush, emit any pending contract events (final/progress/stall)
                if let Some(mut subscription) = ctx.subscription.write().await.take() {
                    tracing::info!(
                        target: "flowip-080o",
                        stage_name = %ctx.stage_name,
                        "sink: FlushBuffers completed, emitting any pending contract events"
                    );
                    let _ = subscription.check_contracts().await;
                    *ctx.subscription.write().await = Some(subscription);
                }

                *ctx.is_flushing.write().await = false;

                tracing::info!(stage_name=%ctx.stage_name, "Sink flushed buffers");
                Ok(())
            }

            JournalSinkAction::Cleanup => {
                // Call handler drain before stopping tasks
                let mut handler = ctx.handler.write().await;
                handler
                    .drain()
                    .await
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

            JournalSinkAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}
