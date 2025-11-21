//! FSM for join stages
//!
//! Join stages have a dedicated FSM that handles per-source EOF semantics.
//! Unlike transforms which have single upstream, joins track two distinct upstreams
//! (reference and stream) with different behaviors.

use obzenflow_core::event::JournalEvent;
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::{ChainEvent, EventId, FlowId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::handlers::JoinHandler;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for join stages
#[derive(Serialize, Deserialize)]
pub enum JoinState<H> {
    /// Initial state - join stage created but not initialized
    Created,

    /// Resources allocated, ready to start processing
    Initialized,

    /// Hydrating: Building reference catalog from reference events
    /// During this state:
    /// - ONLY reads from reference subscription
    /// - Stream events queue in journal/subscription (natural backpressure)
    /// - Transitions to Enriching when reference EOF received
    Hydrating,

    /// Enriching: Reference catalog complete, actively enriching stream events
    /// During this state:
    /// - ONLY reads from stream subscription
    /// - Reference catalog is complete and ready for lookups
    /// - Transitions to Draining when stream EOF received
    Enriching,

    /// All upstreams reached EOF, finishing processing
    Draining,

    /// All events processed, ready to complete
    Drained,

    /// Unrecoverable error occurred
    Failed(String),

    #[serde(skip)]
    _Phantom(PhantomData<H>),
}

// Manual implementations that don't require H to implement these traits
impl<H> Clone for JoinState<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Created => Self::Created,
            Self::Initialized => Self::Initialized,
            Self::Hydrating => Self::Hydrating,
            Self::Enriching => Self::Enriching,
            Self::Draining => Self::Draining,
            Self::Drained => Self::Drained,
            Self::Failed(msg) => Self::Failed(msg.clone()),
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for JoinState<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Created => write!(f, "Created"),
            Self::Initialized => write!(f, "Initialized"),
            Self::Hydrating => write!(f, "Hydrating"),
            Self::Enriching => write!(f, "Enriching"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H> Default for JoinState<H> {
    fn default() -> Self {
        Self::Created
    }
}

impl<H: Send + Sync> PartialEq for JoinState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (JoinState::Created, JoinState::Created) => true,
            (JoinState::Initialized, JoinState::Initialized) => true,
            (JoinState::Hydrating, JoinState::Hydrating) => true,
            (JoinState::Enriching, JoinState::Enriching) => true,
            (JoinState::Draining, JoinState::Draining) => true,
            (JoinState::Drained, JoinState::Drained) => true,
            (JoinState::Failed(a), JoinState::Failed(b)) => a == b,
            _ => false,
        }
    }
}

impl<H: Send + Sync + 'static> StateVariant for JoinState<H> {
    fn variant_name(&self) -> &str {
        match self {
            JoinState::Created => "Created",
            JoinState::Initialized => "Initialized",
            JoinState::Hydrating => "Hydrating",
            JoinState::Enriching => "Enriching",
            JoinState::Draining => "Draining",
            JoinState::Drained => "Drained",
            JoinState::Failed(_) => "Failed",
            JoinState::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Events
// ============================================================================

/// Events that trigger join state transitions
#[derive(Clone)]
pub enum JoinEvent<H> {
    /// Initialize the join stage
    Initialize,

    /// Ready to start processing
    Ready,

    /// Received EOF from upstream (subscription context determines which one)
    ReceivedEOF,

    /// Reference upstream is complete (triggers transition to Enriching)
    ReferenceComplete,

    /// Begin draining process
    BeginDrain,

    /// Draining complete
    DrainComplete,

    /// Unrecoverable error occurred
    Error(String),

    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

impl<H> std::fmt::Debug for JoinEvent<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initialize => write!(f, "Initialize"),
            Self::Ready => write!(f, "Ready"),
            Self::ReceivedEOF => write!(f, "ReceivedEOF"),
            Self::ReferenceComplete => write!(f, "ReferenceComplete"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::DrainComplete => write!(f, "DrainComplete"),
            Self::Error(msg) => write!(f, "Error({:?})", msg),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Clone + Send + Sync + 'static> EventVariant for JoinEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            JoinEvent::Initialize => "Initialize",
            JoinEvent::Ready => "Ready",
            JoinEvent::ReceivedEOF { .. } => "ReceivedEOF",
            JoinEvent::ReferenceComplete => "ReferenceComplete",
            JoinEvent::BeginDrain => "BeginDrain",
            JoinEvent::DrainComplete => "DrainComplete",
            JoinEvent::Error(_) => "Error",
            JoinEvent::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

// ============================================================================
// FSM Actions
// ============================================================================

/// Actions that join FSM transitions can emit
#[derive(Clone)]
pub enum JoinAction<H> {
    /// Allocate resources (writer ID, subscriptions)
    AllocateResources,

    /// Initialize handler state
    InitializeHandlerState,

    /// Publish running event to journal
    PublishRunning,

    /// Hydrate reference catalog from reference event
    HydrateCatalog,

    /// Enrich stream event with reference data
    EnrichEvent,

    /// Forward EOF event downstream
    ForwardEOF,

    /// Send completion event
    SendCompletion,

    /// Clean up all resources
    Cleanup,

    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

impl<H> std::fmt::Debug for JoinAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::InitializeHandlerState => write!(f, "InitializeHandlerState"),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::HydrateCatalog => write!(f, "HydrateCatalog"),
            Self::EnrichEvent => write!(f, "EnrichEvent"),
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

/// Context for join handlers - contains everything actions need
#[derive(Clone)]
pub struct JoinContext<H: JoinHandler> {
    /// The handler instance (immutable, wrapped in Arc like StatefulContext)
    pub handler: Arc<H>,

    /// Handler state (catalogs, buffers)
    pub handler_state: Arc<RwLock<H::State>>,

    /// This join's stage ID
    pub stage_id: StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Reference stage ID (set during construction via with_reference())
    pub reference_stage_id: StageId,

    /// Stream stage IDs (from topology upstreams)
    pub stream_stages: Vec<StageId>,

    /// Data journal for writing joined events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Reference journal (dimension/lookup data)
    pub reference_journal: Arc<dyn Journal<ChainEvent>>,

    /// Stream journals (fact data from topology upstreams) - NOT including reference
    pub stream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this join (initialized during setup)
    pub writer_id: Arc<RwLock<Option<WriterId>>>,

    /// Subscription to reference journal ONLY (used during Hydrating)
    pub reference_subscription: Arc<RwLock<Option<UpstreamSubscription<ChainEvent>>>>,

    /// Subscription to stream journals ONLY (used during Enriching)
    pub stream_subscription: Arc<RwLock<Option<UpstreamSubscription<ChainEvent>>>>,

    /// Buffered EOF event to forward when draining completes
    pub buffered_eof: Arc<RwLock<Option<ChainEvent>>>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Control event handling strategy
    pub control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,
}

impl<H: JoinHandler> JoinContext<H> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handler: H,
        stage_id: StageId,
        stage_name: String,
        flow_name: String,
        flow_id: FlowId,
        reference_stage_id: StageId,
        stream_stages: Vec<StageId>,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        reference_journal: Arc<dyn Journal<ChainEvent>>,
        stream_journals: Vec<(StageId, Arc<dyn Journal<ChainEvent>>)>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,
        instrumentation: Arc<StageInstrumentation>,
    ) -> Self {
        let handler_state = handler.initial_state();
        Self {
            handler: Arc::new(handler),
            handler_state: Arc::new(RwLock::new(handler_state)),
            stage_id,
            stage_name,
            flow_name,
            flow_id,
            reference_stage_id,
            stream_stages,
            data_journal,
            error_journal,
            system_journal,
            reference_journal,
            stream_journals,
            bus,
            writer_id: Arc::new(RwLock::new(None)),
            reference_subscription: Arc::new(RwLock::new(None)),
            stream_subscription: Arc::new(RwLock::new(None)),
            buffered_eof: Arc::new(RwLock::new(None)),
            control_strategy,
            instrumentation,
        }
    }
}

impl<H: JoinHandler + 'static> FsmContext for JoinContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: JoinHandler + Send + Sync + 'static> FsmAction for JoinAction<H> {
    type Context = JoinContext<H>;

    async fn execute(&self, ctx: &Self::Context) -> Result<(), String> {
        match self {
            JoinAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id.clone());
                *ctx.writer_id.write().await = Some(writer_id);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    reference_stage_id = ?ctx.reference_stage_id,
                    stream_stages = ?ctx.stream_stages,
                    "Join: Creating subscriptions to upstream journals"
                );

                // 1) Subscribe to reference journal ONLY
                let ref_subscription = UpstreamSubscription::new(&vec![(
                    ctx.reference_stage_id,
                    ctx.reference_journal.clone(),
                )])
                .await
                .map_err(|e| format!("Failed to create reference subscription: {:?}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join: Created reference subscription successfully"
                );

                *ctx.reference_subscription.write().await = Some(ref_subscription);

                // 2) Subscribe to stream journals ONLY (NOT reference)
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    stream_journal_ids = ?ctx.stream_journals.iter().map(|(id, _)| id).collect::<Vec<_>>(),
                    "Join: Creating stream subscription"
                );
                let stream_subscription = UpstreamSubscription::new(&ctx.stream_journals)
                    .await
                    .map_err(|e| format!("Failed to create stream subscription: {:?}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join: Created stream subscription successfully"
                );

                *ctx.stream_subscription.write().await = Some(stream_subscription);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join allocated resources"
                );
                Ok(())
            }

            JoinAction::InitializeHandlerState => {
                // Handler state already initialized in context creation
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join handler state initialized"
                );
                Ok(())
            }

            JoinAction::PublishRunning => {
                let writer_id_guard = ctx.writer_id.read().await;
                let _writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to publish running event".to_string())?;

                // Write lifecycle event to system journal
                let running_event = SystemEvent::stage_running(ctx.stage_id);

                ctx.system_journal
                    .append(running_event, None)
                    .await
                    .map_err(|e| format!("Failed to publish running event: {}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join published running event"
                );
                Ok(())
            }

            JoinAction::HydrateCatalog => {
                // This action is not used in the current implementation
                // The hydration happens inline in dispatch_state during Hydrating state
                // This is here for future refactoring to make actions more consistent
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    "HydrateCatalog action (currently handled in dispatch_state)"
                );
                Ok(())
            }

            JoinAction::EnrichEvent => {
                // This action is not used in the current implementation
                // The enrichment happens inline in dispatch_state during Enriching state
                // This is here for future refactoring to make actions more consistent
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    "EnrichEvent action (currently handled in dispatch_state)"
                );
                Ok(())
            }

            JoinAction::ForwardEOF => {
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
                    "Join forwarded EOF downstream"
                );
                Ok(())
            }

            JoinAction::SendCompletion => {
                let writer_id_guard = ctx.writer_id.read().await;
                let _writer_id = writer_id_guard
                    .as_ref()
                    .ok_or_else(|| "No writer ID available to send completion".to_string())?;

                // Write completion event to system journal
                let completion_event = SystemEvent::stage_completed(ctx.stage_id);

                ctx.system_journal
                    .append(completion_event, None)
                    .await
                    .map_err(|e| format!("Failed to write completion event: {}", e))?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join sent completion event"
                );
                Ok(())
            }

            JoinAction::Cleanup => {
                // Cleanup is now minimal since we don't use background tasks
                // Just clear subscriptions
                *ctx.reference_subscription.write().await = None;
                *ctx.stream_subscription.write().await = None;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join cleaned up resources"
                );
                Ok(())
            }

            JoinAction::_Phantom(_) => unreachable!("PhantomData action"),
        }
    }
}
