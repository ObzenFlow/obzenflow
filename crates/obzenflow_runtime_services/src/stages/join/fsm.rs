//! FSM for join stages
//!
//! Join stages have a dedicated FSM that handles per-source EOF semantics.
//! Unlike transforms which have single upstream, joins track two distinct upstreams
//! (reference and stream) with different behaviors.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::JournalEvent;
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::{ChainEvent, EventId, FlowId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::messaging::upstream_subscription::{ContractConfig, ReaderProgress};
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::{snapshot_stage_metrics, StageInstrumentation};
use crate::metrics::tail_read;
use crate::stages::common::handlers::JoinHandler;
use crate::stages::resources_builder::BoundSubscriptionFactory;

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

    /// Send failure event to journal with metrics
    SendFailure { message: String },

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
            Self::SendFailure { message } => write!(f, "SendFailure({:?})", message),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

// ============================================================================
// FSM Context
// ============================================================================

/// Context for join handlers - contains everything actions need
pub struct JoinContext<H: JoinHandler> {
    /// The handler instance (immutable, wrapped in Arc like StatefulContext)
    pub handler: Arc<H>,

    /// Handler state (catalogs, buffers)
    pub handler_state: H::State,

    /// This join's stage ID
    pub stage_id: StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Reference stage ID (set during construction via with_reference())
    /// Join stages need to know this to differentiate reference from stream
    pub reference_stage_id: StageId,

    /// Data journal for writing joined events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this join (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// Subscription to reference journal ONLY (used during Hydrating)
    pub reference_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Subscription to stream journals ONLY (used during Enriching)
    pub stream_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// FSM-owned contract state for reference side (aligned with reference_subscription readers)
    pub reference_contract_state: Vec<ReaderProgress>,

    /// FSM-owned contract state for stream side (aligned with stream_subscription readers)
    pub stream_contract_state: Vec<ReaderProgress>,

    /// Buffered EOF event to forward when draining completes
    pub buffered_eof: Option<ChainEvent>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Control event handling strategy
    pub control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,

    /// Bound factories for reference and stream subscriptions
    pub reference_subscription_factory: BoundSubscriptionFactory,
    pub stream_subscription_factory: BoundSubscriptionFactory,

    /// Counter of reference-side events processed since the last heartbeat
    /// (used during Hydrating for observability snapshots).
    pub events_since_last_heartbeat: u64,
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
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        control_strategy: Arc<dyn crate::stages::common::control_strategies::ControlEventStrategy>,
        instrumentation: Arc<StageInstrumentation>,
        reference_subscription_factory: BoundSubscriptionFactory,
        stream_subscription_factory: BoundSubscriptionFactory,
    ) -> Self {
        let handler_state = handler.initial_state();
        Self {
            handler: Arc::new(handler),
            handler_state,
            stage_id,
            stage_name,
            flow_name,
            flow_id,
            reference_stage_id,
            data_journal,
            error_journal,
            system_journal,
            bus,
            writer_id: None,
            reference_subscription: None,
            stream_subscription: None,
            reference_contract_state: Vec::new(),
            stream_contract_state: Vec::new(),
            buffered_eof: None,
            control_strategy,
            instrumentation,
            reference_subscription_factory,
            stream_subscription_factory,
            events_since_last_heartbeat: 0,
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

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            JoinAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id.clone());
                ctx.writer_id = Some(writer_id.clone());

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    reference_stage_id = ?ctx.reference_stage_id,
                    stream_journal_count = ctx.stream_subscription_factory.upstream_stage_ids().len(),
                    "Join: Creating subscriptions to upstream journals"
                );

                // Initialize FSM-owned contract state for reference and stream sides
                let ref_ids = ctx.reference_subscription_factory.upstream_stage_ids();
                ctx.reference_contract_state =
                    ref_ids.into_iter().map(ReaderProgress::new).collect();

                let stream_ids = ctx.stream_subscription_factory.upstream_stage_ids();
                ctx.stream_contract_state =
                    stream_ids.into_iter().map(ReaderProgress::new).collect();

                // 1) Subscribe to reference journal ONLY
                let ref_subscription = ctx
                    .reference_subscription_factory
                    .build_with_contracts(
                        writer_id.clone(),
                        ctx.data_journal.clone(),
                        ContractConfig::default(),
                        Some(ctx.system_journal.clone()),
                        Some(ctx.stage_id),
                        ctx.instrumentation.control_middleware().clone(),
                    )
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to create reference subscription: {e}"
                        ))
                    })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join: Created reference subscription successfully"
                );

                ctx.reference_subscription = Some(ref_subscription);

                // 2) Subscribe to stream journals ONLY (NOT reference)
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    stream_journal_ids = ?ctx.stream_subscription_factory.upstream_stage_ids(),
                    "Join: Creating stream subscription"
                );

                let stream_subscription = ctx
                    .stream_subscription_factory
                    .build_with_contracts(
                        writer_id,
                        ctx.data_journal.clone(),
                        ContractConfig::default(),
                        Some(ctx.system_journal.clone()),
                        Some(ctx.stage_id),
                        ctx.instrumentation.control_middleware().clone(),
                    )
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to create stream subscription: {e}"
                        ))
                    })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join: Created stream subscription successfully"
                );

                ctx.stream_subscription = Some(stream_subscription);

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
                // Write lifecycle event to system journal
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
                let writer_id = ctx.writer_id.as_ref().ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to forward EOF".to_string(),
                    )
                })?;

                // Always emit an EOF authored by this stage, preserving upstream
                // metadata when available.
                let buffered = ctx.buffered_eof.take();
                let mut natural = true;
                let mut upstream_vector_clock = None;
                let mut upstream_last_event = None;
                let runtime_context = ctx.instrumentation.snapshot_with_control();

                if let Some(buffered_event) = buffered {
                    if let obzenflow_core::event::ChainEventContent::FlowControl(
                        FlowControlPayload::Eof {
                            natural: n,
                            writer_seq,
                            vector_clock,
                            last_event_id,
                            ..
                        },
                    ) = buffered_event.content.clone()
                    {
                        natural = n;
                        upstream_vector_clock = vector_clock;
                        upstream_last_event = last_event_id;
                        // We intentionally ignore the upstream writer_seq and
                        // advertise our own position below.
                    }
                }

                let mut eof_event = ChainEventFactory::eof_event(writer_id.clone(), natural);

                if let obzenflow_core::event::ChainEventContent::FlowControl(
                    FlowControlPayload::Eof {
                        writer_id: ref mut eof_writer,
                        writer_seq,
                        vector_clock,
                        last_event_id,
                        ..
                    },
                ) = &mut eof_event.content
                {
                    *eof_writer = Some(writer_id.clone());
                    *writer_seq = Some(SeqNo(runtime_context.writer_seq));
                    if let Some(vc) = upstream_vector_clock {
                        *vector_clock = Some(vc);
                    }
                    *last_event_id = upstream_last_event
                        .or_else(|| runtime_context.last_emitted_event_id.clone());
                }

                // Attach flow/runtime context for downstream contract tracking
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::Join,
                };
                eof_event.runtime_context = Some(runtime_context);

                ctx.instrumentation.record_emitted(&eof_event);

                ctx.data_journal
                    .append(eof_event, None)
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!("Failed to forward EOF: {e}"))
                    })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Join forwarded EOF downstream"
                );
                Ok(())
            }

            JoinAction::SendCompletion => {
                // Write completion event to system journal with tail-read metrics.
                // If no runtime_context is available in the journals at completion
                // time, treat this as a hard error instead of fabricating metrics
                // from instrumentation.
                let metrics = tail_read::read_stage_metrics_from_tail(
                    &ctx.data_journal,
                    Some(&ctx.error_journal),
                    ctx.stage_id,
                )
                .await
                .ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "no runtime_context in journal tail for stage {} completion (data_journal={})",
                        ctx.stage_id,
                        ctx.data_journal.id(),
                    ))
                })?;
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
                    "Join sent completion event"
                );
                Ok(())
            }

            JoinAction::SendFailure { message } => {
                // Write failure event to system journal with tail-read metrics.
                // If no runtime_context is available in the journals at failure
                // time, fall back to a best-effort snapshot from instrumentation.
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

                let error_event = SystemEvent::stage_failed_with_metrics(
                    ctx.stage_id,
                    message.clone(),
                    false, // not recoverable
                    metrics,
                );

                match ctx.system_journal.append(error_event, None).await {
                    Ok(_) => {
                        tracing::error!(
                            stage_name = %ctx.stage_name,
                            error = %message,
                            "Join stage encountered error"
                        );
                    }
                    Err(e) => {
                        tracing::error!(
                            stage_name = %ctx.stage_name,
                            error = %message,
                            journal_error = %e,
                            "Join stage encountered error but failed to write error event"
                        );
                    }
                }

                Ok(())
            }

            JoinAction::Cleanup => {
                // Cleanup is now minimal since we don't use background tasks
                // Just clear subscriptions
                ctx.reference_subscription = None;
                ctx.stream_subscription = None;

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
