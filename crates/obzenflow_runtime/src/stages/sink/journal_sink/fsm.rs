// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal sink stage FSM types and state machine definition
//!
//! Journal sinks consume events and write to external destinations.
//! They have a unique "Flushing" state that ensures all buffered
//! data is written before shutdown.

use super::journalled_delivery_event;
use crate::backpressure::{BackpressureReader, BackpressureWriter};
use crate::effects::{EffectDeclaration, EffectHistory, EffectPortRegistry};
use crate::execution::RuntimeExecution;
use crate::message_bus::FsmMessageBus;
use crate::messaging::upstream_subscription::{
    ContractConfig, ContractsWiring, ReaderProgress, StageInputPosition,
};
use crate::messaging::{DeliveredRecord, UpstreamSubscription};
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::control_strategies::{ProcessingContext, SignalGate};
use crate::stages::common::handler_error::{HandlerError, StageFatal};
use crate::stages::common::handlers::{SinkLifecycleReport, UnifiedSinkHandler};
use crate::stages::common::heartbeat::HeartbeatHandle;
use crate::stages::common::stage_handle::{
    discarded_control_details, FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::lifecycle_actions;
use crate::stages::common::supervision::stage_fatal::{record_stage_fatal, StageFatalCommit};
use crate::stages::observer::dispatch::run_stage_lifecycle_observers;
use crate::stages::observer::{StageLifecyclePhase, StageObserverBundle};
use crate::stages::resources_builder::BoundSubscriptionFactory;
use crate::stages::sink::{record_sink_lifecycle_operation_failure, SinkLifecycleFailureCommit};
use crate::supervised_base::handler_supervised::SupervisorAction;
use crate::supervised_base::publication::{self, BoxError};
use crate::supervised_base::with_external_events::ExternalControlEvent;
use obzenflow_core::config::LineagePolicy;
use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
use obzenflow_core::event::payloads::flow_control_payload::EofKind;
use obzenflow_core::event::provenance::causality_context::CausalityContext;
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainPayload, CommandDiscardDisposition, SinkOperationPhase};
use obzenflow_core::journal::{AppendOptions, Journal};
use obzenflow_core::{ChainEvent, EventId, FlowId, ReaderGeneration, StageId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, FsmError, StateVariant};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::marker::PhantomData;
use std::sync::Arc;

// ============================================================================
// FSM States
// ============================================================================

/// FSM states for journal sink stages
#[derive(Serialize, Deserialize)]
pub enum JournalSinkState<H> {
    /// Initial state - sink has been created but not initialized
    Created,

    Initializing,
    Starting,
    Finalising,
    DrainingWriter,
    CheckingContracts,
    Failing(String),
    Cancelling(String),
    Cancelled(String),

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
            Self::Initializing => Self::Initializing,
            Self::Starting => Self::Starting,
            Self::Finalising => Self::Finalising,
            Self::DrainingWriter => Self::DrainingWriter,
            Self::CheckingContracts => Self::CheckingContracts,
            Self::Failing(cause) => Self::Failing(cause.clone()),
            Self::Cancelling(cause) => Self::Cancelling(cause.clone()),
            Self::Cancelled(cause) => Self::Cancelled(cause.clone()),

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
            Self::Initializing => write!(f, "Initializing"),
            Self::Starting => write!(f, "Starting"),
            Self::Finalising => write!(f, "Finalising"),
            Self::DrainingWriter => write!(f, "DrainingWriter"),
            Self::CheckingContracts => write!(f, "CheckingContracts"),
            Self::Failing(cause) => write!(f, "Failing({cause:?})"),
            Self::Cancelling(cause) => write!(f, "Cancelling({cause:?})"),
            Self::Cancelled(cause) => write!(f, "Cancelled({cause:?})"),

            Self::Initialized => write!(f, "Initialized"),
            Self::Running => write!(f, "Running"),
            Self::Flushing => write!(f, "Flushing"),
            Self::Draining => write!(f, "Draining"),
            Self::Drained => write!(f, "Drained"),
            Self::Failed(msg) => write!(f, "Failed({msg:?})"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync> PartialEq for JournalSinkState<H> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (JournalSinkState::Created, JournalSinkState::Created) => true,
            (Self::Initializing, Self::Initializing) => true,
            (Self::Starting, Self::Starting) => true,
            (Self::Finalising, Self::Finalising) => true,
            (Self::DrainingWriter, Self::DrainingWriter) => true,
            (Self::CheckingContracts, Self::CheckingContracts) => true,
            (Self::Failing(a), Self::Failing(b)) => a == b,
            (Self::Cancelling(a), Self::Cancelling(b)) => a == b,
            (Self::Cancelled(a), Self::Cancelled(b)) => a == b,

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
            Self::Initializing => "Initializing",
            Self::Starting => "Starting",
            Self::Finalising => "Finalising",
            Self::DrainingWriter => "DrainingWriter",
            Self::CheckingContracts => "CheckingContracts",
            Self::Failing(..) => "Failing",
            Self::Cancelling(..) => "Cancelling",
            Self::Cancelled(..) => "Cancelled",

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

impl<H> JournalSinkState<H> {
    pub(crate) fn failure(cause: String) -> Self {
        match cause.as_str() {
            FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP | STOP_REASON_TIMEOUT => {
                Self::Cancelling(cause)
            }
            _ => Self::Failing(cause),
        }
    }

    pub(crate) fn lifecycle_phase(&self) -> LifecyclePhase {
        use crate::stages::common::stage_lifecycle::LifecyclePhase as Phase;
        match self {
            Self::Initializing => Phase::Initializing,
            Self::Initialized => Phase::Initialized,
            Self::Running => Phase::Active,
            Self::Finalising => Phase::Finalising,
            Self::Failing(cause) => Phase::Failing(cause.clone()),
            Self::Cancelling(reason) => Phase::Cancelling(reason.clone()),
            Self::Drained => Phase::Completed,
            Self::Failed(cause) => Phase::Failed(cause.clone()),
            Self::Cancelled(reason) => Phase::Cancelled(reason.clone()),
            _ => Phase::Other,
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
    InitializationCompleted,
    ActivationCompleted,
    FinalisationCompleted,
    TerminationSettled,
    WriterDrained,
    ContractsAccepted,

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
            Self::InitializationCompleted => Self::InitializationCompleted,
            Self::ActivationCompleted => Self::ActivationCompleted,
            Self::FinalisationCompleted => Self::FinalisationCompleted,
            Self::TerminationSettled => Self::TerminationSettled,
            Self::WriterDrained => Self::WriterDrained,
            Self::ContractsAccepted => Self::ContractsAccepted,

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
            Self::InitializationCompleted => write!(f, "InitializationCompleted"),
            Self::ActivationCompleted => write!(f, "ActivationCompleted"),
            Self::FinalisationCompleted => write!(f, "FinalisationCompleted"),
            Self::TerminationSettled => write!(f, "TerminationSettled"),
            Self::WriterDrained => write!(f, "WriterDrained"),
            Self::ContractsAccepted => write!(f, "ContractsAccepted"),

            Self::Ready => write!(f, "Ready"),
            Self::ReceivedEOF => write!(f, "ReceivedEOF"),
            Self::BeginFlush => write!(f, "BeginFlush"),
            Self::FlushComplete => write!(f, "FlushComplete"),
            Self::BeginDrain => write!(f, "BeginDrain"),
            Self::Error(msg) => write!(f, "Error({msg:?})"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

impl<H: Send + Sync + 'static> ExternalControlEvent for JournalSinkEvent<H> {
    fn discard_details(&self) -> (CommandDiscardDisposition, Option<String>) {
        discarded_control_details(match self {
            Self::Error(message) => Some(message.as_str()),
            Self::InitializationCompleted
            | Self::ActivationCompleted
            | Self::FinalisationCompleted
            | Self::TerminationSettled
            | Self::WriterDrained
            | Self::ContractsAccepted => None,
            Self::Initialize
            | Self::Ready
            | Self::ReceivedEOF
            | Self::BeginFlush
            | Self::FlushComplete
            | Self::BeginDrain => None,
            Self::_Phantom(_) => unreachable!("PhantomData variant"),
        })
    }
}

impl<H: Send + Sync + 'static> EventVariant for JournalSinkEvent<H> {
    fn variant_name(&self) -> &str {
        match self {
            JournalSinkEvent::Initialize => "Initialize",
            Self::InitializationCompleted => "InitializationCompleted",
            Self::ActivationCompleted => "ActivationCompleted",
            Self::FinalisationCompleted => "FinalisationCompleted",
            Self::TerminationSettled => "TerminationSettled",
            Self::WriterDrained => "WriterDrained",
            Self::ContractsAccepted => "ContractsAccepted",

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
    Host(SupervisorAction<JournalSinkEvent<H>>),
    /// Allocate resources needed by the sink
    /// - Register writer ID with journal
    /// - Create subscription to upstream stages
    AllocateResources,

    /// Publish running event to journal
    PublishRunning,

    /// Send completion event to journal
    SendCompletion,

    /// Send failure event to journal with metrics
    SendFailure {
        message: String,
    },

    /// Flush any buffered data to ensure durability
    FlushBuffers,

    /// Gracefully drain the writer and journal every returned receipt.
    ///
    /// This is a fallible settlement operation and therefore remains distinct
    /// from drop-only resource cleanup.
    DrainWriter,

    /// Run the authoritative post-flush contract evaluation.
    ///
    /// This must run only after `FlushBuffers` has journalled any per-event commit receipts so that
    /// EOF completion and progress signals are gated on durable receipt evidence.
    VerifyContractsAfterFlush,

    /// Clean up all resources
    Cleanup,

    #[doc(hidden)]
    _Phantom(PhantomData<H>),
}

// Manual implementations for JournalSinkAction
impl<H> Clone for JournalSinkAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::Host(action) => Self::Host(action.clone()),

            Self::AllocateResources => Self::AllocateResources,
            Self::PublishRunning => Self::PublishRunning,
            Self::SendCompletion => Self::SendCompletion,
            Self::SendFailure { message } => Self::SendFailure {
                message: message.clone(),
            },
            Self::FlushBuffers => Self::FlushBuffers,
            Self::DrainWriter => Self::DrainWriter,
            Self::VerifyContractsAfterFlush => Self::VerifyContractsAfterFlush,
            Self::Cleanup => Self::Cleanup,
            Self::_Phantom(_) => Self::_Phantom(PhantomData),
        }
    }
}

impl<H> std::fmt::Debug for JournalSinkAction<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Host(action) => action.fmt(f),

            Self::AllocateResources => write!(f, "AllocateResources"),
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::SendCompletion => write!(f, "SendCompletion"),
            Self::SendFailure { message } => write!(f, "SendFailure({message:?})"),
            Self::FlushBuffers => write!(f, "FlushBuffers"),
            Self::DrainWriter => write!(f, "DrainWriter"),
            Self::VerifyContractsAfterFlush => write!(f, "VerifyContractsAfterFlush"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

// ============================================================================
// FSM Context
// ============================================================================

/// Context for journal sink handlers - contains everything actions need
pub struct JournalSinkResources<H: UnifiedSinkHandler> {
    /// The handler instance that implements sink logic
    pub handler: Option<H>,

    /// This sink's stage ID
    pub stage_id: StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Single-writer receipt identity (FLOWIP-120s): the handler's declared
    /// destination family, else the stage name. Stamped on every journalled
    /// `DeliveryPayload`.
    pub receipt_destination: String,

    /// Connector-described method used for runtime-authored failure receipts.
    pub default_delivery_method: Option<DeliveryMethod>,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Data journal for writing delivery events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Recorded effect outcomes for replay suppression.
    pub effect_history: Option<Arc<EffectHistory>>,

    /// Runtime execution strategy (FLOWIP-120r).
    pub runtime_execution: RuntimeExecution,

    /// Flow-scoped typed ports available to replay-safe effects.
    pub effect_ports: EffectPortRegistry,

    /// Descriptor-owned effect declarations for replay-safe effect invocation.
    pub effect_declarations: Vec<EffectDeclaration>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events

    /// Message bus for pipeline communication
    pub bus: Arc<FsmMessageBus>,

    /// Writer ID for this sink (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// FLOWIP-010 §7: build-resolved lineage policy from stage resources.
    pub lineage_policy: LineagePolicy,

    /// Subscription to upstream events
    pub subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// FSM-owned contract state for each upstream reader (aligned with subscription readers)
    pub contract_state: Vec<ReaderProgress>,

    /// Worst-wins join over the inputs' terminal EOF kinds (FLOWIP-095k).
    pub terminal_eof_kind: Option<EofKind>,

    /// Last supervisor-driven contract check instant (FLOWIP-080r).
    pub(crate) last_contract_check: Option<tokio::time::Instant>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Bound subscription factory for upstream journals
    pub upstream_subscription_factory: BoundSubscriptionFactory,

    /// Control strategy for FlowControl events
    pub control_strategy: Arc<dyn SignalGate>,

    /// Runtime-neutral sink-delivery boundary seam (FLOWIP-115b). Wraps the
    /// data-event `consume_report` attempt only.
    pub sink_delivery_boundary: Option<Arc<dyn super::boundary::SinkDeliveryBoundary>>,

    /// Observe-only middleware hooks for sink delivery.
    pub observers: StageObserverBundle,

    /// Durable per-stage signal-strategy scratch (FLOWIP-115c).
    pub processing_context: ProcessingContext,

    /// Backpressure writer handle for this stage's journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Backpressure readers keyed by upstream stage ID (FLOWIP-086k).
    pub backpressure_readers: HashMap<StageId, BackpressureReader>,

    /// Optional per-stage heartbeat task (FLOWIP-063e).
    pub(crate) heartbeat: Option<HeartbeatHandle>,

    /// Catch-up flip latch (FLOWIP-120n): the last generation this stage
    /// flipped at, making the flip idempotent per generation across both
    /// triggers (watermark and authored EOF).
    pub(crate) catch_up_flip: Option<ReaderGeneration>,

    /// Set once a failed transition has durable lifecycle evidence. Failure
    /// cleanup must never invoke another connector lifecycle method.
    pub(crate) failure_lifecycle_recorded: bool,

    /// Final chain-event cause used by the correctness-bearing failed
    /// lifecycle append for sink protocol and operation failures.
    pub(crate) failure_causal_event_id: Option<EventId>,
}

/// Transition data remains available while an operation owns the sink resources.
/// No handler or subscription is cloned to make the FSM responsive.
pub struct JournalSinkContext<H: UnifiedSinkHandler> {
    pub(crate) resources: Option<JournalSinkResources<H>>,
    pub(crate) instrumentation: Arc<StageInstrumentation>,
}

impl<H: UnifiedSinkHandler> JournalSinkContext<H> {
    pub(crate) fn new(resources: JournalSinkResources<H>) -> Self {
        Self {
            instrumentation: resources.instrumentation.clone(),
            resources: Some(resources),
        }
    }

    pub(crate) fn resources_mut(&mut self) -> Result<&mut JournalSinkResources<H>, FsmError> {
        self.resources.as_mut().ok_or_else(|| {
            FsmError::HandlerError("sink resources belong to a pending operation".into())
        })
    }
}

impl<H: UnifiedSinkHandler + 'static> FsmContext for JournalSinkContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: UnifiedSinkHandler + Send + Sync + 'static> FsmAction for JournalSinkAction<H> {
    type Context = JournalSinkContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), FsmError> {
        self.execute_resources(ctx.resources_mut()?)
            .await
            .map_err(|error| FsmError::HandlerError(error.to_string()))
    }
}

impl<H: UnifiedSinkHandler + Send + Sync + 'static> JournalSinkAction<H> {
    pub(crate) async fn execute_resources(
        &self,
        ctx: &mut JournalSinkResources<H>,
    ) -> Result<(), BoxError> {
        match self {
            JournalSinkAction::Host(_) => Err(FsmError::HandlerError(
                "host action requires the supervised runner".into(),
            )
            .into()),

            JournalSinkAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);

                // Initialize FSM-owned contract state for each upstream reader
                let upstream_ids = ctx.upstream_subscription_factory.upstream_stage_ids();
                ctx.contract_state = upstream_ids.into_iter().map(ReaderProgress::new).collect();

                // Build subscription using bound factory with contracts
                let subscription = ctx
                    .upstream_subscription_factory
                    .build_with_contracts(ContractsWiring {
                        writer_id,
                        contract_journal: ctx.data_journal.clone(),
                        config: ContractConfig::default(),
                        reader_stage: Some(ctx.stage_id),
                        control_plane: ctx.instrumentation.control_plane().clone(),
                        include_delivery_contract: true,
                        cycle_guard_config: None,
                    })
                    .await
                    .map_err(|e| {
                        FsmError::HandlerError(format!("Failed to create subscription: {e}"))
                    })?
                    .with_contract_flow_context(make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        StageType::Sink,
                    ));

                ctx.subscription = Some(subscription);

                // archive-io: recorded effect history is genuine I/O, not a phase decision (FLOWIP-120r).
                let recorded_history = ctx.runtime_execution.archive_for_io();
                if let Some(archive) = recorded_history {
                    let history = EffectHistory::load(archive, &ctx.stage_name)
                        .await
                        .map_err(|e| {
                            FsmError::HandlerError(format!(
                                "Failed to load effect history for '{}': {e}",
                                ctx.stage_name
                            ))
                        })?;
                    // FLOWIP-120n F7: register the recorded effect mark so a
                    // prefix cursor miss fails loud and a live-tail miss runs.
                    if let Some(control) = ctx.runtime_execution.resume_control() {
                        if let Some(max) = history.max_recorded_input_seq() {
                            control.record_effect_high_water(ctx.stage_id, StageInputPosition(max));
                        }
                    }
                    ctx.effect_history = Some(Arc::new(history));
                }

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    upstream_count = ctx.upstream_subscription_factory.upstream_stage_ids().len(),
                    "Sink allocated resources and created subscription"
                );
                Ok(())
            }

            JournalSinkAction::PublishRunning => {
                lifecycle_actions::publish_running(
                    &ctx.data_journal,
                    make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        StageType::Sink,
                    ),
                )
                .await?;
                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                run_stage_lifecycle_observers(
                    &ctx.observers,
                    scope,
                    StageLifecyclePhase::Running,
                    || {
                        (
                            ctx.flow_id,
                            FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: ctx.stage_id,
                                stage_type: StageType::Sink,
                            },
                        )
                    },
                );
                Ok(())
            }

            JournalSinkAction::SendCompletion => {
                if let Some(heartbeat) = &ctx.heartbeat {
                    heartbeat.state.mark_completed();
                }

                lifecycle_actions::send_completion(
                    &ctx.data_journal,
                    make_flow_context(
                        &ctx.flow_name,
                        &ctx.flow_id.to_string(),
                        &ctx.stage_name,
                        ctx.stage_id,
                        StageType::Sink,
                    ),
                    ctx.instrumentation.as_ref(),
                )
                .await?;
                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                run_stage_lifecycle_observers(
                    &ctx.observers,
                    scope,
                    StageLifecyclePhase::Completed,
                    || {
                        (
                            ctx.flow_id,
                            FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: ctx.stage_id,
                                stage_type: StageType::Sink,
                            },
                        )
                    },
                );
                Ok(())
            }

            JournalSinkAction::SendFailure { message } => {
                if !ctx.failure_lifecycle_recorded {
                    lifecycle_actions::send_failure(
                        &ctx.data_journal,
                        make_flow_context(
                            &ctx.flow_name,
                            &ctx.flow_id.to_string(),
                            &ctx.stage_name,
                            ctx.stage_id,
                            StageType::Sink,
                        ),
                        message,
                        ctx.instrumentation.as_ref(),
                        ctx.failure_causal_event_id,
                    )
                    .await?;
                    ctx.failure_lifecycle_recorded = true;
                }
                let scope = ctx.runtime_execution.stage_scope(ctx.stage_id);
                run_stage_lifecycle_observers(
                    &ctx.observers,
                    scope,
                    StageLifecyclePhase::Failed,
                    || {
                        (
                            ctx.flow_id,
                            FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: ctx.stage_id,
                                stage_type: StageType::Sink,
                            },
                        )
                    },
                );
                Ok(())
            }

            JournalSinkAction::FlushBuffers => {
                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: FlushBuffers action - starting flush"
                );

                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: FlushBuffers action - acquiring handler lock"
                );
                let handler = ctx
                    .handler
                    .as_mut()
                    .expect("handler available before cleanup");

                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: FlushBuffers action - calling handler.flush()"
                );
                match handler.flush_report().await {
                    Ok(report) => {
                        // FLOWIP-095k finalizer gate: the audit payload is an
                        // end-of-input completion statement; Truncated discards
                        // it while flush and receipt effects run for every kind.
                        let mut report =
                            apply_terminal_eof_audit_gate(report, ctx.terminal_eof_kind);
                        let mut prepared_commits = Vec::with_capacity(report.commit_receipts.len());
                        for commit in &report.commit_receipts {
                            let Some((_upstream_stage, parent_envelope)) =
                                ctx.subscription.as_ref().and_then(|subscription| {
                                    subscription.pending_receipt_envelope(
                                        commit.parent_event_id,
                                        &ctx.contract_state[..],
                                    )
                                })
                            else {
                                return Err(FsmError::HandlerError(format!(
                                    "FlushBuffers: commit receipt parent {} is not pending",
                                    commit.parent_event_id
                                ))
                                .into());
                            };
                            prepared_commits.push((parent_envelope, commit.payload.clone()));
                        }
                        report.commit_settlements().map_err(|error| {
                            FsmError::HandlerError(format!(
                                "FlushBuffers: settlement validation changed before commit: {error}"
                            ))
                        })?;

                        if let Some(payload) = report.audit_payload.take() {
                            tracing::trace!(
                                target: "flowip-080o",
                                stage_name = %ctx.stage_name,
                                "sink: FlushBuffers action - flush returned audit payload, writing delivery"
                            );
                            let writer_id = ctx.writer_id.ok_or_else(|| {
                                FsmError::HandlerError("writer_id not initialised".to_string())
                            })?;

                            let flow_ctx = FlowContext {
                                flow_name: ctx.flow_name.clone(),
                                flow_id: ctx.flow_id.to_string(),
                                stage_name: ctx.stage_name.clone(),
                                stage_id: ctx.stage_id,
                                stage_type: StageType::Sink,
                            };

                            let evt = journalled_delivery_event(
                                writer_id,
                                &ctx.receipt_destination,
                                payload,
                            )
                            .with_flow_context(flow_ctx);
                            let evt = ctx.instrumentation.capture_accounting().attach_to(evt);

                            publication::append(
                                &ctx.data_journal,
                                evt,
                                AppendOptions::default().with_capture(
                                    ctx.instrumentation.journal_capture(None, vec![(0, false)]),
                                ),
                            )
                            .await
                            .map_err(|e| {
                                FsmError::HandlerError(format!(
                                    "Failed to write delivery receipt: {e}"
                                ))
                            })?;
                        }

                        for (parent_envelope, payload) in prepared_commits {
                            journal_commit_receipt(ctx, &parent_envelope, payload).await?;
                        }
                    }
                    Err(e) => {
                        if let HandlerError::SinkOperation(error) = &e {
                            let recorded = record_sink_lifecycle_operation_failure(
                                SinkLifecycleFailureCommit {
                                    stage_id: ctx.stage_id,
                                    stage_key: &ctx.stage_name,
                                    flow_id: &ctx.flow_id.to_string(),
                                    flow_name: &ctx.flow_name,
                                    logical_destination: &ctx.receipt_destination,
                                    phase: SinkOperationPhase::Flush,
                                    error,
                                    error_journal: &ctx.error_journal,
                                    data_journal: &ctx.data_journal,
                                    instrumentation: &ctx.instrumentation,
                                },
                            )
                            .await
                            .map_err(|error| {
                                FsmError::HandlerError(format!(
                                    "Failed to record sink flush failure: {error}"
                                ))
                            })?;
                            ctx.failure_causal_event_id =
                                Some(recorded.operation.envelope.provenance.event.id);
                            ctx.failure_lifecycle_recorded = true;
                        }
                        if let Some(fatal) = e.as_fatal() {
                            record_sink_lifecycle_fatal(ctx, fatal, "flush").await?;
                        }
                        return Err(
                            FsmError::HandlerError(format!("Failed to flush: {e:?}")).into()
                        );
                    }
                }
                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: FlushBuffers action - COMPLETE (flush only)"
                );
                Ok(())
            }

            JournalSinkAction::VerifyContractsAfterFlush => {
                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: VerifyContractsAfterFlush action - acquiring subscription lock"
                );
                let maybe_subscription = ctx.subscription.take();

                if let Some(mut subscription) = maybe_subscription {
                    let mut contract_state = std::mem::take(&mut ctx.contract_state);
                    tracing::trace!(
                        target: "flowip-080o",
                        stage_name = %ctx.stage_name,
                        "sink: VerifyContractsAfterFlush action - calling authoritative check_contracts"
                    );
                    let status = subscription.check_contracts(&mut contract_state[..]).await;
                    ctx.subscription = Some(subscription);
                    ctx.contract_state = contract_state;
                    status.into_result()?;
                }

                tracing::trace!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: VerifyContractsAfterFlush action - COMPLETE"
                );
                Ok(())
            }

            JournalSinkAction::DrainWriter => {
                if ctx.failure_lifecycle_recorded {
                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        "sink failure state is drop-only; no drain callback will run"
                    );
                    return Ok(());
                }

                let stage_name = ctx.stage_name.clone();
                async {
                    tracing::trace!(
                        target: "flowip-080o",
                        stage_name = %stage_name,
                        "sink: DrainWriter action - acquiring handler lock"
                    );
                    let handler = ctx.handler.as_mut().expect("handler available before cleanup");
                    tracing::trace!(
                        target: "flowip-080o",
                        stage_name = %stage_name,
                        "sink: DrainWriter action - calling handler.drain()"
                    );
                    let drain_result = match handler.drain_report().await {
                        Ok(report) => report,
                        Err(error) => {
                            if let HandlerError::SinkOperation(operation_error) = &error {
                                let recorded = record_sink_lifecycle_operation_failure(
                                    SinkLifecycleFailureCommit {
                                        stage_id: ctx.stage_id,
                                        stage_key: &ctx.stage_name,
                                        flow_id: &ctx.flow_id.to_string(),
                                        flow_name: &ctx.flow_name,
                                        logical_destination: &ctx.receipt_destination,
                                        phase: SinkOperationPhase::Drain,
                                        error: operation_error,
                                        error_journal: &ctx.error_journal,
                                    data_journal: &ctx.data_journal,
                                        instrumentation: &ctx.instrumentation,
                                    },
                                )
                                .await
                                .map_err(|record_error| {
                                    FsmError::HandlerError(format!(
                                        "Failed to record sink drain failure: {record_error}"
                                    ))
                                })?;
                                ctx.failure_causal_event_id = Some(recorded.operation.envelope.provenance.event.id);
                                ctx.failure_lifecycle_recorded = true;
                            }
                            if let Some(fatal) = error.as_fatal() {
                                record_sink_lifecycle_fatal(ctx, fatal, "drain").await?;
                            }
                            return Err(FsmError::HandlerError(format!(
                                "Failed to drain handler: {error:?}"
                            )));
                        }
                    };

                    // FLOWIP-095k finalizer gate: the drain audit payload is an
                    // end-of-input completion statement; Truncated discards it
                    // while the drain call and receipt effects run for every kind.
                    let mut drain_result =
                        apply_terminal_eof_audit_gate(drain_result, ctx.terminal_eof_kind);
                    let mut prepared_commits =
                        Vec::with_capacity(drain_result.commit_receipts.len());
                    for commit in &drain_result.commit_receipts {
                        let Some((_upstream_stage, parent_envelope)) =
                            ctx.subscription.as_ref().and_then(|subscription| {
                                subscription.pending_receipt_envelope(
                                    commit.parent_event_id,
                                    &ctx.contract_state[..],
                                )
                            })
                        else {
                            return Err(FsmError::HandlerError(format!(
                                "DrainWriter: commit receipt parent {} is not pending",
                                commit.parent_event_id
                            )));
                        };
                        prepared_commits.push((parent_envelope, commit.payload.clone()));
                    }
                    drain_result.commit_settlements().map_err(|error| {
                        FsmError::HandlerError(format!(
                            "DrainWriter: settlement validation changed before commit: {error}"
                        ))
                    })?;
                    if let Some(payload) = drain_result.audit_payload.take() {
                        tracing::trace!(
                            target: "flowip-080o",
                            stage_name = %ctx.stage_name,
                            "sink: DrainWriter action - drain returned audit payload, writing delivery"
                        );
                        let writer_id = ctx.writer_id.ok_or_else(|| {
                            FsmError::HandlerError(
                                "writer_id not initialised".to_string(),
                            )
                        })?;

                        let flow_ctx = FlowContext {
                            flow_name: ctx.flow_name.clone(),
                            flow_id: ctx.flow_id.to_string(),
                            stage_name: ctx.stage_name.clone(),
                            stage_id: ctx.stage_id,
                            stage_type: StageType::Sink,
                        };

                        let evt =
                            journalled_delivery_event(writer_id, &ctx.receipt_destination, payload)
                                .with_flow_context(flow_ctx);
                        let evt = ctx.instrumentation.capture_accounting().attach_to(evt);

                        publication::append(&ctx.data_journal, evt, AppendOptions::default().with_capture(ctx.instrumentation.journal_capture(None, vec![(0, false)]))).await.map_err(|e| {
                            FsmError::HandlerError(format!(
                                "Failed to write delivery receipt: {e}"
                            ))
                        })?;
                    }
                    for (parent_envelope, payload) in prepared_commits {
                        journal_commit_receipt(ctx, &parent_envelope, payload).await?;
                    }
                    tracing::trace!(
                        target: "flowip-080o",
                        stage_name = %stage_name,
                        "sink: DrainWriter action - handler.drain() complete"
                    );
                    tracing::trace!(
                        target: "flowip-080o",
                        stage_name = %stage_name,
                        "sink: DrainWriter action - COMPLETE"
                    );
                    Ok::<(), FsmError>(())
                }
                .await?;
                Ok(())
            }

            JournalSinkAction::Cleanup => {
                ctx.handler.take();
                ctx.subscription.take();
                if let Some(heartbeat) = ctx.heartbeat.take() {
                    heartbeat.cancel();
                }
                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Sink cleaned up resources with drop-only teardown"
                );
                Ok(())
            }

            JournalSinkAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}

fn apply_terminal_eof_audit_gate(
    mut report: SinkLifecycleReport,
    terminal_eof_kind: Option<EofKind>,
) -> SinkLifecycleReport {
    if matches!(
        terminal_eof_kind.unwrap_or(EofKind::Natural),
        EofKind::Truncated
    ) {
        report.audit_payload = None;
    }
    report
}

async fn record_sink_lifecycle_fatal<H: UnifiedSinkHandler + Send + Sync + 'static>(
    ctx: &JournalSinkResources<H>,
    fatal: &StageFatal,
    phase: &str,
) -> Result<(), FsmError> {
    let writer_id = ctx.writer_id.ok_or_else(|| {
        FsmError::HandlerError(format!("fatal sink {phase} has no stage writer id"))
    })?;
    record_stage_fatal(
        fatal,
        StageFatalCommit {
            error_journal: &ctx.error_journal,
            writer_id,
            stage_id: ctx.stage_id,
            stage_key: &ctx.stage_name,
            input_position: None,
            parent: None,
            lineage: ctx.lineage_policy,
        },
    )
    .await
    .map(|_| ())
    .map_err(|error| {
        FsmError::HandlerError(format!("Failed to record fatal sink {phase}: {error}"))
    })
}

async fn journal_commit_receipt<H: UnifiedSinkHandler + Send + Sync + 'static>(
    ctx: &mut JournalSinkResources<H>,
    parent_envelope: &DeliveredRecord<ChainPayload>,
    payload: DeliveryPayload,
) -> Result<(), FsmError> {
    let writer_id = ctx
        .writer_id
        .ok_or_else(|| FsmError::HandlerError("writer_id not initialised".to_string()))?;
    let flow_ctx = FlowContext {
        flow_name: ctx.flow_name.clone(),
        flow_id: ctx.flow_id.to_string(),
        stage_name: ctx.stage_name.clone(),
        stage_id: ctx.stage_id,
        stage_type: StageType::Sink,
    };

    let evt = journalled_delivery_event(writer_id, &ctx.receipt_destination, payload)
        .with_flow_context(flow_ctx)
        .with_causality(CausalityContext::with_parent(
            parent_envelope.envelope.provenance.event.id,
        ))
        .with_correlation_from(&parent_envelope.authored())
        .with_cycle_state_from(&parent_envelope.authored());
    let evt = evt
        .try_with_composite_activations(parent_envelope.composite_activations().to_vec())
        .map_err(|error| FsmError::HandlerError(error.to_string()))?;

    let data_journal = ctx.data_journal.clone();
    let instrumentation = ctx.instrumentation.clone();
    let parent = parent_envelope.clone();
    let mut settlement = ctx
        .subscription
        .as_mut()
        .map(|subscription| subscription.take_receipt_settlement(&mut ctx.contract_state));
    let settlement = publication::commit(async move {
        let event = super::with_committed_receipt_snapshot(evt, &instrumentation);
        let written = data_journal
            .append(
                event,
                AppendOptions::from_record(Some(&parent))?
                    .with_capture(instrumentation.journal_capture(None, vec![(1, false)])),
            )
            .await?;
        instrumentation.record_output_event(&written.authored());
        if let Some(settlement) = &mut settlement {
            if let Some((seq, event_id, vector_clock)) = settlement.record(&written.authored()) {
                instrumentation.record_receipted_position(seq.0, event_id, vector_clock);
            }
        }
        Ok(settlement)
    })
    .await
    .map_err(|error| FsmError::HandlerError(error.to_string()))?;
    if let (Some(subscription), Some(settlement)) = (ctx.subscription.as_mut(), settlement) {
        subscription.restore_receipt_settlement(&mut ctx.contract_state, settlement);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        apply_terminal_eof_audit_gate, JournalSinkContext, JournalSinkEvent, JournalSinkState,
    };
    use crate::execution::{RuntimeExecution, RuntimeMode};
    use crate::message_bus::FsmMessageBus;
    use crate::stages::common::handler_error::HandlerError;
    use crate::stages::common::handlers::{CommitReceipt, SinkHandler, SinkLifecycleReport};
    use crate::stages::common::stage_handle::FORCE_SHUTDOWN_MESSAGE;
    use crate::stages::sink::journal_sink::supervisor::JournalSinkSupervisor;
    use crate::stages::source::finite::fsm::tests::TestJournal;
    use obzenflow_core::event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload};
    use obzenflow_core::event::payloads::flow_control_payload::EofKind;
    use obzenflow_core::event::ChainEventFactory;
    use obzenflow_core::EventId;

    struct AuditSink(Arc<Probe>);
    impl std::fmt::Debug for AuditSink {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("AuditSink")
        }
    }

    impl Drop for AuditSink {
        fn drop(&mut self) {
            self.0.drops.fetch_add(1, Ordering::SeqCst);
            let instrumentation = self.0.instrumentation.lock().unwrap();
            if let Some(instrumentation) = instrumentation.as_ref() {
                self.0
                    .drop_states
                    .lock()
                    .unwrap()
                    .push(instrumentation.current_state.read().unwrap().clone());
            }
        }
    }

    #[async_trait::async_trait]
    impl SinkHandler for AuditSink {
        async fn consume(&mut self, _event: ChainEvent) -> Result<DeliveryPayload, HandlerError> {
            self.0.consumes.fetch_add(1, Ordering::SeqCst);
            self.0.consuming.notify_one();
            if self.0.block_consume.load(Ordering::SeqCst) {
                self.0.release_consume.notified().await;
            }
            Ok(DeliveryPayload::success(DeliveryMethod::Noop, None))
        }

        async fn flush(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
            self.0.flushes.fetch_add(1, Ordering::SeqCst);
            self.0.flushing.notify_one();
            if self.0.block_flush.load(Ordering::SeqCst) {
                self.0.release_flush.notified().await;
            }
            Ok(Some(DeliveryPayload::success(DeliveryMethod::Noop, None)))
        }

        async fn drain(&mut self) -> Result<Option<DeliveryPayload>, HandlerError> {
            self.0.drains.fetch_add(1, Ordering::SeqCst);
            Ok(None)
        }
    }

    use super::super::handle::{JournalSinkHandle, JournalSinkHandleExt};
    use crate::metrics::instrumentation::StageInstrumentation;
    use crate::stages::common::stage_lifecycle::{LifecycleExit, StageMilestone};
    use crate::stages::resources_builder::SubscriptionFactory;
    use crate::supervised_base::{
        ChannelBuilder, HandleBuilder, HandlerSupervisedWithExternalEvents, SupervisorHandle,
        SupervisorTaskBuilder,
    };
    use futures::FutureExt;
    use obzenflow_core::event::provenance::FlowContext;
    use obzenflow_core::event::ChainPayload;
    use obzenflow_core::journal::{journal_owner::JournalOwner, Journal};
    use obzenflow_core::{ChainEvent, FlowId, StageId};
    use std::collections::HashMap;
    use std::sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    };

    #[derive(Default)]
    struct Probe {
        instrumentation: std::sync::Mutex<Option<Arc<StageInstrumentation>>>,
        drop_states: std::sync::Mutex<Vec<String>>,
        block_consume: AtomicBool,
        consumes: AtomicUsize,
        consuming: tokio::sync::Notify,
        release_consume: tokio::sync::Notify,
        block_flush: AtomicBool,
        flushes: AtomicUsize,
        drains: AtomicUsize,
        drops: AtomicUsize,
        flushing: tokio::sync::Notify,
        release_flush: tokio::sync::Notify,
    }

    struct Fixture {
        handle: JournalSinkHandle<AuditSink>,
        data: Arc<TestJournal<ChainEvent>>,
        upstream: Arc<TestJournal<ChainEvent>>,
        upstream_id: StageId,
    }

    fn fixture(probe: Arc<Probe>) -> Fixture {
        let stage_id = StageId::new();
        let upstream_id = StageId::new();
        let upstream = Arc::new(TestJournal::new(JournalOwner::stage(upstream_id)));
        let data = Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));
        let instrumentation = Arc::new(StageInstrumentation::new());
        *probe.instrumentation.lock().unwrap() = Some(instrumentation.clone());
        let resources = super::JournalSinkResources {
            handler: Some(AuditSink(probe)),
            stage_id,
            stage_name: "audit_sink".into(),
            receipt_destination: "audit_sink".into(),
            default_delivery_method: None,
            flow_name: "projection_flow".into(),
            flow_id: FlowId::new(),
            data_journal: data.clone(),
            error_journal: Arc::new(TestJournal::new(JournalOwner::stage(stage_id))),
            effect_history: None,
            runtime_execution: RuntimeExecution::new(RuntimeMode::Live, None),
            effect_ports: Default::default(),
            effect_declarations: Vec::new(),
            bus: Arc::new(FsmMessageBus::new()),
            writer_id: None,
            lineage_policy: Default::default(),
            subscription: None,
            contract_state: Vec::new(),
            terminal_eof_kind: None,
            last_contract_check: None,
            instrumentation,
            upstream_subscription_factory: SubscriptionFactory::new(HashMap::new()).bind(&[(
                upstream_id,
                upstream.clone() as Arc<dyn Journal<ChainEvent>>,
            )]),
            control_strategy: Arc::new(
                crate::stages::common::control_strategies::JonestownSignalStrategy,
            ),
            sink_delivery_boundary: None,
            observers: Default::default(),
            processing_context: Default::default(),
            backpressure_writer: crate::backpressure::BackpressureWriter::disabled(),
            backpressure_readers: HashMap::new(),
            heartbeat: None,
            catch_up_flip: None,
            failure_lifecycle_recorded: false,
            failure_causal_event_id: None,
        };
        resources.instrumentation.bind_observations(
            resources.flow_id,
            stage_id.into(),
            &resources.runtime_execution,
        );
        let context = JournalSinkContext::new(resources);
        let supervisor = JournalSinkSupervisor::<AuditSink> {
            name: "sink_audit_sink".into(),
            stage_id,
            subscription: None,
            _marker: std::marker::PhantomData,
        };
        let (sender, receiver, watcher) = ChannelBuilder::new().build(JournalSinkState::Created);
        let wrapped = HandlerSupervisedWithExternalEvents::new(
            supervisor,
            receiver,
            watcher.clone(),
            crate::supervised_base::with_external_events::stage_commands(
                data.clone(),
                FlowContext::new("audit_sink", stage_id),
            ),
        );
        let task = SupervisorTaskBuilder::new("sink_audit_sink").spawn_handler_supervised(
            wrapped,
            JournalSinkState::Created,
            context,
        );
        let handle = HandleBuilder::new()
            .with_event_sender(sender)
            .with_state_watcher(watcher)
            .with_supervisor_task(task)
            .build_standard()
            .unwrap();
        Fixture {
            handle,
            data,
            upstream,
            upstream_id,
        }
    }

    async fn activate(fixture: &Fixture) {
        fixture.handle.initialize().await.unwrap();
        fixture
            .handle
            .wait_for_milestone(StageMilestone::Initialized)
            .await
            .unwrap();
        fixture.handle.ready().await.unwrap();
        fixture
            .handle
            .wait_for_milestone(StageMilestone::Started)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn blocked_initialisation_has_no_acknowledgement_and_duplicates_do_not_restart_it() {
        let probe = Arc::new(Probe::default());
        let fixture = fixture(probe.clone());
        let (entered, release) = fixture.data.block_next_append();
        fixture.handle.initialize().await.unwrap();
        entered.notified().await;
        assert_eq!(
            fixture.handle.current_state(),
            JournalSinkState::Initializing
        );
        assert!(fixture
            .handle
            .wait_for_milestone(StageMilestone::Initialized)
            .now_or_never()
            .is_none());
        fixture.handle.initialize().await.unwrap();
        fixture.handle.initialize().await.unwrap();
        fixture.handle.ready().await.unwrap();
        release.notify_one();
        fixture
            .handle
            .wait_for_milestone(StageMilestone::Initialized)
            .await
            .unwrap();
        fixture
            .handle
            .wait_for_milestone(StageMilestone::Started)
            .await
            .unwrap();
        fixture.handle.received_eof().await.unwrap();
        let exit = fixture.handle.wait_for_stage_exit().await;
        assert!(matches!(exit, LifecycleExit::Completed(_)), "{exit:?}");
        assert_eq!(*probe.drop_states.lock().unwrap(), ["Finalising"]);
        let rows = fixture.data.read_all_unordered().await.unwrap();
        assert_eq!(
            rows.iter()
                .filter(|row| matches!(
                    row.payload,
                    ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SupervisorRegistered { .. })
                ))
                .count(),
            1
        );
        assert_eq!(probe.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.drains.load(Ordering::SeqCst), 1);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        // An abandoned observation did not cancel initialisation. Its result is
        // retained even after the physical task has completed.
        fixture
            .handle
            .wait_for_milestone(StageMilestone::Initialized)
            .await
            .unwrap();
        fixture
            .handle
            .wait_for_milestone(StageMilestone::Started)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failure_is_visible_while_owned_flush_and_failure_publication_are_blocked() {
        let probe = Arc::new(Probe::default());
        probe.block_flush.store(true, Ordering::SeqCst);
        let fixture = fixture(probe.clone());
        activate(&fixture).await;
        fixture.handle.received_eof().await.unwrap();
        probe.flushing.notified().await;
        assert_eq!(fixture.handle.current_state(), JournalSinkState::Flushing);
        fixture.handle.begin_flush().await.unwrap();
        fixture.handle.ready().await.unwrap();
        let (publishing, release_publication) = fixture.data.block_matching_append(|event| matches!(event.payload,
            ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::StageLifecycle(
                obzenflow_core::event::payloads::execution_payload::StageLifecycleFact::Failed { .. }
            ))));
        fixture
            .handle
            .send_event(JournalSinkEvent::Error("original failure".into()))
            .await
            .unwrap();
        let failure = fixture.handle.wait_for_failure().await.unwrap();
        assert!(failure.cause.to_string().contains("original failure"));
        assert_eq!(
            fixture.handle.current_state(),
            JournalSinkState::Failing("original failure".into())
        );
        fixture
            .handle
            .send_event(JournalSinkEvent::Error(FORCE_SHUTDOWN_MESSAGE.into()))
            .await
            .unwrap();
        assert!(fixture
            .handle
            .wait_for_stage_exit()
            .now_or_never()
            .is_none());
        assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
        probe.release_flush.notify_one();
        publishing.notified().await;
        assert_eq!(
            fixture.handle.current_state(),
            JournalSinkState::Failing("original failure".into())
        );
        assert_eq!(probe.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.drains.load(Ordering::SeqCst), 0);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
        assert!(fixture
            .handle
            .wait_for_stage_exit()
            .now_or_never()
            .is_none());
        // The interrupted flush still publishes its accepted result exactly once.
        assert_eq!(
            fixture
                .data
                .read_all_unordered()
                .await
                .unwrap()
                .iter()
                .filter(|record| matches!(record.payload, ChainPayload::Delivery(_)))
                .count(),
            1
        );
        release_publication.notify_one();
        let LifecycleExit::Failed(failure) = fixture.handle.wait_for_stage_exit().await else {
            panic!("failure must survive cancellation during settlement")
        };
        assert!(failure.cause.to_string().contains("original failure"));
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture.handle.current_state(),
            JournalSinkState::Failed("original failure".into())
        );
        fixture
            .handle
            .wait_for_milestone(StageMilestone::Started)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failure_during_active_consume_keeps_the_handler_and_accepted_receipt_until_settlement()
    {
        let probe = Arc::new(Probe::default());
        probe.block_consume.store(true, Ordering::SeqCst);
        let fixture = fixture(probe.clone());
        fixture
            .upstream
            .append(
                ChainEventFactory::data_event(
                    fixture.upstream_id.into(),
                    "audit.input",
                    serde_json::json!({"item": 1}),
                ),
                Default::default(),
            )
            .await
            .unwrap();
        activate(&fixture).await;
        probe.consuming.notified().await;
        let (publishing, release) = fixture
            .data
            .block_matching_append(|event| matches!(event.payload, ChainPayload::Delivery(_)));
        fixture
            .handle
            .send_event(JournalSinkEvent::Error("consume interrupted".into()))
            .await
            .unwrap();
        let failure = fixture.handle.wait_for_failure().await.unwrap();
        assert!(failure.cause.to_string().contains("consume interrupted"));
        assert_eq!(
            fixture.handle.current_state(),
            JournalSinkState::Failing("consume interrupted".into())
        );
        assert_eq!(probe.drops.load(Ordering::SeqCst), 0);
        probe.release_consume.notify_one();
        publishing.notified().await;
        assert!(fixture
            .handle
            .wait_for_stage_exit()
            .now_or_never()
            .is_none());
        fixture
            .handle
            .send_event(JournalSinkEvent::Error(FORCE_SHUTDOWN_MESSAGE.into()))
            .await
            .unwrap();
        release.notify_one();
        assert!(matches!(
            fixture.handle.wait_for_stage_exit().await,
            LifecycleExit::Failed(_)
        ));
        assert_eq!(probe.consumes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        assert_eq!(
            fixture
                .data
                .read_all_unordered()
                .await
                .unwrap()
                .iter()
                .filter(|record| matches!(record.payload, ChainPayload::Delivery(_)))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn contract_failure_retains_its_typed_cause_before_and_after_settlement() {
        use obzenflow_core::event::payloads::execution_payload::{
            ExecutionPayload, StageLifecycleFact,
        };
        use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
        use obzenflow_core::event::types::{SeqNo, ViolationCause};
        use std::time::Duration;

        let probe = Arc::new(Probe::default());
        probe.block_flush.store(true, Ordering::SeqCst);
        let fixture = fixture(probe.clone());
        let mut eof = ChainEventFactory::eof_event(fixture.upstream_id.into(), true);
        let ChainPayload::FlowControl(FlowControlPayload::Eof { writer_seq, .. }) =
            &mut eof.payload
        else {
            unreachable!()
        };
        *writer_seq = Some(SeqNo(1));
        fixture
            .upstream
            .append(eof, Default::default())
            .await
            .unwrap();
        activate(&fixture).await;
        tokio::time::timeout(Duration::from_secs(2), probe.flushing.notified())
            .await
            .unwrap();
        let (entered, release) = fixture.data.block_matching_append(|event| {
            matches!(
                event.payload,
                ChainPayload::Execution(ExecutionPayload::StageLifecycle(
                    StageLifecycleFact::Failed { .. }
                ))
            )
        });
        probe.release_flush.notify_one();
        let expected = ViolationCause::SeqDivergence {
            advertised: Some(SeqNo(1)),
            reader: SeqNo(0),
        };
        let failure =
            tokio::time::timeout(Duration::from_secs(2), fixture.handle.wait_for_failure())
                .await
                .unwrap()
                .unwrap();
        let contract = failure
            .cause
            .contract_failure()
            .expect("retain the consumer's typed decision");
        assert_eq!(contract.upstream, fixture.upstream_id);
        assert_eq!(contract.cause, expected);
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        fixture
            .handle
            .send_event(JournalSinkEvent::Error(FORCE_SHUTDOWN_MESSAGE.into()))
            .await
            .unwrap();
        assert!(fixture
            .handle
            .wait_for_stage_exit()
            .now_or_never()
            .is_none());
        release.notify_one();
        for _ in 0..2 {
            let LifecycleExit::Failed(failure) =
                tokio::time::timeout(Duration::from_secs(2), fixture.handle.wait_for_stage_exit())
                    .await
                    .unwrap()
            else {
                panic!("contract failure must survive cancellation and repeated joins")
            };
            let contract = failure.cause.contract_failure().unwrap();
            assert_eq!(contract.upstream, fixture.upstream_id);
            assert_eq!(contract.cause, expected);
        }
        let rows = fixture.data.read_all_unordered().await.unwrap();
        assert!(rows.iter().any(|row| matches!(&row.payload,
            ChainPayload::Execution(ExecutionPayload::ContractStatus { pass: false, reason: Some(cause), .. }) if cause == &expected)));
    }

    #[tokio::test]
    async fn completion_publication_remains_finalising_until_it_settles() {
        let probe = Arc::new(Probe::default());
        probe.block_flush.store(true, Ordering::SeqCst);
        let fixture = fixture(probe.clone());
        activate(&fixture).await;
        fixture.handle.received_eof().await.unwrap();
        probe.flushing.notified().await;
        let (entered, release) = fixture.data.block_matching_append(|event| matches!(event.payload,
            ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::StageLifecycle(
                obzenflow_core::event::payloads::execution_payload::StageLifecycleFact::Completed { .. }
            ))));
        probe.release_flush.notify_one();
        entered.notified().await;
        assert_eq!(fixture.handle.current_state(), JournalSinkState::Finalising);
        assert_eq!(probe.flushes.load(Ordering::SeqCst), 1);
        assert_eq!(probe.drains.load(Ordering::SeqCst), 1);
        assert!(fixture
            .handle
            .wait_for_stage_exit()
            .now_or_never()
            .is_none());
        release.notify_one();
        let exit = fixture.handle.wait_for_stage_exit().await;
        assert!(matches!(exit, LifecycleExit::Completed(_)), "{exit:?}");
        assert_eq!(*probe.drop_states.lock().unwrap(), ["Finalising"]);
        assert_eq!(fixture.handle.current_state(), JournalSinkState::Drained);
        assert_eq!(probe.drops.load(Ordering::SeqCst), 1);
        let rows = fixture.data.read_all_unordered().await.unwrap();
        assert!(rows.iter().any(|row| row
            .envelope
            .observability
            .as_ref()
            .and_then(|packet| packet.runtime_snapshot.as_ref())
            .is_some_and(|runtime| runtime.fsm_state == "Flushing")));
        assert!(rows.iter().all(|row| row
            .envelope
            .observability
            .as_ref()
            .and_then(|packet| packet.runtime_snapshot.as_ref())
            .is_none_or(|runtime| runtime.fsm_state != "Drained")));
    }

    fn lifecycle_report(parent_event_id: EventId) -> SinkLifecycleReport {
        let mut report = SinkLifecycleReport::default();
        report.audit_payload = Some(DeliveryPayload::success(DeliveryMethod::Noop, None));
        report.commit_receipts = vec![CommitReceipt {
            parent_event_id,
            payload: DeliveryPayload::success(DeliveryMethod::Noop, None),
        }];
        report
    }

    #[test]
    fn terminal_eof_gate_only_suppresses_truncated_audits_and_never_commit_receipts() {
        for (kind, expects_audit) in [
            (EofKind::Natural, true),
            (EofKind::Poison, true),
            (EofKind::Truncated, false),
        ] {
            let parent_event_id = EventId::new();
            let report =
                apply_terminal_eof_audit_gate(lifecycle_report(parent_event_id), Some(kind));
            assert_eq!(report.audit_payload.is_some(), expects_audit, "{kind:?}");
            assert_eq!(report.commit_receipts.len(), 1, "{kind:?}");
            assert_eq!(report.commit_receipts[0].parent_event_id, parent_event_id);
        }
    }

    #[test]
    fn missing_terminal_kind_keeps_the_legacy_natural_audit_default() {
        let report = apply_terminal_eof_audit_gate(lifecycle_report(EventId::new()), None);
        assert!(report.audit_payload.is_some());
        assert_eq!(report.commit_receipts.len(), 1);
    }
}
