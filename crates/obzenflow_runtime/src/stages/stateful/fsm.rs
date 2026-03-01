// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful stage FSM types and state machine definition
//!
//! Stateful stages maintain state across events, enabling aggregations,
//! windowing operations, and session tracking.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::event_envelope::EventEnvelope;
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::{ChainEvent, FlowId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::backpressure::{BackpressureReader, BackpressureWriter};
use crate::messaging::upstream_subscription::{ContractConfig, ContractsWiring, ReaderProgress};
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::ControlEventStrategy;
use crate::stages::common::handlers::StatefulHandler;
use crate::stages::common::supervision::lifecycle_actions;
use crate::stages::resources_builder::BoundSubscriptionFactory;
use crate::supervised_base::idle_backoff::IdleBackoff;

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
            Self::Failed(msg) => write!(f, "Failed({msg:?})"),
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
            Self::Error(msg) => write!(f, "Error({msg:?})"),
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

    /// Send failure event to journal with metrics
    SendFailure { message: String },

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
            Self::SendFailure { message } => Self::SendFailure {
                message: message.clone(),
            },
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
            Self::SendFailure { message } => write!(f, "SendFailure({message:?})"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

// ============================================================================
// FSM Context
// ============================================================================

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PendingTransition {
    EmitComplete,
    DrainComplete,
}

/// Context for stateful handlers - contains everything actions need
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

    /// Current accumulated state (FSM-owned, mutated via supervisor)
    pub current_state: H::State,

    /// Data journal for writing chain events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this stateful stage (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// Subscription to upstream events
    pub subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// FSM-owned contract state for each upstream reader (aligned with subscription readers)
    pub contract_state: Vec<ReaderProgress>,

    /// Supervisor-driven contract-check tick (FLOWIP-080r).
    ///
    /// This avoids starvation under sustained load by allowing the supervisor
    /// to schedule contract checks from the active `PollResult::Event` path.
    pub(crate) last_contract_check: Option<tokio::time::Instant>,

    /// Control event handling strategy
    pub control_strategy: Arc<dyn ControlEventStrategy>,

    /// EOF event to forward when draining completes
    pub buffered_eof: Option<ChainEvent>,

    /// Last upstream envelope consumed by this stage (with a vector-clock merged across all
    /// consumed inputs).
    ///
    /// Used as the parent for emitted aggregate events so their journal envelopes preserve
    /// happened-before relationships via vector clock propagation, even when upstream events are
    /// concurrent.
    pub last_consumed_envelope: Option<EventEnvelope<ChainEvent>>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Bound subscription factory for this stage's upstream journals
    pub upstream_subscription_factory: BoundSubscriptionFactory,

    /// Counter of accumulated events since the last observability heartbeat
    /// (FLOWIP-059 Phase 6.4 - accumulator heartbeats).
    pub events_since_last_heartbeat: u64,

    /// Baseline for supervisor-driven `emit_interval` timing (FLOWIP-086h).
    pub last_data_event_time: Option<Instant>,

    /// Optional supervisor-driven emit interval for timer-driven emission while idle (FLOWIP-086h).
    pub emit_interval: Option<Duration>,

    /// Backpressure writer handle for this stage's journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Backpressure readers keyed by upstream stage ID (FLOWIP-086k).
    pub backpressure_readers: HashMap<StageId, BackpressureReader>,

    /// Pending data outputs blocked on downstream credits (Phase 1: bounded to one input).
    pub(crate) pending_outputs: VecDeque<ChainEvent>,

    /// Pending state transition once blocked outputs are fully written.
    pub(crate) pending_transition: Option<PendingTransition>,

    /// Upstream stage awaiting a consumption ack once pending outputs are drained.
    pub(crate) pending_ack_upstream: Option<StageId>,

    /// Backpressure activity pulse accumulator (Hz UI animation driver).
    pub(crate) backpressure_pulse: BackpressureActivityPulse,

    /// Backoff for blocked output writes (1ms → … → 50ms cap).
    pub(crate) backpressure_backoff: IdleBackoff,
}

impl<H: StatefulHandler + 'static> FsmContext for StatefulContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: StatefulHandler + Send + Sync + 'static> FsmAction for StatefulAction<H> {
    type Context = StatefulContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            StatefulAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);

                // Initialize FSM-owned contract state for each upstream reader
                let upstream_ids = ctx.upstream_subscription_factory.upstream_stage_ids();
                ctx.contract_state = upstream_ids.into_iter().map(ReaderProgress::new).collect();

                // Create subscription using bound factory with contracts
                if !ctx.upstream_subscription_factory.is_empty() {
                    let subscription = ctx
                        .upstream_subscription_factory
                        .build_with_contracts(ContractsWiring {
                            writer_id,
                            contract_journal: ctx.data_journal.clone(),
                            config: ContractConfig::default(),
                            system_journal: Some(ctx.system_journal.clone()),
                            reader_stage: Some(ctx.stage_id),
                            control_middleware: ctx.instrumentation.control_middleware().clone(),
                            include_delivery_contract: false,
                            cycle_guard_config: None,
                        })
                        .await
                        .map_err(|e| {
                            obzenflow_fsm::FsmError::HandlerError(format!(
                                "Failed to create subscription: {e}"
                            ))
                        })?;

                    ctx.subscription = Some(subscription);

                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        upstream_count = ctx.upstream_subscription_factory.upstream_stage_ids().len(),
                        "Created subscription using bound factory"
                    );
                } else {
                    tracing::info!(
                        stage_name = %ctx.stage_name,
                        "No upstream journals - skipping subscription creation"
                    );
                }

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage allocated resources"
                );
                Ok(())
            }

            StatefulAction::InitializeState => {
                // State is initialized when building StatefulContext.
                // This action is a placeholder for future checkpoint/resume functionality
                tracing::debug!(
                    stage_name = %ctx.stage_name,
                    "Stateful stage initialized state"
                );
                Ok(())
            }

            StatefulAction::PublishRunning => {
                lifecycle_actions::publish_running_best_effort(
                    "Stateful",
                    ctx.stage_id,
                    &ctx.stage_name,
                    &ctx.system_journal,
                )
                .await;
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
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to forward EOF".to_string(),
                    )
                })?;

                // Always emit an EOF authored by this stage, preserving upstream
                // metadata (vector clock, last_event_id, writer_seq) when present.
                let buffered = ctx.buffered_eof.take();
                let mut natural = true;
                let mut upstream_vector_clock = None;
                let mut upstream_last_event = None;
                let runtime_context = ctx.instrumentation.snapshot_with_control();

                if let Some(buffered_event) = buffered {
                    if let obzenflow_core::event::ChainEventContent::FlowControl(
                        FlowControlPayload::Eof {
                            natural: n,
                            writer_seq: _,
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

                let mut eof_event = ChainEventFactory::eof_event(writer_id, natural);

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
                    *eof_writer = Some(writer_id);
                    *writer_seq = Some(SeqNo(runtime_context.writer_seq));
                    if let Some(vc) = upstream_vector_clock {
                        *vector_clock = Some(vc);
                    }
                    *last_event_id = upstream_last_event.or(runtime_context.last_emitted_event_id);
                }

                // Attach flow/runtime context for downstream contract tracking
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::Stateful,
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
                    "Stateful stage forwarded EOF downstream"
                );
                Ok(())
            }

            StatefulAction::SendCompletion => {
                lifecycle_actions::send_completion_best_effort(
                    "Stateful",
                    ctx.stage_id,
                    &ctx.stage_name,
                    &ctx.system_journal,
                    &ctx.data_journal,
                    Some(&ctx.error_journal),
                    ctx.instrumentation.as_ref(),
                )
                .await;
                Ok(())
            }

            StatefulAction::SendFailure { message } => {
                lifecycle_actions::send_failure_best_effort(
                    "Stateful",
                    ctx.stage_id,
                    &ctx.stage_name,
                    message,
                    &ctx.system_journal,
                    &ctx.data_journal,
                    Some(&ctx.error_journal),
                    ctx.instrumentation.as_ref(),
                )
                .await;
                Ok(())
            }

            StatefulAction::Cleanup => {
                lifecycle_actions::cleanup_best_effort("Stateful", &ctx.stage_name, || async {
                    Ok::<(), ()>(())
                })
                .await;
                Ok(())
            }

            StatefulAction::_Phantom(_) => unreachable!("PhantomData variant"),
        }
    }
}
