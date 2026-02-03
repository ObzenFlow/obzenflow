// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform stage FSM types and state machine definition
//!
//! Transforms process events from upstream stages and emit transformed events.
//! They start processing immediately without waiting for a start signal.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::SeqNo;
use obzenflow_core::event::{ChainEventFactory, SystemEvent};
use obzenflow_core::journal::Journal;
use obzenflow_core::StageId;
use obzenflow_core::{ChainEvent, EventEnvelope, FlowId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::backpressure::{BackpressureReader, BackpressureWriter};
use crate::messaging::upstream_subscription::{ContractConfig, ReaderProgress};
use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::{snapshot_stage_metrics, StageInstrumentation};
use crate::metrics::tail_read;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::ControlEventStrategy;
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::stage_handle::{
    FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};
use crate::stages::resources_builder::BoundSubscriptionFactory;
use crate::supervised_base::idle_backoff::IdleBackoff;

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
            Self::Failed(msg) => write!(f, "Failed({msg:?})"),
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
            Self::Error(msg) => write!(f, "Error({msg:?})"),
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
pub(crate) enum TransformAction<H> {
    /// Allocate resources (writer ID, subscriptions)
    AllocateResources,

    /// Publish running event to journal
    PublishRunning,

    /// Forward EOF event downstream
    ForwardEOF,

    /// Drain the handler after the upstream subscription queue has been drained.
    DrainHandler,

    /// Send completion event to journal
    SendCompletion,

    /// Send failure event to journal with metrics
    SendFailure { message: String },

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
            Self::DrainHandler => Self::DrainHandler,
            Self::SendCompletion => Self::SendCompletion,
            Self::SendFailure { message } => Self::SendFailure {
                message: message.clone(),
            },
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
            Self::DrainHandler => write!(f, "DrainHandler"),
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

/// Context for transform handlers - contains everything actions need
pub(crate) struct TransformContext<H: UnifiedTransformHandler> {
    /// The handler instance (owned - allows `drain(&mut self)` without locks)
    pub handler: H,

    /// This transform's stage ID
    pub stage_id: obzenflow_core::StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Data journal for writing chain events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Writer ID for this transform (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// Subscription to upstream events
    pub subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// FSM-owned contract state for each upstream reader (aligned with subscription readers)
    pub contract_state: Vec<ReaderProgress>,

    /// Control event handling strategy
    pub control_strategy: Arc<dyn ControlEventStrategy>,

    /// EOF event to forward when draining completes
    pub buffered_eof: Option<ChainEvent>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Bound subscription factory for this stage's upstreams
    pub upstream_subscription_factory: BoundSubscriptionFactory,

    /// Backpressure writer handle for this stage's journal (FLOWIP-086k).
    pub backpressure_writer: BackpressureWriter,

    /// Backpressure readers keyed by upstream stage ID (FLOWIP-086k).
    pub backpressure_readers: HashMap<StageId, BackpressureReader>,

    /// Pending data outputs blocked on downstream credits (Phase 1: bounded to one input).
    pub(crate) pending_outputs: VecDeque<ChainEvent>,

    /// Parent envelope for pending outputs (input that produced them).
    pub(crate) pending_parent: Option<EventEnvelope<ChainEvent>>,

    /// Upstream stage awaiting a consumption ack once pending outputs are drained.
    pub(crate) pending_ack_upstream: Option<StageId>,

    /// Backpressure activity pulse accumulator (Hz UI animation driver).
    pub(crate) backpressure_pulse: BackpressureActivityPulse,

    /// Backoff for blocked output writes (1ms → … → 50ms cap).
    pub(crate) backpressure_backoff: IdleBackoff,
}

impl<H: UnifiedTransformHandler + 'static> FsmContext for TransformContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

#[async_trait::async_trait]
impl<H: UnifiedTransformHandler + Send + Sync + 'static> FsmAction for TransformAction<H> {
    type Context = TransformContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            TransformAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);

                // Initialize FSM-owned contract state for each upstream reader
                let upstream_ids = ctx.upstream_subscription_factory.upstream_stage_ids();
                ctx.contract_state = upstream_ids.into_iter().map(ReaderProgress::new).collect();

                // Build subscription from bound factory (with contracts)
                let subscription = ctx
                    .upstream_subscription_factory
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
                            "Failed to create subscription: {e}"
                        ))
                    })?;

                ctx.subscription = Some(subscription);

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    upstream_count = ctx.upstream_subscription_factory.upstream_stage_ids().len(),
                    "Transform allocated resources and created subscription"
                );
                Ok(())
            }

            TransformAction::PublishRunning => {
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
                    "Transform published running event"
                );
                Ok(())
            }

            TransformAction::DrainHandler => {
                let handler = &mut ctx.handler;
                handler.drain().await.map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to drain transform handler: {e:?}"
                    ))
                })?;
                Ok(())
            }

            TransformAction::ForwardEOF => {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to forward EOF".to_string(),
                    )
                })?;

                // Preserve metadata from the buffered EOF (if any) but always emit
                // an EOF that is authored by this stage.
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
                    stage_type: StageType::Transform,
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
                    "Transform forwarded EOF downstream"
                );
                Ok(())
            }

            TransformAction::SendCompletion => {
                // Write completion event to system journal with tail-read metrics.
                //
                // Some stages may legitimately complete without emitting any runtime-context
                // bearing events (e.g. zero input / filtered streams). In that case, fall back
                // to a best-effort snapshot from instrumentation instead of failing completion.
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
                    "Transform sent completion event"
                );
                Ok(())
            }

            TransformAction::SendFailure { message } => {
                // Write failure event to system journal with tail-read metrics.
                // If no runtime_context is available in the journals at failure
                // time, fall back to a best-effort snapshot from instrumentation
                // rather than failing the failure path and emitting nothing.
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

                let cancel_reason = match message.as_str() {
                    FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP => Some(STOP_REASON_USER_STOP),
                    STOP_REASON_TIMEOUT => Some(STOP_REASON_TIMEOUT),
                    _ => None,
                };

                let system_event = if let Some(reason) = cancel_reason {
                    SystemEvent::stage_cancelled_with_metrics(
                        ctx.stage_id,
                        reason.to_string(),
                        metrics,
                    )
                } else {
                    SystemEvent::stage_failed_with_metrics(
                        ctx.stage_id,
                        message.clone(),
                        false, // not recoverable
                        metrics,
                    )
                };

                match ctx.system_journal.append(system_event, None).await {
                    Ok(_) => {
                        if let Some(reason) = cancel_reason {
                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                reason = %reason,
                                "Transform stage cancelled"
                            );
                        } else {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = %message,
                                "Transform stage encountered error"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            stage_name = %ctx.stage_name,
                            error = %message,
                            journal_error = %e,
                            "Transform stage encountered error but failed to write error event"
                        );
                    }
                }

                Ok(())
            }

            TransformAction::Cleanup => {
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
