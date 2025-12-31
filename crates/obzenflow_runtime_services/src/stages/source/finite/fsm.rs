//! Finite source stage FSM types and state machine definition
//!
//! Finite sources eventually complete (files, bounded collections).
//! They have a unique "WaitingForGun" state that ensures they don't
//! start emitting events until the pipeline is ready.

use obzenflow_core::event::context::{FlowContext, StageType};
use obzenflow_core::event::payloads::flow_control_payload::FlowControlPayload;
use obzenflow_core::event::types::{Count, JournalIndex, JournalPath, SeqNo};
use obzenflow_core::event::{ChainEventContent, ChainEventFactory, SystemEvent};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{ChainEvent, EventId, FlowId, StageId, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateVariant};
use serde::{Deserialize, Serialize};
use std::marker::PhantomData;
use std::sync::Arc;

use crate::metrics::instrumentation::{snapshot_stage_metrics, StageInstrumentation};
use crate::metrics::tail_read;
use crate::stages::common::stage_handle::{
    FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
};
use crate::stages::source::strategies::{SourceControlContext, SourceControlStrategy};

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

    /// Publish running lifecycle event
    PublishRunning,

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
pub struct FiniteSourceContext<H> {
    /// This source's stage ID
    pub stage_id: obzenflow_core::StageId,

    /// Human-readable stage name for logging
    pub stage_name: String,

    /// Flow name for flow context
    pub flow_name: String,

    /// Flow ID from pipeline
    pub flow_id: FlowId,

    /// Data journal for writing generated events
    pub data_journal: Arc<dyn Journal<ChainEvent>>,

    /// Error journal for writing error events (FLOWIP-082e)
    pub error_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for writing lifecycle events
    pub system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Message bus for pipeline communication
    pub bus: Arc<crate::message_bus::FsmMessageBus>,

    /// Writer ID for this source (initialized during setup)
    pub writer_id: Option<WriterId>,

    /// Stage instrumentation for metrics tracking
    pub instrumentation: Arc<StageInstrumentation>,

    /// Source control strategy for this stage
    pub control_strategy: Arc<dyn SourceControlStrategy>,

    /// Mutable context for the source control strategy
    pub control_context: SourceControlContext,

    /// Phantom to keep the handler type in the context's type parameters
    _marker: PhantomData<H>,
}

impl<H> FiniteSourceContext<H> {
    pub fn new(
        stage_id: obzenflow_core::StageId,
        stage_name: String,
        flow_name: String,
        flow_id: FlowId,
        data_journal: Arc<dyn Journal<ChainEvent>>,
        error_journal: Arc<dyn Journal<ChainEvent>>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
        bus: Arc<crate::message_bus::FsmMessageBus>,
        instrumentation: Arc<StageInstrumentation>,
        control_strategy: Arc<dyn SourceControlStrategy>,
    ) -> Self {
        Self {
            stage_id,
            stage_name,
            flow_name,
            flow_id,
            data_journal,
            error_journal,
            system_journal,
            bus,
            writer_id: None,
            instrumentation,
            control_strategy,
            control_context: SourceControlContext::new(),
            _marker: PhantomData,
        }
    }
}

impl<H: Send + Sync + 'static> FsmContext for FiniteSourceContext<H> {}

// ============================================================================
// FSM Action Implementation
// ============================================================================

// Manual implementations for FiniteSourceAction
impl<H> Clone for FiniteSourceAction<H> {
    fn clone(&self) -> Self {
        match self {
            Self::AllocateResources => Self::AllocateResources,
            Self::SendEOF => Self::SendEOF,
            Self::SendError { message } => Self::SendError {
                message: message.clone(),
            },
            Self::PublishRunning => Self::PublishRunning,
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
            Self::PublishRunning => write!(f, "PublishRunning"),
            Self::WriteStageCompleted => write!(f, "WriteStageCompleted"),
            Self::Cleanup => write!(f, "Cleanup"),
            Self::_Phantom(_) => write!(f, "_Phantom"),
        }
    }
}

#[async_trait::async_trait]
impl<H: Send + Sync + 'static> FsmAction for FiniteSourceAction<H> {
    type Context = FiniteSourceContext<H>;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            FiniteSourceAction::AllocateResources => {
                // Create WriterId from our StageId
                let writer_id = WriterId::from(ctx.stage_id.clone());
                ctx.writer_id = Some(writer_id.clone());

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    writer_id = %writer_id,
                    "Finite source allocated resources and registered writer"
                );
                Ok(())
            }

            FiniteSourceAction::SendEOF => {
                // Consult the source control strategy (FLOWIP-081a / 051b).
                let decision = ctx
                    .control_strategy
                    .on_natural_completion(&mut ctx.control_context);

                let writer_id = ctx.writer_id.as_ref().ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to send EOF".to_string(),
                    )
                })?;

                // Snapshot how many events this source emitted
                let emitted = ctx
                    .instrumentation
                    .events_processed_total
                    .load(std::sync::atomic::Ordering::Relaxed);

                // Determine whether EOF should be natural or "poison"
                let natural = match decision {
                    crate::stages::source::strategies::SourceShutdownDecision::PoisonEof => false,
                    _ => true,
                };

                // Take a final runtime snapshot for wide-event semantics
                let runtime_context = ctx.instrumentation.snapshot_with_control();

                // Emit EOF with writer positions populated
                let mut eof_event = ChainEventFactory::eof_event(writer_id.clone(), natural);
                if let ChainEventContent::FlowControl(FlowControlPayload::Eof {
                    writer_id: writer_id_field,
                    writer_seq,
                    ..
                }) = &mut eof_event.content
                {
                    *writer_id_field = Some(writer_id.clone());
                    *writer_seq = Some(SeqNo(emitted));
                }

                // Attach flow/runtime context for downstream consumers
                eof_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::FiniteSource,
                };
                eof_event.runtime_context = Some(runtime_context.clone());

                // Emit consumption_final for the source itself (writer-side contract)
                let mut final_event = ChainEventFactory::consumption_final_event(
                    writer_id.clone(),
                    true, // pass because sources are authoritative on what they wrote
                    Count(emitted),
                    None, // expected_count unknown until config plumbing (010)
                    true, // eof_seen
                    None, // last_event_id unknown here
                    SeqNo(emitted),
                    Some(SeqNo(emitted)), // advertised writer seq = what we wrote
                    None,                 // vector clock unavailable here
                    None,                 // no failure reason
                );

                final_event.flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: ctx.stage_id,
                    stage_type: StageType::FiniteSource,
                };
                final_event.runtime_context = Some(runtime_context);

                ctx.data_journal
                    .append(eof_event, None)
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!("Failed to send EOF: {e}"))
                    })?;

                ctx.data_journal
                    .append(final_event, None)
                    .await
                    .map_err(|e| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to send source consumption_final: {e}"
                        ))
                    })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    emitted,
                    "Finite source sent EOF and consumption_final"
                );
                Ok(())
            }

            FiniteSourceAction::SendError { message } => {
                // Tail-read metrics for failure; if no runtime_context is present
                // in the journals, fall back to a best-effort snapshot from
                // instrumentation rather than failing the failure path.
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

                // Best-effort: log journal failures but don't fail the FSM
                match ctx.system_journal.append(system_event, None).await {
                    Ok(_) => {
                        if let Some(reason) = cancel_reason {
                            tracing::info!(
                                stage_name = %ctx.stage_name,
                                reason = %reason,
                                "Finite source cancelled"
                            );
                        } else {
                            tracing::error!(
                                stage_name = %ctx.stage_name,
                                error = %message,
                                "Finite source encountered error"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::error!(
                            stage_name = %ctx.stage_name,
                            error = %message,
                            journal_error = %e,
                            "Finite source encountered error but failed to write error event"
                        );
                    }
                }
                Ok(())
            }

            FiniteSourceAction::PublishRunning => {
                let writer_id = match ctx.writer_id.as_ref() {
                    Some(id) => id.clone(),
                    None => {
                        tracing::warn!(
                            stage_name = %ctx.stage_name,
                            "No writer ID available to publish running event; skipping running event and source_contract"
                        );
                        return Ok(());
                    }
                };

                // Write running event to system journal
                let running_event = SystemEvent::stage_running(ctx.stage_id);

                if let Err(e) = ctx.system_journal.append(running_event, None).await {
                    tracing::error!(
                        stage_name = %ctx.stage_name,
                        journal_error = %e,
                        "Failed to publish running event; continuing without system journal entry"
                    );
                }

                // Emit writer-side source contract with runtime defaults (expected_count unknown)
                let contract = ChainEventFactory::source_contract_event(
                    writer_id.clone(),
                    None, // expected_count unknown until 010 config plumbing
                    ctx.stage_id,
                    None, // route not available here
                    JournalPath(ctx.stage_id.to_string()),
                    JournalIndex(0),
                    None, // writer_seq unknown at start
                    None, // vector clock not captured at start
                );

                ctx.data_journal.append(contract, None).await.map_err(|e| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to append source_contract: {e}"
                    ))
                })?;

                tracing::info!(
                    stage_name = %ctx.stage_name,
                    "Finite source published running event and source_contract"
                );
                Ok(())
            }

            FiniteSourceAction::WriteStageCompleted => {
                // Write completion event to system journal with tail-read metrics.
                // If no runtime_context is available in the journals at completion
                // time, treat this as a hard error instead of fabricating metrics.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message_bus::FsmMessageBus;
    use crate::metrics::instrumentation::StageInstrumentation;
    use async_trait::async_trait;
    use obzenflow_core::event::event_envelope::EventEnvelope;
    use obzenflow_core::event::identity::JournalWriterId;
    use obzenflow_core::event::journal_event::JournalEvent;
    use obzenflow_core::event::system_event::SystemEvent;
    use obzenflow_core::id::JournalId;
    use obzenflow_core::journal::journal::Journal;
    use obzenflow_core::journal::journal_error::JournalError;
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::journal_reader::JournalReader;
    use obzenflow_core::StageId as CoreStageId;
    use std::sync::atomic::AtomicU8;
    use std::sync::{Arc, Mutex};

    use crate::stages::common::handlers::FiniteSourceHandler as TestFiniteSourceHandler;
    use crate::stages::source::strategies::CircuitBreakerSourceStrategy;

    /// Minimal in-memory journal for tests
    struct TestJournal<T: JournalEvent> {
        id: JournalId,
        owner: Option<JournalOwner>,
        events: Arc<Mutex<Vec<EventEnvelope<T>>>>,
    }

    impl<T: JournalEvent> TestJournal<T> {
        fn new(owner: JournalOwner) -> Self {
            Self {
                id: JournalId::new(),
                owner: Some(owner),
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    struct TestJournalReader<T: JournalEvent> {
        events: Vec<EventEnvelope<T>>,
        pos: usize,
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> Journal<T> for TestJournal<T> {
        fn id(&self) -> &JournalId {
            &self.id
        }

        fn owner(&self) -> Option<&JournalOwner> {
            self.owner.as_ref()
        }

        async fn append(
            &self,
            event: T,
            _parent: Option<&EventEnvelope<T>>,
        ) -> Result<EventEnvelope<T>, JournalError> {
            let env = EventEnvelope::new(JournalWriterId::from(self.id), event);
            let mut guard = self.events.lock().unwrap();
            guard.push(env.clone());
            Ok(env)
        }

        async fn read_causally_ordered(&self) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(guard.clone())
        }

        async fn read_causally_after(
            &self,
            _after_event_id: &obzenflow_core::EventId,
        ) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            // Not needed for this test
            Ok(Vec::new())
        }

        async fn read_event(
            &self,
            _event_id: &obzenflow_core::EventId,
        ) -> Result<Option<EventEnvelope<T>>, JournalError> {
            // Not needed for this test
            Ok(None)
        }

        async fn reader(&self) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(Box::new(TestJournalReader {
                events: guard.clone(),
                pos: 0,
            }))
        }

        async fn reader_from(
            &self,
            position: u64,
        ) -> Result<Box<dyn JournalReader<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            Ok(Box::new(TestJournalReader {
                events: guard.clone(),
                pos: position as usize,
            }))
        }

        async fn read_last_n(&self, count: usize) -> Result<Vec<EventEnvelope<T>>, JournalError> {
            let guard = self.events.lock().unwrap();
            let len = guard.len();
            let start = len.saturating_sub(count);
            // Return most recent first to match Journal contract.
            Ok(guard[start..].iter().rev().cloned().collect())
        }
    }

    #[async_trait]
    impl<T: JournalEvent + 'static> JournalReader<T> for TestJournalReader<T> {
        async fn next(&mut self) -> Result<Option<EventEnvelope<T>>, JournalError> {
            if self.pos >= self.events.len() {
                Ok(None)
            } else {
                let env = self.events.get(self.pos).cloned();
                self.pos += 1;
                Ok(env)
            }
        }

        async fn skip(&mut self, n: u64) -> Result<u64, JournalError> {
            let start = self.pos as u64;
            self.pos = (self.pos as u64 + n) as usize;
            Ok((self.pos as u64).saturating_sub(start))
        }

        fn position(&self) -> u64 {
            self.pos as u64
        }

        fn is_at_end(&self) -> bool {
            self.pos >= self.events.len()
        }
    }

    struct DummySource;

    impl TestFiniteSourceHandler for DummySource {
        fn next(
            &mut self,
        ) -> Result<
            Option<Vec<ChainEvent>>,
            crate::stages::common::handlers::source::traits::SourceError,
        > {
            // This test source never emits data; it's only used to drive EOF behaviour
            // in combination with the control strategy and breaker state.
            Ok(None)
        }
    }

    #[tokio::test]
    async fn send_eof_uses_poison_flag_when_breaker_open() {
        // Shared setup
        let stage_id = CoreStageId::new();
        let flow_id = FlowId::new();
        let flow_name = "test_flow".to_string();
        let stage_name = "finite_source".to_string();

        let data_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));
        let error_journal: Arc<dyn Journal<ChainEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));
        let system_journal: Arc<dyn Journal<SystemEvent>> =
            Arc::new(TestJournal::new(JournalOwner::stage(stage_id)));

        let bus = Arc::new(FsmMessageBus::new());
        let instrumentation = Arc::new(StageInstrumentation::new());

        // Helper to build a fresh context with a given control strategy
        let build_ctx = |control_strategy: Arc<dyn SourceControlStrategy>| {
            FiniteSourceContext::<DummySource>::new(
                stage_id,
                stage_name.clone(),
                flow_name.clone(),
                flow_id,
                data_journal.clone(),
                error_journal.clone(),
                system_journal.clone(),
                bus.clone(),
                instrumentation.clone(),
                control_strategy,
            )
        };

        // Case 1: breaker closed -> natural EOF
        let state_closed = Arc::new(AtomicU8::new(0)); // Closed
        let mut ctx = build_ctx(Arc::new(CircuitBreakerSourceStrategy::new(
            state_closed.clone(),
        )));

        // Allocate resources to set writer_id
        FiniteSourceAction::<DummySource>::AllocateResources
            .execute(&mut ctx)
            .await
            .unwrap();

        // Pretend we emitted some data events
        ctx.instrumentation
            .events_processed_total
            .store(5, std::sync::atomic::Ordering::Relaxed);

        // Send EOF with breaker closed
        FiniteSourceAction::<DummySource>::SendEOF
            .execute(&mut ctx)
            .await
            .unwrap();

        let events_closed = data_journal.read_causally_ordered().await.unwrap();
        let eof_natural_closed = events_closed.iter().any(|env| {
            matches!(
                env.event.content,
                ChainEventContent::FlowControl(FlowControlPayload::Eof { natural: true, .. })
            )
        });
        assert!(
            eof_natural_closed,
            "Expected natural EOF when breaker is closed"
        );

        // Case 2: breaker open -> poison EOF
        let state_open = Arc::new(AtomicU8::new(1)); // Open
        let mut ctx_open = build_ctx(Arc::new(CircuitBreakerSourceStrategy::new(
            state_open.clone(),
        )));

        FiniteSourceAction::<DummySource>::AllocateResources
            .execute(&mut ctx_open)
            .await
            .unwrap();

        ctx_open
            .instrumentation
            .events_processed_total
            .store(10, std::sync::atomic::Ordering::Relaxed);

        FiniteSourceAction::<DummySource>::SendEOF
            .execute(&mut ctx_open)
            .await
            .unwrap();

        let events_open = data_journal.read_causally_ordered().await.unwrap();
        let eof_poison = events_open.iter().any(|env| {
            matches!(
                env.event.content,
                ChainEventContent::FlowControl(FlowControlPayload::Eof { natural: false, .. })
            )
        });
        assert!(
            eof_poison,
            "Expected poison EOF (natural = false) when breaker is open"
        );
    }
}
