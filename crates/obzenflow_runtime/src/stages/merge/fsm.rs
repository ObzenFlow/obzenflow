// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FSM for merge stages. A `then_live` merge reads its batch producers first and
//! polls its live producers only after every batch producer authored natural EOF.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use obzenflow_core::event::context::StageType;
use obzenflow_core::event::payloads::flow_control_payload::{EofKind, FlowControlPayload};
use obzenflow_core::event::provenance::FlowContext;
use obzenflow_core::event::{ChainEventFactory, ChainPayload};
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, FlowId, ReaderGeneration, StageId, WriterId};
use obzenflow_fsm::{FsmAction, FsmContext};

use crate::backpressure::{BackpressureReader, BackpressureWriter, StallEpisode};
use crate::feed_plan::StageOutputContract;
use crate::messaging::upstream_subscription::{ContractConfig, ContractsWiring, ReaderProgress};
use crate::messaging::{DeliveredRecord, UpstreamSubscription};
use crate::metrics::instrumentation::StageInstrumentation;
use crate::stages::common::backpressure_activity_pulse::BackpressureActivityPulse;
use crate::stages::common::control_strategies::{ProcessingContext, SignalGate};
use crate::stages::common::heartbeat::HeartbeatHandle;
use crate::stages::common::supervision::backpressure_drain::PendingOutput;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::lifecycle_actions;
use crate::stages::merges::MergeStrategyKind;
use crate::stages::resources_builder::BoundSubscriptionFactory;
use crate::supervised_base::handler_supervised::SupervisorAction;

/// FSM states for merge stages.
#[derive(Clone, Debug, Default, PartialEq, obzenflow_fsm::StateVariant)]
pub enum MergeState {
    #[default]
    Created,
    Initializing,
    Initialized,
    Starting,
    /// Reads only the batch producers; the live producers wait in their
    /// journals and their edges are renewed as held.
    Batch,
    /// Every batch producer authored natural EOF; reads the live producers.
    Live,
    Draining,
    Finalising,
    Drained,
    Failing(String),
    Failed(String),
    Cancelling(String),
    Cancelled(String),
}

impl MergeState {
    pub(crate) fn failure(cause: String) -> Self {
        use crate::stages::common::stage_handle::{
            FORCE_SHUTDOWN_MESSAGE, STOP_REASON_TIMEOUT, STOP_REASON_USER_STOP,
        };
        match cause.as_str() {
            FORCE_SHUTDOWN_MESSAGE | STOP_REASON_USER_STOP | STOP_REASON_TIMEOUT => {
                Self::Cancelling(cause)
            }
            _ => Self::Failing(cause),
        }
    }

    pub(crate) fn lifecycle_phase(&self) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        use crate::stages::common::stage_lifecycle::LifecyclePhase as Phase;
        match self {
            Self::Initializing => Phase::Initializing,
            Self::Initialized => Phase::Initialized,
            Self::Batch | Self::Live => Phase::Active,
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

/// Events that trigger merge state transitions.
#[derive(Clone, Debug, obzenflow_fsm::EventVariant)]
pub enum MergeEvent {
    Initialize,
    InitializationCompleted,
    Ready,
    ActivationCompleted,
    /// Every batch producer authored natural EOF.
    BatchCompleted,
    /// Every batch producer ended, not all naturally; the merge drains without
    /// reading its live producers.
    BatchLeftOpen,
    /// The live subscription ended.
    ReceivedEOF,
    BeginDrain,
    DrainComplete,
    FinalisationCompleted,
    TerminationSettled,
    Error(String),
}

impl crate::supervised_base::with_external_events::ExternalControlEvent for MergeEvent {
    fn discard_details(
        &self,
    ) -> (
        obzenflow_core::event::CommandDiscardDisposition,
        Option<String>,
    ) {
        crate::stages::common::stage_handle::discarded_control_details(match self {
            Self::Error(message) => Some(message.as_str()),
            _ => None,
        })
    }
}

/// Actions that merge FSM transitions can emit.
#[derive(Clone, Debug)]
pub enum MergeAction {
    Host(SupervisorAction<MergeEvent>),
    AllocateResources,
    PublishRunning,
    /// Author this stage's own EOF, with the worst kind among its inputs.
    ForwardEOF,
    SendCompletion,
    SendFailure {
        message: String,
    },
    Cleanup,
}

/// Everything merge actions and dispatch need.
pub struct MergeContext {
    pub(crate) stage_id: StageId,
    pub(crate) stage_name: String,
    pub(crate) flow_name: String,
    pub(crate) flow_id: FlowId,
    pub(crate) strategy: MergeStrategyKind,
    pub(crate) writer_id: Option<WriterId>,
    pub(crate) runtime_execution: crate::execution::RuntimeExecution,
    pub(crate) lineage_policy: obzenflow_core::config::LineagePolicy,
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,
    pub(crate) instrumentation: Arc<StageInstrumentation>,
    pub(crate) control_strategy: Arc<dyn SignalGate>,
    pub(crate) processing_context: ProcessingContext,

    pub(crate) batch_subscription_factory: BoundSubscriptionFactory,
    pub(crate) live_subscription_factory: BoundSubscriptionFactory,
    pub(crate) batch_subscription: Option<UpstreamSubscription<ChainEvent>>,
    pub(crate) live_subscription: Option<UpstreamSubscription<ChainEvent>>,
    pub(crate) batch_contract_state: Vec<ReaderProgress>,
    pub(crate) batch_last_contract_check: Option<tokio::time::Instant>,
    pub(crate) live_contract_state: Vec<ReaderProgress>,
    pub(crate) live_last_contract_check: Option<tokio::time::Instant>,
    pub(crate) live_producers: Vec<StageId>,

    pub(crate) backpressure_writer: BackpressureWriter,
    pub(crate) backpressure_readers: HashMap<StageId, BackpressureReader>,
    pub(crate) output_contract: StageOutputContract,
    pub(crate) pending_outputs: VecDeque<PendingOutput>,
    pub(crate) pending_parent: Option<DeliveredRecord<ChainPayload>>,
    pub(crate) pending_ack_upstream: Option<StageId>,
    pub(crate) backpressure_pulse: BackpressureActivityPulse,
    pub(crate) backpressure_stall: StallEpisode,
    pub(crate) heartbeat: Option<HeartbeatHandle>,

    pub(crate) terminal_eof_kind: Option<EofKind>,
    pub(crate) buffered_eof: Option<ChainEvent>,
    pub(crate) drain_without_held_inputs: bool,
    pub(crate) pending_promotion: Option<ReaderGeneration>,
    pub(crate) catch_up_flip: Option<ReaderGeneration>,
}

impl FsmContext for MergeContext {}

impl MergeContext {
    pub(crate) fn flow_context(&self) -> FlowContext {
        make_flow_context(
            &self.flow_name,
            &self.flow_id.to_string(),
            &self.stage_name,
            self.stage_id,
            StageType::Merge,
        )
    }
}

#[async_trait::async_trait]
impl FsmAction for MergeAction {
    type Context = MergeContext;

    async fn execute(&self, ctx: &mut Self::Context) -> Result<(), obzenflow_fsm::FsmError> {
        match self {
            MergeAction::Host(_) => Err(obzenflow_fsm::FsmError::HandlerError(
                "host action requires the supervised runner".into(),
            )),
            MergeAction::AllocateResources => {
                let writer_id = WriterId::from(ctx.stage_id);
                ctx.writer_id = Some(writer_id);
                ctx.batch_contract_state = ctx
                    .batch_subscription_factory
                    .upstream_stage_ids()
                    .into_iter()
                    .map(ReaderProgress::new)
                    .collect();
                ctx.live_contract_state = ctx
                    .live_subscription_factory
                    .upstream_stage_ids()
                    .into_iter()
                    .map(ReaderProgress::new)
                    .collect();
                let wiring = |ctx: &MergeContext| ContractsWiring {
                    writer_id,
                    contract_journal: ctx.data_journal.clone(),
                    config: ContractConfig::default(),
                    reader_stage: Some(ctx.stage_id),
                    control_plane: ctx.instrumentation.control_plane().clone(),
                    include_delivery_contract: false,
                    cycle_guard_config: None,
                };
                let batch = ctx
                    .batch_subscription_factory
                    .build_with_contracts(wiring(ctx))
                    .await
                    .map_err(|error| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to create the merge's batch subscription: {error}"
                        ))
                    })?
                    .with_contract_flow_context(ctx.flow_context());
                let live = ctx
                    .live_subscription_factory
                    .build_with_contracts(wiring(ctx))
                    .await
                    .map_err(|error| {
                        obzenflow_fsm::FsmError::HandlerError(format!(
                            "Failed to create the merge's live subscription: {error}"
                        ))
                    })?
                    .with_contract_flow_context(ctx.flow_context());
                ctx.batch_subscription = Some(batch);
                ctx.live_subscription = Some(live);
                Ok(())
            }
            MergeAction::PublishRunning => {
                lifecycle_actions::publish_running(&ctx.data_journal, ctx.flow_context()).await
            }
            MergeAction::ForwardEOF => {
                let writer_id = ctx.writer_id.ok_or_else(|| {
                    obzenflow_fsm::FsmError::HandlerError(
                        "No writer ID available to author the merge's EOF".to_string(),
                    )
                })?;
                let eof_kind = ctx.terminal_eof_kind.unwrap_or(EofKind::Natural);
                let runtime_context = ctx.instrumentation.snapshot();
                let (authored_writer_seq, writer_seq_by_event_type, authored_last_event_id) =
                    ctx.instrumentation.authored_data_frontier();
                let upstream_vector_clock =
                    ctx.buffered_eof
                        .take()
                        .and_then(|buffered| match buffered.payload {
                            ChainPayload::FlowControl(FlowControlPayload::Eof {
                                vector_clock,
                                ..
                            }) => vector_clock,
                            _ => None,
                        });
                let mut eof_event = ChainEventFactory::eof_event_with_kind(writer_id, eof_kind);
                if let ChainPayload::FlowControl(FlowControlPayload::Eof {
                    writer_id: ref mut eof_writer,
                    writer_seq,
                    writer_seq_by_event_type: eof_writer_seq_by_event_type,
                    writer_seq_by_event_type_complete,
                    vector_clock,
                    last_event_id,
                    ..
                }) = &mut eof_event.payload
                {
                    *eof_writer = Some(writer_id);
                    *writer_seq = Some(authored_writer_seq);
                    *eof_writer_seq_by_event_type = writer_seq_by_event_type;
                    *writer_seq_by_event_type_complete = true;
                    if let Some(clock) = upstream_vector_clock {
                        *vector_clock = Some(clock);
                    }
                    *last_event_id = authored_last_event_id;
                }
                eof_event.flow_context = ctx.flow_context();
                eof_event.runtime = Some(runtime_context);
                crate::stages::common::supervision::output_committer::commit_control_output(
                    &ctx.data_journal,
                    &ctx.instrumentation,
                    eof_event,
                )
                .await
                .map_err(|error| {
                    obzenflow_fsm::FsmError::HandlerError(format!(
                        "Failed to author the merge's EOF: {error}"
                    ))
                })?;
                Ok(())
            }
            MergeAction::SendCompletion => {
                if let Some(heartbeat) = &ctx.heartbeat {
                    heartbeat.state.mark_completed();
                }
                lifecycle_actions::send_completion(
                    &ctx.data_journal,
                    ctx.flow_context(),
                    ctx.instrumentation.as_ref(),
                )
                .await
            }
            MergeAction::SendFailure { message } => {
                lifecycle_actions::send_failure(
                    &ctx.data_journal,
                    ctx.flow_context(),
                    message,
                    ctx.instrumentation.as_ref(),
                    None,
                )
                .await
            }
            MergeAction::Cleanup => {
                ctx.batch_subscription.take();
                ctx.live_subscription.take();
                if let Some(heartbeat) = ctx.heartbeat.take() {
                    heartbeat.cancel();
                }
                Ok(())
            }
        }
    }
}
