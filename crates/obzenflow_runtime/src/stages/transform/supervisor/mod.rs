// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Transform supervisor implementation using HandlerSupervised pattern
//!
//! Decomposed into submodules:
//! - `running.rs`  — Running state event loop
//! - `draining.rs` — Draining state event loop
//! - `tests.rs`    — All unit tests

mod draining;
mod running;
#[cfg(test)]
mod tests;

use crate::messaging::UpstreamSubscription;
use crate::stages::common::cycle_guard::CycleGuard;
use crate::stages::common::handlers::transform::traits::UnifiedTransformHandler;
use crate::stages::common::supervision::forward_control_event::forward_control_event as forward_control_event_helper;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::context::FlowContext;
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::EventEnvelope;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{TransformAction, TransformContext, TransformEvent, TransformState};

/// Supervisor for transform stages
pub(crate) struct TransformSupervisor<
    H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Data journal for chain events
    pub(crate) data_journal: Arc<dyn Journal<ChainEvent>>,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Subscription to upstream events (supervisor-owned to avoid borrow conflicts).
    pub(super) subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Supervisor-level cycle protection for backflow cycle members (FLOWIP-051l).
    pub(crate) cycle_guard: Option<CycleGuard>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: std::marker::PhantomData<H>,
}

impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for TransformSupervisor<H>
{
    type State = TransformState<H>;
    type Event = TransformEvent<H>;
    type Context = TransformContext<H>;
    type Action = TransformAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   TransformState<H>;
            event:   TransformEvent<H>;
            context: TransformContext<H>;
            action:  TransformAction<H>;
            initial: initial_state;

            state TransformState::Created {
                on TransformEvent::Initialize => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Initialized");
                        Ok(Transition {
                            next_state: TransformState::Initialized,
                            actions: vec![TransformAction::AllocateResources],
                        })
                    })
                };

                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                let failure_msg = msg.clone();
                                Ok(Transition {
                                    next_state: TransformState::Failed(failure_msg),
                                    actions: vec![
                                        TransformAction::SendFailure { message: msg },
                                        TransformAction::Cleanup,
                                    ],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Initialized {
                on TransformEvent::Ready => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Running");
                        Ok(Transition {
                            next_state: TransformState::Running,
                            actions: vec![TransformAction::PublishRunning],
                        })
                    })
                };

                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                let failure_msg = msg.clone();
                                Ok(Transition {
                                    next_state: TransformState::Failed(failure_msg),
                                    actions: vec![
                                        TransformAction::SendFailure { message: msg },
                                        TransformAction::Cleanup,
                                    ],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Running {
                // Ready can be delivered again (e.g. from wide subscriptions); ignore if already running
                on TransformEvent::Ready => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: TransformState::Running,
                            actions: vec![],
                        })
                    })
                };

                on TransformEvent::ReceivedEOF => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: TransformState::Draining,
                            actions: vec![], // Continue draining in dispatch_state
                        })
                    })
                };

                on TransformEvent::BeginDrain => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: TransformState::Draining,
                            actions: vec![], // Continue draining in dispatch_state
                        })
                    })
                };

                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: TransformState::Failed(failure_msg),
                                actions: vec![
                                    TransformAction::SendFailure { message: msg },
                                    TransformAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Draining {
                on TransformEvent::DrainComplete => |_state: &TransformState<H>, _event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Drained");
                        Ok(Transition {
                            next_state: TransformState::Drained,
                            actions: vec![
                                TransformAction::DrainHandler,
                                TransformAction::ForwardEOF,
                                TransformAction::SendCompletion,
                                TransformAction::Cleanup,
                            ],
                        })
                    })
                };

                on TransformEvent::BeginDrain => |_state: &TransformState<H>, _event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: TransformState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on TransformEvent::Error => |_state: &TransformState<H>, event: &TransformEvent<H>, ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: TransformState::Failed(failure_msg),
                                actions: vec![
                                    TransformAction::SendFailure { message: msg },
                                    TransformAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Drained {
                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                let failure_msg = msg.clone();
                                Ok(Transition {
                                    next_state: TransformState::Failed(failure_msg),
                                    actions: vec![
                                        TransformAction::SendFailure { message: msg },
                                        TransformAction::Cleanup,
                                    ],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state TransformState::Failed {
                on TransformEvent::Error => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                    let event = event.clone();
                    let state = state.clone();
                    Box::pin(async move {
                        if let TransformEvent::Error(msg) = event {
                            if matches!(state, TransformState::Failed(_)) {
                                Ok(Transition {
                                    next_state: state.clone(),
                                    actions: vec![],
                                })
                            } else {
                                Ok(Transition {
                                    next_state: TransformState::Failed(msg),
                                    actions: vec![TransformAction::Cleanup],
                                })
                            }
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            unhandled => |state: &TransformState<H>, event: &TransformEvent<H>, _ctx: &mut TransformContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "TransformSupervisor",
                        state = %state_name,
                        event = %event_name,
                        "Unhandled event in FSM - this indicates a state machine configuration error"
                    );
                    Err(obzenflow_fsm::FsmError::UnhandledEvent {
                        state: state_name,
                        event: event_name,
                    })
                })
            };
        }
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for TransformSupervisor<H>
{
    type Handler = H;

    fn writer_id(&self) -> obzenflow_core::WriterId {
        obzenflow_core::WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> TransformEvent<H> {
        TransformEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                stage_id = %self.stage_id,
                journal_error = %e,
                "Failed to write completion event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            TransformState::Created => {
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            TransformState::Initialized => {
                // Auto-transition to ready (transforms start immediately)
                Ok(EventLoopDirective::Transition(TransformEvent::Ready))
            }

            TransformState::Running => running::dispatch_running(self, state, ctx).await,

            TransformState::Draining => draining::dispatch_draining(self, state, ctx).await,

            TransformState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            TransformState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            TransformState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    ExternalEventPolicy for TransformSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, TransformState::Created) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(state, TransformState::Failed(_)) {
            None
        } else {
            Some(TransformEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// Helper methods shared by running.rs and draining.rs
// ---------------------------------------------------------------------------

impl<H: UnifiedTransformHandler + Clone + std::fmt::Debug + Send + Sync + 'static>
    TransformSupervisor<H>
{
    pub(super) async fn check_cycle_guard_data_event(
        &mut self,
        ctx: &mut TransformContext<H>,
        envelope: &mut EventEnvelope<ChainEvent>,
        upstream: Option<StageId>,
        write_error_context: &'static str,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let Some(guard) = &mut self.cycle_guard else {
            return Ok(false);
        };

        if let obzenflow_core::event::ChainEventContent::Data { .. } = &envelope.event.content {
            if let Err(error_event) = guard.check_data(&mut envelope.event) {
                let flow_context = FlowContext {
                    flow_name: ctx.flow_name.clone(),
                    flow_id: ctx.flow_id.to_string(),
                    stage_name: ctx.stage_name.clone(),
                    stage_id: self.stage_id,
                    stage_type: obzenflow_core::event::context::StageType::Transform,
                };

                let error_event = (*error_event)
                    .with_flow_context(flow_context)
                    .with_runtime_context(ctx.instrumentation.snapshot_with_control());

                ctx.error_journal
                    .append(error_event, Some(envelope))
                    .await
                    .map_err(|e| format!("{write_error_context}: {e}"))?;

                ctx.instrumentation.record_error(
                    obzenflow_core::event::status::processing_status::ErrorKind::Unknown,
                );

                if let Some(upstream) = upstream {
                    if let Some(reader) = ctx.backpressure_readers.get(&upstream) {
                        reader.ack_consumed(1);
                    }
                }

                return Ok(true);
            }
        }

        Ok(false)
    }

    pub(super) async fn forward_control_event_guarded(
        &mut self,
        envelope: &EventEnvelope<ChainEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let should_forward = self
            .cycle_guard
            .as_mut()
            .map(|guard| guard.should_forward_signal(&envelope.event))
            .unwrap_or(true);

        if should_forward {
            self.forward_control_event(envelope).await?;
        }

        Ok(())
    }

    pub(super) async fn maybe_release_buffered_terminal(
        &mut self,
        ctx: &mut TransformContext<H>,
    ) -> Result<
        Option<EventLoopDirective<TransformEvent<H>>>,
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let Some(cfg) = ctx.cycle_guard_config.as_ref() else {
            return Ok(None);
        };
        if !cfg.is_entry_point {
            return Ok(None);
        }

        let Some(buffered) = ctx.buffered_terminal_envelope.take() else {
            return Ok(None);
        };

        if cfg.scc_internal_edges.is_empty() {
            tracing::error!(
                stage_name = %ctx.stage_name,
                scc_id = %cfg.scc_id,
                "Cycle entry point has no scc_internal_edges; refusing to release terminal"
            );
            ctx.buffered_terminal_envelope = Some(buffered);
            return Ok(None);
        }

        let terminal_ready = cfg
            .external_upstreams
            .is_subset(&ctx.external_eofs_received)
            || ctx.drain_received;
        if !terminal_ready {
            ctx.buffered_terminal_envelope = Some(buffered);
            return Ok(None);
        }

        for &(upstream, downstream) in &cfg.scc_internal_edges {
            match ctx
                .backpressure_registry
                .edge_in_flight(upstream, downstream)
            {
                Some(0) => continue,
                Some(_) => {
                    ctx.buffered_terminal_envelope = Some(buffered);
                    return Ok(None);
                }
                None => {
                    tracing::error!(
                        stage_name = %ctx.stage_name,
                        scc_id = %cfg.scc_id,
                        upstream = ?upstream,
                        downstream = ?downstream,
                        "SCC-internal edge has no backpressure tracking; refusing to release terminal"
                    );
                    ctx.buffered_terminal_envelope = Some(buffered);
                    return Ok(None);
                }
            }
        }

        tracing::info!(
            stage_name = %ctx.stage_name,
            scc_id = %cfg.scc_id,
            "Cycle entry point releasing buffered terminal signal (SCC quiescent)"
        );

        self.forward_control_event_guarded(&buffered).await?;
        Ok(Some(EventLoopDirective::Transition(
            TransformEvent::ReceivedEOF,
        )))
    }

    /// Helper to forward control events
    pub(super) async fn forward_control_event(
        &self,
        envelope: &EventEnvelope<ChainEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _ = forward_control_event_helper(
            envelope,
            self.stage_id,
            &format!("{}", self.stage_id),
            obzenflow_core::event::context::StageType::Transform,
            &self.data_journal,
        )
        .await?;
        Ok(())
    }
}
