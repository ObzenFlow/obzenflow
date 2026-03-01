// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Join supervisor implementation using the HandlerSupervised pattern

mod common;
mod draining;
mod enriching;
mod hydrating;
mod live;

use crate::messaging::UpstreamSubscription;
use crate::stages::common::handlers::JoinHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::{ChainEvent, StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;

use super::config::JoinReferenceMode;
use super::fsm::{JoinAction, JoinContext, JoinEvent, JoinState};

/// Supervisor for join stages.
pub(crate) struct JoinSupervisor<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Human-readable stage name (for logging in methods that do not see Context)
    pub(crate) stage_name: String,

    /// Subscription to reference journal events (supervisor-owned to avoid borrow conflicts).
    pub(super) reference_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Subscription to stream journal events (supervisor-owned to avoid borrow conflicts).
    pub(super) stream_subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: std::marker::PhantomData<H>,
}

impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for JoinSupervisor<H>
{
    type State = JoinState<H>;
    type Event = JoinEvent<H>;
    type Context = JoinContext<H>;
    type Action = JoinAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   JoinState<H>;
            event:   JoinEvent<H>;
            context: JoinContext<H>;
            action:  JoinAction<H>;
            initial: initial_state;

            state JoinState::Created {
                on JoinEvent::Initialize => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Initialized");
                        Ok(Transition {
                            next_state: JoinState::Initialized,
                            actions: vec![JoinAction::AllocateResources],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Initialized {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        let next_state = match ctx.reference_mode {
                            JoinReferenceMode::FiniteEof => JoinState::Hydrating,
                            JoinReferenceMode::Live => JoinState::Live,
                        };

                        let next_state_name = match ctx.reference_mode {
                            JoinReferenceMode::FiniteEof => "Hydrating",
                            JoinReferenceMode::Live => "Live",
                        };
                        ctx.instrumentation.transition_to_state(next_state_name);
                        Ok(Transition {
                            next_state,
                            actions: vec![
                                JoinAction::InitializeHandlerState,
                                JoinAction::PublishRunning,
                            ],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Hydrating {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Hydrating; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Hydrating,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::ReceivedEOF = event {
                            // Flush any pending reference-side heartbeat before
                            // transitioning to Enriching so metrics snapshots
                            // include all hydrating activity.
                            if ctx.events_since_last_heartbeat > 0 {
                                if let Err(e) =
                                    common::emit_join_heartbeat_if_due(ctx, ctx.stage_id).await
                                {
                                    tracing::warn!(
                                        stage_name = %ctx.stage_name,
                                        error = ?e,
                                        "Failed to emit final join hydration heartbeat"
                                    );
                                }
                            }

                            ctx.instrumentation.transition_to_state("Enriching");
                            Ok(Transition {
                                next_state: JoinState::Enriching,
                                actions: vec![],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Live {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Live; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Live,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::BeginDrain => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Ok(Transition {
                                next_state: JoinState::Failed(msg),
                                actions: vec![JoinAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Enriching {
                on JoinEvent::Ready => |_state: &JoinState<H>, _event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JoinSupervisor: received Ready in Enriching; treating as no-op");
                        Ok(Transition {
                            next_state: JoinState::Enriching,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::ReceivedEOF => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::BeginDrain => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: JoinState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation.failures_total.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            Ok(Transition {
                                next_state: JoinState::Failed(msg),
                                actions: vec![JoinAction::Cleanup],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Draining {
                on JoinEvent::DrainComplete => |_state: &JoinState<H>, _event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Drained");
                        Ok(Transition {
                            next_state: JoinState::Drained,
                            actions: vec![
                                JoinAction::ForwardEOF,
                                JoinAction::SendCompletion,
                                JoinAction::Cleanup,
                            ],
                        })
                    })
                };

                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Drained {
                on JoinEvent::Error => |_state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JoinState::Failed(failure_msg),
                                actions: vec![
                                    JoinAction::SendFailure { message: msg },
                                    JoinAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JoinState::Failed {
                on JoinEvent::Error => |state: &JoinState<H>, event: &JoinEvent<H>, ctx: &mut JoinContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let JoinEvent::Error(_msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            Ok(Transition {
                                next_state: state,
                                actions: vec![],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            unhandled => |state: &JoinState<H>, event: &JoinEvent<H>, _ctx: &mut JoinContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "JoinSupervisor",
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
impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for JoinSupervisor<H>
{
    type Handler = H;

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> JoinEvent<H> {
        JoinEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::stage_completed(self.stage_id);
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                stage_name = %self.stage_name,
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
        tracing::debug!(
            stage_name = %ctx.stage_name,
            state = ?state,
            "Join dispatch_state"
        );

        match state {
            JoinState::Created => Ok(EventLoopDirective::Continue),
            JoinState::Initialized => Ok(EventLoopDirective::Transition(JoinEvent::Ready)),
            JoinState::Hydrating => hydrating::dispatch_hydrating(self, state, ctx).await,
            JoinState::Enriching => enriching::dispatch_enriching(self, state, ctx).await,
            JoinState::Live => live::dispatch_live(self, ctx).await,
            JoinState::Draining => draining::dispatch_draining(self, state, ctx).await,
            JoinState::Drained | JoinState::Failed(_) => Ok(EventLoopDirective::Terminate),
            _ => Ok(EventLoopDirective::Continue),
        }
    }
}

impl<H: JoinHandler + Clone + std::fmt::Debug + Send + Sync + 'static> ExternalEventPolicy
    for JoinSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, JoinState::Created) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(state, JoinState::Failed(_)) {
            None
        } else {
            Some(JoinEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}
