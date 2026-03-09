// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Journal sink supervisor implementation using HandlerSupervised pattern

use crate::stages::common::handlers::SinkHandler;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{ExternalEventMode, ExternalEventPolicy, HandlerSupervised};
use crate::{messaging::UpstreamSubscription, supervised_base::EventLoopDirective};
use obzenflow_core::event::SystemEvent;
use obzenflow_core::journal::Journal;
use obzenflow_core::ChainEvent;
use obzenflow_core::{StageId, WriterId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::sync::Arc;

use super::fsm::{JournalSinkAction, JournalSinkContext, JournalSinkEvent, JournalSinkState};

mod running;

/// Supervisor for journal sink stages
pub(crate) struct JournalSinkSupervisor<
    H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// System journal for lifecycle events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Human-readable stage name (for logging in methods that don't see Context)
    pub(crate) stage_name: String,

    /// Upstream subscription moved off the FSM context (Phase 1b follow-up).
    ///
    /// `JournalSinkAction::AllocateResources` still creates the subscription and
    /// stores it in `ctx.subscription` as a short-lived staging slot. The first
    /// Running dispatch moves it into this supervisor-owned field.
    pub(crate) subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: std::marker::PhantomData<H>,
}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for JournalSinkSupervisor<H>
{
    type State = JournalSinkState<H>;
    type Event = JournalSinkEvent<H>;
    type Context = JournalSinkContext<H>;
    type Action = JournalSinkAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   JournalSinkState<H>;
            event:   JournalSinkEvent<H>;
            context: JournalSinkContext<H>;
            action:  JournalSinkAction<H>;
            initial: initial_state;

            state JournalSinkState::Created {
                on JournalSinkEvent::Initialize => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JournalSinkState::Initialized,
                            actions: vec![JournalSinkAction::AllocateResources],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Initialized {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JournalSinkState::Running,
                            actions: vec![JournalSinkAction::PublishRunning],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Running {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JournalSinkSupervisor: received Ready in Running; treating as no-op");
                        Ok(Transition {
                            next_state: JournalSinkState::Running,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::ReceivedEOF => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!(
                            stage_name = %ctx.stage_name,
                            "JournalSinkSupervisor: ReceivedEOF -> Drained (flush + completion + cleanup)"
                        );
                        Ok(Transition {
                            next_state: JournalSinkState::Drained,
                            actions: vec![
                                JournalSinkAction::FlushBuffers,
                                JournalSinkAction::SendCompletion,
                                JournalSinkAction::Cleanup,
                            ],
                        })
                    })
                };

                on JournalSinkEvent::BeginFlush => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: JournalSinkState::Flushing,
                            actions: vec![JournalSinkAction::FlushBuffers],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Flushing {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JournalSinkSupervisor: received Ready in Flushing; treating as no-op");
                        Ok(Transition {
                            next_state: JournalSinkState::Flushing,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::FlushComplete => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!(
                            target: "flowip-080o",
                            "JournalSinkSupervisor: FlushComplete -> Draining"
                        );
                        Ok(Transition {
                            next_state: JournalSinkState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Draining {
                on JournalSinkEvent::Ready => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!("JournalSinkSupervisor: received Ready in Draining; treating as no-op");
                        Ok(Transition {
                            next_state: JournalSinkState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on JournalSinkEvent::BeginDrain => |_state: &JournalSinkState<H>, _event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    Box::pin(async move {
                        tracing::info!(
                            target: "flowip-080o",
                            "JournalSinkSupervisor: BeginDrain -> Drained (SendCompletion + Cleanup)"
                        );
                        Ok(Transition {
                            next_state: JournalSinkState::Drained,
                            actions: vec![JournalSinkAction::SendCompletion, JournalSinkAction::Cleanup],
                        })
                    })
                };

                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Drained {
                on JournalSinkEvent::Error => |_state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: JournalSinkState::Failed(failure_msg),
                                actions: vec![
                                    JournalSinkAction::SendFailure { message: msg },
                                    JournalSinkAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state JournalSinkState::Failed {
                on JournalSinkEvent::Error => |state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let JournalSinkEvent::Error(_msg) = event {
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

            unhandled => |state: &JournalSinkState<H>, event: &JournalSinkEvent<H>, _ctx: &mut JournalSinkContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "JournalSinkSupervisor",
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
impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for JournalSinkSupervisor<H>
{
    type Handler = H;

    fn writer_id(&self) -> WriterId {
        WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> JournalSinkEvent<H> {
        JournalSinkEvent::Error(msg)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // The FSM already emits terminal lifecycle events for both success and failure paths.
        let _ = (&self.system_journal, &self.stage_name);
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            JournalSinkState::Created => {
                // Wait for explicit initialization from pipeline
                Ok(EventLoopDirective::Continue)
            }

            JournalSinkState::Initialized => {
                // Auto-transition to ready
                Ok(EventLoopDirective::Transition(JournalSinkEvent::Ready))
            }

            JournalSinkState::Running => running::dispatch_running(self, state, ctx).await,

            JournalSinkState::Flushing => {
                // Wait for flush to complete
                // The actual flush happens in the action
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    supervisor_state = %state.variant_name(),
                    "sink: Flushing dispatch reached (post-flush), transitioning to Draining"
                );
                tracing::debug!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: Flushing -> Draining transition firing"
                );
                Ok(EventLoopDirective::Transition(
                    JournalSinkEvent::FlushComplete,
                ))
            }

            JournalSinkState::Draining => {
                // Move to drained state; completion + cleanup are handled
                // by the FSM transition (BeginDrain) rather than here.
                tracing::info!(
                    target: "flowip-080o",
                    stage_name = %ctx.stage_name,
                    "sink: Draining complete, sending completion and cleaning up"
                );
                Ok(EventLoopDirective::Transition(JournalSinkEvent::BeginDrain))
            }

            JournalSinkState::Drained => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            JournalSinkState::Failed(_) => {
                // Terminal state
                Ok(EventLoopDirective::Terminate)
            }

            JournalSinkState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: SinkHandler + Clone + std::fmt::Debug + Send + Sync + 'static> ExternalEventPolicy
    for JournalSinkSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, JournalSinkState::Created) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(state, JournalSinkState::Failed(_)) {
            None
        } else {
            Some(JournalSinkEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}
