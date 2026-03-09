// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Stateful supervisor implementation using HandlerSupervised pattern.
//!
//! Decomposed into submodules:
//! - `running.rs`  contains the Accumulating and Emitting loops
//! - `draining.rs` contains the Draining loop

mod draining;
mod running;

use crate::messaging::UpstreamSubscription;
use crate::metrics::instrumentation::heartbeat_interval;
use crate::stages::common::handlers::StatefulHandler;
use crate::stages::common::supervision::flow_context_factory::make_flow_context;
use crate::stages::common::supervision::forward_control_event::forward_control_event as forward_control_event_helper;
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{
    EventLoopDirective, ExternalEventMode, ExternalEventPolicy, HandlerSupervised,
};
use obzenflow_core::event::context::StageType;
use obzenflow_core::{ChainEvent, EventEnvelope, StageId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};

use super::fsm::{StatefulAction, StatefulContext, StatefulEvent, StatefulState};

/// Supervisor for stateful stages
pub(crate) struct StatefulSupervisor<
    H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static,
> {
    /// Supervisor name (for logging)
    pub(crate) name: String,

    /// Stage ID
    pub(crate) stage_id: StageId,

    /// Subscription to upstream events (supervisor-owned to avoid borrow conflicts).
    pub(super) subscription: Option<UpstreamSubscription<ChainEvent>>,

    /// Phantom marker to keep H in the type while no fields reference it directly
    pub(crate) _marker: std::marker::PhantomData<H>,
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> Supervisor
    for StatefulSupervisor<H>
{
    type State = StatefulState<H>;
    type Event = StatefulEvent<H>;
    type Context = StatefulContext<H>;
    type Action = StatefulAction<H>;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   StatefulState<H>;
            event:   StatefulEvent<H>;
            context: StatefulContext<H>;
            action:  StatefulAction<H>;
            initial: initial_state;

            state StatefulState::Created {
                on StatefulEvent::Initialize => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Initialized");
                        Ok(Transition {
                            next_state: StatefulState::Initialized,
                            actions: vec![StatefulAction::AllocateResources],
                        })
                    })
                };

                // Fallback error handling for Created (matches original from_any behaviour)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Initialized {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Accumulating");
                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![StatefulAction::PublishRunning],
                        })
                    })
                };

                // Fallback error handling for Initialized (matches original from_any behaviour)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Accumulating {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::ShouldEmit => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Emitting");
                        Ok(Transition {
                            next_state: StatefulState::Emitting,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::ReceivedEOF => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Draining");
                        Ok(Transition {
                            next_state: StatefulState::Draining,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Emitting {
                on StatefulEvent::Ready => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: StatefulState::Emitting,
                            actions: vec![],
                        })
                    })
                };

                on StatefulEvent::EmitComplete => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Accumulating");
                        Ok(Transition {
                            next_state: StatefulState::Accumulating,
                            actions: vec![],
                        })
                    })
                };

                // Fallback error handling for Emitting (matches original from_any behaviour)
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            state StatefulState::Draining {
                on StatefulEvent::DrainComplete => |_state: &StatefulState<H>, _event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    Box::pin(async move {
                        ctx.instrumentation.transition_to_state("Drained");
                        Ok(Transition {
                            next_state: StatefulState::Drained,
                            actions: vec![
                                StatefulAction::ForwardEOF,
                                StatefulAction::SendCompletion,
                                StatefulAction::Cleanup,
                            ],
                        })
                    })
                };

                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            ctx.instrumentation.transition_to_state("Failed");
                            ctx.instrumentation
                                .failures_total
                                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            // Drained: terminal on success; still handle Error like from_any
            state StatefulState::Drained {
                on StatefulEvent::Error => |_state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(msg) = event {
                            let failure_msg = msg.clone();
                            Ok(Transition {
                                next_state: StatefulState::Failed(failure_msg),
                                actions: vec![
                                    StatefulAction::SendFailure { message: msg },
                                    StatefulAction::Cleanup,
                                ],
                            })
                        } else {
                            unreachable!()
                        }
                    })
                };
            }

            // Failed: receiving Error again should be idempotent (no extra cleanup)
            state StatefulState::Failed {
                on StatefulEvent::Error => |state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                    let state = state.clone();
                    let event = event.clone();
                    Box::pin(async move {
                        if let StatefulEvent::Error(_msg) = event {
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

            unhandled => |state: &StatefulState<H>, event: &StatefulEvent<H>, _ctx: &mut StatefulContext<H>| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();

                Box::pin(async move {
                    tracing::error!(
                        supervisor = "StatefulSupervisor",
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
impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> HandlerSupervised
    for StatefulSupervisor<H>
{
    type Handler = H;

    fn writer_id(&self) -> obzenflow_core::WriterId {
        obzenflow_core::WriterId::from(self.stage_id)
    }

    fn stage_id(&self) -> StageId {
        self.stage_id
    }

    fn event_for_action_error(&self, msg: String) -> StatefulEvent<H> {
        StatefulEvent::Error(msg)
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        ctx: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            StatefulState::Created => {
                // Wait for explicit initialization from pipeline.
                Ok(EventLoopDirective::Continue)
            }
            StatefulState::Initialized => {
                // Auto-transition to ready (stateful stages start immediately).
                Ok(EventLoopDirective::Transition(StatefulEvent::Ready))
            }
            StatefulState::Accumulating => running::dispatch_accumulating(self, state, ctx).await,
            StatefulState::Emitting => running::dispatch_emitting(self, state, ctx).await,
            StatefulState::Draining => draining::dispatch_draining(self, state, ctx).await,
            StatefulState::Drained => Ok(EventLoopDirective::Terminate),
            StatefulState::Failed(_) => Ok(EventLoopDirective::Terminate),
            StatefulState::_Phantom(_) => {
                unreachable!("PhantomData variant should never be instantiated")
            }
        }
    }
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> ExternalEventPolicy
    for StatefulSupervisor<H>
{
    fn external_event_mode(state: &Self::State) -> ExternalEventMode {
        if matches!(state, StatefulState::Created) {
            ExternalEventMode::Block
        } else {
            ExternalEventMode::Poll
        }
    }

    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event> {
        if matches!(state, StatefulState::Failed(_)) {
            None
        } else {
            Some(StatefulEvent::Error(
                "External control channel closed".to_string(),
            ))
        }
    }
}

impl<H: StatefulHandler + Clone + std::fmt::Debug + Send + Sync + 'static> StatefulSupervisor<H> {
    /// Forward a control event downstream by appending it to the stateful stage's data journal.
    async fn forward_control_event(
        &self,
        ctx: &StatefulContext<H>,
        envelope: &EventEnvelope<ChainEvent>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _ = forward_control_event_helper(
            envelope,
            self.stage_id,
            &ctx.stage_name,
            StageType::Stateful,
            &ctx.data_journal,
        )
        .await?;
        Ok(())
    }

    /// Emit an observability heartbeat when enough events have been accumulated.
    ///
    /// This writes a lightweight `Observability` event carrying the latest
    /// `runtime_context` snapshot for the accumulator.
    async fn emit_stateful_heartbeat_if_due(
        &self,
        ctx: &mut StatefulContext<H>,
        force: bool,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let interval = heartbeat_interval();
        if interval == 0 {
            return Ok(());
        }

        let delta = ctx.events_since_last_heartbeat;
        if delta == 0 {
            return Ok(());
        }

        // In normal operation we require `delta >= interval` before emitting.
        // When `force` is true (drain path), we bypass this threshold so that
        // short finite flows still publish a final heartbeat snapshot.
        if !force && delta < interval {
            return Ok(());
        }

        let Some(writer_id) = ctx.writer_id else {
            // Writer not initialised yet; skip heartbeat rather than failing.
            return Ok(());
        };

        // Capture a fresh runtime context snapshot for the heartbeat.
        let runtime_context = ctx.instrumentation.snapshot_with_control();

        use obzenflow_core::event::payloads::observability_payload::{
            MetricsLifecycle, ObservabilityPayload,
        };
        use obzenflow_core::event::ChainEventFactory;
        use serde_json::json;

        let flow_id = ctx.flow_id.to_string();
        let flow_context = make_flow_context(
            &ctx.flow_name,
            &flow_id,
            &ctx.stage_name,
            self.stage_id,
            StageType::Stateful,
        );

        let payload = ObservabilityPayload::Metrics(MetricsLifecycle::Custom {
            name: "accumulator_heartbeat".to_string(),
            value: json!({
                "events_accumulated_since_last_heartbeat": delta,
                "events_processed_total": runtime_context.events_processed_total,
            }),
            tags: None,
        });

        let heartbeat = ChainEventFactory::observability_event(writer_id, payload)
            .with_flow_context(flow_context)
            .with_runtime_context(runtime_context);

        ctx.data_journal.append(heartbeat, None).await?;

        // Reset counter now that we've published a snapshot.
        ctx.events_since_last_heartbeat = 0;

        Ok(())
    }
}
