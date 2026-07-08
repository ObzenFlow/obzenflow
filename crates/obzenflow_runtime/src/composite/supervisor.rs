// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The per-composite monitor-supervisor (FLOWIP-128a B3).
//!
//! A `SelfSupervised` node, one per composite, materialised beside the member
//! stage supervisors. It reads its members' `StageLifecycle` from its own
//! system-journal subscription, folds them via `CompositeRollup`, and authors
//! the composite's own `CompositeLifecycle` facts. It owns the composite's
//! lifecycle truth and drives no execution; the PipelineSupervisor stays
//! composite-blind. Like the metrics aggregator, it is a best-effort observer:
//! a recoverable subscription read error is logged and skipped, not fatal.

use super::fsm::{
    build_composite_supervisor_fsm, CompositeSupervisorContext, CompositeSupervisorEvent,
    CompositeSupervisorFsm, CompositeSupervisorState,
};
use super::rollup::CompositeRollup;
use crate::messaging::system_subscription::SystemSubscription;
use crate::messaging::{PollResult, SubscriptionPoller};
use crate::supervised_base::base::Supervisor;
use crate::supervised_base::{EventLoopDirective, SelfSupervised};
use obzenflow_core::event::{PipelineLifecycleEvent, SystemEvent, SystemEventType, WriterId};
use obzenflow_core::id::{CompositeId, SystemId};
use obzenflow_core::journal::Journal;
use std::sync::Arc;

const IDLE_BACKOFF_MS: u64 = 10;

/// The monitor-supervisor for one composite.
pub(crate) struct CompositeSupervisor {
    pub(crate) name: String,
    pub(crate) composite_id: CompositeId,
    pub(crate) system_id: SystemId,
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,
    pub(crate) subscription: Option<SystemSubscription<SystemEvent>>,
    pub(crate) rollup: CompositeRollup,
}

impl Supervisor for CompositeSupervisor {
    type State = CompositeSupervisorState;
    type Event = CompositeSupervisorEvent;
    type Context = CompositeSupervisorContext;
    type Action = super::fsm::CompositeSupervisorAction;

    fn build_state_machine(&self, _initial_state: Self::State) -> CompositeSupervisorFsm {
        build_composite_supervisor_fsm()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[async_trait::async_trait]
impl SelfSupervised for CompositeSupervisor {
    fn writer_id(&self) -> WriterId {
        WriterId::from(self.system_id)
    }

    fn event_for_action_error(&self, msg: String) -> CompositeSupervisorEvent {
        CompositeSupervisorEvent::Error(msg)
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        _ctx: &mut CompositeSupervisorContext,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            CompositeSupervisorState::Running => {
                let Some(sub) = self.subscription.as_mut() else {
                    return Ok(EventLoopDirective::Terminate);
                };

                match sub.poll_next().await {
                    PollResult::Event(envelope) => match &envelope.event.event {
                        SystemEventType::StageLifecycle { stage_id, event } => {
                            if let Some(composite_event) = self.rollup.observe(*stage_id, event) {
                                let fact = SystemEvent::new(
                                    WriterId::from(self.system_id),
                                    SystemEventType::CompositeLifecycle {
                                        composite_id: self.composite_id.clone(),
                                        event: composite_event,
                                    },
                                );
                                if let Err(e) = self.system_journal.append(fact, None).await {
                                    tracing::error!(
                                        composite = %self.composite_id,
                                        journal_error = %e,
                                        "Failed to author CompositeLifecycle; continuing without system journal entry"
                                    );
                                }
                                if self.rollup.is_terminal() {
                                    return Ok(EventLoopDirective::Transition(
                                        CompositeSupervisorEvent::Finish,
                                    ));
                                }
                            }
                            Ok(EventLoopDirective::Continue)
                        }
                        // Backstop: if the flow reaches a terminal before this
                        // composite resolves, stop rather than run forever.
                        SystemEventType::PipelineLifecycle(
                            PipelineLifecycleEvent::Drained
                            | PipelineLifecycleEvent::Completed { .. }
                            | PipelineLifecycleEvent::Failed { .. },
                        ) => Ok(EventLoopDirective::Transition(
                            CompositeSupervisorEvent::Finish,
                        )),
                        _ => Ok(EventLoopDirective::Continue),
                    },
                    PollResult::NoEvents => {
                        idle_backoff().await;
                        Ok(EventLoopDirective::Continue)
                    }
                    PollResult::Error(e) => {
                        // FLOWIP-120q live-tail observer resilience: skip the
                        // unreadable record and continue rather than dying.
                        tracing::warn!(
                            composite = %self.composite_id,
                            error = %e,
                            "Composite supervisor system read error; skipping record and continuing"
                        );
                        idle_backoff().await;
                        Ok(EventLoopDirective::Continue)
                    }
                }
            }

            CompositeSupervisorState::Drained => Ok(EventLoopDirective::Terminate),

            CompositeSupervisorState::Failed { error } => {
                tracing::error!(composite = %self.composite_id, error = %error, "Composite supervisor failed");
                Ok(EventLoopDirective::Terminate)
            }
        }
    }
}

#[inline]
async fn idle_backoff() {
    tokio::time::sleep(std::time::Duration::from_millis(IDLE_BACKOFF_MS)).await;
}
