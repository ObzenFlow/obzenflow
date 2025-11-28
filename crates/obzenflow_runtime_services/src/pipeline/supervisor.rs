//! Pipeline Supervisor - Manages the lifecycle of a pipeline and its stages
//!
//! The supervisor pattern provides:
//! - Hierarchical ownership of FSMs
//! - Message bus for inter-FSM communication
//! - Clean separation between supervision and business logic

use super::fsm::{PipelineAction, PipelineContext, PipelineEvent, PipelineState};
use crate::id_conversions::StageIdExt;
use crate::messaging::SubscriptionPoller;
use crate::supervised_base::{EventLoopDirective, SelfSupervised};
use obzenflow_core::event::types::{SeqNo, ViolationCause};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::{StageId, id::SystemId};
use obzenflow_fsm::{fsm, EventVariant, StateVariant, Transition};
use std::{sync::Arc, time::{Duration, Instant}};

const IDLE_BACKOFF_MS: u64 = 10;
const DRAIN_LIVENESS_MAX_IDLE: u64 = 100;

/// Pipeline supervisor - manages the lifecycle of a pipeline
pub(crate) struct PipelineSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// System ID for this pipeline (used for writer_id and lifecycle events)
    pub(crate) system_id: SystemId,

    /// System journal for pipeline orchestration events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,

    /// Throttled logging for barrier snapshots during drain
    pub(crate) last_barrier_log: Option<Instant>,

    /// Idle iterations observed during draining (for liveness guard)
    pub(crate) drain_idle_iters: u64,
}

impl crate::supervised_base::base::Supervisor for PipelineSupervisor {
    type State = PipelineState;
    type Event = PipelineEvent;
    type Context = PipelineContext;
    type Action = PipelineAction;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        fsm! {
            state:   PipelineState;
            event:   PipelineEvent;
            context: PipelineContext;
            action:  PipelineAction;
            initial: initial_state;

            state PipelineState::Created {
                on PipelineEvent::Materialize => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::info!("🔄 FSM: Created -> Materializing (Materialize event)");
                        Ok(Transition {
                            next_state: PipelineState::Materializing,
                            actions: vec![PipelineAction::CreateStages],
                        })
                    })
                };

                on PipelineEvent::Run => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::error!("🚨 FATAL: Received Run event while in Created state!");
                        tracing::error!(
                            "🚨 This means pipeline supervisor never processed Materialize event"
                        );
                        tracing::error!("🚨 Pipeline supervisor task likely never executed!");
                        panic!("Run event received in Created state - pipeline supervisor not running");
                    })
                };
            }

            state PipelineState::Materializing {
                on PipelineEvent::MaterializationComplete => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::info!(
                            "🔄 FSM: Materializing -> Materialized (MaterializationComplete event)"
                        );
                        Ok(Transition {
                            next_state: PipelineState::Materialized,
                            actions: vec![
                                PipelineAction::StartCompletionSubscription,
                                PipelineAction::StartMetricsAggregator,
                                PipelineAction::NotifyStagesStart,
                            ],
                        })
                    })
                };

                on PipelineEvent::Run => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::error!("🚨 FATAL: Received Run event while in Materializing state!");
                        tracing::error!("🚨 Pipeline has not finished materializing yet");
                        tracing::error!(
                            "🚨 Check for race condition or missing MaterializationComplete"
                        );
                        panic!("Run event received in Materializing state - not ready yet");
                    })
                };

                on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Error { message } = event {
                            Ok(Transition {
                                next_state: PipelineState::Failed { reason: message },
                                actions: vec![PipelineAction::Cleanup],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };
            }

            state PipelineState::Materialized {
                on PipelineEvent::Run => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        tracing::info!("🔄 FSM: Materialized -> Running (Run event)");
                        Ok(Transition {
                            next_state: PipelineState::Running,
                            actions: vec![PipelineAction::NotifySourceStart],
                        })
                    })
                };
            }

            state PipelineState::Running {
                on PipelineEvent::Abort => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Abort { reason, upstream } = event {
                            let reason_clone = reason.clone();
                            Ok(Transition {
                                next_state: PipelineState::AbortRequested {
                                    reason: reason.clone(),
                                    upstream,
                                },
                                actions: vec![
                                    PipelineAction::WritePipelineAbort { reason, upstream },
                                    PipelineAction::AbortTeardown {
                                        reason: reason_clone,
                                        upstream,
                                    },
                                ],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::Shutdown => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::SourceCompleted,
                            actions: vec![], // No actions yet - just track state
                        })
                    })
                };

                on PipelineEvent::StageCompleted => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::StageCompleted { envelope } = event {
                            Ok(Transition {
                                next_state: PipelineState::Running,
                                actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Error { message } = event {
                            Ok(Transition {
                                next_state: PipelineState::Failed { reason: message },
                                actions: vec![PipelineAction::Cleanup],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::AllStagesCompleted => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Drained,
                            actions: vec![
                                PipelineAction::DrainMetrics,
                                PipelineAction::WritePipelineCompleted,
                                PipelineAction::Cleanup,
                            ],
                        })
                    })
                };
            }

            state PipelineState::SourceCompleted {
                on PipelineEvent::BeginDrain => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Draining,
                            actions: vec![PipelineAction::BeginDrain],
                        })
                    })
                };
            }

            state PipelineState::Draining {
                on PipelineEvent::Abort => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Abort { reason, upstream } = event {
                            let reason_clone = reason.clone();
                            Ok(Transition {
                                next_state: PipelineState::AbortRequested {
                                    reason: reason.clone(),
                                    upstream,
                                },
                                actions: vec![
                                    PipelineAction::WritePipelineAbort { reason, upstream },
                                    PipelineAction::AbortTeardown {
                                        reason: reason_clone,
                                        upstream,
                                    },
                                ],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::StageCompleted => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::StageCompleted { envelope } = event {
                            Ok(Transition {
                                next_state: PipelineState::Draining,
                                actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };

                on PipelineEvent::AllStagesCompleted => |_state: &PipelineState, _event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    Box::pin(async move {
                        Ok(Transition {
                            next_state: PipelineState::Drained,
                            actions: vec![
                                PipelineAction::DrainMetrics,
                                PipelineAction::WritePipelineCompleted,
                                PipelineAction::Cleanup,
                            ],
                        })
                    })
                };

                on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Error { message } = event {
                            Ok(Transition {
                                next_state: PipelineState::Failed { reason: message },
                                actions: vec![PipelineAction::Cleanup],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };
            }

            state PipelineState::AbortRequested {
                on PipelineEvent::Error => |_state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                    let event = event.clone();
                    Box::pin(async move {
                        if let PipelineEvent::Error { message } = event {
                            Ok(Transition {
                                next_state: PipelineState::Failed { reason: message },
                                actions: vec![PipelineAction::Cleanup],
                            })
                        } else {
                            Err(obzenflow_fsm::FsmError::HandlerError(
                                "Invalid event".to_string(),
                            ))
                        }
                    })
                };
            }

            // Drained and Failed are terminal; no explicit transitions here.

            unhandled => |state: &PipelineState, event: &PipelineEvent, _ctx: &mut PipelineContext| {
                let state_name = state.variant_name().to_string();
                let event_name = event.variant_name().to_string();
                Box::pin(async move {
                    tracing::error!(
                        supervisor = "PipelineSupervisor",
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

// Implement SelfSupervised trait
#[async_trait::async_trait]
impl SelfSupervised for PipelineSupervisor {
    fn writer_id(&self) -> WriterId {
        WriterId::from(self.system_id)
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let event = SystemEvent::new(
            self.writer_id(),
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::Completed,
            ),
        );
        if let Err(e) = self.system_journal.append(event, None).await {
            tracing::error!(
                pipeline = %self.name,
                journal_error = %e,
                "Failed to write pipeline completion event; continuing without system journal entry"
            );
        }
        Ok(())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut PipelineContext,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            PipelineState::Created => {
                // In Created state, we wait for external trigger to materialize
                // This would typically come from the FlowHandle
                tracing::info!("✅ Pipeline state in Created");
                Ok(EventLoopDirective::Continue)
            }

            PipelineState::Materializing => {
                // Check if all stages have been initialized
                let supervisors = &context.stage_supervisors;
                let source_supers = &context.source_supervisors;
                let expected_count = context.topology.stages().count();
                let initialized_count = supervisors.len() + source_supers.len();

                if initialized_count == expected_count && expected_count > 0 {
                    // All stages created, transition to Materialized
                    tracing::info!("✅ All stages initialized, transitioning to Materialized");
                    Ok(EventLoopDirective::Transition(
                        PipelineEvent::MaterializationComplete,
                    ))
                } else {
                    // Still waiting for stages to be created. Log details once before failing hard.
                    tracing::error!(
                        initialized_count,
                        expected_count,
                        "⚠️ MISMATCH DETECTED: supervisors vs topology stages"
                    );
                    tracing::debug!(
                        supervisors = ?supervisors
                            .keys()
                            .map(|id| format!("{:?}", id))
                            .collect::<Vec<_>>(),
                        sources = ?source_supers
                            .keys()
                            .map(|id| format!("{:?}", id))
                            .collect::<Vec<_>>(),
                        topology = ?context
                            .topology
                            .stages()
                            .map(|s| format!("{} ({:?})", s.name, s.id))
                            .collect::<Vec<_>>(),
                        "Materialization mismatch details"
                    );

                    panic!(
                        "Stage count mismatch: {} supervisors vs {} topology stages",
                        initialized_count, expected_count
                    );
                }
            }

            PipelineState::Materialized => {
                // First check if any stages are already running (they might have published before we subscribed)
                let supervisors = &context.stage_supervisors;
                for (stage_id, stage) in supervisors.iter() {
                    if stage.is_ready()
                        && !context
                            .topology
                            .upstream_stages(stage_id.to_topology_id())
                            .is_empty()
                    {
                        // This is a non-source stage that's already running
                        context.running_stages.insert(*stage_id);
                        tracing::info!("Stage '{}' was already running", stage.stage_name());
                    }
                }

                // Poll for stage running events to know when all non-source stages are ready
                let subscription = context.completion_subscription.as_mut().ok_or(
                    "No subscription available - should have been initialized during materialization",
                )?;

                // Check for stage running events
                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        // Process stage running event
                        let event = &envelope.event;
                        if let obzenflow_core::event::SystemEventType::StageLifecycle {
                            stage_id,
                            event: obzenflow_core::event::StageLifecycleEvent::Running,
                        } = &event.event
                        {
                            // Track this stage as running
                            context.running_stages.insert(stage_id.clone());

                            // Get stage name from topology
                            let stage_info = context
                                .topology
                                .stages()
                                .find(|s| s.id == stage_id.to_topology_id());
                            let stage_name = stage_info
                                .map(|s| s.name.clone())
                                .unwrap_or_else(|| "unknown".to_string());

                            // Log based on stage type
                            if context
                                .topology
                                .upstream_stages(stage_id.to_topology_id())
                                .is_empty()
                            {
                                tracing::debug!(
                                    "Source stage '{}' is running (waiting for pipeline signal)",
                                    stage_name
                                );
                            } else {
                                tracing::info!("Stage '{}' is now running", stage_name);
                            }
                        }
                    }
                    PollResult::NoEvents => {
                        // No new events, but that's OK - stages might already be running
                        // Add a brief sleep to avoid busy loop
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                    PollResult::Error(e) => {
                        tracing::error!("Error polling system journal in Awaiting: {}", e);
                        return Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!("System journal error: {}", e),
                        }));
                    }
                }

                // Check if all non-source stages are running
                let running_stages = &context.running_stages;
                let topology = &context.topology;

                // Get all non-source stage IDs
                let non_source_stages: std::collections::HashSet<_> = topology
                    .stages()
                    .filter(|stage_info| !topology.upstream_stages(stage_info.id).is_empty())
                    .map(|stage_info| StageId::from_topology_id(stage_info.id))
                    .collect();

                // Check if all non-source stages are in the running set
                let all_ready = !non_source_stages.is_empty()
                    && non_source_stages
                        .iter()
                        .all(|stage_id| running_stages.contains(stage_id));

                if all_ready {
                    tracing::info!(
                        "All {} non-source stages are running, starting pipeline",
                        non_source_stages.len()
                    );
                    Ok(EventLoopDirective::Transition(PipelineEvent::Run))
                } else {
                    // Still waiting for stages to report running
                    let waiting_for = non_source_stages.difference(&*running_stages).count();
                    if waiting_for > 0 {
                        tracing::debug!(
                            "Waiting for {} more stage(s) to report running ({}/{} ready)",
                            waiting_for,
                            running_stages.len(),
                            non_source_stages.len()
                        );
                    }
                    Ok(EventLoopDirective::Continue)
                }
            }

            PipelineState::Running => {
                // Poll for completion events from stages (system journal)
                let subscription = context.completion_subscription.as_mut().ok_or(
                    "No subscription available - should have been initialized during materialization",
                )?;

                // Use poll_next for non-blocking event polling
                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        let event = &envelope.event;

                        return match &event.event {
                            obzenflow_core::event::SystemEventType::StageLifecycle { stage_id, event: lifecycle_event } => {
                                match lifecycle_event {
                                    obzenflow_core::event::StageLifecycleEvent::Running => {
                                        // Get stage name from topology
                                        let stage_info = context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        tracing::info!("Stage '{}' is now running", stage_name);
                                        Ok(EventLoopDirective::Continue)
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Draining => {
                                        let stage_info = context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        tracing::info!("Stage '{}' is draining", stage_name);
                                        Ok(EventLoopDirective::Continue)
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Drained => {
                                        let stage_info = context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        tracing::info!("Stage '{}' is drained", stage_name);
                                        Ok(EventLoopDirective::Continue)
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Completed => {
                                        // Stage has fully completed
                                        Ok(EventLoopDirective::Transition(
                                            PipelineEvent::StageCompleted { envelope },
                                        ))
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Failed { error, .. } => {
                                        let stage_info = context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());

                                        Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                                            message: format!("Stage '{}' failed: {}", stage_name, error),
                                        }))
                                    }
                                }
                            }
                            obzenflow_core::event::SystemEventType::PipelineLifecycle(event) => {
                                match event {
                                    obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted => {
                                        tracing::info!("Received AllStagesCompleted event!");
                                        if let Some(abort_directive) = self
                                            .missing_contract_abort(context)
                                        {
                                            return Ok(abort_directive);
                                        }
                                        Ok(EventLoopDirective::Transition(
                                            PipelineEvent::AllStagesCompleted,
                                        ))
                                    }
                                    _ => Ok(EventLoopDirective::Continue)
                                }
                            }
                            obzenflow_core::event::SystemEventType::ContractStatus {
                                upstream,
                                reader,
                                pass,
                                reader_seq,
                                advertised_writer_seq,
                                reason,
                            } => {
                                // Record per-edge contract status
                                if *pass {
                                    context.contract_pairs.insert(
                                        (*upstream, *reader),
                                        ContractEdgeStatus::passed(
                                            *reader_seq,
                                            *advertised_writer_seq,
                                        ),
                                    );
                                } else {
                                    context.contract_pairs.insert(
                                        (*upstream, *reader),
                                        ContractEdgeStatus::failed(
                                            reason.clone(),
                                            *reader_seq,
                                            *advertised_writer_seq,
                                        ),
                                    );
                                }

                                if !pass {
                                    tracing::error!(
                                        ?upstream,
                                        ?reader,
                                        ?reason,
                                        "Contract status failure"
                                    );
                                    return Ok(EventLoopDirective::Transition(PipelineEvent::Abort {
                                        reason: reason.clone().unwrap_or_else(|| {
                                            obzenflow_core::event::types::ViolationCause::Other(
                                                "contract_failed".into(),
                                            )
                                        }),
                                        upstream: Some(*upstream),
                                    }));
                                }

                                // Mark upstream source as having passed its contract
                                context.contract_status.insert(*upstream, true);
                                let expected = &context.expected_sources;
                                let all_pass = !expected.is_empty()
                                    && expected.iter().all(|src| {
                                        context
                                            .contract_status
                                            .get(src)
                                            .copied()
                                            .unwrap_or(false)
                                    });

                                if all_pass {
                                    tracing::info!("All source contracts passed; initiating drain");
                                    Ok(EventLoopDirective::Transition(PipelineEvent::Shutdown))
                                } else {
                                    Ok(EventLoopDirective::Continue)
                                }
                            }
                            _ => Ok(EventLoopDirective::Continue)
                        };
                    }
                    PollResult::NoEvents => {
                        // No events available right now - sleep briefly to avoid busy loop
                        idle_backoff().await;
                        Ok(EventLoopDirective::Continue)
                    }
                    PollResult::Error(e) => {
                        tracing::error!("Error polling system journal: {}", e);
                        Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!("System journal error: {}", e),
                        }))
                    }
                }
            }

            PipelineState::SourceCompleted => {
                // Source has completed - initiate Jonestown protocol
                tracing::info!("Source completed - beginning pipeline drain");

                // Immediately transition to start draining
                Ok(EventLoopDirective::Transition(PipelineEvent::BeginDrain))
            }

            PipelineState::Draining => {
                // Continue polling for completion events during drain
                let subscription = context
                    .completion_subscription
                    .as_mut()
                    .ok_or("No subscription available")?;

                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        let event = &envelope.event;

                        return match &event.event {
                            obzenflow_core::event::SystemEventType::StageLifecycle {
                                stage_id: _,
                                event: obzenflow_core::event::StageLifecycleEvent::Completed,
                            } => {
                                // Process stage completion immediately
                                Ok(EventLoopDirective::Transition(
                                    PipelineEvent::StageCompleted { envelope },
                                ))
                            }
                            obzenflow_core::event::SystemEventType::ContractStatus {
                                upstream,
                                reader,
                                pass,
                                reader_seq,
                                advertised_writer_seq,
                                reason,
                            } => {
                                if *pass {
                                    context.contract_pairs.insert(
                                        (*upstream, *reader),
                                        ContractEdgeStatus::passed(
                                            *reader_seq,
                                            *advertised_writer_seq,
                                        ),
                                    );
                                } else {
                                    context.contract_pairs.insert(
                                        (*upstream, *reader),
                                        ContractEdgeStatus::failed(
                                            reason.clone(),
                                            *reader_seq,
                                            *advertised_writer_seq,
                                        ),
                                    );
                                }

                                if !pass {
                                    tracing::error!(
                                        ?upstream,
                                        ?reader,
                                        ?reason,
                                        "Contract status failure during drain"
                                    );
                                    return Ok(EventLoopDirective::Transition(
                                        PipelineEvent::Abort {
                                            reason: reason.clone().unwrap_or_else(|| {
                                                obzenflow_core::event::types::ViolationCause::Other(
                                                    "contract_failed".into(),
                                                )
                                            }),
                                            upstream: Some(upstream.clone()),
                                        },
                                    ));
                                } else {
                                    context.contract_status.insert(upstream.clone(), true);
                                }
                                Ok(EventLoopDirective::Continue)
                            }
                            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                                obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted,
                            ) => {
                                tracing::info!(
                                    "All stages have completed - transitioning to drained"
                                );
                                if let Some(abort_directive) = self.missing_contract_abort(context)
                                {
                                    return Ok(abort_directive);
                                }
                                Ok(EventLoopDirective::Transition(
                                    PipelineEvent::AllStagesCompleted,
                                ))
                            }
                            _ => {
                                // Log other events
                                tracing::debug!("Received event during drain: {:?}", event.event);
                                Ok(EventLoopDirective::Continue)
                            }
                        };
                    }
                    PollResult::NoEvents => {
                        // No events available right now - sleep briefly to avoid busy loop
                        idle_backoff().await;
                        if self.should_log_barrier() {
                            let snapshot = self.barrier_snapshot(context);
                            tracing::info!(
                                pending_stages = ?snapshot.pending_stages,
                                missing_contracts = ?snapshot.missing_contracts,
                                completed_stages = snapshot.completed,
                                total_stages = snapshot.total,
                                satisfied_contracts = snapshot.satisfied_contracts,
                                total_contracts = snapshot.total_contracts,
                                "Drain barrier snapshot (no events)"
                            );
                        }
                        if self.all_stages_and_contracts_complete(context) {
                            // Synthesize AllStagesCompleted when everything is done
                            if let Err(e) = self.write_all_stages_completed(context).await {
                                tracing::error!(error = %e, "Failed to write synthetic AllStagesCompleted");
                            }
                            Ok(EventLoopDirective::Transition(
                                PipelineEvent::AllStagesCompleted,
                            ))
                        } else {
                            self.drain_idle_iters = self.drain_idle_iters.saturating_add(1);
                            // Soft warning when drain is taking unusually long.
                            // This does NOT cause an abort - 080o-part-2 semantics require
                            // explicit contract failures for aborts, not elapsed time.
                            // Future work (090a) may introduce configurable, rate-aware
                            // liveness bounds as a separate concern from transport contracts.
                            if self.drain_idle_iters == DRAIN_LIVENESS_MAX_IDLE {
                                let snapshot = self.barrier_snapshot(context);
                                tracing::warn!(
                                    pending_stages = ?snapshot.pending_stages,
                                    missing_contracts = ?snapshot.missing_contracts,
                                    completed_stages = snapshot.completed,
                                    total_stages = snapshot.total,
                                    idle_iterations = self.drain_idle_iters,
                                    "Drain taking unusually long; waiting for stages to complete. \
                                     No abort will occur - only explicit contract failures cause aborts. \
                                     See FLOWIP-080o-part-2 and FLOWIP-090a for liveness semantics."
                                );
                            }
                            Ok(EventLoopDirective::Continue)
                        }
                    }
                    PollResult::Error(e) => {
                        tracing::error!("Error polling system journal during drain: {}", e);
                        Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                            message: format!("System journal error during drain: {}", e),
                        }))
                    }
                }
            }

            PipelineState::Drained => {
                // Terminal state
                tracing::info!("Pipeline drained, terminating");
                Ok(EventLoopDirective::Terminate)
            }

            PipelineState::Failed { reason } => {
                // Terminal state
                tracing::error!("Pipeline failed: {}", reason);
                Ok(EventLoopDirective::Terminate)
            }

            PipelineState::AbortRequested { reason, upstream } => {
                let msg = format!(
                    "Pipeline abort requested: {:?} (upstream={:?})",
                    reason, upstream
                );
                Ok(EventLoopDirective::Transition(PipelineEvent::Error {
                    message: msg,
                }))
            }
        }
    }
}

impl PipelineSupervisor {
    /// If any contract edge has an explicit failure recorded, return an abort directive
    fn missing_contract_abort(
        &self,
        context: &PipelineContext,
    ) -> Option<EventLoopDirective<PipelineEvent>> {
        let seen = &context.contract_pairs;

        // Find any edge with an explicit failure (contract violated)
        if let Some(((upstream, reader), status)) =
            seen.iter().find(|(_pair, status)| !status.is_passed())
        {
            let upstream_name = context
                .topology
                .stage_name(upstream.to_topology_id())
                .unwrap_or("unknown")
                .to_string();
            let reader_name = context
                .topology
                .stage_name(reader.to_topology_id())
                .unwrap_or("unknown")
                .to_string();

            // Prefer the recorded violation cause, fall back to a generic label
            let reason = status
                .reason
                .clone()
                .unwrap_or_else(|| ViolationCause::Other("contract_failed".into()));

            tracing::error!(
                ?upstream,
                ?reader,
                upstream_name,
                reader_name,
                "Contract edge recorded as failed; aborting pipeline based on explicit contract violation"
            );

            Some(EventLoopDirective::Transition(PipelineEvent::Abort {
                reason,
                upstream: Some(*upstream),
            }))
        } else {
            None
        }
    }

    /// Check if all stages have completed and all contract pairs are satisfied
    fn all_stages_and_contracts_complete(&self, context: &PipelineContext) -> bool {
        let completed = context.completed_stages.len();
        let total = context.topology.num_stages();

        if completed < total {
            return false;
        }

        let seen = &context.contract_pairs;

        // Success requires that no contract edge has an explicit failure recorded.
        // Missing contract evidence is tolerated here; it is surfaced via logs/metrics
        // but does not block drain at the transport-contract layer.
        !seen.values().any(|status| !status.is_passed())
    }

    /// Synthesize and write AllStagesCompleted when we know we’re done
    async fn write_all_stages_completed(
        &self,
        context: &PipelineContext,
    ) -> Result<(), String> {
        let event = SystemEvent::new(
            WriterId::from(self.system_id),
            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted,
            ),
        );
        self.system_journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| format!("Failed to write AllStagesCompleted: {}", e))
    }

    /// Snapshot the current drain barrier state for logging/inspection
    fn barrier_snapshot(&self, context: &PipelineContext) -> BarrierSnapshot {
        let completed: Vec<StageId> = context.completed_stages.clone();
        let expected_stages: Vec<StageId> = context
            .topology
            .stages()
            .map(|s| StageId::from_topology_id(s.id))
            .collect();
        let pending_stages: Vec<StageId> = expected_stages
            .iter()
            .copied()
            .filter(|id| !completed.contains(id))
            .collect();

        let expected_contracts = context.expected_contract_pairs.clone();
        let seen = &context.contract_pairs;
        let missing_contracts: Vec<(StageId, StageId)> = expected_contracts
            .iter()
            .filter(|pair| !matches!(seen.get(pair), Some(status) if status.is_passed()))
            .copied()
            .collect();

        let total_contracts = expected_contracts.len();
        let satisfied_contracts = expected_contracts
            .iter()
            .filter(|pair| matches!(seen.get(pair), Some(status) if status.is_passed()))
            .count();

        BarrierSnapshot {
            pending_stages,
            missing_contracts,
            completed: completed.len(),
            total: expected_stages.len(),
            satisfied_contracts,
            total_contracts,
        }
    }

    /// Throttle barrier logging to avoid spamming the drain loop
    fn should_log_barrier(&mut self) -> bool {
        let now = Instant::now();
        match self.last_barrier_log {
            Some(last) if now.duration_since(last) < Duration::from_secs(1) => false,
            _ => {
                self.last_barrier_log = Some(now);
                true
            }
        }
    }
}

/// Lightweight snapshot of drain barrier progress for diagnostics
#[derive(Debug)]
struct BarrierSnapshot {
    pending_stages: Vec<StageId>,
    missing_contracts: Vec<(StageId, StageId)>,
    completed: usize,
    total: usize,
    satisfied_contracts: usize,
    total_contracts: usize,
}

/// Status for a contract edge (upstream -> reader)
#[derive(Clone, Debug, Default)]
pub struct ContractEdgeStatus {
    passed: bool,
    reason: Option<ViolationCause>,
    reader_seq: Option<SeqNo>,
    advertised_writer_seq: Option<SeqNo>,
}

impl ContractEdgeStatus {
    pub(crate) fn passed(reader_seq: Option<SeqNo>, advertised_writer_seq: Option<SeqNo>) -> Self {
        Self {
            passed: true,
            reason: None,
            reader_seq,
            advertised_writer_seq,
        }
    }

    pub(crate) fn failed(
        reason: Option<ViolationCause>,
        reader_seq: Option<SeqNo>,
        advertised_writer_seq: Option<SeqNo>,
    ) -> Self {
        Self {
            passed: false,
            reason,
            reader_seq,
            advertised_writer_seq,
        }
    }

    pub(crate) fn is_passed(&self) -> bool {
        self.passed
    }
}

#[inline]
async fn idle_backoff() {
    tokio::time::sleep(Duration::from_millis(IDLE_BACKOFF_MS)).await;
}
