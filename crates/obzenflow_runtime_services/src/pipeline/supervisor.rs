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
use obzenflow_core::StageId;
use std::sync::Arc;
use std::time::{Duration, Instant};

const IDLE_BACKOFF_MS: u64 = 10;
const DRAIN_LIVENESS_MAX_IDLE: u64 = 100;

/// Pipeline supervisor - manages the lifecycle of a pipeline
pub(crate) struct PipelineSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// Pipeline context (contains all mutable state)
    pub(crate) pipeline_context: Arc<PipelineContext>,

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

    fn configure_fsm(
        &self,
        builder: obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action>,
    ) -> obzenflow_fsm::FsmBuilder<Self::State, Self::Event, Self::Context, Self::Action> {
        builder
            .when("Created")
            .on("Materialize", |_state, _event: &PipelineEvent, _ctx| {
                Box::pin(async move {
                    tracing::info!("🔄 FSM: Created -> Materializing (Materialize event)");
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::Materializing,
                        actions: vec![PipelineAction::CreateStages],
                    })
                })
            })
            .on("Run", |_state, _event: &PipelineEvent, _ctx| {
                Box::pin(async move {
                    tracing::error!("🚨 FATAL: Received Run event while in Created state!");
                    tracing::error!(
                        "🚨 This means pipeline supervisor never processed Materialize event"
                    );
                    tracing::error!("🚨 Pipeline supervisor task likely never executed!");
                    panic!("Run event received in Created state - pipeline supervisor not running");
                })
            })
            .done()
            .when("Materializing")
            .on(
                "MaterializationComplete",
                |_state, _event: &PipelineEvent, _ctx| {
                    Box::pin(async move {
                        tracing::info!(
                            "🔄 FSM: Materializing -> Materialized (MaterializationComplete event)"
                        );
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Materialized,
                            actions: vec![
                                PipelineAction::StartCompletionSubscription,
                                PipelineAction::StartMetricsAggregator,
                                PipelineAction::NotifyStagesStart,
                            ],
                        })
                    })
                },
            )
            .on("Run", |_state, _event: &PipelineEvent, _ctx| {
                Box::pin(async move {
                    tracing::error!("🚨 FATAL: Received Run event while in Materializing state!");
                    tracing::error!("🚨 Pipeline has not finished materializing yet");
                    tracing::error!(
                        "🚨 Check for race condition or missing MaterializationComplete"
                    );
                    panic!("Run event received in Materializing state - not ready yet");
                })
            })
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Failed { reason: message },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            })
            .done()
            .when("Materialized")
            .on("Run", |_state, _event: &PipelineEvent, _ctx| {
                Box::pin(async move {
                    tracing::info!("🔄 FSM: Materialized -> Running (Run event)");
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::Running,
                        actions: vec![PipelineAction::NotifySourceStart],
                    })
                })
            })
            .done()
            .when("Running")
            .on("Abort", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Abort { reason, upstream } = event {
                        let reason_clone = reason.clone();
                        Ok(obzenflow_fsm::Transition {
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
            })
            .on("Shutdown", |_state, _event: &PipelineEvent, _ctx| {
                Box::pin(async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::SourceCompleted,
                        actions: vec![], // No actions yet - just track state
                    })
                })
            })
            .on("StageCompleted", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::StageCompleted { envelope } = event {
                        // Just track the completion, stay in Running state
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Running,
                            actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            })
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Failed { reason: message },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            })
            .on(
                "AllStagesCompleted",
                |_state, _event: &PipelineEvent, _ctx| {
                    Box::pin(async move {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Drained,
                            actions: vec![
                                PipelineAction::DrainMetrics,
                                PipelineAction::WritePipelineCompleted,
                                PipelineAction::Cleanup,
                            ],
                        })
                    })
                },
            )
            .done()
            .when("SourceCompleted")
            .on("BeginDrain", |_state, _event: &PipelineEvent, _ctx| {
                Box::pin(async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![PipelineAction::BeginDrain],
                    })
                })
            })
            .done()
            .when("Draining")
            .on("Abort", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Abort { reason, upstream } = event {
                        let reason_clone = reason.clone();
                        Ok(obzenflow_fsm::Transition {
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
            })
            .on("StageCompleted", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::StageCompleted { envelope } = event {
                        // Track completion and check if all stages are done
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Draining,
                            actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            })
            .on(
                "AllStagesCompleted",
                |_state, _event: &PipelineEvent, _ctx| {
                    Box::pin(async move {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Drained,
                            actions: vec![
                                PipelineAction::DrainMetrics,
                                PipelineAction::WritePipelineCompleted,
                                PipelineAction::Cleanup,
                            ],
                        })
                    })
                },
            )
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Failed { reason: message },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            })
            .done()
            .when("AbortRequested")
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                Box::pin(async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Failed { reason: message },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err(obzenflow_fsm::FsmError::HandlerError(
                            "Invalid event".to_string(),
                        ))
                    }
                })
            })
            .done()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

// Implement SelfSupervised trait
#[async_trait::async_trait]
impl SelfSupervised for PipelineSupervisor {
    fn writer_id(&self) -> WriterId {
        WriterId::from(self.pipeline_context.system_id)
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
                let supervisors = self.pipeline_context.stage_supervisors.read().await;
                let source_supers = self.pipeline_context.source_supervisors.read().await;
                let expected_count = self.pipeline_context.topology.stages().count();
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
                        topology = ?self
                            .pipeline_context
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
                let supervisors = self.pipeline_context.stage_supervisors.read().await;
                for (stage_id, stage) in supervisors.iter() {
                    if stage.is_ready()
                        && !self
                            .pipeline_context
                            .topology
                            .upstream_stages(stage_id.to_topology_id())
                            .is_empty()
                    {
                        // This is a non-source stage that's already running
                        self.pipeline_context
                            .running_stages
                            .write()
                            .await
                            .insert(*stage_id);
                        tracing::info!("Stage '{}' was already running", stage.stage_name());
                    }
                }
                drop(supervisors);

                // Poll for stage running events to know when all non-source stages are ready
                let mut sub_guard = self.pipeline_context.completion_subscription.write().await;
                let subscription = sub_guard.as_mut().ok_or(
                    "No subscription available - should have been initialized during materialization",
                )?;

                // Check for stage running events
                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        drop(sub_guard);

                        // Process stage running event
                        let event = &envelope.event;
                        if let obzenflow_core::event::SystemEventType::StageLifecycle {
                            stage_id,
                            event: obzenflow_core::event::StageLifecycleEvent::Running,
                        } = &event.event
                        {
                            // Track this stage as running
                            self.pipeline_context
                                .running_stages
                                .write()
                                .await
                                .insert(stage_id.clone());

                            // Get stage name from topology
                            let stage_info = self
                                .pipeline_context
                                .topology
                                .stages()
                                .find(|s| s.id == stage_id.to_topology_id());
                            let stage_name = stage_info
                                .map(|s| s.name.clone())
                                .unwrap_or_else(|| "unknown".to_string());

                            // Log based on stage type
                            if self
                                .pipeline_context
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
                let running_stages = self.pipeline_context.running_stages.read().await;
                let topology = &self.pipeline_context.topology;

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
                let mut sub_guard = self.pipeline_context.completion_subscription.write().await;
                let subscription = sub_guard.as_mut().ok_or(
                    "No subscription available - should have been initialized during materialization",
                )?;

                // Use poll_next for non-blocking event polling
                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        drop(sub_guard);

                        let event = &envelope.event;

                        return match &event.event {
                            obzenflow_core::event::SystemEventType::StageLifecycle { stage_id, event: lifecycle_event } => {
                                match lifecycle_event {
                                    obzenflow_core::event::StageLifecycleEvent::Running => {
                                        // Get stage name from topology
                                        let stage_info = self.pipeline_context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        tracing::info!("Stage '{}' is now running", stage_name);
                                        Ok(EventLoopDirective::Continue)
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Draining => {
                                        let stage_info = self.pipeline_context.topology
                                            .stages()
                                            .find(|s| s.id == stage_id.to_topology_id());
                                        let stage_name = stage_info
                                            .map(|s| s.name.clone())
                                            .unwrap_or_else(|| "unknown".to_string());
                                        tracing::info!("Stage '{}' is draining", stage_name);
                                        Ok(EventLoopDirective::Continue)
                                    }
                                    obzenflow_core::event::StageLifecycleEvent::Drained => {
                                        let stage_info = self.pipeline_context.topology
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
                                        let stage_info = self.pipeline_context.topology
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
                                            .missing_contract_abort()
                                            .await
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
                            obzenflow_core::event::SystemEventType::ContractStatus { upstream, reader, pass, reader_seq, advertised_writer_seq, reason } => {
                                {
                                    let mut pair_status = self.pipeline_context.contract_pairs.write().await;
                                    if *pass {
                                        pair_status.insert(
                                            (*upstream, *reader),
                                            ContractEdgeStatus::passed(*reader_seq, *advertised_writer_seq),
                                        );
                                    } else {
                                        pair_status.insert(
                                            (*upstream, *reader),
                                            ContractEdgeStatus::failed(
                                                reason.clone(),
                                                *reader_seq,
                                                *advertised_writer_seq,
                                            ),
                                        );
                                    }
                                }

                                if !pass {
                                    tracing::error!(?upstream, ?reader, ?reason, "Contract status failure");
                                    return Ok(EventLoopDirective::Transition(PipelineEvent::Abort {
                                        reason: reason.clone().unwrap_or_else(|| obzenflow_core::event::types::ViolationCause::Other("contract_failed".into())),
                                        upstream: Some(*upstream),
                                    }));
                                }

                                let mut status = self.pipeline_context.contract_status.write().await;
                                status.insert(*upstream, true);
                                let expected = self.pipeline_context.expected_sources.as_ref();
                                let all_pass = !expected.is_empty()
                                    && expected
                                        .iter()
                                        .all(|src| status.get(src).copied().unwrap_or(false));
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
                let mut sub_guard = self.pipeline_context.completion_subscription.write().await;
                let subscription = sub_guard.as_mut().ok_or("No subscription available")?;

                use crate::messaging::PollResult;
                match subscription.poll_next().await {
                    PollResult::Event(envelope) => {
                        drop(sub_guard);

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
                                {
                                    let mut pair_status =
                                        self.pipeline_context.contract_pairs.write().await;
                                    if *pass {
                                        pair_status.insert(
                                            (*upstream, *reader),
                                            ContractEdgeStatus::passed(
                                                *reader_seq,
                                                *advertised_writer_seq,
                                            ),
                                        );
                                    } else {
                                        pair_status.insert(
                                            (*upstream, *reader),
                                            ContractEdgeStatus::failed(
                                                reason.clone(),
                                                *reader_seq,
                                                *advertised_writer_seq,
                                            ),
                                        );
                                    }
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
                                    let mut status =
                                        self.pipeline_context.contract_status.write().await;
                                    status.insert(upstream.clone(), true);
                                }
                                Ok(EventLoopDirective::Continue)
                            }
                            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                                obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted,
                            ) => {
                                tracing::info!(
                                    "All stages have completed - transitioning to drained"
                                );
                                if let Some(abort_directive) = self.missing_contract_abort().await {
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
                        drop(sub_guard);
                        if self.should_log_barrier() {
                            let snapshot = self.barrier_snapshot().await;
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
                        if self.all_stages_and_contracts_complete().await {
                            // Synthesize AllStagesCompleted when everything is done
                            if let Err(e) = self.write_all_stages_completed().await {
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
                                let snapshot = self.barrier_snapshot().await;
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
    async fn missing_contract_abort(&self) -> Option<EventLoopDirective<PipelineEvent>> {
        let seen = self.pipeline_context.contract_pairs.read().await;

        // Find any edge with an explicit failure (contract violated)
        if let Some(((upstream, reader), status)) =
            seen.iter().find(|(_pair, status)| !status.is_passed())
        {
            let upstream_name = self
                .pipeline_context
                .topology
                .stage_name(upstream.to_topology_id())
                .unwrap_or("unknown")
                .to_string();
            let reader_name = self
                .pipeline_context
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
    async fn all_stages_and_contracts_complete(&self) -> bool {
        let completed = self.pipeline_context.completed_stages.read().await.len();
        let total = self.pipeline_context.topology.num_stages();

        if completed < total {
            return false;
        }

        let seen = self.pipeline_context.contract_pairs.read().await;

        // Success requires that no contract edge has an explicit failure recorded.
        // Missing contract evidence is tolerated here; it is surfaced via logs/metrics
        // but does not block drain at the transport-contract layer.
        !seen.values().any(|status| !status.is_passed())
    }

    /// Synthesize and write AllStagesCompleted when we know we’re done
    async fn write_all_stages_completed(&self) -> Result<(), String> {
        let event = SystemEvent::new(
            WriterId::from(self.pipeline_context.system_id),
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
    async fn barrier_snapshot(&self) -> BarrierSnapshot {
        let completed: Vec<StageId> = self.pipeline_context.completed_stages.read().await.clone();
        let expected_stages: Vec<StageId> = self
            .pipeline_context
            .topology
            .stages()
            .map(|s| StageId::from_topology_id(s.id))
            .collect();
        let pending_stages: Vec<StageId> = expected_stages
            .iter()
            .copied()
            .filter(|id| !completed.contains(id))
            .collect();

        let expected_contracts = self.pipeline_context.expected_contract_pairs.clone();
        let seen = self.pipeline_context.contract_pairs.read().await;
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
