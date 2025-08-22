//! Pipeline Supervisor - Manages the lifecycle of a pipeline and its stages
//!
//! The supervisor pattern provides:
//! - Hierarchical ownership of FSMs
//! - Message bus for inter-FSM communication
//! - Clean separation between supervision and business logic

use super::{
    fsm::{PipelineAction, PipelineContext, PipelineEvent, PipelineState},
};
use crate::supervised_base::{EventLoopDirective, SelfSupervised};
use obzenflow_core::event::{SystemEvent, WriterId};
use obzenflow_core::journal::journal::Journal;
use obzenflow_core::StageId;
use crate::id_conversions::StageIdExt;
use std::sync::Arc;

/// Pipeline supervisor - manages the lifecycle of a pipeline
pub(crate) struct PipelineSupervisor {
    /// Supervisor name
    pub(crate) name: String,

    /// Pipeline context (contains all mutable state)
    pub(crate) pipeline_context: Arc<PipelineContext>,

    /// System journal for pipeline orchestration events
    pub(crate) system_journal: Arc<dyn Journal<SystemEvent>>,
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
            .on(
                "Materialize",
                |_state, _event: &PipelineEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::Materializing,
                        actions: vec![PipelineAction::CreateStages],
                    })
                },
            )
            .done()
            .when("Materializing")
            .on(
                "MaterializationComplete",
                |_state, _event: &PipelineEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::Materialized,
                        actions: vec![
                            PipelineAction::StartCompletionSubscription,
                            PipelineAction::StartMetricsAggregator,
                            PipelineAction::NotifyStagesStart,
                        ],
                    })
                },
            )
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Failed { reason: message },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .done()
            .when("Materialized")
            .on("Run", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(obzenflow_fsm::Transition {
                    next_state: PipelineState::Running,
                    actions: vec![PipelineAction::NotifySourceStart],
                })
            })
            .done()
            .when("Running")
            .on(
                "Shutdown",
                |_state, _event: &PipelineEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::SourceCompleted,
                        actions: vec![], // No actions yet - just track state
                    })
                },
            )
            .on("StageCompleted", |_state, event, _ctx| {
                let event = event.clone();
                async move {
                    if let PipelineEvent::StageCompleted { envelope } = event {
                        // Just track the completion, stay in Running state
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Running,
                            actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Failed { reason: message },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .on("AllStagesCompleted", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(obzenflow_fsm::Transition {
                    next_state: PipelineState::Drained,
                    actions: vec![
                        PipelineAction::DrainMetrics,
                        PipelineAction::WritePipelineCompleted,
                        PipelineAction::Cleanup
                    ],
                })
            })
            .done()
            .when("SourceCompleted")
            .on(
                "BeginDrain",
                |_state, _event: &PipelineEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::Draining,
                        actions: vec![PipelineAction::BeginDrain],
                    })
                },
            )
            .done()
            .when("Draining")
            .on("StageCompleted", |_state, event, _ctx| {
                let event = event.clone();
                async move {
                    if let PipelineEvent::StageCompleted { envelope } = event {
                        // Track completion and check if all stages are done
                        Ok(obzenflow_fsm::Transition {
                            next_state: PipelineState::Draining,
                            actions: vec![PipelineAction::HandleStageCompleted { envelope }],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .on(
                "AllStagesCompleted",
                |_state, _event: &PipelineEvent, _ctx| async move {
                    Ok(obzenflow_fsm::Transition {
                        next_state: PipelineState::Drained,
                        actions: vec![
                            PipelineAction::DrainMetrics,
                            PipelineAction::WritePipelineCompleted,
                            PipelineAction::Cleanup,
                        ],
                    })
                },
            )
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
                obzenflow_core::event::PipelineLifecycleEvent::Completed
            )
        );
        self.system_journal
            .append(event, None)
            .await
            .map(|_| ())
            .map_err(|e| format!("Failed to write completion event: {}", e).into())
    }
    
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        match state {
            PipelineState::Created => {
                // In Created state, we wait for external trigger to materialize
                // This would typically come from the FlowHandle
                Ok(EventLoopDirective::Continue)
            }

            PipelineState::Materializing => {
                // Check if all stages have been initialized
                let supervisors = self.pipeline_context.stage_supervisors.read().await;
                let expected_count = self.pipeline_context.topology.stages().count();
                let initialized_count = supervisors.len();
                drop(supervisors);
                
                if initialized_count == expected_count && expected_count > 0 {
                    // All stages created, transition to Materialized
                    Ok(EventLoopDirective::Transition(
                        PipelineEvent::MaterializationComplete,
                    ))
                } else {
                    // Still waiting for stages to be created
                    Ok(EventLoopDirective::Continue)
                }
            }

            PipelineState::Materialized => {
                // First check if any stages are already running (they might have published before we subscribed)
                let supervisors = self.pipeline_context.stage_supervisors.read().await;
                for (stage_id, stage) in supervisors.iter() {
                    if stage.is_ready() && !self.pipeline_context.topology.upstream_stages(stage_id.to_topology_id()).is_empty() {
                        // This is a non-source stage that's already running
                        self.pipeline_context.running_stages.write().await.insert(*stage_id);
                        tracing::info!("Stage '{}' was already running", stage.stage_name());
                    }
                }
                drop(supervisors);
                
                // Poll for stage running events to know when all non-source stages are ready
                let mut reader_guard =
                    self.pipeline_context.completion_reader.write().await;
                let reader = reader_guard
                    .as_mut()
                    .ok_or("No reader available - should have been initialized during materialization")?;

                // Check for stage running events with timeout
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(100),
                    reader.next(),
                )
                .await
                {
                    Ok(Ok(Some(envelope))) => {
                        drop(reader_guard);
                        
                        // Process stage running event
                        let event = &envelope.event;
                        if let obzenflow_core::event::SystemEventType::StageLifecycle { 
                            stage_id, 
                            event: obzenflow_core::event::StageLifecycleEvent::Running 
                        } = &event.event {
                            // Track this stage as running
                            self.pipeline_context.running_stages.write().await.insert(*stage_id);
                            
                            // Get stage name from topology
                            let stage_info = self.pipeline_context.topology
                                .stages()
                                .find(|s| s.id == stage_id.to_topology_id());
                            let stage_name = stage_info
                                .map(|s| s.name.clone())
                                .unwrap_or_else(|| "unknown".to_string());
                            
                            // Log based on stage type
                            if self.pipeline_context.topology.upstream_stages(stage_id.to_topology_id()).is_empty() {
                                tracing::debug!("Source stage '{}' is running (waiting for pipeline signal)", stage_name);
                            } else {
                                tracing::info!("Stage '{}' is now running", stage_name);
                            }
                        }
                    }
                    _ => {
                        // No new events, but that's OK - stages might already be running
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
                let all_ready = !non_source_stages.is_empty() && 
                    non_source_stages.iter().all(|stage_id| running_stages.contains(stage_id));
                
                if all_ready {
                    tracing::info!(
                        "All {} non-source stages are running, starting pipeline", 
                        non_source_stages.len()
                    );
                    Ok(EventLoopDirective::Transition(PipelineEvent::Run))
                } else {
                    // Still waiting for stages to report running
                    let waiting_for = non_source_stages
                        .difference(&*running_stages)
                        .count();
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
                // Poll for completion events from stages
                let mut reader_guard =
                    self.pipeline_context.completion_reader.write().await;
                let reader = reader_guard
                    .as_mut()
                    .ok_or("No reader available - should have been initialized during materialization")?;

                // Use timeout to allow periodic state checks while waiting for events
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(100),
                    reader.next(),
                )
                .await
                {
                    Ok(Ok(Some(envelope))) => {
                        drop(reader_guard);
                        
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
                                        Ok(EventLoopDirective::Transition(
                                            PipelineEvent::AllStagesCompleted,
                                        ))
                                    }
                                    _ => Ok(EventLoopDirective::Continue)
                                }
                            }
                            _ => Ok(EventLoopDirective::Continue)
                        };
                    }
                    _ => {
                        // No events or timeout - continue running
                    }
                }

                Ok(EventLoopDirective::Continue)
            }

            PipelineState::SourceCompleted => {
                // Source has completed - initiate Jonestown protocol
                tracing::info!("Source completed - beginning pipeline drain");

                // Immediately transition to start draining
                Ok(EventLoopDirective::Transition(PipelineEvent::BeginDrain))
            }

            PipelineState::Draining => {
                // Continue polling for completion events during drain
                let mut reader_guard =
                    self.pipeline_context.completion_reader.write().await;
                let reader = reader_guard
                    .as_mut()
                    .ok_or("No reader available")?;

                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(100),
                    reader.next(),
                )
                .await
                {
                    Ok(Ok(Some(envelope))) => {
                        drop(reader_guard);
                        
                        let event = &envelope.event;

                        return match &event.event {
                            obzenflow_core::event::SystemEventType::StageLifecycle { 
                                stage_id: _, 
                                event: obzenflow_core::event::StageLifecycleEvent::Completed 
                            } => {
                                // Process stage completion immediately
                                Ok(EventLoopDirective::Transition(
                                    PipelineEvent::StageCompleted { envelope },
                                ))
                            }
                            obzenflow_core::event::SystemEventType::PipelineLifecycle(
                                obzenflow_core::event::PipelineLifecycleEvent::AllStagesCompleted
                            ) => {
                                tracing::info!(
                                    "All stages have completed - transitioning to drained"
                                );
                                Ok(EventLoopDirective::Transition(
                                    PipelineEvent::AllStagesCompleted,
                                ))
                            }
                            _ => {
                                // Log other events
                                tracing::debug!(
                                    "Received event during drain: {:?}",
                                    event.event
                                );
                                Ok(EventLoopDirective::Continue)
                            }
                        };
                    }
                    _ => {
                        // No events - continue draining
                    }
                }

                Ok(EventLoopDirective::Continue)
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
        }
    }
}
