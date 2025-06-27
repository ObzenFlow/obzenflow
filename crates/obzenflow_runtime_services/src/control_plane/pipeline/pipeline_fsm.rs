//! Pipeline FSM using obzenflow_fsm
//!
//! This defines the pipeline state machine without the supervision logic

use crate::message_bus::FsmMessageBus;
use obzenflow_topology_services::stages::StageId;
use obzenflow_fsm::{FsmBuilder, StateMachine, Transition, StateVariant, EventVariant};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Pipeline states
#[derive(Clone, Debug, PartialEq)]
pub enum PipelineState {
    Created,
    Materializing,
    Materialized,
    Running,
    Draining,
    Drained,
    Failed { reason: String },
}

impl StateVariant for PipelineState {
    fn variant_name(&self) -> &str {
        match self {
            PipelineState::Created => "Created",
            PipelineState::Materializing => "Materializing",
            PipelineState::Materialized => "Materialized",
            PipelineState::Running => "Running",
            PipelineState::Draining => "Draining",
            PipelineState::Drained => "Drained",
            PipelineState::Failed { .. } => "Failed",
        }
    }
}

/// Pipeline events
#[derive(Clone, Debug)]
pub enum PipelineEvent {
    Materialize,
    MaterializationComplete,
    Run,
    Shutdown,
    StageCompleted { stage_id: StageId },
    AllStagesCompleted,
    Error { message: String },
}

impl EventVariant for PipelineEvent {
    fn variant_name(&self) -> &str {
        match self {
            PipelineEvent::Materialize => "Materialize",
            PipelineEvent::MaterializationComplete => "MaterializationComplete",
            PipelineEvent::Run => "Run",
            PipelineEvent::Shutdown => "Shutdown",
            PipelineEvent::StageCompleted { .. } => "StageCompleted",
            PipelineEvent::AllStagesCompleted => "AllStagesCompleted",
            PipelineEvent::Error { .. } => "Error",
        }
    }
}

/// Pipeline actions
#[derive(Clone, Debug)]
pub enum PipelineAction {
    CreateStages,
    NotifyStagesStart,
    NotifySourceStart,
    BeginDrain,
    Cleanup,
}

/// Pipeline context
#[derive(Clone)]
pub struct PipelineContext {
    /// Message bus for communication
    pub bus: Arc<FsmMessageBus>,
    
    /// Topology for structure queries
    pub topology: Arc<obzenflow_topology_services::topology::Topology>,
    
    /// Journal for event flow
    pub journal: Arc<dyn obzenflow_core::Journal>,
    
    /// Completed stages tracking
    pub completed_stages: Arc<RwLock<Vec<StageId>>>,
}

/// Type alias for our pipeline FSM
pub type PipelineFsm = StateMachine<PipelineState, PipelineEvent, PipelineContext, PipelineAction>;

/// Build the pipeline FSM with all transitions
pub fn build_pipeline_fsm() -> PipelineFsm {
    FsmBuilder::new(PipelineState::Created)
        .when("Created")
            .on("Materialize", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Materializing,
                    actions: vec![PipelineAction::CreateStages],
                })
            })
            .done()
        
        .when("Materializing")
            .on("MaterializationComplete", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Materialized,
                    actions: vec![PipelineAction::NotifyStagesStart],
                })
            })
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(Transition {
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
                Ok(Transition {
                    next_state: PipelineState::Running,
                    actions: vec![PipelineAction::NotifySourceStart],
                })
            })
            .done()
        
        .when("Running")
            .on("Shutdown", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Draining,
                    actions: vec![PipelineAction::BeginDrain],
                })
            })
            .on("Error", |_state, event, _ctx| {
                let event = event.clone();
                async move {
                    if let PipelineEvent::Error { message } = event {
                        Ok(Transition {
                            next_state: PipelineState::Failed { reason: message },
                            actions: vec![PipelineAction::Cleanup],
                        })
                    } else {
                        Err("Invalid event".to_string())
                    }
                }
            })
            .done()
        
        .when("Draining")
            .on("AllStagesCompleted", |_state, _event: &PipelineEvent, _ctx| async move {
                Ok(Transition {
                    next_state: PipelineState::Drained,
                    actions: vec![PipelineAction::Cleanup],
                })
            })
            .done()
        
        .build()
}