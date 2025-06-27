//! Message Bus for Inter-FSM Communication
//!
//! Provides type-safe channels for communication between Pipeline and Stage FSMs
//! without creating circular dependencies.

use obzenflow_topology_services::stages::StageId;
use tokio::sync::{mpsc, broadcast};
use crate::errors::MessageBusError;

/// Message bus for FSM communication
pub struct FsmMessageBus {
    /// Pipeline FSM receives these events
    pipeline_inbox_tx: mpsc::Sender<PipelineInternalEvent>,
    pipeline_inbox_rx: Option<mpsc::Receiver<PipelineInternalEvent>>,
    
    /// Broadcast commands to all stages
    stage_commands: broadcast::Sender<StageCommand>,
    
}

impl FsmMessageBus {
    /// Create a new message bus
    pub fn new() -> Self {
        let (pipeline_inbox_tx, pipeline_inbox_rx) = mpsc::channel(100);
        let (stage_commands, _) = broadcast::channel(16);
        
        Self {
            pipeline_inbox_tx,
            pipeline_inbox_rx: Some(pipeline_inbox_rx),
            stage_commands,
        }
    }
    
    /// Take the pipeline inbox receiver (can only be called once)
    pub fn take_pipeline_inbox_receiver(&mut self) -> Result<mpsc::Receiver<PipelineInternalEvent>, MessageBusError> {
        self.pipeline_inbox_rx.take()
            .ok_or(MessageBusError::ReceiverAlreadyTaken)
    }
    
    /// Get a stage command receiver (for stages to subscribe)
    pub fn subscribe_to_stage_commands(&self) -> broadcast::Receiver<StageCommand> {
        self.stage_commands.subscribe()
    }
    
    /// Send a command to all stages
    #[track_caller]
    pub async fn send_stage_command(&self, command: StageCommand) -> Result<(), MessageBusError> {
        self.stage_commands.send(command)
            .map_err(|_| {
                tracing::error!(
                    location = %std::panic::Location::caller(),
                    "No stages listening for commands - receiver count: {}",
                    self.stage_commands.receiver_count()
                );
                MessageBusError::NoStageReceivers
            })?;
        Ok(())
    }
    
    /// Send an internal event to the pipeline
    #[track_caller]
    pub async fn send_pipeline_event(&self, event: PipelineInternalEvent) -> Result<(), MessageBusError> {
        self.pipeline_inbox_tx.send(event).await
            .map_err(|_| {
                tracing::error!(
                    location = %std::panic::Location::caller(),
                    "Pipeline inbox dropped while sending event"
                );
                MessageBusError::PipelineInboxDropped
            })
    }
}

/// Internal events for the pipeline FSM
#[derive(Clone, Debug)]
pub enum PipelineInternalEvent {
    /// A stage has been created and initialized
    StageReady { stage_id: StageId },
    
    /// All stages are ready
    AllStagesReady,
    
    /// Drain progress update
    DrainProgress { completed: usize, total: usize },
}

/// Commands that pipeline sends to stages
#[derive(Clone, Debug)]
pub enum StageCommand {
    /// Initialize the stage
    Initialize,
    
    /// Start processing (for sources after gun fires)
    Start,
    
    /// Begin graceful drain
    BeginDrain,
    
    /// Force immediate shutdown
    ForceShutdown { reason: String },
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_message_bus_creation() {
        let mut bus = FsmMessageBus::new();
        
        // Can take receivers once
        let _pipeline_rx = bus.take_pipeline_inbox_receiver();
        let _stage_rx = bus.take_stage_events_receiver();
        
        // Can subscribe multiple times to broadcasts
        let _sub1 = bus.subscribe_to_stage_commands();
        let _sub2 = bus.subscribe_to_stage_commands();
    }
    
    #[tokio::test]
    async fn test_stage_command_broadcast() {
        let bus = FsmMessageBus::new();
        
        // Create two subscribers
        let mut sub1 = bus.subscribe_to_stage_commands();
        let mut sub2 = bus.subscribe_to_stage_commands();
        
        // Send a command
        bus.send_stage_command(StageCommand::Start).await.unwrap();
        
        // Both should receive it
        let cmd1 = sub1.recv().await.unwrap();
        let cmd2 = sub2.recv().await.unwrap();
        
        assert!(matches!(cmd1, StageCommand::Start));
        assert!(matches!(cmd2, StageCommand::Start));
    }
}