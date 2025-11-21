//! Message Bus for Inter-FSM Communication
//!
//! Provides type-safe channels for communication between Pipeline and Stage FSMs
//! without creating circular dependencies.

use crate::errors::MessageBusError;
use tokio::sync::broadcast;

/// Message bus for FSM communication
pub struct FsmMessageBus {
    /// Broadcast commands to all stages
    stage_commands: broadcast::Sender<StageCommand>,
}

impl FsmMessageBus {
    /// Create a new message bus
    pub fn new() -> Self {
        let (stage_commands, _) = broadcast::channel(16);

        Self { stage_commands }
    }

    /// Get a stage command receiver (for stages to subscribe)
    pub fn subscribe_to_stage_commands(&self) -> broadcast::Receiver<StageCommand> {
        self.stage_commands.subscribe()
    }

    /// Send a command to all stages
    #[track_caller]
    pub async fn send_stage_command(&self, command: StageCommand) -> Result<(), MessageBusError> {
        self.stage_commands.send(command).map_err(|_| {
            tracing::error!(
                location = %std::panic::Location::caller(),
                "No stages listening for commands - receiver count: {}",
                self.stage_commands.receiver_count()
            );
            MessageBusError::NoStageReceivers
        })?;
        Ok(())
    }
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
        let bus = FsmMessageBus::new();

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
