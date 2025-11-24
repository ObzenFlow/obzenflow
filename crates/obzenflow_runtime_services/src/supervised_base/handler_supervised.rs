//! Handler-supervised state machine implementation
//!
//! This module provides supervision for state machines that delegate to handlers,
//! such as source, transform, and sink supervisors.

use super::base::{EventLoopDirective, Supervisor};
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::WriterId;
use obzenflow_core::{ChainEvent, StageId};
use obzenflow_fsm::FsmAction;
use obzenflow_fsm::StateVariant;
use tokio::task::JoinHandle;

/// Trait for handler-supervised components
/// This ensures they provide handler access while still going through FSM
#[async_trait::async_trait]
pub trait HandlerSupervised: Supervisor {
    type Handler: Send + Sync;

    /// Dispatch state logic with access to handler
    /// Similar to SelfSupervised but with handler access
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get the writer ID for this component
    fn writer_id(&self) -> WriterId;

    /// Get the stage ID for this component
    fn stage_id(&self) -> StageId;

    /// Write a completion event when the stage terminates
    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Helper method to run a processing function only if the event doesn't have Error status
    /// If the event has Error status, it's passed through unchanged
    fn run_if_not_error<F>(&self, event: ChainEvent, next: F) -> Vec<ChainEvent>
    where
        F: FnOnce(ChainEvent) -> Vec<ChainEvent>,
    {
        if matches!(event.processing_info.status, ProcessingStatus::Error(_)) {
            vec![event] // pass straight through
        } else {
            next(event)
        }
    }
}

/// Extension trait to add run functionality to any HandlerSupervised type
pub trait HandlerSupervisedExt: HandlerSupervised {
    /// Run the supervision loop
    async fn run(
        mut self,
        initial_state: Self::State,
        context: Self::Context,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        Self: Sized,
        Self::State: Send + Sync + 'static,
        Self::Event: Send + Sync + 'static,
        Self::Context: 'static,
        Self::Action: 'static,
    {
        let context = std::sync::Arc::new(context);

        // Create the builder and let the supervisor configure it
        let builder = obzenflow_fsm::FsmBuilder::new(initial_state);
        let configured_builder = self.configure_fsm(builder);

        // Build the state machine
        let mut machine = configured_builder.build();
        let mut loop_iteration: u64 = 0;

        loop {
            loop_iteration += 1;

            // Get current state
            let current_state = machine.state().clone();

            tracing::debug!(
                target: "obzenflow_runtime_services::supervised_base::handler_supervised",
                iteration = loop_iteration,
                state = %current_state.variant_name(),
                "HandlerSupervised::run loop iteration start"
            );

            // Get directive from the supervisor's dispatch logic
            let directive = self.dispatch_state(&current_state).await?;

            tracing::debug!(
                target: "obzenflow_runtime_services::supervised_base::handler_supervised",
                iteration = loop_iteration,
                state = %current_state.variant_name(),
                directive = ?directive,
                "HandlerSupervised::run dispatch_state returned directive"
            );

            match directive {
                EventLoopDirective::Continue => {
                    // Yield to prevent busy loop when waiting for external events
                    tokio::task::yield_now().await;
                    continue;
                }

                EventLoopDirective::Transition(event) => {
                    let actions = machine
                        .handle(event, context.clone())
                        .await
                        .map_err(|e| format!("FSM error: {}", e))?;

                    for action in actions {
                        action
                            .execute(&context)
                            .await
                            .map_err(|e| format!("Action error: {}", e))?;
                    }
                }

                EventLoopDirective::Terminate => {
                    self.write_completion_event().await?;
                    break;
                }
            }
        }

        Ok(())
    }

    /// Helper to spawn a task and return the handle
    /// Useful for handler-based supervisors that need to spawn processing tasks
    async fn spawn_task<F>(future: F) -> JoinHandle<()>
    where
        F: std::future::Future<Output = ()> + Send + 'static,
    {
        tokio::spawn(future)
    }

    /// Helper to cancel a task handle
    async fn cancel_task(handle: JoinHandle<()>) {
        handle.abort();
    }
}

// Blanket implementation - any type that implements HandlerSupervised gets run() for free
impl<T: HandlerSupervised> HandlerSupervisedExt for T {}
