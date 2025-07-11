//! Handler-supervised state machine implementation
//!
//! This module provides supervision for state machines that delegate to handlers,
//! such as source, transform, and sink supervisors.

use super::base::{EventLoopDirective, Supervisor};
use obzenflow_fsm::{FsmAction, StateVariant};
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

        loop {
            // Get current state
            let current_state = machine.state().clone();

            // Get directive from the supervisor's dispatch logic
            let directive = self.dispatch_state(&current_state).await?;

            match directive {
                EventLoopDirective::Continue => continue,

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
                    self.write_completion_event(machine.state().variant_name())
                        .await?;
                    break;
                }
            }
        }

        Ok(())
    }
    
    /// Helper to spawn a task and return the handle
    /// Useful for handler-based supervisors that need to spawn processing tasks
    async fn spawn_task<F>(
        future: F,
    ) -> JoinHandle<()>
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
