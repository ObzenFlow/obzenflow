//! Self-supervised state machine implementation
//!
//! This module provides supervision for state machines that contain their own logic,
//! such as the metrics aggregator and pipeline supervisor.

use super::base::{EventLoopDirective, Supervisor};
use obzenflow_core::event::WriterId;
use obzenflow_fsm::FsmAction;

/// Trait that self-supervised components MUST implement
/// This ensures they provide all required functionality
///
/// By implementing SelfSupervised, you get:
/// - Enforced implementation of build_state_machine()
/// - Enforced implementation of dispatch_state()
/// - Free run() method via SelfSupervisedExt
/// - No way to bypass the supervised pattern
#[async_trait::async_trait]
pub trait SelfSupervised: Supervisor {
    /// Run the state dispatch logic for the current state
    /// Returns a directive indicating what to do next
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>>;

    /// Get the writer ID for this component
    fn writer_id(&self) -> WriterId;

    /// Write a completion event when the component terminates
    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;
}

/// Extension trait to add run functionality to any SelfSupervised type
pub trait SelfSupervisedExt: SelfSupervised {
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
        let supervisor_name = self.name().to_string();
        let supervisor_writer = self.writer_id();
        tracing::info!(
            supervisor = %supervisor_name,
            writer_id = ?supervisor_writer,
            "SelfSupervised::run() starting with initial state: {:?}",
            initial_state
        );
        let mut context = context;

        // Build the state machine via the Supervisor API
        tracing::debug!(
            supervisor = %supervisor_name,
            "Building FSM via Supervisor::build_state_machine"
        );
        let mut machine = self.build_state_machine(initial_state);

        let mut iteration = 0;
        loop {
            iteration += 1;

            // Get current state
            let current_state = machine.state().clone();
            // tracing::debug!("Loop iteration {}: Current state: {:?}", iteration, current_state);

            // Get directive from the supervisor's dispatch logic, with full access to context
            tracing::trace!("Calling dispatch_state for state: {:?}", current_state);
            let directive = self.dispatch_state(&current_state, &mut context).await?;
            tracing::debug!(
                supervisor = %supervisor_name,
                writer_id = ?supervisor_writer,
                "Loop iteration {}: dispatch_state returned: {:?}",
                iteration,
                directive
            );

            match directive {
                EventLoopDirective::Continue => {
                    tracing::trace!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Got Continue directive, continuing loop",
                        iteration
                    );
                    // Yield to prevent busy loop when waiting for external events
                    tokio::task::yield_now().await;
                    continue;
                }

                EventLoopDirective::Transition(event) => {
                    tracing::debug!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Transitioning with event: {:?}",
                        iteration,
                        event
                    );
                    let actions = machine
                        .handle(event, &mut context)
                        .await
                        .map_err(|e| format!("FSM error: {e}"))?;

                    tracing::debug!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Got {} actions to execute",
                        iteration,
                        actions.len()
                    );
                    for (i, action) in actions.iter().enumerate() {
                        tracing::debug!(
                            supervisor = %supervisor_name,
                            writer_id = ?supervisor_writer,
                            "Loop iteration {}: Executing action {}/{}: {:?}",
                            iteration,
                            i + 1,
                            actions.len(),
                            action
                        );
                        action
                            .execute(&mut context)
                            .await
                            .map_err(|e| format!("Action error: {e}"))?;
                        tracing::debug!(
                            supervisor = %supervisor_name,
                            writer_id = ?supervisor_writer,
                            "Loop iteration {}: Action {}/{} completed",
                            iteration,
                            i + 1,
                            actions.len()
                        );
                    }
                    tracing::debug!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Transition complete, new state: {:?}",
                        iteration,
                        machine.state()
                    );
                }

                EventLoopDirective::Terminate => {
                    tracing::info!(
                        supervisor = %supervisor_name,
                        writer_id = ?supervisor_writer,
                        "Loop iteration {}: Got Terminate directive",
                        iteration
                    );
                    self.write_completion_event().await?;
                    break;
                }
            }
        }

        tracing::info!(
            supervisor = %supervisor_name,
            writer_id = ?supervisor_writer,
            "SelfSupervised::run() completed"
        );
        Ok(())
    }
}

// Blanket implementation - any type that implements SelfSupervised gets run() for free
impl<T: SelfSupervised> SelfSupervisedExt for T {}

// Seal implementation - any type that implements SelfSupervised can be a Supervisor
impl<T: SelfSupervised> super::base::private::Sealed for T {}
