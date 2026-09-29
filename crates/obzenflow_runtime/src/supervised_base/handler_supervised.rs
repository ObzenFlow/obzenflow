// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler-supervised state machine implementation
//!
//! This module provides supervision for state machines that delegate to handlers,
//! such as source, transform, and sink supervisors.

use super::base::{self, EventLoopDirective, Supervisor};
use super::publication::BoxError;
use crate::stages::common::stage_handle::StageError;
use futures::future::BoxFuture;
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisionMode;
use obzenflow_core::event::status::processing_status::ProcessingStatus;
use obzenflow_core::event::WriterId;
use obzenflow_core::ChainEvent;
use obzenflow_fsm::{FsmAction, FsmError};
use std::collections::VecDeque;
use std::error::Error;
use std::future::Future;
use tokio::task::JoinHandle;

/// Host obligations selected explicitly by a stage transition.
#[derive(Clone, Debug)]
pub enum SupervisorAction<E> {
    Register,
    CloseMailbox,
    Cleanup,
    SettlePublications,
    Emit(E),
}

/// An operation owns everything it needs until it returns resources to its
/// context. Polling a control command never drops this future. Forced task
/// abortion remains a distinct task-owner operation.
pub type OwnedAction<C, E> = BoxFuture<'static, ActionCompletion<C, E>>;
pub type ActionCompletion<C, E> = Box<dyn FnOnce(&mut C) -> Result<Option<E>, BoxError> + Send>;

pub enum ActionExecution<C, E> {
    Completed,
    Pending(OwnedAction<C, E>),
}

/// One processing turn retains the stage's operation resources while control
/// events continue to reach its FSM. Completion returns both the resources and
/// the resulting event to their owner; interruption never drops handler work.
pub type OwnedDispatch<S> = BoxFuture<'static, DispatchCompletion<S>>;
pub type DispatchCompletion<S> = Box<
    dyn FnOnce(
            &mut S,
            &mut <S as Supervisor>::Context,
        )
            -> Result<EventLoopDirective<<S as Supervisor>::Event>, Box<dyn Error + Send + Sync>>
        + Send,
>;

/// Trait for handler-supervised components
/// This ensures they provide handler access while still going through FSM
#[async_trait::async_trait]
pub trait HandlerSupervised: Supervisor + Sync {
    type Handler: Send + Sync;

    /// Dispatch state logic with access to handler
    /// Similar to SelfSupervised but with handler access and mutable FSM context
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>>;

    fn owned_dispatch(
        &mut self,
        _state: &Self::State,
        _context: &mut Self::Context,
    ) -> Option<OwnedDispatch<Self>>
    where
        Self: Sized,
    {
        None
    }

    /// Concrete FSM states define achieved milestones and pending settlement.
    fn lifecycle_phase(
        &self,
        _state: &Self::State,
    ) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        Default::default()
    }

    fn accounting(
        &self,
        _context: &Self::Context,
    ) -> obzenflow_core::event::provenance::ExecutionAccounting {
        Default::default()
    }

    /// Called after the FSM assigns a state, before starting its operations.
    fn after_transition(&mut self, _state: &Self::State, _context: &Self::Context) {}

    fn supervisor_action(&self, _action: &Self::Action) -> Option<SupervisorAction<Self::Event>> {
        None
    }

    /// Inline actions must finish before returning. An operation which needs
    /// concurrent command handling transfers its resources to a Pending action.
    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        action.execute(context).await?;
        Ok(ActionExecution::Completed)
    }

    async fn execute_cleanup(
        &mut self,
        _context: &Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        Ok(ActionExecution::Completed)
    }

    async fn next_control(
        &mut self,
        _state: &Self::State,
        _context: &mut Self::Context,
    ) -> Option<Self::Event> {
        std::future::pending().await
    }

    fn close_mailbox(
        &mut self,
        _state: &Self::State,
    ) -> BoxFuture<'static, Result<(), Box<dyn Error + Send + Sync>>> {
        Box::pin(async { Ok(()) })
    }

    /// Get the writer ID for this component
    fn writer_id(&self) -> WriterId;

    fn supervision_mode(&self) -> SupervisionMode {
        SupervisionMode::HandlerSupervised
    }

    /// Map an action error into a stage-specific failure event.
    ///
    /// This is used by the supervision loop to ensure that any action
    /// failure drives the FSM through an explicit failure path instead
    /// of terminating the task with an opaque error.
    fn event_for_action_error(&self, msg: String) -> Self::Event;

    /// Helper method to run a processing function only if the event doesn't have Error status
    /// If the event has Error status, it's passed through unchanged
    fn run_if_not_error<F>(&self, event: ChainEvent, next: F) -> Vec<ChainEvent>
    where
        F: FnOnce(ChainEvent) -> Vec<ChainEvent>,
    {
        if matches!(event.processing.status, ProcessingStatus::Error { .. }) {
            vec![event] // pass straight through
        } else {
            next(event)
        }
    }
}

/// Extension trait to add run functionality to any HandlerSupervised type
#[async_trait::async_trait]
pub trait HandlerSupervisedExt: HandlerSupervised {
    /// Execute the action plan returned by the FSM. A state change or a new nonempty plan replaces
    /// unstarted actions; an already-started operation remains owned until its
    /// completion has restored its resources. The FSM can therefore expose a
    /// failure immediately while the previous operation is still settling.
    async fn run(
        mut self,
        initial_state: Self::State,
        mut context: Self::Context,
    ) -> Result<(), Box<dyn Error + Send + Sync>>
    where
        Self: Sized,
        Self::State: Send + Sync + 'static,
        Self::Event: Send + Sync + 'static,
        Self::Context: 'static,
        Self::Action: 'static,
    {
        use crate::stages::common::stage_lifecycle::LifecycleResults;

        fn owned_unit<C: 'static, E: Send + 'static>(
            future: BoxFuture<'static, Result<(), Box<dyn Error + Send + Sync>>>,
        ) -> OwnedAction<C, E> {
            Box::pin(async move {
                let result = future.await;
                Box::new(move |_: &mut C| result.map(|()| None)) as ActionCompletion<C, E>
            })
        }

        let mut machine = self.build_state_machine(initial_state);
        let mut actions = VecDeque::new();
        let mut operation: Option<OwnedAction<Self::Context, Self::Event>> = None;
        let mut dispatch: Option<OwnedDispatch<Self>> = None;
        let mut publication_failure_delivered = false;
        loop {
            let state = machine.state().clone();
            let publications = super::publication::PublicationScope::current();
            let mut failure = None;
            let mut retain_error = |error: BoxError| {
                let message = error.to_string();
                failure = Some(StageError::Execution(error.into()));
                message
            };
            let directive = if let Some(future) = operation.as_mut() {
                tokio::select! {
                    biased;
                    error = async {
                        match &publications {
                            Some(scope) => scope.wait_for_failure().await,
                            None => std::future::pending().await,
                        }
                    }, if !publication_failure_delivered => {
                        publication_failure_delivered = true;
                        EventLoopDirective::Transition(self.event_for_action_error(retain_error(error.into())))
                    }
                    Some(event) = self.next_control(&state, &mut context) => EventLoopDirective::Transition(event),
                    complete = future => {
                        operation = None;
                        match complete(&mut context) {
                            Ok(None) => continue,
                            Ok(Some(event)) => EventLoopDirective::Transition(event),
                            Err(error) => EventLoopDirective::Transition(self.event_for_action_error(retain_error(error))),
                        }
                    }
                }
            } else if let Some(future) = dispatch.as_mut() {
                tokio::select! {
                    biased;
                    error = async {
                        match &publications {
                            Some(scope) => scope.wait_for_failure().await,
                            None => std::future::pending().await,
                        }
                    }, if !publication_failure_delivered => {
                        publication_failure_delivered = true;
                        EventLoopDirective::Transition(self.event_for_action_error(retain_error(error.into())))
                    }
                    Some(event) = self.next_control(&state, &mut context) => EventLoopDirective::Transition(event),
                    complete = future => {
                        dispatch = None;
                        match complete(&mut self, &mut context) {
                            Ok(directive) => directive,
                            Err(error) => EventLoopDirective::Transition(self.event_for_action_error(retain_error(error))),
                        }
                    }
                }
            } else if let Some(action) = actions.pop_front() {
                match self.supervisor_action(&action) {
                    Some(SupervisorAction::Register) => {
                        operation = Some(owned_unit(base::register(
                            &self,
                            &context,
                            self.writer_id(),
                            self.supervision_mode(),
                        )));
                        continue;
                    }
                    Some(SupervisorAction::CloseMailbox) => {
                        operation = Some(owned_unit(self.close_mailbox(&state)));
                        continue;
                    }
                    Some(SupervisorAction::Cleanup) => match self.execute_cleanup(&context).await {
                        Ok(ActionExecution::Completed) => continue,
                        Ok(ActionExecution::Pending(future)) => {
                            operation = Some(future);
                            continue;
                        }
                        Err(error) => EventLoopDirective::Transition(
                            self.event_for_action_error(retain_error(error.into())),
                        ),
                    },
                    Some(SupervisorAction::SettlePublications) => {
                        operation = Some(owned_unit(Box::pin(async move {
                            if let Some(scope) = publications {
                                scope.join().await?;
                            }
                            Ok(())
                        })));
                        continue;
                    }
                    Some(SupervisorAction::Emit(event)) => EventLoopDirective::Transition(event),
                    None => match self.execute_action(action, &mut context).await {
                        Ok(ActionExecution::Completed) => continue,
                        Ok(ActionExecution::Pending(future)) => {
                            operation = Some(future);
                            continue;
                        }
                        Err(error) => EventLoopDirective::Transition(
                            self.event_for_action_error(retain_error(error.into())),
                        ),
                    },
                }
            } else if let Some(owned) = self.owned_dispatch(&state, &mut context) {
                dispatch = Some(owned);
                continue;
            } else {
                match self.dispatch_state(&state, &mut context).await {
                    Ok(directive) => directive,
                    Err(error) => EventLoopDirective::Transition(
                        self.event_for_action_error(retain_error(error)),
                    ),
                }
            };
            let event = match directive {
                EventLoopDirective::Continue => {
                    tokio::task::yield_now().await;
                    continue;
                }
                EventLoopDirective::Terminate => return Ok(()),
                EventLoopDirective::Transition(event) => event,
            };
            let previous_state = machine.state().clone();
            let selected = machine
                .handle(event, &mut context)
                .await
                .map_err(|error| format!("FSM error: {error}"))?;
            let state = machine.state().clone();
            self.after_transition(&state, &context);
            LifecycleResults::observe(
                &self.lifecycle_phase(&state),
                self.accounting(&context),
                failure,
            );
            if state != previous_state || !selected.is_empty() {
                actions = selected.into();
            }
        }
    }

    /// Helper to spawn a task and return the handle
    /// Useful for handler-based supervisors that need to spawn processing tasks
    async fn spawn_task<F>(future: F) -> JoinHandle<()>
    where
        F: Future<Output = ()> + Send + 'static,
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
