// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Self-supervised state machine implementation
//!
//! This module provides supervision for state machines that contain their own logic,
//! such as the metrics aggregator and pipeline supervisor.

use super::base::{EventLoopDirective, Supervisor};
use super::handler_supervised::{
    ActionExecution, HandlerSupervised, HandlerSupervisedExt, SupervisorAction,
};
use crate::stages::common::stage_lifecycle::LifecyclePhase;
use futures::future::BoxFuture;
use obzenflow_core::event::WriterId;
use obzenflow_fsm::{FsmAction, FsmError};
use std::error::Error;

type BoxError = Box<dyn Error + Send + Sync>;

/// Self-contained supervisors select the same explicit host obligations as
/// handler stages. Both families use one operation-owning runner.
#[async_trait::async_trait]
pub trait SelfSupervised: Supervisor + Sync {
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, BoxError>;
    fn writer_id(&self) -> WriterId;
    fn event_for_action_error(&self, msg: String) -> Self::Event;
    fn after_transition(&mut self, _state: &Self::State, _context: &Self::Context) {}
    fn lifecycle_phase(&self, _state: &Self::State) -> LifecyclePhase {
        LifecyclePhase::Other
    }
    fn supervisor_action(&self, _action: &Self::Action) -> Option<SupervisorAction<Self::Event>> {
        None
    }
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
    fn close_mailbox(&mut self, _state: &Self::State) -> BoxFuture<'static, Result<(), BoxError>> {
        Box::pin(async { Ok(()) })
    }
}

/// Delegation only: the concrete supervisor retains its FSM, commands and
/// resources. This adapter adds no lifecycle decision or task.
struct SelfRunner<S>(S);

impl<S: SelfSupervised> Supervisor for SelfRunner<S> {
    type State = S::State;
    type Event = S::Event;
    type Context = S::Context;
    type Action = S::Action;
    fn build_state_machine(
        &self,
        state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        self.0.build_state_machine(state)
    }
    fn name(&self) -> &str {
        self.0.name()
    }
    fn event_prefix(&self) -> String {
        self.0.event_prefix()
    }
    fn supervisor_kind(
        &self,
    ) -> obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind {
        self.0.supervisor_kind()
    }
    fn registration(
        &self,
        context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        self.0.registration(context, descriptor)
    }
}

#[async_trait::async_trait]
impl<S: SelfSupervised + Send> HandlerSupervised for SelfRunner<S> {
    type Handler = ();
    fn writer_id(&self) -> WriterId {
        self.0.writer_id()
    }
    fn supervision_mode(
        &self,
    ) -> obzenflow_core::event::payloads::supervisor_descriptor::SupervisionMode {
        obzenflow_core::event::payloads::supervisor_descriptor::SupervisionMode::SelfSupervised
    }
    fn event_for_action_error(&self, message: String) -> Self::Event {
        self.0.event_for_action_error(message)
    }
    fn lifecycle_phase(&self, state: &Self::State) -> LifecyclePhase {
        self.0.lifecycle_phase(state)
    }
    fn after_transition(&mut self, state: &Self::State, context: &Self::Context) {
        self.0.after_transition(state, context);
    }
    fn supervisor_action(&self, action: &Self::Action) -> Option<SupervisorAction<Self::Event>> {
        self.0.supervisor_action(action)
    }
    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        self.0.execute_action(action, context).await
    }
    async fn execute_cleanup(
        &mut self,
        context: &Self::Context,
    ) -> Result<ActionExecution<Self::Context, Self::Event>, FsmError> {
        self.0.execute_cleanup(context).await
    }
    async fn next_control(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<Self::Event> {
        self.0.next_control(state, context).await
    }
    fn close_mailbox(&mut self, state: &Self::State) -> BoxFuture<'static, Result<(), BoxError>> {
        self.0.close_mailbox(state)
    }
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, BoxError> {
        self.0.dispatch_state(state, context).await
    }
}

#[async_trait::async_trait]
pub trait SelfSupervisedExt: SelfSupervised + Send {
    async fn run(self, initial_state: Self::State, context: Self::Context) -> Result<(), BoxError>
    where
        Self: Sized,
        Self::State: Send + Sync + 'static,
        Self::Event: Send + Sync + 'static,
        Self::Context: 'static,
        Self::Action: 'static,
    {
        HandlerSupervisedExt::run(SelfRunner(self), initial_state, context).await
    }
}
impl<T: SelfSupervised + Send> SelfSupervisedExt for T {}
