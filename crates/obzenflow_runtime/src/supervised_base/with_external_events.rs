// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared wrapper to inject external control-plane events into supervised dispatch loops.

use super::base::Supervisor;
use super::builder::{EventReceiver, ReceivedCommand, StateWatcher};
use super::handler_supervised::HandlerSupervised;
use super::{publication, EventLoopDirective};
use obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind;
use obzenflow_core::event::{CommandDiscardDisposition, WriterId};
use obzenflow_fsm::{EventVariant, StateMachine, StateVariant};
use std::collections::VecDeque;
use std::error::Error;
use tokio::sync::mpsc::error::TryRecvError;

#[cfg(test)]
use super::self_supervised::SelfSupervised;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExternalEventMode {
    /// Block on `recv()` until an external event arrives (or the channel closes).
    Block,
    /// Poll using `try_recv()` and proceed if empty.
    Poll,
    /// Close admission and journal every accepted command without executing it.
    /// Used by disposal fixtures; production FSMs select CloseMailbox actions.
    #[cfg(test)]
    CloseAndRecord,
    /// Leave commands queued for a later state to execute.
    #[cfg(test)]
    Defer,
}

/// Semantic command details supplied by the owning supervisor family.
/// The shared runner does not parse debug output or stage-specific error strings.
pub(crate) trait ExternalControlEvent: EventVariant {
    fn discard_details(&self) -> (CommandDiscardDisposition, Option<String>);
}

/// Disposal details are supplied by the command owner, then authored directly
/// in that owner's canonical journal. They are never a read-side record type.
pub(crate) struct DiscardedCommand {
    supervisor: String,
    terminal_state: String,
    command: String,
    disposition: CommandDiscardDisposition,
    error: Option<String>,
}

type DisposalResult = futures::future::BoxFuture<'static, Result<(), Box<dyn Error + Send + Sync>>>;
pub(crate) type CommandRecorder = Box<dyn Fn(DiscardedCommand) -> DisposalResult + Send + Sync>;

pub(crate) fn stage_commands(
    journal: std::sync::Arc<dyn obzenflow_core::Journal<obzenflow_core::ChainEvent>>,
    context: obzenflow_core::event::provenance::FlowContext,
) -> CommandRecorder {
    Box::new(move |discarded| {
        let journal = journal.clone();
        let context = context.clone();
        Box::pin(async move {
            use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
            use obzenflow_core::event::ChainEventFactory;
            let DiscardedCommand {
                supervisor,
                terminal_state,
                command,
                disposition,
                error,
            } = discarded;
            let event = ChainEventFactory::execution_event(
                context.stage_id.into(),
                ExecutionPayload::SupervisorCommandDiscarded {
                    supervisor,
                    terminal_state,
                    command,
                    disposition,
                    error,
                },
            )
            .with_flow_context(context);
            publication::append_inline(&journal, event, Default::default()).await?;
            Ok(())
        })
    })
}

/// The closed queue is retained in one owned publication. Cancelling a waiter
/// cannot lose commands, and deferred commands retain causality until disposal.
#[cfg(test)]
pub(crate) fn record_terminal_commands<E: ExternalControlEvent + Send + 'static>(
    external_events: &mut EventReceiver<E>,
    publish: CommandRecorder,
    supervisor: &str,
    terminal_state: &str,
) -> DisposalResult {
    record_commands(
        external_events.close_and_take(),
        VecDeque::new(),
        publish,
        supervisor.to_owned(),
        terminal_state.to_owned(),
    )
}

fn record_commands<E: ExternalControlEvent + Send + 'static>(
    mut receiver: Option<EventReceiver<E>>,
    mut deferred: VecDeque<ReceivedCommand<E>>,
    publish: CommandRecorder,
    supervisor: String,
    terminal_state: String,
) -> DisposalResult {
    if receiver.is_none() && deferred.is_empty() {
        return Box::pin(async { Ok(()) });
    }
    Box::pin(async move {
        publication::commit(async move {
            loop {
                let command = match deferred.pop_front() {
                    Some(command) => Some(command),
                    None => match receiver.as_mut() {
                        Some(receiver) => receiver.recv_command().await,
                        None => None,
                    },
                };
                let Some(event) = command.and_then(ReceivedCommand::admit) else {
                    break;
                };
                let (disposition, error) = event.discard_details();
                publish(DiscardedCommand {
                    supervisor: supervisor.clone(),
                    terminal_state: terminal_state.clone(),
                    command: event.variant_name().to_owned(),
                    disposition,
                    error,
                })
                .await?;
            }
            Ok(())
        })
        .await
    })
}

/// Runtime-owned commands which the stage has explicitly deferred. The
/// admission predicate belongs to the stage FSM; this queue never classifies,
/// merges or discards commands and retains their causal context unchanged.
pub(crate) struct CommandMailbox<E> {
    receiver: EventReceiver<E>,
    deferred: VecDeque<ReceivedCommand<E>>,
}

impl<E> From<EventReceiver<E>> for CommandMailbox<E> {
    fn from(receiver: EventReceiver<E>) -> Self {
        Self {
            receiver,
            deferred: VecDeque::new(),
        }
    }
}

impl<E> CommandMailbox<E> {
    /// The owner explicitly disposes queued internal commands at termination.
    /// Stage mailboxes use close_and_record when a canonical disposition fact
    /// is part of their contract.
    pub(crate) fn close(&mut self) {
        drop(self.receiver.close_and_take());
        self.deferred.clear();
    }

    fn ready(&mut self, admits: &impl Fn(&E) -> bool) -> Option<ReceivedCommand<E>> {
        let index = self
            .deferred
            .iter()
            .position(|command| admits(&command.event))?;
        self.deferred.remove(index)
    }

    pub(crate) async fn recv(&mut self, admits: impl Fn(&E) -> bool) -> Option<E> {
        loop {
            let command = match self.ready(&admits) {
                Some(command) => command,
                None => self.receiver.recv_command().await?,
            };
            if admits(&command.event) {
                return command.admit();
            }
            self.deferred.push_back(command);
        }
    }

    pub(crate) fn try_recv(&mut self, admits: impl Fn(&E) -> bool) -> Result<E, TryRecvError> {
        loop {
            let command = match self.ready(&admits) {
                Some(command) => command,
                None => self.receiver.try_recv_command()?,
            };
            if admits(&command.event) {
                return command.admit().ok_or(TryRecvError::Disconnected);
            }
            self.deferred.push_back(command);
        }
    }
}

impl<E: ExternalControlEvent + Send + 'static> CommandMailbox<E> {
    pub(crate) fn close_and_record(
        &mut self,
        publish: CommandRecorder,
        supervisor: &str,
        state: &str,
    ) -> futures::future::BoxFuture<'static, Result<(), Box<dyn Error + Send + Sync>>> {
        record_commands(
            self.receiver.close_and_take(),
            std::mem::take(&mut self.deferred),
            publish,
            supervisor.to_owned(),
            state.to_owned(),
        )
    }
}

pub(crate) trait ExternalEventPolicy: Supervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode;
    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event>;
    /// The owning stage decides when a command can enter its transition table.
    fn defer_external_event(_state: &Self::State, _event: &Self::Event) -> bool {
        false
    }
}

pub(crate) struct HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync + 'static,
{
    inner: S,
    external_events: CommandMailbox<S::Event>,
    state_watcher: StateWatcher<S::State>,
    last_state: Option<S::State>,
    terminal_commands: Option<CommandRecorder>,
}

impl<S> HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync + 'static,
{
    pub(crate) fn new(
        inner: S,
        external_events: EventReceiver<S::Event>,
        state_watcher: StateWatcher<S::State>,
        terminal_commands: CommandRecorder,
    ) -> Self {
        Self {
            inner,
            external_events: external_events.into(),
            state_watcher,
            last_state: None,
            terminal_commands: Some(terminal_commands),
        }
    }
    async fn receive_control(&mut self, state: &S::State) -> Option<S::Event> {
        self.external_events
            .recv(|event| !S::defer_external_event(state, event))
            .await
    }

    fn poll_control(&mut self, state: &S::State) -> Result<S::Event, TryRecvError> {
        self.external_events
            .try_recv(|event| !S::defer_external_event(state, event))
    }
}

impl<S> Supervisor for HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync + 'static,
{
    type State = S::State;
    type Event = S::Event;
    type Context = S::Context;
    type Action = S::Action;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        self.inner.build_state_machine(initial_state)
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        self.inner.supervisor_kind()
    }

    fn registration(
        &self,
        context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        self.inner.registration(context, descriptor)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
    fn event_prefix(&self) -> String {
        self.inner.event_prefix()
    }
}

#[async_trait::async_trait]
impl<S> HandlerSupervised for HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync + 'static,
    S::State: Clone + PartialEq,
    S::Event: ExternalControlEvent,
{
    type Handler = S::Handler;

    fn owned_dispatch(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Option<super::handler_supervised::OwnedDispatch<Self>> {
        let operation = self.inner.owned_dispatch(state, context)?;
        Some(Box::pin(async move {
            let complete = operation.await;
            Box::new(move |owner: &mut Self, context: &mut Self::Context| {
                complete(&mut owner.inner, context)
            }) as super::handler_supervised::DispatchCompletion<Self>
        }))
    }

    fn lifecycle_phase(
        &self,
        state: &Self::State,
    ) -> crate::stages::common::stage_lifecycle::LifecyclePhase {
        self.inner.lifecycle_phase(state)
    }

    fn accounting(
        &self,
        context: &Self::Context,
    ) -> obzenflow_core::event::provenance::ExecutionAccounting {
        self.inner.accounting(context)
    }

    fn supervisor_action(
        &self,
        action: &Self::Action,
    ) -> Option<super::handler_supervised::SupervisorAction<Self::Event>> {
        self.inner.supervisor_action(action)
    }

    async fn execute_action(
        &mut self,
        action: Self::Action,
        context: &mut Self::Context,
    ) -> Result<
        super::handler_supervised::ActionExecution<Self::Context, Self::Event>,
        obzenflow_fsm::FsmError,
    > {
        self.inner.execute_action(action, context).await
    }

    async fn execute_cleanup(
        &mut self,
        context: &Self::Context,
    ) -> Result<
        super::handler_supervised::ActionExecution<Self::Context, Self::Event>,
        obzenflow_fsm::FsmError,
    > {
        self.inner.execute_cleanup(context).await
    }

    fn after_transition(&mut self, state: &Self::State, context: &Self::Context) {
        self.inner.after_transition(state, context);
        if self.last_state.as_ref() != Some(state) {
            let _ = self.state_watcher.update(state.clone());
            self.last_state = Some(state.clone());
        }
    }

    async fn next_control(
        &mut self,
        state: &Self::State,
        _context: &mut Self::Context,
    ) -> Option<Self::Event> {
        self.receive_control(state).await
    }

    fn close_mailbox(
        &mut self,
        state: &Self::State,
    ) -> futures::future::BoxFuture<'static, Result<(), Box<dyn Error + Send + Sync>>> {
        let Some(publish) = self.terminal_commands.take() else {
            return Box::pin(async { Ok(()) });
        };
        self.external_events
            .close_and_record(publish, self.inner.name(), state.variant_name())
    }

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>> {
        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        match <S as ExternalEventPolicy>::external_event_mode(state) {
            #[cfg(test)]
            ExternalEventMode::CloseAndRecord => {
                self.close_mailbox(state).await?;
            }
            #[cfg(test)]
            ExternalEventMode::Defer => {}
            ExternalEventMode::Block => match self.receive_control(state).await {
                Some(event) => return Ok(EventLoopDirective::Transition(event)),
                None => {
                    if let Some(event) =
                        <S as ExternalEventPolicy>::on_external_event_channel_closed(state)
                    {
                        return Ok(EventLoopDirective::Transition(event));
                    }
                }
            },
            ExternalEventMode::Poll => match self.poll_control(state) {
                Ok(event) => return Ok(EventLoopDirective::Transition(event)),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    if let Some(event) =
                        <S as ExternalEventPolicy>::on_external_event_channel_closed(state)
                    {
                        return Ok(EventLoopDirective::Transition(event));
                    }
                }
            },
        }

        self.inner.dispatch_state(state, context).await
    }

    fn writer_id(&self) -> WriterId {
        self.inner.writer_id()
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        self.inner.event_for_action_error(msg)
    }
}

#[cfg(test)]
pub(crate) struct SelfSupervisedWithExternalEvents<S>
where
    S: SelfSupervised + ExternalEventPolicy + Send + Sync,
{
    inner: S,
    external_events: EventReceiver<S::Event>,
    state_watcher: StateWatcher<S::State>,
    last_state: Option<S::State>,
}

#[cfg(test)]
impl<S> SelfSupervisedWithExternalEvents<S>
where
    S: SelfSupervised + ExternalEventPolicy + Send + Sync,
{
    pub(crate) fn new(
        inner: S,
        external_events: EventReceiver<S::Event>,
        state_watcher: StateWatcher<S::State>,
    ) -> Self {
        Self {
            inner,
            external_events,
            state_watcher,
            last_state: None,
        }
    }
}

#[cfg(test)]
impl<S> Supervisor for SelfSupervisedWithExternalEvents<S>
where
    S: SelfSupervised + ExternalEventPolicy + Send + Sync,
{
    type State = S::State;
    type Event = S::Event;
    type Context = S::Context;
    type Action = S::Action;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        self.inner.build_state_machine(initial_state)
    }

    fn supervisor_kind(&self) -> SupervisorKind {
        self.inner.supervisor_kind()
    }

    fn registration(
        &self,
        context: &Self::Context,
        descriptor: obzenflow_core::event::payloads::supervisor_descriptor::SupervisorDescriptor,
    ) -> crate::supervised_base::base::Registration {
        self.inner.registration(context, descriptor)
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
    fn event_prefix(&self) -> String {
        self.inner.event_prefix()
    }
}

#[cfg(test)]
#[async_trait::async_trait]
impl<S> SelfSupervised for SelfSupervisedWithExternalEvents<S>
where
    S: SelfSupervised + ExternalEventPolicy + Send + Sync,
    S::State: Clone + PartialEq,
    S::Event: ExternalControlEvent,
{
    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn Error + Send + Sync>> {
        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        match <S as ExternalEventPolicy>::external_event_mode(state) {
            ExternalEventMode::CloseAndRecord => {
                drop(self.external_events.close_and_take());
            }
            ExternalEventMode::Defer => {}
            ExternalEventMode::Block => match self.external_events.recv().await {
                Some(event) => return Ok(EventLoopDirective::Transition(event)),
                None => {
                    if let Some(event) =
                        <S as ExternalEventPolicy>::on_external_event_channel_closed(state)
                    {
                        return Ok(EventLoopDirective::Transition(event));
                    }
                }
            },
            ExternalEventMode::Poll => match self.external_events.try_recv() {
                Ok(event) => return Ok(EventLoopDirective::Transition(event)),
                Err(TryRecvError::Empty) => {}
                Err(TryRecvError::Disconnected) => {
                    if let Some(event) =
                        <S as ExternalEventPolicy>::on_external_event_channel_closed(state)
                    {
                        return Ok(EventLoopDirective::Transition(event));
                    }
                }
            },
        }

        self.inner.dispatch_state(state, context).await
    }

    fn writer_id(&self) -> WriterId {
        self.inner.writer_id()
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        self.inner.event_for_action_error(msg)
    }

    fn after_transition(&mut self, state: &Self::State, context: &Self::Context) {
        self.inner.after_transition(state, context);

        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }
    }
}
