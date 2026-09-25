// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared wrapper to inject external control-plane events into supervised dispatch loops.

use super::base::Supervisor;
use super::builder::{EventReceiver, StateWatcher};
use super::cleanup::HandlerSupervisedCleanup;
use super::handler_supervised::HandlerSupervised;
#[cfg(test)]
use super::self_supervised::SelfSupervised;
use super::EventLoopDirective;
use obzenflow_core::event::{CommandDiscardDisposition, SystemEvent, SystemPayload, WriterId};
use obzenflow_core::journal::Journal;
use obzenflow_fsm::{EventVariant, StateVariant};
use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExternalEventMode {
    /// Block on `recv()` until an external event arrives (or the channel closes).
    Block,
    /// Poll using `try_recv()` and proceed if empty.
    Poll,
    /// Close admission and journal every accepted command without executing it.
    /// Terminal states use this after their transition actions have completed.
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

/// Account for a terminal mailbox through the supervisor's retained publication
/// scope. The whole closed queue belongs to one publication, so cancellation of
/// its waiter cannot discard commands after the first append. Journal errors are
/// propagated and retained by the scope; a broken journal cannot certify disposal.
pub(crate) async fn record_terminal_commands<E: ExternalControlEvent>(
    external_events: &mut EventReceiver<E>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
    writer_id: WriterId,
    supervisor: &str,
    terminal_state: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let Some(mut receiver) = external_events.close_and_take() else {
        return Ok(());
    };
    let supervisor = supervisor.to_owned();
    let terminal_state = terminal_state.to_owned();
    super::publication::commit(async move {
        // recv() also accounts for a send that held a permit when we closed.
        while let Some(event) = receiver.recv().await {
            let (disposition, error) = event.discard_details();
            let fact = SystemEvent::new(
                writer_id,
                SystemPayload::SupervisorCommandDiscarded {
                    supervisor: supervisor.clone(),
                    terminal_state: terminal_state.clone(),
                    command: event.variant_name().to_owned(),
                    disposition,
                    error,
                },
            );
            system_journal.append(fact, Default::default()).await?;
        }
        Ok(())
    })
    .await
}

pub(crate) trait ExternalEventPolicy: Supervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode;
    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event>;
}

pub(crate) struct HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
{
    inner: S,
    external_events: EventReceiver<S::Event>,
    state_watcher: StateWatcher<S::State>,
    last_state: Option<S::State>,
    system_journal: Arc<dyn Journal<SystemEvent>>,
}

impl<S> HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
{
    pub(crate) fn new(
        inner: S,
        external_events: EventReceiver<S::Event>,
        state_watcher: StateWatcher<S::State>,
        system_journal: Arc<dyn Journal<SystemEvent>>,
    ) -> Self {
        Self {
            inner,
            external_events,
            state_watcher,
            last_state: None,
            system_journal,
        }
    }
}

impl<S> Supervisor for HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
{
    type State = S::State;
    type Event = S::Event;
    type Context = S::Context;
    type Action = S::Action;

    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        self.inner.build_state_machine(initial_state)
    }

    fn supervisor_kind(
        &self,
    ) -> obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind {
        self.inner.supervisor_kind()
    }

    fn system_journal(&self, _context: &Self::Context) -> Arc<dyn Journal<SystemEvent>> {
        self.system_journal.clone()
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

#[async_trait::async_trait]
impl<S> HandlerSupervisedCleanup for HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
{
    async fn cleanup_after_run(
        &mut self,
        context: &Self::Context,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.cleanup_after_run(context).await
    }
}

#[async_trait::async_trait]
impl<S> HandlerSupervised for HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
    S::State: Clone + PartialEq,
    S::Event: ExternalControlEvent,
{
    type Handler = S::Handler;

    async fn dispatch_state(
        &mut self,
        state: &Self::State,
        context: &mut Self::Context,
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        match <S as ExternalEventPolicy>::external_event_mode(state) {
            ExternalEventMode::CloseAndRecord => {
                let writer_id = self.inner.writer_id();
                record_terminal_commands(
                    &mut self.external_events,
                    self.system_journal.clone(),
                    writer_id,
                    self.inner.name(),
                    state.variant_name(),
                )
                .await?;
            }
            #[cfg(test)]
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
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
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

    fn writer_id(&self) -> obzenflow_core::event::WriterId {
        self.inner.writer_id()
    }

    fn stage_id(&self) -> obzenflow_core::StageId {
        self.inner.stage_id()
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.write_completion_event().await
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
    system_journal: Arc<dyn Journal<SystemEvent>>,
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
        system_journal: Arc<dyn Journal<SystemEvent>>,
    ) -> Self {
        Self {
            inner,
            external_events,
            state_watcher,
            last_state: None,
            system_journal,
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
    ) -> obzenflow_fsm::StateMachine<Self::State, Self::Event, Self::Context, Self::Action> {
        self.inner.build_state_machine(initial_state)
    }

    fn supervisor_kind(
        &self,
    ) -> obzenflow_core::event::payloads::supervisor_descriptor::SupervisorKind {
        self.inner.supervisor_kind()
    }

    fn system_journal(&self, _context: &Self::Context) -> Arc<dyn Journal<SystemEvent>> {
        self.system_journal.clone()
    }

    fn name(&self) -> &str {
        self.inner.name()
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
    ) -> Result<EventLoopDirective<Self::Event>, Box<dyn std::error::Error + Send + Sync>> {
        // Update state for external observers only when it changes (FLOWIP-086i).
        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        match <S as ExternalEventPolicy>::external_event_mode(state) {
            ExternalEventMode::CloseAndRecord => {
                let writer_id = self.inner.writer_id();
                record_terminal_commands(
                    &mut self.external_events,
                    self.system_journal.clone(),
                    writer_id,
                    self.inner.name(),
                    state.variant_name(),
                )
                .await?;
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
                Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
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

    fn writer_id(&self) -> obzenflow_core::event::WriterId {
        self.inner.writer_id()
    }

    async fn write_completion_event(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.write_completion_event().await
    }

    fn event_for_action_error(&self, msg: String) -> Self::Event {
        self.inner.event_for_action_error(msg)
    }

    async fn after_transition(
        &mut self,
        state: &Self::State,
        context: &Self::Context,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.inner.after_transition(state, context).await?;

        if self.last_state.as_ref() != Some(state) {
            let new_state = state.clone();
            let _ = self.state_watcher.update(new_state.clone());
            self.last_state = Some(new_state);
        }

        Ok(())
    }
}
