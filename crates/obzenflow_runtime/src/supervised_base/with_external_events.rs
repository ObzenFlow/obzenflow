// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Shared wrapper to inject external control-plane events into supervised dispatch loops.

use super::base::Supervisor;
use super::builder::{EventReceiver, StateWatcher};
use super::handler_supervised::HandlerSupervised;
use super::self_supervised::SelfSupervised;
use super::EventLoopDirective;
use crate::stages::common::{BoundaryStopController, BoundaryStopIntent};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ExternalEventMode {
    /// Block on `recv()` until an external event arrives (or the channel closes).
    Block,
    /// Poll using `try_recv()` and proceed if empty.
    Poll,
    /// Do not check the external event channel in this state.
    Ignore,
}

pub(crate) trait ExternalEventPolicy: Supervisor {
    fn external_event_mode(state: &Self::State) -> ExternalEventMode;
    fn on_external_event_channel_closed(state: &Self::State) -> Option<Self::Event>;

    /// Optional ephemeral stop controller for an active live-I/O boundary.
    fn boundary_stop_controller(_context: &Self::Context) -> Option<BoundaryStopController> {
        None
    }

    /// Classify an external stage event as graceful drain or force abort.
    fn boundary_stop_intent(_event: &Self::Event) -> Option<BoundaryStopIntent> {
        None
    }
}

pub(crate) struct HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
{
    inner: S,
    external_events: EventReceiver<S::Event>,
    state_watcher: StateWatcher<S::State>,
    last_state: Option<S::State>,
}

impl<S> HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
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

    fn name(&self) -> &str {
        self.inner.name()
    }
}

#[async_trait::async_trait]
impl<S> HandlerSupervised for HandlerSupervisedWithExternalEvents<S>
where
    S: HandlerSupervised + ExternalEventPolicy + Send + Sync,
    S::State: Clone + PartialEq,
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
            ExternalEventMode::Ignore => {}
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
            ExternalEventMode::Poll => {
                let stop_controller = <S as ExternalEventPolicy>::boundary_stop_controller(context);

                // A drain already queued at dispatch entry starts no new
                // handler or live-boundary attempt.
                match self.external_events.try_recv() {
                    Ok(event) => {
                        if let (Some(controller), Some(intent)) = (
                            &stop_controller,
                            <S as ExternalEventPolicy>::boundary_stop_intent(&event),
                        ) {
                            match intent {
                                BoundaryStopIntent::Running => {}
                                BoundaryStopIntent::Drain => controller.request_drain(),
                                BoundaryStopIntent::Abort => controller.request_abort(),
                            }
                        }
                        return Ok(EventLoopDirective::Transition(event));
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        if let Some(controller) = &stop_controller {
                            controller.request_abort();
                        }
                        if let Some(event) =
                            <S as ExternalEventPolicy>::on_external_event_channel_closed(state)
                        {
                            return Ok(EventLoopDirective::Transition(event));
                        }
                    }
                }

                let dispatch = self.inner.dispatch_state(state, context);
                tokio::pin!(dispatch);

                return tokio::select! {
                    biased;
                    maybe_event = self.external_events.recv() => {
                        match maybe_event {
                            Some(event) => match <S as ExternalEventPolicy>::boundary_stop_intent(&event) {
                                Some(BoundaryStopIntent::Drain) => {
                                    if let Some(controller) = &stop_controller {
                                        controller.request_drain();
                                    }
                                    // Graceful drain lets an active call settle. If normal
                                    // dispatch itself reached a transition, preserve that
                                    // terminal truth; otherwise apply the pending drain.
                                    match dispatch.await? {
                                        EventLoopDirective::Continue => {
                                            Ok(EventLoopDirective::Transition(event))
                                        }
                                        directive => Ok(directive),
                                    }
                                }
                                Some(BoundaryStopIntent::Abort) => {
                                    if let Some(controller) = &stop_controller {
                                        controller.request_abort();
                                    }
                                    // Dropping `dispatch` promptly drops the live boundary
                                    // future. No surface outcome is manufactured here.
                                    Ok(EventLoopDirective::Transition(event))
                                }
                                Some(BoundaryStopIntent::Running) | None => {
                                    Ok(EventLoopDirective::Transition(event))
                                }
                            },
                            None => {
                                if let Some(controller) = &stop_controller {
                                    controller.request_abort();
                                }
                                if let Some(event) = <S as ExternalEventPolicy>::on_external_event_channel_closed(state) {
                                    Ok(EventLoopDirective::Transition(event))
                                } else {
                                    dispatch.await
                                }
                            }
                        }
                    }
                    directive = &mut dispatch => directive,
                };
            }
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

pub(crate) struct SelfSupervisedWithExternalEvents<S>
where
    S: SelfSupervised + ExternalEventPolicy + Send + Sync,
{
    inner: S,
    external_events: EventReceiver<S::Event>,
    state_watcher: StateWatcher<S::State>,
    last_state: Option<S::State>,
}

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

    fn name(&self) -> &str {
        self.inner.name()
    }
}

#[async_trait::async_trait]
impl<S> SelfSupervised for SelfSupervisedWithExternalEvents<S>
where
    S: SelfSupervised + ExternalEventPolicy + Send + Sync,
    S::State: Clone + PartialEq,
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
            ExternalEventMode::Ignore => {}
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
