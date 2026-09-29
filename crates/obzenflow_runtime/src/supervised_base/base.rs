// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Common base functionality for supervised state machines
//!
//! This module provides shared types and traits used by both self-supervised
//! and handler-supervised state machine implementations.

use obzenflow_core::event::payloads::supervisor_descriptor::{
    SupervisionMode, SupervisorDescriptor, SupervisorKind,
};
use obzenflow_core::event::{SystemEvent, SystemPayload, WriterId};
use obzenflow_fsm::{EventVariant, FsmAction, FsmContext, StateMachine, StateVariant};

/// Directives that control a state's event loop
#[derive(Debug, Clone)]
pub enum EventLoopDirective<E> {
    /// Continue running this state's event loop (non-blocking)
    Continue,

    /// This state is done - transition via this event
    Transition(E),

    /// This state machine should terminate
    Terminate,
}

/// Base trait that all supervisors must implement
/// This enforces that every supervisor provides FSM building capabilities
///
/// This is crate-internal (see `supervised_base::mod.rs`). External code should
/// implement `SelfSupervised` or `HandlerSupervised` instead of implementing
/// `Supervisor` directly.
pub trait Supervisor {
    type State: StateVariant;
    type Event: EventVariant;
    type Context: FsmContext;
    type Action: FsmAction<Context = Self::Context>;

    /// Build the fully configured FSM for this supervisor.
    ///
    /// Implementors are expected to use the typed `fsm!` DSL (or an
    /// equivalent strongly-typed constructor) to define their state
    /// machines. The legacy FsmBuilder-based path has been removed.
    fn build_state_machine(
        &self,
        initial_state: Self::State,
    ) -> StateMachine<Self::State, Self::Event, Self::Context, Self::Action>;

    /// Get the name of this supervised component
    fn name(&self) -> &str;

    /// The actual supervisor family, independent of its task name or event types.
    fn supervisor_kind(&self) -> SupervisorKind;

    /// The owner selects its canonical journal and authors its registration fact.
    /// The runner invokes this only for an FSM-selected Register action.
    fn registration(
        &self,
        context: &Self::Context,
        descriptor: SupervisorDescriptor,
    ) -> Registration;
}

pub(crate) type Registration =
    futures::future::BoxFuture<'static, Result<(), Box<dyn std::error::Error + Send + Sync>>>;

pub(crate) fn register<S: Supervisor>(
    supervisor: &S,
    context: &S::Context,
    writer: WriterId,
    supervision: SupervisionMode,
) -> Registration {
    let descriptor = SupervisorDescriptor {
        name: supervisor.name().to_owned(),
        kind: supervisor.supervisor_kind(),
        supervision,
    };
    if let Err(error) = descriptor.validate(&writer) {
        return Box::pin(async move { Err(std::io::Error::other(error).into()) });
    }
    supervisor.registration(context, descriptor)
}

pub(crate) fn register_stage(
    journal: std::sync::Arc<dyn obzenflow_core::Journal<obzenflow_core::ChainEvent>>,
    context: obzenflow_core::event::provenance::FlowContext,
    descriptor: SupervisorDescriptor,
) -> Registration {
    use obzenflow_core::event::payloads::execution_payload::ExecutionPayload;
    use obzenflow_core::event::{ChainEventFactory, ChainPayload};
    Box::pin(async move {
        let event = ChainEventFactory::create_with_context(
            context.stage_id.into(),
            ChainPayload::Execution(ExecutionPayload::SupervisorRegistered { descriptor }),
            context,
        );
        super::publication::append(&journal, event, Default::default()).await?;
        Ok(())
    })
}

pub(crate) fn register_system(
    journal: std::sync::Arc<dyn obzenflow_core::Journal<SystemEvent>>,
    writer: WriterId,
    descriptor: SupervisorDescriptor,
) -> Registration {
    Box::pin(async move {
        super::publication::append(
            &journal,
            SystemEvent::new(writer, SystemPayload::SupervisorRegistered { descriptor }),
            Default::default(),
        )
        .await?;
        Ok(())
    })
}
