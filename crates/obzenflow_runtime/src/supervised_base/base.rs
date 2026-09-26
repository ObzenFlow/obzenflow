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

    /// Existing journal owned by this run. Registration is ordinary journal
    /// evidence, published through the same supervised publication scope.
    fn report_journal(&self, context: &Self::Context) -> crate::supervised_base::SupervisorJournal;
}

pub(super) async fn register<S: Supervisor>(
    supervisor: &S,
    context: &S::Context,
    writer: WriterId,
    supervision: SupervisionMode,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let descriptor = SupervisorDescriptor {
        name: supervisor.name().to_owned(),
        kind: supervisor.supervisor_kind(),
        supervision,
    };
    descriptor
        .validate(&writer)
        .map_err(std::io::Error::other)?;
    super::publication::report(
        &supervisor.report_journal(context),
        SystemEvent::new(writer, SystemPayload::SupervisorRegistered { descriptor }),
        Default::default(),
    )
    .await?;
    Ok(())
}
