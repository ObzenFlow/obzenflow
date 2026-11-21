// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Explicit terminal consumption without an external delivery.

use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    port, system, Command, DeclaredEffectSet, EffectError, Port, SinkRole,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::Serialize;
use std::marker::PhantomData;
use std::sync::OnceLock;

/// The request a discard sink sends for each input, which holds nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Discarded;

/// The port a discard sink sends through. Discarding twice changes nothing,
/// so the method is `exactly_once`.
#[port]
pub trait DiscardPort {
    #[effect(type = "obzenflow.discard.record", exactly_once, records = reply)]
    fn discard(&self, discarded: &Discarded) -> Result<(), EffectError>;
}

struct Discard;

#[system("discard")]
impl DiscardPort for Discard {
    fn discard(&self, _: &Discarded) -> Result<(), EffectError> {
        Ok(())
    }
}

fn discard_port() -> Port<dyn DiscardPort> {
    static PORT: OnceLock<Port<dyn DiscardPort>> = OnceLock::new();
    PORT.get_or_init(|| Port::to(Discard)).clone()
}

/// Consume inputs deliberately without storing or publishing them. Each input
/// still settles with a recorded call that changes nothing outside the flow.
pub struct DiscardSink<T> {
    port: Port<dyn DiscardPort>,
    _input: PhantomData<fn() -> T>,
}

impl<T: TypedPayload + Send + Sync + 'static> DiscardSink<T> {
    pub fn new() -> Self {
        Self {
            port: discard_port(),
            _input: PhantomData,
        }
    }
}

impl<T: TypedPayload + Send + Sync + 'static> Default for DiscardSink<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> Clone for DiscardSink<T> {
    fn clone(&self) -> Self {
        Self {
            port: self.port.clone(),
            _input: PhantomData,
        }
    }
}

impl<T> std::fmt::Debug for DiscardSink<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscardSink")
            .field("input", &std::any::type_name::<T>())
            .finish()
    }
}

impl<T> Command for DiscardSink<T> {
    type Ports = (Port<dyn DiscardPort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<Discarded, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

impl<T: TypedPayload + Send + Sync + 'static> SinkRole<T> for DiscardSink<T> {
    type Request = Discarded;

    fn prepare(&self, _input: T) -> Result<Discarded, HandlerError> {
        Ok(Discarded)
    }
}
