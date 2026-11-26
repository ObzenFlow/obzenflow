// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[cfg(doc)]
use super::port::Port;
use crate::effects::EffectPortResolutionError;

/// `SystemConfig` is the configuration of a system, for example a file and its
/// format, an application's fixture or a function that returns the system. With
/// [`Port::deferred`], the port builds the system at the first live call. A replay
/// builds no system.
///
/// ```
/// # use obzenflow_runtime::effects::EffectPortResolutionError;
/// # use obzenflow_runtime::systems::{Port, SystemConfig};
/// # #[obzenflow_runtime::effects::port]
/// # trait TicketStore: Send + Sync {}
/// struct ScriptedTickets {
///     count: usize,
/// }
///
/// impl TicketStore for ScriptedTickets {}
///
/// #[derive(Clone)]
/// struct TicketFixture {
///     count: usize,
/// }
///
/// impl SystemConfig for TicketFixture {
///     type System = ScriptedTickets;
///
///     fn resolve(&self) -> Result<ScriptedTickets, EffectPortResolutionError> {
///         Ok(ScriptedTickets { count: self.count })
///     }
/// }
///
/// let store: Port<dyn TicketStore> = Port::deferred(TicketFixture { count: 3 });
/// ```
pub trait SystemConfig: Send + Sync + 'static {
    type System: Send + Sync + 'static;

    /// `resolve` builds the system for a live call.
    fn resolve(&self) -> Result<Self::System, EffectPortResolutionError>;
}

impl<F, S> SystemConfig for F
where
    F: Fn() -> Result<S, EffectPortResolutionError> + Send + Sync + 'static,
    S: Send + Sync + 'static,
{
    type System = S;

    fn resolve(&self) -> Result<S, EffectPortResolutionError> {
        self()
    }
}
