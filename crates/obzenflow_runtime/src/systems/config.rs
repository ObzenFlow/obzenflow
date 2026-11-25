// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::port::{Port, PortImpl, ServesPort};
use crate::effects::EffectPortResolutionError;

/// `SystemConfig` is the configuration of a system, for example a file and its
/// format, or an application's fixture. Its port builds the system at the first
/// live call. A replay builds no system.
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
/// let store: Port<dyn TicketStore> = TicketFixture { count: 3 }.port();
/// ```
pub trait SystemConfig: Send + Sync + 'static {
    type System: Send + Sync + 'static;

    /// `resolve` builds the system for a live call.
    fn resolve(&self) -> Result<Self::System, EffectPortResolutionError>;

    /// `port` returns the port of the system that this configuration builds.
    fn port<P>(self) -> Port<P>
    where
        Self: Sized,
        P: ?Sized + Send + Sync + 'static,
        PortImpl<Self::System>: ServesPort<P, Self::System>,
    {
        Port::deferred(move || self.resolve())
    }
}
