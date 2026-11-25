// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::port::{checked_system_name, Port, PortImpl, ServesPort};
use super::System;
use crate::effects::EffectPortResolutionError;
use obzenflow_core::effect_binding::ConfigurationEvidence;
use obzenflow_core::{BindingEvidenceError, SystemBindingEvidence};
use serde::Serialize;

/// The configuration of a system, such as a file and its format or an
/// application's fixture. It records its evidence when it builds its port, and
/// builds its system from the same value at the first live call. Admission then
/// refuses a replay or resume that binds another configuration before any system
/// is built.
///
/// ```
/// # use obzenflow_runtime::effects::EffectPortResolutionError;
/// # use obzenflow_runtime::systems::{Port, SystemConfig};
/// # #[obzenflow_runtime::effects::port]
/// # trait TicketStore: Send + Sync {}
/// #[obzenflow_runtime::effects::system("scripted-tickets")]
/// struct ScriptedTickets {
///     count: usize,
/// }
///
/// impl TicketStore for ScriptedTickets {}
///
/// #[derive(Clone, serde::Serialize)]
/// struct TicketFixture {
///     count: usize,
/// }
///
/// impl SystemConfig for TicketFixture {
///     type System = ScriptedTickets;
///     type Evidence = TicketFixture;
///
///     fn evidence(&self) -> TicketFixture {
///         self.clone()
///     }
///
///     fn resolve(&self) -> Result<ScriptedTickets, EffectPortResolutionError> {
///         Ok(ScriptedTickets { count: self.count })
///     }
/// }
///
/// let store: Port<dyn TicketStore> = TicketFixture { count: 3 }.port().unwrap();
/// assert_eq!(store.system_name(), "scripted-tickets");
/// ```
pub trait SystemConfig: Send + Sync + 'static {
    type System: System;
    type Evidence: Serialize;

    /// `evidence` returns the configuration that the binding records. It holds no
    /// credentials.
    fn evidence(&self) -> Self::Evidence;

    /// `resolve` builds the system for a live call.
    fn resolve(&self) -> Result<Self::System, EffectPortResolutionError>;

    /// `configuration` returns the evidence as the binding records it.
    fn configuration(&self) -> Result<ConfigurationEvidence, BindingEvidenceError> {
        let configuration = serde_json::to_value(self.evidence())
            .map_err(|_| BindingEvidenceError::InvalidConfiguration)?;
        Ok(ConfigurationEvidence::Application { configuration })
    }

    /// `port` returns the port of the system that this configuration builds. Each
    /// call declares its own binding.
    fn port<P>(self) -> Result<Port<P>, BindingEvidenceError>
    where
        Self: Sized,
        P: ?Sized + Send + Sync + 'static,
        PortImpl<Self::System>: ServesPort<P, Self::System>,
    {
        let evidence = SystemBindingEvidence::configured(
            checked_system_name::<Self::System>(),
            self.configuration()?,
        )?;
        Ok(Port::configured(evidence, move || self.resolve()))
    }
}
