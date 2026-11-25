// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Systems and the ports they serve. A flow chooses the system behind each port
//! with [`Port::to`], [`Port::deferred`] or [`SystemConfig::port`].

mod config;
mod port;

pub use config::SystemConfig;
pub use port::Port;
#[doc(hidden)]
pub use port::{BuiltSystem, PortImpl, ServesPort};

pub use obzenflow_core::effect_binding::{
    ConfigurationEvidence, DelimitedHeaders, HttpEndpoint, SystemBinding,
};
pub use obzenflow_core::BindingEvidenceError;

/// A system: a type that answers the ports it implements, under the name that
/// every call it answers records. `#[obzenflow::system("name")]` on the type
/// implements it, once, whatever ports the type serves.
///
/// A system without a name cannot become a [`Port`], whether it implements a
/// port trait:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::systems::Port;
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {}
/// struct RiskEngineOutage;
/// impl RiskEnginePort for RiskEngineOutage {}
/// # fn main() {
/// let outage = Port::to(RiskEngineOutage);
/// # let port: Port<dyn RiskEnginePort> = outage;
/// # }
/// ```
///
/// or serves as its own port:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::systems::Port;
/// struct AuditLedger;
///
/// #[obzenflow_runtime::effects::port]
/// impl AuditLedger {}
/// # fn main() {
/// let ledger = Port::to(AuditLedger);
/// # let port: Port<AuditLedger> = ledger;
/// # }
/// ```
///
/// A type names itself once, so one system serves two ports under one name:
///
/// ```
/// # use obzenflow_runtime::systems::Port;
/// # use std::sync::Arc;
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {}
/// # #[obzenflow_runtime::effects::port]
/// # trait CaseSystemPort: Send + Sync {}
/// #[obzenflow_runtime::effects::system("vendor")]
/// struct Vendor;
///
/// impl RiskEnginePort for Vendor {}
/// impl CaseSystemPort for Vendor {}
///
/// let vendor = Arc::new(Vendor);
/// let risk: Port<dyn RiskEnginePort> = Port::to(Arc::clone(&vendor));
/// let cases: Port<dyn CaseSystemPort> = Port::to(vendor);
/// assert_eq!(risk.system_name(), cases.system_name());
/// ```
///
/// Two names for one type do not compile:
///
/// ```compile_fail,E0119
/// #[obzenflow_runtime::effects::system("vendor-risk")]
/// struct Vendor;
///
/// impl obzenflow_runtime::systems::System for Vendor {
///     const NAME: &'static str = "vendor-cases";
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no system name",
    note = "put `#[obzenflow::system(\"…\")]` above the type that answers the port",
    label = "this system does not name itself",
    note = "a resolver returns the system itself, never an `Arc`; `Port::to` takes the system by value or in an `Arc` of its concrete type, and an erased `Arc<dyn Port>` names no system"
)]
pub trait System: Send + Sync + 'static {
    const NAME: &'static str;
}
