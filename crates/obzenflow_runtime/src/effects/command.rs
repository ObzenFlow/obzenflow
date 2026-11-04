// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::binding::is_system_name;
use super::call::{call_port_slot, Call, CallRequest};
use super::declaration::EffectDeclarationSyntax;
use super::ports::SystemPort;
use super::{
    Effect, EffectBinding, EffectBindingBuildError, EffectDeclaration, EffectError, EffectName,
    EffectPortResolutionError, EffectRegistrationBuilder, EffectSet, LogicalEffectBindingName,
    NamedEffect, RequestList,
};
use crate::stages::common::handlers::{EffectfulStatefulHandler, EffectfulTransformHandler};
use obzenflow_core::event::schema::{EffectOutcomeFacts, EmptySet, WithMember};
use obzenflow_core::SystemBindingEvidence;
use std::marker::PhantomData;
use std::sync::Arc;

/// A port `P` with the system that answers it chosen. A command holds one per
/// port type, in a `#[port]` field or through `through`, and performs requests
/// through `fx` without ever seeing the system.
pub struct Port<P: ?Sized + Send + Sync + 'static> {
    name: LogicalEffectBindingName,
    evidence: SystemBindingEvidence,
    port: Arc<SystemPort>,
    port_type: PhantomData<fn() -> Arc<P>>,
}

impl<P: ?Sized + Send + Sync + 'static> Port<P> {
    /// A port answered by a system that is already built, by value or in an
    /// `Arc`. The port's trait comes from the `#[obzenflow::port]` trait the
    /// system implements, so the port needs no annotation.
    ///
    /// ```
    /// # use obzenflow_runtime::effects::Port;
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # struct RiskEngineOutage;
    /// # #[obzenflow_runtime::effects::system("risk-engine-outage", effects = obzenflow_runtime::effects)]
    /// # impl RiskEnginePort for RiskEngineOutage {}
    /// let outage = Port::to(RiskEngineOutage);
    /// let shared = Port::to(Arc::new(RiskEngineOutage));
    /// # let ports: [Port<dyn RiskEnginePort>; 2] = [outage, shared];
    /// ```
    ///
    /// A port trait left unmarked does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::effects::Port;
    /// trait RiskEnginePort: Send + Sync {}
    /// struct RiskEngineOutage;
    /// # #[obzenflow_runtime::effects::system("risk-engine-outage", effects = obzenflow_runtime::effects)]
    /// impl RiskEnginePort for RiskEngineOutage {}
    /// # fn main() {
    /// let outage = Port::to(RiskEngineOutage);
    /// # let port: Port<dyn RiskEnginePort> = outage;
    /// # }
    /// ```
    ///
    /// An already-erased port names no system, so it does not compile either:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::effects::Port;
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # struct RiskEngineOutage;
    /// # #[obzenflow_runtime::effects::system("risk-engine-outage", effects = obzenflow_runtime::effects)]
    /// # impl RiskEnginePort for RiskEngineOutage {}
    /// # fn main() {
    /// let erased: Arc<dyn RiskEnginePort> = Arc::new(RiskEngineOutage);
    /// let outage = Port::to(erased);
    /// # let port: Port<dyn RiskEnginePort> = outage;
    /// # }
    /// ```
    ///
    /// A struct whose inherent `impl` block carries `#[obzenflow::port]` serves
    /// as its own port and names itself there:
    ///
    /// ```
    /// # use obzenflow_runtime::effects::{EffectError, Port};
    /// # #[derive(Clone, Debug, serde::Serialize)]
    /// # struct AuditEntry;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct AuditAppended;
    /// # impl obzenflow_core::TypedPayload for AuditAppended { const EVENT_TYPE: &'static str = "docs.audit.appended"; }
    /// struct AuditLedger;
    ///
    /// #[obzenflow_runtime::effects::port(system = "audit-ledger", effects = obzenflow_runtime::effects)]
    /// impl AuditLedger {
    ///     #[effect(type = "docs.audit.append", exactly_once)]
    ///     fn append(&self, _: &AuditEntry) -> Result<AuditAppended, EffectError> {
    ///         Ok(AuditAppended)
    ///     }
    /// }
    /// # fn main() {
    /// let ledger = Port::to(AuditLedger);
    /// # let port: Port<AuditLedger> = ledger;
    /// # }
    /// ```
    ///
    /// A struct left unmarked does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::effects::{Port, SystemName};
    /// struct AuditLedger;
    /// # impl SystemName for AuditLedger { const NAME: &'static str = "audit-ledger"; }
    /// # fn main() {
    /// let ledger = Port::to(AuditLedger);
    /// # let port: Port<AuditLedger> = ledger;
    /// # }
    /// ```
    ///
    /// A name outside the grammar does not compile, even from a hand-written
    /// [`SystemName`]:
    ///
    /// ```compile_fail,E0080
    /// # use obzenflow_runtime::effects::{Port, SystemName};
    /// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// # trait RiskEnginePort: Send + Sync {}
    /// struct RiskEngineOutage;
    /// impl RiskEnginePort for RiskEngineOutage {}
    /// impl SystemName for RiskEngineOutage {
    ///     const NAME: &'static str = "risk engine outage";
    /// }
    /// # fn main() {
    /// let outage = Port::to(RiskEngineOutage);
    /// # let port: Port<dyn RiskEnginePort> = outage;
    /// # }
    /// ```
    pub fn to<S>(system: S) -> Self
    where
        S: BuiltSystem,
        S::System: Send + Sync + 'static,
        PortImpl<S::System>: ServesPort<P, S::System>,
    {
        Self::answered_by::<S::System>(SystemPort::eager(<PortImpl<S::System> as ServesPort<
            P,
            S::System,
        >>::coerce(system.shared())))
    }

    /// A port whose system is built only when a live call needs it, at most
    /// once per run. The port's trait comes from the `#[obzenflow::port]` trait
    /// the system implements.
    ///
    /// ```
    /// # use obzenflow_runtime::effects::{EffectPortResolutionError, Port};
    /// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # struct SimulatedRiskEngine;
    /// # #[obzenflow_runtime::effects::system("simulated-risk-engine", effects = obzenflow_runtime::effects)]
    /// # impl RiskEnginePort for SimulatedRiskEngine {}
    /// # impl SimulatedRiskEngine {
    /// #     fn from_env() -> Result<Self, EffectPortResolutionError> {
    /// #         Ok(Self)
    /// #     }
    /// # }
    /// let engine = Port::deferred(SimulatedRiskEngine::from_env);
    /// # let port: Port<dyn RiskEnginePort> = engine;
    /// ```
    ///
    /// A resolver that returns an `Arc` does not compile; the port wraps the
    /// system itself:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::effects::{EffectPortResolutionError, Port};
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # struct SimulatedRiskEngine;
    /// # #[obzenflow_runtime::effects::system("simulated-risk-engine", effects = obzenflow_runtime::effects)]
    /// # impl RiskEnginePort for SimulatedRiskEngine {}
    /// # fn main() {
    /// let engine = Port::deferred(|| -> Result<Arc<SimulatedRiskEngine>, EffectPortResolutionError> {
    ///     Ok(Arc::new(SimulatedRiskEngine))
    /// });
    /// # let port: Port<dyn RiskEnginePort> = engine;
    /// # }
    /// ```
    pub fn deferred<T, F>(resolver: F) -> Self
    where
        T: SystemName + Send + Sync + 'static,
        F: Fn() -> Result<T, EffectPortResolutionError> + Send + Sync + 'static,
        PortImpl<T>: ServesPort<P, T>,
    {
        Self::answered_by::<T>(SystemPort::deferred::<P>(Arc::new(move || {
            resolver().map(|system| <PortImpl<T> as ServesPort<P, T>>::coerce(Arc::new(system)))
        })))
    }

    fn answered_by<T: SystemName>(port: SystemPort) -> Self {
        let name = checked_system_name::<T>();
        Self {
            name: LogicalEffectBindingName::of_system(name),
            evidence: SystemBindingEvidence::named(name)
                .expect("a checked system name fits the evidence bound"),
            port: Arc::new(port),
            port_type: PhantomData,
        }
    }

    /// The name of the system behind this port, as its evidence and diagnostics
    /// record it.
    pub fn system_name(&self) -> &str {
        self.name.as_str()
    }
}

const fn checked_system_name<T: SystemName>() -> &'static str {
    const {
        assert!(
            is_system_name(T::NAME),
            "a system name is 1 to 64 characters from `[A-Za-z0-9._-]` and starts with a letter or digit"
        );
    }
    T::NAME
}

/// The name a system answers under, which every call it answers records.
/// `#[obzenflow::system("name")]` implements it on the system's `impl` of its
/// port, and `#[obzenflow::port(system = "name")]` on a struct that serves as
/// its own port. Name a system once, with one of those attributes.
///
/// A system without a name cannot become a [`Port`], whether it implements a
/// port trait:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::Port;
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
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
/// # use obzenflow_runtime::effects::Port;
/// struct AuditLedger;
///
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// impl AuditLedger {}
/// # fn main() {
/// let ledger = Port::to(AuditLedger);
/// # let port: Port<AuditLedger> = ledger;
/// # }
/// ```
///
/// Two names for one type do not compile:
///
/// ```compile_fail,E0119
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {}
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CaseSystemPort: Send + Sync {}
/// struct Vendor;
///
/// #[obzenflow_runtime::effects::system("vendor-risk", effects = obzenflow_runtime::effects)]
/// impl RiskEnginePort for Vendor {}
///
/// #[obzenflow_runtime::effects::system("vendor-cases", effects = obzenflow_runtime::effects)]
/// impl CaseSystemPort for Vendor {}
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no system name; put `#[obzenflow::system(\"…\")]` above its `impl` of the port, or `system = \"…\"` on its `#[obzenflow::port]` block if it serves as its own port",
    label = "this system does not name itself",
    note = "a resolver returns the system itself, never an `Arc`; `Port::to` takes the system by value or in an `Arc` of its concrete type, and an erased `Arc<dyn Port>` names no system"
)]
pub trait SystemName {
    const NAME: &'static str;
}

/// A built system, by value or in an `Arc`, as [`Port::to`] takes it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no system name; put `#[obzenflow::system(\"…\")]` above its `impl` of the port, or `system = \"…\"` on its `#[obzenflow::port]` block if it serves as its own port",
    label = "this system does not name itself",
    note = "a resolver returns the system itself, never an `Arc`; `Port::to` takes the system by value or in an `Arc` of its concrete type, and an erased `Arc<dyn Port>` names no system"
)]
pub trait BuiltSystem {
    type System: SystemName;

    fn shared(self) -> Arc<Self::System>;
}

impl<T: SystemName> BuiltSystem for T {
    type System = T;

    fn shared(self) -> Arc<T> {
        Arc::new(self)
    }
}

impl<T: SystemName> BuiltSystem for Arc<T> {
    type System = T;

    fn shared(self) -> Arc<T> {
        self
    }
}

/// The type a system is looked up under. The port attribute implements
/// [`ServesPort`] for it.
#[doc(hidden)]
pub struct PortImpl<T>(PhantomData<fn() -> Arc<T>>);

/// Turns a system `T` into the trait object `P` of the port it serves,
/// generated for each trait marked `#[obzenflow::port]`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this system implements no trait marked `#[obzenflow::port]`",
    label = "the system this port is built from",
    note = "mark the port trait it implements, or the `impl` block of a struct that serves as its own port, with `#[obzenflow::port]`"
)]
pub trait ServesPort<P: ?Sized, T> {
    fn coerce(port: Arc<T>) -> Arc<P>;
}

/// The answer a port method gives, its request's outcome or an `EffectError`.
/// `#[obzenflow::port]` requires it of every method on a port trait.
///
/// ```
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "docs.port.charged"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait CardPort: Send + Sync {
///     fn charge(&self) -> Result<Charged, EffectError>;
/// }
/// ```
///
/// A method that returns a system's own answer does not compile:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # enum GatewayResponse { Charged }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait GatewayPort: Send + Sync {
///     fn authorize(&self) -> Result<GatewayResponse, EffectError>;
/// }
/// ```
///
/// Nor does one that fails with its own error:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "docs.port.charged"; }
/// # #[derive(Debug)]
/// # struct GatewayError;
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait GatewayPort: Send + Sync {
///     fn authorize(&self) -> Result<Charged, GatewayError>;
/// }
/// ```
///
/// Nor one that answers with a bare fact:
///
/// ```compile_fail,E0277
/// # use obzenflow_core::TypedPayload;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "docs.port.charged"; }
/// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// trait CardPort: Send + Sync {
///     fn charge(&self) -> Charged;
/// }
/// ```
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a port method answers with its request's outcome and fails with `EffectError`",
    label = "`{Self}` is not `Result<Outcome, EffectError>`",
    note = "keep a system's own answer behind the port and turn it into the outcome there"
)]
pub trait PortAnswer {
    type Outcome;
}

impl<O: EffectOutcomeFacts> PortAnswer for Result<O, EffectError> {
    type Outcome = O;
}

#[doc(hidden)]
pub fn assert_port_answer<T: PortAnswer>() {}

impl<P: ?Sized + Send + Sync + 'static> Clone for Port<P> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            evidence: self.evidence.clone(),
            port: Arc::clone(&self.port),
            port_type: PhantomData,
        }
    }
}

impl<P: ?Sized + Send + Sync + 'static> std::fmt::Debug for Port<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Port")
            .field("system", &self.name.as_str())
            .field("port", &"<not disclosed>")
            .finish()
    }
}

mod sealed {
    pub trait StagePort {}
}

/// A value a `#[port]` field holds: a [`Port`], or a typed binding of a
/// hand-written named effect.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a port",
    label = "a `#[port]` field holds a port the command works through",
    note = "hold a `Port` value, or the typed binding of a hand-written named effect; \
            AI effects are performed by `ai!`, whose model comes from [ai.*] configuration"
)]
pub trait StagePort: sealed::StagePort {}

impl<P: ?Sized + Send + Sync + 'static> sealed::StagePort for Port<P> {}
impl<P: ?Sized + Send + Sync + 'static> StagePort for Port<P> {}
impl<E: NamedEffect> sealed::StagePort for EffectBinding<E> {}
impl<E: NamedEffect> StagePort for EffectBinding<E> {}

/// How a command holds one of its ports, as its diagnostics name it.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortField {
    /// A `#[port]` field, by its name.
    Named(&'static str),
    /// A field `through` generated, by the name of its port.
    Through(&'static str),
    /// The one port of a `perform` command, which allows only its one request.
    Performed,
}

/// A command: an effectful handler that declares the ports it works through.
/// `#[obzenflow::command(through = P)]` on a unit struct implements it, as does
/// `#[obzenflow::command]` on a struct that marks each field holding a port
/// `#[port]`; a command that works through no port has no such field. The
/// command may perform every effect a port declares, and a field written
/// `#[port(RequestA, RequestB)]` allows only the requests it lists.
///
/// ```
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {}
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CaseSystemPort: Send + Sync {}
/// # struct Vendor;
/// # #[obzenflow_runtime::effects::system("vendor", effects = obzenflow_runtime::effects)]
/// # impl RiskEnginePort for Vendor {}
/// # impl CaseSystemPort for Vendor {}
/// # use obzenflow_runtime::effects::Port;
/// #[obzenflow_runtime::effects::command(through = RiskEnginePort, effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct Scorer;
///
/// #[obzenflow_runtime::effects::command(through = (RiskEnginePort, CaseSystemPort), effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct Triage;
///
/// let scorer = Scorer::through(Port::to(Vendor));
/// let triage = Triage::through(Port::to(std::sync::Arc::new(Vendor)), Port::to(Vendor));
/// ```
///
/// A command with state of its own names its port as a field:
///
/// ```
/// # use obzenflow_runtime::effects::Port;
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {}
/// #[obzenflow_runtime::effects::command(effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct Scorer {
///     #[port]
///     risk_engine_port: Port<dyn RiskEnginePort>,
///     limit: u32,
/// }
/// ```
///
/// An effectful handler without it does not compile at its impl:
///
/// ```compile_fail,E0277
/// # use async_trait::async_trait;
/// # use obzenflow_runtime::effects::{Effects, StageCompletion};
/// # use obzenflow_runtime::stages::common::handler_error::HandlerError;
/// # use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Order;
/// # impl obzenflow_core::TypedPayload for Order { const EVENT_TYPE: &'static str = "docs.handler.order"; }
/// #[derive(Clone, Debug)]
/// struct Checkout;
///
/// #[async_trait]
/// impl EffectfulTransformHandler for Checkout {
///     type Input = Order;
///     type Output = Order;
///
///     async fn process(
///         &self,
///         order: Order,
///         fx: &mut Effects<Order, Self::AllowedEffects>,
///     ) -> Result<StageCompletion<Order>, HandlerError> {
///         fx.emit(order).await?;
///         Ok(fx.complete()?)
///     }
/// }
/// ```
///
/// A `through` path that is not a port does not compile:
///
/// ```compile_fail,E0277
/// trait RiskEnginePort: Send + Sync {}
/// #[obzenflow_runtime::effects::command(through = RiskEnginePort, effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct Scorer;
/// ```
///
/// Nor does a `through` struct with a field of its own:
///
/// ```compile_fail
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {}
/// #[obzenflow_runtime::effects::command(through = RiskEnginePort, effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct Scorer {
///     limit: u32,
/// }
/// ```
///
/// Nor two ports of one type:
///
/// ```compile_fail
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {}
/// #[obzenflow_runtime::effects::command(through = (RiskEnginePort, RiskEnginePort), effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct Scorer;
/// ```
///
/// Nor a `#[port]` field that holds no port:
///
/// ```compile_fail,E0277
/// #[obzenflow_runtime::effects::command(effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct Scorer {
///     #[port]
///     risk_engine_port: std::sync::Arc<u32>,
/// }
/// ```
///
/// A field lists only requests its own port declares:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::{EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.handler.scored"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct CaseRequest;
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {
/// #     #[effect(type = "docs.handler.score", exactly_once)]
/// #     fn score(&self, request: &ScoreRequest) -> Result<Scored, EffectError>;
/// # }
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait CasePort: Send + Sync {
/// #     #[effect(type = "docs.handler.case", exactly_once)]
/// #     fn open(&self, request: &CaseRequest) -> Result<Scored, EffectError>;
/// # }
/// #[obzenflow_runtime::effects::command(effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct ScoreHandler {
///     #[port(CaseRequest)]
///     risk_engine: Port<dyn RiskEnginePort>,
/// }
/// ```
///
/// lists each request once:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.handler.scored"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest;
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {
/// #     #[effect(type = "docs.handler.score", exactly_once)]
/// #     fn score(&self, request: &ScoreRequest) -> Result<Scored, EffectError>;
/// # }
/// #[obzenflow_runtime::effects::command(effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct ScoreHandler {
///     #[port(ScoreRequest, ScoreRequest)]
///     risk_engine: Port<dyn RiskEnginePort>,
/// }
/// ```
///
/// and lists at least one, since a bare `#[port]` allows the whole port:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{EffectError, Port};
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.handler.scored"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest;
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {
/// #     #[effect(type = "docs.handler.score", exactly_once)]
/// #     fn score(&self, request: &ScoreRequest) -> Result<Scored, EffectError>;
/// # }
/// #[obzenflow_runtime::effects::command(effects = obzenflow_runtime::effects)]
/// #[derive(Clone, Debug)]
/// struct ScoreHandler {
///     #[port()]
///     risk_engine: Port<dyn RiskEnginePort>,
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a command; add `#[obzenflow::command(through = …)]` above it, or `#[obzenflow::command]` with a `#[port]` field for each port it works through; a `perform` becomes one with `.through(port)`",
    label = "an effectful stage's handler is a command"
)]
pub trait Command {
    #[doc(hidden)]
    type Ports: PortList;

    /// The effects this command may perform, which its ports allow.
    type AllowedEffects: EffectSet;

    #[doc(hidden)]
    const PORT_FIELDS: &'static [PortField] = &[];

    #[doc(hidden)]
    fn ports(&self) -> Self::Ports;
}

#[doc(hidden)]
pub fn assert_command<H: Command>(_: &H) {}

/// The effects a port declares, in method order. `#[obzenflow::port]`
/// implements it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a port; mark the trait, or the `impl` block of a struct that serves as its own port, with `#[obzenflow::port]`",
    label = "a command works through ports, which declare its effects"
)]
pub trait PortEffects {
    type Members;
}

/// A port held whole, which allows every effect it declares.
#[doc(hidden)]
pub struct WholePort<S>(PhantomData<fn() -> S>);

/// A `#[port(...)]` field, which allows only the requests it lists.
#[doc(hidden)]
pub struct NarrowedPort<S, Requests>(PhantomData<fn() -> (S, Requests)>);

/// The effects one held port allows.
#[doc(hidden)]
pub trait FieldEffects {
    type Members;
}

impl<P: ?Sized + PortEffects + Send + Sync + 'static> FieldEffects for WholePort<Port<P>> {
    type Members = P::Members;
}

impl<E: NamedEffect> FieldEffects for WholePort<EffectBinding<E>> {
    type Members = WithMember<E, EmptySet>;
}

impl<S, Requests: RequestList> FieldEffects for NarrowedPort<S, Requests> {
    type Members = Requests::Members;
}

/// The effects a command's ports allow, in the order it holds them.
#[doc(hidden)]
pub trait FieldsEffects {
    type Members;
}

impl FieldsEffects for () {
    type Members = EmptySet;
}

impl<Field, Rest> FieldsEffects for (Field, Rest)
where
    Field: FieldEffects,
    Rest: FieldsEffects,
    Field::Members: JoinMembers<Rest::Members>,
{
    type Members = <Field::Members as JoinMembers<Rest::Members>>::Joined;
}

#[doc(hidden)]
pub trait JoinMembers<Rest> {
    type Joined;
}

impl<Rest> JoinMembers<Rest> for EmptySet {
    type Joined = Rest;
}

impl<Head, Tail: JoinMembers<Rest>, Rest> JoinMembers<Rest> for WithMember<Head, Tail> {
    type Joined = WithMember<Head, Tail::Joined>;
}

/// Proof that a `#[port(...)]` field's port declares a request it lists.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{R}` is not an effect of this `#[port]` field's port",
    label = "this field's port has no `#[effect]` method that takes `{R}`",
    note = "list only requests whose `#[effect]` method is on the field's port"
)]
pub trait PortServesRequest<R> {}

#[diagnostic::do_not_recommend]
impl<P, R> PortServesRequest<R> for Port<P>
where
    P: ?Sized + Send + Sync + 'static,
    R: CallRequest<Port = P>,
{
}

/// How a stage row acknowledges an at-least-once effect.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgedSafety {
    AtLeastOnce,
}

/// One `.at_least_once::<R>()` decoration.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct EffectAcknowledgement {
    effect_type: &'static str,
    written: &'static str,
    safety: AcknowledgedSafety,
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is an AI effect; an `ai!` row is itself the acknowledgement for its AI effect",
    label = "only `ai!` performs and acknowledges this effect",
    note = "AI effects are performed by `ai!`; the model comes from [ai.*] configuration"
)]
pub trait RowAcknowledgedEffect: Effect {}

impl<E: NamedEffect> RowAcknowledgedEffect for E {}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "an effectful handler cannot perform an AI effect",
    label = "this handler's allowed effects include an AI effect",
    note = "AI effects are performed by `ai!`; the model comes from [ai.*] configuration"
)]
pub trait HandlerPerformableEffects {}

impl HandlerPerformableEffects for EmptySet {}

impl<N, Rest> HandlerPerformableEffects for WithMember<N, Rest>
where
    N: EffectName,
    N::Effect: NamedEffect,
    Rest: HandlerPerformableEffects,
{
}

#[doc(hidden)]
pub fn assert_handler_performs_no_ai_effect<H>(_: &H)
where
    H: EffectfulTransformHandler,
    <H::AllowedEffects as EffectSet>::Members: HandlerPerformableEffects,
{
}

#[doc(hidden)]
pub fn assert_stateful_handler_performs_no_ai_effect<H>(_: &H)
where
    H: EffectfulStatefulHandler,
    <H::AllowedEffects as EffectSet>::Members: HandlerPerformableEffects,
{
}

impl EffectAcknowledgement {
    pub fn at_least_once<N>(written: &'static str) -> Self
    where
        N: EffectName,
        N::Effect: RowAcknowledgedEffect,
    {
        Self {
            effect_type: <N::Effect as Effect>::EFFECT_TYPE,
            written,
            safety: AcknowledgedSafety::AtLeastOnce,
        }
    }

    fn decoration(&self) -> String {
        match self.safety {
            AcknowledgedSafety::AtLeastOnce => format!(".at_least_once::<{}>()", self.written),
        }
    }
}

fn declaration_syntax(acknowledged: Option<AcknowledgedSafety>) -> EffectDeclarationSyntax {
    match acknowledged {
        None => EffectDeclarationSyntax::Bare,
        Some(AcknowledgedSafety::AtLeastOnce) => EffectDeclarationSyntax::AtLeastOnce,
    }
}

/// A refusal found while declaring a stage's effects from its handler.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageEffectsError {
    UnperformedAcknowledgement {
        effect_type: &'static str,
        decoration: String,
    },
    RepeatedAcknowledgement {
        effect_type: &'static str,
    },
    Registration {
        source: EffectBindingBuildError,
    },
}

impl StageEffectsError {
    pub fn for_stage(&self, stage: &str) -> String {
        match self {
            Self::UnperformedAcknowledgement {
                effect_type,
                decoration,
            } => format!(
                "Effectful stage '{stage}' acknowledges `{effect_type}`, which its handler does not perform; remove `{decoration}`"
            ),
            Self::RepeatedAcknowledgement { effect_type } => format!(
                "Effectful stage '{stage}' acknowledges `{effect_type}` more than once"
            ),
            Self::Registration { source } => {
                format!("Effectful stage '{stage}' cannot register its system: {source}")
            }
        }
    }
}

impl From<EffectBindingBuildError> for StageEffectsError {
    fn from(source: EffectBindingBuildError) -> Self {
        Self::Registration { source }
    }
}

/// A port whose system answers the named effect `E`.
#[doc(hidden)]
pub trait AnswersEffect<E: Effect> {
    #[doc(hidden)]
    fn declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<P, R> AnswersEffect<Call<R>> for Port<P>
where
    P: ?Sized + Send + Sync + 'static,
    R: CallRequest<Port = P>,
{
    fn declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        super::binding::validate_effect_type(R::EFFECT_TYPE)
            .map_err(|_| EffectBindingBuildError::InvalidEffectType)?;
        let binding =
            EffectRegistrationBuilder::<Call<R>>::new(self.name.clone(), self.evidence.clone())
                .bind_system(call_port_slot::<R>(), &self.port)?
                .finish()?;
        Ok(EffectDeclaration::for_binding::<Call<R>>(
            binding.declaration_binding(),
            declaration_syntax(acknowledged),
        )
        .allowed_by_port_field())
    }
}

impl<E: NamedEffect> AnswersEffect<E> for EffectBinding<E> {
    fn declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        Ok(EffectDeclaration::for_binding::<E>(
            self.declaration_binding(),
            declaration_syntax(acknowledged),
        ))
    }
}

/// Index witness: the port that answers is the head of the list.
#[doc(hidden)]
pub struct PortFound;

/// Index witness: the port that answers follows the head.
#[doc(hidden)]
pub struct PortAfter<Index>(PhantomData<fn() -> Index>);

/// Proof that exactly one of the command's ports answers the effect named
/// `N`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this command performs `{N}`, which needs a port",
    label = "no port of the command answers `{N}`",
    note = "add a `#[port]` field whose port the request names"
)]
pub trait FindPort<N: EffectName, Index> {
    #[doc(hidden)]
    const POSITION: usize;

    #[doc(hidden)]
    fn found_declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<N: EffectName, S: AnswersEffect<N::Effect>, Rest> FindPort<N, PortFound> for (S, Rest) {
    const POSITION: usize = 0;

    fn found_declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        self.0.declaration(acknowledged)
    }
}

#[diagnostic::do_not_recommend]
impl<N: EffectName, S, Rest: FindPort<N, Index>, Index> FindPort<N, PortAfter<Index>>
    for (S, Rest)
{
    const POSITION: usize = 1 + Rest::POSITION;

    fn found_declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        self.1.found_declaration(acknowledged)
    }
}

/// A port that answers at least one of the command's effects.
#[doc(hidden)]
pub struct Used<S>(PhantomData<fn() -> S>);

/// A port that has not yet answered any of the command's effects.
#[doc(hidden)]
pub struct Unused<S>(PhantomData<fn() -> S>);

#[doc(hidden)]
pub trait PortMark {
    type Port;
}

impl<S> PortMark for Used<S> {
    type Port = S;
}

impl<S> PortMark for Unused<S> {
    type Port = S;
}

#[doc(hidden)]
pub trait MarkPortUsed<Index> {
    type Marked;
}

impl<Index> MarkPortUsed<Index> for () {
    type Marked = ();
}

impl<M: PortMark, Rest> MarkPortUsed<PortFound> for (M, Rest) {
    type Marked = (Used<M::Port>, Rest);
}

impl<M, Rest: MarkPortUsed<Index>, Index> MarkPortUsed<PortAfter<Index>> for (M, Rest) {
    type Marked = (M, Rest::Marked);
}

#[doc(hidden)]
pub trait PortList {
    type Unmarked;
}

impl PortList for () {
    type Unmarked = ();
}

impl<S: StagePort, Rest: PortList> PortList for (S, Rest) {
    type Unmarked = (Unused<S>, Rest::Unmarked);
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this field's port declares no `#[effect]` method",
    label = "`{Self}` allows no effect",
    note = "remove the field, or declare an `#[effect]` method on its port"
)]
pub trait PortAnswersAnEffect {}

impl<S> PortAnswersAnEffect for Used<S> {}

#[doc(hidden)]
pub trait EveryPortAnswers {}

impl EveryPortAnswers for () {}

impl<M: PortAnswersAnEffect, Rest: EveryPortAnswers> EveryPortAnswers for (M, Rest) {}

#[doc(hidden)]
pub trait DeclareEffects<Ports, Mask, Index> {
    type Marked;

    #[doc(hidden)]
    fn declarations(
        ports: &Ports,
        fields: &[PortField],
        acknowledgements: &[EffectAcknowledgement],
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError>;
}

impl<Ports, Mask> DeclareEffects<Ports, Mask, ()> for EmptySet {
    type Marked = Mask;

    fn declarations(
        _: &Ports,
        _: &[PortField],
        _: &[EffectAcknowledgement],
        _: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError> {
        Ok(())
    }
}

impl<N, Rest, Ports, Mask, Index, RestIndex> DeclareEffects<Ports, Mask, (Index, RestIndex)>
    for WithMember<N, Rest>
where
    N: EffectName,
    Ports: FindPort<N, Index>,
    Mask: MarkPortUsed<Index>,
    Rest: DeclareEffects<Ports, Mask::Marked, RestIndex>,
{
    type Marked = <Rest as DeclareEffects<Ports, Mask::Marked, RestIndex>>::Marked;

    fn declarations(
        ports: &Ports,
        fields: &[PortField],
        acknowledgements: &[EffectAcknowledgement],
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError> {
        let acknowledged = acknowledgements
            .iter()
            .find(|acknowledgement| {
                acknowledgement.effect_type == <N::Effect as Effect>::EFFECT_TYPE
            })
            .map(|acknowledgement| acknowledgement.safety);
        let field = fields.get(Ports::POSITION).copied();
        out.push(
            ports
                .found_declaration(acknowledged)?
                .written_as::<N>()
                .held_in_field(field),
        );
        Rest::declarations(ports, fields, acknowledgements, out)
    }
}

fn declare_stage_effects<Set, Ports, Index>(
    ports: &Ports,
    fields: &[PortField],
    acknowledgements: &[EffectAcknowledgement],
) -> Result<Vec<EffectDeclaration>, StageEffectsError>
where
    Set: EffectSet,
    Ports: PortList,
    Set::Members: DeclareEffects<Ports, Ports::Unmarked, Index>,
{
    for (position, acknowledgement) in acknowledgements.iter().enumerate() {
        if acknowledgements[..position]
            .iter()
            .any(|earlier| earlier.effect_type == acknowledgement.effect_type)
        {
            return Err(StageEffectsError::RepeatedAcknowledgement {
                effect_type: acknowledgement.effect_type,
            });
        }
        if !Set::effect_types().contains(&acknowledgement.effect_type) {
            return Err(StageEffectsError::UnperformedAcknowledgement {
                effect_type: acknowledgement.effect_type,
                decoration: acknowledgement.decoration(),
            });
        }
    }
    let mut declarations = Vec::new();
    <Set::Members as DeclareEffects<Ports, Ports::Unmarked, Index>>::declarations(
        ports,
        fields,
        acknowledgements,
        &mut declarations,
    )?;
    Ok(declarations)
}

#[doc(hidden)]
pub fn effectful_transform_effects<H, Index>(
    handler: &H,
    acknowledgements: &[EffectAcknowledgement],
) -> Result<Vec<EffectDeclaration>, StageEffectsError>
where
    H: EffectfulTransformHandler,
    <H::AllowedEffects as EffectSet>::Members:
        DeclareEffects<H::Ports, <H::Ports as PortList>::Unmarked, Index>,
    <<H::AllowedEffects as EffectSet>::Members as DeclareEffects<
        H::Ports,
        <H::Ports as PortList>::Unmarked,
        Index,
    >>::Marked: EveryPortAnswers,
{
    declare_stage_effects::<H::AllowedEffects, H::Ports, Index>(
        &handler.ports(),
        H::PORT_FIELDS,
        acknowledgements,
    )
}

#[doc(hidden)]
pub fn effectful_stateful_effects<H, Index>(
    handler: &H,
    acknowledgements: &[EffectAcknowledgement],
) -> Result<Vec<EffectDeclaration>, StageEffectsError>
where
    H: EffectfulStatefulHandler,
    <H::AllowedEffects as EffectSet>::Members:
        DeclareEffects<H::Ports, <H::Ports as PortList>::Unmarked, Index>,
    <<H::AllowedEffects as EffectSet>::Members as DeclareEffects<
        H::Ports,
        <H::Ports as PortList>::Unmarked,
        Index,
    >>::Marked: EveryPortAnswers,
{
    declare_stage_effects::<H::AllowedEffects, H::Ports, Index>(
        &handler.ports(),
        H::PORT_FIELDS,
        acknowledgements,
    )
}
