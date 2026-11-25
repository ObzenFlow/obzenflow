// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::System;
use crate::effects::binding::is_system_name;
use crate::effects::ports::SystemPort;
use crate::effects::{EffectPortResolutionError, LogicalEffectBindingName, PortEffects};
use obzenflow_core::SystemBindingEvidence;
use std::marker::PhantomData;
use std::sync::Arc;

/// A port `P` with the system that answers it chosen. A source asks its queries
/// through one, and a command holds one per port type, in a `#[port]` field or
/// through `through`, and performs requests through `fx` without ever seeing the
/// system.
///
/// A port trait is `Send` and `Sync`, so a system that isn't thread-safe is
/// refused at its own name:
///
/// ```compile_fail,E0277
/// #[obzenflow_runtime::effects::port]
/// trait CustomerMessaging {}
/// #[obzenflow_runtime::effects::system("simulated")]
/// struct SimulatedMessaging(std::rc::Rc<()>);
/// impl CustomerMessaging for SimulatedMessaging {}
/// ```
pub struct Port<P: ?Sized + Send + Sync + 'static> {
    pub(crate) name: LogicalEffectBindingName,
    pub(crate) evidence: SystemBindingEvidence,
    pub(crate) port: Arc<SystemPort>,
    port_type: PhantomData<fn() -> Arc<P>>,
}

impl<P: ?Sized + Send + Sync + 'static> Port<P> {
    /// A port answered by a system that is already built, by value or in an
    /// `Arc`. The port's trait comes from the `#[obzenflow::port]` trait the
    /// system implements, so the port needs no annotation.
    ///
    /// ```
    /// # use obzenflow_runtime::systems::Port;
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # #[obzenflow_runtime::effects::system("risk-engine-outage")]
    /// # struct RiskEngineOutage;
    /// # impl RiskEnginePort for RiskEngineOutage {}
    /// let outage = Port::to(RiskEngineOutage);
    /// let shared = Port::to(Arc::new(RiskEngineOutage));
    /// # let ports: [Port<dyn RiskEnginePort>; 2] = [outage, shared];
    /// ```
    ///
    /// A port trait left unmarked does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::systems::Port;
    /// trait RiskEnginePort: Send + Sync {}
    /// # #[obzenflow_runtime::effects::system("risk-engine-outage")]
    /// struct RiskEngineOutage;
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
    /// # use obzenflow_runtime::systems::Port;
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # #[obzenflow_runtime::effects::system("risk-engine-outage")]
    /// # struct RiskEngineOutage;
    /// # impl RiskEnginePort for RiskEngineOutage {}
    /// # fn main() {
    /// let erased: Arc<dyn RiskEnginePort> = Arc::new(RiskEngineOutage);
    /// let outage = Port::to(erased);
    /// # let port: Port<dyn RiskEnginePort> = outage;
    /// # }
    /// ```
    ///
    /// A struct whose inherent `impl` block carries `#[obzenflow::port]` serves
    /// as its own port, and names itself on its type:
    ///
    /// ```
    /// # use obzenflow_runtime::effects::EffectError;
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Clone, Debug, serde::Serialize)]
    /// # struct AuditEntry;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct AuditAppended;
    /// # impl obzenflow_core::TypedPayload for AuditAppended { const EVENT_TYPE: &'static str = "docs.audit.appended"; }
    /// #[obzenflow_runtime::effects::system("audit-ledger")]
    /// struct AuditLedger;
    ///
    /// #[obzenflow_runtime::effects::port]
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
    /// A name outside the grammar does not compile, even from a hand-written
    /// [`System`]:
    ///
    /// ```compile_fail,E0080
    /// # use obzenflow_runtime::systems::{Port, System};
    /// # #[obzenflow_runtime::effects::port]
    /// # trait RiskEnginePort: Send + Sync {}
    /// struct RiskEngineOutage;
    /// impl RiskEnginePort for RiskEngineOutage {}
    /// impl System for RiskEngineOutage {
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
    /// # use obzenflow_runtime::effects::EffectPortResolutionError;
    /// # use obzenflow_runtime::systems::Port;
    /// # #[obzenflow_runtime::effects::port]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # #[obzenflow_runtime::effects::system("simulated-risk-engine")]
    /// # struct SimulatedRiskEngine;
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
    /// # use obzenflow_runtime::effects::EffectPortResolutionError;
    /// # use obzenflow_runtime::systems::Port;
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # #[obzenflow_runtime::effects::system("simulated-risk-engine")]
    /// # struct SimulatedRiskEngine;
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
        T: System,
        F: Fn() -> Result<T, EffectPortResolutionError> + Send + Sync + 'static,
        PortImpl<T>: ServesPort<P, T>,
    {
        Self::answered_by::<T>(SystemPort::deferred::<P>(Arc::new(move || {
            resolver().map(|system| <PortImpl<T> as ServesPort<P, T>>::coerce(Arc::new(system)))
        })))
    }

    #[doc(hidden)]
    pub fn deferred_grouped<R, T, F>(resolver: F) -> Self
    where
        R: crate::effects::SinkRequest<Port = P>,
        T: System + crate::effects::SinkGroupSystem<R>,
        PortImpl<T>: ServesPort<P, T>,
        F: Fn() -> Result<T, EffectPortResolutionError> + Send + Sync + 'static,
    {
        Self::answered_by::<T>(SystemPort::grouped::<P, R, T, F>(resolver))
    }

    pub(crate) fn configured<T, F>(evidence: SystemBindingEvidence, resolver: F) -> Self
    where
        T: System,
        F: Fn() -> Result<T, EffectPortResolutionError> + Send + Sync + 'static,
        PortImpl<T>: ServesPort<P, T>,
    {
        let port = SystemPort::deferred::<P>(Arc::new(move || {
            resolver().map(|system| <PortImpl<T> as ServesPort<P, T>>::coerce(Arc::new(system)))
        }));
        let name = LogicalEffectBindingName::new(instance_name(
            checked_system_name::<T>(),
            &port.ordinal().to_string(),
        ))
        .unwrap_or_else(|_| LogicalEffectBindingName::of_system(checked_system_name::<T>()));
        Self {
            name,
            evidence,
            port: Arc::new(port),
            port_type: PhantomData,
        }
    }

    fn answered_by<T: System>(port: SystemPort) -> Self {
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
        if self.binding().configuration().is_some() {
            self.binding().name()
        } else {
            self.name.as_str()
        }
    }

    /// `binding` returns the system binding that this port records when it answers a call.
    pub fn binding(&self) -> &obzenflow_core::effect_binding::SystemBinding {
        self.evidence.binding()
    }

    pub(crate) fn query_name(&self) -> LogicalEffectBindingName {
        LogicalEffectBindingName::new(instance_name(
            self.binding().name(),
            &self.port.ordinal().to_string(),
        ))
        .unwrap_or_else(|_| self.name.clone())
    }

    /// The same port bound under `system.instance`, for a framework system
    /// whose instances each hold their own destination, such as one file each.
    /// Characters a binding name can't hold become `_`.
    #[doc(hidden)]
    pub fn instance(mut self, instance: &str) -> Self
    where
        P: PortEffects,
    {
        if let Ok(name) = LogicalEffectBindingName::new(instance_name(self.name.as_str(), instance))
        {
            self.name = name;
        }
        self
    }
}

fn instance_name(base: &str, instance: &str) -> String {
    let valid = |character: char| {
        if character.is_ascii_alphanumeric() || matches!(character, '.' | '_' | '-') {
            character
        } else {
            '_'
        }
    };
    let suffix: String = format!(".{instance}").chars().map(valid).take(63).collect();
    let base: String = base
        .chars()
        .map(valid)
        .take(64 - suffix.chars().count())
        .collect();
    format!("{base}{suffix}")
}

pub(crate) const fn checked_system_name<T: System>() -> &'static str {
    const {
        assert!(
            is_system_name(T::NAME),
            "a system name is 1 to 64 characters from `[A-Za-z0-9._-]` and starts with a letter or digit"
        );
    }
    T::NAME
}

/// A built system, by value or in an `Arc`, as [`Port::to`] takes it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no system name",
    note = "put `#[obzenflow::system(\"…\")]` above the type that answers the port",
    label = "this system does not name itself",
    note = "a resolver returns the system itself, never an `Arc`; `Port::to` takes the system by value or in an `Arc` of its concrete type, and an erased `Arc<dyn Port>` names no system"
)]
pub trait BuiltSystem {
    type System: System;

    fn shared(self) -> Arc<Self::System>;
}

impl<T: System> BuiltSystem for T {
    type System = T;

    fn shared(self) -> Arc<T> {
        Arc::new(self)
    }
}

impl<T: System> BuiltSystem for Arc<T> {
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
            .field("system", &self.system_name())
            .field("port", &"<not disclosed>")
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::{instance_name, Port};
    use crate::commands::AnswersEffect;
    use crate::effects::{Call, EffectError, EffectPortRegistry};
    use crate::queries::{FiniteRead, QueryCall, QueryContext, QueryError};
    use std::sync::Arc;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct NextEntry(u64);

    #[derive(Clone, Debug, serde::Serialize)]
    struct AppendEntry;

    #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    struct EntryAppended;

    impl obzenflow_core::TypedPayload for EntryAppended {
        const EVENT_TYPE: &'static str = "systems.entry_appended";
    }

    #[crate::effects::port]
    #[async_trait::async_trait]
    trait EntryFeed: Send + Sync {
        #[query(type = "systems.entries.read")]
        async fn read(
            &self,
            from: &NextEntry,
            ctx: &QueryContext,
        ) -> Result<FiniteRead<u64, NextEntry>, QueryError>;
    }

    #[crate::effects::port]
    trait EntryLedger: Send + Sync {
        #[effect(type = "systems.entries.append", exactly_once)]
        fn append(&self, request: &AppendEntry) -> Result<EntryAppended, EffectError>;
    }

    #[crate::effects::system("entry-ledger")]
    struct Ledger;

    #[async_trait::async_trait]
    impl EntryFeed for Ledger {
        async fn read(
            &self,
            _: &NextEntry,
            _: &QueryContext,
        ) -> Result<FiniteRead<u64, NextEntry>, QueryError> {
            Ok(FiniteRead::EndOfInput)
        }
    }

    impl EntryLedger for Ledger {
        fn append(&self, _: &AppendEntry) -> Result<EntryAppended, EffectError> {
            Ok(EntryAppended)
        }
    }

    #[test]
    fn shared_system_binds_both_ports() {
        let system = Arc::new(Ledger);
        let feed: Port<dyn EntryFeed> = Port::to(Arc::clone(&system));
        let ledger: Port<dyn EntryLedger> = Port::to(Arc::clone(&system));
        let read = <Port<dyn EntryFeed> as AnswersEffect<QueryCall<NextEntry>>>::declaration(&feed)
            .unwrap();
        let append =
            <Port<dyn EntryLedger> as AnswersEffect<Call<AppendEntry>>>::declaration(&ledger)
                .unwrap();
        EffectPortRegistry::collect_from_declarations([&read, &append])
            .expect("one system answers both ports");
        assert_eq!(
            [feed.binding().name(), ledger.binding().name()],
            ["entry-ledger", "entry-ledger"]
        );
    }

    #[test]
    fn long_base_instance_keeps_suffix() {
        let base = format!("r{}", "a".repeat(63));
        let one = instance_name(&base, "1");
        let two = instance_name(&base, "2");
        assert_ne!(one, two);
        assert_eq!(one.len(), 64);
        assert!(one.ends_with(".1"), "{one}");
        assert_eq!(instance_name("in-memory", "7"), "in-memory.7");
    }
}
