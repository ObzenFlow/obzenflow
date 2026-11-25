// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::ports::SystemPort;
use crate::effects::{EffectPortResolutionError, LogicalEffectBindingName};
use std::marker::PhantomData;
use std::sync::Arc;

/// A port `P` with the system that answers it chosen. A source asks its queries
/// through one, and a command holds one per port type, in a `#[port]` field or
/// through `through`, and performs requests through `fx` without ever seeing the
/// system.
///
/// A port trait is `Send` and `Sync`, so a system that isn't thread-safe is
/// refused where it implements the port:
///
/// ```compile_fail,E0277
/// #[obzenflow_runtime::effects::port]
/// trait CustomerMessaging {}
/// struct SimulatedMessaging(std::rc::Rc<()>);
/// impl CustomerMessaging for SimulatedMessaging {}
/// ```
pub struct Port<P: ?Sized + Send + Sync + 'static> {
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
    /// struct RiskEngineOutage;
    /// impl RiskEnginePort for RiskEngineOutage {}
    /// # fn main() {
    /// let outage = Port::to(RiskEngineOutage);
    /// # let port: Port<dyn RiskEnginePort> = outage;
    /// # }
    /// ```
    ///
    /// A struct whose inherent `impl` block carries `#[obzenflow::port]` serves
    /// as its own port:
    ///
    /// ```
    /// # use obzenflow_runtime::effects::EffectError;
    /// # use obzenflow_runtime::systems::Port;
    /// # #[derive(Clone, Debug, serde::Serialize)]
    /// # struct AuditEntry;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct AuditAppended;
    /// # impl obzenflow_core::TypedPayload for AuditAppended { const EVENT_TYPE: &'static str = "docs.audit.appended"; }
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
    pub fn to<S>(system: S) -> Self
    where
        S: Send + Sync + 'static,
        PortImpl<S>: ServesPort<P, S>,
    {
        Self::over(SystemPort::eager(
            <PortImpl<S> as ServesPort<P, S>>::coerce(Arc::new(system)),
        ))
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
    pub fn deferred<T, F>(resolver: F) -> Self
    where
        T: Send + Sync + 'static,
        F: Fn() -> Result<T, EffectPortResolutionError> + Send + Sync + 'static,
        PortImpl<T>: ServesPort<P, T>,
    {
        Self::over(SystemPort::deferred::<P>(Arc::new(move || {
            resolver().map(|system| <PortImpl<T> as ServesPort<P, T>>::coerce(Arc::new(system)))
        })))
    }

    #[doc(hidden)]
    pub fn deferred_grouped<R, T, F>(resolver: F) -> Self
    where
        R: crate::effects::SinkRequest<Port = P>,
        T: crate::effects::SinkGroupSystem<R>,
        PortImpl<T>: ServesPort<P, T>,
        F: Fn() -> Result<T, EffectPortResolutionError> + Send + Sync + 'static,
    {
        Self::over(SystemPort::grouped::<P, R, T, F>(resolver))
    }

    fn over(port: SystemPort) -> Self {
        Self {
            port: Arc::new(port),
            port_type: PhantomData,
        }
    }

    /// `binding_name` returns the name that the run's registry keys this port's
    /// registrations by. It comes from the port's build-local coordinate, so two
    /// ports never share it and no journal records it.
    pub(crate) fn binding_name(&self) -> LogicalEffectBindingName {
        LogicalEffectBindingName::new(format!("port.{}", self.port.ordinal()))
            .expect("a port coordinate forms a binding name")
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
            port: Arc::clone(&self.port),
            port_type: PhantomData,
        }
    }
}

impl<P: ?Sized + Send + Sync + 'static> std::fmt::Debug for Port<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Port")
            .field("coordinate", &self.port.ordinal())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::Port;
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
            Arc::strong_count(&system),
            3,
            "both ports hold the one instance"
        );
    }
}
