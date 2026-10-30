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
use obzenflow_core::{BindingEvidenceError, SystemBindingEvidence};
use std::marker::PhantomData;
use std::sync::Arc;

/// One system that answers requests through its port `P`. A handler holds it
/// in a `#[system]` field, and it answers each of the handler's requests for `P`.
pub struct System<P: ?Sized + Send + Sync + 'static> {
    name: LogicalEffectBindingName,
    evidence: SystemBindingEvidence,
    port: Arc<SystemPort>,
    port_type: PhantomData<fn() -> Arc<P>>,
}

impl<P: ?Sized + Send + Sync + 'static> System<P> {
    /// A system whose port is already built. The port's trait comes from the
    /// `#[obzenflow::port]` trait that `T` implements, so the system needs no
    /// annotation, whether `T` is the port's implementation or its trait
    /// object.
    ///
    /// ```
    /// # use obzenflow_runtime::effects::{System, SystemError};
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # struct RiskEngineOutage;
    /// # impl RiskEnginePort for RiskEngineOutage {}
    /// # fn main() -> Result<(), SystemError> {
    /// let outage = System::eager("risk-engine-outage", Arc::new(RiskEngineOutage))?;
    /// let erased: Arc<dyn RiskEnginePort> = Arc::new(RiskEngineOutage);
    /// let spare = System::eager("spare-risk-engine", erased)?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A port trait left unmarked does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::effects::{System, SystemError};
    /// # use std::sync::Arc;
    /// trait RiskEnginePort: Send + Sync {}
    /// struct RiskEngineOutage;
    /// impl RiskEnginePort for RiskEngineOutage {}
    /// # fn main() -> Result<(), SystemError> {
    /// let outage = System::eager("risk-engine-outage", Arc::new(RiskEngineOutage))?;
    /// # let system: System<dyn RiskEnginePort> = outage;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A struct whose inherent `impl` block carries `#[obzenflow::port]` serves
    /// as its own port:
    ///
    /// ```
    /// # use obzenflow_runtime::effects::{EffectError, System, SystemError};
    /// # use std::sync::Arc;
    /// # #[derive(Clone, Debug, serde::Serialize)]
    /// # struct AuditEntry;
    /// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
    /// # struct AuditAppended;
    /// # impl obzenflow_core::TypedPayload for AuditAppended { const EVENT_TYPE: &'static str = "docs.audit.appended"; }
    /// struct AuditLedger;
    ///
    /// #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// impl AuditLedger {
    ///     #[effect(type = "docs.audit.append", exactly_once)]
    ///     fn append(&self, _: &AuditEntry) -> Result<AuditAppended, EffectError> {
    ///         Ok(AuditAppended)
    ///     }
    /// }
    /// # fn main() -> Result<(), SystemError> {
    /// let ledger = System::eager("audit-ledger", Arc::new(AuditLedger))?;
    /// # let system: System<AuditLedger> = ledger;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A struct left unmarked does not compile:
    ///
    /// ```compile_fail,E0277
    /// # use obzenflow_runtime::effects::{System, SystemError};
    /// # use std::sync::Arc;
    /// struct AuditLedger;
    /// # fn main() -> Result<(), SystemError> {
    /// let ledger = System::eager("audit-ledger", Arc::new(AuditLedger))?;
    /// # let system: System<AuditLedger> = ledger;
    /// # Ok(())
    /// # }
    /// ```
    pub fn eager<T>(name: &str, port: Arc<T>) -> Result<Self, SystemError>
    where
        T: ?Sized + Send + Sync + 'static,
        PortImpl<T>: ServesPort<P, T>,
    {
        Self::with_port(
            name,
            SystemPort::eager(<PortImpl<T> as ServesPort<P, T>>::coerce(port)),
        )
    }

    /// A system whose port is built only when a live call needs it. The
    /// port's trait comes from the `#[obzenflow::port]` trait that the
    /// resolver's port implements.
    ///
    /// ```
    /// # use obzenflow_runtime::effects::{System, SystemError};
    /// # use std::sync::Arc;
    /// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
    /// # trait RiskEnginePort: Send + Sync {}
    /// # struct SimulatedRiskEngine;
    /// # impl RiskEnginePort for SimulatedRiskEngine {}
    /// # fn main() -> Result<(), SystemError> {
    /// let engine = System::deferred("simulated-risk-engine", || {
    ///     Ok(Arc::new(SimulatedRiskEngine))
    /// })?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn deferred<T, F>(name: &str, resolver: F) -> Result<Self, SystemError>
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn() -> Result<Arc<T>, EffectPortResolutionError> + Send + Sync + 'static,
        PortImpl<T>: ServesPort<P, T>,
    {
        Self::with_port(
            name,
            SystemPort::deferred::<P>(Arc::new(move || {
                resolver().map(<PortImpl<T> as ServesPort<P, T>>::coerce)
            })),
        )
    }

    fn with_port(name: &str, port: SystemPort) -> Result<Self, SystemError> {
        if !is_system_name(name) {
            return Err(SystemError::InvalidName {
                system: name.to_string(),
            });
        }
        let evidence =
            SystemBindingEvidence::named(name).map_err(|source| SystemError::Evidence {
                system: name.to_string(),
                source,
            })?;
        let name = LogicalEffectBindingName::new(name).map_err(|_| SystemError::InvalidName {
            system: name.to_string(),
        })?;
        Ok(Self {
            name,
            evidence,
            port: Arc::new(port),
            port_type: PhantomData,
        })
    }

    /// The system's name, as its evidence and diagnostics record it.
    pub fn name(&self) -> &str {
        self.name.as_str()
    }
}

/// The type a port implementation is looked up under. The port attribute
/// implements [`ServesPort`] for it.
#[doc(hidden)]
pub struct PortImpl<T: ?Sized>(PhantomData<fn() -> Arc<T>>);

/// Turns a port implementation `T` into the trait object `P` of the port it
/// serves, generated for each trait marked `#[obzenflow::port]`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this port implements no trait marked `#[obzenflow::port]`",
    label = "the port this system is built from",
    note = "mark the port trait it implements, or the `impl` block of a struct that serves as its own port, with `#[obzenflow::port]`"
)]
pub trait ServesPort<P: ?Sized, T: ?Sized> {
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

impl<P: ?Sized + Send + Sync + 'static> Clone for System<P> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            evidence: self.evidence.clone(),
            port: Arc::clone(&self.port),
            port_type: PhantomData,
        }
    }
}

impl<P: ?Sized + Send + Sync + 'static> std::fmt::Debug for System<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("System")
            .field("name", &self.name.as_str())
            .field("port", &"<not disclosed>")
            .finish()
    }
}

/// Why a system value could not be built.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SystemError {
    #[error("system names must match [A-Za-z0-9][A-Za-z0-9._-]{{0,63}}")]
    InvalidName { system: String },
    #[error("{source}")]
    Evidence {
        system: String,
        source: BindingEvidenceError,
    },
}

impl SystemError {
    /// The name the system was given.
    pub fn system(&self) -> &str {
        match self {
            Self::InvalidName { system } | Self::Evidence { system, .. } => system,
        }
    }
}

mod sealed {
    pub trait StageSystem {}
}

/// A value a `#[system]` field holds: a [`System`], or a typed binding of a
/// hand-written named effect.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a system",
    label = "a `#[system]` field holds a system the handler calls",
    note = "hold a `System` value, or the typed binding of a hand-written named effect; \
            AI effects are performed by `ai!`, whose model comes from [ai.*] configuration"
)]
pub trait StageSystem: sealed::StageSystem {}

impl<P: ?Sized + Send + Sync + 'static> sealed::StageSystem for System<P> {}
impl<P: ?Sized + Send + Sync + 'static> StageSystem for System<P> {}
impl<E: NamedEffect> sealed::StageSystem for EffectBinding<E> {}
impl<E: NamedEffect> StageSystem for EffectBinding<E> {}

/// A handler that declares the systems it calls. Derive it with
/// `#[derive(EffectHandler)]` and mark each field that holds a system
/// `#[system]`; a handler that calls no system derives it with no such field.
/// The handler may perform every effect a field's port declares, and a field
/// written `#[system(RequestA, RequestB)]` allows only the requests it lists.
///
/// ```
/// # use obzenflow_runtime::effects::{EffectHandler, System};
/// # #[obzenflow_runtime::effects::port(effects = obzenflow_runtime::effects)]
/// # trait RiskEnginePort: Send + Sync {}
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct ScoreHandler {
///     #[system]
///     risk_engine: System<dyn RiskEnginePort>,
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
/// A field lists only requests its own port declares:
///
/// ```compile_fail,E0277
/// # use obzenflow_runtime::effects::{EffectError, EffectHandler, System};
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
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct ScoreHandler {
///     #[system(CaseRequest)]
///     risk_engine: System<dyn RiskEnginePort>,
/// }
/// ```
///
/// lists each request once:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{EffectError, EffectHandler, System};
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
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct ScoreHandler {
///     #[system(ScoreRequest, ScoreRequest)]
///     risk_engine: System<dyn RiskEnginePort>,
/// }
/// ```
///
/// and lists at least one, since a bare `#[system]` allows the whole port:
///
/// ```compile_fail
/// # use obzenflow_runtime::effects::{EffectError, EffectHandler, System};
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
/// #[derive(Clone, Debug, EffectHandler)]
/// # #[effect_handler(effects = obzenflow_runtime::effects)]
/// struct ScoreHandler {
///     #[system()]
///     risk_engine: System<dyn RiskEnginePort>,
/// }
/// ```
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an effect handler",
    label = "an effectful stage's handler declares the systems it calls",
    note = "add `#[derive(EffectHandler)]` to `{Self}`, with a `#[system]` field for each system it calls"
)]
pub trait EffectHandler {
    #[doc(hidden)]
    type Systems: SystemList;

    /// The effects this handler may perform, which its `#[system]` fields
    /// allow.
    type AllowedEffects: EffectSet;

    #[doc(hidden)]
    const SYSTEM_FIELDS: &'static [&'static str] = &[];

    #[doc(hidden)]
    fn systems(&self) -> Self::Systems;
}

/// The effects a port declares, in method order. `#[obzenflow::port]`
/// implements it.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a port",
    label = "a `#[system]` field holds a system whose port declares its effects",
    note = "mark the trait or impl that declares its `#[effect]` methods with `#[obzenflow::port]`"
)]
pub trait PortEffects {
    type Members;
}

/// A `#[system]` field, which allows every effect its port declares.
#[doc(hidden)]
pub struct WholeSystem<S>(PhantomData<fn() -> S>);

/// A `#[system(...)]` field, which allows only the requests it lists.
#[doc(hidden)]
pub struct NarrowedSystem<S, Requests>(PhantomData<fn() -> (S, Requests)>);

/// The effects one `#[system]` field allows.
#[doc(hidden)]
pub trait SystemEffects {
    type Members;
}

impl<P: ?Sized + PortEffects + Send + Sync + 'static> SystemEffects for WholeSystem<System<P>> {
    type Members = P::Members;
}

impl<E: NamedEffect> SystemEffects for WholeSystem<EffectBinding<E>> {
    type Members = WithMember<E, EmptySet>;
}

impl<S, Requests: RequestList> SystemEffects for NarrowedSystem<S, Requests> {
    type Members = Requests::Members;
}

/// The effects a handler's `#[system]` fields allow, in field order.
#[doc(hidden)]
pub trait SystemsEffects {
    type Members;
}

impl SystemsEffects for () {
    type Members = EmptySet;
}

impl<Field, Rest> SystemsEffects for (Field, Rest)
where
    Field: SystemEffects,
    Rest: SystemsEffects,
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

/// Proof that a `#[system(...)]` field's port declares a request it lists.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{R}` is not an effect of this `#[system]` field's port",
    label = "this field's port has no `#[effect]` method that takes `{R}`",
    note = "list only requests whose `#[effect]` method is on the field's port"
)]
pub trait SystemServesRequest<R> {}

#[diagnostic::do_not_recommend]
impl<P, R> SystemServesRequest<R> for System<P>
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

/// A system that answers the named effect `E`.
#[doc(hidden)]
pub trait AnswersEffect<E: Effect> {
    #[doc(hidden)]
    fn declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<P, R> AnswersEffect<Call<R>> for System<P>
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
        .allowed_by_system_field())
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

/// Index witness: the system that answers is the head of the list.
#[doc(hidden)]
pub struct SystemFound;

/// Index witness: the system that answers follows the head.
#[doc(hidden)]
pub struct SystemAfter<Index>(PhantomData<fn() -> Index>);

/// Proof that exactly one of the handler's systems answers the effect named
/// `N`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this handler performs `{N}`, which needs a system",
    label = "no `#[system]` field of the handler answers `{N}`",
    note = "add a `#[system]` field whose port the request names"
)]
pub trait FindSystem<N: EffectName, Index> {
    #[doc(hidden)]
    const POSITION: usize;

    #[doc(hidden)]
    fn found_declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<N: EffectName, S: AnswersEffect<N::Effect>, Rest> FindSystem<N, SystemFound> for (S, Rest) {
    const POSITION: usize = 0;

    fn found_declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        self.0.declaration(acknowledged)
    }
}

#[diagnostic::do_not_recommend]
impl<N: EffectName, S, Rest: FindSystem<N, Index>, Index> FindSystem<N, SystemAfter<Index>>
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

/// A system that answers at least one of the handler's effects.
#[doc(hidden)]
pub struct Used<S>(PhantomData<fn() -> S>);

/// A system that has not yet answered any of the handler's effects.
#[doc(hidden)]
pub struct Unused<S>(PhantomData<fn() -> S>);

#[doc(hidden)]
pub trait SystemMark {
    type System;
}

impl<S> SystemMark for Used<S> {
    type System = S;
}

impl<S> SystemMark for Unused<S> {
    type System = S;
}

#[doc(hidden)]
pub trait MarkSystemUsed<Index> {
    type Marked;
}

impl<Index> MarkSystemUsed<Index> for () {
    type Marked = ();
}

impl<M: SystemMark, Rest> MarkSystemUsed<SystemFound> for (M, Rest) {
    type Marked = (Used<M::System>, Rest);
}

impl<M, Rest: MarkSystemUsed<Index>, Index> MarkSystemUsed<SystemAfter<Index>> for (M, Rest) {
    type Marked = (M, Rest::Marked);
}

#[doc(hidden)]
pub trait SystemList {
    type Unmarked;
}

impl SystemList for () {
    type Unmarked = ();
}

impl<S: StageSystem, Rest: SystemList> SystemList for (S, Rest) {
    type Unmarked = (Unused<S>, Rest::Unmarked);
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this field's port declares no `#[effect]` method",
    label = "`{Self}` allows no effect",
    note = "remove the field, or declare an `#[effect]` method on its port"
)]
pub trait SystemAnswersAnEffect {}

impl<S> SystemAnswersAnEffect for Used<S> {}

#[doc(hidden)]
pub trait EverySystemAnswers {}

impl EverySystemAnswers for () {}

impl<M: SystemAnswersAnEffect, Rest: EverySystemAnswers> EverySystemAnswers for (M, Rest) {}

#[doc(hidden)]
pub trait DeclareEffects<Systems, Mask, Index> {
    type Marked;

    #[doc(hidden)]
    fn declarations(
        systems: &Systems,
        fields: &[&'static str],
        acknowledgements: &[EffectAcknowledgement],
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError>;
}

impl<Systems, Mask> DeclareEffects<Systems, Mask, ()> for EmptySet {
    type Marked = Mask;

    fn declarations(
        _: &Systems,
        _: &[&'static str],
        _: &[EffectAcknowledgement],
        _: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError> {
        Ok(())
    }
}

impl<N, Rest, Systems, Mask, Index, RestIndex> DeclareEffects<Systems, Mask, (Index, RestIndex)>
    for WithMember<N, Rest>
where
    N: EffectName,
    Systems: FindSystem<N, Index>,
    Mask: MarkSystemUsed<Index>,
    Rest: DeclareEffects<Systems, Mask::Marked, RestIndex>,
{
    type Marked = <Rest as DeclareEffects<Systems, Mask::Marked, RestIndex>>::Marked;

    fn declarations(
        systems: &Systems,
        fields: &[&'static str],
        acknowledgements: &[EffectAcknowledgement],
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError> {
        let acknowledged = acknowledgements
            .iter()
            .find(|acknowledgement| {
                acknowledgement.effect_type == <N::Effect as Effect>::EFFECT_TYPE
            })
            .map(|acknowledgement| acknowledgement.safety);
        let field = fields.get(Systems::POSITION).copied();
        out.push(
            systems
                .found_declaration(acknowledged)?
                .written_as::<N>()
                .held_in_field(field),
        );
        Rest::declarations(systems, fields, acknowledgements, out)
    }
}

fn declare_stage_effects<Set, Systems, Index>(
    systems: &Systems,
    fields: &[&'static str],
    acknowledgements: &[EffectAcknowledgement],
) -> Result<Vec<EffectDeclaration>, StageEffectsError>
where
    Set: EffectSet,
    Systems: SystemList,
    Set::Members: DeclareEffects<Systems, Systems::Unmarked, Index>,
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
    <Set::Members as DeclareEffects<Systems, Systems::Unmarked, Index>>::declarations(
        systems,
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
        DeclareEffects<H::Systems, <H::Systems as SystemList>::Unmarked, Index>,
    <<H::AllowedEffects as EffectSet>::Members as DeclareEffects<
        H::Systems,
        <H::Systems as SystemList>::Unmarked,
        Index,
    >>::Marked: EverySystemAnswers,
{
    declare_stage_effects::<H::AllowedEffects, H::Systems, Index>(
        &handler.systems(),
        H::SYSTEM_FIELDS,
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
        DeclareEffects<H::Systems, <H::Systems as SystemList>::Unmarked, Index>,
    <<H::AllowedEffects as EffectSet>::Members as DeclareEffects<
        H::Systems,
        <H::Systems as SystemList>::Unmarked,
        Index,
    >>::Marked: EverySystemAnswers,
{
    declare_stage_effects::<H::AllowedEffects, H::Systems, Index>(
        &handler.systems(),
        H::SYSTEM_FIELDS,
        acknowledgements,
    )
}
