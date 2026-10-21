// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::binding::{is_system_name, BindingCoordinate};
use super::call::{call_port_slot, Call, CallRequest};
use super::declaration::EffectDeclarationSyntax;
use super::ports::SystemPort;
use super::{
    Effect, EffectBinding, EffectBindingBuildError, EffectDeclaration, EffectPortResolutionError,
    EffectRegistrationBuilder, EffectSet, LogicalEffectBindingName, Named, NamedEffect, Portless,
};
use crate::stages::common::handlers::{EffectfulStatefulHandler, EffectfulTransformHandler};
use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::{BindingEvidenceError, SystemBindingEvidence};
use std::marker::PhantomData;
use std::sync::Arc;

/// One system that answers requests through its port `P`. A handler holds it
/// in a `#[system]` field and passes it to `fx.call`.
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

    pub(super) fn coordinate(&self) -> BindingCoordinate {
        self.port.coordinate()
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
    note = "mark the port trait it implements with `#[obzenflow::port]`"
)]
pub trait ServesPort<P: ?Sized, T: ?Sized> {
    fn coerce(port: Arc<T>) -> Arc<P>;
}

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
    note = "hold a `System` value, or a typed binding such as `EffectBinding<ChatCompletion>`"
)]
pub trait StageSystem: sealed::StageSystem {}

impl<P: ?Sized + Send + Sync + 'static> sealed::StageSystem for System<P> {}
impl<P: ?Sized + Send + Sync + 'static> StageSystem for System<P> {}
impl<E: NamedEffect> sealed::StageSystem for EffectBinding<E> {}
impl<E: NamedEffect> StageSystem for EffectBinding<E> {}

/// A handler that declares the systems it calls. Derive it with
/// `#[derive(EffectHandler)]` and mark each field that holds a system
/// `#[system]`; a handler that calls no system derives it with no such field.
///
/// ```
/// # use obzenflow_runtime::effects::{EffectHandler, System};
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
///     type AllowedEffects = obzenflow_runtime::effect_set![];
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
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an effect handler",
    label = "an effectful stage's handler declares the systems it calls",
    note = "add `#[derive(EffectHandler)]` to `{Self}`, with a `#[system]` field for each system it calls"
)]
pub trait EffectHandler {
    #[doc(hidden)]
    type Systems: SystemList;

    #[doc(hidden)]
    fn systems(&self) -> Self::Systems;
}

/// How a stage row acknowledges a paid at-least-once or transactional effect.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgedSafety {
    AtLeastOnce,
    Transactional,
}

/// One `.at_least_once::<E>()` or `.transactional::<E>()` decoration.
#[doc(hidden)]
#[derive(Debug, Clone, Copy)]
pub struct EffectAcknowledgement {
    effect_type: &'static str,
    written: &'static str,
    safety: AcknowledgedSafety,
}

impl EffectAcknowledgement {
    pub fn at_least_once<E: Effect>(written: &'static str) -> Self {
        Self {
            effect_type: E::EFFECT_TYPE,
            written,
            safety: AcknowledgedSafety::AtLeastOnce,
        }
    }

    pub fn transactional<E: Effect>(written: &'static str) -> Self {
        Self {
            effect_type: E::EFFECT_TYPE,
            written,
            safety: AcknowledgedSafety::Transactional,
        }
    }

    fn decoration(&self) -> String {
        match self.safety {
            AcknowledgedSafety::AtLeastOnce => format!(".at_least_once::<{}>()", self.written),
            AcknowledgedSafety::Transactional => format!(".transactional::<{}>()", self.written),
        }
    }
}

fn declaration_syntax(acknowledged: Option<AcknowledgedSafety>) -> EffectDeclarationSyntax {
    match acknowledged {
        None => EffectDeclarationSyntax::Bare,
        Some(AcknowledgedSafety::AtLeastOnce) => EffectDeclarationSyntax::AtLeastOnce,
        Some(AcknowledgedSafety::Transactional) => EffectDeclarationSyntax::Transactional,
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
        ))
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

/// Proof that exactly one of the handler's systems answers `E`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this handler performs `{E}`, which needs a system",
    label = "no `#[system]` field of the handler answers `{E}`",
    note = "add a `#[system]` field whose port the request names"
)]
pub trait FindSystem<E: Effect, Index> {
    #[doc(hidden)]
    fn found_declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<E: Effect, S: AnswersEffect<E>, Rest> FindSystem<E, SystemFound> for (S, Rest) {
    fn found_declaration(
        &self,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        self.0.declaration(acknowledged)
    }
}

#[diagnostic::do_not_recommend]
impl<E: Effect, S, Rest: FindSystem<E, Index>, Index> FindSystem<E, SystemAfter<Index>>
    for (S, Rest)
{
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
    message = "a `#[system]` field of this handler answers none of its effects",
    label = "`{Self}` answers nothing this handler performs",
    note = "remove that field, or perform a request whose port it serves"
)]
pub trait SystemAnswersAnEffect {}

impl<S> SystemAnswersAnEffect for Used<S> {}

#[doc(hidden)]
pub trait EverySystemAnswers {}

impl EverySystemAnswers for () {}

impl<M: SystemAnswersAnEffect, Rest: EverySystemAnswers> EverySystemAnswers for (M, Rest) {}

/// One effect's declaration, chosen by its binding mode.
#[doc(hidden)]
pub trait DeclareByMode<E: Effect, Systems, Mask, Index> {
    type Marked;

    #[doc(hidden)]
    fn declaration(
        systems: &Systems,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<E, Systems, Mask> DeclareByMode<E, Systems, Mask, ()> for Portless
where
    E: Effect<BindingMode = Portless>,
{
    type Marked = Mask;

    fn declaration(
        _: &Systems,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        Ok(EffectDeclaration::for_binding::<E>(
            super::binding::EffectDeclarationBinding::Portless,
            declaration_syntax(acknowledged),
        ))
    }
}

impl<E, B, Systems, Mask, Index> DeclareByMode<E, Systems, Mask, Index> for Named<B>
where
    E: Effect,
    B: super::EffectBindingEvidence,
    Systems: FindSystem<E, Index>,
    Mask: MarkSystemUsed<Index>,
{
    type Marked = Mask::Marked;

    fn declaration(
        systems: &Systems,
        acknowledged: Option<AcknowledgedSafety>,
    ) -> Result<EffectDeclaration, StageEffectsError> {
        systems.found_declaration(acknowledged)
    }
}

#[doc(hidden)]
pub trait DeclareEffects<Systems, Mask, Index> {
    type Marked;

    #[doc(hidden)]
    fn declarations(
        systems: &Systems,
        acknowledgements: &[EffectAcknowledgement],
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError>;
}

impl<Systems, Mask> DeclareEffects<Systems, Mask, ()> for EmptySet {
    type Marked = Mask;

    fn declarations(
        _: &Systems,
        _: &[EffectAcknowledgement],
        _: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError> {
        Ok(())
    }
}

impl<E, Rest, Systems, Mask, Index, RestIndex> DeclareEffects<Systems, Mask, (Index, RestIndex)>
    for WithMember<E, Rest>
where
    E: Effect,
    E::BindingMode: DeclareByMode<E, Systems, Mask, Index>,
    Rest: DeclareEffects<
        Systems,
        <E::BindingMode as DeclareByMode<E, Systems, Mask, Index>>::Marked,
        RestIndex,
    >,
{
    type Marked = <Rest as DeclareEffects<
        Systems,
        <E::BindingMode as DeclareByMode<E, Systems, Mask, Index>>::Marked,
        RestIndex,
    >>::Marked;

    fn declarations(
        systems: &Systems,
        acknowledgements: &[EffectAcknowledgement],
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError> {
        let acknowledged = acknowledgements
            .iter()
            .find(|acknowledgement| acknowledgement.effect_type == E::EFFECT_TYPE)
            .map(|acknowledgement| acknowledgement.safety);
        out.push(<E::BindingMode as DeclareByMode<
            E,
            Systems,
            Mask,
            Index,
        >>::declaration(systems, acknowledged)?);
        Rest::declarations(systems, acknowledgements, out)
    }
}

fn declare_stage_effects<Set, Systems, Index>(
    systems: &Systems,
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
        acknowledgements,
    )
}
