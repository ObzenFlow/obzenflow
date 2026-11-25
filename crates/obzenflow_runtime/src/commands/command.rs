// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::request::CallRequest;
use crate::effects::{
    call_port_slot, Call, Effect, EffectBinding, EffectBindingBuildError, EffectDeclaration,
    EffectError, EffectName, EffectRegistrationBuilder, EffectSet, NamedEffect, RequestList,
};
use crate::stages::common::handlers::{EffectfulStatefulHandler, EffectfulTransformHandler};
use crate::systems::Port;
use obzenflow_core::event::schema::{EffectOutcome, EmptySet, WithMember};
use std::marker::PhantomData;

/// The answer a port method gives, its request's outcome or an `EffectError`.
/// `#[obzenflow::port]` requires it of every method on a port trait.
///
/// ```
/// # use obzenflow_core::TypedPayload;
/// # use obzenflow_runtime::effects::EffectError;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Charged;
/// # impl TypedPayload for Charged { const EVENT_TYPE: &'static str = "docs.port.charged"; }
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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
/// #[obzenflow_runtime::effects::port]
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

impl<O: EffectOutcome> PortAnswer for Result<O, EffectError> {
    type Outcome = O;
}

/// The answer of a port method marked `records = reply`.
#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "a port method marked `records = reply` answers with a reply and fails with `EffectError`",
    label = "`{Self}` is not `Result<Reply, EffectError>`",
    note = "a reply is a plain serializable value the runtime records and returns, such as `()`"
)]
pub trait PortReply {
    type Reply;
}

impl<O> PortReply for Result<O, EffectError>
where
    O: Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
{
    type Reply = O;
}

#[doc(hidden)]
pub fn assert_port_answer<T: PortAnswer>() {}

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

/// A command: an effectful handler that declares the ports it works through.
/// `#[obzenflow::command(through = P)]` on a unit struct implements it, as does
/// `#[obzenflow::command]` on a struct that marks each field holding a port
/// `#[port]`; a command that works through no port has no such field. The
/// command may perform every effect a port declares, and a field written
/// `#[port(RequestA, RequestB)]` allows only the requests it lists.
///
/// ```
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {}
/// # #[obzenflow_runtime::effects::port]
/// # trait CaseSystemPort: Send + Sync {}
/// # struct Vendor;
/// # impl RiskEnginePort for Vendor {}
/// # impl CaseSystemPort for Vendor {}
/// # use obzenflow_runtime::systems::Port;
/// #[obzenflow_runtime::effects::command(through = RiskEnginePort)]
/// #[derive(Clone, Debug)]
/// struct Scorer;
///
/// #[obzenflow_runtime::effects::command]
/// #[derive(Clone, Debug)]
/// struct Triage {
///     #[port]
///     risk_engine: Port<dyn RiskEnginePort>,
///     #[port]
///     case_system: Port<dyn CaseSystemPort>,
/// }
///
/// let scorer = Scorer::through(Port::to(Vendor));
/// let triage = Triage {
///     risk_engine: Port::to(std::sync::Arc::new(Vendor)),
///     case_system: Port::to(Vendor),
/// };
/// ```
///
/// A command with state of its own names its port as a field:
///
/// ```
/// # use obzenflow_runtime::systems::Port;
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {}
/// #[obzenflow_runtime::effects::command]
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
/// #[obzenflow_runtime::effects::command(through = RiskEnginePort)]
/// #[derive(Clone, Debug)]
/// struct Scorer;
/// ```
///
/// Nor does a `through` struct with a field of its own:
///
/// ```compile_fail
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {}
/// #[obzenflow_runtime::effects::command(through = RiskEnginePort)]
/// #[derive(Clone, Debug)]
/// struct Scorer {
///     limit: u32,
/// }
/// ```
///
/// Nor two ports of one type:
///
/// ```compile_fail
/// # use obzenflow_runtime::systems::Port;
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {}
/// #[obzenflow_runtime::effects::command]
/// #[derive(Clone, Debug)]
/// struct Scorer {
///     #[port]
///     first: Port<dyn RiskEnginePort>,
///     #[port]
///     second: Port<dyn RiskEnginePort>,
/// }
/// ```
///
/// Nor a `#[port]` field that holds no port:
///
/// ```compile_fail,E0277
/// #[obzenflow_runtime::effects::command]
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
/// # use obzenflow_runtime::effects::EffectError;
/// # use obzenflow_runtime::systems::Port;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.handler.scored"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest;
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct CaseRequest;
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {
/// #     #[effect(type = "docs.handler.score", exactly_once)]
/// #     fn score(&self, request: &ScoreRequest) -> Result<Scored, EffectError>;
/// # }
/// # #[obzenflow_runtime::effects::port]
/// # trait CasePort: Send + Sync {
/// #     #[effect(type = "docs.handler.case", exactly_once)]
/// #     fn open(&self, request: &CaseRequest) -> Result<Scored, EffectError>;
/// # }
/// #[obzenflow_runtime::effects::command]
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
/// # use obzenflow_runtime::effects::EffectError;
/// # use obzenflow_runtime::systems::Port;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.handler.scored"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest;
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {
/// #     #[effect(type = "docs.handler.score", exactly_once)]
/// #     fn score(&self, request: &ScoreRequest) -> Result<Scored, EffectError>;
/// # }
/// #[obzenflow_runtime::effects::command]
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
/// # use obzenflow_runtime::effects::EffectError;
/// # use obzenflow_runtime::systems::Port;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Scored;
/// # impl obzenflow_core::TypedPayload for Scored { const EVENT_TYPE: &'static str = "docs.handler.scored"; }
/// # #[derive(Clone, Debug, serde::Serialize)]
/// # struct ScoreRequest;
/// # #[obzenflow_runtime::effects::port]
/// # trait RiskEnginePort: Send + Sync {
/// #     #[effect(type = "docs.handler.score", exactly_once)]
/// #     fn score(&self, request: &ScoreRequest) -> Result<Scored, EffectError>;
/// # }
/// #[obzenflow_runtime::effects::command]
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

/// A failure to register the system behind a command's port.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageEffectsError(EffectBindingBuildError);

impl StageEffectsError {
    pub fn for_stage(&self, stage: &str) -> String {
        format!(
            "Effectful stage '{stage}' cannot register its system: {}",
            self.0
        )
    }
}

impl From<EffectBindingBuildError> for StageEffectsError {
    fn from(source: EffectBindingBuildError) -> Self {
        Self(source)
    }
}

/// A port whose system answers the named effect `E`.
#[doc(hidden)]
pub trait AnswersEffect<E: Effect> {
    #[doc(hidden)]
    fn declaration(&self) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<P, R> AnswersEffect<Call<R>> for Port<P>
where
    P: ?Sized + PortEffects + Send + Sync + 'static,
    R: CallRequest<Port = P>,
{
    fn declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        crate::effects::binding::validate_effect_type(R::EFFECT_TYPE)
            .map_err(|_| EffectBindingBuildError::InvalidEffectType)?;
        let binding = EffectRegistrationBuilder::<Call<R>>::new(
            self.binding_name(),
            crate::effects::SystemPortEvidence,
        )
        .bind_system(call_port_slot::<R>(), &self.port)?
        .finish()?;
        Ok(EffectDeclaration::for_binding::<Call<R>>(
            binding.declaration_binding(),
        ))
    }
}

impl<E: NamedEffect> AnswersEffect<E> for EffectBinding<E> {
    fn declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        Ok(EffectDeclaration::for_binding::<E>(
            self.declaration_binding(),
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
    fn found_declaration(&self) -> Result<EffectDeclaration, StageEffectsError>;
}

impl<N: EffectName, S: AnswersEffect<N::Effect>, Rest> FindPort<N, PortFound> for (S, Rest) {
    fn found_declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        self.0.declaration()
    }
}

#[diagnostic::do_not_recommend]
impl<N: EffectName, S, Rest: FindPort<N, Index>, Index> FindPort<N, PortAfter<Index>>
    for (S, Rest)
{
    fn found_declaration(&self) -> Result<EffectDeclaration, StageEffectsError> {
        self.1.found_declaration()
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
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError>;
}

impl<Ports, Mask> DeclareEffects<Ports, Mask, ()> for EmptySet {
    type Marked = Mask;

    fn declarations(_: &Ports, _: &mut Vec<EffectDeclaration>) -> Result<(), StageEffectsError> {
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
        out: &mut Vec<EffectDeclaration>,
    ) -> Result<(), StageEffectsError> {
        out.push(ports.found_declaration()?);
        Rest::declarations(ports, out)
    }
}

fn declare_stage_effects<Set, Ports, Index>(
    ports: &Ports,
) -> Result<Vec<EffectDeclaration>, StageEffectsError>
where
    Set: EffectSet,
    Ports: PortList,
    Set::Members: DeclareEffects<Ports, Ports::Unmarked, Index>,
{
    let mut declarations = Vec::new();
    <Set::Members as DeclareEffects<Ports, Ports::Unmarked, Index>>::declarations(
        ports,
        &mut declarations,
    )?;
    Ok(declarations)
}

#[doc(hidden)]
pub fn effectful_transform_effects<H, Index>(
    handler: &H,
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
    declare_stage_effects::<H::AllowedEffects, H::Ports, Index>(&handler.ports())
}

#[doc(hidden)]
pub fn effectful_stateful_effects<H, Index>(
    handler: &H,
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
    declare_stage_effects::<H::AllowedEffects, H::Ports, Index>(&handler.ports())
}
