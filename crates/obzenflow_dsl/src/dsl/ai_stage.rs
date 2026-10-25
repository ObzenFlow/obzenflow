// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Handler shapes for `ai!` stages, the model binding each resolves at
//! lowering, and the members each shape lowers to.

use crate::dsl::composition::{
    CompositeBuildContext, CompositeBuildError, CompositeDescriptor, FlowMember, IntoFlowMember,
};
use crate::dsl::stage_descriptor::{EffectfulTransformDescriptor, StatefulDescriptor};
use crate::dsl::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use crate::dsl::FlowBuildError;
use obzenflow_adapters::ai::{
    AiKeySpec, AiTrigger, Chunked, Infer, Map, MapReduce, TriggeredMapReduce,
};
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_core::ai::{
    AiMapReduceChunkFailed, AiMapReduceGroupingEnded, AiMapReduceMapInput, AiMapReduceReduceFailed,
    AiMapReduceReduceInput, AiMapReduceTaggedPartial, AiMapReduceWindowFailed, AiModelSpec, Chunk,
    Partials,
};
use obzenflow_core::event::schema::StageOutputFacts;
use obzenflow_core::journal::archive::manifest::AI_MODEL_SPECS_CAPABILITY;
use obzenflow_core::{StageFactSet, TypedPayload};
use obzenflow_runtime::ai::{
    AiBinding, AiCollector, AiEffect, AiGrouping, AiInferHandler, AiKeying, AiMapHandler,
    AiModelSource, AiPartialMapHandler, AiProviderPort, AiReduceHandler, AiRole,
};
use obzenflow_runtime::bootstrap::{replay_archive, replay_bootstrap, ReplayVerb};
use obzenflow_runtime::effects::{Effect, EffectDeclaration};
use obzenflow_runtime::run_context::FlowBuildContext;
use obzenflow_runtime::runtime_config::DEFAULT_MAX_OPEN_KEYS;
use serde::{de::DeserializeOwned, Serialize};
use std::any::Any;
use std::collections::HashMap;
use std::num::NonZeroU64;
use std::sync::Arc;

const AI_SURFACE: &str = "ai!";
const DIRECT_FACT_BOUND: NonZeroU64 = NonZeroU64::MIN.saturating_add(2);

/// A handler value an `ai!` row accepts, built with `ai::infer(role)`,
/// `ai::map(role).chunks(n)`, or a map-reduce with its trigger.
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not an `ai!` handler",
    label = "an `ai!` row takes a handler built with `ai::infer` or `ai::map`",
    note = "build the handler above `flow!`, as in `let digest = ai::map(role).chunks(10).reduce(role).on_eof();`"
)]
pub trait AiStageHandler: Send + 'static {
    /// The stage's input fact.
    type In: TypedPayload + Clone + Send + Sync + 'static;
    /// What the stage authors.
    type Out: StageFactSet;
    /// What one call takes, for diagnostics.
    type CallIn;
    /// What one call produces, for diagnostics.
    type CallOut;

    #[doc(hidden)]
    fn into_plan(self) -> Box<dyn AiStagePlan>;
}

#[doc(hidden)]
pub trait AiHandlerRole {
    type Role;
}

impl<R, In> AiHandlerRole for Infer<R, In> {
    type Role = R;
}

impl<R, T, K> AiHandlerRole for Map<R, T, K> {
    type Role = R;
}

impl<R, T, K> AiHandlerRole for Chunked<R, T, K> {
    type Role = R;
}

impl<R, S, T, K> AiHandlerRole for MapReduce<R, S, T, K> {
    type Role = R;
}

impl<R, S, T, K> AiHandlerRole for TriggeredMapReduce<R, S, T, K> {
    type Role = R;
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`ai::map({Role})` needs a group size",
    label = "this handler has no group size",
    note = "call `.chunks(n)` to cap each group at n items"
)]
pub trait AiGroupSizeChosen<Role> {}

impl<R, In> AiGroupSizeChosen<R> for Infer<R, In> {}
impl<R, T, K> AiGroupSizeChosen<R> for Chunked<R, T, K> {}
impl<R, S, T, K> AiGroupSizeChosen<R> for MapReduce<R, S, T, K> {}
impl<R, S, T, K> AiGroupSizeChosen<R> for TriggeredMapReduce<R, S, T, K> {}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` needs a reduce trigger",
    label = "this map-reduce never reduces",
    note = "call `.on_eof()` to reduce once when input ends, or `.every_n_chunks(k)` to reduce once per k chunks"
)]
pub trait AiReduceTriggerChosen {}

impl<R, In> AiReduceTriggerChosen for Infer<R, In> {}
impl<R, T, K> AiReduceTriggerChosen for Map<R, T, K> {}
impl<R, T, K> AiReduceTriggerChosen for Chunked<R, T, K> {}
impl<R, S, T, K> AiReduceTriggerChosen for TriggeredMapReduce<R, S, T, K> {}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this `ai!` row declares `{In} -> {Out}`, but its handler is `{CallIn} -> {CallOut}` per call",
    label = "the arrow and the handler disagree",
    note = "an `ai::map` handler without `.reduce` produces its map role's output; add `.reduce(role)` with a trigger, or declare the handler's output after `->`"
)]
pub trait AiArrowMatches<In, Out, CallIn, CallOut> {}

impl<H: AiStageHandler> AiArrowMatches<H::In, H::Out, H::CallIn, H::CallOut> for H {}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "this `ai!` row's input `{In}` is not its handler's input",
    label = "the handler takes another input",
    note = "declare the handler's input before `->`"
)]
pub trait AiInputMatches<In> {}

impl<H: AiStageHandler> AiInputMatches<H::In> for H {}

#[doc(hidden)]
pub fn assert_ai_group_size<H>(_: &H)
where
    H: AiHandlerRole + AiGroupSizeChosen<<H as AiHandlerRole>::Role>,
{
}

#[doc(hidden)]
pub fn assert_ai_reduce_trigger<H: AiReduceTriggerChosen>(_: &H) {}

#[doc(hidden)]
pub fn assert_ai_arrow<In, Out, H>(_: &H)
where
    H: AiStageHandler + AiArrowMatches<In, Out, H::CallIn, H::CallOut>,
{
}

#[doc(hidden)]
pub fn assert_ai_input<In, H>(_: &H)
where
    H: AiStageHandler + AiInputMatches<In>,
{
}

#[doc(hidden)]
pub trait AiStagePlan: Send + Sync {
    fn lower(self: Box<Self>, stage: AiStageLowering<'_>) -> LoweringResult<FlowMember>;
}

#[doc(hidden)]
pub struct AiStageLowering<'a> {
    pub name: &'a str,
    pub binding: &'a str,
    pub build: &'a FlowBuildContext,
    pub middleware: Vec<Box<dyn MiddlewareFactory>>,
    pub typing: StageTypingMetadata,
}

const AI_MAP_KIND: &str = "ai_map";
const AI_MAP_REDUCE_KIND: &str = "ai_map_reduce";

pub(crate) fn is_ai_composite_kind(kind: &str) -> bool {
    kind == AI_MAP_KIND || kind == AI_MAP_REDUCE_KIND
}

/// One authored `ai!` row before its model is bound.
pub struct AiStageDescriptor {
    name: String,
    plan: Box<dyn AiStagePlan>,
    middleware: Vec<Box<dyn MiddlewareFactory>>,
    typing: StageTypingMetadata,
}

impl AiStageDescriptor {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }
}

impl IntoFlowMember for AiStageDescriptor {
    fn into_flow_member(self) -> FlowMember {
        FlowMember::Ai(Box::new(self))
    }
}

#[doc(hidden)]
pub fn ai_stage<H: AiStageHandler>(
    name: impl Into<String>,
    handler: H,
    middleware: Vec<Box<dyn MiddlewareFactory>>,
    typing: StageTypingMetadata,
) -> AiStageDescriptor {
    AiStageDescriptor {
        name: name.into(),
        plan: handler.into_plan(),
        middleware,
        typing,
    }
}

#[doc(hidden)]
pub fn lower_ai_members(
    members: HashMap<String, FlowMember>,
    build: &FlowBuildContext,
) -> LoweringResult<HashMap<String, FlowMember>> {
    members
        .into_iter()
        .map(|(binding, member)| match member {
            FlowMember::Ai(descriptor) => {
                let AiStageDescriptor {
                    name,
                    plan,
                    middleware,
                    typing,
                } = *descriptor;
                let lowered = plan.lower(AiStageLowering {
                    name: &name,
                    binding: &binding,
                    build,
                    middleware,
                    typing,
                })?;
                Ok((binding, lowered))
            }
            member => Ok((binding, member)),
        })
        .collect()
}

type LoweringResult<T> = Result<T, Box<FlowBuildError>>;

fn ai_error(message: impl Into<String>) -> Box<FlowBuildError> {
    Box::new(FlowBuildError::AiStage(message.into()))
}

fn strict_replay() -> bool {
    replay_bootstrap().is_some_and(|replay| replay.verb == ReplayVerb::Replay)
}

fn resuming() -> bool {
    replay_bootstrap().is_some_and(|replay| replay.verb == ReplayVerb::Resume)
}

#[derive(Clone)]
struct AiStageBindings<E: AiEffect> {
    current: AiBinding<E>,
    recorded: Vec<AiBinding<E>>,
}

impl<E: AiEffect> AiStageBindings<E> {
    fn declaration(&self) -> EffectDeclaration {
        EffectDeclaration::configured_with_recorded(
            &self.current.binding,
            self.recorded.iter().map(|recorded| &recorded.binding),
        )
    }

    fn specs(&self) -> Vec<AiModelSpec> {
        std::iter::once(&self.current)
            .chain(&self.recorded)
            .map(|binding| binding.spec.clone())
            .collect()
    }
}

fn archived_specs(members: &[String], effect_type: &str) -> Vec<AiModelSpec> {
    let Some(archive) = replay_archive() else {
        return Vec::new();
    };
    let mut specs: Vec<AiModelSpec> = Vec::new();
    for archive in std::iter::once(archive.clone()).chain(archive.ancestors()) {
        for spec in members
            .iter()
            .flat_map(|member| archive.recorded_ai_models(member))
        {
            if spec.effect_type == effect_type && !specs.contains(&spec) {
                specs.push(spec);
            }
        }
    }
    specs
}

fn recorded_binding_name(stage: &str, index: usize) -> String {
    format!("{stage}.recorded.{}", index + 1)
}

fn bind<E: AiEffect>(
    stage: &str,
    source: AiModelSource<E::Provider>,
) -> LoweringResult<AiBinding<E>> {
    E::bind(source).map_err(|error| ai_error(format!("stage `{stage}`: {error}")))
}

fn resolve_bindings<E: AiEffect>(
    stage: &str,
    recorded_members: &[String],
    build: &FlowBuildContext,
) -> LoweringResult<AiStageBindings<E>> {
    let provider = build.ai_models().provider::<E::Provider>().ok_or_else(|| {
        ai_error(format!(
            "stage `{stage}`: this build has no {} model provider; enable the `ai` feature or install one with FlowBuildContext::with_ai_models",
            E::FAMILY
        ))
    })?;
    if strict_replay() {
        return replay_bindings(stage, recorded_members, provider);
    }
    let resolved = build
        .runtime_config()
        .resolve_ai_model(E::SECTION, stage, E::EFFECT_TYPE)
        .map_err(|error| ai_error(error.to_string()))?;
    if !provider.serves(&resolved.endpoint.provider) {
        return Err(ai_error(format!(
            "config: stage `{stage}` names endpoint `{}`, whose provider `{}` cannot serve {}",
            resolved.endpoint.name,
            resolved.endpoint.provider,
            E::EFFECT_TYPE
        )));
    }
    let current = bind::<E>(
        stage,
        AiModelSource::Configured {
            stage: stage.to_string(),
            endpoint: resolved.endpoint,
            model: resolved.model,
            provider: provider.clone(),
        },
    )?;
    let recorded = if resuming() {
        kept_bindings(stage, recorded_members, &current.spec, provider, build)?
    } else {
        Vec::new()
    };
    Ok(AiStageBindings { current, recorded })
}

fn replay_bindings<E: AiEffect>(
    stage: &str,
    recorded_members: &[String],
    provider: Arc<E::Provider>,
) -> LoweringResult<AiStageBindings<E>> {
    let archive = replay_archive().ok_or_else(|| {
        ai_error(format!(
            "replay: stage `{stage}` needs its recorded model, but no archive is open"
        ))
    })?;
    if archive.manifest_capability(AI_MODEL_SPECS_CAPABILITY) != Some(1) {
        return Err(ai_error(format!(
            "replay: archive lacks run-manifest capability '{AI_MODEL_SPECS_CAPABILITY}: 1'; it was recorded before AI stages recorded their models and cannot be strictly replayed"
        )));
    }
    let mut specs = archived_specs(recorded_members, E::EFFECT_TYPE).into_iter();
    let current = specs.next().ok_or_else(|| {
        ai_error(format!(
            "replay: the archive records no model for stage `{stage}` and {}",
            E::EFFECT_TYPE
        ))
    })?;
    let current = bind::<E>(
        stage,
        AiModelSource::Recorded {
            stage: stage.to_string(),
            spec: current,
            provider: provider.clone(),
        },
    )?;
    let recorded = specs
        .enumerate()
        .map(|(index, spec)| {
            bind::<E>(
                stage,
                AiModelSource::Recorded {
                    stage: recorded_binding_name(stage, index),
                    spec,
                    provider: provider.clone(),
                },
            )
        })
        .collect::<Result<_, _>>()?;
    Ok(AiStageBindings { current, recorded })
}

fn kept_bindings<E: AiEffect>(
    stage: &str,
    recorded_members: &[String],
    configured: &AiModelSpec,
    provider: Arc<E::Provider>,
    build: &FlowBuildContext,
) -> LoweringResult<Vec<AiBinding<E>>> {
    let kept = archived_specs(recorded_members, E::EFFECT_TYPE)
        .into_iter()
        .filter(|spec| spec != configured)
        .collect::<Vec<_>>();
    if let Some(recorded) = kept.first() {
        let generation = replay_archive()
            .map(|archive| archive.max_recorded_generation().0 + 1)
            .unwrap_or_default();
        tracing::info!(
            "resume: stage `{stage}` changes model at generation {generation}, from {}/{} (recorded) to {}/{} (configured); recorded and in-flight calls keep {}",
            recorded.endpoint,
            recorded.model,
            configured.endpoint,
            configured.model,
            recorded.model
        );
    }
    kept.into_iter()
        .enumerate()
        .map(|(index, spec)| {
            let endpoint = build
                .runtime_config()
                .ai_endpoints()
                .resolve(stage, &spec.endpoint)
                .ok()
                .filter(|endpoint| provider.serves(&endpoint.provider));
            bind::<E>(
                stage,
                AiModelSource::Kept {
                    stage: recorded_binding_name(stage, index),
                    spec,
                    endpoint,
                    provider: provider.clone(),
                },
            )
        })
        .collect()
}

fn resolve_max_open_keys(
    stage: &str,
    grouping_member: &str,
    build: &FlowBuildContext,
) -> LoweringResult<NonZeroU64> {
    let limit = if strict_replay() {
        replay_archive()
            .and_then(|archive| archive.recorded_max_open_keys(grouping_member))
            .ok_or_else(|| {
                ai_error(format!(
                    "replay: the archive records no open-key limit for keyed stage `{stage}`"
                ))
            })?
    } else {
        build
            .runtime_config()
            .resolve_max_open_keys(stage)
            .map_err(FlowBuildError::ConfigResolution)?
    };
    NonZeroU64::new(limit)
        .ok_or_else(|| ai_error(format!("stage `{stage}`: max_open_keys must be 1 or more")))
}

fn same_bindings<From, To>(bindings: &AiStageBindings<From>) -> Option<AiStageBindings<To>>
where
    From: AiEffect,
    To: AiEffect,
{
    (bindings as &dyn Any)
        .downcast_ref::<AiStageBindings<To>>()
        .cloned()
}

fn row_middleware(
    stage: &str,
    effects: &[&'static str],
    middleware: Vec<Box<dyn MiddlewareFactory>>,
) -> LoweringResult<Vec<Box<dyn MiddlewareFactory>>> {
    for factory in &middleware {
        match factory.effect_scope() {
            Some(effect) if !effects.contains(&effect) => {
                return Err(ai_error(format!(
                    "stage `{stage}` attaches policy middleware to undeclared effect '{effect}'; `.for_effect::<E>()` must name an effect its roles perform"
                )));
            }
            None if effects.len() > 1 && !factory.declaration().is_observer() => {
                return Err(ai_error(format!(
                    "stage `{stage}`: control '{}' requires exactly one declared effect; build it with `.for_effect::<E>()` for one of [{}]",
                    factory.label(),
                    effects.join(", ")
                )));
            }
            _ => {}
        }
    }
    Ok(middleware)
}

fn shared_middleware(
    middleware: Vec<Box<dyn MiddlewareFactory>>,
) -> Vec<Arc<dyn MiddlewareFactory>> {
    middleware.into_iter().map(Arc::from).collect()
}

fn member_middleware(
    shared: &[Arc<dyn MiddlewareFactory>],
    effect: &'static str,
) -> Vec<Box<dyn MiddlewareFactory>> {
    shared
        .iter()
        .filter(|factory| factory.effect_scope().is_none_or(|scope| scope == effect))
        .map(|factory| Box::new(Arc::clone(factory)) as Box<dyn MiddlewareFactory>)
        .collect()
}

fn fact_set_hints<S: StageFactSet>() -> Vec<TypeHint> {
    S::member_fact_types()
        .into_iter()
        .map(TypeHint::exact_fact_type)
        .collect()
}

fn output_metadata<S: StageFactSet>(input: TypeHint, extra: Vec<TypeHint>) -> StageTypingMetadata {
    let mut members = fact_set_hints::<S>();
    let primary = if members.is_empty() {
        TypeHint::Unspecified
    } else {
        members.remove(0)
    };
    members.extend(extra);
    StageTypingMetadata::transform(input, primary, false, None)
        .with_additional_output_contract(members)
}

fn chunk_size(stage: &str, chunk_size: usize) -> LoweringResult<NonZeroU64> {
    NonZeroU64::new(chunk_size as u64).ok_or_else(|| {
        ai_error(format!(
            "stage `{stage}`: `.chunks(0)` can never hold an item; use 1 or more"
        ))
    })
}

impl<R, In> AiStageHandler for Infer<R, In>
where
    In: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<In>,
    R::Out: StageOutputFacts,
{
    type In = In;
    type Out = R::Out;
    type CallIn = In;
    type CallOut = R::Out;

    fn into_plan(self) -> Box<dyn AiStagePlan> {
        Box::new(InferPlan::<R, In> {
            role: self.into_role(),
            _input: std::marker::PhantomData,
        })
    }
}

struct InferPlan<R, In> {
    role: R,
    _input: std::marker::PhantomData<fn() -> In>,
}

impl<R, In> AiStagePlan for InferPlan<R, In>
where
    In: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<In>,
    R::Out: StageOutputFacts,
{
    fn lower(self: Box<Self>, stage: AiStageLowering<'_>) -> LoweringResult<FlowMember> {
        let middleware = row_middleware(stage.name, &[R::Effect::EFFECT_TYPE], stage.middleware)?;
        let bindings =
            resolve_bindings::<R::Effect>(stage.name, &[stage.name.to_string()], stage.build)?;
        let descriptor = EffectfulTransformDescriptor::generated_for_surface::<In>(
            AI_SURFACE,
            "stage",
            stage.name,
            AiInferHandler::<R, In>::new(Arc::new(self.role)),
            vec![bindings.declaration()],
            Vec::new(),
            DIRECT_FACT_BOUND,
        )
        .with_implementation_middleware(middleware)
        .with_ai_models::<R::Effect>(bindings.specs());
        Ok(FlowMember::Stage(wrap_typed_descriptor(
            Box::new(descriptor),
            stage.typing,
        )))
    }
}

impl<R, T, K> AiStageHandler for Chunked<R, T, K>
where
    T: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: StageOutputFacts,
    K: AiKeySpec<T>,
{
    type In = T;
    type Out = R::Out;
    type CallIn = Chunk<T>;
    type CallOut = R::Out;

    fn into_plan(self) -> Box<dyn AiStagePlan> {
        let (role, key, chunk_size) = self.into_parts();
        Box::new(MapPlan::<R, T, K> {
            role,
            key,
            chunk_size,
            _items: std::marker::PhantomData,
        })
    }
}

struct MapPlan<R, T, K> {
    role: R,
    key: K,
    chunk_size: usize,
    _items: std::marker::PhantomData<fn() -> T>,
}

fn keying<T, K: AiKeySpec<T>>(
    stage: &str,
    key: K,
    build: &FlowBuildContext,
) -> LoweringResult<AiKeying<T, K::Key>> {
    match key.key_fn() {
        None => Ok(AiKeying::Unkeyed),
        Some(key) => Ok(AiKeying::Keyed {
            key,
            max_open_keys: resolve_max_open_keys(stage, &format!("{stage}__chunk"), build)?,
        }),
    }
}

impl<R, T, K> AiStagePlan for MapPlan<R, T, K>
where
    T: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: StageOutputFacts,
    K: AiKeySpec<T>,
{
    fn lower(self: Box<Self>, stage: AiStageLowering<'_>) -> LoweringResult<FlowMember> {
        let name = stage.binding;
        let chunk_size = chunk_size(name, self.chunk_size)?;
        let middleware = row_middleware(name, &[R::Effect::EFFECT_TYPE], stage.middleware)?;
        let keying = keying(name, self.key, stage.build)?;
        let max_open_keys = keying.max_open_keys();
        let bindings = resolve_bindings::<R::Effect>(name, &[format!("{name}__map")], stage.build)?;
        Ok(FlowMember::Composite(Box::new(AiMapComposite::<
            R,
            T,
            K::Key,
        > {
            name: name.to_string(),
            role: Arc::new(self.role),
            grouping: AiGrouping::new(name, chunk_size, None, keying),
            max_open_keys,
            bindings,
            middleware,
        })))
    }
}

struct AiMapComposite<R: AiRole<Chunk<T>>, T, Key> {
    name: String,
    role: Arc<R>,
    grouping: AiGrouping<T, Key>,
    max_open_keys: Option<u64>,
    bindings: AiStageBindings<R::Effect>,
    middleware: Vec<Box<dyn MiddlewareFactory>>,
}

impl<R, T, Key> CompositeDescriptor for AiMapComposite<R, T, Key>
where
    T: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: StageOutputFacts,
    Key: Ord + Clone + Serialize + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn kind(&self) -> &'static str {
        AI_MAP_KIND
    }

    fn schema_version(&self) -> u32 {
        1
    }

    fn expand(self: Box<Self>, ctx: &mut CompositeBuildContext) -> Result<(), CompositeBuildError> {
        let chunk = StatefulDescriptor {
            name: "chunk".to_string(),
            handler: self.grouping,
            emit_interval: None,
            observers: Vec::new(),
            max_open_keys: self.max_open_keys,
        };
        let chunk = wrap_typed_descriptor(
            Box::new(chunk),
            StageTypingMetadata::stateful(
                TypeHint::exact_payload::<T>(),
                TypeHint::exact_payload::<AiMapReduceMapInput<T>>(),
                false,
                None,
            ),
        );
        let map = EffectfulTransformDescriptor::generated_for_surface::<AiMapReduceMapInput<T>>(
            AI_SURFACE,
            "stage",
            "map",
            AiMapHandler::<R, T>::new(self.role),
            vec![self.bindings.declaration()],
            Vec::new(),
            DIRECT_FACT_BOUND,
        )
        .with_implementation_middleware(self.middleware)
        .with_ai_models::<R::Effect>(self.bindings.specs())
        .with_policy_owner(&self.name);
        let map = wrap_typed_descriptor(
            Box::new(map),
            output_metadata::<R::Out>(
                TypeHint::exact_payload::<AiMapReduceMapInput<T>>(),
                Vec::new(),
            ),
        );

        ctx.member("chunk").descriptor(chunk);
        ctx.member("map").descriptor(map);
        ctx.feed("chunk", "map")
            .lane("data")
            .payload::<AiMapReduceMapInput<T>>();
        ctx.boundary()
            .input("in", "chunk")
            .payload::<T>()
            .default()
            .output("out", "map")
            .payload_hints(fact_set_hints::<R::Out>())
            .default();
        Ok(())
    }
}

impl<R, S, T, K> AiStageHandler for TriggeredMapReduce<R, S, T, K>
where
    T: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: AiRole<Partials<T, R::Out>>,
    S::Out: StageOutputFacts,
    K: AiKeySpec<T>,
{
    type In = T;
    type Out = S::Out;
    type CallIn = T;
    type CallOut = S::Out;

    fn into_plan(self) -> Box<dyn AiStagePlan> {
        let (map, reduce, key, chunk_size, trigger) = self.into_parts();
        Box::new(MapReducePlan::<R, S, T, K> {
            map,
            reduce,
            key,
            chunk_size,
            trigger,
            _items: std::marker::PhantomData,
        })
    }
}

struct MapReducePlan<R, S, T, K> {
    map: R,
    reduce: S,
    key: K,
    chunk_size: usize,
    trigger: AiTrigger,
    _items: std::marker::PhantomData<fn() -> T>,
}

impl<R, S, T, K> AiStagePlan for MapReducePlan<R, S, T, K>
where
    T: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: AiRole<Partials<T, R::Out>>,
    S::Out: StageOutputFacts,
    K: AiKeySpec<T>,
{
    fn lower(self: Box<Self>, stage: AiStageLowering<'_>) -> LoweringResult<FlowMember> {
        let name = stage.binding;
        let chunk_size = chunk_size(name, self.chunk_size)?;
        let trigger = self.trigger.resolve().ok_or_else(|| {
            ai_error(format!(
                "stage `{name}`: `.every_n_chunks(0)` can never reduce; use 1 or more"
            ))
        })?;
        let effects = if R::Effect::EFFECT_TYPE == S::Effect::EFFECT_TYPE {
            vec![R::Effect::EFFECT_TYPE]
        } else {
            vec![R::Effect::EFFECT_TYPE, S::Effect::EFFECT_TYPE]
        };
        let middleware = shared_middleware(row_middleware(name, &effects, stage.middleware)?);
        let keying = keying(name, self.key, stage.build)?;
        let max_open_keys = keying.max_open_keys();
        let max_open_windows = NonZeroU64::new(max_open_keys.unwrap_or(DEFAULT_MAX_OPEN_KEYS))
            .ok_or_else(|| ai_error(format!("stage `{name}`: max_open_keys must be 1 or more")))?;
        let recorded_members = [format!("{name}__map"), format!("{name}__reduce")];
        let map_binding = resolve_bindings::<R::Effect>(name, &recorded_members, stage.build)?;
        let reduce_binding = match same_bindings::<R::Effect, S::Effect>(&map_binding) {
            Some(bindings) => bindings,
            None => resolve_bindings::<S::Effect>(name, &recorded_members, stage.build)?,
        };
        Ok(FlowMember::Composite(Box::new(AiMapReduceComposite::<
            R,
            S,
            T,
            K::Key,
        > {
            name: name.to_string(),
            map: Arc::new(self.map),
            reduce: Arc::new(self.reduce),
            grouping: AiGrouping::new(name, chunk_size, Some(trigger), keying),
            collector: AiCollector::new(name, trigger, max_open_windows),
            max_open_keys,
            map_binding,
            reduce_binding,
            middleware,
        })))
    }
}

struct AiMapReduceComposite<R, S, T, Key>
where
    R: AiRole<Chunk<T>>,
    S: AiRole<Partials<T, R::Out>>,
{
    name: String,
    map: Arc<R>,
    reduce: Arc<S>,
    grouping: AiGrouping<T, Key>,
    collector: AiCollector,
    max_open_keys: Option<u64>,
    map_binding: AiStageBindings<R::Effect>,
    reduce_binding: AiStageBindings<S::Effect>,
    middleware: Vec<Arc<dyn MiddlewareFactory>>,
}

impl<R, S, T, Key> CompositeDescriptor for AiMapReduceComposite<R, S, T, Key>
where
    T: TypedPayload + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: AiRole<Partials<T, R::Out>>,
    S::Out: StageOutputFacts,
    Key: Ord + Clone + Serialize + Send + Sync + 'static,
{
    fn name(&self) -> &str {
        &self.name
    }

    fn set_name(&mut self, name: String) {
        self.name = name;
    }

    fn kind(&self) -> &'static str {
        AI_MAP_REDUCE_KIND
    }

    fn schema_version(&self) -> u32 {
        3
    }

    fn expand(self: Box<Self>, ctx: &mut CompositeBuildContext) -> Result<(), CompositeBuildError> {
        let chunk = StatefulDescriptor {
            name: "chunk".to_string(),
            handler: self.grouping,
            emit_interval: None,
            observers: Vec::new(),
            max_open_keys: self.max_open_keys,
        };
        let chunk = wrap_typed_descriptor(
            Box::new(chunk),
            StageTypingMetadata::stateful(
                TypeHint::exact_payload::<T>(),
                TypeHint::exact_payload::<AiMapReduceMapInput<T>>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![TypeHint::exact_payload::<
                AiMapReduceGroupingEnded,
            >()]),
        );

        let map = EffectfulTransformDescriptor::generated_with_pass_through::<
            AiMapReduceMapInput<T>,
            AiMapReduceGroupingEnded,
        >(
            "map",
            AiPartialMapHandler::<R, T>::new(self.map),
            vec![self.map_binding.declaration()],
            Vec::new(),
            DIRECT_FACT_BOUND,
        )
        .with_implementation_middleware(member_middleware(&self.middleware, R::Effect::EFFECT_TYPE))
        .with_ai_models::<R::Effect>(self.map_binding.specs())
        .with_policy_owner(&self.name);
        let map = wrap_typed_descriptor(
            Box::new(map),
            StageTypingMetadata::transform(
                TypeHint::exact_payload::<AiMapReduceMapInput<T>>(),
                TypeHint::exact_payload::<AiMapReduceTaggedPartial<T, R::Out>>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![
                TypeHint::exact_payload::<AiMapReduceGroupingEnded>(),
                TypeHint::exact_payload::<AiMapReduceChunkFailed>(),
            ]),
        );

        let collect = wrap_typed_descriptor(
            Box::new(StatefulDescriptor {
                name: "collect".to_string(),
                handler: self.collector,
                emit_interval: None,
                observers: Vec::new(),
                max_open_keys: None,
            }),
            StageTypingMetadata::stateful(
                TypeHint::exact_payload::<AiMapReduceTaggedPartial<T, R::Out>>(),
                TypeHint::exact_payload::<AiMapReduceReduceInput<T, R::Out>>(),
                false,
                None,
            )
            .with_additional_output_contract(vec![TypeHint::exact_payload::<
                AiMapReduceWindowFailed,
            >()]),
        );

        let reduce = EffectfulTransformDescriptor::generated::<AiMapReduceReduceInput<T, R::Out>>(
            "reduce",
            AiReduceHandler::<S, T, R::Out>::new(self.reduce),
            vec![self.reduce_binding.declaration()],
            Vec::new(),
            DIRECT_FACT_BOUND,
        )
        .with_implementation_middleware(member_middleware(&self.middleware, S::Effect::EFFECT_TYPE))
        .with_ai_models::<S::Effect>(self.reduce_binding.specs())
        .with_policy_owner(&self.name);
        let reduce = wrap_typed_descriptor(
            Box::new(reduce),
            output_metadata::<S::Out>(
                TypeHint::exact_payload::<AiMapReduceReduceInput<T, R::Out>>(),
                vec![TypeHint::exact_payload::<AiMapReduceReduceFailed>()],
            ),
        );

        ctx.member("chunk").descriptor(chunk);
        ctx.member("map").descriptor(map);
        ctx.member("collect").descriptor(collect);
        ctx.member("reduce").descriptor(reduce);

        ctx.feed("chunk", "map")
            .lane("data")
            .payload::<AiMapReduceMapInput<T>>()
            .payload::<AiMapReduceGroupingEnded>();
        ctx.feed("map", "collect")
            .lane("data")
            .payload::<AiMapReduceGroupingEnded>()
            .payload::<AiMapReduceTaggedPartial<T, R::Out>>()
            .payload::<AiMapReduceChunkFailed>();
        ctx.feed("collect", "reduce")
            .lane("data")
            .payload::<AiMapReduceReduceInput<T, R::Out>>();

        ctx.boundary()
            .input("in", "chunk")
            .payload::<T>()
            .default()
            .output("out", "reduce")
            .payload_hints(fact_set_hints::<S::Out>())
            .default();
        Ok(())
    }
}
