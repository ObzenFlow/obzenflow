// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::unavailable::{integrity_failure, unavailable_or_integrity};
use super::{AiEffect, AiRole};
use crate::effects::{
    AllowedEffectsAllowEffect, DeclaredEffectSet, EffectHandler, EffectOutcomeFitsOutput,
    EffectSet, Effects, StageCompletion,
};
use crate::stages::common::handler_error::HandlerError;
use crate::stages::common::handlers::EffectfulTransformHandler;
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiMapReduceChunkFailed, AiMapReduceMapInput, AiMapReduceReduceFailed, AiMapReduceReduceInput,
    AiMapReduceRoleFailure, AiMapReduceTaggedPartial, AiOutcome, AiUnavailable, Chunk, Partials,
};
use obzenflow_core::event::schema::{
    EmptySet, MemberList, StageOutputFacts, SubsetProofEnd, TypedFactType, WithMember,
};
use obzenflow_core::{StageFactSet, TypedPayload};
use serde::{de::DeserializeOwned, Serialize};
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

const INFER_LABEL: &str = "ai.infer";
const MAP_LABEL: &str = "ai.map";
const REDUCE_LABEL: &str = "ai.reduce";

enum CallFailure {
    Request(String),
    Integrity(HandlerError),
}

async fn call<E, Output, Allowed, At>(
    fx: &mut Effects<Output, Allowed>,
    label: &'static str,
    request: &E::Request,
) -> Result<AiOutcome<E::Reply>, CallFailure>
where
    E: AiEffect
        + AllowedEffectsAllowEffect<Allowed, At>
        + EffectOutcomeFitsOutput<Output, SubsetProofEnd>,
    Output: StageFactSet,
    Allowed: EffectSet,
{
    let effect = fx
        .configured_effect_for_next_cursor::<E, At, _>(|binding| E::effect(label, request, binding))
        .map_err(|error| CallFailure::Integrity(integrity_failure(error)))?
        .map_err(|error| CallFailure::Request(error.to_string()))?;
    match fx.perform::<E, At, SubsetProofEnd>(effect).await {
        Ok(outcome) => Ok(AiOutcome::Replied(E::reply(request.clone(), outcome))),
        Err(error) => unavailable_or_integrity(error)
            .map(AiOutcome::Unavailable)
            .map_err(CallFailure::Integrity),
    }
}

fn role_failure(error: HandlerError) -> Result<AiMapReduceRoleFailure, HandlerError> {
    if error.is_fatal() || error.is_contract_violation() {
        return Err(error);
    }
    Ok(AiMapReduceRoleFailure::Role {
        kind: error.kind(),
        message: error.to_string(),
    })
}

fn interpret_failure(
    error: HandlerError,
    unavailable: Option<AiUnavailable>,
) -> Result<AiMapReduceRoleFailure, HandlerError> {
    match unavailable {
        Some(unavailable) if !error.is_fatal() && !error.is_contract_violation() => {
            Ok(AiMapReduceRoleFailure::Unavailable { unavailable })
        }
        _ => role_failure(error),
    }
}

type RoleEffect<E> = DeclaredEffectSet<WithMember<E, EmptySet>>;

#[doc(hidden)]
pub struct AiInferHandler<R, In> {
    role: Arc<R>,
    _input: PhantomData<fn() -> In>,
}

impl<R: AiRole<In>, In> EffectHandler for AiInferHandler<R, In> {
    type Systems = ();
    type AllowedEffects = RoleEffect<R::Effect>;

    fn systems(&self) -> Self::Systems {}
}

impl<R, In> AiInferHandler<R, In> {
    pub fn new(role: Arc<R>) -> Self {
        Self {
            role,
            _input: PhantomData,
        }
    }
}

impl<R, In> Clone for AiInferHandler<R, In> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.role))
    }
}

impl<R, In> fmt::Debug for AiInferHandler<R, In> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiInferHandler")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<R, In> EffectfulTransformHandler for AiInferHandler<R, In>
where
    In: TypedPayload + Send + Sync + 'static,
    R: AiRole<In>,
    R::Out: StageOutputFacts,
{
    type Input = In;
    type Output = R::Out;

    async fn process(
        &self,
        input: In,
        fx: &mut Effects<R::Out, Self::AllowedEffects>,
    ) -> Result<StageCompletion<R::Out>, HandlerError> {
        let request = self.role.prepare(&input)?;
        let outcome = match call::<R::Effect, _, _, _>(fx, INFER_LABEL, &request).await {
            Ok(outcome) => outcome,
            Err(CallFailure::Request(message)) => return Err(HandlerError::Validation(message)),
            Err(CallFailure::Integrity(error)) => return Err(error),
        };
        let output = self.role.interpret(input, outcome)?;
        fx.emit_carrier(output).await.map_err(integrity_failure)?;
        fx.complete().map_err(integrity_failure)
    }

    fn stage_logic_version(&self) -> &str {
        self.role.stage_logic_version()
    }
}

fn chunk_of<T>(group: AiMapReduceMapInput<T>) -> Chunk<T> {
    Chunk::new(group.group, group.first_number, group.items)
}

#[doc(hidden)]
pub struct AiMapHandler<R, T> {
    role: Arc<R>,
    _items: PhantomData<fn() -> T>,
}

impl<R: AiRole<Chunk<T>>, T> EffectHandler for AiMapHandler<R, T> {
    type Systems = ();
    type AllowedEffects = RoleEffect<R::Effect>;

    fn systems(&self) -> Self::Systems {}
}

impl<R, T> AiMapHandler<R, T> {
    pub fn new(role: Arc<R>) -> Self {
        Self {
            role,
            _items: PhantomData,
        }
    }
}

impl<R, T> Clone for AiMapHandler<R, T> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.role))
    }
}

impl<R, T> fmt::Debug for AiMapHandler<R, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiMapHandler")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<R, T> EffectfulTransformHandler for AiMapHandler<R, T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: StageOutputFacts,
{
    type Input = AiMapReduceMapInput<T>;
    type Output = R::Out;

    async fn process(
        &self,
        group: AiMapReduceMapInput<T>,
        fx: &mut Effects<R::Out, Self::AllowedEffects>,
    ) -> Result<StageCompletion<R::Out>, HandlerError> {
        let chunk = chunk_of(group);
        let request = self.role.prepare(&chunk)?;
        let outcome = match call::<R::Effect, _, _, _>(fx, MAP_LABEL, &request).await {
            Ok(outcome) => outcome,
            Err(CallFailure::Request(message)) => return Err(HandlerError::Validation(message)),
            Err(CallFailure::Integrity(error)) => return Err(error),
        };
        let output = self.role.interpret(chunk, outcome)?;
        fx.emit_carrier(output).await.map_err(integrity_failure)?;
        fx.complete().map_err(integrity_failure)
    }

    fn stage_logic_version(&self) -> &str {
        self.role.stage_logic_version()
    }
}

#[doc(hidden)]
pub type AiMapPartialFacts<T, P> =
    obzenflow_core::stage_fact_set![AiMapReduceTaggedPartial<T, P>, AiMapReduceChunkFailed];

#[doc(hidden)]
pub struct AiPartialMapHandler<R, T> {
    role: Arc<R>,
    _items: PhantomData<fn() -> T>,
}

impl<R: AiRole<Chunk<T>>, T> EffectHandler for AiPartialMapHandler<R, T> {
    type Systems = ();
    type AllowedEffects = RoleEffect<R::Effect>;

    fn systems(&self) -> Self::Systems {}
}

impl<R, T> AiPartialMapHandler<R, T> {
    pub fn new(role: Arc<R>) -> Self {
        Self {
            role,
            _items: PhantomData,
        }
    }
}

impl<R, T> Clone for AiPartialMapHandler<R, T> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.role))
    }
}

impl<R, T> fmt::Debug for AiPartialMapHandler<R, T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiPartialMapHandler")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<R, T> EffectfulTransformHandler for AiPartialMapHandler<R, T>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    R: AiRole<Chunk<T>>,
    R::Out: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    type Input = AiMapReduceMapInput<T>;
    type Output = AiMapPartialFacts<T, R::Out>;

    async fn process(
        &self,
        group: AiMapReduceMapInput<T>,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let key = group.key.clone();
        let ordinal = group.group;
        let items = group.items.clone();
        let chunk = chunk_of(group);
        let failed = |cause| AiMapReduceChunkFailed {
            key: key.clone(),
            group: ordinal,
            cause,
        };

        let request = match self.role.prepare(&chunk) {
            Ok(request) => request,
            Err(error) => {
                return fx
                    .complete_with_pre_effect_fact(failed(role_failure(error)?))
                    .await
                    .map_err(integrity_failure);
            }
        };
        let outcome = match call::<R::Effect, _, _, _>(fx, MAP_LABEL, &request).await {
            Ok(outcome) => outcome,
            Err(CallFailure::Request(message)) => {
                return fx
                    .complete_with_pre_effect_fact(failed(AiMapReduceRoleFailure::Request {
                        message,
                    }))
                    .await
                    .map_err(integrity_failure);
            }
            Err(CallFailure::Integrity(error)) => return Err(error),
        };
        let unavailable = outcome.unavailable().cloned();
        match self.role.interpret(chunk, outcome) {
            Ok(partial) => fx
                .emit(AiMapReduceTaggedPartial {
                    key: key.clone(),
                    group: ordinal,
                    items,
                    partial,
                })
                .await
                .map_err(integrity_failure)?,
            Err(error) => fx
                .emit(failed(interpret_failure(error, unavailable)?))
                .await
                .map_err(integrity_failure)?,
        }
        fx.complete().map_err(integrity_failure)
    }

    fn stage_logic_version(&self) -> &str {
        self.role.stage_logic_version()
    }
}

#[doc(hidden)]
pub struct AiReduceFacts<O>(PhantomData<fn() -> O>);

impl<O: StageFactSet> StageFactSet for AiReduceFacts<O>
where
    WithMember<AiMapReduceReduceFailed, O::Members>: MemberList,
{
    type Members = WithMember<AiMapReduceReduceFailed, O::Members>;

    fn member_fact_types() -> Vec<TypedFactType> {
        let mut types = O::member_fact_types();
        types.push(TypedFactType::of::<AiMapReduceReduceFailed>());
        types
    }
}

#[doc(hidden)]
pub struct AiReduceHandler<S, T, P> {
    role: Arc<S>,
    _types: PhantomData<fn() -> (T, P)>,
}

impl<S: AiRole<Partials<T, P>>, T, P> EffectHandler for AiReduceHandler<S, T, P> {
    type Systems = ();
    type AllowedEffects = RoleEffect<S::Effect>;

    fn systems(&self) -> Self::Systems {}
}

impl<S, T, P> AiReduceHandler<S, T, P> {
    pub fn new(role: Arc<S>) -> Self {
        Self {
            role,
            _types: PhantomData,
        }
    }
}

impl<S, T, P> Clone for AiReduceHandler<S, T, P> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.role))
    }
}

impl<S, T, P> fmt::Debug for AiReduceHandler<S, T, P> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AiReduceHandler")
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<S, T, P> EffectfulTransformHandler for AiReduceHandler<S, T, P>
where
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    P: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    S: AiRole<Partials<T, P>>,
    S::Out: StageOutputFacts,
    WithMember<AiMapReduceReduceFailed, <S::Out as StageFactSet>::Members>: MemberList,
{
    type Input = AiMapReduceReduceInput<T, P>;
    type Output = AiReduceFacts<S::Out>;

    async fn process(
        &self,
        window: AiMapReduceReduceInput<T, P>,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let key = window.key.clone();
        let ordinal = window.window;
        let partials = Partials::new(window.window, window.items, window.partials);
        let failed = |cause| AiMapReduceReduceFailed {
            key: key.clone(),
            window: ordinal,
            cause,
        };

        let request = match self.role.prepare(&partials) {
            Ok(request) => request,
            Err(error) => {
                return fx
                    .complete_with_pre_effect_fact(failed(role_failure(error)?))
                    .await
                    .map_err(integrity_failure);
            }
        };
        let outcome = match call::<S::Effect, _, _, _>(fx, REDUCE_LABEL, &request).await {
            Ok(outcome) => outcome,
            Err(CallFailure::Request(message)) => {
                return fx
                    .complete_with_pre_effect_fact(failed(AiMapReduceRoleFailure::Request {
                        message,
                    }))
                    .await
                    .map_err(integrity_failure);
            }
            Err(CallFailure::Integrity(error)) => return Err(error),
        };
        let unavailable = outcome.unavailable().cloned();
        match self.role.interpret(partials, outcome) {
            Ok(output) => fx.emit_carrier(output).await.map_err(integrity_failure)?,
            Err(error) => fx
                .emit(failed(interpret_failure(error, unavailable)?))
                .await
                .map_err(integrity_failure)?,
        }
        fx.complete().map_err(integrity_failure)
    }

    fn stage_logic_version(&self) -> &str {
        self.role.stage_logic_version()
    }
}
