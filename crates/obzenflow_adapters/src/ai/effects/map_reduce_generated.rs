// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ChatCompletion, ChatCompletionBuildError};
use async_trait::async_trait;
use obzenflow_core::ai::{
    AiFinaliseRole, AiMapReduceChunkFailed, AiMapReduceFinaliseFailed, AiMapReduceRoleFailure,
    AiMapReduceTaggedPartial, AiMapRole, AiProviderFailureKind, ChatTarget, ChunkEnvelope,
    ResolvedTokenEstimator,
};
use obzenflow_core::event::{EffectFailureDetail, StageFatalCode, StageFatalReason};
use obzenflow_core::id::CompositeId;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use std::fmt;
use std::sync::Arc;

pub const MAP_CHAT_COMPLETION_LABEL: &str = "ai_map_reduce.map.chat_completion";
pub const FINALISE_CHAT_COMPLETION_LABEL: &str = "ai_map_reduce.finalise.chat_completion";

pub struct GeneratedAiMapHandler<Item, Partial, Role> {
    role: Arc<Role>,
    chat_target: ChatTarget,
    chat_estimator: ResolvedTokenEstimator,
    composite_id: CompositeId,
    _types: std::marker::PhantomData<fn() -> (Item, Partial)>,
}

impl<Item, Partial, Role> Clone for GeneratedAiMapHandler<Item, Partial, Role> {
    fn clone(&self) -> Self {
        Self {
            role: self.role.clone(),
            chat_target: self.chat_target.clone(),
            chat_estimator: self.chat_estimator.clone(),
            composite_id: self.composite_id.clone(),
            _types: std::marker::PhantomData,
        }
    }
}

impl<Item, Partial, Role> GeneratedAiMapHandler<Item, Partial, Role> {
    pub fn new(
        role: Role,
        chat_target: ChatTarget,
        chat_estimator: ResolvedTokenEstimator,
        composite_id: CompositeId,
    ) -> Self {
        Self {
            role: Arc::new(role),
            chat_target,
            chat_estimator,
            composite_id,
            _types: std::marker::PhantomData,
        }
    }
}

impl<Item, Partial, Role> fmt::Debug for GeneratedAiMapHandler<Item, Partial, Role> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedAiMapHandler")
            .field("chat_target", &self.chat_target)
            .field("composite_id", &self.composite_id)
            .finish_non_exhaustive()
    }
}

pub struct GeneratedAiFinaliseHandler<Seed, Collected, Out, Role> {
    role: Arc<Role>,
    chat_target: ChatTarget,
    chat_estimator: ResolvedTokenEstimator,
    _types: std::marker::PhantomData<fn() -> (Seed, Collected, Out)>,
}

impl<Seed, Collected, Out, Role> Clone for GeneratedAiFinaliseHandler<Seed, Collected, Out, Role> {
    fn clone(&self) -> Self {
        Self {
            role: self.role.clone(),
            chat_target: self.chat_target.clone(),
            chat_estimator: self.chat_estimator.clone(),
            _types: std::marker::PhantomData,
        }
    }
}

impl<Seed, Collected, Out, Role> GeneratedAiFinaliseHandler<Seed, Collected, Out, Role> {
    pub fn new(
        role: Role,
        chat_target: ChatTarget,
        chat_estimator: ResolvedTokenEstimator,
    ) -> Self {
        Self {
            role: Arc::new(role),
            chat_target,
            chat_estimator,
            _types: std::marker::PhantomData,
        }
    }
}

impl<Seed, Collected, Out, Role> fmt::Debug
    for GeneratedAiFinaliseHandler<Seed, Collected, Out, Role>
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedAiFinaliseHandler")
            .field("chat_target", &self.chat_target)
            .finish_non_exhaustive()
    }
}

fn fatal(
    code: StageFatalCode,
    reason: StageFatalReason,
    detail: impl Into<String>,
) -> HandlerError {
    HandlerError::Fatal(StageFatal::new(code, reason, detail))
}

fn fatal_from_effect(error: EffectError) -> HandlerError {
    match error {
        EffectError::MissingEffectPort { name, .. } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortRegistrationMissing,
            format!("required effect port '{name}' is not registered"),
        ),
        EffectError::EffectPortResolutionFailed { name, message, .. } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortResolutionFailed,
            format!("effect port '{name}' failed to resolve: {message}"),
        ),
        EffectError::EffectPortBindingMismatch {
            port,
            expected,
            observed,
        } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortBindingMismatch,
            format!(
                "effect port '{port}' binding mismatch: expected {expected}, observed {observed}"
            ),
        ),
        EffectError::EffectPortBindingInvariantViolation {
            port,
            expected,
            observed,
        } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortTargetInvariantViolation,
            format!(
                "effect port '{port}' target invariant failed: expected {expected}, observed {observed}"
            ),
        ),
        EffectError::RecordedFailure {
            detail:
                Some(EffectFailureDetail::PortBindingInvariantViolation {
                    port,
                    expected,
                    observed,
                }),
            ..
        } => fatal(
            StageFatalCode::Configuration,
            StageFatalReason::EffectPortTargetInvariantViolation,
            format!(
                "effect port '{port}' target invariant failed: expected {expected}, observed {observed}"
            ),
        ),
        EffectError::Journal(message) => fatal(
            StageFatalCode::Journal,
            StageFatalReason::JournalFailure,
            message,
        ),
        error @ (EffectError::MissingRecordedEffect { .. }
        | EffectError::EffectInDoubt { .. }
        | EffectError::DuplicateRecordedEffect { .. }
        | EffectError::DescriptorMismatch { .. }
        | EffectError::IncompleteOutcomeGroup { .. }) => fatal(
            StageFatalCode::Replay,
            StageFatalReason::ReplayDivergence,
            error.to_string(),
        ),
        EffectError::Serialization(message)
        | EffectError::EffectProvenanceMismatch(message)
        | EffectError::ReplayArchive(message) => fatal(
            StageFatalCode::Replay,
            StageFatalReason::ReplayDivergence,
            message,
        ),
        other => fatal(
            StageFatalCode::Coordination,
            StageFatalReason::CoordinationFailure,
            other.to_string(),
        ),
    }
}

fn provider_kind(code: &str) -> AiProviderFailureKind {
    match code {
        "timeout" => AiProviderFailureKind::Timeout,
        "remote" | "transport" => AiProviderFailureKind::Remote,
        "rate_limited" => AiProviderFailureKind::RateLimited,
        "authentication" | "auth" => AiProviderFailureKind::Authentication,
        "invalid_request" | "validation" => AiProviderFailureKind::InvalidRequest,
        "unsupported" => AiProviderFailureKind::Unsupported,
        _ => AiProviderFailureKind::Other,
    }
}

fn role_failure_from_effect(error: EffectError) -> Result<AiMapReduceRoleFailure, HandlerError> {
    match error {
        EffectError::BoundaryRejected {
            rejected_by,
            code,
            message,
            ..
        } => Ok(AiMapReduceRoleFailure::BoundaryRejected {
            source: rejected_by.to_string(),
            code: code.to_string(),
            message,
        }),
        EffectError::RecoveryAbandoned {
            last_started_attempt,
            failure_source,
            code,
            message,
            ..
        } => Ok(AiMapReduceRoleFailure::RecoveryAbandoned {
            last_started_attempt: last_started_attempt.get(),
            source: failure_source.to_string(),
            code: code.to_string(),
            message,
        }),
        EffectError::DependencyFailed { code, message, .. } => {
            Ok(AiMapReduceRoleFailure::Provider {
                provider_kind: provider_kind(code.as_str()),
                message,
            })
        }
        EffectError::RecordedFailure {
            error_type,
            error_message,
            cause,
            detail: None,
            ..
        } if error_type.as_str() == "boundary_rejected" => {
            let cause = cause.ok_or_else(|| {
                fatal(
                    StageFatalCode::Replay,
                    StageFatalReason::ReplayDivergence,
                    "recorded boundary rejection is missing its structured cause",
                )
            })?;
            Ok(AiMapReduceRoleFailure::BoundaryRejected {
                source: cause.source.to_string(),
                code: cause.code.to_string(),
                message: error_message,
            })
        }
        EffectError::RecordedFailure {
            error_type,
            error_message,
            cause,
            detail: None,
            ..
        } if error_type.as_str() != "recovery_abandoned" => {
            let code = cause
                .as_ref()
                .map(|cause| cause.code.as_str())
                .unwrap_or_else(|| error_type.as_str());
            Ok(AiMapReduceRoleFailure::Provider {
                provider_kind: provider_kind(code),
                message: error_message,
            })
        }
        other => Err(fatal_from_effect(other)),
    }
}

fn request_canonicalization_failure(error: ChatCompletionBuildError) -> AiMapReduceRoleFailure {
    match error {
        ChatCompletionBuildError::RequestCanonicalization { component, detail } => {
            AiMapReduceRoleFailure::RequestCanonicalization {
                component,
                message: detail,
            }
        }
    }
}

fn exact_job_key<Output, EffectsSet>(
    fx: &Effects<Output, EffectsSet>,
    composite_id: &CompositeId,
) -> Result<obzenflow_core::EventId, HandlerError>
where
    Output: obzenflow_core::StageFactSet,
    EffectsSet: obzenflow_runtime::effects::EffectSet,
{
    let matching = fx
        .__generated_parent_composite_activations()
        .into_iter()
        .filter(|activation| &activation.composite_id == composite_id)
        .collect::<Vec<_>>();
    let [activation] = matching.as_slice() else {
        return Err(fatal(
            StageFatalCode::Protocol,
            StageFatalReason::ProtocolInputIntegrity,
            format!(
                "generated AI map input requires exactly one activation for composite '{composite_id}', found {}",
                matching.len()
            ),
        ));
    };
    Ok(activation.activation)
}

async fn prepare_pre_effect_terminal<Output, EffectsSet>(
    fx: &Effects<Output, EffectsSet>,
) -> Result<(), HandlerError>
where
    Output: obzenflow_core::StageFactSet,
    EffectsSet: obzenflow_runtime::effects::EffectSet,
{
    fx.__generated_preflight_first_effect_is_empty()
        .await
        .map_err(|error| {
            fatal(
                StageFatalCode::Replay,
                StageFatalReason::ReplayDivergence,
                error.to_string(),
            )
        })?;
    fx.__generated_request_live_admission()
        .await
        .map_err(fatal_from_effect)
}

fn target_assertion_failure(expected: &ChatTarget, observed: &ChatTarget) -> HandlerError {
    fatal(
        StageFatalCode::Configuration,
        StageFatalReason::EffectTargetAssertionMismatch,
        format!("prepared chat target {observed} does not match declared target {expected}"),
    )
}

fn emit_failure(error: EffectError) -> HandlerError {
    fatal_from_effect(error)
}

#[async_trait]
impl<Item, Partial, Role> EffectfulTransformHandler for GeneratedAiMapHandler<Item, Partial, Role>
where
    Item: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
    Partial: TypedPayload + Clone + Send + Sync + 'static,
    Role: AiMapRole<Item, Partial>,
{
    type Input = ChunkEnvelope<Item>;
    type Output = obzenflow_core::stage_fact_set![
        obzenflow_core::ai::ChatCompletionCompleted,
        AiMapReduceTaggedPartial<Partial>,
        AiMapReduceChunkFailed,
    ];
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let job_key = exact_job_key(fx, &self.composite_id)?;
        let chunk_index = input.chunk_index;
        let chunk_count = input.chunk_count;
        if chunk_count == 0 || chunk_index >= chunk_count {
            return Err(fatal(
                StageFatalCode::Protocol,
                StageFatalReason::ProtocolInputIntegrity,
                format!(
                    "map chunk index/count is invalid: index={chunk_index}, count={chunk_count}"
                ),
            ));
        }
        let info = input.chunk_info();
        let items = input.items;

        let (request, prepared) = match self.role.prepare(&items, &info) {
            Ok(prepared) => prepared,
            Err(logic) => {
                prepare_pre_effect_terminal(fx).await?;
                fx.emit(AiMapReduceChunkFailed {
                    job_key,
                    chunk_index,
                    chunk_count,
                    cause: AiMapReduceRoleFailure::Logic { logic },
                })
                .await
                .map_err(emit_failure)?;
                return fx.complete().map_err(emit_failure);
            }
        };
        let observed_target = request.target();
        if observed_target != self.chat_target {
            return Err(target_assertion_failure(
                &self.chat_target,
                &observed_target,
            ));
        }
        let effect = match ChatCompletion::new(
            MAP_CHAT_COMPLETION_LABEL,
            request,
            self.chat_estimator.clone(),
        ) {
            Ok(effect) => effect,
            Err(error) => {
                prepare_pre_effect_terminal(fx).await?;
                fx.emit(AiMapReduceChunkFailed {
                    job_key,
                    chunk_index,
                    chunk_count,
                    cause: request_canonicalization_failure(error),
                })
                .await
                .map_err(emit_failure)?;
                return fx.complete().map_err(emit_failure);
            }
        };

        match fx.perform(effect).await {
            Ok(completion) => {
                let terminal = match self.role.interpret(items, prepared, completion) {
                    Ok(partial) => AiMapReduceTaggedPartial {
                        job_key,
                        chunk_index,
                        chunk_count,
                        partial,
                    },
                    Err(logic) => {
                        fx.emit(AiMapReduceChunkFailed {
                            job_key,
                            chunk_index,
                            chunk_count,
                            cause: AiMapReduceRoleFailure::Logic { logic },
                        })
                        .await
                        .map_err(emit_failure)?;
                        return fx.complete().map_err(emit_failure);
                    }
                };
                fx.emit(terminal).await.map_err(emit_failure)?;
            }
            Err(error) => {
                let cause = role_failure_from_effect(error)?;
                fx.emit(AiMapReduceChunkFailed {
                    job_key,
                    chunk_index,
                    chunk_count,
                    cause,
                })
                .await
                .map_err(emit_failure)?;
            }
        }
        fx.complete().map_err(emit_failure)
    }

    async fn __generated_raw_dispatch(
        &self,
        event: obzenflow_core::ChainEvent,
        _fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Option<Result<Vec<obzenflow_core::ChainEvent>, HandlerError>> {
        if obzenflow_core::ai::AiMapReducePlanningManifest::event_type_matches(&event.event_type())
        {
            return Some(Ok(vec![event]));
        }
        None
    }
}

#[async_trait]
impl<Seed, Collected, Out, Role> EffectfulTransformHandler
    for GeneratedAiFinaliseHandler<Seed, Collected, Out, Role>
where
    Seed: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
    Collected: serde::Serialize + serde::de::DeserializeOwned + Clone + Send + Sync + 'static,
    Out: TypedPayload + Clone + Send + Sync + 'static,
    Role: AiFinaliseRole<Seed, Collected, Out>,
{
    type Input = obzenflow_core::ai::AiMapReduceReduceInput<Seed, Collected>;
    type Output = obzenflow_core::stage_fact_set![
        obzenflow_core::ai::ChatCompletionCompleted,
        Out,
        AiMapReduceFinaliseFailed,
    ];
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let job_key = input.job_key;
        let seed = input.seed;
        let collected = input.collected;
        let (request, prepared) = match self.role.prepare(&seed, &collected) {
            Ok(prepared) => prepared,
            Err(logic) => {
                prepare_pre_effect_terminal(fx).await?;
                fx.emit(AiMapReduceFinaliseFailed {
                    job_key,
                    cause: AiMapReduceRoleFailure::Logic { logic },
                })
                .await
                .map_err(emit_failure)?;
                return fx.complete().map_err(emit_failure);
            }
        };
        let observed_target = request.target();
        if observed_target != self.chat_target {
            return Err(target_assertion_failure(
                &self.chat_target,
                &observed_target,
            ));
        }
        let effect = match ChatCompletion::new(
            FINALISE_CHAT_COMPLETION_LABEL,
            request,
            self.chat_estimator.clone(),
        ) {
            Ok(effect) => effect,
            Err(error) => {
                prepare_pre_effect_terminal(fx).await?;
                fx.emit(AiMapReduceFinaliseFailed {
                    job_key,
                    cause: request_canonicalization_failure(error),
                })
                .await
                .map_err(emit_failure)?;
                return fx.complete().map_err(emit_failure);
            }
        };

        match fx.perform(effect).await {
            Ok(completion) => match self.role.interpret(seed, collected, prepared, completion) {
                Ok(output) => fx.emit(output).await.map_err(emit_failure)?,
                Err(logic) => {
                    fx.emit(AiMapReduceFinaliseFailed {
                        job_key,
                        cause: AiMapReduceRoleFailure::Logic { logic },
                    })
                    .await
                    .map_err(emit_failure)?;
                }
            },
            Err(error) => {
                let cause = role_failure_from_effect(error)?;
                fx.emit(AiMapReduceFinaliseFailed { job_key, cause })
                    .await
                    .map_err(emit_failure)?;
            }
        }
        fx.complete().map_err(emit_failure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_runtime::effects::{EffectAttemptOrdinal, EffectCursor};

    #[test]
    fn strict_in_doubt_history_maps_to_typed_replay_fatal() {
        let error = EffectError::EffectInDoubt {
            cursor: EffectCursor::new("flow", "stage", 1_u64, 0_u32),
            highest_started_attempt: EffectAttemptOrdinal::new(1),
        };

        let fatal = fatal_from_effect(error)
            .as_fatal()
            .expect("in-doubt history is stage-fatal")
            .clone();
        assert_eq!(fatal.code, StageFatalCode::Replay);
        assert_eq!(fatal.reason, StageFatalReason::ReplayDivergence);
    }
}
