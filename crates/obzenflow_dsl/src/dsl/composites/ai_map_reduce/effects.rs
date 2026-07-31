// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::dsl::ai_effect::{invoke_generated_chat, GeneratedChatInvocationError};
use async_trait::async_trait;
use obzenflow_adapters::ai::{ChatCompletion, ChatCompletionBuildError};
use obzenflow_core::ai::{
    AiFinaliseRole, AiMapReduceChunkFailed, AiMapReduceFinaliseFailed, AiMapReduceMapInput,
    AiMapReduceRoleFailure, AiMapReduceTaggedPartial, AiMapRole, AiProviderFailureKind,
    ChatBindingContract, ChunkEnvelope,
};
use obzenflow_core::event::{EffectFailureDetail, StageFatalCode, StageFatalReason};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{EffectError, Effects, StageCompletion};
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use std::fmt;
use std::sync::Arc;

pub(super) const MAP_CHAT_COMPLETION_LABEL: &str = "ai_map_reduce.map.chat_completion";
pub(super) const FINALISE_CHAT_COMPLETION_LABEL: &str = "ai_map_reduce.finalise.chat_completion";

type GeneratedFinaliseTypes<Seed, Collected, Out> = fn() -> (Seed, Collected, Out);

pub(super) struct GeneratedAiMapHandler<Item, Partial, Role> {
    role: Arc<Role>,
    chat_binding: ChatBindingContract,
    _types: std::marker::PhantomData<fn() -> (Item, Partial)>,
}

impl<Item, Partial, Role> Clone for GeneratedAiMapHandler<Item, Partial, Role> {
    fn clone(&self) -> Self {
        Self {
            role: self.role.clone(),
            chat_binding: self.chat_binding.clone(),
            _types: std::marker::PhantomData,
        }
    }
}

impl<Item, Partial, Role> GeneratedAiMapHandler<Item, Partial, Role> {
    pub(super) fn new(role: Role, chat_binding: ChatBindingContract) -> Self {
        Self {
            role: Arc::new(role),
            chat_binding,
            _types: std::marker::PhantomData,
        }
    }
}

impl<Item, Partial, Role> fmt::Debug for GeneratedAiMapHandler<Item, Partial, Role> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedAiMapHandler")
            .field("chat_binding", &self.chat_binding)
            .finish_non_exhaustive()
    }
}

pub(super) struct GeneratedAiFinaliseHandler<Seed, Collected, Out, Role> {
    role: Arc<Role>,
    chat_binding: ChatBindingContract,
    _types: std::marker::PhantomData<GeneratedFinaliseTypes<Seed, Collected, Out>>,
}

impl<Seed, Collected, Out, Role> Clone for GeneratedAiFinaliseHandler<Seed, Collected, Out, Role> {
    fn clone(&self) -> Self {
        Self {
            role: self.role.clone(),
            chat_binding: self.chat_binding.clone(),
            _types: std::marker::PhantomData,
        }
    }
}

impl<Seed, Collected, Out, Role> GeneratedAiFinaliseHandler<Seed, Collected, Out, Role> {
    pub(super) fn new(role: Role, chat_binding: ChatBindingContract) -> Self {
        Self {
            role: Arc::new(role),
            chat_binding,
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
            .field("chat_binding", &self.chat_binding)
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
            detail: Some(detail),
            ..
        } => match *detail {
            EffectFailureDetail::PortBindingInvariantViolation {
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
        },
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
    type Input = AiMapReduceMapInput<ChunkEnvelope<Item>>;
    type Output =
        obzenflow_core::stage_fact_set![AiMapReduceTaggedPartial<Partial>, AiMapReduceChunkFailed,];
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let job_key = input.job_key;
        let input = input.chunk;
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

        let request = match self.role.prepare(&items, &info) {
            Ok(request) => request,
            Err(logic) => {
                return fx
                    .complete_with_pre_effect_fact(AiMapReduceChunkFailed {
                        job_key,
                        chunk_index,
                        chunk_count,
                        cause: AiMapReduceRoleFailure::Logic { logic },
                    })
                    .await
                    .map_err(emit_failure);
            }
        };
        let reply = match invoke_generated_chat(
            fx,
            &self.chat_binding,
            &request,
            MAP_CHAT_COMPLETION_LABEL,
        )
        .await
        {
            Ok(reply) => reply,
            Err(GeneratedChatInvocationError::Build(error)) => {
                return fx
                    .complete_with_pre_effect_fact(AiMapReduceChunkFailed {
                        job_key,
                        chunk_index,
                        chunk_count,
                        cause: request_canonicalization_failure(error),
                    })
                    .await
                    .map_err(emit_failure);
            }
            Err(GeneratedChatInvocationError::Effect(error)) => {
                let cause = role_failure_from_effect(error)?;
                fx.emit(AiMapReduceChunkFailed {
                    job_key,
                    chunk_index,
                    chunk_count,
                    cause,
                })
                .await
                .map_err(emit_failure)?;
                return fx.complete().map_err(emit_failure);
            }
        };

        let terminal = match self.role.interpret(items, info, request, reply) {
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
        fx.complete().map_err(emit_failure)
    }

    fn stage_logic_version(&self) -> &str {
        Role::LOGIC_VERSION
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
    type Output = obzenflow_core::stage_fact_set![Out, AiMapReduceFinaliseFailed,];
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let job_key = input.job_key;
        let seed = input.seed;
        let collected = input.collected;
        let request = match self.role.prepare(&seed, &collected) {
            Ok(request) => request,
            Err(logic) => {
                return fx
                    .complete_with_pre_effect_fact(AiMapReduceFinaliseFailed {
                        job_key,
                        cause: AiMapReduceRoleFailure::Logic { logic },
                    })
                    .await
                    .map_err(emit_failure);
            }
        };
        let reply = match invoke_generated_chat(
            fx,
            &self.chat_binding,
            &request,
            FINALISE_CHAT_COMPLETION_LABEL,
        )
        .await
        {
            Ok(reply) => reply,
            Err(GeneratedChatInvocationError::Build(error)) => {
                return fx
                    .complete_with_pre_effect_fact(AiMapReduceFinaliseFailed {
                        job_key,
                        cause: request_canonicalization_failure(error),
                    })
                    .await
                    .map_err(emit_failure);
            }
            Err(GeneratedChatInvocationError::Effect(error)) => {
                let cause = role_failure_from_effect(error)?;
                fx.emit(AiMapReduceFinaliseFailed { job_key, cause })
                    .await
                    .map_err(emit_failure)?;
                return fx.complete().map_err(emit_failure);
            }
        };

        match self.role.interpret(seed, collected, request, reply) {
            Ok(output) => fx.emit(output).await.map_err(emit_failure)?,
            Err(logic) => {
                fx.emit(AiMapReduceFinaliseFailed {
                    job_key,
                    cause: AiMapReduceRoleFailure::Logic { logic },
                })
                .await
                .map_err(emit_failure)?;
            }
        }
        fx.complete().map_err(emit_failure)
    }

    fn stage_logic_version(&self) -> &str {
        Role::LOGIC_VERSION
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::ai::CanonicalizationComponent;
    use obzenflow_runtime::effects::{EffectAttemptOrdinal, EffectCursor};

    #[test]
    fn canonicalization_components_map_one_to_one_into_domain_failures() {
        for component in [
            CanonicalizationComponent::Prompt,
            CanonicalizationComponent::Parameters,
            CanonicalizationComponent::ResponseSchema,
        ] {
            let mapped = request_canonicalization_failure(
                ChatCompletionBuildError::RequestCanonicalization {
                    component,
                    detail: format!("{component:?} fixture"),
                },
            );
            assert_eq!(
                mapped,
                AiMapReduceRoleFailure::RequestCanonicalization {
                    component,
                    message: format!("{component:?} fixture"),
                }
            );
            assert_eq!(
                serde_json::to_value(&mapped).expect("canonicalisation domain failure serialises")
                    ["component"],
                serde_json::to_value(component).expect("component serialises")
            );
        }
    }

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
