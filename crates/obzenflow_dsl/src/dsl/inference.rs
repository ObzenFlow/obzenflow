// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Generated scalar AI inference leaf.

use super::ai_effect::{invoke_generated_chat, GeneratedChatInvocationError};
use super::stage_descriptor::{
    EffectPolicyAttachment, EffectfulTransformDescriptor, StageDescriptor,
};
use super::typing::{wrap_typed_descriptor, StageTypingMetadata, TypeHint};
use async_trait::async_trait;
use obzenflow_adapters::ai::ChatCompletion;
use obzenflow_adapters::middleware::MiddlewareFactory;
use obzenflow_core::ai::{AiInferenceRole, ChatBindingContract};
use obzenflow_core::event::{StageFatalCode, StageFatalReason};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    Effect, EffectDeclaration, EffectError, Effects, StageCompletion,
};
use obzenflow_runtime::stages::common::handler_error::{HandlerError, StageFatal};
use obzenflow_runtime::stages::common::handlers::EffectfulTransformHandler;
use std::fmt;
use std::marker::PhantomData;
use std::num::NonZeroU64;
use std::sync::Arc;

pub(crate) const INFERENCE_CHAT_COMPLETION_LABEL: &str = "inference.chat_completion";

struct GeneratedInferenceHandler<Input, Out, Role> {
    role: Arc<Role>,
    chat_binding: ChatBindingContract,
    _types: PhantomData<fn() -> (Input, Out)>,
}

impl<Input, Out, Role> Clone for GeneratedInferenceHandler<Input, Out, Role> {
    fn clone(&self) -> Self {
        Self {
            role: self.role.clone(),
            chat_binding: self.chat_binding.clone(),
            _types: PhantomData,
        }
    }
}

impl<Input, Out, Role> fmt::Debug for GeneratedInferenceHandler<Input, Out, Role> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GeneratedInferenceHandler")
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

fn effect_handler_error(error: EffectError) -> HandlerError {
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
        EffectError::Journal(message) => fatal(
            StageFatalCode::Journal,
            StageFatalReason::JournalFailure,
            message,
        ),
        EffectError::DependencyFailed { message, .. }
        | EffectError::BoundaryRejected { message, .. }
        | EffectError::RecoveryAbandoned { message, .. }
        | EffectError::RecordedFailure {
            error_message: message,
            ..
        } => HandlerError::Remote(message),
        other => HandlerError::Other(other.to_string()),
    }
}

#[async_trait]
impl<Input, Out, Role> EffectfulTransformHandler for GeneratedInferenceHandler<Input, Out, Role>
where
    Input: TypedPayload + Clone + Send + Sync + 'static,
    Out: TypedPayload + Clone + Send + Sync + 'static,
    Role: AiInferenceRole<Input, Out>,
{
    type Input = Input;
    type Output = Out;
    type AllowedEffects = obzenflow_runtime::effect_set![ChatCompletion];

    async fn process(
        &self,
        input: Self::Input,
        fx: &mut Effects<Self::Output, Self::AllowedEffects>,
    ) -> Result<StageCompletion<Self::Output>, HandlerError> {
        let request = self
            .role
            .prepare(&input)
            .map_err(|error| HandlerError::Domain(format!("{error:?}")))?;
        let reply = invoke_generated_chat(
            fx,
            &self.chat_binding,
            &request,
            INFERENCE_CHAT_COMPLETION_LABEL,
        )
        .await
        .map_err(|error| match error {
            GeneratedChatInvocationError::Build(error) => {
                HandlerError::Validation(error.to_string())
            }
            GeneratedChatInvocationError::Effect(error) => effect_handler_error(error),
        })?;
        let output = self
            .role
            .interpret(input, request, reply)
            .map_err(|error| HandlerError::Domain(format!("{error:?}")))?;
        fx.emit(output).await.map_err(effect_handler_error)?;
        fx.complete().map_err(effect_handler_error)
    }

    fn stage_logic_version(&self) -> &str {
        Role::LOGIC_VERSION
    }
}

/// Macro-only constructor for the scalar AI leaf.
#[doc(hidden)]
pub fn generated_inference<Input, Out, Role>(
    name: impl Into<String>,
    role: Role,
    chat_binding: ChatBindingContract,
    policy: Box<dyn MiddlewareFactory>,
) -> Box<dyn StageDescriptor>
where
    Input: TypedPayload + Clone + Send + Sync + 'static,
    Out: TypedPayload + Clone + Send + Sync + 'static,
    Role: AiInferenceRole<Input, Out>,
{
    let handler = GeneratedInferenceHandler::<Input, Out, Role> {
        role: Arc::new(role),
        chat_binding,
        _types: PhantomData,
    };
    let direct_bound = NonZeroU64::MIN.saturating_add(2);
    let descriptor = EffectfulTransformDescriptor::generated_for_surface::<Input>(
        "inference!",
        "stage",
        name,
        handler,
        vec![EffectDeclaration::at_least_once::<ChatCompletion>()],
        vec![EffectPolicyAttachment {
            effect_type: ChatCompletion::EFFECT_TYPE,
            factories: vec![policy],
        }],
        direct_bound,
    );
    wrap_typed_descriptor(
        Box::new(descriptor),
        StageTypingMetadata::transform(
            TypeHint::exact_payload::<Input>(),
            TypeHint::exact_payload::<Out>(),
            false,
            None,
        ),
    )
}
