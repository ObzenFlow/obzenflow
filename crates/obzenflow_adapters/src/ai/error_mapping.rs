// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::{AiClientError, AiModelProviderError};
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_runtime::effects::{EffectError, EffectPortResolutionError, EffectPortSlot};

pub(crate) fn ai_client_error_to_effect_error<P, M>(
    error: AiClientError,
    port: EffectPortSlot<P, M>,
    failure_source: &'static str,
) -> EffectError
where
    P: ?Sized + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    match error {
        AiClientError::TargetMismatch { .. } => EffectError::target_invariant_violation(port),
        AiClientError::Timeout { message } => dependency(failure_source, "timeout", message),
        AiClientError::Remote { message } => dependency(failure_source, "remote", message),
        AiClientError::RateLimited { message, .. } => {
            dependency(failure_source, "rate_limited", message)
        }
        AiClientError::Auth { message } => dependency(failure_source, "authentication", message),
        AiClientError::InvalidRequest { message } => {
            dependency(failure_source, "invalid_request", message)
        }
        AiClientError::Unsupported { message } => {
            dependency(failure_source, "unsupported", message)
        }
        AiClientError::Other { message } => dependency(failure_source, "other", message),
    }
}

pub(crate) fn provider_error_to_resolution(error: AiModelProviderError) -> EffectPortResolutionError {
    match error {
        AiModelProviderError::CredentialUnavailable { .. } => {
            EffectPortResolutionError::CredentialUnavailable
        }
        _ => EffectPortResolutionError::ClientConstructionFailed,
    }
}

fn dependency(source: &'static str, code: &'static str, message: String) -> EffectError {
    EffectError::DependencyFailed {
        failure_source: EffectFailureSource::new(source),
        code: EffectFailureCode::new(code),
        message,
        retry: RetryDisposition::NotRetryable,
    }
}
