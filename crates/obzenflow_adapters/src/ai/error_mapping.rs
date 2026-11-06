// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::effects::http::CONNECT_FAILED;
use obzenflow_core::ai::{AiProviderError, AiResolutionError};
use obzenflow_core::event::{EffectFailureCode, EffectFailureSource, RetryDisposition};
use obzenflow_runtime::effects::{EffectError, EffectPortResolutionError};

pub(crate) fn ai_provider_error_to_effect_error(
    error: AiProviderError,
    failure_source: &'static str,
) -> EffectError {
    match error {
        AiProviderError::RateLimited {
            message,
            retry_after,
        } => EffectError::RateLimited {
            message,
            retry_after,
        },
        AiProviderError::ConnectFailed { message } => {
            EffectError::unavailable(failure_source, CONNECT_FAILED, message)
        }
        AiProviderError::Timeout { message } => dependency(failure_source, "timeout", message),
        AiProviderError::Remote { message } => dependency(failure_source, "remote", message),
        AiProviderError::Auth { message } => dependency(failure_source, "authentication", message),
        AiProviderError::InvalidRequest { message } => {
            dependency(failure_source, "invalid_request", message)
        }
        AiProviderError::Unsupported { message } => {
            dependency(failure_source, "unsupported", message)
        }
        AiProviderError::Other { message } => dependency(failure_source, "other", message),
    }
}

pub(crate) fn resolution_error_to_port(error: AiResolutionError) -> EffectPortResolutionError {
    match error {
        AiResolutionError::CredentialUnavailable { .. } => {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn rate_limit_keeps_provider_floor() {
        let error = ai_provider_error_to_effect_error(
            AiProviderError::RateLimited {
                message: "429 Too Many Requests".to_string(),
                retry_after: Duration::from_secs(3),
            },
            "chat_model",
        );
        assert!(matches!(
            error,
            EffectError::RateLimited { retry_after, .. } if retry_after == Duration::from_secs(3)
        ));
        assert!(error.is_definite_no());
    }

    #[test]
    fn refused_connection_returns_definite_no() {
        let error = ai_provider_error_to_effect_error(
            AiProviderError::ConnectFailed {
                message: "connection refused".to_string(),
            },
            "chat_model",
        );
        assert!(error.is_definite_no());
        assert!(matches!(
            error,
            EffectError::DependencyFailed { failure_source, code, .. }
                if failure_source.as_str() == "chat_model" && code.as_str() == CONNECT_FAILED
        ));
    }

    #[test]
    fn timeout_stays_maybe() {
        let error = ai_provider_error_to_effect_error(
            AiProviderError::Timeout {
                message: "no reply".to_string(),
            },
            "chat_model",
        );
        assert!(!error.is_definite_no());
    }
}
