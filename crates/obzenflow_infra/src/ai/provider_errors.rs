// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_adapters::effects::http::{unclaimed, unreadable, unsent_from};
use obzenflow_core::ai::AiProviderError;
use obzenflow_core::event::RetryDisposition;
use obzenflow_core::http_client::{HttpClientError, HttpResponse};
use obzenflow_runtime::effects::EffectError;
use std::fmt::Display;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub(crate) struct ModelCalls {
    pub(crate) source: &'static str,
    pub(crate) section: &'static str,
    pub(crate) request_timeout: Duration,
}

impl ModelCalls {
    pub(crate) fn unsent(&self, error: HttpClientError) -> AiProviderError {
        match unsent_from(self.source, error) {
            EffectError::Timeout(_) => AiProviderError::Timeout {
                message: format!(
                    "no reply within {} ms; request_timeout_ms under [{}] sets this",
                    self.request_timeout.as_millis(),
                    self.section
                ),
            },
            error => provider_error(error, None),
        }
    }
}

pub(crate) fn unclaimed_status(response: &HttpResponse) -> AiProviderError {
    provider_error(unclaimed(response), Some(response))
}

pub(crate) fn unreadable_body(error: impl Display) -> AiProviderError {
    provider_error(unreadable(error), None)
}

fn provider_error(error: EffectError, response: Option<&HttpResponse>) -> AiProviderError {
    match error {
        EffectError::Timeout(message) => AiProviderError::Timeout { message },
        EffectError::Transport(message) => AiProviderError::Remote { message },
        EffectError::RateLimited {
            message,
            retry_after,
        } => AiProviderError::RateLimited {
            message,
            retry_after,
        },
        EffectError::Validation(message) => AiProviderError::InvalidRequest { message },
        EffectError::Permanent(message)
            if response.is_some_and(|response| matches!(response.status, 401 | 403)) =>
        {
            AiProviderError::Auth { message }
        }
        EffectError::DependencyFailed {
            message,
            retry: RetryDisposition::Retryable,
            ..
        } => AiProviderError::ConnectFailed { message },
        error => AiProviderError::Other {
            message: error.semantic_reason().into_owned(),
        },
    }
}
