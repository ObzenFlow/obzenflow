// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::AiClientError;
use obzenflow_runtime_services::stages::common::handler_error::HandlerError;

/// Map a core `AiClientError` to runtime `HandlerError` according to FLOWIP-086d.
pub fn ai_client_error_to_handler_error(err: AiClientError) -> HandlerError {
    ai_client_error_to_handler_error_with_context(err, None)
}

/// Map a core `AiClientError` to runtime `HandlerError`, optionally prefixing context.
pub fn ai_client_error_to_handler_error_with_context(
    err: AiClientError,
    context: Option<&str>,
) -> HandlerError {
    let prefix = context.unwrap_or("");
    let wrap = |message: String| {
        if prefix.is_empty() {
            message
        } else {
            format!("{prefix}: {message}")
        }
    };

    match err {
        AiClientError::Timeout { message } => HandlerError::Timeout(wrap(message)),
        AiClientError::Remote { message } => HandlerError::Remote(wrap(message)),
        AiClientError::RateLimited {
            message,
            retry_after,
        } => {
            let msg = match retry_after {
                Some(wait) => format!(
                    "rate_limited: {} (retry_after_ms={})",
                    message,
                    wait.as_millis()
                ),
                None => format!("rate_limited: {message}"),
            };
            HandlerError::Remote(wrap(msg))
        }
        AiClientError::Auth { message } => HandlerError::Domain(wrap(format!("auth: {message}"))),
        AiClientError::InvalidRequest { message } => {
            HandlerError::Validation(wrap(format!("invalid_request: {message}")))
        }
        AiClientError::Unsupported { message } => {
            HandlerError::Domain(wrap(format!("unsupported: {message}")))
        }
        AiClientError::Other { message } => HandlerError::Other(wrap(message)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn maps_rate_limited_to_remote_with_context() {
        let out = ai_client_error_to_handler_error_with_context(
            AiClientError::RateLimited {
                message: "429 from provider".to_string(),
                retry_after: Some(Duration::from_secs(2)),
            },
            Some("chat_call"),
        );

        match out {
            HandlerError::Remote(message) => {
                assert!(message.contains("chat_call"));
                assert!(message.contains("rate_limited"));
                assert!(message.contains("retry_after_ms=2000"));
            }
            other => panic!("expected Remote, got {other:?}"),
        }
    }

    #[test]
    fn maps_auth_to_domain() {
        let out = ai_client_error_to_handler_error(AiClientError::Auth {
            message: "bad api key".to_string(),
        });

        match out {
            HandlerError::Domain(message) => assert!(message.contains("auth")),
            other => panic!("expected Domain, got {other:?}"),
        }
    }
}
