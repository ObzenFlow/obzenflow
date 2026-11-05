// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The framework's one rule for an HTTP call that failed: a send that got no
//! response, a body that would not parse, and a status the system did not claim.

use http::StatusCode;
use obzenflow_core::http_client::{HttpClientError, HttpResponse};
use obzenflow_runtime::effects::{EffectError, SystemName};
use std::fmt::Display;
use std::time::Duration;

pub(crate) const CONNECT_FAILED: &str = "connect_failed";

const BODY_PREFIX_BYTES: usize = 512;

/// The effect error for a send that got no response. A refused connection
/// names the system, since it certainly did not act.
pub fn unsent<S: SystemName>(error: HttpClientError) -> EffectError {
    unsent_from(S::NAME, error)
}

/// [`unsent`] for a sender that names its source itself.
pub fn unsent_from(source: &str, error: HttpClientError) -> EffectError {
    match error {
        HttpClientError::Timeout(message) => EffectError::Timeout(message),
        HttpClientError::Connection(message) => {
            EffectError::unavailable(source, CONNECT_FAILED, message)
        }
        HttpClientError::Transport(message) => EffectError::Transport(message),
        HttpClientError::Cancelled => EffectError::Transport(error.to_string()),
        HttpClientError::Unencodable(_) => EffectError::Permanent(error.to_string()),
    }
}

/// The effect error for a response whose body would not parse.
pub fn unreadable(error: impl Display) -> EffectError {
    EffectError::Permanent(error.to_string())
}

/// The effect error for a status the system did not claim. A 429's
/// `Retry-After`, in seconds, is the retry's floor.
pub fn unclaimed(response: &HttpResponse) -> EffectError {
    let message = unclaimed_message(response);
    match response.status {
        429 => EffectError::RateLimited {
            message,
            retry_after: retry_after(response),
        },
        408 => EffectError::Timeout(message),
        409 | 500..=599 => EffectError::Transport(message),
        400 | 422 => EffectError::Validation(message),
        _ => EffectError::Permanent(message),
    }
}

fn retry_after(response: &HttpResponse) -> Duration {
    response
        .header("retry-after")
        .and_then(|value| value.trim().parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or(Duration::ZERO)
}

fn unclaimed_message(response: &HttpResponse) -> String {
    let mut message = response.status.to_string();
    if let Some(reason) = StatusCode::from_u16(response.status)
        .ok()
        .and_then(|status| status.canonical_reason())
    {
        message.push(' ');
        message.push_str(reason);
    }
    if let Some(request_id) = response
        .header("request-id")
        .or_else(|| response.header("x-request-id"))
    {
        message.push_str("; request ");
        message.push_str(request_id);
    }
    let body = &response.body[..response.body.len().min(BODY_PREFIX_BYTES)];
    if !body.is_empty() {
        message.push_str("; ");
        message.push_str(&String::from_utf8_lossy(body));
    }
    message
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::event::status::processing_status::ErrorKind;
    use obzenflow_core::event::RetryDisposition;
    use obzenflow_core::http_client::HeaderMap;

    struct Stripe;

    impl SystemName for Stripe {
        const NAME: &'static str = "stripe";
    }

    fn response(status: u16, headers: &[(&'static str, &'static str)], body: &str) -> HttpResponse {
        let mut map = HeaderMap::new();
        for (name, value) in headers {
            map.insert(*name, value.parse().unwrap());
        }
        HttpResponse::new(status, map, body.to_string())
    }

    #[test]
    fn unsent_classifies_each_failure() {
        assert!(matches!(
            unsent::<Stripe>(HttpClientError::Timeout("slow".into())),
            EffectError::Timeout(message) if message == "slow"
        ));
        assert!(matches!(
            unsent::<Stripe>(HttpClientError::Connection("refused".into())),
            EffectError::DependencyFailed { failure_source, code, retry: RetryDisposition::Retryable, .. }
                if failure_source.as_str() == "stripe" && code.as_str() == CONNECT_FAILED
        ));
        assert!(matches!(
            unsent::<Stripe>(HttpClientError::Transport("reset".into())),
            EffectError::Transport(_)
        ));
        assert!(matches!(
            unsent::<Stripe>(HttpClientError::Cancelled),
            EffectError::Transport(_)
        ));
        assert!(matches!(
            unsent::<Stripe>(HttpClientError::Unencodable("bad header".into())),
            EffectError::Permanent(message)
                if message == "the request could not be encoded: bad header"
        ));
    }

    #[test]
    fn unreadable_returns_permanent() {
        let error = serde_json::from_str::<serde_json::Value>("<html>").unwrap_err();
        assert!(matches!(unreadable(error), EffectError::Permanent(_)));
    }

    #[test]
    fn unclaimed_classifies_each_status() {
        for (status, expected) in [
            (408, ErrorKind::Timeout),
            (409, ErrorKind::Remote),
            (500, ErrorKind::Remote),
            (502, ErrorKind::Remote),
            (503, ErrorKind::Remote),
            (400, ErrorKind::Validation),
            (422, ErrorKind::Validation),
            (401, ErrorKind::PermanentFailure),
            (403, ErrorKind::PermanentFailure),
            (404, ErrorKind::PermanentFailure),
            (204, ErrorKind::PermanentFailure),
            (302, ErrorKind::PermanentFailure),
            (429, ErrorKind::RateLimited),
        ] {
            let error = unclaimed(&response(status, &[], ""));
            assert_eq!(error.error_kind(), expected, "{status}");
        }
        let unavailable = unclaimed(&response(503, &[("retry-after", "3")], ""));
        assert!(matches!(unavailable, EffectError::Transport(_)));
    }

    #[test]
    fn rate_limit_floor_reads_retry_after_seconds() {
        for (header, floor) in [
            (Some("3"), Duration::from_secs(3)),
            (None, Duration::ZERO),
            (Some("Wed, 21 Oct 2015 07:28:00 GMT"), Duration::ZERO),
            (Some("soon"), Duration::ZERO),
        ] {
            let headers: Vec<(&'static str, &'static str)> = header
                .map(|value| ("retry-after", value))
                .into_iter()
                .collect();
            let error = unclaimed(&response(429, &headers, ""));
            assert!(
                matches!(error, EffectError::RateLimited { retry_after, .. } if retry_after == floor),
                "{header:?}"
            );
        }
    }

    #[test]
    fn unclaimed_message_bounds_body() {
        let body = "x".repeat(BODY_PREFIX_BYTES + 100);
        for id_header in ["request-id", "x-request-id"] {
            let error = unclaimed(&response(
                402,
                &[
                    (id_header, "req_123"),
                    ("authorization", "Bearer sk_test_secret"),
                ],
                &body,
            ));
            let EffectError::Permanent(message) = error else {
                panic!("a 402 the system did not claim is permanent");
            };
            assert!(message.starts_with("402 Payment Required; request req_123; "));
            assert!(message.ends_with(&"x".repeat(BODY_PREFIX_BYTES)));
            assert!(!message.contains(&"x".repeat(BODY_PREFIX_BYTES + 1)));
            assert!(!message.contains("sk_test_secret"));
        }
    }
}
