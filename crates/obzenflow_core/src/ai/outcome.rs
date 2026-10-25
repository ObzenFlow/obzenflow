// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use serde::{Deserialize, Serialize};
use std::fmt;

/// Closed provider-facing failure taxonomy of one AI call.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AiProviderFailureKind {
    Timeout,
    Remote,
    RateLimited,
    Authentication,
    InvalidRequest,
    Unsupported,
    Other,
}

impl AiProviderFailureKind {
    /// The failure kind recorded under a provider failure code.
    pub fn from_code(code: &str) -> Self {
        match code {
            "timeout" => Self::Timeout,
            "remote" | "transport" => Self::Remote,
            "rate_limited" => Self::RateLimited,
            "authentication" | "auth" => Self::Authentication,
            "invalid_request" | "validation" => Self::InvalidRequest,
            "unsupported" => Self::Unsupported,
            _ => Self::Other,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::Remote => "remote",
            Self::RateLimited => "rate_limited",
            Self::Authentication => "authentication",
            Self::InvalidRequest => "invalid_request",
            Self::Unsupported => "unsupported",
            Self::Other => "other",
        }
    }
}

impl fmt::Display for AiProviderFailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Why one AI call produced no reply.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AiUnavailableCause {
    /// The provider failed the call.
    Provider { failure: AiProviderFailureKind },
    /// A resilience control refused the call before it reached the provider.
    BoundaryRejected { source: String, code: String },
    /// Recovery stopped repeating an attempt whose outcome stayed unknown.
    RecoveryAbandoned {
        last_started_attempt: u32,
        source: String,
        code: String,
    },
}

impl fmt::Display for AiUnavailableCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Provider { failure } => write!(formatter, "provider {failure}"),
            Self::BoundaryRejected { source, code } => {
                write!(formatter, "rejected by {source} ({code})")
            }
            Self::RecoveryAbandoned {
                last_started_attempt,
                source,
                code,
            } => write!(
                formatter,
                "recovery abandoned after attempt {last_started_attempt} by {source} ({code})"
            ),
        }
    }
}

/// An AI call that produced no reply, with its cause and the reason the
/// provider or control gave. Live and replayed runs report the same value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AiUnavailable {
    pub cause: AiUnavailableCause,
    pub reason: String,
}

impl AiUnavailable {
    pub fn new(cause: AiUnavailableCause, reason: impl Into<String>) -> Self {
        Self {
            cause,
            reason: reason.into(),
        }
    }
}

impl fmt::Display for AiUnavailable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "AI call unavailable ({}): {}",
            self.cause, self.reason
        )
    }
}

impl std::error::Error for AiUnavailable {}

/// The reply a role sees for one AI call, or the typed reason no reply exists.
#[derive(Debug, Clone, PartialEq)]
pub enum AiOutcome<R> {
    Replied(R),
    Unavailable(AiUnavailable),
}

impl<R> AiOutcome<R> {
    /// The reply, or the unavailability for a role to propagate with `?`.
    pub fn replied(self) -> Result<R, AiUnavailable> {
        match self {
            Self::Replied(reply) => Ok(reply),
            Self::Unavailable(unavailable) => Err(unavailable),
        }
    }

    pub fn is_replied(&self) -> bool {
        matches!(self, Self::Replied(_))
    }

    pub fn unavailable(&self) -> Option<&AiUnavailable> {
        match self {
            Self::Replied(_) => None,
            Self::Unavailable(unavailable) => Some(unavailable),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn unavailable_wire_shape_tags_its_cause() {
        let unavailable = AiUnavailable::new(
            AiUnavailableCause::Provider {
                failure: AiProviderFailureKind::Authentication,
            },
            "token revoked",
        );
        let value = serde_json::to_value(&unavailable).unwrap();
        assert_eq!(
            value,
            json!({
                "cause": {"kind": "provider", "failure": "authentication"},
                "reason": "token revoked"
            })
        );
        assert_eq!(
            serde_json::from_value::<AiUnavailable>(value).unwrap(),
            unavailable
        );
    }

    #[test]
    fn replied_returns_the_unavailability_for_propagation() {
        let rejected = AiUnavailable::new(
            AiUnavailableCause::BoundaryRejected {
                source: "circuit_breaker".to_string(),
                code: "open".to_string(),
            },
            "breaker open",
        );
        let outcome: AiOutcome<String> = AiOutcome::Unavailable(rejected.clone());
        assert_eq!(outcome.unavailable(), Some(&rejected));
        assert_eq!(outcome.replied(), Err(rejected));
        assert_eq!(
            AiOutcome::Replied("reply".to_string()).replied(),
            Ok("reply".to_string())
        );
    }

    #[test]
    fn provider_codes_map_to_closed_failure_kinds() {
        assert_eq!(
            AiProviderFailureKind::from_code("rate_limited"),
            AiProviderFailureKind::RateLimited
        );
        assert_eq!(
            AiProviderFailureKind::from_code("transport"),
            AiProviderFailureKind::Remote
        );
        assert_eq!(
            AiProviderFailureKind::from_code("anything"),
            AiProviderFailureKind::Other
        );
    }
}
