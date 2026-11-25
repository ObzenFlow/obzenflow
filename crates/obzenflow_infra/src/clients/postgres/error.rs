// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_runtime::effects::EffectError;

/// Why a statement didn't complete.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostgresClientError {
    /// No connection was made, so the statement never reached the server.
    #[error("connection failed: {0}")]
    Connection(String),

    #[error("statement timed out: {0}")]
    Timeout(String),

    #[error("transport error: {0}")]
    Transport(String),

    /// The server refused the statement, with the SQLSTATE it named.
    #[error("PostgreSQL refused the statement{}: {message}", .sqlstate.as_deref().map(|code| format!(" ({code})")).unwrap_or_default())]
    Refused {
        sqlstate: Option<String>,
        message: String,
    },

    /// A bound value could not be encoded, so nothing was sent.
    #[error("the statement could not be encoded: {0}")]
    Unencodable(String),
}

/// The effect error for a statement that failed. A connection that never
/// opened is a retryable refusal, a definite no, whose failure source is the
/// transport, `postgres`.
pub fn unsent(error: PostgresClientError) -> EffectError {
    match error {
        PostgresClientError::Connection(message) => {
            EffectError::unavailable("postgres", "connect_failed", message)
        }
        PostgresClientError::Timeout(message) => EffectError::Timeout(message),
        PostgresClientError::Transport(message) => EffectError::Transport(message),
        PostgresClientError::Refused { .. } => EffectError::Transport(error.to_string()),
        PostgresClientError::Unencodable(_) => EffectError::Permanent(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn refused_connection_names_transport() {
        assert!(matches!(
            unsent(PostgresClientError::Connection("refused".into())),
            EffectError::DependencyFailed { failure_source, code, .. }
                if failure_source.as_str() == "postgres" && code.as_str() == "connect_failed"
        ));
    }

    #[test]
    fn unencodable_value_returns_permanent_error() {
        assert!(matches!(
            unsent(PostgresClientError::Unencodable("bad".into())),
            EffectError::Permanent(_)
        ));
    }
}
