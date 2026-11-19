// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The framework's one rule for a PostgreSQL statement that failed.

use obzenflow_core::postgres_client::PostgresClientError;
use obzenflow_runtime::effects::{EffectError, SystemName};

use super::http::CONNECT_FAILED;

/// The effect error for a statement that failed. A connection that never
/// opened is the system's retryable refusal, a definite no.
pub fn unsent<S: SystemName>(error: PostgresClientError) -> EffectError {
    match error {
        PostgresClientError::Connection(message) => {
            EffectError::unavailable(S::NAME, CONNECT_FAILED, message)
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

    struct Ledger;

    impl SystemName for Ledger {
        const NAME: &'static str = "ledger";
    }

    #[test]
    fn refused_connection_names_system() {
        assert!(matches!(
            unsent::<Ledger>(PostgresClientError::Connection("refused".into())),
            EffectError::DependencyFailed { failure_source, code, .. }
                if failure_source.as_str() == "ledger" && code.as_str() == CONNECT_FAILED
        ));
    }

    #[test]
    fn unencodable_value_returns_permanent_error() {
        assert!(matches!(
            unsent::<Ledger>(PostgresClientError::Unencodable("bad".into())),
            EffectError::Permanent(_)
        ));
    }
}
