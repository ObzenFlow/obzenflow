// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::{anyhow, Result};
use obzenflow::sinks::postgres::PostgresTransport;

const POSTGRES_TRANSPORT_ENV: &str = "OBZENFLOW_POSTGRES_TRANSPORT";
const VERIFIED_TLS: &str = "verified-tls";
const EXTERNALLY_PROTECTED_PLAINTEXT: &str = "externally-protected-plaintext";

pub(crate) fn from_environment() -> Result<PostgresTransport> {
    match std::env::var(POSTGRES_TRANSPORT_ENV) {
        Ok(value) => parse(Some(&value)),
        Err(std::env::VarError::NotPresent) => parse(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            Err(anyhow!("{POSTGRES_TRANSPORT_ENV} must be valid Unicode"))
        }
    }
}

fn parse(value: Option<&str>) -> Result<PostgresTransport> {
    match value {
        None | Some(VERIFIED_TLS) => Ok(PostgresTransport::VerifiedTls),
        Some(EXTERNALLY_PROTECTED_PLAINTEXT) => {
            Ok(PostgresTransport::ExternallyProtectedPlaintext)
        }
        Some(_) => Err(anyhow!(
            "{POSTGRES_TRANSPORT_ENV} must be `{VERIFIED_TLS}` or `{EXTERNALLY_PROTECTED_PLAINTEXT}`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn absent_transport_defaults_to_verified_tls() {
        assert_eq!(
            parse(None).expect("default transport"),
            PostgresTransport::VerifiedTls
        );
    }

    #[test]
    fn transport_values_are_closed_and_explicit() {
        assert_eq!(
            parse(Some(VERIFIED_TLS)).expect("verified transport"),
            PostgresTransport::VerifiedTls
        );
        assert_eq!(
            parse(Some(EXTERNALLY_PROTECTED_PLAINTEXT)).expect("loopback transport"),
            PostgresTransport::ExternallyProtectedPlaintext
        );
        assert!(parse(Some("prefer")).is_err());
    }
}
