// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#![cfg(feature = "postgres")]

#[allow(dead_code)]
#[path = "../examples/hn_ai_digest_demo/config.rs"]
mod config;
#[allow(dead_code)]
#[path = "../examples/hn_ai_digest_demo/mock_server.rs"]
mod mock_server;

use config::{resolve_digest_configuration, DigestOutput, HnDigestPostgresConfig};
use obzenflow::sinks::postgres::{PostgresConnection, PostgresTransport};
use std::cell::Cell;

fn postgres_config() -> HnDigestPostgresConfig {
    let connection = PostgresConnection::from_url(
        "postgres://obzenflow:sentinel@localhost/obzenflow?sslmode=verify-full",
        PostgresTransport::VerifiedTls,
    )
    .expect("the cold verified-TLS fixture URL is valid without I/O");
    HnDigestPostgresConfig {
        connection,
        schema: "hn_digest_fixture".to_string(),
    }
}

#[test]
fn console_is_the_default_and_does_not_resolve_postgres_configuration() {
    for configured_output in [None, Some("console")] {
        let postgres_loads = Cell::new(0);
        let (output, postgres) = resolve_digest_configuration(configured_output, || {
            postgres_loads.set(postgres_loads.get() + 1);
            Ok(postgres_config())
        })
        .expect("console configuration resolves");

        assert_eq!(output, DigestOutput::Console);
        assert_eq!(output.as_str(), "console");
        assert!(postgres.is_none());
        assert_eq!(postgres_loads.get(), 0);
    }
}

#[test]
fn postgres_selection_resolves_its_cold_configuration_once() {
    let postgres_loads = Cell::new(0);
    let (output, postgres) = resolve_digest_configuration(Some("postgres"), || {
        postgres_loads.set(postgres_loads.get() + 1);
        Ok(postgres_config())
    })
    .expect("PostgreSQL configuration resolves");

    assert_eq!(output, DigestOutput::Postgres);
    assert_eq!(output.as_str(), "postgres");
    assert_eq!(postgres_loads.get(), 1);
    assert_eq!(
        postgres.expect("selected PostgreSQL config").schema,
        "hn_digest_fixture"
    );
}

#[test]
fn unknown_output_has_the_locked_diagnostic_without_resolving_postgres() {
    let postgres_loads = Cell::new(0);
    let error = resolve_digest_configuration(Some("Postgres"), || {
        postgres_loads.set(postgres_loads.get() + 1);
        Ok(postgres_config())
    })
    .expect_err("selector values are exact lowercase");

    assert_eq!(
        error.to_string(),
        "HN_DIGEST_OUTPUT must be \"console\" or \"postgres\"; got \"Postgres\""
    );
    assert_eq!(postgres_loads.get(), 0);
}
