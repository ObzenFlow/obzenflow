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
use std::process::Command;

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

#[test]
fn unselected_wrong_input_sink_fails_an_ordinary_build() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let fixture = include_str!("compile_fail/hn_config_selected_sink_wrong_input.rs");
    let temp = tempfile::tempdir().expect("temporary compile-fail crate");
    let source_dir = temp.path().join("src");
    std::fs::create_dir(&source_dir).expect("compile-fail source directory is created");
    std::fs::write(source_dir.join("main.rs"), fixture)
        .expect("compile-fail fixture source is written");
    let manifest = format!(
        r#"[package]
name = "hn-config-selected-sink-wrong-input"
version = "0.0.0"
edition = "2021"

[dependencies]
obzenflow = {{ path = {root:?} }}
obzenflow_core = {{ path = {core:?} }}
obzenflow_dsl = {{ path = {dsl:?} }}
"#,
        root = root,
        core = root.join("crates/obzenflow_core"),
        dsl = root.join("crates/obzenflow_dsl"),
    );
    let manifest_path = temp.path().join("Cargo.toml");
    std::fs::write(&manifest_path, manifest).expect("compile-fail manifest is written");
    std::fs::copy(root.join("Cargo.lock"), temp.path().join("Cargo.lock"))
        .expect("workspace lockfile is copied into the compile-fail crate");

    let output = Command::new("cargo")
        .args([
            "check",
            "--offline",
            "--quiet",
            "--manifest-path",
            manifest_path
                .to_str()
                .expect("temporary manifest path is UTF-8"),
            "--target-dir",
            root.join("target")
                .to_str()
                .expect("workspace target path is UTF-8"),
        ])
        .env("CARGO_TERM_COLOR", "never")
        .output()
        .expect("ordinary cargo check runs for the compile-fail witness");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "the unselected wrong-input arm unexpectedly compiled"
    );
    assert!(
        stderr.contains("AiMapReduceChunkFailed")
            && stderr.contains("AiMapReducePlanningManifest")
            && stderr.contains("SinkInputMatchesArrow"),
        "the build must fail at the sink input proof boundary; stderr:\n{stderr}"
    );
}
