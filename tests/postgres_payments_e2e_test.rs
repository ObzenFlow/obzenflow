// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Black-box verification for the PostgreSQL payments application fixture.
//!
//! The fixture runs in a separate process through the public application API.
//! This test owns destination inspection, archive-redelivery assertions, and
//! evidence hygiene; the fixture has no database-inspection code.

#[path = "test_support/postgres_payments/mod.rs"]
mod payments;
#[path = "support/postgres.rs"]
mod postgres_support;

use postgres_support::{pool, required_env};
use serde_json::Value;
use sqlx::PgPool;
use std::{
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
};

const FIXTURE_ARGS_ENV: &str = "OBZENFLOW_POSTGRES_TEST_PAYMENTS_ARGS";
const FORBIDDEN_DURABLE_KEYS: &[&str] = &[
    "certificate",
    "certificate_path",
    "compose_project",
    "connection_url",
    "container_id",
    "health",
    "identifier_limit",
    "max_identifier_length",
    "port",
    "postgres_url",
    "sql",
    "sslrootcert",
    "statement",
    "statement_fingerprint",
    "transport",
    "trust_store",
];
const FORBIDDEN_DURABLE_DETAILS: &[&str] = &[
    "VerifiedTls",
    "ExternallyProtectedPlaintext",
    "sslrootcert",
    "max_identifier_length",
    "statement_fingerprint",
];

#[test]
fn payments_application_defers_managed_connection_resolution() {
    let source = include_str!("test_support/postgres_payments/mod.rs");
    assert!(source.contains("PostgresConnection::deferred_from_env("));
    assert!(!source.contains("PostgresConnection::from_env("));
}

#[tokio::test(flavor = "multi_thread")]
async fn payments_application_converges_after_verified_archive_redelivery() {
    let pool = pool().await;
    let schema = required_env("OBZENFLOW_POSTGRES_EXAMPLE_SCHEMA");
    let journal_directory = tempfile::tempdir().expect("create application journal directory");
    let journal_root = journal_directory.path().join("journals");

    let live = run_payments(&schema, &journal_root, &[]);
    assert_success("live payments application", &live);
    assert_payment_rows(&pool, &schema).await;

    let live_runs = run_directories(&journal_root);
    let [live_run] = live_runs.as_slice() else {
        panic!(
            "the live application must create one run archive, found {}",
            live_runs.len()
        );
    };

    let replay = run_payments(
        &schema,
        &journal_root,
        &[
            OsString::from("--replay-from"),
            live_run.as_os_str().to_owned(),
            OsString::from("--verify"),
        ],
    );
    assert_success("payments application archive redelivery", &replay);
    assert_payment_rows(&pool, &schema).await;

    let replay_runs = run_directories(&journal_root);
    assert_eq!(
        replay_runs.len(),
        2,
        "archive redelivery preserves the live archive and creates one treatment archive"
    );
    assert!(replay_runs.contains(live_run));
    assert_durable_evidence_is_redacted(&journal_root);
}

// The same entry point is launched by the development-service lifecycle test.
#[test]
#[ignore = "application subprocess; launched by PostgreSQL acceptance tests"]
fn payments_application_process() {
    let args = match env::var(FIXTURE_ARGS_ENV) {
        Ok(args) => serde_json::from_str(&args).expect("decode application arguments"),
        Err(env::VarError::NotPresent) => vec![OsString::from("postgres-payments")],
        Err(error) => panic!("read application arguments: {error}"),
    };
    payments::run(args).expect("payments application completes");
}

#[test]
fn payments_application_subprocess_accepts_its_own_cli_arguments() {
    let journals = tempfile::tempdir().expect("temporary journal root");
    let output = run_payments(
        "obzenflow_example",
        journals.path(),
        &[OsString::from("--help")],
    );
    // FlowApplication currently reports CLI help through its configuration
    // error path. Check the rendered application help, independently of that
    // exit convention.
    let help = format!(
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        help.contains("Usage: postgres-payments [OPTIONS]") && help.contains("--replay-from"),
        "the child must parse application arguments rather than test-harness arguments: {help}"
    );
    assert!(!journals.path().join("flows").exists());
}

fn run_payments(schema: &str, journals: &Path, args: &[OsString]) -> Output {
    let mut application_args = vec![OsString::from("postgres-payments")];
    application_args.extend_from_slice(args);
    Command::new(env::current_exe().expect("current test executable"))
        .args([
            "--exact",
            "payments_application_process",
            "--ignored",
            "--nocapture",
        ])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .env(
            FIXTURE_ARGS_ENV,
            serde_json::to_string(&application_args).expect("encode application arguments"),
        )
        .env("OBZENFLOW_POSTGRES_SCHEMA", schema)
        .env("OBZENFLOW_JOURNAL_ROOT", journals)
        .output()
        .expect("launch the payments application as a separate process")
}

fn assert_success(label: &str, output: &Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let forbidden = [
        managed_postgres_secret(),
        required_env("OBZENFLOW_POSTGRES_URL"),
        required_env("OBZENFLOW_POSTGRES_TEST_URL"),
        required_env("OBZENFLOW_POSTGRES_TEST_PROJECT"),
        required_env("OBZENFLOW_POSTGRES_TEST_CONTAINER_ID"),
        required_env("OBZENFLOW_POSTGRES_TEST_TLS_DIR"),
    ];
    for value in forbidden {
        assert!(
            !stdout.contains(&value),
            "{label} stdout exposed harness evidence"
        );
        assert!(
            !stderr.contains(&value),
            "{label} stderr exposed harness evidence"
        );
    }
    assert!(
        output.status.success(),
        "{label} failed with {}; stdout={}; stderr={}",
        output.status,
        stdout.trim(),
        stderr.trim()
    );
}

async fn assert_payment_rows(pool: &PgPool, schema: &str) {
    let rows = sqlx::query_as::<_, (i64, String, String, i64)>(&format!(
        "SELECT payment_id, order_id, customer_id, amount_cents \
         FROM \"{schema}\".payments ORDER BY payment_id"
    ))
    .fetch_all(pool)
    .await
    .expect("inspect payment destination from the test boundary");
    assert_eq!(
        rows,
        vec![
            (
                1001,
                "order-501".to_string(),
                "customer-71".to_string(),
                12_500,
            ),
            (
                1002,
                "order-502".to_string(),
                "customer-93".to_string(),
                8_750,
            ),
        ]
    );
}

fn run_directories(root: &Path) -> Vec<PathBuf> {
    let flows = root.join("flows");
    let mut runs = fs::read_dir(flows)
        .expect("read example flow archives")
        .map(|entry| entry.expect("read archive entry").path())
        .filter(|path| path.join("run_manifest.json").is_file())
        .collect::<Vec<_>>();
    runs.sort();
    runs
}

fn assert_durable_evidence_is_redacted(root: &Path) {
    let port = required_env("OBZENFLOW_POSTGRES_TEST_PORT");
    let forbidden = [
        managed_postgres_secret(),
        required_env("OBZENFLOW_POSTGRES_URL"),
        required_env("OBZENFLOW_POSTGRES_TEST_URL"),
        required_env("OBZENFLOW_POSTGRES_TEST_PROJECT"),
        required_env("OBZENFLOW_POSTGRES_TEST_CONTAINER_ID"),
        required_env("OBZENFLOW_POSTGRES_TEST_TLS_DIR"),
    ];
    let mut pending = vec![root.to_path_buf()];
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            pending.extend(
                fs::read_dir(&path)
                    .expect("read durable evidence directory")
                    .map(|entry| entry.expect("read durable evidence entry").path()),
            );
            continue;
        }
        let bytes = fs::read(&path).expect("read durable evidence file");
        let text = String::from_utf8_lossy(&bytes);
        for value in &forbidden {
            assert!(
                value.is_empty() || !text.contains(value),
                "durable evidence {} exposed service configuration",
                path.display()
            );
        }
        for detail in FORBIDDEN_DURABLE_DETAILS {
            assert!(
                !text.contains(detail),
                "durable evidence {} exposed connector policy detail",
                path.display()
            );
        }
        if path.extension().and_then(|value| value.to_str()) == Some("json") {
            let value: Value = serde_json::from_slice(&bytes).expect("durable JSON is valid");
            assert_json_is_redacted(&path, &value, &port);
        }
    }
}

fn managed_postgres_secret() -> String {
    let path = required_env("PGPASSFILE");
    let contents = fs::read_to_string(path).expect("read managed PostgreSQL pgpass file");
    let secret = contents
        .lines()
        .next()
        .and_then(|line| line.rsplit_once(':').map(|(_, secret)| secret.to_string()))
        .expect("managed pgpass has five fields");
    assert_eq!(secret.len(), 64, "managed secret uses the generated shape");
    secret
}

fn assert_json_is_redacted(path: &Path, value: &Value, forbidden_port: &str) {
    match value {
        Value::Object(fields) => {
            for (key, value) in fields {
                let normalized = key.to_ascii_lowercase().replace('-', "_");
                assert!(
                    !FORBIDDEN_DURABLE_KEYS.contains(&normalized.as_str()),
                    "durable JSON {} contains forbidden field {key}",
                    path.display()
                );
                assert_json_is_redacted(path, value, forbidden_port);
            }
        }
        Value::Array(values) => {
            for value in values {
                assert_json_is_redacted(path, value, forbidden_port);
            }
        }
        Value::Number(number) => assert_ne!(number.to_string(), forbidden_port),
        Value::String(value) => assert_ne!(value, forbidden_port),
        _ => {}
    }
}
