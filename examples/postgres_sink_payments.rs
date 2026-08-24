// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! PostgreSQL sink witness for live delivery and archive redelivery.
//!
//! Set `OBZENFLOW_POSTGRES_URL`, run once, then pass the printed archive back
//! with `--replay-from <run-directory> --verify`. The deterministic UPSERT
//! converges to the same complete destination state. The repository proof
//! builds this example with `test-support` and separately observes exactly two
//! real PostgreSQL executions in each archive treatment.

// allow-sink-io: live PostgreSQL witness intentionally exercises and verifies destination I/O

use anyhow::{ensure, Context, Result};
use clap::Parser;
#[cfg(feature = "test-support")]
use obzenflow::sinks::postgres::testing::PostgresTestProbe;
use obzenflow::sinks::postgres::{
    PostgresBind, PostgresBindings, PostgresConnection, PostgresSink, PostgresTransport,
};
use obzenflow::sources;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::{FlowApplication, FlowConfig};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
#[cfg(feature = "test-support")]
use obzenflow_runtime::testing::sink::SinkExternalCallKind;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::path::{Path, PathBuf};

const POSTGRES_SQL_EVIDENCE_CANARY: &str = "obz083c_sql_body_canary_7f3c91a6";

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Payment {
    id: i64,
    amount_cents: i64,
}

impl TypedPayload for Payment {
    const EVENT_TYPE: &'static str = "example.postgres.payment";
}

#[derive(Clone, Debug)]
struct PaymentBinder;

impl PostgresBind<Payment> for PaymentBinder {
    fn bind(&self, bindings: &mut PostgresBindings, input: &Payment) {
        bindings.bind(input.id).bind(input.amount_cents);
    }
}

fn build_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("PostgreSQL sink configuration failed: {error}"))
}

fn quote_fixture_identifier(value: &str) -> Result<String> {
    let bytes = value.as_bytes();
    ensure!(
        !bytes.is_empty()
            && bytes.len() <= 63
            && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
            && bytes[1..]
                .iter()
                .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_' || *byte == b'$'),
        "OBZENFLOW_POSTGRES_SCHEMA must match [A-Za-z_][A-Za-z0-9_$]* and be at most 63 bytes"
    );
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

fn latest_run_dir(root: &Path) -> Result<PathBuf> {
    let mut runs = std::fs::read_dir(root.join("flows"))
        .context("read PostgreSQL example flow archives")?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.join("run_manifest.json").is_file())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop().context("locate PostgreSQL example run archive")
}

fn configured_payment_flow(
    journals: PathBuf,
    connection: PostgresConnection,
    schema: String,
    #[cfg(feature = "test-support")] probe: PostgresTestProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let payments = sources::finite([
            Payment {
                id: 1001,
                amount_cents: 12_500,
            },
            Payment {
                id: 1002,
                amount_cents: 8_750,
            },
        ]);
        let postgres = PostgresSink::<Payment>::builder()
            .connection(connection)
            .insert_into(
                &schema,
                "payments",
                format!(
                    "(id, amount_cents) VALUES ($1, $2) \
                     ON CONFLICT (id) DO UPDATE SET amount_cents = EXCLUDED.amount_cents \
                     /* {POSTGRES_SQL_EVIDENCE_CANARY} */"
                ),
            )
            .map_err(build_error)?
            .batch_size(2)
            .map_err(build_error)?
            .redelivery_safety(SinkRedeliverySafety::SafeToRepeat);
        #[cfg(feature = "test-support")]
        let postgres = postgres.test_probe(probe);
        let postgres = postgres
            .bind_with(PaymentBinder)
            .build()
            .map_err(build_error)?;

        Ok(flow! {
            name: "postgres_sink_payments",
            journals: disk_journals(journals),

            stages: {
                payments = source!(Payment => payments);
                postgres = sink!(Payment => postgres);
            },

            topology: {
                payments |> postgres;
            }
        })
    })
}

#[derive(Debug)]
struct PaymentDestinationSnapshot {
    rows: Vec<(i64, i64)>,
}

async fn inspect_destination(url: &str, schema: &str) -> Result<PaymentDestinationSnapshot> {
    let quoted_schema = quote_fixture_identifier(schema)?;
    let pool = PgPool::connect(url)
        .await
        .context("connect to PostgreSQL for example verification")?;
    let rows = sqlx::query(&format!(
        "SELECT id, amount_cents FROM {quoted_schema}.payments \
         WHERE id IN (1001, 1002) ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .context("verify converged payment rows")?
    .into_iter()
    .map(|row| (row.get::<i64, _>("id"), row.get::<i64, _>("amount_cents")))
    .collect::<Vec<_>>();
    ensure!(
        rows == vec![(1001, 12_500), (1002, 8_750)],
        "payment destination did not converge: {rows:?}"
    );
    let relations = sqlx::query_scalar::<_, String>(
        "SELECT c.relname::text FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') ORDER BY c.relname",
    )
    .bind(schema)
    .fetch_all(&pool)
    .await
    .context("verify destination relations")?;
    ensure!(
        relations == vec!["payments"],
        "payment destination contains unexpected relations: {relations:?}"
    );
    let user_triggers = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pg_trigger t \
         JOIN pg_class c ON c.oid = t.tgrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND NOT t.tgisinternal",
    )
    .bind(schema)
    .fetch_one(&pool)
    .await
    .context("verify destination triggers")?;
    ensure!(
        user_triggers == 0,
        "payment destination contains duplicate-sensitive user triggers"
    );
    let functions = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM pg_proc p \
         JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1",
    )
    .bind(schema)
    .fetch_one(&pool)
    .await
    .context("verify destination functions")?;
    ensure!(
        functions == 0,
        "payment destination contains unexpected user functions"
    );
    Ok(PaymentDestinationSnapshot { rows })
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli_args = std::env::args_os().collect::<Vec<_>>();
    let inspect_only = cli_args.get(1).is_some_and(|arg| arg == "--inspect");
    if inspect_only {
        ensure!(
            cli_args.len() == 2,
            "--inspect accepts no additional arguments"
        );
        let url = std::env::var("OBZENFLOW_POSTGRES_URL")
            .context("set OBZENFLOW_POSTGRES_URL to a PostgreSQL connection URL")?;
        let schema = std::env::var("OBZENFLOW_POSTGRES_SCHEMA")
            .unwrap_or_else(|_| "obzenflow_example".to_string());
        let snapshot = inspect_destination(&url, &schema).await?;
        println!(
            "Inspected converged rows {:?}; secondary_state=converged",
            snapshot.rows
        );
        return Ok(());
    }
    let cli = FlowConfig::try_parse_from(cli_args.clone()).context("parse example arguments")?;
    let verifies_archive = cli.replay_from.is_some();
    if verifies_archive {
        ensure!(
            cli.verify,
            "archive redelivery in this witness requires --verify so journals are compared"
        );
    }

    let url = std::env::var("OBZENFLOW_POSTGRES_URL")
        .context("set OBZENFLOW_POSTGRES_URL to a PostgreSQL connection URL")?;
    let schema = std::env::var("OBZENFLOW_POSTGRES_SCHEMA")
        .unwrap_or_else(|_| "obzenflow_example".to_string());
    let journals = std::env::var_os("OBZENFLOW_JOURNAL_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/postgres-sink-payments"));
    let journal_root = journals.clone();

    let connection =
        PostgresConnection::from_url(&url, PostgresTransport::ExternallyProtectedPlaintext)
            .context("parse PostgreSQL connection configuration")?;
    #[cfg(feature = "test-support")]
    let probe = PostgresTestProbe::default();
    FlowApplication::builder()
        .with_cli_args(cli_args)
        .run_async(configured_payment_flow(
            journals,
            connection,
            schema.clone(),
            #[cfg(feature = "test-support")]
            probe.clone(),
        ))
        .await?;

    let snapshot = inspect_destination(&url, &schema).await?;
    #[cfg(feature = "test-support")]
    {
        let calls = probe.snapshot();
        ensure!(
            calls.count(SinkExternalCallKind::Execute) == 2,
            "expected exactly two PostgreSQL executions, observed {}",
            calls.count(SinkExternalCallKind::Execute)
        );
        ensure!(
            calls.count(SinkExternalCallKind::Commit) == 1,
            "expected exactly one PostgreSQL commit, observed {}",
            calls.count(SinkExternalCallKind::Commit)
        );
        println!("PostgreSQL proof calls: execute=2 commit=1");
    }
    if verifies_archive {
        println!(
            "Verified converged rows {:?}, converged secondary state, and matching archive-redelivery journals.",
            snapshot.rows
        );
    } else {
        println!(
            "Verified converged rows {:?} and converged secondary state.",
            snapshot.rows
        );
    }
    let run_directory = latest_run_dir(&journal_root)?;
    let quoted_schema = quote_fixture_identifier(&schema)?;
    println!("Run directory: {}", run_directory.display());
    println!("Logical destination: postgres.{schema}.payments");
    println!(
        "Inspect rows safely through the active session: cargo xtask postgres run -- psql \"$OBZENFLOW_POSTGRES_URL\" -c 'TABLE {quoted_schema}.payments'"
    );
    if !verifies_archive {
        println!(
            "After confirming the current operation is safe, authorize archive redelivery: cargo xtask postgres run -- cargo run -p obzenflow --features postgres --example postgres_sink_payments -- --replay-from {} --verify",
            run_directory.display()
        );
    }
    Ok(())
}
