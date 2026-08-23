// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! PostgreSQL sink witness for live delivery and archive redelivery.
//!
//! Set `OBZENFLOW_POSTGRES_URL`, run once, then pass the printed archive back
//! with `--replay-from <run-directory> --verify`. The deterministic UPSERT
//! converges to the same rows while the checked witness requires matching
//! journals and exactly two real PostgreSQL mutations in each treatment.

// allow-sink-io: live PostgreSQL witness intentionally exercises and verifies destination I/O

use anyhow::{ensure, Context, Result};
use clap::Parser;
use obzenflow::sinks::postgres::{
    PostgresBind, PostgresBindings, PostgresConnection, PostgresSink, PostgresTransport,
};
use obzenflow::sources;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowBuildFailure, FlowDefinition};
use obzenflow_infra::application::{FlowApplication, FlowConfig};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use serde::{Deserialize, Serialize};
use sqlx::{PgPool, Row};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

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
            .redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
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

async fn prepare_destination(url: &str, schema: &str) -> Result<i64> {
    let schema = quote_fixture_identifier(schema)?;
    let pool = PgPool::connect(url)
        .await
        .context("connect to PostgreSQL for example setup")?;
    sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool)
        .await
        .context("create example schema")?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {schema}.payments (\
         id BIGINT PRIMARY KEY, amount_cents BIGINT NOT NULL)"
    ))
    .execute(&pool)
    .await
    .context("create example payment table")?;
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {schema}.payment_delivery_audit (\
         call_id BIGSERIAL PRIMARY KEY, payment_id BIGINT NOT NULL)"
    ))
    .execute(&pool)
    .await
    .context("create example delivery-audit table")?;
    sqlx::query(&format!(
        "CREATE OR REPLACE FUNCTION {schema}.record_payment_delivery() \
         RETURNS trigger AS $$ BEGIN \
         INSERT INTO {schema}.payment_delivery_audit (payment_id) VALUES (NEW.id); \
         RETURN NEW; END; $$ LANGUAGE plpgsql"
    ))
    .execute(&pool)
    .await
    .context("create example delivery-audit function")?;
    sqlx::query(&format!(
        "DROP TRIGGER IF EXISTS record_payment_delivery ON {schema}.payments"
    ))
    .execute(&pool)
    .await
    .context("reset example delivery-audit trigger")?;
    sqlx::query(&format!(
        "CREATE TRIGGER record_payment_delivery AFTER INSERT OR UPDATE \
         ON {schema}.payments FOR EACH ROW \
         EXECUTE FUNCTION {schema}.record_payment_delivery()"
    ))
    .execute(&pool)
    .await
    .context("create example delivery-audit trigger")?;
    sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {schema}.payment_delivery_audit"
    ))
    .fetch_one(&pool)
    .await
    .context("read the delivery-audit baseline")
}

#[derive(Debug)]
struct PaymentDestinationSnapshot {
    rows: Vec<(i64, i64)>,
    delivery_audit: i64,
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
    let delivery_audit = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {quoted_schema}.payment_delivery_audit"
    ))
    .fetch_one(&pool)
    .await
    .context("verify PostgreSQL delivery calls")?;
    Ok(PaymentDestinationSnapshot {
        rows,
        delivery_audit,
    })
}

fn payment_flow(
    journals: PathBuf,
    connection: PostgresConnection,
    url: String,
    schema: String,
    audit_baseline: Arc<AtomicI64>,
) -> FlowDefinition {
    // FlowApplication opens and validates any replay archive before invoking
    // this build closure. Keeping destination setup here is what proves a bad
    // raw manifest cannot cause PostgreSQL I/O.
    FlowDefinition::new(move |build_context| async move {
        let flow = configured_payment_flow(journals, connection, schema.clone());
        let baseline = prepare_destination(&url, &schema)
            .await
            .map_err(|error| FlowBuildFailure::from(build_error(error)))?;
        audit_baseline.store(baseline, Ordering::Release);
        flow.build(build_context).await
    })
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
            "Inspected converged rows {:?}; delivery_audit={}",
            snapshot.rows, snapshot.delivery_audit
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
    let audit_baseline = Arc::new(AtomicI64::new(-1));
    FlowApplication::builder()
        .with_cli_args(cli_args)
        .run_async(payment_flow(
            journals,
            connection,
            url.clone(),
            schema.clone(),
            audit_baseline.clone(),
        ))
        .await?;

    let snapshot = inspect_destination(&url, &schema).await?;
    let baseline = audit_baseline.load(Ordering::Acquire);
    ensure!(baseline >= 0, "delivery-audit baseline was not captured");
    ensure!(
        snapshot.delivery_audit - baseline == 2,
        "expected two PostgreSQL sink mutations, observed {}",
        snapshot.delivery_audit - baseline
    );
    if verifies_archive {
        println!(
            "Verified converged rows {:?}, two PostgreSQL sink mutations, and matching replay journals.",
            snapshot.rows
        );
    } else {
        println!(
            "Verified converged rows {:?} and two PostgreSQL sink mutations.",
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
            "Verify redelivery with the same compiled operation: cargo xtask postgres run -- cargo run -p obzenflow --features postgres --example postgres_sink_payments -- --replay-from {} --verify",
            run_directory.display()
        );
    }
    Ok(())
}
