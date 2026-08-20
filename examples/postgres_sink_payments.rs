// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! PostgreSQL sink witness for live delivery and archive redelivery.
//!
//! Set `OBZENFLOW_POSTGRES_URL`, run once, then pass the printed archive back
//! with `--replay-from <run-directory>`. The deterministic UPSERT converges to
//! the same rows while still making real PostgreSQL calls.

// allow-sink-io: live PostgreSQL witness intentionally exercises and verifies destination I/O

use anyhow::{Context, Result};
use obzenflow::sinks::postgres::sqlx::postgres::PgArguments;
use obzenflow::sinks::postgres::sqlx::query::Query;
use obzenflow::sinks::postgres::sqlx::{PgPool, Postgres, Row};
use obzenflow::sinks::postgres::{PostgresBind, PostgresConnection, PostgresSink};
use obzenflow::sources;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::application::FlowApplication;
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
    fn bind<'q>(
        &self,
        query: Query<'q, Postgres, PgArguments>,
        input: &'q Payment,
    ) -> Query<'q, Postgres, PgArguments> {
        query.bind(input.id).bind(input.amount_cents)
    }
}

fn build_error(error: impl std::fmt::Display) -> FlowBuildError {
    FlowBuildError::StageResourcesFailed(format!("PostgreSQL sink configuration failed: {error}"))
}

fn payment_flow(
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
            .table(&schema, "payments")
            .map_err(build_error)?
            .statement(format!(
                "INSERT INTO {schema}.payments (id, amount_cents) VALUES ($1, $2) \
                 ON CONFLICT (id) DO UPDATE SET amount_cents = EXCLUDED.amount_cents"
            ))
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

#[tokio::main]
async fn main() -> Result<()> {
    let url = std::env::var("OBZENFLOW_POSTGRES_URL")
        .context("set OBZENFLOW_POSTGRES_URL to a PostgreSQL connection URL")?;
    let schema = std::env::var("OBZENFLOW_POSTGRES_SCHEMA")
        .unwrap_or_else(|_| "obzenflow_example".to_string());
    let journals = std::env::var_os("OBZENFLOW_JOURNAL_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/postgres-sink-payments"));

    let pool = PgPool::connect(&url)
        .await
        .context("connect to PostgreSQL for example setup")?;
    obzenflow::sinks::postgres::sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(&pool)
        .await
        .context("create example schema")?;
    obzenflow::sinks::postgres::sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS {schema}.payments (\
         id BIGINT PRIMARY KEY, amount_cents BIGINT NOT NULL)"
    ))
    .execute(&pool)
    .await
    .context("create example payment table")?;

    let connection =
        PostgresConnection::from_url(&url).context("parse PostgreSQL connection configuration")?;
    FlowApplication::builder()
        .with_cli_args(std::env::args_os())
        .run_async(payment_flow(journals, connection, schema.clone()))
        .await?;

    let rows = obzenflow::sinks::postgres::sqlx::query(&format!(
        "SELECT id, amount_cents FROM {schema}.payments ORDER BY id"
    ))
    .fetch_all(&pool)
    .await
    .context("verify converged payment rows")?;
    println!("PostgreSQL rows after this treatment:");
    for row in rows {
        println!(
            "  payment {}: {} cents",
            row.get::<i64, _>("id"),
            row.get::<i64, _>("amount_cents")
        );
    }
    Ok(())
}
