// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A second production-shaped PostgreSQL sink consumer.
//!
//! The repository-owned xtask provisions the destination. This example owns
//! only typed inventory inputs, the fixed UPSERT operation, flow execution,
//! and read-only verification of the resulting destination state.

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
use sqlx::PgPool;
use std::path::{Path, PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InventoryLevel {
    warehouse: String,
    sku: String,
    available_units: i64,
    replenishment_due: bool,
}

impl TypedPayload for InventoryLevel {
    const EVENT_TYPE: &'static str = "example.postgres.inventory_level";
}

#[derive(Clone, Debug)]
struct InventoryLevelBinder;

impl PostgresBind<InventoryLevel> for InventoryLevelBinder {
    fn bind(&self, bindings: &mut PostgresBindings, input: &InventoryLevel) {
        bindings
            .bind(&input.warehouse)
            .bind(&input.sku)
            .bind(input.available_units)
            .bind(input.replenishment_due);
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
        "OBZENFLOW_POSTGRES_INVENTORY_SCHEMA must match [A-Za-z_][A-Za-z0-9_$]* and be at most 63 bytes"
    );
    Ok(format!("\"{}\"", value.replace('"', "\"\"")))
}

fn latest_run_dir(root: &Path) -> Result<PathBuf> {
    let mut runs = std::fs::read_dir(root.join("flows"))
        .context("read PostgreSQL inventory flow archives")?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.join("run_manifest.json").is_file())
        .collect::<Vec<_>>();
    runs.sort();
    runs.pop()
        .context("locate PostgreSQL inventory run archive")
}

fn inventory_flow(
    journals: PathBuf,
    connection: PostgresConnection,
    schema: String,
    #[cfg(feature = "test-support")] probe: PostgresTestProbe,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let levels = sources::finite([
            InventoryLevel {
                warehouse: "toronto".to_string(),
                sku: "sku-100".to_string(),
                available_units: 18,
                replenishment_due: false,
            },
            InventoryLevel {
                warehouse: "vancouver".to_string(),
                sku: "sku-200".to_string(),
                available_units: 3,
                replenishment_due: true,
            },
        ]);
        let postgres = PostgresSink::<InventoryLevel>::builder()
            .connection(connection)
            .insert_into(
                &schema,
                "inventory_levels",
                "(warehouse, sku, available_units, replenishment_due) \
                 VALUES ($1, $2, $3, $4) \
                 ON CONFLICT (warehouse, sku) DO UPDATE SET \
                 available_units = EXCLUDED.available_units, \
                 replenishment_due = EXCLUDED.replenishment_due",
            )
            .map_err(build_error)?
            .batch_size(1)
            .map_err(build_error)?
            .redelivery_safety(SinkRedeliverySafety::SafeToRepeat);
        #[cfg(feature = "test-support")]
        let postgres = postgres.test_probe(probe);
        let postgres = postgres
            .bind_with(InventoryLevelBinder)
            .build()
            .map_err(build_error)?;

        Ok(flow! {
            name: "postgres_sink_inventory",
            journals: disk_journals(journals),

            stages: {
                levels = source!(InventoryLevel => levels);
                postgres = sink!(InventoryLevel => postgres);
            },

            topology: {
                levels |> postgres;
            }
        })
    })
}

async fn inspect_destination(url: &str, schema: &str) -> Result<Vec<(String, String, i64, bool)>> {
    let quoted_schema = quote_fixture_identifier(schema)?;
    let pool = PgPool::connect(url)
        .await
        .context("connect to PostgreSQL for inventory verification")?;
    let rows = sqlx::query_as::<_, (String, String, i64, bool)>(&format!(
        "SELECT warehouse, sku, available_units, replenishment_due \
         FROM {quoted_schema}.inventory_levels \
         WHERE (warehouse, sku) IN (('toronto', 'sku-100'), ('vancouver', 'sku-200')) \
         ORDER BY warehouse, sku"
    ))
    .fetch_all(&pool)
    .await
    .context("verify inventory destination rows")?;
    ensure!(
        rows == vec![
            ("toronto".to_string(), "sku-100".to_string(), 18, false,),
            ("vancouver".to_string(), "sku-200".to_string(), 3, true,),
        ],
        "inventory destination did not converge: {rows:?}"
    );
    Ok(rows)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli_args = std::env::args_os().collect::<Vec<_>>();
    let cli = FlowConfig::try_parse_from(cli_args.clone()).context("parse example arguments")?;
    if cli.replay_from.is_some() {
        ensure!(
            cli.verify,
            "archive redelivery in this witness requires --verify so journals are compared"
        );
    }

    let url = std::env::var("OBZENFLOW_POSTGRES_URL")
        .context("set OBZENFLOW_POSTGRES_URL to a PostgreSQL connection URL")?;
    let schema = std::env::var("OBZENFLOW_POSTGRES_INVENTORY_SCHEMA")
        .unwrap_or_else(|_| "obzenflow_inventory_example".to_string());
    let journals = std::env::var_os("OBZENFLOW_INVENTORY_JOURNAL_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/postgres-sink-inventory"));
    let journal_root = journals.clone();
    let connection =
        PostgresConnection::from_url(&url, PostgresTransport::ExternallyProtectedPlaintext)
            .context("parse PostgreSQL connection configuration")?;
    #[cfg(feature = "test-support")]
    let probe = PostgresTestProbe::default();

    FlowApplication::builder()
        .with_cli_args(cli_args)
        .run_async(inventory_flow(
            journals,
            connection,
            schema.clone(),
            #[cfg(feature = "test-support")]
            probe.clone(),
        ))
        .await?;

    let rows = inspect_destination(&url, &schema).await?;
    #[cfg(feature = "test-support")]
    {
        let calls = probe.snapshot();
        ensure!(
            calls.count(SinkExternalCallKind::Execute) == 2,
            "expected exactly two PostgreSQL executions, observed {}",
            calls.count(SinkExternalCallKind::Execute)
        );
        ensure!(
            calls.count(SinkExternalCallKind::Commit) == 2,
            "expected exactly two PostgreSQL commits, observed {}",
            calls.count(SinkExternalCallKind::Commit)
        );
        println!("Inventory PostgreSQL proof calls: execute=2 commit=2");
    }
    println!("Verified inventory rows {rows:?}.");
    println!(
        "Run directory: {}",
        latest_run_dir(&journal_root)?.display()
    );
    println!("Logical destination: postgres.{schema}.inventory_levels");
    Ok(())
}
