// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A second, independent PostgreSQL consumer used only as acceptance proof.
//!
//! This intentionally lives under `tests/`, not `examples/`: its destination
//! setup and read-back assertions are test concerns rather than user guidance.

#[path = "support/postgres.rs"]
mod postgres_support;

use obzenflow::application::FlowApplication;
use obzenflow::sinks::postgres::{PostgresBind, PostgresBindings, PostgresSink};
use obzenflow::sources;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::effects::SinkRedeliverySafety;
use postgres_support::{connection, pool, required_env};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct InventoryLevelChanged {
    warehouse: String,
    sku: String,
    available_units: i64,
    replenishment_due: bool,
}

impl TypedPayload for InventoryLevelChanged {
    const EVENT_TYPE: &'static str = "inventory.level_changed";
}

#[derive(Clone, Debug)]
struct InventoryBinder;

impl PostgresBind<InventoryLevelChanged> for InventoryBinder {
    fn bind(&self, bindings: &mut PostgresBindings, level: &InventoryLevelChanged) {
        bindings
            .bind(&level.warehouse)
            .bind(&level.sku)
            .bind(level.available_units)
            .bind(level.replenishment_due);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_inventory_consumer_delivers_through_the_public_connector() {
    let pool = pool().await;
    let schema = required_env("OBZENFLOW_POSTGRES_INVENTORY_SCHEMA");
    let temp = tempfile::tempdir().expect("create inventory consumer journal directory");
    let journal_root = temp.path().join("journals");

    let postgres = PostgresSink::<InventoryLevelChanged>::builder()
        .connection(connection())
        .insert_into(
            &schema,
            "inventory_levels",
            "(warehouse, sku, available_units, replenishment_due) VALUES ($1, $2, $3, $4) \
             ON CONFLICT (warehouse, sku) DO UPDATE SET \
             available_units = EXCLUDED.available_units, \
             replenishment_due = EXCLUDED.replenishment_due",
        )
        .expect("configure inventory destination")
        .batch_size(1)
        .expect("configure inventory batching")
        .redelivery_safety(SinkRedeliverySafety::SafeToRepeat)
        .bind_with(InventoryBinder)
        .build()
        .expect("build the inventory connector without opening PostgreSQL");

    let flow = FlowDefinition::materialize(move |_runtime_config| {
        let levels = sources::finite([
            InventoryLevelChanged {
                warehouse: "toronto".to_string(),
                sku: "sku-100".to_string(),
                available_units: 18,
                replenishment_due: false,
            },
            InventoryLevelChanged {
                warehouse: "vancouver".to_string(),
                sku: "sku-200".to_string(),
                available_units: 3,
                replenishment_due: true,
            },
        ]);
        Ok(flow! {
            name: "postgres_sink_inventory_consumer",
            journals: disk_journals(journal_root),

            stages: {
                levels = source!(InventoryLevelChanged => levels);
                postgres = sink!(InventoryLevelChanged => postgres);
            },

            topology: {
                levels |> postgres;
            }
        })
    });

    FlowApplication::builder()
        .with_cli_args(["postgres-sink-inventory-consumer"])
        .run_async(flow)
        .await
        .expect("run the independent inventory consumer");

    let rows = sqlx::query_as::<_, (String, String, i64, bool)>(&format!(
        "SELECT warehouse, sku, available_units, replenishment_due \
         FROM \"{schema}\".inventory_levels ORDER BY warehouse, sku"
    ))
    .fetch_all(&pool)
    .await
    .expect("inspect inventory destination from the test boundary");
    assert_eq!(
        rows,
        vec![
            ("toronto".to_string(), "sku-100".to_string(), 18, false),
            ("vancouver".to_string(), "sku-200".to_string(), 3, true),
        ]
    );
}
