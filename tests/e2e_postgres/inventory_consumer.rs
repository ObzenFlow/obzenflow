// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A second, independent PostgreSQL consumer used only as acceptance proof.
//!
//! This intentionally lives under `tests/`, not `examples/`: its destination
//! setup and read-back assertions are test concerns rather than user guidance.

use crate::postgres_support;

use async_trait::async_trait;
use obzenflow::effects::postgres::{
    self, default_postgres_client, PostgresClient, PostgresStatement,
};
use obzenflow::effects::{perform, EffectError, Port};
use obzenflow::stages::sources;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use postgres_support::{connection, pool, required_env};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

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

#[obzenflow::port]
#[async_trait]
trait InventoryStorePort {
    #[effect(type = "inventory.store", at_least_once, records = reply)]
    async fn store(&self, level: &InventoryLevelChanged) -> Result<(), EffectError>;
}

struct PostgresInventory {
    postgres: Arc<dyn PostgresClient>,
    insert: PostgresStatement,
}

#[obzenflow::system("inventory-postgres")]
#[async_trait]
impl InventoryStorePort for PostgresInventory {
    async fn store(&self, level: &InventoryLevelChanged) -> Result<(), EffectError> {
        self.postgres
            .execute(&self.insert, |bindings| {
                bindings
                    .bind(&level.warehouse)
                    .bind(&level.sku)
                    .bind(level.available_units)
                    .bind(level.replenishment_due);
            })
            .await
            .map_err(postgres::unsent::<Self>)?;
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn independent_consumer_upserts_inventory_rows() {
    let pool = pool().await;
    let schema = required_env("OBZENFLOW_POSTGRES_INVENTORY_SCHEMA");
    let temp = tempfile::tempdir().expect("create inventory consumer journal directory");
    let journal_root = temp.path().join("journals");

    let insert = PostgresStatement::insert_into(
        &schema,
        "inventory_levels",
        "(warehouse, sku, available_units, replenishment_due) VALUES ($1, $2, $3, $4) \
         ON CONFLICT (warehouse, sku) DO UPDATE SET \
         available_units = EXCLUDED.available_units, \
         replenishment_due = EXCLUDED.replenishment_due",
    )
    .expect("configure inventory destination");
    let system = PostgresInventory {
        postgres: default_postgres_client(connection())
            .expect("build the inventory client without opening PostgreSQL"),
        insert,
    };
    let postgres = perform(|level: &InventoryLevelChanged| level.clone()).through(Port::to(system));

    let flow = FlowDefinition::materialize(move |_runtime_config| {
        let levels = sources::ValuesSource::new([
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

    crate::replay_testkit::unsynced_application()
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
