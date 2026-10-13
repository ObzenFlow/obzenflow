// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow assembly for the flash-sale allocation witness.

use super::allocation::Allocator;
use super::domain::{
    AllocationInput, CancelIgnored, OrderCancelled, OrderId, OrderPlaced, ReservationFailed, Sku,
    SoldOut, StockReleased, StockReserved,
};
use super::warehouse::{
    ReleaseStock, ReserveStock, WarehouseBindingsBuildError, WarehouseConfig,
    WarehouseEffectBindings, WarehouseStats,
};
use obzenflow::flow::{effectful_stateful, flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::circuit_breaker;
use obzenflow::stages::sinks::TracingSink;
use obzenflow::stages::sources;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const DEMO_JOURNAL_ROOT: &str = "target/flash-sale-allocation-logs";

/// The demo's `[app]` settings (FLOWIP-010t).
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashSaleSettings {
    warehouse: WarehouseSettings,
}

/// The simulated warehouse. Its id is binding evidence for every warehouse
/// effect, so changing it refuses replay and resume of earlier runs.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WarehouseSettings {
    id: String,
    reserve_latency_ms: u64,
    reserve_unavailable: bool,
}

impl WarehouseSettings {
    fn into_config(self) -> WarehouseConfig {
        WarehouseConfig::new(
            self.id,
            Duration::from_millis(self.reserve_latency_ms),
            self.reserve_unavailable,
        )
    }
}

pub fn scripted_inputs() -> Vec<AllocationInput> {
    let sku = Sku::from("flash-sku");
    vec![
        AllocationInput::OrderPlaced(OrderPlaced {
            order_id: OrderId::from("order-1"),
            sku: sku.clone(),
        }),
        // Capacity is still occupied by order-1, so no reserve effect runs.
        AllocationInput::OrderPlaced(OrderPlaced {
            order_id: OrderId::from("order-2"),
            sku: sku.clone(),
        }),
        // Release has no reserve policy and remains live after the slow first
        // reservation opens the reserve-only circuit breaker.
        AllocationInput::OrderCancelled(OrderCancelled {
            order_id: OrderId::from("order-1"),
        }),
        AllocationInput::OrderPlaced(OrderPlaced {
            order_id: OrderId::from("order-3"),
            sku: sku.clone(),
        }),
        AllocationInput::OrderCancelled(OrderCancelled {
            order_id: OrderId::from("missing-order"),
        }),
        AllocationInput::OrderPlaced(OrderPlaced {
            order_id: OrderId::from("order-4"),
            sku,
        }),
    ]
}

pub fn build_flow() -> FlowDefinition {
    settings_flow(
        scripted_inputs(),
        Arc::new(WarehouseStats::default()),
        PathBuf::from(DEMO_JOURNAL_ROOT),
    )
}

/// The shipped flow, with its warehouse read from the `[app]` settings.
pub fn settings_flow(
    inputs: Vec<AllocationInput>,
    warehouse_stats: Arc<WarehouseStats>,
    journal_root: PathBuf,
) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: FlashSaleSettings| {
        allocation_flow(
            inputs,
            settings.warehouse.into_config(),
            warehouse_stats,
            journal_root,
        )
        .map_err(warehouse_binding_failed)
    })
}

/// The same topology with an explicit warehouse, for the journal acceptance tests.
#[cfg(test)]
pub fn assemble_flow(
    inputs: Vec<AllocationInput>,
    warehouse_config: WarehouseConfig,
    warehouse_stats: Arc<WarehouseStats>,
    journal_root: PathBuf,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        allocation_flow(inputs, warehouse_config, warehouse_stats, journal_root)
            .map_err(warehouse_binding_failed)
    })
}

fn warehouse_binding_failed(error: WarehouseBindingsBuildError) -> FlowBuildError {
    FlowBuildError::BindingConfiguration {
        binding: "warehouse".to_string(),
        detail: error.to_string(),
    }
}

fn allocation_flow(
    inputs: Vec<AllocationInput>,
    warehouse_config: WarehouseConfig,
    warehouse_stats: Arc<WarehouseStats>,
    journal_root: PathBuf,
) -> Result<FlowDefinition, WarehouseBindingsBuildError> {
    let WarehouseEffectBindings {
        reserve: reserve_warehouse,
        release: release_warehouse,
    } = WarehouseEffectBindings::with_stats(&warehouse_config, warehouse_stats)?;

    // One deliberately slow successful reservation opens this breaker.
    // Subsequent ReserveStock calls fail fast, while ReleaseStock remains
    // isolated because it has no policy attachment.
    let reserve_breaker = circuit_breaker()
        .count_window(1)
        .minimum_calls(1)
        .failure_rate_threshold(1.0)
        .slow_call_duration(Duration::from_millis(1))
        .slow_call_rate_threshold(1.0)
        .open_for(Duration::from_secs(60))
        .probes(1);
    let reserve_resilience = reserve_breaker;

    let allocation_feed = sources::ValuesSource::new(inputs);
    let allocator = Allocator::new(1);
    let record_reservation = TracingSink::new(|reserved: &StockReserved| {
        tracing::info!(order_id = %reserved.order_id.0, "stock reservation delivered");
    });
    let record_release = TracingSink::new(|released: &StockReleased| {
        tracing::info!(order_id = %released.order_id.0, "stock release delivered");
    });
    let record_reservation_failure = TracingSink::new(|failed: &ReservationFailed| {
        tracing::info!(order_id = %failed.order_id.0, "reservation failure delivered");
    });

    Ok(flow! {
        name: "flash_sale_allocation",
        journals: disk_journals(journal_root),

        stages: {
            allocation_inputs = source!(AllocationInput => allocation_feed);

            allocate_stock = effectful_stateful!(
                AllocationInput -> {
                    StockReserved,
                    StockReleased,
                    ReservationFailed,
                    SoldOut,
                    CancelIgnored,
                }
                uses {
                    ReserveStock
                        via reserve_warehouse
                        with reserve_resilience,
                    ReleaseStock
                        via release_warehouse,
                }
                => allocator,
            );

            reservations = sink!(
                StockReserved => record_reservation,
                delivery: idempotent
            );
            releases = sink!(
                StockReleased => record_release,
                delivery: idempotent
            );
            reservation_failures = sink!(
                ReservationFailed => record_reservation_failure,
                delivery: idempotent
            );
        },

        topology: {
            allocation_inputs |> allocate_stock;
            allocate_stock |> reservations;
            allocate_stock |> releases;
            allocate_stock |> reservation_failures;
        }
    })
}
