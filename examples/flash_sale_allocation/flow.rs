// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flow assembly for the flash-sale allocation witness.

use super::allocation::Allocator;
use super::desk::ReservationDesk;
use super::domain::{
    AllocationInput, CancelIgnored, HoldId, OrderCancelled, OrderId, OrderPlaced,
    ReservationConfirmed, ReservationFailed, Sku, SoldOut, StockReleased, StockReserved,
};
use super::warehouse::{warehouse_system, StockReservation, WarehouseConfig, WarehouseStats};
use obzenflow::effects::SystemError;
use obzenflow::flow::{effectful_stateful, flow, sink, source, stateful, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::middleware::{circuit_breaker, MiddlewareFactory};
use obzenflow::stages::sinks::TracingSink;
use obzenflow::stages::sources;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const DEMO_JOURNAL_ROOT: &str = "target/flash-sale-allocation-logs";

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlashSaleSettings {
    warehouse: WarehouseSettings,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct WarehouseSettings {
    id: String,
    reserve_latency_ms: u64,
    reserve_unavailable: bool,
    reservation_pace_ms: u64,
}

impl WarehouseSettings {
    fn reservation_pace(&self) -> Duration {
        Duration::from_millis(self.reservation_pace_ms)
    }

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

pub fn phone_reservations() -> Vec<StockReserved> {
    let sku = Sku::from("store-allotment");
    (1..=3)
        .map(|n| StockReserved {
            order_id: OrderId(format!("phone-{n}")),
            sku: sku.clone(),
            hold_id: HoldId(format!("desk:phone-{n}")),
        })
        .collect()
}

pub fn build_flow() -> FlowDefinition {
    settings_flow(
        scripted_inputs(),
        Arc::new(WarehouseStats::default()),
        PathBuf::from(DEMO_JOURNAL_ROOT),
    )
}

pub fn settings_flow(
    inputs: Vec<AllocationInput>,
    warehouse_stats: Arc<WarehouseStats>,
    journal_root: PathBuf,
) -> FlowDefinition {
    FlowDefinition::materialize_with(move |_runtime_config, settings: FlashSaleSettings| {
        let reservation_pace = settings.warehouse.reservation_pace();
        Ok(allocation_flow(
            inputs,
            settings.warehouse.into_config(),
            reservation_pace,
            warehouse_stats,
            journal_root,
        )?)
    })
}

#[cfg(test)]
pub fn assemble_flow(
    inputs: Vec<AllocationInput>,
    warehouse_config: WarehouseConfig,
    warehouse_stats: Arc<WarehouseStats>,
    journal_root: PathBuf,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        Ok(allocation_flow(
            inputs,
            warehouse_config,
            Duration::ZERO,
            warehouse_stats,
            journal_root,
        )?)
    })
}

fn allocation_flow(
    inputs: Vec<AllocationInput>,
    warehouse_config: WarehouseConfig,
    reservation_pace: Duration,
    warehouse_stats: Arc<WarehouseStats>,
    journal_root: PathBuf,
) -> Result<FlowDefinition, SystemError> {
    let warehouse = warehouse_system(&warehouse_config, warehouse_stats)?;

    let reserve_breaker = circuit_breaker()
        .count_window(1)
        .minimum_calls(1)
        .failure_rate_threshold(1.0)
        .slow_call_duration(Duration::from_millis(1))
        .slow_call_rate_threshold(1.0)
        .open_for(Duration::from_secs(60))
        .probes(1);
    let reserve_resilience = reserve_breaker.for_effect::<StockReservation>();

    let allocation_feed = sources::ValuesSource::new(inputs);
    let phone_feed = sources::ValuesSource::new(phone_reservations());
    let allocator = Allocator::new(warehouse, 1).with_reservation_pace(reservation_pace);
    let desk = ReservationDesk;
    let record_confirmation = TracingSink::new(|confirmed: &ReservationConfirmed| {
        tracing::info!(
            order_id = %confirmed.order_id.0,
            confirmation = confirmed.confirmation,
            "reservation confirmation delivered"
        );
    });
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
                with reserve_resilience
                => allocator,
            );

            reservations = sink!(StockReserved => record_reservation.idempotent());
            releases = sink!(StockReleased => record_release.idempotent());
            reservation_failures = sink!(
                ReservationFailed => record_reservation_failure.idempotent()
            );

            phone_reservations = source!(StockReserved => phone_feed);
            reservation_desk = stateful!(StockReserved -> ReservationConfirmed => desk);
            confirmations = sink!(ReservationConfirmed => record_confirmation.idempotent());
        },

        topology: {
            allocation_inputs |> allocate_stock;
            allocate_stock |> reservations;
            allocate_stock |> releases;
            allocate_stock |> reservation_failures;
            allocate_stock |> reservation_desk;
            phone_reservations |> reservation_desk;
            reservation_desk |> confirmations;
        }
    })
}
