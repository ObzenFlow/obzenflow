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
    ReleaseStock, ReserveStock, WarehouseConfig, WarehouseEffectBindings, WarehouseStats,
};
use obzenflow::sources;
use obzenflow_adapters::middleware::{CircuitBreaker, EffectResilience};
use obzenflow_dsl::{effectful_stateful, flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::stages::sink::SinkTyped;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const DEMO_JOURNAL_ROOT: &str = "target/flash-sale-allocation-logs";

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
    assemble_flow(
        scripted_inputs(),
        WarehouseConfig::default(),
        Arc::new(WarehouseStats::default()),
        PathBuf::from(DEMO_JOURNAL_ROOT),
    )
}

/// Build the same topology for the executable and its journal acceptance test.
pub fn assemble_flow(
    inputs: Vec<AllocationInput>,
    warehouse_config: WarehouseConfig,
    warehouse_stats: Arc<WarehouseStats>,
    journal_root: PathBuf,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let WarehouseEffectBindings {
            reserve: reserve_warehouse,
            release: release_warehouse,
        } = WarehouseEffectBindings::with_stats(&warehouse_config, warehouse_stats.clone())
            .expect("static warehouse bindings must be valid");

        // One deliberately slow successful reservation opens this breaker.
        // Subsequent ReserveStock calls fail fast, while ReleaseStock remains
        // isolated because it has no policy attachment.
        let reserve_breaker = CircuitBreaker::builder()
            .count_window(1)
            .minimum_calls(1)
            .failure_rate_threshold(1.0)
            .slow_call_duration(Duration::from_millis(1))
            .slow_call_rate_threshold(1.0)
            .open_for(Duration::from_secs(60))
            .probes(1)
            .build()
            .expect("reserve breaker configuration must be valid");
        let reserve_resilience = EffectResilience::with_breaker(reserve_breaker)
            .build()
            .expect("reserve resilience configuration must be valid");

        let allocation_feed = sources::finite(inputs.clone());
        let allocator = Allocator::new(1);
        let record_reservation = SinkTyped::new(|reserved: StockReserved| async move {
            tracing::info!(order_id = %reserved.order_id.0, "stock reservation delivered");
        });
        let record_release = SinkTyped::new(|released: StockReleased| async move {
            tracing::info!(order_id = %released.order_id.0, "stock release delivered");
        });
        let record_reservation_failure = SinkTyped::new(|failed: ReservationFailed| async move {
            tracing::info!(order_id = %failed.order_id.0, "reservation failure delivered");
        });

        Ok(flow! {
            name: "flash_sale_allocation",
            journals: disk_journals(journal_root.clone()),

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
                    observers: [],
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
    })
}
