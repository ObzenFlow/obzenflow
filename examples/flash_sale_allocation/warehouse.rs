// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! The warehouse port, its simulated system, and the requests it answers.

use super::domain::{HoldId, OrderId, Sku, StockReleased, StockReserved};
use async_trait::async_trait;
use obzenflow::effects::EffectError;
use obzenflow::systems::Port;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reservations {
    Available,
    Outage,
}

#[derive(Debug, Clone)]
pub struct WarehouseConfig {
    pub reserve_latency: Duration,
    pub reservations: Reservations,
}

impl WarehouseConfig {
    pub fn new(reserve_latency: Duration, reservations: Reservations) -> Self {
        Self {
            reserve_latency,
            reservations,
        }
    }
}

impl Default for WarehouseConfig {
    fn default() -> Self {
        Self::new(Duration::from_millis(20), Reservations::Available)
    }
}

/// The physical integration port. Capacity is intentionally not implemented
/// here: the allocator is the domain authority and sends only authorised work.
#[obzenflow::port]
#[async_trait]
pub trait WarehousePort {
    #[effect(type = "allocation.reserve_stock", exactly_once)]
    async fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError>;

    #[effect(type = "allocation.release_stock", exactly_once)]
    async fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError>;
}

#[derive(Debug)]
struct SimulatedWarehouse {
    reserve_latency: Duration,
    reservations: Reservations,
}

#[async_trait]
impl WarehousePort for SimulatedWarehouse {
    async fn reserve(&self, request: &StockReservation) -> Result<StockReserved, EffectError> {
        tokio::time::sleep(self.reserve_latency).await;
        match self.reservations {
            Reservations::Available => Ok(request.reserved("warehouse-demo")),
            Reservations::Outage => Err(EffectError::unavailable(
                "warehouse",
                "unavailable",
                "warehouse unavailable",
            )),
        }
    }

    async fn release(&self, request: &StockRelease) -> Result<StockReleased, EffectError> {
        Ok(request.released())
    }
}

pub fn warehouse_port(config: &WarehouseConfig) -> Port<dyn WarehousePort> {
    Port::to(SimulatedWarehouse {
        reserve_latency: config.reserve_latency,
        reservations: config.reservations,
    })
}

#[derive(Clone, Debug, Serialize)]
pub struct StockReservation {
    pub order_id: OrderId,
    pub sku: Sku,
}

impl StockReservation {
    pub fn reserved(&self, warehouse: &str) -> StockReserved {
        StockReserved {
            order_id: self.order_id.clone(),
            sku: self.sku.clone(),
            hold_id: HoldId::from(format!("{warehouse}:{}", self.order_id)),
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct StockRelease {
    pub order_id: OrderId,
    pub sku: Sku,
    pub hold_id: HoldId,
}

impl StockRelease {
    pub fn released(&self) -> StockReleased {
        StockReleased {
            order_id: self.order_id.clone(),
            sku: self.sku.clone(),
            hold_id: self.hold_id.clone(),
        }
    }
}
